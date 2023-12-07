// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/partitioned_aggregation_node.h"

#include <memory>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/spill/spill_stream.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::vectorized {
PartitionedAggregationNode::PartitionedAggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {
    in_memory_agg_node_ = std::make_unique<AggregationNode>(_pool, tnode, descs);
}

PartitionedAggregationNode::~PartitionedAggregationNode() {}
Status PartitionedAggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    state_ = state;
    if (state->query_options().__isset.external_agg_partition_bits) {
        partition_count_bits_ = state->query_options().external_agg_partition_bits;
    }
    partition_count_ = 1 << partition_count_bits_;
    max_partition_index_ = partition_count_ - 1;
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    in_memory_agg_node_->set_children(get_children());
    in_memory_agg_node_->set_prepare_children(false);
    return in_memory_agg_node_->init(tnode, state);
}
Status PartitionedAggregationNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(in_memory_agg_node_->prepare(state));
    return ExecNode::prepare(state);
}
Status PartitionedAggregationNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    return child(0)->open(state);
}
Status PartitionedAggregationNode::alloc_resource(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    return in_memory_agg_node_->alloc_resource(state);
}
Status PartitionedAggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    return Status::OK();
}
Status PartitionedAggregationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}
void PartitionedAggregationNode::release_spill_streams() {
    for (auto& stream : spilled_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    spilled_streams_.clear();
}
void PartitionedAggregationNode::release_resource(RuntimeState* state) {
    ExecNode::release_resource(state);

    release_spill_streams();

    if (spill_merge_promise_) {
        spill_merge_future_.wait();
    }

    in_memory_agg_node_->release_resource(state);
    in_memory_agg_node_.reset();
}

Status PartitionedAggregationNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    RETURN_IF_ERROR(status_);
    sink_eos_ = eos;
    Status st;

    VLOG_ROW << this << " id: " << id() << " sink, eos: " << eos;
    RETURN_IF_ERROR(in_memory_agg_node_->sink(state, input_block, false));
    if (eos) {
        RETURN_IF_ERROR(_prepare_for_reading());
    }
    return Status::OK();
}

Status PartitionedAggregationNode::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                        bool* eos) {
    RETURN_IF_ERROR(status_);
    if (!io_task_finished()) {
        VLOG_ROW << this << " id: " << id() << " pull, !io_task_finished";
        return Status::WaitForIO("merging spilled blocks");
    }
    DCHECK(!spill_merge_promise_);
    VLOG_ROW << this << " id: " << id() << " pull";
    auto status = in_memory_agg_node_->pull(state, output_block, eos);
    if (*eos) {
        *eos = (read_cursor_ == partition_count_);
        VLOG_ROW << this << " id: " << id() << " pull partition eos, read_cursor: " << read_cursor_;
        if (!*eos) {
            VLOG_ROW << this << " id: " << id()
                     << " pull, _initiate_merge_spill_partition_agg_data: " << read_cursor_;
            RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data());
        }
    }
    reached_limit(output_block, eos);
    return status;
}

Status PartitionedAggregationNode::_prepare_for_reading() {
    if (0 == in_memory_agg_node_->revocable_mem_size(state_)) {
        RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data());
        _can_read = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_revoke_memory_internal());
    VLOG_ROW << this << " id: " << id() << " sink eos";
    _can_read = true;
    return Status::OK();
}

Status PartitionedAggregationNode::_initiate_merge_spill_partition_agg_data() {
    DCHECK(!spill_merge_promise_);
    spill_merge_promise_ = std::make_unique<std::promise<Status>>();
    spill_merge_future_ = spill_merge_promise_->get_future();
    RETURN_IF_ERROR(in_memory_agg_node_->prepare_merge_partition_aggregation_data());

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
            [this] {
                Defer defer {[&]() {
                    if (!status_.ok()) {
                        LOG(WARNING) << "merge spill agg data failed: " << status_;
                    }
                    spill_merge_promise_->set_value(status_);
                }};
                bool has_agg_data = false;
                while (!_is_resource_released && !has_agg_data && read_cursor_ < partition_count_) {
                    // merge aggregation data of a spilled partition
                    VLOG_ROW << this << " id: " << id() << " read spilled partition "
                             << read_cursor_ << ", stream count: " << spilled_streams_.size();
                    // optmize: initiate read for many streams and wait for read finish
                    // need to consider memory pressure
                    for (auto& stream : spilled_streams_) {
                        status_ = stream->seek_for_read(read_cursor_);
                        RETURN_IF_ERROR(status_);
                        Block block;
                        bool eos;
                        status_ = stream->read_current_block_sync(&block, &eos);
                        RETURN_IF_ERROR(status_);

                        if (!block.empty()) {
                            has_agg_data = true;
                            status_ = in_memory_agg_node_->merge_spilt_partition_aggregation_data(
                                    &block);
                            RETURN_IF_ERROR(status_);
                        }
                    }
                    read_cursor_++;
                }
                if (read_cursor_ == partition_count_) {
                    release_spill_streams();
                }
                status_ = in_memory_agg_node_->prepare_pull();
                VLOG_ROW << this << " id: " << id()
                         << " merge spilled streams finished, next read_cursor: " << read_cursor_;
                return status_;
            });
}

size_t PartitionedAggregationNode::revocable_mem_size(RuntimeState* state) const {
    if (sink_eos_) {
        return 0;
    }
    size_t size = in_memory_agg_node_->revocable_mem_size(state);
    size_t self_size = size;
    // size += child(0)->revocable_mem_size();
    VLOG_ROW << this << " revocable_mem_size: " << size << ", self size: " << self_size;
    return size;
}

Status PartitionedAggregationNode::_revoke_memory_internal() {
    DCHECK(!spilling_stream_);

    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            spilling_stream_, print_id(state_->query_id()), "agg", id(),
            std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
            runtime_profile()));
    RETURN_IF_ERROR(spilling_stream_->prepare_spill());
    spilled_streams_.emplace_back(spilling_stream_);

    return ExecEnv::GetInstance()
            ->spill_stream_mgr()
            ->get_spill_io_thread_pool(spilling_stream_->get_spill_root_dir())
            ->submit_func([this] {
                bool has_agg_data = false;
                status_ = _release_in_memory_agg_data(has_agg_data);
                if (!status_.ok()) {
                    LOG(WARNING) << this << " id: " << id()
                                 << " spill agg data failed: " << status_;
                    spilling_stream_->end_spill(status_);
                    release_spill_streams();
                    return;
                }
                if (!has_agg_data) {
                    spilled_streams_.pop_back();
                }
                spilling_stream_->end_spill(status_);
            });
}

Status PartitionedAggregationNode::revoke_memory(RuntimeState* state) {
    VLOG_ROW << this << " id: " << id() << " revoke_memory";
    // not allow spill in pull stage
    if (sink_eos_) {
        VLOG_ROW << this << " id: " << id() << " revoke_memory, sink already eos";
        return Status::OK();
    }
    RETURN_IF_ERROR(_revoke_memory_internal());
    return Status::WaitForIO("Spilling");
}

Status PartitionedAggregationNode::_release_in_memory_agg_data(bool& has_agg_data) {
    bool has_null_key;
    Block block;
    std::vector<size_t> keys_hashes;
    RETURN_IF_ERROR(
            in_memory_agg_node_->get_and_release_aggregate_data(block, has_null_key, keys_hashes));
    has_agg_data = !block.empty();
    if (!has_agg_data) {
        return Status::OK();
    }

    std::vector<size_t> partitioned_indices(block.rows());
    std::vector<size_t> blocks_rows(partition_count_);

    // The last row may contain a null key.
    const size_t rows = has_null_key ? block.rows() - 1 : block.rows();
    for (size_t i = 0; i < rows; ++i) {
        const auto index = _get_partition_index(keys_hashes[i]);
        partitioned_indices[i] = index;
        blocks_rows[index]++;
    }

    if (has_null_key) {
        // Here put the row with null key at the last partition.
        const auto index = partition_count_ - 1;
        partitioned_indices[rows] = index;
        blocks_rows[index]++;
    }

    Block block_to_write = block.clone_empty();
    Block empty_block = block.clone_empty();
    for (size_t i = 0; i < partition_count_; ++i) {
        if (blocks_rows[i] == 0) {
            /// Here write one empty block to ensure there are enough blocks in the file,
            /// blocks' count should be equal with partition_count.
            RETURN_IF_ERROR(spilling_stream_->spill_block(empty_block, i == partition_count_ - 1));
            continue;
        }

        MutableBlock mutable_block(std::move(block_to_write));

        for (auto& column : mutable_block.mutable_columns()) {
            column->reserve(blocks_rows[i]);
        }

        size_t begin = 0;
        size_t length = 0;
        for (size_t j = 0; j < partitioned_indices.size(); ++j) {
            if (partitioned_indices[j] != i) {
                if (length > 0) {
                    mutable_block.add_rows(&block, begin, length);
                }
                length = 0;
                continue;
            }

            if (length == 0) {
                begin = j;
            }
            length++;
        }

        if (length > 0) {
            mutable_block.add_rows(&block, begin, length);
        }

        CHECK_EQ(mutable_block.rows(), blocks_rows[i]);
        block_to_write = mutable_block.to_block();
        RETURN_IF_ERROR(spilling_stream_->spill_block(block_to_write, i == partition_count_ - 1));
        block_to_write.clear_column_data();
    }
    return Status::OK();
}

bool PartitionedAggregationNode::io_task_finished() {
    VLOG_ROW << this << " id: " << id() << " io_task_finished???";
    if (spilling_stream_) {
        SpillState spill_state;
        // status will be checked in sink and pull
        status_ = spilling_stream_->get_spill_state(spill_state);
        switch (spill_state) {
        case SpillState::SPILLING:
            VLOG_ROW << this << " id: " << id() << " io_task_finished, is spilling";
            return false;
        case SpillState::SPILL_FINISHED:
            spilling_stream_.reset();
            if (sink_eos_) {
                VLOG_ROW << this << " id: " << id()
                         << " io_task_finished, spill finished, "
                            "_initiate_merge_spill_partition_agg_data: "
                         << read_cursor_;
                if (status_.ok()) {
                    status_ = _initiate_merge_spill_partition_agg_data();
                }
                return false;
            }
            VLOG_ROW << this << " id: " << id() << " io_task_finished, spill finished";
            return true;
        default:
            DCHECK(false);
            return true;
        }
    } else if (sink_eos_) {
        if (spill_merge_promise_) {
            auto status = spill_merge_future_.wait_for(std::chrono::milliseconds(10));
            if (status == std::future_status::ready) {
                spill_merge_promise_.reset();
                VLOG_ROW << this << " id: " << id()
                         << " io_task_finished merge spilled blocks "
                            "finished, status: "
                         << status_;
                return true;
            } else {
                VLOG_ROW << this << " id: " << id()
                         << " io_task_finished merge spilled blocks "
                            "not finished";
                return false;
            }
        } else {
            VLOG_ROW << this << " id: " << id()
                     << " io_task_finished true, sink eos, not spilling, not merging";
            return true;
        }
    }
    return true;
}

} // namespace doris::vectorized