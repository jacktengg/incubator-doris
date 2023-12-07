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

#include "partitioned_aggregation_sink_operator.h"

#include <memory>
#include <string>

#include "pipeline/exec/operator.h"
#include "runtime/primitive_type.h"
#include "vec/common/hash_table/hash.h"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(PartitionedAggSinkOperator, StreamingOperator)

PartitionedBlockingAggSinkLocalState::PartitionedBlockingAggSinkLocalState(
        DataSinkOperatorXBase* parent, RuntimeState* state)
        : PipelineXSinkLocalState<PartitionedAggSinkDependency>(parent, state) {
    agg_sink_local_state_ = BlockingAggSinkLocalState::create_unique(parent, state);
}

Status PartitionedBlockingAggSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = Base::_parent->template cast<Parent>();
    Base::_shared_state->init_spill_params(p.spill_partition_count_bits_);
    return agg_sink_local_state_->init(state, info);
}
Status PartitionedBlockingAggSinkLocalState::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));
    return agg_sink_local_state_->open(state);
}
Status PartitionedBlockingAggSinkLocalState::close(RuntimeState* state, Status exec_status) {
    if (Base::_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(agg_sink_local_state_->close(state, exec_status));
    return Base::close(state, exec_status);
}
template <typename LocalStateType>
PartitionedAggSinkOperatorX<LocalStateType>::PartitionedAggSinkOperatorX(ObjectPool* pool,
                                                                         int operator_id,
                                                                         const TPlanNode& tnode,
                                                                         const DescriptorTbl& descs,
                                                                         bool is_streaming)
        : DataSinkOperatorX<LocalStateType>(operator_id, tnode.node_id) {
    in_memory_agg_operator_ =
            std::make_unique<AggSinkOperatorX<>>(pool, operator_id, tnode, descs, is_streaming);
    in_memory_agg_operator_->set_dests_id({operator_id});
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::init(const TPlanNode& tnode,
                                                         RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::init(tnode, state));
    if (state->query_options().__isset.external_agg_partition_bits) {
        spill_partition_count_bits_ = state->query_options().external_agg_partition_bits;
    }
    spill_partition_count_ = 1 << spill_partition_count_bits_;
    max_spill_partition_index_ = spill_partition_count_ - 1;
    RETURN_IF_ERROR(
            in_memory_agg_operator_->set_child(DataSinkOperatorX<LocalStateType>::_child_x));
    return in_memory_agg_operator_->init(tnode, state);
}
template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::prepare(RuntimeState* state) {
    return in_memory_agg_operator_->prepare(state);
}
template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::open(RuntimeState* state) {
    return in_memory_agg_operator_->open(state);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::sink(doris::RuntimeState* state,
                                                         vectorized::Block* in_block,
                                                         SourceState source_state) {
    RETURN_IF_ERROR(in_memory_agg_operator_->sink(state, in_block, source_state));
    if (source_state == SourceState::FINISHED) {
        RETURN_IF_ERROR(_prepare_for_reading(state));
    }
    return Status::OK();
}
template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::_prepare_for_reading(RuntimeState* state) {
    if (0 == in_memory_agg_operator_->revocable_mem_size(state)) {
        // RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data(state));
        auto& local_state = get_local_state(state);
        local_state._dependency->set_ready_to_read();
        return Status::OK();
    }
    RETURN_IF_ERROR(_revoke_memory_internal(state));
    return Status::OK();
}

template <typename LocalStateType>
size_t PartitionedAggSinkOperatorX<LocalStateType>::revocable_mem_size(RuntimeState* state) const {
    return in_memory_agg_operator_->revocable_mem_size(state);
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::revoke_memory(RuntimeState* state) {
    RETURN_IF_ERROR(_revoke_memory_internal(state));
    return Status::WaitForIO("Spilling");
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::_release_in_memory_agg_data(
        RuntimeState* state, bool& has_agg_data) {
    bool has_null_key;
    vectorized::Block block;
    std::vector<size_t> keys_hashes;
    RETURN_IF_ERROR(in_memory_agg_operator_->get_and_release_aggregate_data(
            state, block, has_null_key, keys_hashes));
    has_agg_data = !block.empty();
    if (!has_agg_data) {
        return Status::OK();
    }

    std::vector<size_t> partitioned_indices(block.rows());
    std::vector<size_t> blocks_rows(spill_partition_count_);

    // The last row may contain a null key.
    const size_t rows = has_null_key ? block.rows() - 1 : block.rows();
    for (size_t i = 0; i < rows; ++i) {
        const auto index = _get_partition_index(keys_hashes[i]);
        partitioned_indices[i] = index;
        blocks_rows[index]++;
    }

    if (has_null_key) {
        // Here put the row with null key at the last partition.
        const auto index = spill_partition_count_ - 1;
        partitioned_indices[rows] = index;
        blocks_rows[index]++;
    }

    vectorized::Block block_to_write = block.clone_empty();
    vectorized::Block empty_block = block.clone_empty();
    for (size_t i = 0; i < spill_partition_count_; ++i) {
        if (blocks_rows[i] == 0) {
            /// Here write one empty block to ensure there are enough blocks in the file,
            /// blocks' count should be equal with partition_count.
            RETURN_IF_ERROR(
                    spilling_stream_->spill_block(empty_block, i == spill_partition_count_ - 1));
            continue;
        }

        vectorized::MutableBlock mutable_block(std::move(block_to_write));

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
        RETURN_IF_ERROR(
                spilling_stream_->spill_block(block_to_write, i == spill_partition_count_ - 1));
        block_to_write.clear_column_data();
    }
    return Status::OK();
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::_revoke_memory_internal(RuntimeState* state) {
    DCHECK(!spilling_stream_);

    auto& local_state = get_local_state(state);

    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            spilling_stream_, print_id(state->query_id()), "agg", id(),
            std::numeric_limits<int32_t>::max(), std::numeric_limits<size_t>::max(),
            local_state.Base::profile()));
    RETURN_IF_ERROR(spilling_stream_->prepare_spill());
    local_state._shared_state->spilled_streams_.emplace_back(spilling_stream_);
    if (!sink_eos_) {
        local_state._dependency->block();
    }

    return ExecEnv::GetInstance()
            ->spill_stream_mgr()
            ->get_spill_io_thread_pool(spilling_stream_->get_spill_root_dir())
            ->submit_func([this, state, &local_state] {
                bool has_agg_data = false;
                status_ = _release_in_memory_agg_data(state, has_agg_data);
                if (!status_.ok()) {
                    LOG(WARNING) << this << " id: " << id()
                                 << " spill agg data failed: " << status_;
                    spilling_stream_->end_spill(status_);
                    release_spill_streams();
                    local_state._dependency->set_ready();
                    return;
                }
                if (!has_agg_data) {
                    local_state._shared_state->spilled_streams_.pop_back();
                }
                spilling_stream_->end_spill(status_);
                if (!sink_eos_) {
                    local_state._dependency->set_ready();
                } else {
                    local_state._dependency->set_ready_to_read();
                }
            });
}

template <typename LocalStateType>
Status PartitionedAggSinkOperatorX<LocalStateType>::_initiate_merge_spill_partition_agg_data(
        RuntimeState* state) {
    // DCHECK(!spill_merge_promise_);
    // spill_merge_promise_ = std::make_unique<std::promise<Status>>();
    // spill_merge_future_ = spill_merge_promise_->get_future();
    return Status::OK();
    // RETURN_IF_ERROR(in_memory_agg_operator_->prepare_merge_partition_aggregation_data());

    /*
    return ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
            [this, state] {
                auto& local_state = get_local_state(state);
                Defer defer {[&]() {
                    if (!status_.ok()) {
                        LOG(WARNING) << "merge spill agg data failed: " << status_;
                    }
                    spill_merge_promise_->set_value(status_);
                    local_state._dependency->set_ready_to_read();
                }};
                bool has_agg_data = false;
                while (!_is_resource_released && !has_agg_data && read_cursor_ < spill_partition_count_) {
                    // merge aggregation data of a spilled partition
                    VLOG_ROW << this << " id: " << id() << " read spilled partition "
                             << read_cursor_ << ", stream count: " << spilled_streams_.size();
                    // optmize: initiate read for many streams and wait for read finish
                    // need to consider memory pressure
                    for (auto& stream : spilled_streams_) {
                        status_ = stream->seek_for_read(read_cursor_);
                        RETURN_IF_ERROR(status_);
                        vectorized::Block block;
                        bool eos;
                        status_ = stream->read_current_block_sync(&block, &eos);
                        RETURN_IF_ERROR(status_);

                        if (!block.empty()) {
                            has_agg_data = true;
                            status_ = in_memory_agg_operator_->merge_spilt_partition_aggregation_data(
                                    &block);
                            RETURN_IF_ERROR(status_);
                        }
                    }
                    read_cursor_++;
                }
                if (read_cursor_ == spill_partition_count_) {
                    release_spill_streams();
                }
                status_ = in_memory_agg_operator_->prepare_pull();
                VLOG_ROW << this << " id: " << id()
                         << " merge spilled streams finished, next read_cursor: " << read_cursor_;
                return status_;
            });
            */
}
template <typename LocalStateType>
void PartitionedAggSinkOperatorX<LocalStateType>::release_spill_streams() {
    // for (auto& stream : spilled_streams_) {
    //     (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    // }
    // spilled_streams_.clear();
}
template class PartitionedAggSinkOperatorX<PartitionedBlockingAggSinkLocalState>;
} // namespace doris::pipeline
