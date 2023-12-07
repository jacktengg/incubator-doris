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

#include "vec/exec/spill_sort_node.h"

#include <glog/logging.h>

#include <memory>

#include "common/status.h"
#include "vec/spill/spill_stream_manager.h"
namespace doris {
namespace vectorized {
SpillSortNode::SpillSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          t_plan_node_(tnode),
          desc_tbl_(descs) {
    async_task_thread_pool_ =
            ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool();
}

SpillSortNode::~SpillSortNode() {
    for (auto& stream : current_merging_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
}

Status SpillSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    state_ = state;
    return ExecNode::init(tnode, state);
}
Status SpillSortNode::prepare(RuntimeState* state) {
    return ExecNode::prepare(state);
}

Status SpillSortNode::alloc_resource(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));

    return _prepare_inmemory_sort_node(state);
}

Status SpillSortNode::_prepare_inmemory_sort_node(RuntimeState* state) {
    in_memory_sort_node_ = std::make_unique<VSortNode>(_pool, t_plan_node_, desc_tbl_);
    in_memory_sort_node_->set_children(get_children());
    in_memory_sort_node_->set_prepare_children(false);
    RETURN_IF_ERROR(in_memory_sort_node_->init(t_plan_node_, state));
    RETURN_IF_ERROR(in_memory_sort_node_->prepare(state));
    RETURN_IF_ERROR(in_memory_sort_node_->alloc_resource(state));
    return Status::OK();
}

void SpillSortNode::release_resource(doris::RuntimeState* state) {
    VLOG_ROW << this << " release_resource";
    in_memory_sort_node_->release_resource(state);
    in_memory_sort_node_.reset();

    for (auto& stream : sorted_streams_) {
        (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
    }
    ExecNode::release_resource(state);
}

Status SpillSortNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    return child(0)->open(state);
}

Status SpillSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    return ExecNode::close(state);
}

Status SpillSortNode::sink(RuntimeState* state, Block* input_block, bool eos) {
    RETURN_IF_ERROR(status_);
    sink_eos_ = eos;
    Status st;
    if (input_block->rows() > 0) {
        update_spill_block_batch_size(input_block);
        RETURN_IF_ERROR(in_memory_sort_node_->sink(state, input_block, false));
    }
    if (eos) {
        RETURN_IF_ERROR(_prepare_for_pull(state));
    }
    return Status::OK();
}

Status SpillSortNode::_prepare_for_pull(RuntimeState* state) {
    if (0 == in_memory_sort_node_->revocable_mem_size(state)) {
        status_ = _merge_sort_spill_streams();
        _can_read = true;
        return status_;
    }
    RETURN_IF_ERROR(_revoke_memory_internal());
    _can_read = true;
    return Status::OK();
}

Status SpillSortNode::_merge_sort_spill_streams() {
    DCHECK(!spill_merge_promise_);
    spill_merge_promise_ = std::make_unique<std::promise<Status>>();
    spill_merge_future_ = spill_merge_promise_->get_future();
    auto* this_ptr = this;
    return async_task_thread_pool_->submit_func([this, this_ptr] {
        Defer defer {[&]() {
            if (!status_.ok()) {
                LOG(WARNING) << this_ptr << " id: " << id()
                             << " _merge_sort_spill_streams failed: " << status_;
            }
            spill_merge_promise_->set_value(status_);
        }};
        while (true) {
            int max_stream_count = (sorted_streams_.size() + 1) / 2;
            max_stream_count = std::max(2, max_stream_count);
            max_stream_count = std::min(32, max_stream_count);
            VLOG_ROW << this_ptr << " id: " << id()
                     << " spill sort merge intermediate streams, stream count: "
                     << sorted_streams_.size()
                     << ", create merger stream count: " << max_stream_count;
            status_ = _create_intermediate_merger(max_stream_count, sort_description_);
            RETURN_IF_ERROR(status_);

            // all the remaining streams can be merged in a run
            if (sorted_streams_.empty()) {
                VLOG_ROW << this_ptr << " id: " << id()
                         << " spill sort merge intermediate streams, final merge";
                return Status::OK();
            }

            SpillStreamSPtr tmp_stream;
            status_ = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    tmp_stream, print_id(state_->query_id()), "sort", id(), spill_block_batch_size_,
                    SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile());
            RETURN_IF_ERROR(status_);

            bool eos = false;
            while (!eos) {
                merge_sorted_block_.clear();
                status_ = merger_->get_next(&merge_sorted_block_, &eos);
                RETURN_IF_ERROR(status_);
                status_ = tmp_stream->add_block(std::move(merge_sorted_block_), false);
                RETURN_IF_ERROR(status_);
            }

            for (auto& stream : current_merging_streams_) {
                (void)ExecEnv::GetInstance()->spill_stream_mgr()->delete_spill_stream(stream);
            }

            sorted_streams_.emplace_back(tmp_stream);
            // status_ = tmp_stream->spill_async_and_wait();
            return status_;
        }
        VLOG_ROW << this_ptr << " id: " << id()
                 << ", spill sort merge intermediate streams finished";
        return Status::OK();
    });
}

void SpillSortNode::update_spill_block_batch_size(const Block* block) {
    auto rows = block->rows();
    if (rows > 0 && 0 == avg_row_bytes_) {
        avg_row_bytes_ = std::max((std::size_t)1, block->bytes() / rows);
        spill_block_batch_size_ =
                (SORT_BLOCK_SPILL_BATCH_BYTES + avg_row_bytes_ - 1) / avg_row_bytes_;
    }
}

size_t SpillSortNode::revocable_mem_size(RuntimeState* state) const {
    if (sink_eos_) {
        VLOG_ROW << this << " id: " << id() << " revocable_mem_size, already sink_eos";
        return 0;
    }
    // size += child(0)->revocable_mem_size();
    return in_memory_sort_node_->revocable_mem_size(state);
}

Status SpillSortNode::_revoke_memory_internal() {
    DCHECK(!spilling_stream_);

    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            spilling_stream_, print_id(state_->query_id()), "sort", id(), spill_block_batch_size_,
            SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile()));
    RETURN_IF_ERROR(spilling_stream_->prepare_spill());
    sorted_streams_.emplace_back(spilling_stream_);

    auto* this_ptr = this;
    return ExecEnv::GetInstance()
            ->spill_stream_mgr()
            ->get_spill_io_thread_pool(spilling_stream_->get_spill_root_dir())
            ->submit_func([this, this_ptr] {
                Defer defer {[&]() {
                    if (!status_.ok()) {
                        LOG(WARNING) << this_ptr << " id: " << id()
                                     << " spill sort data failed: " << status_;
                        sorted_streams_.pop_back();
                        spilling_stream_->end_spill(status_);
                    }
                }};
                Blocks blocks;
                status_ = in_memory_sort_node_->release_sorted_blocks(state_, blocks,
                                                                      spill_block_batch_size_);
                RETURN_IF_ERROR(status_);

                if (sort_description_.empty()) {
                    sort_description_ = in_memory_sort_node_->get_sort_description();
                }

                status_ = _prepare_inmemory_sort_node(state_);
                RETURN_IF_ERROR(status_);
                status_ = spilling_stream_->add_blocks(std::move(blocks), false);
                RETURN_IF_ERROR(status_);
                VLOG_ROW << this_ptr << " id: " << id() << " start spill";
                // spilling_stream_->spill();
                return Status::OK();
            });
}

Status SpillSortNode::revoke_memory(RuntimeState* state) {
    // not allow spill in pull stage
    if (sink_eos_) {
        VLOG_ROW << this << " id: " << id() << " spill sort node revoke_memory: sink already eos";
        return Status::OK();
    }
    VLOG_ROW << this << " id: " << id() << " spill sort node revoke_memory";
    RETURN_IF_ERROR(_revoke_memory_internal());
    return Status::WaitForIO("Spilling");
}

bool SpillSortNode::io_task_finished() {
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
                if (status_.ok()) {
                    status_ = _merge_sort_spill_streams();
                }
                return false;
            }
            VLOG_ROW << this << " io_task_finished, spill finished";
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
                         << " io_task_finished merge spilled blocks finished, status: " << status_;
                return true;
            } else {
                VLOG_ROW << this << " id: " << id()
                         << " io_task_finished merge spilled blocks not finished";
                return false;
            }
        } else if (sorted_streams_.size() == 1) {
            VLOG_ROW << this << " id: " << id() << " io_task_finished, one stream";
            return !sorted_streams_[0]->is_reading();
        } else {
            VLOG_ROW << this << " id: " << id() << " io_task_finished not merging spilled blocks";
            return true;
        }
    } else {
        return true;
    }
}

Status SpillSortNode::_create_intermediate_merger(int num_blocks,
                                                  const SortDescription& sort_description) {
    std::vector<BlockSupplier> child_block_suppliers;
    merger_ = std::make_unique<VSortedRunMerger>(sort_description, spill_block_batch_size_, _limit,
                                                 _offset, runtime_profile());

    current_merging_streams_.clear();
    for (int i = 0; i < num_blocks && !sorted_streams_.empty(); ++i) {
        auto stream = sorted_streams_.front();
        current_merging_streams_.emplace_back(stream);
        child_block_suppliers.emplace_back(
                std::bind(std::mem_fn(&SpillStream::read_current_block_sync), stream.get(),
                          std::placeholders::_1, std::placeholders::_2));

        sorted_streams_.pop_front();
    }
    RETURN_IF_ERROR(merger_->prepare(child_block_suppliers));
    return Status::OK();
}

Status SpillSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) {
    RETURN_IF_ERROR(status_);
    if (!io_task_finished()) {
        return Status::WaitForIO("merging spilled blocks");
    }
    if (merger_) {
        VLOG_ROW << this << " spill sort pull, merge get next";
        return merger_->get_next(output_block, eos);
    }
    if (sorted_streams_.size() == 1) {
        VLOG_ROW << this << " spill sort pull, one stream";
        return sorted_streams_[0]->get_next(output_block, eos);
    }
    DCHECK(sorted_streams_.empty());
    VLOG_ROW << this << " spill sort pull, not spilled";
    auto status = in_memory_sort_node_->pull(state, output_block, eos);
    VLOG_ROW << "pull merger block: " << output_block->dump_data();
    reached_limit(output_block, eos);
    return status;
}
} // namespace vectorized
} // namespace doris