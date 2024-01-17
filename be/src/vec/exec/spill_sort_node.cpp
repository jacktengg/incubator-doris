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
    io_thread_pool_ = ExecEnv::GetInstance()->spill_io_pool();
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
    in_memory_sort_node_->release_resource(state);
    in_memory_sort_node_.reset();

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
    for (auto& stream : sorted_streams_) {
        stream->close();
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
    /*
    // no spill
    if (sorted_streams_.empty()) {
        LOG(WARNING) << this << " spill sort not spilled";
        _can_read = true;
        return in_memory_sort_node_->prepare_for_read();
    }
    */

    RETURN_IF_ERROR(_release_in_mem_sorted_blocks());

    spill_merge_promise_ = std::make_unique<std::promise<Status>>();
    spill_merge_future_ = spill_merge_promise_->get_future();

    auto status = io_thread_pool_->submit_func([this, state] {
        Defer defer {[&]() {
            _can_read = true;
            spill_merge_promise_->set_value(status_);
        }};
        while (true) {
            int max_stream_count = (sorted_streams_.size() + 1) / 2;
            max_stream_count = std::max(2, max_stream_count);
            max_stream_count = std::min(32, max_stream_count);
            LOG(WARNING) << "spill sort merge intermediate streams, stream count: "
                         << sorted_streams_.size()
                         << ", create merger stream count: " << max_stream_count;
            status_ = _create_intermediate_merger(max_stream_count, sort_description_);
            if (!status_.ok()) {
                LOG(WARNING) << "spill sort merge intermediate streams create merger error: "
                             << status_;
                return;
            }
            // all the remaining streams can be merged in a run
            if (sorted_streams_.empty()) {
                LOG(WARNING) << "spill sort merge intermediate streams, final merge";
                break;
            }

            SpillStreamSPtr stream;
            status_ = ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
                    stream, print_id(state->query_id()), "sort", id(), spill_block_batch_size_,
                    SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile());
            if (!status_.ok()) {
                LOG(WARNING) << "spill sort merge intermediate streams, register stream error: "
                             << status_;
                return;
            }

            bool eos = false;
            while (!eos) {
                merge_sorted_block_.clear();
                status_ = merger_->get_next(&merge_sorted_block_, &eos);
                if (!status_.ok()) {
                    LOG(WARNING) << "spill sort merge intermediate streams, merge get_next: "
                                 << status_;
                    return;
                }
                status_ = stream->add_block(std::move(merge_sorted_block_), false);
                if (!status_.ok()) {
                    LOG(WARNING) << "spill sort merge intermediate streams, add blocks error: "
                                 << status_;
                    return;
                }
            }
            sorted_streams_.emplace_back(stream);
        }
        LOG(WARNING) << "spill sort merge intermediate streams finished";
    });
    RETURN_IF_ERROR(status);
    // return Status::WaitForIO("merging spilled blocks");
    return Status::OK();
}

void SpillSortNode::update_spill_block_batch_size(const Block* block) {
    auto rows = block->rows();
    if (rows > 0 && 0 == avg_row_bytes_) {
        avg_row_bytes_ = std::max((std::size_t)1, block->bytes() / rows);
        spill_block_batch_size_ =
                (SORT_BLOCK_SPILL_BATCH_BYTES + avg_row_bytes_ - 1) / avg_row_bytes_;
    }
}

size_t SpillSortNode::revokable_mem_size() const {
    size_t size = in_memory_sort_node_->revokable_mem_size();
    size += child(0)->revokable_mem_size();
    return size;
}

Status SpillSortNode::_release_in_mem_sorted_blocks() {
    Blocks blocks;
    RETURN_IF_ERROR(
            in_memory_sort_node_->release_sorted_blocks(state_, blocks, spill_block_batch_size_));

    if (sort_description_.empty()) {
        sort_description_ = in_memory_sort_node_->get_sort_description();
    }

    RETURN_IF_ERROR(_prepare_inmemory_sort_node(state_));

    SpillStreamSPtr stream;
    RETURN_IF_ERROR(ExecEnv::GetInstance()->spill_stream_mgr()->register_spill_stream(
            stream, print_id(state_->query_id()), "sort", id(), spill_block_batch_size_,
            SORT_BLOCK_SPILL_BATCH_BYTES, runtime_profile()));
    RETURN_IF_ERROR(stream->add_blocks(std::move(blocks), false));
    sorted_streams_.emplace_back(stream);
    return Status::OK();
}

Status SpillSortNode::revoke_memory() {
    LOG(WARNING) << this << " spill sort node revoke_memory";
    _can_read = false;
    DCHECK(!spilling_stream_);
    RETURN_IF_ERROR(_release_in_mem_sorted_blocks());
    spilling_stream_ = sorted_streams_.back();
    return ExecEnv::GetInstance()->spill_stream_mgr()->spill_stream(spilling_stream_);
}

bool SpillSortNode::io_task_finished() {
    if (sorted_streams_.empty()) {
        LOG(WARNING) << this << "SpillSortNode::io_task_finished, not spilled";
        return true;
    }
    if (spilling_stream_) {
        if (spilling_stream_->is_spilling()) {
            return false;
        } else {
            spilling_stream_.reset();
            if (sink_eos_) {
                _can_read = true;
            }
            LOG(WARNING) << this
                         << "SpillSortNode::io_task_finished, spill finished";
            return true;
        }
    } else if (sink_eos_) {
        if (spill_merge_promise_) {
            auto status = spill_merge_future_.wait_for(std::chrono::milliseconds(10));
            if (status == std::future_status::ready) {
                _can_read = true;
                spill_merge_promise_.reset();
                LOG(WARNING)
                        << this
                        << "SpillSortNode::io_task_finished merge spilled blocks finished, status: "
                        << status_;
                return true;
            } else {
                LOG(WARNING) << this
                             << "SpillSortNode::io_task_finished merge spilled blocks not finished";
                return false;
            }
        } else if (sorted_streams_.size() == 1) {
            LOG(WARNING) << this << " SpillSortNode::io_task_finished, one stream";
            return !sorted_streams_[0]->is_reading();
        } else {
            LOG(WARNING) << this << "SpillSortNode::io_task_finished not merging spilled blocks";
            return true;
        }
    } else {
        return true;
    }
}

Status SpillSortNode::_create_intermediate_merger(int num_blocks,
                                                  const SortDescription& sort_description) {
    std::vector<BlockSupplier> child_block_suppliers;
    merger_.reset(new VSortedRunMerger(sort_description, spill_block_batch_size_, _limit, _offset,
                                       runtime_profile()));

    current_merging_streams_.clear();
    for (int i = 0; i < num_blocks && !sorted_streams_.empty(); ++i) {
        auto stream = sorted_streams_.front();
        current_merging_streams_.emplace_back(stream);
        child_block_suppliers.emplace_back(std::bind(std::mem_fn(&SpillStream::get_next_sync),
                                                     stream.get(), std::placeholders::_1,
                                                     std::placeholders::_2));

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
        LOG(WARNING) << this << " spill sort pull, merge get next";
        return merger_->get_next(output_block, eos);
    }
    if (sorted_streams_.size() == 1) {
        LOG(WARNING) << this << " spill sort pull, one stream";
        return sorted_streams_[0]->get_next(output_block, eos);
    }
    DCHECK(sorted_streams_.empty());
    LOG(WARNING) << this << " spill sort pull, not spilled";
    auto status = in_memory_sort_node_->pull(state, output_block, eos);
    LOG(WARNING) << "pull merger block: " << output_block->dump_data();
    return status;
}
} // namespace vectorized
} // namespace doris