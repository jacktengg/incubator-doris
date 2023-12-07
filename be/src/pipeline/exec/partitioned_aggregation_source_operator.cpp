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

#include "partitioned_aggregation_source_operator.h"

#include <string>

#include "aggregation_source_operator.h"
#include "common/exception.h"
#include "pipeline/exec/operator.h"
#include "vec//utils/util.hpp"
#include "vec/spill/spill_stream_manager.h"

namespace doris::pipeline {

OPERATOR_CODE_GENERATOR(PartitionedAggSourceOperator, SourceOperator)

PartitionedAggLocalState::PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent)
        : Base(state, parent) {
    agg_local_state_ = AggLocalState::create_unique(state, parent);
}

Status PartitionedAggLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    return agg_local_state_->init(state, info);
}
Status PartitionedAggLocalState::close(RuntimeState* state) {
    RETURN_IF_ERROR(agg_local_state_->close(state));
    return Base::close(state);
}
PartitionedAggSourceOperatorX ::PartitionedAggSourceOperatorX(ObjectPool* pool,
                                                              const TPlanNode& tnode,
                                                              int operator_id,
                                                              const DescriptorTbl& descs,
                                                              bool is_streaming)
        : Base(pool, tnode, operator_id, descs) {
    agg_source_op_x_ =
            std::make_unique<AggSourceOperatorX>(pool, tnode, operator_id, descs, is_streaming);
}

Status PartitionedAggSourceOperatorX ::get_block(RuntimeState* state, vectorized::Block* block,
                                                 SourceState& source_state) {
    RETURN_IF_ERROR(status_);
    if (current_partition_eos_) {
        return _initiate_merge_spill_partition_agg_data(state);
    }
    RETURN_IF_ERROR(agg_source_op_x_->get_block(state, block, source_state));
    if (SourceState::FINISHED == source_state) {
        auto& local_state = get_local_state(state);
        if (local_state._shared_state->read_cursor_ !=
            local_state._shared_state->partition_count_) {
            RETURN_IF_ERROR(_initiate_merge_spill_partition_agg_data(state));
            source_state = SourceState::DEPEND_ON_SOURCE;
        }
    }
    return Status::OK();
}

Status PartitionedAggSourceOperatorX ::_initiate_merge_spill_partition_agg_data(
        RuntimeState* state) {
    DCHECK(!spill_merge_promise_);
    spill_merge_promise_ = std::make_unique<std::promise<Status>>();
    spill_merge_future_ = spill_merge_promise_->get_future();

    auto& local_state = get_local_state(state);
    RETURN_IF_ERROR(local_state._shared_state->prepare_merge_partition_aggregation_data());

    local_state._dependency->block();

    return ExecEnv::GetInstance()->spill_stream_mgr()->get_async_task_thread_pool()->submit_func(
            [this, &local_state] {
                Defer defer {[&]() {
                    if (!status_.ok()) {
                        LOG(WARNING) << "merge spill agg data failed: " << status_;
                    }
                    spill_merge_promise_->set_value(status_);
                    current_partition_eos_ = false;
                    local_state._dependency->set_ready();
                }};
                bool has_agg_data = false;
                while (/*!_is_resource_released && */ !has_agg_data &&
                       local_state._shared_state->read_cursor_ <
                               local_state._shared_state->partition_count_) {
                    // merge aggregation data of a spilled partition
                    VLOG_ROW << this << " id: " << id() << " read spilled partition "
                             << local_state._shared_state->read_cursor_ << ", stream count: "
                             << local_state._shared_state->spilled_streams_.size();
                    // optmize: initiate read for many streams and wait for read finish
                    // need to consider memory pressure
                    for (auto& stream : local_state._shared_state->spilled_streams_) {
                        status_ = stream->seek_for_read(local_state._shared_state->read_cursor_);
                        RETURN_IF_ERROR(status_);
                        vectorized::Block block;
                        bool eos;
                        status_ = stream->read_current_block_sync(&block, &eos);
                        RETURN_IF_ERROR(status_);

                        if (!block.empty()) {
                            has_agg_data = true;
                            status_ = local_state._shared_state
                                              ->merge_spilt_partition_aggregation_data(&block);
                            RETURN_IF_ERROR(status_);
                        }
                    }
                    local_state._shared_state->read_cursor_++;
                }
                if (local_state._shared_state->read_cursor_ ==
                    local_state._shared_state->partition_count_) {
                    // release_spill_streams();
                }
                // status_ = agg_source_op_x_->prepare_pull();
                VLOG_ROW << this << " id: " << id()
                         << " merge spilled streams finished, next read_cursor: "
                         << local_state._shared_state->read_cursor_;
                return status_;
            });
}

} // namespace doris::pipeline
