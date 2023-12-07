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

#pragma once

#include <stdint.h>

#include <memory>

#include "aggregation_sink_operator.h"
#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "vec/exec/partitioned_aggregation_node.h"
#include "vec/spill/spill_stream.h"

namespace doris {
class ExecNode;

namespace pipeline {

class PartitionedAggSinkOperatorBuilder final
        : public OperatorBuilder<vectorized::PartitionedAggregationNode> {
public:
    PartitionedAggSinkOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
    bool is_sink() const override { return true; }
};

class PartitionedAggSinkOperator final
        : public StreamingOperator<vectorized::PartitionedAggregationNode> {
public:
    PartitionedAggSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* node);
    bool can_write() override { return true; }

    bool io_task_finished() override { return _node->io_task_finished(); }
};
class PartitionedAggSinkDependency final : public Dependency {
public:
    using SharedState = PartitionedAggSharedState;
    PartitionedAggSinkDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "PartitionedAggSinkDependency", true, query_ctx) {}
    ~PartitionedAggSinkDependency() override = default;
};

template <typename LocalStateType>
class PartitionedAggSinkOperatorX;

class BlockingAggSinkLocalState;
class PartitionedBlockingAggSinkLocalState
        : public PipelineXSinkLocalState<PartitionedAggSinkDependency> {
public:
    ENABLE_FACTORY_CREATOR(PartitionedBlockingAggSinkLocalState);
    using Base = PipelineXSinkLocalState<PartitionedAggSinkDependency>;
    using Parent = PartitionedAggSinkOperatorX<PartitionedBlockingAggSinkLocalState>;

    PartitionedBlockingAggSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state);
    ~PartitionedBlockingAggSinkLocalState() override = default;

    template <typename LocalStateType>
    friend class PartitionedAggSinkOperatorX;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

private:
    std::unique_ptr<BlockingAggSinkLocalState> agg_sink_local_state_;
};

template <typename LocalStateType = PartitionedBlockingAggSinkLocalState>
class PartitionedAggSinkOperatorX : public DataSinkOperatorX<LocalStateType> {
public:
    PartitionedAggSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                                const DescriptorTbl& descs, bool is_streaming = false);
    ~PartitionedAggSinkOperatorX() override = default;
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<LocalStateType>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override;

    DataDistribution required_data_distribution() const override {
        return {ExchangeType::BUCKET_HASH_SHUFFLE};
    }

    Status set_child(OperatorXPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<LocalStateType>::set_child(child));
        return in_memory_agg_operator_->set_child(child);
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

    using DataSinkOperatorX<LocalStateType>::id;
    using DataSinkOperatorX<LocalStateType>::operator_id;
    using DataSinkOperatorX<LocalStateType>::get_local_state;

private:
    size_t _get_partition_index(size_t hash_value) const {
        return (hash_value >> (32 - spill_partition_count_bits_)) & max_spill_partition_index_;
    }

    Status _prepare_for_reading(RuntimeState* state);

    Status _revoke_memory_internal(RuntimeState* state);

    Status _initiate_merge_spill_partition_agg_data(RuntimeState* state);

    Status _release_in_memory_agg_data(RuntimeState* state, bool& has_agg_data);

    void release_spill_streams();

    friend class PartitionedBlockingAggSinkLocalState;
    std::unique_ptr<AggSinkOperatorX<>> in_memory_agg_operator_;
    bool sink_eos_ = false;
    Status status_;

    size_t spill_partition_count_bits_ = 4;
    size_t spill_partition_count_;
    size_t max_spill_partition_index_;

    // RuntimeProfile* runtime_profile = nullptr;
    // std::vector<vectorized::SpillStreamSPtr> spilled_streams_;
    vectorized::SpillStreamSPtr spilling_stream_;

    size_t read_cursor_ {};
};

} // namespace pipeline
} // namespace doris
