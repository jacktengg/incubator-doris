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

#include "common/status.h"
#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/operator.h"
#include "vec/exec/partitioned_aggregation_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class PartitionedAggSourceOperatorBuilder final
        : public OperatorBuilder<vectorized::PartitionedAggregationNode> {
public:
    PartitionedAggSourceOperatorBuilder(int32_t, ExecNode*);

    bool is_source() const override { return true; }

    OperatorPtr build_operator() override;
};

class PartitionedAggSourceOperator final
        : public SourceOperator<vectorized::PartitionedAggregationNode> {
public:
    PartitionedAggSourceOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
    bool io_task_finished() override { return _node->io_task_finished(); }
};

class PartitionedAggSourceDependency final : public Dependency {
public:
    using SharedState = PartitionedAggSharedState;
    PartitionedAggSourceDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "PartitionedAggSourceDependency", query_ctx) {}
    ~PartitionedAggSourceDependency() override = default;

    void block() override {
        // if (_is_streaming_agg_state()) {
        Dependency::block();
        // }
    }

private:
};

class AggLocalState;
class PartitionedAggSourceOperatorX;
class PartitionedAggLocalState final : public PipelineXLocalState<PartitionedAggSourceDependency> {
public:
    using Base = PipelineXLocalState<PartitionedAggSourceDependency>;
    ENABLE_FACTORY_CREATOR(PartitionedAggLocalState);
    PartitionedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~PartitionedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

protected:
    friend class PartitionedAggSourceOperatorX;
    std::unique_ptr<AggLocalState> agg_local_state_;
};
class AggSourceOperatorX;
class PartitionedAggSourceOperatorX : public OperatorX<PartitionedAggLocalState> {
public:
    using Base = OperatorX<PartitionedAggLocalState>;
    PartitionedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                  const DescriptorTbl& descs, bool is_streaming = false);
    ~PartitionedAggSourceOperatorX() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    bool is_source() const override { return true; }

private:
    Status _initiate_merge_spill_partition_agg_data(RuntimeState* state);

    Status status_;
    std::unique_ptr<AggSourceOperatorX> agg_source_op_x_;
    std::unique_ptr<std::promise<Status>> spill_merge_promise_;
    std::future<Status> spill_merge_future_;
    bool current_partition_eos_ = true;
};
} // namespace pipeline
} // namespace doris
