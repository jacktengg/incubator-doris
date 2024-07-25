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

#include "analytic_sink_operator.h"
#include "operator.h"
#include "pipeline/dependency.h"

namespace doris::pipeline {
class SpillAnalyticSinkOperatorX;
class SpillAnalyticSinkLocalState : public PipelineXSpillSinkLocalState<SpillAnalyticSharedState> {
    ENABLE_FACTORY_CREATOR(SpillAnalyticSinkLocalState);

public:
    using Base = PipelineXSpillSinkLocalState<SpillAnalyticSharedState>;
    using Parent = SpillAnalyticSinkOperatorX;
    SpillAnalyticSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSpillSinkLocalState<SpillAnalyticSharedState>(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;
    Status close(RuntimeState* state, Status exec_status) override;
    Status revoke_memory(RuntimeState* state);

private:
    friend class SpillAnalyticSinkOperatorX;

    Status _setup_in_memory_sort_op(RuntimeState* state);

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;
    bool _eos = false;
};
class SpillAnalyticSinkOperatorX final : public DataSinkOperatorX<SpillAnalyticSinkLocalState> {
public:
    using LocalStateType = SpillAnalyticSinkLocalState;
    SpillAnalyticSinkOperatorX(ObjectPool* pool, int operator_id, const TPlanNode& tnode,
                               const DescriptorTbl& descs, bool require_bucket_distribution);
    Status init(const TDataSink& tsink) override {
        return Status::InternalError("{} should not init with TPlanNode",
                                     DataSinkOperatorX<SpillAnalyticSinkLocalState>::_name);
    }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;
    DataDistribution required_data_distribution() const override {
        /*
        if (_partition_by_eq_expr_ctxs.empty()) {
            return {ExchangeType::PASSTHROUGH};
        } else if (_order_by_eq_expr_ctxs.empty()) {
            return _is_colocate && _require_bucket_distribution
                           ? DataDistribution(ExchangeType::BUCKET_HASH_SHUFFLE, _partition_exprs)
                           : DataDistribution(ExchangeType::HASH_SHUFFLE, _partition_exprs);
        }
        */
        return DataSinkOperatorX<SpillAnalyticSinkLocalState>::required_data_distribution();
    }

    bool require_data_distribution() const override { return true; }

    Status set_child(OperatorXPtr child) override {
        RETURN_IF_ERROR(DataSinkOperatorX<SpillAnalyticSinkLocalState>::set_child(child));
        return _analytic_sink_operator->set_child(child);
    }

    size_t revocable_mem_size(RuntimeState* state) const override;

    Status revoke_memory(RuntimeState* state) override;

private:
    friend class SpillAnalyticSinkLocalState;
    std::unique_ptr<AnalyticSinkOperatorX> _analytic_sink_operator;
};
} // namespace doris::pipeline