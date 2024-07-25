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

#include "spill_analytic_source_operator.h"

#include "analytic_source_operator.h"
#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {
SpillAnalyticLocalState::SpillAnalyticLocalState(RuntimeState* state, OperatorXBase* parent)
        : PipelineXLocalState<SpillAnalyticSharedState>(state, parent) {

        }

Status SpillAnalyticLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<SpillAnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");
    return Status::OK();
}

Status SpillAnalyticLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    if (_opened) {
        return Status::OK();
    }
    RETURN_IF_ERROR(setup_in_memory_op(state));
    return PipelineXLocalState<SpillAnalyticSharedState>::open(state);
}
SpillAnalyticSourceOperatorX::SpillAnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                                           int operator_id,
                                                           const DescriptorTbl& descs)
        : OperatorX<SpillAnalyticLocalState>(pool, tnode, operator_id, descs) {
    _analytic_source_operator =
            std::make_unique<AnalyticSourceOperatorX>(pool, tnode, operator_id, descs);
}
Status SpillAnalyticSourceOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<SpillAnalyticLocalState>::init(tnode, state));
    _op_name = "SPILL_ANALYTIC_SOURCE_OPERATOR";
    return _analytic_source_operator->init(tnode, state);
}

Status SpillAnalyticSourceOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<SpillAnalyticLocalState>::prepare(state));
    return _analytic_source_operator->prepare(state);
}
Status SpillAnalyticSourceOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorX<SpillAnalyticLocalState>::open(state));
    return _analytic_source_operator->open(state);
}
} // namespace doris::pipeline