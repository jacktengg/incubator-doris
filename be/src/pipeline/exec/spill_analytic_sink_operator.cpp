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

#include "spill_analytic_sink_operator.h"

#include <memory>
#include <string>

#include "pipeline/exec/operator.h"
#include "vec/exprs/vectorized_agg_fn.h"

namespace doris::pipeline {

Status SpillAnalyticSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(PipelineXSinkLocalState<SpillAnalyticSharedState>::init(state, info));
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);

    _internal_runtime_profile = std::make_unique<RuntimeProfile>("internal_profile");

    RETURN_IF_ERROR(_setup_in_memory_sort_op(state));

    return Status::OK();
}

Status SpillAnalyticSinkLocalState::_setup_in_memory_sort_op(RuntimeState* state) {
    _runtime_state = RuntimeState::create_unique(
            nullptr, state->fragment_instance_id(), state->query_id(), state->fragment_id(),
            state->query_options(), TQueryGlobals {}, state->exec_env(), state->get_query_ctx());
    _runtime_state->set_task_execution_context(state->get_task_execution_context().lock());
    _runtime_state->set_be_number(state->be_number());

    _runtime_state->set_desc_tbl(&state->desc_tbl());
    _runtime_state->set_pipeline_x_runtime_filter_mgr(state->local_runtime_filter_mgr());

    auto& parent = Base::_parent->template cast<Parent>();
    Base::_shared_state->in_mem_shared_state_sptr =
            parent._analytic_sink_operator->create_shared_state();
    Base::_shared_state->in_mem_shared_state =
            static_cast<AnalyticSharedState*>(Base::_shared_state->in_mem_shared_state_sptr.get());
    LocalSinkStateInfo info {0,  _internal_runtime_profile.get(),
                             -1, Base::_shared_state->in_mem_shared_state,
                             {}, {}};
    RETURN_IF_ERROR(parent._analytic_sink_operator->setup_local_state(_runtime_state.get(), info));
    auto* sink_local_state = _runtime_state->get_sink_local_state();
    DCHECK(sink_local_state != nullptr);

    RETURN_IF_ERROR(sink_local_state->open(state));

    return Status::OK();
}

Status SpillAnalyticSinkLocalState::close(RuntimeState* state, Status execsink_status) {
    dec_running_big_mem_op_num(state);
    return Status::OK();
}

Status SpillAnalyticSinkLocalState::revoke_memory(RuntimeState* state) {
    return Status::OK();
}

SpillAnalyticSinkOperatorX::SpillAnalyticSinkOperatorX(ObjectPool* pool, int operator_id,
                                                       const TPlanNode& tnode,
                                                       const DescriptorTbl& descs,
                                                       bool require_bucket_distribution)
        : DataSinkOperatorX(operator_id, tnode.node_id) {
    _analytic_sink_operator = std::make_unique<AnalyticSinkOperatorX>(
            pool, operator_id, tnode, descs, require_bucket_distribution);
}

Status SpillAnalyticSinkOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(DataSinkOperatorX::init(tnode, state));
    _name = "SPILL_ANALYTIC_SINK_OPERATOR";

    _analytic_sink_operator->set_dests_id(DataSinkOperatorX<LocalStateType>::dests_id());
    RETURN_IF_ERROR(
            _analytic_sink_operator->set_child(DataSinkOperatorX<LocalStateType>::_child_x));
    return _analytic_sink_operator->init(tnode, state);
}

Status SpillAnalyticSinkOperatorX::prepare(RuntimeState* state) {
    return _analytic_sink_operator->prepare(state);
}

Status SpillAnalyticSinkOperatorX::open(RuntimeState* state) {
    return _analytic_sink_operator->open(state);
}

Status SpillAnalyticSinkOperatorX::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)input_block->rows());

    local_state._eos = eos;
    DBUG_EXECUTE_IF("fault_inject::spill_analytic_sink::sink",
                    { return Status::InternalError("fault_inject spill_sort_sink sink failed"); });
    if (eos && input_block->rows() == 0) {
        local_state._dependency->set_ready_to_read();
        local_state._dependency->block();
        return Status::OK();
    }
    RETURN_IF_ERROR(
            _analytic_sink_operator->sink(local_state._runtime_state.get(), input_block, false));
    if (_analytic_sink_operator->need_more_input(local_state._runtime_state.get())) {
        local_state._dependency->set_block_to_read();
        local_state._dependency->set_ready();
    } else {
        local_state._dependency->block();
        local_state._dependency->set_ready_to_read();
    }

    return Status::OK();
}

Status SpillAnalyticSinkOperatorX::revoke_memory(RuntimeState* state) {
    auto& local_state = get_local_state(state);
    return local_state.revoke_memory(state);
}

size_t SpillAnalyticSinkOperatorX::revocable_mem_size(RuntimeState* state) const {
    auto& local_state = get_local_state(state);
    if (!local_state.Base::_shared_state->sink_status.ok()) {
        return UINT64_MAX;
    }
    return _analytic_sink_operator->get_revocable_mem_size(local_state._runtime_state.get());
}
template class DataSinkOperatorX<SpillAnalyticSinkLocalState>;
} // namespace doris::pipeline