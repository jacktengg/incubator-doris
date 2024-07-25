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

#include "operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {

class AnalyticSourceOperatorX;
class SpillAnalyticSourceOperatorX;
class SpillAnalyticLocalState final : public PipelineXLocalState<SpillAnalyticSharedState> {
public:
    ENABLE_FACTORY_CREATOR(SpillAnalyticLocalState);
    SpillAnalyticLocalState(RuntimeState* state, OperatorXBase* parent);

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

private:
    friend class SpillAnalyticSourceOperatorX;
    Status _setup_in_memory_op(RuntimeState* state);

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<RuntimeProfile> _internal_runtime_profile;

    int64_t _output_block_index;
    int64_t _window_end_position;
    bool _next_partition;
    std::vector<vectorized::MutableColumnPtr> _result_window_columns;

    int64_t _rows_start_offset;
    int64_t _rows_end_offset;
    vectorized::AggregateDataPtr _fn_place_ptr;
    size_t _agg_functions_size;
    bool _agg_functions_created;
    bool _current_window_empty = false;

    BlockRowPos _order_by_start;
    BlockRowPos _order_by_end;
    BlockRowPos _partition_by_start;
    std::unique_ptr<vectorized::Arena> _agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    RuntimeProfile::Counter* _evaluation_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _blocks_memory_usage = nullptr;

    using vectorized_execute = std::function<void(int64_t peer_group_start, int64_t peer_group_end,
                                                  int64_t frame_start, int64_t frame_end)>;
    using vectorized_get_next = std::function<Status(size_t rows)>;
    using vectorized_get_result = std::function<void(int64_t current_block_rows)>;
};

class SpillAnalyticSourceOperatorX final : public OperatorX<SpillAnalyticLocalState> {
public:
    SpillAnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                 const DescriptorTbl& descs);

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

private:
    friend class SpillAnalyticLocalState;

    std::unique_ptr<AnalyticSourceOperatorX> _analytic_source_operator;

    TAnalyticWindow _window;

    TupleId _intermediate_tuple_id;
    TupleId _output_tuple_id;

    bool _has_window;
    bool _has_range_window;
    bool _has_window_start;
    bool _has_window_end;

    std::vector<vectorized::AggFnEvaluator*> _agg_functions;

    TupleDescriptor* _intermediate_tuple_desc = nullptr;
    TupleDescriptor* _output_tuple_desc = nullptr;

    /// The offset of the n-th functions.
    std::vector<size_t> _offsets_of_aggregate_states;
    /// The total size of the row from the functions.
    size_t _total_size_of_aggregate_states = 0;
    /// The max align size for functions
    size_t _align_aggregate_states = 1;

    std::vector<bool> _change_to_nullable_flags;
};

} // namespace pipeline
} // namespace doris
