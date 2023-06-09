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

#include "operator.h"
#include "vec/exec/spill_sort_node.h"

namespace doris {
class ExecNode;

namespace pipeline {

class SpillSortSinkOperatorBuilder final : public OperatorBuilder<vectorized::SpillSortNode> {
public:
    SpillSortSinkOperatorBuilder(int32_t id, ExecNode* sort_node);

    bool is_sink() const override { return true; }

    OperatorPtr build_operator() override;
};

class SpillSortSinkOperator final : public StreamingOperator<SpillSortSinkOperatorBuilder> {
public:
    SpillSortSinkOperator(OperatorBuilderBase* operator_builder, ExecNode* sort_node);

    bool can_write() override { return true; }

    bool io_task_finished() override { return _node->io_task_finished(); }
};

} // namespace pipeline
} // namespace doris
