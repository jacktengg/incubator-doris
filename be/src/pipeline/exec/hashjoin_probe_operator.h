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

#include "common/status.h"
#include "operator.h"
#include "vec/exec/join/grace_hash_join_node.h"
#include "vec/exec/join/vhash_join_node.h"

namespace doris {
class ExecNode;
class RuntimeState;

namespace pipeline {

class HashJoinProbeOperatorBuilder final : public OperatorBuilder<vectorized::HashJoinNode> {
public:
    HashJoinProbeOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
};

class HashJoinProbeOperator final : public StatefulOperator<HashJoinProbeOperatorBuilder> {
public:
    HashJoinProbeOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
};

class GraceHashJoinProbeOperatorBuilder final
        : public OperatorBuilder<vectorized::GraceHashJoinNode> {
public:
    GraceHashJoinProbeOperatorBuilder(int32_t, ExecNode*);

    OperatorPtr build_operator() override;
};

class GraceHashJoinProbeOperator final
        : public StatefulOperator<GraceHashJoinProbeOperatorBuilder> {
public:
    GraceHashJoinProbeOperator(OperatorBuilderBase*, ExecNode*);
    // if exec node split to: sink, source operator. the source operator
    // should skip `alloc_resource()` function call, only sink operator
    // call the function
    Status open(RuntimeState*) override { return Status::OK(); }
    bool io_task_finished() override { return _node->io_task_finished(); }
};

} // namespace pipeline
} // namespace doris