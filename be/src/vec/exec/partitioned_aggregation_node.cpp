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

#include "vec/exec/partitioned_aggregation_node.h"

#include "vec/exec/vaggregation_node.h"
namespace doris::vectorized {
PartitionedAggregationNode::PartitionedAggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

Status PartitionedAggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    return Status::OK();
}
Status PartitionedAggregationNode::prepare(RuntimeState* state) {
    return Status::OK();
}
Status PartitionedAggregationNode::open(RuntimeState* state) {
    return Status::OK();
}
Status PartitionedAggregationNode::alloc_resource(RuntimeState* state) {
    return Status::OK();
}
Status PartitionedAggregationNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    return Status::OK();
}
Status PartitionedAggregationNode::close(RuntimeState* state) {
    return Status::OK();
}
void PartitionedAggregationNode::release_resource(RuntimeState* state) {}
Status PartitionedAggregationNode::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                        bool* eos) {
    return Status::OK();
}

Status PartitionedAggregationNode::sink(doris::RuntimeState* state, vectorized::Block* input_block,
                                        bool eos) {
    return Status::OK();
}
} // namespace doris::vectorized