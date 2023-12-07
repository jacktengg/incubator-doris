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

#include <assert.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/global_types.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector_helper.h"
#include "vec/columns/columns_number.h"
#include "vec/common/allocator.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_map_context.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/hash_table/hash_map_util.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/common/hash_table/ph_hash_map.h"
#include "vec/common/hash_table/string_hash_map.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/block.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vectorized_agg_fn.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {
class TPlanNode;
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TupleDescriptor;

namespace pipeline {} // namespace pipeline

namespace vectorized {
class AggregationNode;
using AggregationNodeUPtr = std::unique_ptr<AggregationNode>;

class PartitionedAggregationNode : public ::doris::ExecNode {
public:
    PartitionedAggregationNode(ObjectPool* pool, const TPlanNode& tnode,
                               const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status alloc_resource(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status pull(doris::RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;

private:
    AggregationNodeUPtr in_memory_agg_node_;
};
} // namespace vectorized
} // namespace doris