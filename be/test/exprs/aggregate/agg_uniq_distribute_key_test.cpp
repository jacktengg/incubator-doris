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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function_simple_factory.h"
#include "testutil/column_helper.h"

namespace doris {

// The function is registered under "multi_distinct_count_distribute_key" while
// get_name() returns "multi_distinct_distribute_key". This is the
// distribute-key optimized count-distinct: add() fills a hash set, serialize
// writes set.size(), deserialize reads that size into `count`, and merge sums
// per-partition counts. Tests therefore drive the full
// add -> serialize -> deserialize -> merge -> insert_result_into pipeline.
class AggUniqDistributeKeyTest : public testing::Test {
protected:
    AggregateFunctionPtr create_fn(const DataTypePtr& arg_type) {
        DataTypes arg_types {arg_type};
        return AggregateFunctionSimpleFactory::instance().get(
                "multi_distinct_count_distribute_key", arg_types, std::make_shared<DataTypeInt64>(),
                /*result_is_nullable=*/false, BeExecVersionManager::get_newest_version());
    }

    AggregateDataPtr alloc_place(const AggregateFunctionPtr& fn, Arena& arena) {
        auto* place = arena.aligned_alloc(fn->size_of_data(), fn->align_of_data());
        fn->create(place);
        return place;
    }

    int64_t result_of(const AggregateFunctionPtr& fn, ConstAggregateDataPtr place) {
        auto result = ColumnInt64::create();
        fn->insert_result_into(place, *result);
        EXPECT_EQ(result->size(), 1);
        return result->get_data()[0];
    }

    // serialize() writes set.size() into a ColumnString; deserialize() reads it
    // back into `count`. Returns the round-tripped count.
    int64_t serialize_deserialize_count(const AggregateFunctionPtr& fn, ConstAggregateDataPtr src,
                                        Arena& arena) {
        ColumnString serialize_col;
        {
            BufferWritable buf(serialize_col);
            fn->serialize(src, buf);
            buf.commit();
        }
        auto* dst = alloc_place(fn, arena);
        {
            StringRef ref = serialize_col.get_data_at(0);
            BufferReadable reader(ref);
            fn->deserialize(dst, reader, arena);
        }
        int64_t count = result_of(fn, dst);
        fn->destroy(dst);
        return count;
    }
};

// Numeric (Int32) path: add() one row at a time, then serialize/deserialize
// round-trip and reset(). Covers add, serialize, deserialize,
// insert_result_into, reset, get_name, get_return_type, get_serialized_type,
// create_serialize_column.
TEST_F(AggUniqDistributeKeyTest, NumericInt32AddSerializeDeserializeReset) {
    auto fn = create_fn(std::make_shared<DataTypeInt32>());
    ASSERT_NE(fn, nullptr);

    EXPECT_EQ(fn->get_name(), "multi_distinct_distribute_key");
    EXPECT_EQ(fn->get_return_type()->get_primitive_type(), TYPE_BIGINT);
    ASSERT_NE(fn->get_serialized_type(), nullptr);

    auto serialize_col = fn->create_serialize_column();
    EXPECT_EQ(assert_cast<ColumnFixedLengthObject&>(*serialize_col).item_size(), sizeof(UInt64));

    Arena arena;
    ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({1, 1, 2, 3, 3, 3, 5});
    const IColumn* columns[1] = {col.get()};

    auto* place = alloc_place(fn, arena);
    for (ssize_t i = 0; i < static_cast<ssize_t>(col->size()); ++i) {
        fn->add(place, columns, i, arena);
    }

    // Distinct values {1, 2, 3, 5} -> 4.
    EXPECT_EQ(serialize_deserialize_count(fn, place, arena), 4);

    // reset() clears the set, so the round-tripped count becomes 0.
    fn->reset(place);
    EXPECT_EQ(serialize_deserialize_count(fn, place, arena), 0);

    fn->destroy(place);
}

// Numeric (Int64) path through add_batch_single_place (batch > prefetch dist),
// serialize_without_key_to_column, and deserialize_and_merge_from_column_range.
TEST_F(AggUniqDistributeKeyTest, NumericInt64AddBatchSinglePlace) {
    auto fn = create_fn(std::make_shared<DataTypeInt64>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    // 40 rows (> HASH_MAP_PREFETCH_DIST) of values 0..19 repeated -> distinct 20.
    std::vector<int64_t> vals;
    for (int rep = 0; rep < 2; ++rep) {
        for (int64_t v = 0; v < 20; ++v) {
            vals.push_back(v);
        }
    }
    ColumnPtr col = ColumnHelper::create_column<DataTypeInt64>(vals);
    const IColumn* columns[1] = {col.get()};

    auto* place = alloc_place(fn, arena);
    fn->add_batch_single_place(col->size(), place, columns, arena);

    auto serialize_col = fn->create_serialize_column();
    fn->serialize_without_key_to_column(place, *serialize_col);
    auto& fixed = assert_cast<ColumnFixedLengthObject&>(*serialize_col);
    ASSERT_EQ(fixed.size(), 1);
    EXPECT_EQ(reinterpret_cast<const UInt64*>(fixed.get_data().data())[0], 20);

    auto* merged = alloc_place(fn, arena);
    fn->deserialize_and_merge_from_column_range(merged, *serialize_col, 0, 0, arena);
    EXPECT_EQ(result_of(fn, merged), 20);

    fn->destroy(place);
    fn->destroy(merged);
}

// add_batch distributing rows to two places (batch > prefetch dist), then
// serialize_to_column and deserialize_and_merge_vec sum the per-place distinct
// counts. This mirrors the distribute-key two-phase aggregation.
TEST_F(AggUniqDistributeKeyTest, AddBatchMultiPlaceSerializeAndMergeVec) {
    auto fn = create_fn(std::make_shared<DataTypeInt64>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    // Rows 0..19 -> place0 with values 0..19 (distinct 20).
    // Rows 20..39 -> place1 with values 0..4 repeated (distinct 5).
    std::vector<int64_t> vals(40);
    for (int i = 0; i < 20; ++i) {
        vals[i] = i;
    }
    for (int i = 20; i < 40; ++i) {
        vals[i] = (i - 20) % 5;
    }
    ColumnPtr col = ColumnHelper::create_column<DataTypeInt64>(vals);
    const IColumn* columns[1] = {col.get()};

    auto* place0 = alloc_place(fn, arena);
    auto* place1 = alloc_place(fn, arena);
    std::vector<AggregateDataPtr> places(40);
    for (int i = 0; i < 40; ++i) {
        places[i] = (i < 20) ? place0 : place1;
    }
    fn->add_batch(40, places.data(), 0, columns, arena, false);

    std::vector<AggregateDataPtr> place_vec {place0, place1};
    auto serialize_col = fn->create_serialize_column();
    fn->serialize_to_column(place_vec, 0, serialize_col, 2);
    auto& fixed = assert_cast<ColumnFixedLengthObject&>(*serialize_col);
    ASSERT_EQ(fixed.size(), 2);
    const auto* sizes = reinterpret_cast<const UInt64*>(fixed.get_data().data());
    EXPECT_EQ(sizes[0], 20);
    EXPECT_EQ(sizes[1], 5);

    auto* out0 = alloc_place(fn, arena);
    auto* out1 = alloc_place(fn, arena);
    std::vector<AggregateDataPtr> out_places {out0, out1};
    auto* rhs = arena.aligned_alloc(2 * fn->size_of_data(), fn->align_of_data());
    fn->deserialize_and_merge_vec(out_places.data(), 0, rhs, serialize_col.get(), arena, 2);
    EXPECT_EQ(result_of(fn, out0), 20);
    EXPECT_EQ(result_of(fn, out1), 5);

    fn->destroy(place0);
    fn->destroy(place1);
    fn->destroy(out0);
    fn->destroy(out1);
}

// deserialize_and_merge_vec_selected: only places that are non-null get merged.
TEST_F(AggUniqDistributeKeyTest, DeserializeAndMergeVecSelected) {
    auto fn = create_fn(std::make_shared<DataTypeInt64>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    auto serialize_col = ColumnFixedLengthObject::create(sizeof(UInt64));
    serialize_col->resize(2);
    auto* data = reinterpret_cast<UInt64*>(serialize_col->get_data().data());
    data[0] = 3;
    data[1] = 7;

    auto* out0 = alloc_place(fn, arena);
    std::vector<AggregateDataPtr> out_places {out0, nullptr};
    auto* rhs = arena.aligned_alloc(2 * fn->size_of_data(), fn->align_of_data());
    fn->deserialize_and_merge_vec_selected(out_places.data(), 0, rhs, serialize_col.get(), arena,
                                           2);

    EXPECT_EQ(result_of(fn, out0), 3);

    fn->destroy(out0);
}

// String key path: get_key (XXH128) is exercised by both get_keys (add_batch
// family) and OneAdder (add). Uses a batch > prefetch dist to cover prefetch.
TEST_F(AggUniqDistributeKeyTest, StringKeyPath) {
    auto fn = create_fn(std::make_shared<DataTypeString>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    const char* base[] = {"a", "bb", "ccc", "dddd", "eeeee"};
    std::vector<std::string> svals;
    for (int i = 0; i < 40; ++i) {
        svals.emplace_back(base[i % 5]);
    }
    ColumnPtr col = ColumnHelper::create_column<DataTypeString>(svals);
    const IColumn* columns[1] = {col.get()};

    // add_batch_single_place -> get_keys string branch + Data::get_key.
    auto* batch_place = alloc_place(fn, arena);
    fn->add_batch_single_place(col->size(), batch_place, columns, arena);
    EXPECT_EQ(serialize_deserialize_count(fn, batch_place, arena), 5);

    // add() -> OneAdder string branch + Data::get_key.
    auto* add_place = alloc_place(fn, arena);
    for (ssize_t i = 0; i < static_cast<ssize_t>(col->size()); ++i) {
        fn->add(add_place, columns, i, arena);
    }
    EXPECT_EQ(serialize_deserialize_count(fn, add_place, arena), 5);

    fn->destroy(batch_place);
    fn->destroy(add_place);
}

// streaming_agg_serialize_to_column writes 1 per input row; summing those via
// deserialize_and_merge_from_column_range yields the row count.
TEST_F(AggUniqDistributeKeyTest, StreamingAggSerialize) {
    auto fn = create_fn(std::make_shared<DataTypeInt32>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    ColumnPtr col = ColumnHelper::create_column<DataTypeInt32>({7, 7, 8});
    const IColumn* columns[1] = {col.get()};

    auto serialize_col = fn->create_serialize_column();
    fn->streaming_agg_serialize_to_column(columns, serialize_col, col->size(), arena);
    auto& fixed = assert_cast<ColumnFixedLengthObject&>(*serialize_col);
    ASSERT_EQ(fixed.size(), 3);
    const auto* data = reinterpret_cast<const UInt64*>(fixed.get_data().data());
    EXPECT_EQ(data[0], 1);
    EXPECT_EQ(data[1], 1);
    EXPECT_EQ(data[2], 1);

    auto* place = alloc_place(fn, arena);
    fn->deserialize_and_merge_from_column_range(place, *serialize_col, 0, 2, arena);
    EXPECT_EQ(result_of(fn, place), 3);

    fn->destroy(place);
}

// merge() accumulates per-partition counts.
TEST_F(AggUniqDistributeKeyTest, MergeAccumulatesCounts) {
    auto fn = create_fn(std::make_shared<DataTypeInt32>());
    ASSERT_NE(fn, nullptr);

    Arena arena;
    auto serialize_col = ColumnFixedLengthObject::create(sizeof(UInt64));
    serialize_col->resize(2);
    auto* data = reinterpret_cast<UInt64*>(serialize_col->get_data().data());
    data[0] = 4;
    data[1] = 6;

    auto* place_a = alloc_place(fn, arena);
    fn->deserialize_and_merge_from_column_range(place_a, *serialize_col, 0, 0, arena);
    auto* place_b = alloc_place(fn, arena);
    fn->deserialize_and_merge_from_column_range(place_b, *serialize_col, 1, 1, arena);

    fn->merge(place_a, place_b, arena);
    EXPECT_EQ(result_of(fn, place_a), 10);

    fn->destroy(place_a);
    fn->destroy(place_b);
}

} // namespace doris
