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

#include <memory>

#include "common/exception.h"
#include "core/arena.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/string_buffer.hpp"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function_product.h"
#include "util/defer_op.h"

namespace doris {

template <PrimitiveType T, PrimitiveType TResult>
using ProductFunction = AggregateFunctionProduct<T, TResult, AggregateFunctionProductData<TResult>>;

class AggregateFunctionProductTest : public testing::Test {};

// Covers: get_name(), get_return_type() non-decimal branch, add() integral path,
// reset() non-decimal branch and insert_result_into().
TEST_F(AggregateFunctionProductTest, test_bigint_product) {
    DataTypes argument_types = {std::make_shared<DataTypeInt64>()};
    auto fn = std::make_shared<ProductFunction<TYPE_BIGINT, TYPE_BIGINT>>(argument_types);

    EXPECT_EQ(fn->get_name(), "product");
    auto return_type = fn->get_return_type();
    EXPECT_EQ(return_type->get_primitive_type(), TYPE_BIGINT);

    auto column = ColumnInt64::create();
    column->insert_value(2);
    column->insert_value(3);
    column->insert_value(4);
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory(new char[fn->size_of_data()]);
    AggregateDataPtr place = memory.get();
    fn->create(place);
    Defer defer([&]() { fn->destroy(place); });

    fn->reset(place);
    for (size_t i = 0; i < column->size(); i++) {
        fn->add(place, columns, i, arena);
    }

    auto result = ColumnInt64::create();
    fn->insert_result_into(place, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->get_element(0), 24);
}

// Covers: add_impl() integral overflow branch (throw Exception).
TEST_F(AggregateFunctionProductTest, test_int_overflow) {
    DataTypes argument_types = {std::make_shared<DataTypeInt32>()};
    auto fn = std::make_shared<ProductFunction<TYPE_INT, TYPE_INT>>(argument_types);

    auto column = ColumnInt32::create();
    column->insert_value(100000);
    column->insert_value(100000);
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory(new char[fn->size_of_data()]);
    AggregateDataPtr place = memory.get();
    fn->create(place);
    Defer defer([&]() { fn->destroy(place); });

    fn->reset(place);
    fn->add(place, columns, 0, arena);
    EXPECT_THROW(fn->add(place, columns, 1, arena), doris::Exception);
}

// Covers: generic merge() and the main class merge().
TEST_F(AggregateFunctionProductTest, test_bigint_merge) {
    DataTypes argument_types = {std::make_shared<DataTypeInt64>()};
    auto fn = std::make_shared<ProductFunction<TYPE_BIGINT, TYPE_BIGINT>>(argument_types);

    auto column = ColumnInt64::create();
    column->insert_value(2);
    column->insert_value(3);
    column->insert_value(4);
    column->insert_value(5);
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory1(new char[fn->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[fn->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();
    fn->create(place1);
    fn->create(place2);
    Defer defer([&]() {
        fn->destroy(place1);
        fn->destroy(place2);
    });

    fn->reset(place1);
    fn->add(place1, columns, 0, arena);
    fn->add(place1, columns, 1, arena);

    fn->reset(place2);
    fn->add(place2, columns, 2, arena);
    fn->add(place2, columns, 3, arena);

    fn->merge(place1, place2, arena);

    auto result = ColumnInt64::create();
    fn->insert_result_into(place1, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->get_element(0), 120);
}

// Covers: generic write()/read() and the main class serialize()/deserialize().
TEST_F(AggregateFunctionProductTest, test_bigint_serialize_deserialize) {
    DataTypes argument_types = {std::make_shared<DataTypeInt64>()};
    auto fn = std::make_shared<ProductFunction<TYPE_BIGINT, TYPE_BIGINT>>(argument_types);

    auto column = ColumnInt64::create();
    column->insert_value(2);
    column->insert_value(3);
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory1(new char[fn->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[fn->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();
    fn->create(place1);
    fn->create(place2);
    Defer defer([&]() {
        fn->destroy(place1);
        fn->destroy(place2);
    });

    fn->reset(place1);
    fn->add(place1, columns, 0, arena);
    fn->add(place1, columns, 1, arena);

    ColumnString buffer;
    VectorBufferWriter writer(buffer);
    fn->serialize(place1, writer);
    writer.commit();

    VectorBufferReader reader(buffer.get_data_at(0));
    fn->reset(place2);
    fn->deserialize(place2, reader, arena);

    auto result = ColumnInt64::create();
    fn->insert_result_into(place2, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(result->get_element(0), 6);
}

// Covers: float/double add_impl() branch together with generic merge() and write()/read().
TEST_F(AggregateFunctionProductTest, test_double_product) {
    DataTypes argument_types = {std::make_shared<DataTypeFloat64>()};
    auto fn = std::make_shared<ProductFunction<TYPE_DOUBLE, TYPE_DOUBLE>>(argument_types);

    auto column = ColumnFloat64::create();
    column->insert_value(2.0);
    column->insert_value(2.5);
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory1(new char[fn->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[fn->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();
    fn->create(place1);
    fn->create(place2);
    Defer defer([&]() {
        fn->destroy(place1);
        fn->destroy(place2);
    });

    fn->reset(place1);
    fn->add(place1, columns, 0, arena);

    fn->reset(place2);
    fn->add(place2, columns, 1, arena);

    fn->merge(place1, place2, arena);

    auto result = ColumnFloat64::create();
    fn->insert_result_into(place1, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_DOUBLE_EQ(result->get_element(0), 5.0);
}

// Covers: decimalv3 add(), reset() decimal branch, get_return_type() decimal branch and
// insert_result_into() for DECIMAL128I.
TEST_F(AggregateFunctionProductTest, test_decimal128_product) {
    constexpr UInt32 scale = 2;
    DataTypes argument_types = {std::make_shared<DataTypeDecimal128>(38, scale)};
    auto fn = std::make_shared<ProductFunction<TYPE_DECIMAL128I, TYPE_DECIMAL128I>>(argument_types);

    auto return_type = fn->get_return_type();
    EXPECT_EQ(return_type->get_primitive_type(), TYPE_DECIMAL128I);
    EXPECT_EQ(get_decimal_scale(*return_type), scale);

    auto column = ColumnDecimal128V3::create(0, scale);
    column->insert_value(Decimal128V3(200)); // 2.00
    column->insert_value(Decimal128V3(300)); // 3.00
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory(new char[fn->size_of_data()]);
    AggregateDataPtr place = memory.get();
    fn->create(place);
    Defer defer([&]() { fn->destroy(place); });

    fn->reset(place);
    for (size_t i = 0; i < column->size(); i++) {
        fn->add(place, columns, i, arena);
    }

    auto result = ColumnDecimal128V3::create(0, scale);
    fn->insert_result_into(place, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(static_cast<int64_t>(result->get_element(0).value), 600); // 6.00
}

// Covers: decimalv3 merge() for DECIMAL128I.
TEST_F(AggregateFunctionProductTest, test_decimal128_merge) {
    constexpr UInt32 scale = 2;
    DataTypes argument_types = {std::make_shared<DataTypeDecimal128>(38, scale)};
    auto fn = std::make_shared<ProductFunction<TYPE_DECIMAL128I, TYPE_DECIMAL128I>>(argument_types);

    auto column = ColumnDecimal128V3::create(0, scale);
    column->insert_value(Decimal128V3(200)); // 2.00
    column->insert_value(Decimal128V3(300)); // 3.00
    column->insert_value(Decimal128V3(400)); // 4.00
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory1(new char[fn->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[fn->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();
    fn->create(place1);
    fn->create(place2);
    Defer defer([&]() {
        fn->destroy(place1);
        fn->destroy(place2);
    });

    fn->reset(place1);
    fn->add(place1, columns, 0, arena);
    fn->add(place1, columns, 1, arena); // 2.00 * 3.00 = 6.00

    fn->reset(place2);
    fn->add(place2, columns, 2, arena); // 4.00

    fn->merge(place1, place2, arena); // 6.00 * 4.00 = 24.00

    auto result = ColumnDecimal128V3::create(0, scale);
    fn->insert_result_into(place1, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(static_cast<int64_t>(result->get_element(0).value), 2400); // 24.00
}

// Covers: decimalv3 write()/read() for DECIMAL128I.
TEST_F(AggregateFunctionProductTest, test_decimal128_serialize_deserialize) {
    constexpr UInt32 scale = 2;
    DataTypes argument_types = {std::make_shared<DataTypeDecimal128>(38, scale)};
    auto fn = std::make_shared<ProductFunction<TYPE_DECIMAL128I, TYPE_DECIMAL128I>>(argument_types);

    auto column = ColumnDecimal128V3::create(0, scale);
    column->insert_value(Decimal128V3(200)); // 2.00
    column->insert_value(Decimal128V3(300)); // 3.00
    const IColumn* columns[1] = {column.get()};

    Arena arena;
    std::unique_ptr<char[]> memory1(new char[fn->size_of_data()]);
    std::unique_ptr<char[]> memory2(new char[fn->size_of_data()]);
    AggregateDataPtr place1 = memory1.get();
    AggregateDataPtr place2 = memory2.get();
    fn->create(place1);
    fn->create(place2);
    Defer defer([&]() {
        fn->destroy(place1);
        fn->destroy(place2);
    });

    fn->reset(place1);
    fn->add(place1, columns, 0, arena);
    fn->add(place1, columns, 1, arena); // 6.00

    ColumnString buffer;
    VectorBufferWriter writer(buffer);
    fn->serialize(place1, writer);
    writer.commit();

    VectorBufferReader reader(buffer.get_data_at(0));
    fn->reset(place2);
    fn->deserialize(place2, reader, arena);

    auto result = ColumnDecimal128V3::create(0, scale);
    fn->insert_result_into(place2, *result);
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(static_cast<int64_t>(result->get_element(0).value), 600); // 6.00
}

// Covers: the DECIMALV2 data specialization (add/merge/write/read/get/reset). This struct is
// not reachable through the AggregateFunctionProduct class for DECIMALV2, so it is exercised
// directly. DecimalV2Value uses a fixed scale of 9, so 1.0 is encoded as 1e9.
TEST_F(AggregateFunctionProductTest, test_decimalv2_data) {
    constexpr int64_t one = 1000000000LL; // 1.0 encoded with DecimalV2 scale 9

    AggregateFunctionProductData<TYPE_DECIMALV2> data;
    data.reset(Decimal128V2(one));
    data.add(Decimal128V2(2 * one), Decimal128V2()); // 2.0
    data.add(Decimal128V2(3 * one), Decimal128V2()); // 2.0 * 3.0 = 6.0
    EXPECT_EQ(static_cast<int64_t>(data.get().value), 6 * one);

    AggregateFunctionProductData<TYPE_DECIMALV2> other;
    other.reset(Decimal128V2(one));
    other.add(Decimal128V2(4 * one), Decimal128V2()); // 4.0

    data.merge(other, Decimal128V2()); // 6.0 * 4.0 = 24.0
    EXPECT_EQ(static_cast<int64_t>(data.get().value), 24 * one);

    ColumnString buffer;
    VectorBufferWriter writer(buffer);
    data.write(writer);
    writer.commit();

    VectorBufferReader reader(buffer.get_data_at(0));
    AggregateFunctionProductData<TYPE_DECIMALV2> restored;
    restored.read(reader);
    EXPECT_EQ(static_cast<int64_t>(restored.get().value), 24 * one);
}

} // namespace doris
