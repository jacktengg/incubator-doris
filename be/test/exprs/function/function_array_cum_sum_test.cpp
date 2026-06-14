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

#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
using namespace ut_type;

namespace {

// Build an Array<Nullable<nested>> input column, run array_cum_sum through the function
// factory, and assert the produced array column equals the expected array column built
// from the same DataSet. The DataSet maps each input array (TestArray, with Null() for
// null elements) to its expected output array.
void check_array_cum_sum(const DataTypePtr& input_nested, const DataTypePtr& result_nested,
                         const DataSet& data_set, bool array_nullable = false) {
    DataTypePtr input_array_type = std::make_shared<DataTypeArray>(input_nested);
    DataTypePtr result_array_type = std::make_shared<DataTypeArray>(result_nested);
    DataTypePtr arg_type = array_nullable ? make_nullable(input_array_type) : input_array_type;
    DataTypePtr result_type = array_nullable ? make_nullable(result_array_type) : result_array_type;

    size_t rows = data_set.size();
    MutableColumnPtr arg_col = arg_type->create_column();
    for (const auto& row : data_set) {
        ASSERT_TRUE(insert_cell(arg_col, arg_type, row.first[0]));
    }
    ColumnPtr arg_column = std::move(arg_col);

    ColumnsWithTypeAndName args = {{arg_column, arg_type, "arg"}};
    auto func = SimpleFunctionFactory::instance().get_function("array_cum_sum", args, result_type);
    ASSERT_NE(func, nullptr);

    Block block;
    block.insert({arg_column, arg_type, "arg"});
    block.insert({nullptr, result_type, "result"});

    auto st = func->execute(nullptr, block, {0}, 1, rows);
    ASSERT_TRUE(st.ok()) << st.to_string();

    MutableColumnPtr expected_col = result_type->create_column();
    for (const auto& row : data_set) {
        ASSERT_TRUE(insert_cell(expected_col, result_type, row.second));
    }

    ColumnPtr result_column = block.get_by_position(1).column;
    ASSERT_EQ(result_column->size(), rows);
    for (size_t i = 0; i < rows; ++i) {
        EXPECT_EQ(0, result_column->compare_at(i, i, *expected_col, 1))
                << "row " << i << ": result=" << result_type->to_string(*result_column, i)
                << " expected=" << result_type->to_string(*expected_col, i);
    }
}

template <typename InT>
DataSet make_int_to_bigint_dataset() {
    return DataSet {
            {{TestArray {InT(1), InT(2), InT(3)}}, TestArray {Int64(1), Int64(3), Int64(6)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {InT(5), Null(), InT(5)}}, TestArray {Int64(5), Int64(5), Int64(10)}},
            {{TestArray {InT(10)}}, TestArray {Int64(10)}},
    };
}

} // namespace

// FunctionArrayCumSum<TYPE_BIGINT>: boolean/tinyint/smallint/int/bigint inputs all map to
// the Int64 result instantiation, exercising those get_return_type_impl/_execute_by_type cases.
TEST(function_array_cum_sum_test, integer_types) {
    DataSet bool_data = {
            {{TestArray {UInt8(1), UInt8(1), UInt8(1)}}, TestArray {Int64(1), Int64(2), Int64(3)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {UInt8(1), UInt8(0), UInt8(1)}}, TestArray {Int64(1), Int64(1), Int64(2)}},
            {{TestArray {UInt8(1)}}, TestArray {Int64(1)}},
    };
    check_array_cum_sum(std::make_shared<DataTypeBool>(), std::make_shared<DataTypeInt64>(),
                        bool_data);

    check_array_cum_sum(std::make_shared<DataTypeInt8>(), std::make_shared<DataTypeInt64>(),
                        make_int_to_bigint_dataset<Int8>());
    check_array_cum_sum(std::make_shared<DataTypeInt16>(), std::make_shared<DataTypeInt64>(),
                        make_int_to_bigint_dataset<Int16>());
    check_array_cum_sum(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>(),
                        make_int_to_bigint_dataset<Int32>());
    check_array_cum_sum(std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>(),
                        make_int_to_bigint_dataset<Int64>());
}

// FunctionArrayCumSum<TYPE_LARGEINT>: largeint input keeps Int128 accumulation.
TEST(function_array_cum_sum_test, largeint) {
    DataSet largeint_data = {
            {{TestArray {int128_t(1), int128_t(2), int128_t(3)}},
             TestArray {int128_t(1), int128_t(3), int128_t(6)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {int128_t(100), Null(), int128_t(100)}},
             TestArray {int128_t(100), int128_t(100), int128_t(200)}},
            {{TestArray {int128_t(10)}}, TestArray {int128_t(10)}},
    };
    check_array_cum_sum(std::make_shared<DataTypeInt128>(), std::make_shared<DataTypeInt128>(),
                        largeint_data);
}

// FunctionArrayCumSum<TYPE_DOUBLE>: float and double inputs both accumulate as double.
TEST(function_array_cum_sum_test, floating_types) {
    DataSet float_data = {
            {{TestArray {float(1.5), float(2.5)}}, TestArray {double(1.5), double(4.0)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {float(2.0), Null(), float(1.0)}},
             TestArray {double(2.0), double(2.0), double(3.0)}},
    };
    check_array_cum_sum(std::make_shared<DataTypeFloat32>(), std::make_shared<DataTypeFloat64>(),
                        float_data);

    DataSet double_data = {
            {{TestArray {double(1.5), double(2.5), double(3.0)}},
             TestArray {double(1.5), double(4.0), double(7.0)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {double(10.0)}}, TestArray {double(10.0)}},
    };
    check_array_cum_sum(std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeFloat64>(),
                        double_data);
}

// FunctionArrayCumSum<TYPE_DECIMAL128I>: decimalv3 widened to Decimal128 by the FE.
TEST(function_array_cum_sum_test, decimal128) {
    DataSet d128 = {
            {{TestArray {DECIMAL128V3(1, 0, 2), DECIMAL128V3(2, 0, 2), DECIMAL128V3(3, 0, 2)}},
             TestArray {DECIMAL128V3(1, 0, 2), DECIMAL128V3(3, 0, 2), DECIMAL128V3(6, 0, 2)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {DECIMAL128V3(5, 0, 2), Null(), DECIMAL128V3(5, 0, 2)}},
             TestArray {DECIMAL128V3(5, 0, 2), DECIMAL128V3(5, 0, 2), DECIMAL128V3(10, 0, 2)}},
    };
    check_array_cum_sum(
            std::make_shared<DataTypeDecimal128>(DataTypeDecimal128::max_precision(), 2),
            std::make_shared<DataTypeDecimal128>(DataTypeDecimal128::max_precision(), 2), d128);
}

// FunctionArrayCumSum<TYPE_DECIMAL256>: decimalv3 widened to Decimal256 by the FE.
TEST(function_array_cum_sum_test, decimal256) {
    DataSet d256 = {
            {{TestArray {DECIMAL256(1, 0, 3), DECIMAL256(2, 0, 3)}},
             TestArray {DECIMAL256(1, 0, 3), DECIMAL256(3, 0, 3)}},
            {{TestArray {}}, TestArray {}},
            {{TestArray {DECIMAL256(10, 0, 3)}}, TestArray {DECIMAL256(10, 0, 3)}},
    };
    check_array_cum_sum(
            std::make_shared<DataTypeDecimal256>(DataTypeDecimal256::max_precision(), 3),
            std::make_shared<DataTypeDecimal256>(DataTypeDecimal256::max_precision(), 3), d256);
}

// Null handling inside a single array row: only the leading null prefix becomes NULL in the
// output, while interior nulls are treated as zero. Each array is checked in its own block so
// the global "first valid position" logic applies per array.
TEST(function_array_cum_sum_test, null_elements) {
    auto int32 = []() { return std::make_shared<DataTypeInt32>(); };
    auto int64 = []() { return std::make_shared<DataTypeInt64>(); };

    check_array_cum_sum(int32(), int64(),
                        DataSet {{{TestArray {Null(), Null(), Int32(1), Int32(2), Int32(3)}},
                                  TestArray {Null(), Null(), Int64(1), Int64(3), Int64(6)}}});
    check_array_cum_sum(int32(), int64(),
                        DataSet {{{TestArray {Null(), Int32(1), Null(), Int32(2), Int32(3)}},
                                  TestArray {Null(), Int64(1), Int64(1), Int64(3), Int64(6)}}});
    check_array_cum_sum(
            int32(), int64(),
            DataSet {{{TestArray {Null(), Null(), Null()}}, TestArray {Null(), Null(), Null()}}});
    check_array_cum_sum(int32(), int64(),
                        DataSet {{{TestArray {Int32(1), Null(), Null(), Int32(3)}},
                                  TestArray {Int64(1), Int64(1), Int64(1), Int64(4)}}});
}

// NULL array rows go through the default nullable implementation, which masks the per-row
// result and re-wraps the output array column as nullable.
TEST(function_array_cum_sum_test, nullable_array_rows) {
    DataSet data = {
            {{Null()}, Null()},
            {{TestArray {Int32(1), Int32(2), Int32(3)}}, TestArray {Int64(1), Int64(3), Int64(6)}},
            {{Null()}, Null()},
            {{TestArray {Int32(4), Int32(5)}}, TestArray {Int64(4), Int64(9)}},
    };
    check_array_cum_sum(std::make_shared<DataTypeInt32>(), std::make_shared<DataTypeInt64>(), data,
                        /*array_nullable=*/true);
}

// An unsupported nested element type makes get_return_type_impl fail and throw.
TEST(function_array_cum_sum_test, unsupported_return_type_throws) {
    DataTypePtr arg_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    DataTypePtr result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

    MutableColumnPtr arg_col = arg_type->create_column();
    ColumnPtr arg_column = std::move(arg_col);
    ColumnsWithTypeAndName args = {{arg_column, arg_type, "arg"}};

    EXPECT_ANY_THROW({
        auto func =
                SimpleFunctionFactory::instance().get_function("array_cum_sum", args, result_type);
        static_cast<void>(func);
    });
}

// A decimal input routed to the integer instantiation hits the decimalv3 type guard in
// _execute_number, so _execute_by_type returns false and execute reports InvalidArgument.
TEST(function_array_cum_sum_test, unsupported_execute_returns_error) {
    DataTypePtr arg_type = std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeDecimal128>(DataTypeDecimal128::max_precision(), 2));
    DataTypePtr result_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>());

    MutableColumnPtr arg_col = arg_type->create_column();
    ASSERT_TRUE(insert_cell(arg_col, arg_type,
                            TestArray {DECIMAL128V3(1, 0, 2), DECIMAL128V3(2, 0, 2)}));
    ColumnPtr arg_column = std::move(arg_col);

    ColumnsWithTypeAndName args = {{arg_column, arg_type, "arg"}};
    auto func = SimpleFunctionFactory::instance().get_function("array_cum_sum", args, result_type);
    ASSERT_NE(func, nullptr);

    Block block;
    block.insert({arg_column, arg_type, "arg"});
    block.insert({nullptr, result_type, "result"});

    auto st = func->execute(nullptr, block, {0}, 1, 1);
    EXPECT_FALSE(st.ok());
}

// ColumnArrayExecutionData::to_mutable_data() copies offsets and nested data while
// preserving per-element NULL information of a nullable nested column.
TEST(function_array_utils_test, to_mutable_data_nullable_nested) {
    DataTypePtr array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
    MutableColumnPtr col = array_type->create_column();
    ASSERT_TRUE(insert_cell(col, array_type, TestArray {Int32(1), Int32(2), Int32(3)}));
    ASSERT_TRUE(insert_cell(col, array_type, TestArray {}));
    ASSERT_TRUE(insert_cell(col, array_type, TestArray {Null(), Int32(5)}));
    ASSERT_TRUE(insert_cell(col, array_type, TestArray {Int32(10)}));
    ColumnPtr src = std::move(col);

    ColumnArrayExecutionData data;
    ASSERT_TRUE(extract_column_array_info(*src, data));
    ASSERT_NE(data.nested_nullmap_data, nullptr);

    ColumnArrayMutableData dst = data.to_mutable_data();

    size_t num_rows = data.offsets_ptr->size();
    ASSERT_EQ(num_rows, 4);
    ASSERT_EQ(dst.offsets_ptr->size(), num_rows);
    for (size_t r = 0; r < num_rows; ++r) {
        EXPECT_EQ((*dst.offsets_ptr)[r], (*data.offsets_ptr)[r]) << "row " << r;
    }

    size_t total = (*data.offsets_ptr)[num_rows - 1];
    ASSERT_EQ(dst.nested_col->size(), total);
    ASSERT_EQ(dst.nested_nullmap_data->size(), total);
    for (size_t i = 0; i < total; ++i) {
        UInt8 src_null = data.nested_nullmap_data[i];
        EXPECT_EQ((*dst.nested_nullmap_data)[i], src_null) << "elem " << i;
        if (!src_null) {
            EXPECT_EQ(0, dst.nested_col->compare_at(i, i, *data.nested_col, 1)) << "elem " << i;
        }
    }

    // The reassembled array must equal the source array row-by-row.
    MutableColumnPtr assembled = assemble_column_array(dst);
    ASSERT_EQ(assembled->size(), src->size());
    for (size_t i = 0; i < src->size(); ++i) {
        EXPECT_EQ(0, assembled->compare_at(i, i, *src, 1)) << "row " << i;
    }
}

// When the nested column is not nullable, to_mutable_data() marks every copied element as
// non-null (the nested_nullmap_data branch is skipped during extraction).
TEST(function_array_utils_test, to_mutable_data_non_nullable_nested) {
    auto nested = ColumnInt32::create();
    for (Int32 v : {1, 2, 3, 5, 10}) {
        nested->insert_value(v);
    }
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(3); // [1, 2, 3]
    offsets->get_data().push_back(3); // []
    offsets->get_data().push_back(5); // [5, 10]
    ColumnPtr src = ColumnArray::create(std::move(nested), std::move(offsets));

    ColumnArrayExecutionData data;
    ASSERT_TRUE(extract_column_array_info(*src, data));
    ASSERT_EQ(data.nested_nullmap_data, nullptr);

    ColumnArrayMutableData dst = data.to_mutable_data();

    size_t num_rows = data.offsets_ptr->size();
    ASSERT_EQ(num_rows, 3);
    ASSERT_EQ(dst.offsets_ptr->size(), num_rows);
    for (size_t r = 0; r < num_rows; ++r) {
        EXPECT_EQ((*dst.offsets_ptr)[r], (*data.offsets_ptr)[r]) << "row " << r;
    }

    size_t total = (*data.offsets_ptr)[num_rows - 1];
    ASSERT_EQ(dst.nested_col->size(), total);
    ASSERT_EQ(dst.nested_nullmap_data->size(), total);
    for (size_t i = 0; i < total; ++i) {
        EXPECT_EQ((*dst.nested_nullmap_data)[i], 0) << "elem " << i;
        EXPECT_EQ(0, dst.nested_col->compare_at(i, i, *data.nested_col, 1)) << "elem " << i;
    }
}

} // namespace doris
