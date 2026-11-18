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

#include <limits>
#include <string>

#include "core/data_type/data_type_bitmap.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/types.h"
#include "core/value/bitmap_value.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function_test_util.h"

namespace doris {
using namespace ut_type;

TEST(FunctionParameterBoundaryTest, RightMinimumPosition) {
    // One character and one position are enough. INT_MIN must return empty before negation.
    InputTypeSet types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_INT};
    DataSet rows = {{{std::string("a"), std::numeric_limits<Int32>::min()}, std::string("")}};
    check_function_all_arg_comb<DataTypeString, true>("right", types, rows);
    check_function_all_arg_comb<DataTypeString, true>("strright", types, rows);
}

TEST(FunctionParameterBoundaryTest, SplitPartMinimumPosition) {
    // One separator produces two parts. The negative position is out of range, so the result is NULL.
    InputTypeSet types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_INT};
    DataSet rows = {
            {{std::string("a,"), std::string(","), std::numeric_limits<Int32>::min()}, Null()}};
    check_function_all_arg_comb<DataTypeString, true>("split_part", types, rows);
}

TEST(FunctionParameterBoundaryTest, SubstringIndexMinimumCount) {
    // With no separator, an excessive negative count returns the entire one-character string.
    InputTypeSet types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_INT};
    DataSet rows = {{{std::string("a"), std::string(","), std::numeric_limits<Int32>::min()},
                     std::string("a")}};
    check_function_all_arg_comb<DataTypeString, true>("substring_index", types, rows);
}

TEST(FunctionParameterBoundaryTest, CountSubstringsMinimumPosition) {
    // Nonpositive positions mean zero matches. Subtracting one from INT_MIN must not occur.
    InputTypeSet types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_INT};
    DataSet rows = {
            {{std::string("a"), std::string("a"), std::numeric_limits<Int32>::min()}, Int32(0)}};
    check_function_all_arg_comb<DataTypeInt32, true>("count_substrings", types, rows);
}

TEST(FunctionParameterBoundaryTest, ExportSetZeroBits) {
    // No output bit needs a separator. A one-byte separator exposed the old unsigned -1 capacity.
    InputTypeSet types = {PrimitiveType::TYPE_LARGEINT, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR,
                          PrimitiveType::TYPE_INT};
    DataSet rows = {{{Int128(1), std::string("1"), std::string("0"), std::string(","), Int32(0)},
                     std::string("")}};
    check_function_all_arg_comb<DataTypeString, true>("export_set", types, rows);
}

TEST(FunctionParameterBoundaryTest, ArraySliceSignedBounds) {
    // Two elements are the minimum needed for start + INT64_MAX to overflow.
    auto values = ColumnInt32::create();
    values->insert_value(1);
    values->insert_value(2);
    auto ends = ColumnArray::ColumnOffsets::create();
    ends->insert_value(2);
    auto array = ColumnArray::create(std::move(values), std::move(ends));
    ColumnArrayExecutionData source;
    ASSERT_TRUE(extract_column_array_info(*array, source));
    for (Int64 offset : {Int64(2), Int64(-1), std::numeric_limits<Int64>::min()}) {
        auto offsets = ColumnInt64::create();
        offsets->insert_value(offset);
        auto lengths = ColumnInt64::create();
        lengths->insert_value(std::numeric_limits<Int64>::max());
        auto result = create_mutable_data(source.nested_col.get(), false);
        slice_array(result, source, *offsets, lengths.get());
        const size_t expected = offset == std::numeric_limits<Int64>::min() ? 0 : 1;
        ASSERT_EQ(result.nested_col->size(), expected);
        EXPECT_EQ((*result.offsets_ptr)[0], expected);
        if (expected != 0) {
            EXPECT_EQ(result.nested_col->get_int(0), 2);
        }
    }
}

TEST(FunctionParameterBoundaryTest, BitmapNegativeElement) {
    // A single UINT64_MAX element makes the old -1 conversion observable in both functions.
    BitmapValue bitmap(std::numeric_limits<UInt64>::max());
    for (bool const_bitmap : {false, true}) {
        for (bool const_element : {false, true}) {
            InputTypeSet types = {PrimitiveType::TYPE_BITMAP, PrimitiveType::TYPE_BIGINT};
            if (const_bitmap) {
                types[0] = Consted {PrimitiveType::TYPE_BITMAP};
            }
            if (const_element) {
                types[1] = Consted {PrimitiveType::TYPE_BIGINT};
            }
            DataSet contains = {{{&bitmap, Int64(-1)}, UInt8(0)}};
            auto status = check_function<DataTypeUInt8, true>("bitmap_contains", types, contains,
                                                              -1, -1, true);
            EXPECT_NE(status.to_string().find("element must be >= 0"), std::string::npos);
            DataSet remove = {{{&bitmap, Int64(-1)}, &bitmap}};
            status = check_function<DataTypeBitMap, true>("bitmap_remove", types, remove, -1, -1,
                                                          true);
            EXPECT_NE(status.to_string().find("element must be >= 0"), std::string::npos);
            EXPECT_TRUE(bitmap.contains(std::numeric_limits<UInt64>::max()));
        }
    }
}

TEST(FunctionParameterBoundaryTest, SleepRejectsNegativeDuration) {
    // Only negative and zero durations are used. This test never waits for positive time.
    for (bool constant : {false, true}) {
        InputTypeSet types = {Notnull {PrimitiveType::TYPE_INT}};
        InputTypeSet nullable_types = {PrimitiveType::TYPE_INT};
        if (constant) {
            types[0] = ConstedNotnull {PrimitiveType::TYPE_INT};
            nullable_types[0] = Consted {PrimitiveType::TYPE_INT};
        }
        DataSet invalid = {{{Int32(-1)}, UInt8(0)}};
        auto status = check_function<DataTypeUInt8>("sleep", types, invalid, -1, -1, true);
        EXPECT_NE(status.to_string().find("duration must be nonnegative"), std::string::npos);
        status =
                check_function<DataTypeUInt8, true>("sleep", nullable_types, invalid, -1, -1, true);
        EXPECT_NE(status.to_string().find("duration must be nonnegative"), std::string::npos);
        DataSet zero = {{{Int32(0)}, UInt8(0)}};
        EXPECT_TRUE(check_function<DataTypeUInt8>("sleep", types, zero).ok());
        EXPECT_TRUE((check_function<DataTypeUInt8, true>("sleep", nullable_types, zero).ok()));
        DataSet null = {{{Null()}, Null()}};
        EXPECT_TRUE((check_function<DataTypeUInt8, true>("sleep", nullable_types, null).ok()));
    }
}

TEST(FunctionParameterBoundaryTest, WidthBucketNegativeBounds) {
    // Each row isolates a boundary. Negative values and negative bounds remain valid.
    InputTypeSet types = {PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT,
                          PrimitiveType::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT};
    DataSet rows = {{{Int64(-1), Int64(-1), Int64(0), Int64(2)}, Int64(1)},
                    {{Int64(-1), Int64(-2), Int64(0), Int64(2)}, Int64(2)},
                    {{Int64(0), std::numeric_limits<Int64>::min(),
                      std::numeric_limits<Int64>::max(), Int64(2)},
                     Int64(2)}};
    // The bucket count is 2 for all rows because this argument must be constant in SQL.
    ASSERT_TRUE(check_function<DataTypeInt64>("width_bucket", types, rows).ok());
    for (Int64 count : {Int64(-1), Int64(0), std::numeric_limits<Int64>::max()}) {
        DataSet invalid = {{{Int64(0), Int64(-1), Int64(0), count}, Int64(0)}};
        auto status = check_function<DataTypeInt64>("width_bucket", types, invalid, -1, -1, true);
        EXPECT_NE(status.to_string().find("number of buckets must be between"), std::string::npos);
    }
    // NULL rows must not compare their internal zero payloads as equal bounds.
    DataSet nullable_rows = {{{Null(), Null(), Null(), Int64(2)}, Null()},
                             {{Int64(-1), Int64(-1), Int64(0), Int64(2)}, Int64(1)}};
    ASSERT_TRUE((check_function<DataTypeInt64, true>("width_bucket", types, nullable_rows).ok()));
    DataSet null_count = {{{Int64(-1), Int64(-1), Int64(0), Null()}, Null()}};
    ASSERT_TRUE((check_function<DataTypeInt64, true>("width_bucket", types, null_count).ok()));
    DataSet equal_bounds = {{{Int64(-1), Int64(-1), Int64(-1), Int64(1)}, Int64(0)}};
    auto status = check_function<DataTypeInt64>("width_bucket", types, equal_bounds, -1, -1, true);
    EXPECT_NE(status.to_string().find("bounds must be different"), std::string::npos);
}

TEST(FunctionParameterBoundaryTest, WidthBucketNonfiniteInput) {
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE,
                          PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_BIGINT};
    // A single nonfinite input must fail before conversion of its bucket to BIGINT.
    for (double value :
         {std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        DataSet rows = {{{value, double(-1), double(0), Int64(2)}, Int64(0)}};
        auto status = check_function<DataTypeInt64>("width_bucket", types, rows, -1, -1, true);
        EXPECT_NE(status.to_string().find("values and bounds must be finite"), std::string::npos);
    }
}

TEST(FunctionParameterBoundaryTest, FloatingNegativeRoundingScale) {
    InputTypeSet types = {PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_INT};
    // One value at 10^19 distinguishes 10^20 from the old saturated UINT64_MAX factor.
    for (const std::string function : {"round", "round_bankers", "truncate", "floor"}) {
        DataSet rows = {{{1e19, Int32(-20)}, double(0)}};
        check_function_all_arg_comb<DataTypeFloat64, true>(function, types, rows);
    }
    DataSet ceiling = {{{1e19, Int32(-20)}, 1e20}};
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types, ceiling);
    // The minimum supported scale has no finite factor. ROUND still returns zero.
    DataSet tiny = {{{double(1), Int32(-32768)}, double(0)}};
    check_function_all_arg_comb<DataTypeFloat64, true>("round", types, tiny);
    DataSet directed = {{{double(1), Int32(-309)}, double(0)}};
    auto status = check_function<DataTypeFloat64, true>("ceil", types, directed, -1, -1, true);
    EXPECT_NE(status.to_string().find("Arithmetic overflow"), std::string::npos);
    DataSet overflow = {{{1.7e308, Int32(-308)}, double(0)}};
    status = check_function<DataTypeFloat64, true>("ceil", types, overflow, -1, -1, true);
    EXPECT_NE(status.to_string().find("Arithmetic overflow"), std::string::npos);
    // A finite factor can underflow the quotient. CEIL must still return one unit.
    DataSet underflow = {{{1e-300, Int32(-308)}, 1e308}};
    check_function_all_arg_comb<DataTypeFloat64, true>("ceil", types, underflow);
}
} // namespace doris
