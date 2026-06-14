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

#include <cmath>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "exprs/function/function_string_format.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

ColumnPtr make_int32_column(const std::vector<int32_t>& vals) {
    auto col = ColumnInt32::create();
    for (auto v : vals) {
        col->insert_value(v);
    }
    return col;
}

ColumnPtr make_const_int32_column(int32_t val, size_t rows) {
    auto inner = ColumnInt32::create();
    inner->insert_value(val);
    return ColumnConst::create(std::move(inner), rows);
}

std::vector<std::string> run_format_round(const FunctionPtr& func, ColumnPtr value_column,
                                          const DataTypePtr& value_type, ColumnPtr places_column,
                                          size_t rows, Status* out_status) {
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    auto return_type = std::make_shared<DataTypeString>();
    std::vector<DataTypePtr> arg_types = {value_type, std::make_shared<DataTypeInt32>()};
    auto context = FunctionContext::create_context(runtime_state.get(), return_type, arg_types);

    Block block;
    block.insert({std::move(value_column), value_type, "val"});
    block.insert({std::move(places_column), std::make_shared<DataTypeInt32>(), "places"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0, 1};
    Status st = func->execute_impl(context.get(), block, arguments, 2, rows);
    if (out_status != nullptr) {
        *out_status = st;
    }

    std::vector<std::string> results;
    if (st.ok()) {
        auto col_res = block.get_by_position(2).column;
        for (size_t i = 0; i < col_res->size(); ++i) {
            results.push_back(col_res->get_data_at(i).to_string());
        }
    }
    return results;
}

template <PrimitiveType PT>
void check_format_round_decimal(
        UInt32 precision, UInt32 scale,
        const std::vector<typename PrimitiveTypeTraits<PT>::CppType::NativeType>& raws,
        const std::vector<int32_t>& places, const std::vector<std::string>& expected) {
    using ColumnType = typename PrimitiveTypeTraits<PT>::ColumnType;
    using ValueType = typename ColumnType::value_type;
    auto col = ColumnType::create(0, scale);
    for (auto r : raws) {
        col->insert_value(ValueType(r));
    }
    auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<PT>>::create();
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>(precision, scale);
    Status st;
    auto results = run_format_round(func, std::move(col), type, make_int32_column(places),
                                    raws.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i << ", scale " << scale;
    }
}

} // namespace

TEST(function_format_round_test, format_round_double) {
    auto func = FunctionStringFormatRound<FormatRoundDoubleImpl>::create();
    auto col = ColumnFloat64::create();
    std::vector<double> values = {0.0, 12.5, 1234567.0, 1234567.25, 123456.0, -1234.5, -7654321.5};
    std::vector<int32_t> places = {2, 2, 0, 2, 2, 1, 2};
    for (double v : values) {
        col->insert_value(v);
    }
    Status st;
    auto results = run_format_round(func, std::move(col), std::make_shared<DataTypeFloat64>(),
                                    make_int32_column(places), values.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    std::vector<std::string> expected = {"0.00",       "12.50",    "1,234,567",    "1,234,567.25",
                                         "123,456.00", "-1,234.5", "-7,654,321.50"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_double_non_finite) {
    auto func = FunctionStringFormatRound<FormatRoundDoubleImpl>::create();
    auto col = ColumnFloat64::create();
    col->insert_value(std::numeric_limits<double>::infinity());
    col->insert_value(-std::numeric_limits<double>::infinity());
    col->insert_value(std::numeric_limits<double>::quiet_NaN());
    Status st;
    auto results = run_format_round(func, std::move(col), std::make_shared<DataTypeFloat64>(),
                                    make_int32_column({2, 2, 2}), 3, &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    std::vector<std::string> expected = {"inf", "-inf", "nan"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_int64) {
    auto func = FunctionStringFormatRound<FormatRoundInt64Impl>::create();
    auto col = ColumnInt64::create();
    std::vector<int64_t> values = {1234567, 1234567, -7654321, 0, 7};
    std::vector<int32_t> places = {0, 2, 3, 2, 4};
    for (auto v : values) {
        col->insert_value(v);
    }
    Status st;
    auto results = run_format_round(func, std::move(col), std::make_shared<DataTypeInt64>(),
                                    make_int32_column(places), values.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    std::vector<std::string> expected = {"1,234,567", "1,234,567.00", "-7,654,321.000", "0.00",
                                         "7.0000"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_largeint) {
    auto func = FunctionStringFormatRound<FormatRoundInt128Impl>::create();
    auto col = ColumnInt128::create();
    std::vector<Int128> values = {1234567890123, -98765, 0};
    std::vector<int32_t> places = {2, 0, 3};
    for (auto v : values) {
        col->insert_value(v);
    }
    Status st;
    auto results = run_format_round(func, std::move(col), std::make_shared<DataTypeInt128>(),
                                    make_int32_column(places), values.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    std::vector<std::string> expected = {"1,234,567,890,123.00", "-98,765", "0.000"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_decimal32) {
    // scale > decimal_places: rounding branch (and append-sign for -0.0xx)
    check_format_round_decimal<TYPE_DECIMAL32>(9, 3, {-12, 12, 1234, 1235, 0}, {2, 2, 2, 2, 2},
                                               {"-0.01", "0.01", "1.23", "1.24", "0.00"});
}

TEST(function_format_round_test, format_round_decimal64) {
    check_format_round_decimal<TYPE_DECIMAL64>(
            18, 4, {123456789, 123456789, 123456789, -123456789, 99995}, {2, 6, 0, 2, 3},
            {"12,345.68", "12,345.678900", "12,345", "-12,345.68", "10.000"});
}

TEST(function_format_round_test, format_round_decimal128) {
    check_format_round_decimal<TYPE_DECIMAL128I>(
            38, 6, {Int128(123456789012345), Int128(123456789012345), Int128(9999995)}, {2, 4, 3},
            {"123,456,789.01", "123,456,789.0123", "10.000"});
}

TEST(function_format_round_test, format_round_decimalv2) {
    std::vector<std::string> dec_strs = {"123.456789", "123.454", "12.345", "-0.001", "1234.5"};
    std::vector<int32_t> places = {2, 2, 3, 2, 0};
    std::vector<std::string> expected = {"123.46", "123.45", "12.345", "-0.00", "1,234"};

    auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMALV2>>::create();
    auto col = ColumnDecimal128V2::create(0, 9);
    for (const auto& s : dec_strs) {
        col->insert_value(DecimalV2Value(s));
    }
    auto type = std::make_shared<DataTypeDecimalV2>(27, 9);
    Status st;
    auto results = run_format_round(func, std::move(col), type, make_int32_column(places),
                                    dec_strs.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_const_decimal_places) {
    // A constant second argument exercises the is_const == true execution path.
    auto func = FunctionStringFormatRound<FormatRoundInt64Impl>::create();
    auto col = ColumnInt64::create();
    std::vector<int64_t> values = {1234567, -7654321, 0};
    for (auto v : values) {
        col->insert_value(v);
    }
    Status st;
    auto results = run_format_round(func, std::move(col), std::make_shared<DataTypeInt64>(),
                                    make_const_int32_column(2, values.size()), values.size(), &st);
    EXPECT_TRUE(st.ok()) << st.to_string();
    std::vector<std::string> expected = {"1,234,567.00", "-7,654,321.00", "0.00"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_format_round_test, format_round_decimal256_unsupported) {
    // format_round does not support decimal256; execution returns an error status.
    auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMAL256>>::create();
    auto col = ColumnDecimal256::create(0, 4);
    col->insert_value(Decimal256(Int128(12345)));
    auto type = std::make_shared<DataTypeDecimal256>(40, 4);
    Status st;
    run_format_round(func, std::move(col), type, make_int32_column({2}), 1, &st);
    EXPECT_FALSE(st.ok());
}

TEST(function_format_round_test, format_round_invalid_decimal_places) {
    // The second argument must be in range [0, 1024]; out-of-range values fail for every impl.
    {
        auto func = FunctionStringFormatRound<FormatRoundDoubleImpl>::create();
        auto col = ColumnFloat64::create();
        col->insert_value(1.0);
        Status st;
        run_format_round(func, std::move(col), std::make_shared<DataTypeFloat64>(),
                         make_int32_column({-1}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundInt64Impl>::create();
        auto col = ColumnInt64::create();
        col->insert_value(1);
        Status st;
        run_format_round(func, std::move(col), std::make_shared<DataTypeInt64>(),
                         make_int32_column({1025}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundInt128Impl>::create();
        auto col = ColumnInt128::create();
        col->insert_value(Int128(1));
        Status st;
        run_format_round(func, std::move(col), std::make_shared<DataTypeInt128>(),
                         make_int32_column({-1}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMALV2>>::create();
        auto col = ColumnDecimal128V2::create(0, 9);
        col->insert_value(DecimalV2Value(std::string("1.5")));
        auto type = std::make_shared<DataTypeDecimalV2>(27, 9);
        Status st;
        run_format_round(func, std::move(col), type, make_int32_column({1025}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMAL32>>::create();
        auto col = ColumnDecimal32::create(0, 2);
        col->insert_value(Decimal32(150));
        auto type = std::make_shared<DataTypeDecimal32>(9, 2);
        Status st;
        run_format_round(func, std::move(col), type, make_int32_column({-1}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMAL64>>::create();
        auto col = ColumnDecimal64::create(0, 4);
        col->insert_value(Decimal64(15000));
        auto type = std::make_shared<DataTypeDecimal64>(18, 4);
        Status st;
        run_format_round(func, std::move(col), type, make_int32_column({1025}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
    {
        auto func = FunctionStringFormatRound<FormatRoundDecimalImpl<TYPE_DECIMAL128I>>::create();
        auto col = ColumnDecimal128V3::create(0, 6);
        col->insert_value(Decimal128V3(Int128(1500000)));
        auto type = std::make_shared<DataTypeDecimal128>(38, 6);
        Status st;
        run_format_round(func, std::move(col), type, make_int32_column({-1}), 1, &st);
        EXPECT_FALSE(st.ok());
    }
}

TEST(function_format_round_test, format_round_wrong_argument_count) {
    auto func = FunctionStringFormatRound<FormatRoundInt64Impl>::create();
    DataTypes args = {std::make_shared<DataTypeInt64>()};
    EXPECT_THROW({ static_cast<void>(func->get_return_type_impl(args)); }, doris::Exception);
}

} // namespace doris
