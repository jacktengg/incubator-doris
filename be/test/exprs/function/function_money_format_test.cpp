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

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_decimal.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "core/value/decimalv2_value.h"
#include "exprs/function/function_string_format.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {

std::vector<std::string> run_money_format(const FunctionPtr& func, ColumnPtr input_column,
                                          const DataTypePtr& input_type) {
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    auto return_type = std::make_shared<DataTypeString>();
    std::vector<DataTypePtr> arg_types = {input_type};
    auto context = FunctionContext::create_context(runtime_state.get(), return_type, arg_types);

    size_t rows = input_column->size();
    Block block;
    block.insert({std::move(input_column), input_type, "arg"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0};
    Status st = func->execute_impl(context.get(), block, arguments, 1, rows);
    EXPECT_TRUE(st.ok()) << st.to_string();

    std::vector<std::string> results;
    auto col_res = block.get_by_position(1).column;
    for (size_t i = 0; i < col_res->size(); ++i) {
        results.push_back(col_res->get_data_at(i).to_string());
    }
    return results;
}

template <PrimitiveType PT>
void check_money_format_decimal(
        UInt32 precision, UInt32 scale,
        const std::vector<typename PrimitiveTypeTraits<PT>::CppType::NativeType>& raws,
        const std::vector<std::string>& expected) {
    using ColumnType = typename PrimitiveTypeTraits<PT>::ColumnType;
    using ValueType = typename ColumnType::value_type;
    auto col = ColumnType::create(0, scale);
    for (auto r : raws) {
        col->insert_value(ValueType(r));
    }
    auto func = FunctionMoneyFormat<MoneyFormatDecimalImpl<PT>>::create();
    auto type = std::make_shared<typename PrimitiveTypeTraits<PT>::DataType>(precision, scale);
    auto results = run_money_format(func, std::move(col), type);
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i << ", scale " << scale;
    }
}

} // namespace

TEST(function_money_format_test, money_format_with_decimalV2) {
    // why not using
    std::multimap<std::string, std::string> input_dec_str_and_expected_str = {
            {std::string("123.12"), std::string("123.12")},
            {std::string("-123.12"), std::string("-123.12")},
            {std::string("-0.12434"), std::string("-0.12")},
            {std::string("-0.12534"), std::string("-0.13")},
            {std::string("-123456789.12434"), std::string("-123,456,789.12")},
            {std::string("-123456789.12534"), std::string("-123,456,789.13")},
            {std::string("0.999999999"), std::string("1.00")},
            {std::string("-0.999999999"), std::string("-1.00")},
            {std::string("999999999999999999.994999999"),
             std::string("999,999,999,999,999,999.99")},
            {std::string("-999999999999999999.994999999"),
             std::string("-999,999,999,999,999,999.99")},
            {std::string("-999999999999999999.995999999"),
             std::string("-1,000,000,000,000,000,000.00")}};

    auto money_format = FunctionMoneyFormat<MoneyFormatDecimalImpl<TYPE_DECIMALV2>>::create();
    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    auto return_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_VARCHAR, false);
    auto arg_type = DataTypeFactory::instance().create_data_type(
            PrimitiveType::TYPE_DECIMALV2, false, BeConsts::MAX_DECIMALV2_PRECISION,
            BeConsts::MAX_DECIMALV2_SCALE);
    std::vector<DataTypePtr> arg_types = {arg_type};

    auto context = FunctionContext::create_context(runtime_state.get(), return_type, arg_types);

    Block block;
    ColumnNumbers arguments = {0};
    size_t result_idx = 1;
    auto col_dec_v2 = ColumnDecimal128V2::create(0, 9);
    auto col_res_expected = ColumnString::create();
    for (const auto& input_and_expected : input_dec_str_and_expected_str) {
        DecimalV2Value dec_v2_value(input_and_expected.first);
        col_dec_v2->insert_value(dec_v2_value);
        col_res_expected->insert_data(input_and_expected.second.c_str(),
                                      input_and_expected.second.size());
    }

    block.insert({std::move(col_dec_v2), std::make_shared<DataTypeDecimalV2>(10, 1), "col_dec_v2"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "col_res"});

    Status exec_status = money_format->execute_impl(context.get(), block, arguments, result_idx,
                                                    block.get_by_position(0).column->size());

    // Check result
    auto col_res = block.get_by_position(result_idx).column;
    for (size_t i = 0; i < col_res->size(); ++i) {
        auto res = col_res->get_data_at(i);
        auto res_expected = col_res_expected->get_data_at(i);
        EXPECT_EQ(res.debug_string(), res_expected.debug_string())
                << "res " << res.debug_string() << ' ' << "res_expected "
                << res_expected.debug_string();
    }
}

TEST(function_money_format_test, money_format_double) {
    auto func = FunctionMoneyFormat<MoneyFormatDoubleImpl>::create();
    auto col = ColumnFloat64::create();
    std::vector<double> inputs = {0.0, 12.5, 1000.5, 7654321.5, -7654321.5, 1234567.25, 123.0};
    for (double v : inputs) {
        col->insert_value(v);
    }
    auto results = run_money_format(func, std::move(col), std::make_shared<DataTypeFloat64>());
    std::vector<std::string> expected = {
            "0.00", "12.50", "1,000.50", "7,654,321.50", "-7,654,321.50", "1,234,567.25", "123.00"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_money_format_test, money_format_int64) {
    auto func = FunctionMoneyFormat<MoneyFormatInt64Impl>::create();
    auto col = ColumnInt64::create();
    std::vector<int64_t> inputs = {0, 7, 123, -1, 1234567, -7654321, 1000000000000};
    for (auto v : inputs) {
        col->insert_value(v);
    }
    auto results = run_money_format(func, std::move(col), std::make_shared<DataTypeInt64>());
    std::vector<std::string> expected = {"0.00",
                                         "7.00",
                                         "123.00",
                                         "-1.00",
                                         "1,234,567.00",
                                         "-7,654,321.00",
                                         "1,000,000,000,000.00"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_money_format_test, money_format_largeint) {
    auto func = FunctionMoneyFormat<MoneyFormatInt128Impl>::create();
    auto col = ColumnInt128::create();
    // INT128_MAX = 2^127 - 1
    Int128 int128_max = static_cast<Int128>((static_cast<unsigned __int128>(-1)) >> 1);
    std::vector<Int128> inputs = {0, 1234567890123, -98765432109876, int128_max};
    for (auto v : inputs) {
        col->insert_value(v);
    }
    auto results = run_money_format(func, std::move(col), std::make_shared<DataTypeInt128>());
    std::vector<std::string> expected = {"0.00", "1,234,567,890,123.00", "-98,765,432,109,876.00",
                                         "170,141,183,460,469,231,731,687,303,715,884,105,727.00"};
    ASSERT_EQ(results.size(), expected.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_EQ(results[i], expected[i]) << "row " << i;
    }
}

TEST(function_money_format_test, money_format_decimal32) {
    // scale == 2: no rounding branch
    check_money_format_decimal<TYPE_DECIMAL32>(
            9, 2, {123456, -123456, 12, -12, 0, 99},
            {"1,234.56", "-1,234.56", "0.12", "-0.12", "0.00", "0.99"});
    // scale < 2: multiply branch (and append-sign for -0.xx)
    check_money_format_decimal<TYPE_DECIMAL32>(9, 1, {123, -123, 5, -5, 0},
                                               {"12.30", "-12.30", "0.50", "-0.50", "0.00"});
    // scale == 0: integer-like values go through the scale < 2 branch
    check_money_format_decimal<TYPE_DECIMAL32>(9, 0, {123, -45, 0}, {"123.00", "-45.00", "0.00"});
    // scale > 2: rounding branch, including rounding carry
    check_money_format_decimal<TYPE_DECIMAL32>(9, 4, {123450, 123440, 999950, -123450, -999950},
                                               {"12.35", "12.34", "100.00", "-12.35", "-100.00"});
}

TEST(function_money_format_test, money_format_decimal64) {
    check_money_format_decimal<TYPE_DECIMAL64>(
            18, 4, {123456789, -123456789, 123456700, 123456789012},
            {"12,345.68", "-12,345.68", "12,345.67", "12,345,678.90"});
}

TEST(function_money_format_test, money_format_decimal128) {
    check_money_format_decimal<TYPE_DECIMAL128I>(
            38, 6, {Int128(123456789012345), Int128(-123456789012345), Int128(9999995)},
            {"123,456,789.01", "-123,456,789.01", "10.00"});
}

TEST(function_money_format_test, money_format_decimal256_unsupported) {
    // money_format does not support decimal256; it should throw on execution.
    auto func = FunctionMoneyFormat<MoneyFormatDecimalImpl<TYPE_DECIMAL256>>::create();
    auto col = ColumnDecimal256::create(0, 4);
    col->insert_value(Decimal256(Int128(12345)));
    auto type = std::make_shared<DataTypeDecimal256>(40, 4);

    std::unique_ptr<RuntimeState> runtime_state = std::make_unique<RuntimeState>();
    auto return_type = std::make_shared<DataTypeString>();
    std::vector<DataTypePtr> arg_types = {type};
    auto context = FunctionContext::create_context(runtime_state.get(), return_type, arg_types);

    Block block;
    block.insert({std::move(col), type, "arg"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
    ColumnNumbers arguments = {0};
    EXPECT_THROW({ static_cast<void>(func->execute_impl(context.get(), block, arguments, 1, 1)); },
                 doris::Exception);
}

TEST(function_money_format_test, money_format_wrong_argument_count) {
    auto func = FunctionMoneyFormat<MoneyFormatInt64Impl>::create();
    DataTypes args = {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt64>()};
    EXPECT_THROW({ static_cast<void>(func->get_return_type_impl(args)); }, doris::Exception);
}

}; // namespace doris
