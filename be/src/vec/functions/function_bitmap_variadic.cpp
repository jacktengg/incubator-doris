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

#include "vec/columns/column_complex.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_string.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

#define BITMAP_FUNCTION_VARIADIC(CLASS, FUNCTION_NAME, OP)                                         \
    struct CLASS {                                                                                 \
        static constexpr auto name = #FUNCTION_NAME;                                               \
        using ResultDataType = DataTypeBitMap;                                                     \
        static Status vector_vector(ColumnPtr argument_columns[], size_t col_size,                 \
                                    size_t input_rows_count, std::vector<BitmapValue>& res,        \
                                    IColumn* res_nulls) {                                          \
            const ColumnUInt8::value_type* null_map_datas[col_size] = {nullptr};                   \
            int nullable_cols_count = 0;                                                           \
            ColumnUInt8::value_type* __restrict res_nulls_data = nullptr;                          \
            if (res_nulls) {                                                                       \
                res_nulls_data = assert_cast<ColumnUInt8*>(res_nulls)->get_data().data();          \
            }                                                                                      \
            const ColumnBitmap::Container* data_ptr;                                               \
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[0])) {     \
                const auto& nested_col_ptr = nullable->get_nested_column_ptr();                    \
                null_map_datas[nullable_cols_count++] = nullable->get_null_map_data().data();      \
                data_ptr = &(assert_cast<const ColumnBitmap*>(nested_col_ptr.get())->get_data());  \
            } else {                                                                               \
                data_ptr = &(                                                                      \
                        assert_cast<const ColumnBitmap*>(argument_columns[0].get())->get_data());  \
            }                                                                                      \
            const auto& mid_data = *data_ptr;                                                      \
            for (size_t row = 0; row < input_rows_count; ++row) {                                  \
                res[row] = mid_data[row];                                                          \
            }                                                                                      \
            for (size_t col = 1; col < col_size; ++col) {                                          \
                if (auto* nullable =                                                               \
                            check_and_get_column<ColumnNullable>(*argument_columns[col])) {        \
                    const auto& nested_col_ptr = nullable->get_nested_column_ptr();                \
                    null_map_datas[nullable_cols_count++] = nullable->get_null_map_data().data();  \
                    data_ptr =                                                                     \
                            &(assert_cast<const ColumnBitmap*>(nested_col_ptr.get())->get_data()); \
                } else {                                                                           \
                    data_ptr = &(assert_cast<const ColumnBitmap*>(argument_columns[col].get())     \
                                         ->get_data());                                            \
                }                                                                                  \
                const auto& col_data = *data_ptr;                                                  \
                for (size_t row = 0; row < input_rows_count; ++row) {                              \
                    res[row] OP col_data[row];                                                     \
                }                                                                                  \
            }                                                                                      \
            if (res_nulls_data) {                                                                  \
                if (nullable_cols_count == col_size) {                                             \
                    const auto* null_map_data = null_map_datas[0];                                 \
                    for (size_t row = 0; row < input_rows_count; ++row) {                          \
                        res_nulls_data[row] = null_map_data[row];                                  \
                    }                                                                              \
                    for (int i = 1; i < nullable_cols_count; ++i) {                                \
                        const auto* null_map_data = null_map_datas[i];                             \
                        for (size_t row = 0; row < input_rows_count; ++row) {                      \
                            res_nulls_data[row] &= null_map_data[row];                             \
                        }                                                                          \
                    }                                                                              \
                } else {                                                                           \
                    for (size_t row = 0; row < input_rows_count; ++row) {                          \
                        res_nulls_data[row] = 0;                                                   \
                    }                                                                              \
                }                                                                                  \
            }                                                                                      \
            return Status::OK();                                                                   \
        }                                                                                          \
    }

#define BITMAP_FUNCTION_COUNT_VARIADIC(CLASS, FUNCTION_NAME, OP)                                   \
    struct CLASS {                                                                                 \
        static constexpr auto name = #FUNCTION_NAME;                                               \
        using ResultDataType = DataTypeInt64;                                                      \
        using TData = std::vector<BitmapValue>;                                                    \
        using ResTData = typename ColumnVector<Int64>::Container;                                  \
        static Status vector_vector(ColumnPtr argument_columns[], size_t col_size,                 \
                                    size_t input_rows_count, ResTData& res, IColumn* res_nulls) {  \
            TData vals;                                                                            \
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[0])) {     \
                const auto& nested_col_ptr = nullable->get_nested_column_ptr();                    \
                vals = assert_cast<const ColumnBitmap*>(nested_col_ptr.get())->get_data();         \
            } else {                                                                               \
                vals = assert_cast<const ColumnBitmap*>(argument_columns[0].get())->get_data();    \
            }                                                                                      \
            for (size_t col = 1; col < col_size; ++col) {                                          \
                const ColumnBitmap::Container* col_data_ptr;                                       \
                if (auto* nullable =                                                               \
                            check_and_get_column<ColumnNullable>(*argument_columns[col])) {        \
                    const auto& nested_col_ptr = nullable->get_nested_column_ptr();                \
                    col_data_ptr =                                                                 \
                            &(assert_cast<const ColumnBitmap*>(nested_col_ptr.get())->get_data()); \
                } else {                                                                           \
                    col_data_ptr = &(assert_cast<const ColumnBitmap*>(argument_columns[col].get()) \
                                             ->get_data());                                        \
                }                                                                                  \
                const auto& col_data = *col_data_ptr;                                              \
                for (size_t row = 0; row < input_rows_count; ++row) {                              \
                    vals[row] OP col_data[row];                                                    \
                }                                                                                  \
            }                                                                                      \
            for (size_t row = 0; row < input_rows_count; ++row) {                                  \
                res[row] = vals[row].cardinality();                                                \
            }                                                                                      \
            return Status::OK();                                                                   \
        }                                                                                          \
    }

BITMAP_FUNCTION_VARIADIC(BitmapOr, bitmap_or, |=);
BITMAP_FUNCTION_VARIADIC(BitmapAnd, bitmap_and, &=);
BITMAP_FUNCTION_VARIADIC(BitmapXor, bitmap_xor, ^=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapOrCount, bitmap_or_count, |=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapAndCount, bitmap_and_count, &=);
BITMAP_FUNCTION_COUNT_VARIADIC(BitmapXorCount, bitmap_xor_count, ^=);

template <typename Impl>
class FunctionBitMapVariadic : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionBitMapVariadic>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        using ResultDataType = typename Impl::ResultDataType;
        if (std::is_same_v<Impl, BitmapOr>) {
            bool has_nullable_arguments = false;
            for (size_t i = 0; i < arguments.size(); ++i) {
                if (arguments[i]->is_nullable()) {
                    has_nullable_arguments = true;
                    break;
                }
            }
            auto result_type = std::make_shared<ResultDataType>();
            return has_nullable_arguments ? make_nullable(result_type) : result_type;
        } else {
            return std::make_shared<ResultDataType>();
        }
    }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override {
        if (std::is_same_v<Impl, BitmapOr> || std::is_same_v<Impl, BitmapOrCount>) {
            return false;
        } else {
            return true;
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        size_t argument_size = arguments.size();
        ColumnPtr argument_columns[argument_size];

        for (size_t i = 0; i < argument_size; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
        }

        using ResultDataType = typename Impl::ResultDataType;  //DataTypeBitMap or DataTypeInt64
        using ResultType = typename ResultDataType::FieldType; //BitmapValue or Int64
        using ColVecResult =
                std::conditional_t<is_complex_v<ResultType>, ColumnComplexType<ResultType>,
                                   ColumnVector<ResultType>>;
        typename ColVecResult::MutablePtr col_res = nullptr;

        typename ColumnUInt8::MutablePtr col_res_nulls;
        auto& result_info = block.get_by_position(result);
        // special case for bitmap_or
        if (!use_default_implementation_for_nulls() && result_info.type->is_nullable()) {
            col_res_nulls = ColumnUInt8::create(input_rows_count);
        }

        col_res = ColVecResult::create();

        auto& vec_res = col_res->get_data();
        vec_res.resize(input_rows_count);

        Impl::vector_vector(argument_columns, argument_size, input_rows_count, vec_res,
                            col_res_nulls);
        if (!use_default_implementation_for_nulls() && result_info.type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(col_res), std::move(col_res_nulls)));
        } else {
            block.replace_by_position(result, std::move(col_res));
        }
        return Status::OK();
    }
};

using FunctionBitmapOr = FunctionBitMapVariadic<BitmapOr>;
using FunctionBitmapXor = FunctionBitMapVariadic<BitmapXor>;
using FunctionBitmapAnd = FunctionBitMapVariadic<BitmapAnd>;
using FunctionBitmapOrCount = FunctionBitMapVariadic<BitmapOrCount>;
using FunctionBitmapAndCount = FunctionBitMapVariadic<BitmapAndCount>;
using FunctionBitmapXorCount = FunctionBitMapVariadic<BitmapXorCount>;

void register_function_bitmap_variadic(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionBitmapOr>();
    factory.register_function<FunctionBitmapXor>();
    factory.register_function<FunctionBitmapAnd>();
    factory.register_function<FunctionBitmapOrCount>();
    factory.register_function<FunctionBitmapAndCount>();
    factory.register_function<FunctionBitmapXorCount>();
}
} // namespace doris::vectorized