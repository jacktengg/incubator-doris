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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHash.cpp
// and modified by Doris

#include "vec/functions/function_hash.h"

#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "common/status.h"
#include "olap/olap_common.h"
#include "util/bit_util.h"
#include "util/hash_util.hpp"
#include "util/murmur_hash3.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/sip_hash.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/function_variadic_arguments.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/template_helpers.hpp"

namespace doris::vectorized {
constexpr uint64_t emtpy_value = 0xe28dbde7fe22e41c;

template <typename ReturnType>
struct MurmurHash3ImplName {};

template <>
struct MurmurHash3ImplName<Int32> {
    static constexpr auto name = "murmur_hash3_32";
};

template <>
struct MurmurHash3ImplName<Int64> {
    static constexpr auto name = "murmur_hash3_64";
};

template <typename ReturnType>
struct MurmurHash3Impl {
    static constexpr auto name = MurmurHash3ImplName<ReturnType>::name;

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        ColumnVector<ReturnType>& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        vec_to.get_data().assign(input_rows_count, static_cast<ReturnType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        auto* col_to_data = assert_cast<ColumnVector<ReturnType>&>(col_to).get_data().data();
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                if (first) {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        UInt32 val = HashUtil::murmur_hash3_32(
                                reinterpret_cast<const char*>(&data[current_offset]),
                                offsets[i] - current_offset, HashUtil::MURMUR3_32_SEED);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    } else {
                        UInt64 val = 0;
                        murmur_hash3_x64_64(reinterpret_cast<const char*>(&data[current_offset]),
                                            offsets[i] - current_offset, 0, &val);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    }
                } else {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        col_to_data[i] = HashUtil::murmur_hash3_32(
                                reinterpret_cast<const char*>(&data[current_offset]),
                                offsets[i] - current_offset,
                                assert_cast<ColumnInt32&>(col_to).get_data()[i]);
                    } else {
                        murmur_hash3_x64_64(reinterpret_cast<const char*>(&data[current_offset]),
                                            offsets[i] - current_offset,
                                            assert_cast<ColumnInt64&>(col_to).get_data()[i],
                                            col_to_data + i);
                    }
                }
                current_offset = offsets[i];
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<String>();
            for (size_t i = 0; i < input_rows_count; ++i) {
                if (first) {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        UInt32 val = HashUtil::murmur_hash3_32(value.data(), value.size(),
                                                               HashUtil::MURMUR3_32_SEED);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    } else {
                        UInt64 val = 0;
                        murmur_hash3_x64_64(value.data(), value.size(), 0, &val);
                        col_to.insert_data(const_cast<const char*>(reinterpret_cast<char*>(&val)),
                                           0);
                    }
                } else {
                    if constexpr (std::is_same_v<ReturnType, Int32>) {
                        col_to_data[i] = HashUtil::murmur_hash3_32(
                                value.data(), value.size(),
                                assert_cast<ColumnInt32&>(col_to).get_data()[i]);
                    } else {
                        murmur_hash3_x64_64(value.data(), value.size(),
                                            assert_cast<ColumnInt64&>(col_to).get_data()[i],
                                            col_to_data + i);
                    }
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }
};
using FunctionMurmurHash3_32 = FunctionVariadicArgumentsBase<DataTypeInt32, MurmurHash3Impl<Int32>>;
using FunctionMurmurHash3_64 = FunctionVariadicArgumentsBase<DataTypeInt64, MurmurHash3Impl<Int64>>;

template <bool is_reference = false>
struct SipHash128ImplName {
    static constexpr auto name = "siphash128";
};
template <>
struct SipHash128ImplName<true> {
    static constexpr auto name = "siphash128reference";
};

template <std::endian ToEndian, std::endian FromEndian = std::endian::native, typename T>
    requires std::is_integral_v<T> || std::is_same_v<T, int128_t> || std::is_same_v<T, uint128_t>
inline void transform_endianness(T& value) {
    if constexpr (ToEndian != FromEndian) {
        value = BitUtil::byteswap(value);
    }
}

struct SipHash128Impl {
    static void hash(const char* data, const size_t size, char* out) {
        sip_hash128(data, size, out);
    }
};
struct SipHash128ReferenceImpl {
    static void hash(const char* data, const size_t size, char* out) {
        sip_hash128_reference(data, size, out);
    }
};

template <bool is_reference>
struct SipHash128 {
    static constexpr auto name = SipHash128ImplName<is_reference>::name;
    using Impl = std::conditional_t<is_reference, SipHash128ReferenceImpl, SipHash128Impl>;

    static Status empty_apply(IColumn& icolumn, size_t input_rows_count) {
        // ColumnVector<ReturnType>& vec_to = assert_cast<ColumnVector<ReturnType>&>(icolumn);
        // vec_to.get_data().assign(input_rows_count, static_cast<ReturnType>(emtpy_value));
        return Status::OK();
    }

    static Status first_apply(const IDataType* type, const IColumn* column, size_t input_rows_count,
                              IColumn& icolumn) {
        return execute<true>(type, column, input_rows_count, icolumn);
    }

    static Status combine_apply(const IDataType* type, const IColumn* column,
                                size_t input_rows_count, IColumn& icolumn) {
        return execute<false>(type, column, input_rows_count, icolumn);
    }

    template <bool first>
    static Status execute(const IDataType* type, const IColumn* column, size_t input_rows_count,
                          IColumn& col_to) {
        WhichDataType which(type);
        if (which.is_int8()) {
            return execute_number<Int8, first>(column, input_rows_count, col_to);
        } else if (which.is_int16()) {
            return execute_number<Int16, first>(column, input_rows_count, col_to);
        } else if (which.is_int32()) {
            return execute_number<Int32, first>(column, input_rows_count, col_to);
        } else if (which.is_int64()) {
            return execute_number<Int64, first>(column, input_rows_count, col_to);
        } else if (which.is_int128()) {
            return execute_number<Int128, first>(column, input_rows_count, col_to);
        } else if (which.is_float32()) {
            return execute_number<Float32, first>(column, input_rows_count, col_to);
        } else if (which.is_float64()) {
            return execute_number<Float64, first>(column, input_rows_count, col_to);
        } else if (which.is_date()) {
            return execute_number<Int64, first>(column, input_rows_count, col_to);
        } else if (which.is_date_v2()) {
            return execute_number<UInt32, first>(column, input_rows_count, col_to);
        } else if (which.is_date_time()) {
            return execute_number<Int64, first>(column, input_rows_count, col_to);
        } else if (which.is_date_time_v2()) {
            return execute_number<UInt64, first>(column, input_rows_count, col_to);
        } else if (which.is_decimal128v2()) {
            return execute_number<Decimal128V2, first>(column, input_rows_count, col_to);
        } else if (which.is_decimal32()) {
            return execute_number<Decimal32, first>(column, input_rows_count, col_to);
        } else if (which.is_decimal64()) {
            return execute_number<Decimal64, first>(column, input_rows_count, col_to);
        } else if (which.is_decimal128v3()) {
            return execute_number<Decimal128V3, first>(column, input_rows_count, col_to);
        } else if (which.is_decimal256()) {
            return execute_number<Decimal256, first>(column, input_rows_count, col_to);
        } else if (which.is_string_or_fixed_string()) {
            return execute_string<first>(column, input_rows_count, col_to);
        } else {
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
    }

    template <typename FromType, bool first>
    static Status execute_number(const IColumn* column, size_t input_rows_count, IColumn& col_to) {
        auto& col_to_str = assert_cast<ColumnString&>(col_to);
        auto& col_to_chars = col_to_str.get_chars();
        auto& col_to_offsets = col_to_str.get_offsets();
        constexpr bool need_transform_endian =
                (std::endian::native == std::endian::big && sizeof(FromType) > 1);
        using ColVecType = ColumnVectorOrDecimal<FromType>;
        if (const auto* col_from = check_and_get_column<ColVecType>(column)) {
            LOG(WARNING) << "siphash128 vector";
            if constexpr (first) {
                const size_t chars_total_size = sizeof(uint128_t) * input_rows_count;
                ColumnString::check_chars_length(chars_total_size, input_rows_count);
                col_to_chars.resize(chars_total_size);
                col_to_offsets.resize(input_rows_count);
            };
            const auto* from_data = col_from->get_data().data();
            auto* col_to_chars_ptr = col_to_chars.data();
            size_t dst_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i) {
                uint128_t hash_value;
                if constexpr (need_transform_endian) {
                    auto from_value = from_data[i];
                    transform_endianness<std::endian::little>(from_value);
                    Impl::hash(reinterpret_cast<const char*>(&from_value), sizeof(FromType),
                               reinterpret_cast<char*>(&hash_value));
                } else {
                    Impl::hash(reinterpret_cast<const char*>(from_data + i), sizeof(FromType),
                               reinterpret_cast<char*>(&hash_value));
                }
                if (first) {
                    memcpy(col_to_chars_ptr + dst_offset, reinterpret_cast<char*>(&hash_value),
                           sizeof(hash_value));
                } else {
                    auto* old_hash_value = col_to_chars_ptr + dst_offset;
                    combine_hashes_func(*(reinterpret_cast<const uint128_t*>(old_hash_value)),
                                        hash_value, (char*)&old_hash_value);
                }
                dst_offset += sizeof(hash_value);
            }
            if (first) {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    col_to_offsets[i] = col_to_offsets[i - 1] + sizeof(uint128_t);
                }
            }
        } else if (const auto* col_from_const = check_and_get_column<ColumnConst>(column)) {
            LOG(WARNING) << "siphash128 const";
            auto value = col_from_const->get_value<FromType>();
            uint128_t hash_value;
            Impl::hash(reinterpret_cast<const char*>(&value), sizeof(value),
                       reinterpret_cast<char*>(&hash_value));

            auto* col_to_chars_ptr = col_to_chars.data();
            size_t dst_offset = 0;
            if constexpr (first) {
                const size_t chars_total_size = sizeof(uint128_t) * input_rows_count;
                ColumnString::check_chars_length(chars_total_size, input_rows_count);
                col_to_chars.resize(chars_total_size);
                col_to_offsets.resize(input_rows_count);
                for (size_t i = 0; i < input_rows_count; ++i) {
                    memcpy(col_to_chars_ptr + dst_offset, reinterpret_cast<char*>(&hash_value),
                           sizeof(hash_value));
                    dst_offset += sizeof(uint128_t);
                }
                for (size_t i = 0; i < input_rows_count; ++i) {
                    col_to_offsets[i] = col_to_offsets[i - 1] + sizeof(uint128_t);
                }
            } else {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto* old_hash_value = col_to_chars_ptr + dst_offset;
                    combine_hashes_func(*(reinterpret_cast<const uint128_t*>(old_hash_value)),
                                        hash_value, (char*)old_hash_value);
                    dst_offset += sizeof(uint128_t);
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }

    template <bool first>
    static Status execute_string(const IColumn* column, size_t input_rows_count, IColumn& col_to) {
        auto& col_to_str = assert_cast<ColumnString&>(col_to);
        auto& col_to_chars = col_to_str.get_chars();
        auto& col_to_offsets = col_to_str.get_offsets();
        if (const auto* col_from = check_and_get_column<ColumnString>(column)) {
            const typename ColumnString::Chars& data = col_from->get_chars();
            const typename ColumnString::Offsets& offsets = col_from->get_offsets();
            size_t size = offsets.size();

            if constexpr (first) {
                const size_t chars_total_size = sizeof(uint128_t) * size;
                ColumnString::check_chars_length(chars_total_size, size);
                col_to_chars.resize(chars_total_size);
                col_to_offsets.resize(size);
            }
            auto* col_to_chars_ptr = col_to_chars.data();
            ColumnString::Offset current_offset = 0;
            size_t dst_offset = 0;
            for (size_t i = 0; i < size; ++i) {
                uint128_t hash_value;
                Impl::hash(reinterpret_cast<const char*>(&data[current_offset]),
                           offsets[i] - current_offset, reinterpret_cast<char*>(&hash_value));
                if (first) {
                    memcpy(col_to_chars_ptr + dst_offset, reinterpret_cast<char*>(&hash_value),
                           sizeof(hash_value));
                } else {
                    auto* old_hash_value = col_to_chars_ptr + dst_offset;
                    combine_hashes_func(*(reinterpret_cast<const uint128_t*>(old_hash_value)),
                                        hash_value, (char*)old_hash_value);
                }
                current_offset = offsets[i];
                dst_offset += sizeof(uint128_t);
            }
            if (first) {
                for (size_t i = 0; i < size; ++i) {
                    col_to_offsets[i] = col_to_offsets[i - 1] + sizeof(uint128_t);
                }
            }
        } else if (const ColumnConst* col_from_const =
                           check_and_get_column_const_string_or_fixedstring(column)) {
            auto value = col_from_const->get_value<String>();
            uint128_t hash_value;
            Impl::hash(value.data(), value.size(), reinterpret_cast<char*>(&hash_value));

            auto* col_to_chars_ptr = col_to_chars.data();
            size_t dst_offset = 0;
            if constexpr (first) {
                const size_t chars_total_size = sizeof(uint128_t) * input_rows_count;
                ColumnString::check_chars_length(chars_total_size, input_rows_count);
                col_to_chars.resize(chars_total_size);
                col_to_offsets.resize(input_rows_count);
                for (size_t i = 0; i < input_rows_count; ++i) {
                    memcpy(col_to_chars_ptr + dst_offset, reinterpret_cast<char*>(&hash_value),
                           sizeof(hash_value));
                    dst_offset += sizeof(uint128_t);
                }
                for (size_t i = 0; i < input_rows_count; ++i) {
                    col_to_offsets[i] = col_to_offsets[i - 1] + sizeof(uint128_t);
                }
            } else {
                for (size_t i = 0; i < input_rows_count; ++i) {
                    auto* old_hash_value = col_to_chars_ptr + dst_offset;
                    combine_hashes_func(*(reinterpret_cast<const uint128_t*>(old_hash_value)),
                                        hash_value, (char*)old_hash_value);
                    dst_offset += sizeof(uint128_t);
                }
            }
        } else {
            DCHECK(false);
            return Status::NotSupported("Illegal column {} of argument of function {}",
                                        column->get_name(), name);
        }
        return Status::OK();
    }

    static void combine_hashes_func(uint128_t t1, uint128_t t2, char* out) {
        transform_endianness<std::endian::little>(t1);
        transform_endianness<std::endian::little>(t2);
        const uint128_t hashes[] {t1, t2};
        Impl::hash(reinterpret_cast<const char*>(hashes), sizeof(hashes), out);
    }
};

using FunctionSipHash128 = FunctionVariadicArgumentsBase<DataTypeString, SipHash128<false>>;
using FunctionSipHash128Reference = FunctionVariadicArgumentsBase<DataTypeString, SipHash128<true>>;

void register_function_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMurmurHash3_32>();
    factory.register_function<FunctionMurmurHash3_64>();
    factory.register_function<FunctionSipHash128>();
    factory.register_function<FunctionSipHash128Reference>();
}
} // namespace doris::vectorized