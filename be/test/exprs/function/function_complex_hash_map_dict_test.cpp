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

#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/hash_table/hash_key_type.h"
#include "exprs/function/complex_hash_map_dictionary.h"
#include "exprs/function/dictionary.h"
#include "exprs/function/function_test_util.h"

namespace doris {

template <typename DataType>
ColumnPtr create_column_with_data(std::vector<typename DataType::FieldType> datas) {
    auto column = DataType::ColumnType::create();
    if constexpr (std::is_same_v<DataType, DataTypeString>) {
        for (auto data : datas) {
            column->insert_data(data.data(), data.size());
        }
    } else {
        for (auto data : datas) {
            column->insert_value(data);
        }
    }
    return std::move(column);
}

template <typename DataType>
ColumnWithTypeAndName create_column_with_data_and_name(
        std::vector<typename DataType::FieldType> datas, std::string name) {
    auto column = create_column_with_data<DataType>(datas);
    return ColumnWithTypeAndName(std::move(column), std::make_shared<DataType>(), name);
}

void test_complex_hash_map_dict(ColumnsWithTypeAndName key_data, ColumnsWithTypeAndName values_data,
                                const std::string dict_name) {
    ColumnPtrs key_columns;
    DataTypes key_types;
    for (auto column : key_data) {
        auto key_column = column.column;
        auto add_column = column.type->create_column();
        add_column->insert_default();
        add_column->insert_range_from(*key_column, 0, key_column->size());
        key_columns.push_back(std::move(add_column));
        key_types.push_back(column.type);
    }

    std::vector<DictionaryAttribute> attributes;
    for (const auto& att : values_data) {
        // attributes do not handle nullable DataType
        attributes.push_back({att.name, remove_nullable(att.type)});
    }

    std::vector<std::string> attribute_names;
    DataTypes attribute_types;
    for (auto column : values_data) {
        attribute_names.push_back(column.name);
        attribute_types.push_back(column.type);
    }

    auto dict = create_complex_hash_map_dict_from_column("dict1", key_data, values_data);

    auto result = dict->get_tuple_columns(attribute_names, attribute_types, key_columns, key_types);

    const auto rows = result[0]->size();

    for (int j = 0; j < attribute_names.size(); j++) {
        std::cout << attribute_names[j] << "\t";
    }
    std::cout << std::endl;

    for (int i = 0; i < rows; i++) {
        for (int j = 0; j < attribute_types.size(); j++) {
            std::cout << attribute_types[j]->to_string(*remove_nullable(result[j]), i) << "\t";
        }
        std::cout << std::endl;
    }
}

TEST(ComplexHashMapDictTest, Test1) {
    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2, 3}, "key"),
                    create_column_with_data_and_name<DataTypeInt64>({1, 1, 3}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({11, 45, 14}, "value1"),
                    create_column_with_data_and_name<DataTypeInt64>({19, 19, 810}, "value2"),
                    create_column_with_data_and_name<DataTypeString>({"a", "b", "c"}, "value3"),
            },
            "dict1");

    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2, 3, 34}, "key"),
                    create_column_with_data_and_name<DataTypeInt64>({1, 1, 3, 1231231}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({11, 45, 14, 123123}, "value1"),
                    create_column_with_data_and_name<DataTypeInt64>({19, 19, 810, 32123213},
                                                                    "value2"),
                    create_column_with_data_and_name<DataTypeString>({"a", "b", "c", "sadawe"},
                                                                     "value3"),
            },
            "dict1");

    test_complex_hash_map_dict(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({1, 2}, "key"),
                    create_column_with_data_and_name<DataTypeString>({"abc", "ABC"}, "key"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeFloat32>({1, 2}, "value1"),
                    create_column_with_data_and_name<DataTypeString>({"def", "DEF"}, "value2")},
            "dict1");
}

// Build a dictionary from the given key/value columns, then look up the exact same key
// columns. Because every key was inserted, every lookup must hit, so each result value
// must equal the corresponding value column entry.
void check_complex_hash_map_dict_all_found(ColumnsWithTypeAndName key_data,
                                           ColumnsWithTypeAndName values_data) {
    auto dict = create_complex_hash_map_dict_from_column("dict_check", key_data, values_data);

    ColumnPtrs key_columns;
    DataTypes key_types;
    for (const auto& column : key_data) {
        key_columns.push_back(column.column);
        key_types.push_back(column.type);
    }

    std::vector<std::string> attribute_names;
    DataTypes attribute_types;
    for (const auto& column : values_data) {
        attribute_names.push_back(column.name);
        attribute_types.push_back(column.type);
    }

    auto result = dict->get_tuple_columns(attribute_names, attribute_types, key_columns, key_types);

    const auto rows = key_columns[0]->size();
    ASSERT_EQ(result.size(), values_data.size());
    for (size_t j = 0; j < values_data.size(); ++j) {
        ASSERT_EQ(result[j]->size(), rows);
        for (size_t i = 0; i < rows; ++i) {
            EXPECT_EQ(attribute_types[j]->to_string(*remove_nullable(result[j]), i),
                      attribute_types[j]->to_string(*values_data[j].column, i));
        }
    }
}

// Exercise the fixed-size multi-column key paths of DictionaryHashMapMethod::init by choosing
// key column combinations whose packed byte width selects each fixed64/72/96/104/128/136/256
// variant (see get_hash_key_type_fixed).
TEST(ComplexHashMapDictTest, FixedKeyWidths) {
    // key bytes = 1 + 8 = 9 -> fixed72
    check_complex_hash_map_dict_all_found(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt8>({1, 2, 3}, "k1"),
                    create_column_with_data_and_name<DataTypeInt64>({100, 200, 300}, "k2"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({10, 20, 30}, "v1"),
                    create_column_with_data_and_name<DataTypeString>({"a", "b", "c"}, "v2"),
            });

    // key bytes = 1 + 4 + 8 = 13 -> fixed104
    check_complex_hash_map_dict_all_found(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt8>({1, 2, 3}, "k1"),
                    create_column_with_data_and_name<DataTypeInt32>({10, 20, 30}, "k2"),
                    create_column_with_data_and_name<DataTypeInt64>({100, 200, 300}, "k3"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt64>({11, 22, 33}, "v1"),
            });

    // key bytes = 8 + 8 = 16 -> fixed128
    check_complex_hash_map_dict_all_found(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt64>({1, 2, 3}, "k1"),
                    create_column_with_data_and_name<DataTypeInt64>({1000000000000, 2, 3}, "k2"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({10, 20, 30}, "v1"),
            });

    // key bytes = 1 + 8 + 8 = 17 -> fixed136
    check_complex_hash_map_dict_all_found(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt8>({1, 2, 3}, "k1"),
                    create_column_with_data_and_name<DataTypeInt64>({11, 22, 33}, "k2"),
                    create_column_with_data_and_name<DataTypeInt64>({111, 222, 333}, "k3"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeString>({"x", "y", "z"}, "v1"),
            });

    // key bytes = 16 + 8 = 24 -> fixed256
    check_complex_hash_map_dict_all_found(
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt128>({1, 2, 3}, "k1"),
                    create_column_with_data_and_name<DataTypeInt64>({100, 200, 300}, "k2"),
            },
            ColumnsWithTypeAndName {
                    create_column_with_data_and_name<DataTypeInt32>({10, 20, 30}, "v1"),
            });
}

// Directly drive DictionaryHashMapMethod::init for the key types that are not reachable through
// the existing end-to-end tests, including int256_key and the invalid default branch.
TEST(ComplexHashMapDictTest, InitKeyTypeVariants) {
    DataTypes key_types = {std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeInt8>()};

    {
        DictionaryHashMapMethod method;
        method.init({}, HashKeyType::int256_key);
        EXPECT_FALSE(method.method_variant.valueless_by_exception());
        EXPECT_TRUE((std::holds_alternative<MethodOneNumber<UInt256, DictHashMap<UInt256>>>(
                method.method_variant)));
    }
    {
        DictionaryHashMapMethod method;
        method.init(key_types, HashKeyType::fixed72);
        EXPECT_TRUE((std::holds_alternative<MethodKeysFixed<DictHashMap<UInt72>>>(
                method.method_variant)));
    }
    {
        DictionaryHashMapMethod method;
        method.init(key_types, HashKeyType::fixed104);
        EXPECT_TRUE((std::holds_alternative<MethodKeysFixed<DictHashMap<UInt104>>>(
                method.method_variant)));
    }
    {
        DictionaryHashMapMethod method;
        method.init(key_types, HashKeyType::fixed128);
        EXPECT_TRUE((std::holds_alternative<MethodKeysFixed<DictHashMap<UInt128>>>(
                method.method_variant)));
    }
    {
        DictionaryHashMapMethod method;
        method.init(key_types, HashKeyType::fixed136);
        EXPECT_TRUE((std::holds_alternative<MethodKeysFixed<DictHashMap<UInt136>>>(
                method.method_variant)));
    }
    {
        DictionaryHashMapMethod method;
        method.init(key_types, HashKeyType::fixed256);
        EXPECT_TRUE((std::holds_alternative<MethodKeysFixed<DictHashMap<UInt256>>>(
                method.method_variant)));
    }
    {
        // without_key has no matching case, so init must fall through to the default branch
        // and throw.
        DictionaryHashMapMethod method;
        EXPECT_THROW(method.init({}, HashKeyType::without_key), doris::Exception);
    }
}

} // namespace doris
