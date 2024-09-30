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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeNothing.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_nothing_serde.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/** Data type that cannot have any values.
  * Used to represent NULL of unknown type as Nullable(Nothing),
  * and possibly for empty array of unknown type as Array(Nothing).
  */
class DataTypeNothing final : public IDataType {
public:
    static constexpr bool is_parametric = false;

    const char* get_family_name() const override { return "Nothing"; }
    TypeIndex get_type_id() const override { return TypeIndex::Nothing; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(INVALID_TYPE);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_NONE;
    }

    MutableColumnPtr create_column() const override;

    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return false; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return 0; }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override {
        return 0;
    }
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;

    [[noreturn]] Field get_default() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Method get_default() is not implemented for data type {}.",
                               get_name());
        __builtin_unreachable();
    }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Unimplemented get_field for Nothing");
        __builtin_unreachable();
    }

    bool have_subtypes() const override { return false; }
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeNothingSerde>();
    };
};

} // namespace doris::vectorized
