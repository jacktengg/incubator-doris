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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/IColumn.cpp
// and modified by Doris

#include "vec/columns/column.h"

#include <sstream>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"

namespace doris::vectorized {

void dump_indices(const IndiceArrayPtr indice_array, int count = 100) {
    int n = count;
    if (n > indice_array->size()) {
        n = indice_array->size();
    }
    std::cout << "dump indices:\n";
    for (int i = 0; i < n; ++i) {
        std::cout << (*indice_array)[i] << ", ";
        if (i > 0 && 0 == (i + 1) % 8) std::cout << "\n";
    }
    std::cout << "\n";
}

void dump_indices(const int* indices_begin, const int* indices_end) {
    auto count = indices_end - indices_begin;
    std::cout << "dump indices2:\n";
    for (int i = 0; i < count; ++i) {
        std::cout << indices_begin[i] << ", ";
        if (i > 0 && 0 == (i + 1) % 8) std::cout << "\n";
    }
    std::cout << "\n";
}

void RowIndice::add_indice(IndiceArrayPtr indice_array) {
    _total_rows += indice_array->size();
    _indices.emplace_back(indice_array);
    _indice_prefix_sum.push_back(_total_rows);
}

IndiceArrayPtr RowIndice::get_indices() {
    if (_indices.size() == 1) {
        return _indices[0];
    }
    std::cout << "RowIndice::get_indices, more than one indice array\n";
    auto indice_array = std::make_shared<IndiceArray>();
    indice_array->resize(_total_rows);
    auto* p = indice_array->data();
    for (auto& indice : _indices) {
        auto item_size = sizeof(IndiceArray::value_type);
        memcpy(p, indice->data(), indice->size() * item_size);
        p += indice->size();
    }
    _indices.clear();
    _indices.emplace_back(indice_array);

    _indice_prefix_sum.clear();
    _indice_prefix_sum.push_back(0);
    _indice_prefix_sum.push_back(_total_rows);

    return indice_array;
}

void IColumn::materialize() {
    if(is_materialized()) {
        return;
    }
    const auto& ref_row_indices_array = ref_row_indice->get_indices_array();
    auto& column = *ref_column;
    for (auto& indice_array : ref_row_indices_array) {
        // for (auto index : *indice_array) {
        //     insert_from(column, index);
        // }
        // dump_indices(indice_array, 512);
        // auto size = std::min(indice_array->size(), (size_t)512);
        // dump_indices(indice_array->data(), indice_array->data() + size);

        insert_indices_from(column, indice_array->data(), indice_array->data() + indice_array->size());
    }
    ref_column = nullptr;
    ref_row_indice = nullptr;
}

MutableColumnPtr IColumn::make_unmaterialized_column(const Ptr column, const RowIndicePtr row_indice) {
    assert(column->size() > 0);
    auto new_column = column->clone_empty();
    new_column->ref_column = column;
    new_column->ref_row_indice = row_indice;
    return new_column;
}

std::string IColumn::dump_structure() const {
    std::stringstream res;
    res << get_family_name() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr& subcolumn) {
        res << ", " << subcolumn->dump_structure();
    };

    const_cast<IColumn*>(this)->for_each_subcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insert_from(const IColumn& src, size_t n) {
    insert(src[n]);
}

bool is_column_nullable(const IColumn& column) {
    return check_column<ColumnNullable>(column);
}

bool is_column_const(const IColumn& column) {
    return check_column<ColumnConst>(column);
}
} // namespace doris::vectorized
