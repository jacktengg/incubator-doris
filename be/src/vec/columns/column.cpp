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

void RowIndice::add_indice(IndiceArrayPtr indice_array) {
    _total_rows += indice_array->size();
    _indices.emplace_back(indice_array);
    _indice_prefix_sum.push_back(_total_rows);
}

IndiceArrayPtr RowIndice::get_indices() {
    if (_indices.size() == 1) {
        return _indices[0];
    }
    auto indice_array = std::make_shared<IndiceArray>();
    indice_array->resize(_total_rows);
    auto* p = indice_array->data();
    const auto item_size = sizeof(IndiceArray::value_type);
    for (auto& indice : _indices) {
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

RowIndicePtr RowIndice::clone(size_t size) {
    auto res = std::make_shared<RowIndice>();
    if (0 == size) {
        return res;
    }
    if (size > this->size()) {
        LOG(FATAL) << fmt::format(
                "row indices clone count = {} is bigger than indice count = {}",
                size, this->size());
    }
    auto array_ptr = std::make_shared<IndiceArray>(size);
    size_t count_copied = 0;
    for (const auto& indice_array : _indices) {
        size_t count_to_copy = std::min(size - count_copied, indice_array->size());
        memcpy(array_ptr->data() + count_copied, indice_array->data(), count_to_copy * sizeof(RowIndiceType));
        count_copied += count_to_copy;
        if (count_copied >= size) {
            break;
        }
    }
    res->add_indice(array_ptr);
    return res; 
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
