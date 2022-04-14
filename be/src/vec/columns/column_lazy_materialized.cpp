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

#include "vec/columns/column_lazy_materialized.h"

namespace doris::vectorized {
ColumnLazyMaterialized::ColumnLazyMaterialized(const ColumnPtr& ref_column_, const RowIndicePtr& ref_row_indice_)
    : ref_column(ref_column_), ref_row_indice(ref_row_indice_) {

}
void ColumnLazyMaterialized::materialize() const {
    /*
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
    */
}
}