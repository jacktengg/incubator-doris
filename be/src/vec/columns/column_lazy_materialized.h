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

#pragma once
#include "vec/columns/column.h"
#include "vec/columns/column_impl.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"


namespace doris::vectorized {
class ColumnLazyMaterialized final : public COWHelper<IColumn, ColumnLazyMaterialized> {
private:
    friend class COWHelper<IColumn, ColumnLazyMaterialized>;

    ColumnLazyMaterialized(const ColumnPtr& ref_column_, const RowIndicePtr& ref_row_indice_);
    ColumnLazyMaterialized(const ColumnLazyMaterialized&) = default;
public:
    using Base = COWHelper<IColumn, ColumnLazyMaterialized>;
    static Ptr create(const ColumnPtr& ref_column_, const RowIndicePtr& ref_row_indice_) {
        return ColumnLazyMaterialized::create(ref_column_, ref_row_indice_);
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }
    const char* get_family_name() const override {
        return "LazyMaterialized";
    }

    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override { return ref_row_indice->size(); }
    bool is_null_at(size_t n) const override;

    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) override;
    UInt64 get64(size_t n) const override;
    StringRef get_data_at(size_t n) const override;

    void insert_data(const char* pos, size_t length) override;


    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_indices_from(const IColumn& src, const int* indices_begin,
                             const int* indices_end) override;
    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;

    void insert_many_fix_len_data(const char* pos, size_t num) override;
    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t data_num, uint32_t dict_num) override;
    void insert_many_binary_data(char* data_array, uint32_t* len_array,
                                 uint32_t* start_offset_array, size_t num) override;
    void insert_default() override;
    void insert_many_defaults(size_t length) override;

    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override;
    ColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;
    void get_permutation(bool reverse, size_t limit, int null_direction_hint,
                         Permutation& res) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    void protect() override;
    ColumnPtr replicate(const Offsets& replicate_offsets) const override;
    void replicate(const uint32_t* counts, size_t target_size, IColumn& column) const override;
    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void get_extremes(Field& min, Field& max) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector& selector) const override;

    void for_each_subcolumn(ColumnCallback callback) override;

    bool structure_equals(const IColumn& rhs) const override;

    bool is_date_type() override;
    bool is_datetime_type() override;
    void set_date_type() override;
    void set_datetime_type() override;

    bool is_nullable() const override;
    bool is_bitmap() const override;
    bool is_column_decimal() const override;
    bool is_column_string() const override;
    bool is_fixed_and_contiguous() const override;
    bool values_have_fixed_size() const override;
    size_t size_of_value_if_fixed() const override;
    bool only_null() const override;

    void clear() override;
    bool has_null() const override;
    bool has_null(size_t size) const override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override;
    void replace_column_data_default(size_t self_row = 0) override;

    bool is_materialized() const override {
        return false;
    }
    void materialize() const override;
    void set_ref_column(const Ptr column) {
        ref_column = column;
    }
    const ColumnPtr& get_ref_column() const {
        return ref_column;
    }

    void set_ref_row_indice(const RowIndicePtr indice) {
        ref_row_indice = indice;
    }

    RowIndicePtr get_ref_row_indice() const {
        return ref_row_indice;
    }

    bool share_the_same_ref_column(const Ptr column) const {
        if (nullptr != column) {
            return ref_column.get() == column->ref_column.get();
        } else {
            return false;
        }
    }

private:
    WrappedPtr ref_column;
    RowIndicePtr ref_row_indice;
    WrappedPtr materialized_column;
};
}