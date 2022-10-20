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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/TwoLevelHashTable.h
// and modified by Doris
#pragma once

#include "vec/common/hash_table/hash_table.h"

/** Two-level hash table.
  * Represents 256 (or 1ULL << BITS_FOR_BUCKET) small hash tables (buckets of the first level).
  * To determine which one to use, one of the bytes of the hash function is taken.
  *
  * Usually works a little slower than a simple hash table.
  * However, it has advantages in some cases:
  * - if you need to merge two hash tables together, then you can easily parallelize it by buckets;
  * - delay during resizes is amortized, since the small hash tables will be resized separately;
  * - in theory, resizes are cache-local in a larger range of sizes.
  */

template <size_t initial_size_degree = 8>
struct TwoLevelHashTableGrower : public HashTableGrowerWithPrecalculation<initial_size_degree> {
    /// Increase the size of the hash table.
    void increase_size() { this->increase_size_degree(this->size_degree() >= 15 ? 1 : 2); }
};

template <typename Key, typename Cell, typename Hash, typename Grower, typename Allocator,
          typename ImplTable = HashTable<Key, Cell, Hash, Grower, Allocator>,
          size_t BITS_FOR_BUCKET = 4>
class TwoLevelHashTable : private boost::noncopyable,
                          protected Hash /// empty base optimization
{
protected:
    friend class const_iterator;
    friend class iterator;

    using HashValue = size_t;
    using Self = TwoLevelHashTable;

public:
    using Impl = ImplTable;

    static constexpr size_t NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr size_t MAX_BUCKET = NUM_BUCKETS - 1;

    //factor that will trigger growing the hash table on insert.
    static constexpr float MAX_BUCKET_OCCUPANCY_FRACTION = 0.5f;

    size_t hash(const Key& x) const { return Hash::operator()(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) {
        return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET;
    }

    float get_factor() const { return MAX_BUCKET_OCCUPANCY_FRACTION; }

    bool should_be_shrink(int64_t valid_row) { return false; }

    void init_buf_size(size_t reserve_for_num_elements) {}

    void delete_zero_key(Key key) {}

    size_t get_buffer_size_in_bytes() const {
        size_t buff_size = 0;
        for (const auto& impl : impls) buff_size += impl.get_buffer_size_in_bytes();
        return buff_size;
    }

    size_t get_buffer_size_in_cells() const {
        size_t buff_size = 0;
        for (const auto& impl : impls) buff_size += impl.get_buffer_size_in_cells();
        return buff_size;
    }

    size_t* get_buffer_sizes_in_cells(size_t& num_buckets) const {
        num_buckets = NUM_BUCKETS;
        size_t* sizes = new size_t[NUM_BUCKETS];
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            sizes[i] = impls[i].get_buffer_size_in_cells();
        }
        return sizes;
    }

    void reset_resize_timer() {
        for (auto& impl : impls) {
            impl.reset_resize_timer();
        }
    }
    int64_t get_resize_timer_value() const {
        int64_t resize_timer_ns = 0;
        for (const auto& impl : impls) {
            resize_timer_ns += impl.get_resize_timer_value();
        }
        return resize_timer_ns;
    }

protected:
    typename Impl::iterator beginOfNextNonEmptyBucket(size_t& bucket) {
        while (bucket != NUM_BUCKETS && impls[bucket].empty()) ++bucket;

        if (bucket != NUM_BUCKETS) return impls[bucket].begin();

        --bucket;
        return impls[MAX_BUCKET].end();
    }

    typename Impl::const_iterator beginOfNextNonEmptyBucket(size_t& bucket) const {
        while (bucket != NUM_BUCKETS && impls[bucket].empty()) ++bucket;

        if (bucket != NUM_BUCKETS) return impls[bucket].begin();

        --bucket;
        return impls[MAX_BUCKET].end();
    }

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];

    TwoLevelHashTable() = default;

    explicit TwoLevelHashTable(size_t size_hint) {
        for (auto& impl : impls) impl.reserve(size_hint / NUM_BUCKETS);
    }

    /// Copy the data from another (normal) hash table. It should have the same hash function.
    template <typename Source>
    explicit TwoLevelHashTable(const Source& src) {
        typename Source::const_iterator it = src.begin();

        /// It is assumed that the zero key (stored separately) is first in iteration order.
        if (it != src.end() && it.get_ptr()->is_zero(src)) {
            insert(it->get_value());
            ++it;
        }

        for (; it != src.end(); ++it) {
            const Cell* cell = it.get_ptr();
            size_t hash_value = cell->get_hash(src);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].insert_unique_non_zero(cell, hash_value);
        }
    }

    TwoLevelHashTable(TwoLevelHashTable&& rhs) {
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            impls[i] = std::move(rhs.impls[i]);
        }
    }

    TwoLevelHashTable& operator=(TwoLevelHashTable&& rhs) {
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            impls[i] = std::move(rhs.impls[i]);
        }
        return *this;
    }

    class iterator /// NOLINT
    {
        Self* container {};
        size_t bucket {};
        typename Impl::iterator current_it {};

        friend class TwoLevelHashTable;

        iterator(Self* container_, size_t bucket_, typename Impl::iterator current_it_)
                : container(container_), bucket(bucket_), current_it(current_it_) {}

    public:
        iterator() = default;

        bool operator==(const iterator& rhs) const {
            return bucket == rhs.bucket && current_it == rhs.current_it;
        }
        bool operator!=(const iterator& rhs) const { return !(*this == rhs); }

        iterator& operator++() {
            ++current_it;
            if (current_it == container->impls[bucket].end()) {
                ++bucket;
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        Cell& operator*() const { return *current_it; }
        Cell* operator->() const { return current_it.get_ptr(); }

        Cell* get_ptr() const { return current_it.get_ptr(); }
        size_t get_hash() const { return current_it.get_hash(); }
    };

    class const_iterator /// NOLINT
    {
        Self* container {};
        size_t bucket {};
        typename Impl::const_iterator current_it {};

        friend class TwoLevelHashTable;

        const_iterator(Self* container_, size_t bucket_, typename Impl::const_iterator current_it_)
                : container(container_), bucket(bucket_), current_it(current_it_) {}

    public:
        const_iterator() = default;
        const_iterator(const iterator& rhs)
                : container(rhs.container),
                  bucket(rhs.bucket),
                  current_it(rhs.current_it) {} /// NOLINT

        bool operator==(const const_iterator& rhs) const {
            return bucket == rhs.bucket && current_it == rhs.current_it;
        }
        bool operator!=(const const_iterator& rhs) const { return !(*this == rhs); }

        const_iterator& operator++() {
            ++current_it;
            if (current_it == container->impls[bucket].end()) {
                ++bucket;
                current_it = container->beginOfNextNonEmptyBucket(bucket);
            }

            return *this;
        }

        const Cell& operator*() const { return *current_it; }
        const Cell* operator->() const { return current_it->get_ptr(); }

        const Cell* get_ptr() const { return current_it.get_ptr(); }
        size_t get_hash() const { return current_it.get_hash(); }
    };

    const_iterator begin() const {
        size_t buck = 0;
        typename Impl::const_iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return {this, buck, impl_it};
    }

    iterator begin() {
        size_t buck = 0;
        typename Impl::iterator impl_it = beginOfNextNonEmptyBucket(buck);
        return {this, buck, impl_it};
    }

    const_iterator end() const { return {this, MAX_BUCKET, impls[MAX_BUCKET].end()}; }
    iterator end() { return {this, MAX_BUCKET, impls[MAX_BUCKET].end()}; }

    void expanse_for_add_elem(size_t num_elem) {
        size_t num_elem_per_bucket = (num_elem + NUM_BUCKETS - 1) / NUM_BUCKETS;
        for (size_t i = 0; i < NUM_BUCKETS; ++i) impls[i].expanse_for_add_elem(num_elem_per_bucket);
    }

    /// Insert a value. In the case of any more complex values, it is better to use the `emplace` function.
    std::pair<LookupResult, bool> ALWAYS_INLINE insert(const value_type& x) {
        size_t hash_value = hash(Cell::get_key(x));

        std::pair<LookupResult, bool> res;
        emplace(Cell::get_key(x), res.first, res.second, hash_value);

        if (res.second) insert_set_mapped(lookup_result_get_mapped(res.first), x);

        return res;
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder& key_holder) {
        const auto& key = key_holder_get_key(key_holder);
        const auto key_hash = hash(key);
        const auto bucket = getBucketFromHash(key_hash);
        impls[bucket].prefetch(key_holder);
    }

    template <bool READ>
    void ALWAYS_INLINE prefetch_by_hash(size_t hash_value) {
        const auto bucket = getBucketFromHash(hash_value);
        impls[bucket].template prefetch_by_hash<READ>(hash_value);
    }

    template <bool READ, typename KeyHolder>
    void ALWAYS_INLINE prefetch(KeyHolder& key_holder) {
        const auto& key = key_holder_get_key(key_holder);
        const auto key_hash = hash(key);
        const auto bucket = getBucketFromHash(key_hash);
        impls[bucket].template prefetch<READ>(key_holder);
    }

    /** Insert the key,
      * return an iterator to a position that can be used for `placement new` of value,
      * as well as the flag - whether a new key was inserted.
      *
      * You have to make `placement new` values if you inserted a new key,
      * since when destroying a hash table, the destructor will be invoked for it!
      *
      * Example usage:
      *
      * Map::iterator it;
      * bool inserted;
      * map.emplace(key, it, inserted);
      * if (inserted)
      *     new(&it->second) Mapped(value);
      */
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted) {
        size_t hash_value = hash(key_holder_get_key(key_holder));
        emplace(key_holder, it, inserted, hash_value);
    }

    /// Same, but with a precalculated values of hash function.
    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, bool& inserted,
                               size_t hash_value) {
        size_t buck = getBucketFromHash(hash_value);
        impls[buck].emplace(key_holder, it, inserted, hash_value);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder&& key_holder, LookupResult& it, size_t hash_value,
                               bool& inserted) {
        emplace(key_holder, it, inserted, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x, size_t hash_value) {
        size_t buck = getBucketFromHash(hash_value);
        return impls[buck].find(x, hash_value);
    }

    ConstLookupResult ALWAYS_INLINE find(Key x, size_t hash_value) const {
        return const_cast<std::decay_t<decltype(*this)>*>(this)->find(x, hash_value);
    }

    LookupResult ALWAYS_INLINE find(Key x) { return find(x, hash(x)); }

    ConstLookupResult ALWAYS_INLINE find(Key x) const { return find(x, hash(x)); }

    void write(doris::vectorized::BufferWritable& wb) const {
        for (size_t i = 0; i < NUM_BUCKETS; ++i) impls[i].write(wb);
    }

    /*
    void writeText(DB::WriteBuffer & wb) const
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            impls[i].writeText(wb);
        }
    }
    */

    void read(doris::vectorized::BufferReadable& rb) {
        for (size_t i = 0; i < NUM_BUCKETS; ++i) impls[i].read(rb);
    }

    /*
    void readText(DB::ReadBuffer & rb)
    {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::assertChar(',', rb);
            impls[i].readText(rb);
        }
    }
    */

    size_t size() const {
        size_t res = 0;
        for (size_t i = 0; i < NUM_BUCKETS; ++i) res += impls[i].size();

        return res;
    }

    size_t* sizes(size_t& num_buckets) const {
        num_buckets = NUM_BUCKETS;
        size_t* sizes = new size_t[NUM_BUCKETS];
        for (size_t i = 0; i < NUM_BUCKETS; ++i) {
            sizes[i] = impls[i].size();
        }
        return sizes;
    }

    bool empty() const {
        for (size_t i = 0; i < NUM_BUCKETS; ++i)
            if (!impls[i].empty()) return false;

        return true;
    }
};
