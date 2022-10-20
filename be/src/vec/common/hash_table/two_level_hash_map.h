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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/HashTable/TwoLevelHashMap.h
// and modified by Doris
#pragma once

#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/two_level_hash_table.h"

template <typename Key, typename Cell, typename Hash = DefaultHash<Key>,
          typename Grower = TwoLevelHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
class TwoLevelHashMapTable
        : public TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator,
                                   ImplTable<Key, Cell, Hash, Grower, Allocator>> {
public:
    using Impl = ImplTable<Key, Cell, Hash, Grower, Allocator>;
    using Base = TwoLevelHashTable<Key, Cell, Hash, Grower, Allocator,
                                   ImplTable<Key, Cell, Hash, Grower, Allocator>>;
    using LookupResult = typename Impl::LookupResult;

    using Base::Base;
    using Base::prefetch;

    template <typename Func>
    void ALWAYS_INLINE forEachMapped(Func&& func) {
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i) this->impls[i].forEachMapped(func);
    }

    typename Cell::Mapped& ALWAYS_INLINE operator[](const Key& x) {
        LookupResult it;
        bool inserted;
        this->emplace(x, it, inserted);

        if (inserted) new (&it->getMapped()) typename Cell::Mapped();

        return it->getMapped();
    }

    size_t get_size() {
        size_t count = 0;
        for (auto i = 0u; i < this->NUM_BUCKETS; ++i) {
            for (auto& v : this->impls[i]) {
                count += v.get_second().get_row_count();
            }
        }
        return count;
    }
};

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = TwoLevelHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using TwoLevelHashMap = TwoLevelHashMapTable<Key, HashMapCell<Key, Mapped, Hash>, Hash, Grower,
                                             Allocator, ImplTable>;

template <typename Key, typename Mapped, typename Hash = DefaultHash<Key>,
          typename Grower = TwoLevelHashTableGrower<>, typename Allocator = HashTableAllocator,
          template <typename...> typename ImplTable = HashMapTable>
using TwoLevelHashMapWithSavedHash =
        TwoLevelHashMapTable<Key, HashMapCellWithSavedHash<Key, Mapped, Hash>, Hash, Grower,
                             Allocator, ImplTable>;
