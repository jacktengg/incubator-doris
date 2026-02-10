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

#include <butil/macros.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <roaring/roaring.hh>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "olap/lru_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/lru_cache_policy.h"
#include "runtime/memory/mem_tracker.h"
#include "util/hash_util.hpp"
#include "util/slice.h"
#include "util/time.h"

namespace doris::segment_v2 {

class ConditionCacheHandle;

// The number of rows per condition cache block.
// Both internal table segments and external table row groups use this granularity.
inline constexpr int CONDITION_CACHE_OFFSET = 2048;

class ConditionCache : public LRUCachePolicy {
public:
    using LRUCachePolicy::insert;

    // The cache key for segment lru cache (internal tables)
    struct CacheKey {
        CacheKey(RowsetId rowset_id_, int64_t segment_id_, uint64_t digest_)
                : rowset_id(rowset_id_), segment_id(segment_id_), digest(digest_) {}
        RowsetId rowset_id;
        int64_t segment_id;
        uint64_t digest;

        // Encode to a flat binary which can be used as LRUCache's key
        [[nodiscard]] std::string encode() const {
            char buf[16];
            memcpy(buf, &segment_id, 8);
            memcpy(buf + 8, &digest, 8);

            return rowset_id.to_string() + std::string(buf, 16);
        }
    };

    // The cache key for external table files (Parquet/ORC)
    struct FileCacheKey {
        FileCacheKey(const std::string& file_path_, int32_t row_group_id_, uint64_t digest_)
                : file_path(file_path_), row_group_id(row_group_id_), digest(digest_) {}
        std::string file_path;
        int32_t row_group_id;
        uint64_t digest;

        // Encode to a flat binary which can be used as LRUCache's key
        [[nodiscard]] std::string encode() const {
            if (file_path.empty()) {
                return "";
            }
            char buf[12];
            memcpy(buf, &row_group_id, 4);
            memcpy(buf + 4, &digest, 8);
            // Use hash of file path to reduce key size
            uint64_t path_hash = HashUtil::hash64(file_path.c_str(), file_path.size(), 0);
            char path_buf[8];
            memcpy(path_buf, &path_hash, 8);
            return std::string(path_buf, 8) + std::string(buf, 12);
        }
    };

    class CacheValue : public LRUCacheValueBase {
    public:
        std::shared_ptr<std::vector<bool>> filter_result;
    };

    // Create global instance of this class
    static ConditionCache* create_global_cache(size_t capacity, uint32_t num_shards = 16) {
        auto* res = new ConditionCache(capacity, num_shards);
        return res;
    }

    // Return global instance.
    // Client should call create_global_cache before.
    static ConditionCache* instance() { return ExecEnv::GetInstance()->get_condition_cache(); }

    ConditionCache() = delete;

    ConditionCache(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(CachePolicy::CacheType::CONDITION_CACHE, capacity, LRUCacheType::SIZE,
                             config::inverted_index_cache_stale_sweep_time_sec, num_shards,
                             /*element_count_capacity*/ 0, /*enable_prune*/ true,
                             /*is_lru_k*/ true) {}

    bool lookup(const CacheKey& key, ConditionCacheHandle* handle);

    bool lookup(const FileCacheKey& key, ConditionCacheHandle* handle);

    void insert(const CacheKey& key, std::shared_ptr<std::vector<bool>> filter_result);

    void insert(const FileCacheKey& key, std::shared_ptr<std::vector<bool>> filter_result);
};

class ConditionCacheHandle {
public:
    ConditionCacheHandle() = default;

    ConditionCacheHandle(LRUCachePolicy* cache, Cache::Handle* handle)
            : _cache(cache), _handle(handle) {}

    ~ConditionCacheHandle() {
        if (_handle != nullptr) {
            _cache->release(_handle);
        }
    }

    ConditionCacheHandle(ConditionCacheHandle&& other) noexcept {
        // we can use std::exchange if we switch c++14 on
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
    }

    ConditionCacheHandle& operator=(ConditionCacheHandle&& other) noexcept {
        std::swap(_cache, other._cache);
        std::swap(_handle, other._handle);
        return *this;
    }

    LRUCachePolicy* cache() const { return _cache; }

    std::shared_ptr<std::vector<bool>> get_filter_result() const {
        if (!_cache) {
            return nullptr;
        }
        return ((ConditionCache::CacheValue*)_cache->value(_handle))->filter_result;
    }

private:
    LRUCachePolicy* _cache = nullptr;
    Cache::Handle* _handle = nullptr;

    // Don't allow copy and assign
    DISALLOW_COPY_AND_ASSIGN(ConditionCacheHandle);
};

} // namespace doris::segment_v2
