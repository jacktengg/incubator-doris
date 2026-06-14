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

#include "exprs/block_bloom_filter.hpp"

#include <butil/iobuf.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "core/string_ref.h"
#include "util/slice.h"

namespace doris {

class BlockBloomFilterTest : public testing::Test {};

// A freshly initialized filter has no elements: it is "always false" and find()
// short-circuits to false for every input.
TEST_F(BlockBloomFilterTest, init_empty_always_false) {
    BlockBloomFilter bf;
    ASSERT_TRUE(bf.init(8, /*hash_seed=*/0).ok());
    EXPECT_TRUE(bf.always_false());
    EXPECT_EQ(bf.log_space_bytes(), 8);
    EXPECT_FALSE(bf.find(12345u));
    EXPECT_FALSE(bf.find(0u));
    bf.close();
}

TEST_F(BlockBloomFilterTest, insert_and_find_hash) {
    BlockBloomFilter bf;
    ASSERT_TRUE(bf.init(10, 0).ok());

    const std::vector<uint32_t> hashes = {1u, 2u, 1024u, 0xdeadbeefu, 987654321u};
    for (uint32_t h : hashes) {
        bf.insert(h);
    }
    EXPECT_FALSE(bf.always_false());
    // Every inserted hash must be found (no false negatives).
    for (uint32_t h : hashes) {
        EXPECT_TRUE(bf.find(h)) << "missing hash " << h;
    }
}

TEST_F(BlockBloomFilterTest, insert_and_find_string_key) {
    BlockBloomFilter bf;
    ASSERT_TRUE(bf.init(10, 0).ok());

    StringRef key("doris-bloom", 11);
    bf.insert(key);
    EXPECT_FALSE(bf.always_false());
    EXPECT_TRUE(bf.find(key));

    // A null key is a no-op on insert and always returns false on find.
    const char* null_data = nullptr;
    StringRef null_key {null_data, 0};
    bf.insert(null_key);
    EXPECT_FALSE(bf.find(null_key));
}

// _log_num_buckets = max(1, log_space_bytes - 5). 40 - 5 = 35 > 32, which is
// rejected because Insert()/Find() only use 32-bit arguments.
TEST_F(BlockBloomFilterTest, init_too_large) {
    BlockBloomFilter bf;
    Status st = bf.init(40, 0);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST_F(BlockBloomFilterTest, or_equal_array_size_check) {
    // Not a multiple of the 32-byte bucket size -> InvalidArgument.
    uint8_t in[16] = {0};
    uint8_t out[16] = {0};
    Status st = BlockBloomFilter::or_equal_array(sizeof(in), in, out);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST_F(BlockBloomFilterTest, or_equal_array_success) {
    constexpr size_t n = 64; // multiple of 32
    uint8_t in[n];
    uint8_t out[n];
    uint8_t expected[n];
    for (size_t i = 0; i < n; ++i) {
        in[i] = static_cast<uint8_t>(i);
        out[i] = static_cast<uint8_t>(0xF0);
        expected[i] = out[i] | in[i];
    }
    ASSERT_TRUE(BlockBloomFilter::or_equal_array(n, in, out).ok());
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(out[i], expected[i]) << "mismatch at " << i;
    }
}

TEST_F(BlockBloomFilterTest, merge_success) {
    BlockBloomFilter a;
    BlockBloomFilter b;
    ASSERT_TRUE(a.init(10, 0).ok());
    ASSERT_TRUE(b.init(10, 0).ok());

    a.insert(111u);
    b.insert(222u);
    ASSERT_TRUE(a.merge(b).ok());
    EXPECT_FALSE(a.always_false());
    // After merge, a contains the union of both filters' elements.
    EXPECT_TRUE(a.find(111u));
    EXPECT_TRUE(a.find(222u));
}

TEST_F(BlockBloomFilterTest, merge_self_is_noop) {
    BlockBloomFilter a;
    ASSERT_TRUE(a.init(10, 0).ok());
    a.insert(333u);
    ASSERT_TRUE(a.merge(a).ok());
    EXPECT_TRUE(a.find(333u));
}

TEST_F(BlockBloomFilterTest, merge_always_false_other_is_noop) {
    BlockBloomFilter a;
    BlockBloomFilter b;
    ASSERT_TRUE(a.init(10, 0).ok());
    ASSERT_TRUE(b.init(10, 0).ok());
    a.insert(444u);
    // b is still always_false; merging it should not change a.
    ASSERT_TRUE(a.merge(b).ok());
    EXPECT_TRUE(a.find(444u));
}

TEST_F(BlockBloomFilterTest, merge_size_mismatch) {
    BlockBloomFilter a;
    BlockBloomFilter b;
    ASSERT_TRUE(a.init(8, 0).ok());  // directory_size 1<<8
    ASSERT_TRUE(b.init(10, 0).ok()); // directory_size 1<<10
    Status st = a.merge(b);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

TEST_F(BlockBloomFilterTest, init_from_directory_roundtrip) {
    BlockBloomFilter src;
    ASSERT_TRUE(src.init(8, 0).ok());
    src.insert(55u);
    src.insert(66u);

    Slice dir = src.directory();
    butil::IOBuf buf;
    buf.append(dir.get_data(), dir.get_size());
    butil::IOBufAsZeroCopyInputStream stream(buf);

    BlockBloomFilter dst;
    ASSERT_TRUE(
            dst.init_from_directory(8, &stream, dir.get_size(), /*always_false=*/false, 0).ok());
    EXPECT_FALSE(dst.always_false());
    EXPECT_TRUE(dst.find(55u));
    EXPECT_TRUE(dst.find(66u));
}

TEST_F(BlockBloomFilterTest, init_from_directory_size_mismatch) {
    BlockBloomFilter src;
    ASSERT_TRUE(src.init(8, 0).ok());
    Slice dir = src.directory();
    butil::IOBuf buf;
    buf.append(dir.get_data(), dir.get_size());
    butil::IOBufAsZeroCopyInputStream stream(buf);

    BlockBloomFilter dst;
    // data_size that does not match directory_size() -> InvalidArgument.
    Status st = dst.init_from_directory(8, &stream, dir.get_size() - 1, false, 0);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>()) << st.to_string();
}

// On an AVX2-enabled build, insert()/find() dispatch to the AVX2 routines, so the scalar
// (non-AVX2) bucket_insert()/bucket_find() helpers are only reachable by calling them
// directly. BE-UT disables access control, so these private helpers are callable here.
TEST_F(BlockBloomFilterTest, scalar_bucket_insert_and_find) {
    BlockBloomFilter bf;
    ASSERT_TRUE(bf.init(10, 0).ok());
    const uint32_t hash = 0x12345678u;
    const uint32_t bucket_idx = 0;
    // A freshly zeroed bucket has no bits set, so bucket_find short-circuits to false.
    EXPECT_FALSE(bf.bucket_find(bucket_idx, hash));
    // bucket_insert sets the eight split-bits; bucket_find then matches the same hash.
    bf.bucket_insert(bucket_idx, hash);
    EXPECT_TRUE(bf.bucket_find(bucket_idx, hash));
}

// or_equal_array() dispatches to the AVX2 implementation on this build, so exercise the
// scalar or_equal_array_no_avx2() directly. It computes out[i] |= in[i] over 32-byte blocks.
TEST_F(BlockBloomFilterTest, or_equal_array_no_avx2_direct) {
    constexpr size_t n = 64; // multiple of 32
    uint8_t in[n];
    uint8_t out[n];
    uint8_t expected[n];
    for (size_t i = 0; i < n; ++i) {
        in[i] = static_cast<uint8_t>(i * 3 + 1);
        out[i] = static_cast<uint8_t>(0x0F);
        expected[i] = out[i] | in[i];
    }
    BlockBloomFilter::or_equal_array_no_avx2(n, in, out);
    for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(out[i], expected[i]) << "mismatch at " << i;
    }
}

} // namespace doris
