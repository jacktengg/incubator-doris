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

#include "exprs/hash_functions.h"

#include "udf/udf.h"
#include "util/hash_util.hpp"

namespace doris {

using doris_udf::FunctionContext;
using doris_udf::IntVal;
using doris_udf::StringVal;

void HashFunctions::init() {}

uint32_t HashFunctions::_murmur_hash3_32(doris_udf::FunctionContext* ctx, int num_children,
                                         const doris_udf::StringVal* inputs, bool& is_null) {
    is_null = false;
    uint32_t seed = HashUtil::MURMUR3_32_SEED;
    for (int i = 0; i < num_children; ++i) {
        if (inputs[i].is_null) {
            is_null = true;
            return 0;
        }
        seed = HashUtil::murmur_hash3_32(inputs[i].ptr, inputs[i].len, seed);
    }
    return seed;
}
IntVal HashFunctions::murmur_hash3_32(FunctionContext* ctx, int num_children,
                                      const StringVal* inputs) {
    bool is_null = false;
    auto hash = _murmur_hash3_32(ctx, num_children, inputs, is_null);
    if (is_null) {
        return IntVal::null();
    }
    return hash;
}

BigIntVal HashFunctions::murmur_hash3_32_unsigned(FunctionContext* ctx, int num_children,
                                                  const StringVal* inputs) {
    bool is_null = false;
    auto hash = _murmur_hash3_32(ctx, num_children, inputs, is_null);
    if (is_null) {
        return BigIntVal::null();
    }
    return hash;
}

uint64_t HashFunctions::_murmur_hash3_64(doris_udf::FunctionContext* ctx, int num_children,
                                         const doris_udf::StringVal* inputs, bool& is_null) {
    is_null = false;
    uint64_t seed = 0;
    uint64_t hash = 0;
    for (int i = 0; i < num_children; ++i) {
        if (inputs[i].is_null) {
            is_null = true;
            return 0;
        }
        murmur_hash3_x64_64(inputs[i].ptr, inputs[i].len, seed, &hash);
        seed = hash;
    }
    return hash;
}
BigIntVal HashFunctions::murmur_hash3_64(FunctionContext* ctx, int num_children,
                                         const StringVal* inputs) {
    bool is_null = false;
    auto hash = _murmur_hash3_64(ctx, num_children, inputs, is_null);
    if (is_null) {
        return BigIntVal::null();
    }
    return hash;
}

LargeIntVal HashFunctions::murmur_hash3_64_unsigned(FunctionContext* ctx, int num_children,
                                                    const StringVal* inputs) {
    bool is_null = false;
    auto hash = _murmur_hash3_64(ctx, num_children, inputs, is_null);
    if (is_null) {
        return LargeIntVal::null();
    }
    return hash;
}

} // namespace doris
