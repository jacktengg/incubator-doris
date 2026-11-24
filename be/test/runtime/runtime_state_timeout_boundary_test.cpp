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

#include <gtest/gtest.h>

#include "runtime/runtime_state.h"

namespace doris {

TEST(RuntimeStateTimeoutBoundaryTest, UnitConversionsPreserveBoundaries) {
    // The millisecond API uses int32. Compare the last exact second with the first overflow.
    EXPECT_EQ(timeout_ms_for_int32(2147483), 2147483000);
    EXPECT_EQ(timeout_ms_for_int32(2147484), INT32_MAX);
    EXPECT_EQ(timeout_ms_for_int32(INT64_MAX), INT32_MAX);
    EXPECT_EQ(timeout_ms_for_int32(60), 60000);
    // Clamp before multiplying at both signed endpoints.
    EXPECT_EQ(timeout_ms_for_int32(-2147483), -2147483000);
    EXPECT_EQ(timeout_ms_for_int32(-2147484), INT32_MIN);
    EXPECT_EQ(timeout_ms_for_int32(INT64_MIN), INT32_MIN);
    const auto saved_timeout = config::execution_max_rpc_timeout_sec;
    config::execution_max_rpc_timeout_sec = INT32_MAX;
    EXPECT_EQ(get_execution_rpc_timeout_ms(2147484), INT32_MAX);
    config::execution_max_rpc_timeout_sec = 1;
    EXPECT_EQ(get_execution_rpc_timeout_ms(2147484), 1000);
    config::execution_max_rpc_timeout_sec = saved_timeout;
}

} // namespace doris
