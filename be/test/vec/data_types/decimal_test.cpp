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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <memory>

#include "gtest/gtest_pred_impl.h"
#include "runtime/type_limit.h"
#include "vec/core/types.h"
namespace doris::vectorized {

TEST(DecimalTest, Decimal256) {
    // 9999999999999999999999999999999999999999999999999999999999999999999999999999
    Decimal256 dec1(type_limit<vectorized::Decimal256>::max());
    auto des_str = dec1.to_string(10);
    EXPECT_EQ(des_str,
              "999999999999999999999999999999999999999999999999999999999999999999.9999999999");
    des_str = dec1.to_string(0);
    EXPECT_EQ(des_str,
              "9999999999999999999999999999999999999999999999999999999999999999999999999999");
    des_str = dec1.to_string(76);
    EXPECT_EQ(des_str,
              "0.9999999999999999999999999999999999999999999999999999999999999999999999999999");

    auto dec2 = type_limit<vectorized::Decimal256>::min();
    des_str = dec2.to_string(10);
    EXPECT_EQ(des_str,
              "-999999999999999999999999999999999999999999999999999999999999999999.9999999999");
    des_str = dec2.to_string(0);
    EXPECT_EQ(des_str,
              "-9999999999999999999999999999999999999999999999999999999999999999999999999999");
    des_str = dec2.to_string(76);
    EXPECT_EQ(des_str,
              "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999");

    // plus
    Decimal256 dec3 = dec1 + dec2;
    des_str = dec3.to_string(10);
    EXPECT_EQ(des_str, "0.0000000000");
    des_str = dec3.to_string(0);
    EXPECT_EQ(des_str, "0");
    des_str = dec3.to_string(76);
    EXPECT_EQ(des_str,
              "0.0000000000000000000000000000000000000000000000000000000000000000000000000000");

    // minus
    dec2 = type_limit<vectorized::Decimal256>::max();
    dec3 = dec1 - dec2;
    des_str = dec3.to_string(10);
    EXPECT_EQ(des_str, "0.0000000000");

    // multiply

    // divide
    dec1 = type_limit<vectorized::Decimal256>::max();
    dec2 = vectorized::Decimal256(10);
    dec3 = dec1 / dec2;
    des_str = dec3.to_string(1);
    EXPECT_EQ(des_str,
              "99999999999999999999999999999999999999999999999999999999999999999999999999.9");

    // overflow
}
} // namespace doris::vectorized