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

#include "exprs/create_predicate_function.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/exception.h"
#include "core/data_type/define_primitive_type.h"

namespace doris {

class CreatePredicateFunctionTest : public testing::Test {
protected:
    CreatePredicateFunctionTest() = default;
    ~CreatePredicateFunctionTest() override = default;
};

// All primitive types accepted by create_predicate_function's switch
// (TYPE_BOOLEAN, TYPE_DECIMALV2 plus the APPLY_FOR_PRIMTYPE list).
static std::vector<PrimitiveType> all_supported_types() {
    return {
            TYPE_BOOLEAN,    TYPE_DECIMALV2, TYPE_TINYINT,    TYPE_SMALLINT,    TYPE_INT,
            TYPE_BIGINT,     TYPE_LARGEINT,  TYPE_FLOAT,      TYPE_DOUBLE,      TYPE_DATE,
            TYPE_DATETIME,   TYPE_DATEV2,    TYPE_DATETIMEV2, TYPE_TIMESTAMPTZ, TYPE_CHAR,
            TYPE_VARCHAR,    TYPE_STRING,    TYPE_DECIMAL32,  TYPE_DECIMAL64,   TYPE_DECIMAL128I,
            TYPE_DECIMAL256, TYPE_IPV4,      TYPE_IPV6,
    };
}

// Cover every case of create_predicate_function's switch through create_set
// (HybridSetTraits), including TYPE_BOOLEAN, TYPE_DECIMALV2 and each
// APPLY_FOR_PRIMTYPE entry (create_predicate_function.h:120-137).
TEST_F(CreatePredicateFunctionTest, CreateSetAllTypes) {
    for (auto type : all_supported_types()) {
        std::unique_ptr<HybridSetBase> set(create_set(type, false));
        ASSERT_NE(set, nullptr);
        EXPECT_EQ(set->size(), 0);
        EXPECT_TRUE(set->empty());
    }
}

// The default branch of the switch throws for an unsupported type
// (create_predicate_function.h:133-134).
TEST_F(CreatePredicateFunctionTest, CreateSetUnsupportedTypeThrows) {
    EXPECT_THROW({ std::unique_ptr<HybridSetBase> set(create_set(INVALID_TYPE, false)); },
                 doris::Exception);
}

// create_set with a runtime size selects the corresponding FixedContainer
// specialization (create_predicate_function.h:170-192).
TEST_F(CreatePredicateFunctionTest, CreateSetWithRuntimeSize) {
    for (size_t size = 0; size <= FIXED_CONTAINER_MAX_SIZE + 1; ++size) {
        std::unique_ptr<HybridSetBase> set(create_set(TYPE_INT, size, false));
        ASSERT_NE(set, nullptr);
        EXPECT_EQ(set->size(), 0);
    }
}

// create_bitmap_filter dispatches through create_bitmap_predicate_function for the
// four supported integer types (create_predicate_function.h:144-152).
TEST_F(CreatePredicateFunctionTest, CreateBitmapFilterSupportedTypes) {
    for (auto type : {TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT}) {
        std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(type));
        ASSERT_NE(func, nullptr);
    }
}

// Unsupported bitmap type hits the default throw
// (create_predicate_function.h:153-155).
TEST_F(CreatePredicateFunctionTest, CreateBitmapFilterUnsupportedTypeThrows) {
    EXPECT_THROW(
            { std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(TYPE_LARGEINT)); },
            doris::Exception);
}

} // namespace doris
