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

#include "exprs/vexpr_context.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/exception.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/define_primitive_type.h"
#include "core/types.h"
#include "exprs/vexpr.h"
#include "exprs/vliteral.h"
#include "runtime/descriptors.h"

namespace doris {

static VExprSPtr make_int_literal(int32_t v) {
    return VLiteral::create_shared(create_texpr_node_from(&v, TYPE_INT));
}

TEST(VExprContextSimpleTest, FnContextInvalidIndexThrows) {
    auto root = make_int_literal(1);
    VExprContext ctx(root);
    EXPECT_THROW(static_cast<void>(ctx.fn_context(-1)), doris::Exception);
    EXPECT_THROW(static_cast<void>(ctx.fn_context(0)), doris::Exception);
}

// ---------------------------------------------------------------------------
// VExpr::check_expr_output_type
// ---------------------------------------------------------------------------
TEST(VExprStaticTest, CheckExprOutputType) {
    // Empty ctxs -> OK.
    VExprContextSPtrs empty;
    RowDescriptor empty_desc;
    EXPECT_TRUE(VExpr::check_expr_output_type(empty, empty_desc).ok());

    // Size mismatch: one ctx but empty output row desc.
    auto root = make_int_literal(1);
    VExprContextSPtrs ctxs {std::make_shared<VExprContext>(root)};
    auto st = VExpr::check_expr_output_type(ctxs, empty_desc);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("output type size not match") != std::string::npos);
}

// ---------------------------------------------------------------------------
// VExpr::check_constant and get_const_col
// ---------------------------------------------------------------------------
TEST(VExprStaticTest, CheckConstant) {
    auto literal = make_int_literal(1);

    // OK path: empty arguments -> all_arguments_are_constant returns true.
    Block empty_block;
    ColumnNumbers no_args;
    EXPECT_TRUE(literal->check_constant(empty_block, no_args).ok());

    // Error path: a non-const column referenced by arguments.
    Block block;
    auto col = ColumnInt32::create();
    col->insert_value(1);
    block.insert({std::move(col), std::make_shared<DataTypeInt32>(), "c0"});
    ColumnNumbers args {0};
    auto st = literal->check_constant(block, args);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("const check failed") != std::string::npos);
}

TEST(VExprStaticTest, GetConstColCachesResult) {
    auto literal = make_int_literal(42);
    std::shared_ptr<ColumnPtrWrapper> wrapper1;
    auto st = literal->get_const_col(nullptr, &wrapper1);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(wrapper1);

    // Second call returns the cached constant column.
    std::shared_ptr<ColumnPtrWrapper> wrapper2;
    st = literal->get_const_col(nullptr, &wrapper2);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(wrapper1.get(), wrapper2.get());
}

TEST(VExprInlineTest, ExecuteRuntimeFilterDelegatesToExecuteColumn) {
    auto literal = make_int_literal(99);
    ColumnPtr result;
    auto st = literal->execute_runtime_filter(nullptr, nullptr, nullptr, 1, result, nullptr);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(result);
    EXPECT_EQ(result->size(), 1u);
}

} // namespace doris
