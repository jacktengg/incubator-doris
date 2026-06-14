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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/data_type/define_primitive_type.h"
#include "core/field.h"
#include "core/types.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "runtime/descriptors.h"

namespace doris {

// ---------------------------------------------------------------------------
// create_texpr_node_from(const void*, ...) : exercise the switch dispatch for
// the scalar primitive branches plus the default "invalid type" throw.
// ---------------------------------------------------------------------------
TEST(VExprCreateNodeTest, CreateTexprNodeFromVoidPtrScalars) {
    {
        bool v = true;
        auto node = create_texpr_node_from(&v, TYPE_BOOLEAN);
        EXPECT_EQ(node.node_type, TExprNodeType::BOOL_LITERAL);
        EXPECT_TRUE(node.bool_literal.value);
    }
    {
        int8_t v = -7;
        auto node = create_texpr_node_from(&v, TYPE_TINYINT);
        EXPECT_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        EXPECT_EQ(node.int_literal.value, -7);
    }
    {
        int16_t v = 1234;
        auto node = create_texpr_node_from(&v, TYPE_SMALLINT);
        EXPECT_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        EXPECT_EQ(node.int_literal.value, 1234);
    }
    {
        int32_t v = 567890;
        auto node = create_texpr_node_from(&v, TYPE_INT);
        EXPECT_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        EXPECT_EQ(node.int_literal.value, 567890);
    }
    {
        int64_t v = 1234567890123LL;
        auto node = create_texpr_node_from(&v, TYPE_BIGINT);
        EXPECT_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        EXPECT_EQ(node.int_literal.value, 1234567890123LL);
    }
    {
        __int128 v = static_cast<__int128>(123456789);
        auto node = create_texpr_node_from(&v, TYPE_LARGEINT);
        EXPECT_EQ(node.node_type, TExprNodeType::LARGE_INT_LITERAL);
        EXPECT_EQ(node.large_int_literal.value, "123456789");
    }
    {
        float v = 3.5f;
        auto node = create_texpr_node_from(&v, TYPE_FLOAT);
        EXPECT_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        EXPECT_FLOAT_EQ(node.float_literal.value, 3.5);
    }
    {
        double v = 6.25;
        auto node = create_texpr_node_from(&v, TYPE_DOUBLE);
        EXPECT_EQ(node.node_type, TExprNodeType::FLOAT_LITERAL);
        EXPECT_DOUBLE_EQ(node.float_literal.value, 6.25);
    }
    {
        std::string v = "hello-char";
        auto node = create_texpr_node_from(&v, TYPE_CHAR);
        EXPECT_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        EXPECT_EQ(node.string_literal.value, "hello-char");
    }
    {
        std::string v = "hello-varchar";
        auto node = create_texpr_node_from(&v, TYPE_VARCHAR);
        EXPECT_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        EXPECT_EQ(node.string_literal.value, "hello-varchar");
    }
    {
        std::string v = "hello-string";
        auto node = create_texpr_node_from(&v, TYPE_STRING);
        EXPECT_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        EXPECT_EQ(node.string_literal.value, "hello-string");
    }
    {
        IPv4 v = 0x7f000001; // 127.0.0.1
        auto node = create_texpr_node_from(&v, TYPE_IPV4);
        EXPECT_EQ(node.node_type, TExprNodeType::IPV4_LITERAL);
        EXPECT_EQ(node.ipv4_literal.value, static_cast<int64_t>(0x7f000001));
    }
    {
        IPv6 v = static_cast<IPv6>(1);
        auto node = create_texpr_node_from(&v, TYPE_IPV6);
        EXPECT_EQ(node.node_type, TExprNodeType::IPV6_LITERAL);
        EXPECT_FALSE(node.ipv6_literal.value.empty());
    }
    {
        double v = 12.0;
        auto node = create_texpr_node_from(&v, TYPE_TIMEV2, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::TIMEV2_LITERAL);
    }
}

TEST(VExprCreateNodeTest, CreateTexprNodeFromVoidPtrInvalidTypeThrows) {
    // The default branch throws Exception(INTERNAL_ERROR, "runtime filter meet invalid type ...")
    // without dereferencing data, so a null data pointer is safe here.
    EXPECT_THROW(create_texpr_node_from(nullptr, TYPE_HLL), doris::Exception);
}

// ---------------------------------------------------------------------------
// create_texpr_node_from(const Field&, ...) : cover branches not exercised by
// the existing LITERALTEST (TINYINT/CHAR/VARCHAR/IPV4/IPV6) and default throw.
// ---------------------------------------------------------------------------
TEST(VExprCreateNodeTest, CreateTexprNodeFromFieldExtraTypes) {
    {
        Field f = Field::create_field<TYPE_TINYINT>(static_cast<int8_t>(42));
        auto node = create_texpr_node_from(f, TYPE_TINYINT, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::INT_LITERAL);
        EXPECT_EQ(node.int_literal.value, 42);
    }
    {
        Field f = Field::create_field<TYPE_CHAR>(String("char-field"));
        auto node = create_texpr_node_from(f, TYPE_CHAR, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        EXPECT_EQ(node.string_literal.value, "char-field");
    }
    {
        Field f = Field::create_field<TYPE_VARCHAR>(String("varchar-field"));
        auto node = create_texpr_node_from(f, TYPE_VARCHAR, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::STRING_LITERAL);
        EXPECT_EQ(node.string_literal.value, "varchar-field");
    }
    {
        Field f = Field::create_field<TYPE_IPV4>(static_cast<IPv4>(0x01020304));
        auto node = create_texpr_node_from(f, TYPE_IPV4, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::IPV4_LITERAL);
        EXPECT_EQ(node.ipv4_literal.value, static_cast<int64_t>(0x01020304));
    }
    {
        Field f = Field::create_field<TYPE_IPV6>(static_cast<IPv6>(255));
        auto node = create_texpr_node_from(f, TYPE_IPV6, 0, 0);
        EXPECT_EQ(node.node_type, TExprNodeType::IPV6_LITERAL);
        EXPECT_FALSE(node.ipv6_literal.value.empty());
    }
}

TEST(VExprCreateNodeTest, CreateTexprNodeFromFieldInvalidTypeThrows) {
    // type does not match any switch case -> default throw. The Field value is
    // never inspected on the default path.
    Field f = Field::create_field<TYPE_INT>(static_cast<int32_t>(1));
    EXPECT_THROW(create_texpr_node_from(f, TYPE_HLL, 0, 0), doris::Exception);
}

// ---------------------------------------------------------------------------
// VExpr::create_expr error / branch paths
// ---------------------------------------------------------------------------
TEST(VExprCreateExprTest, UnknownExprNodeType) {
    int32_t v = 1;
    TExprNode node = create_texpr_node_from(&v, TYPE_INT);
    // AGG_EXPR (0) is not handled by create_expr's switch -> "Unknown expr node type".
    node.__set_node_type(TExprNodeType::AGG_EXPR);
    VExprSPtr expr;
    auto st = VExpr::create_expr(node, expr);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("Unknown expr node type") != std::string::npos);
}

TEST(VExprCreateExprTest, CaseExpressionNotSet) {
    int32_t v = 1;
    TExprNode node = create_texpr_node_from(&v, TYPE_INT);
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    // case_expr intentionally left unset.
    VExprSPtr expr;
    auto st = VExpr::create_expr(node, expr);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("Case expression not set") != std::string::npos);
}

TEST(VExprCreateExprTest, InfoFuncCreated) {
    int64_t v = 100;
    TExprNode node = create_texpr_node_from(&v, TYPE_BIGINT);
    node.__set_node_type(TExprNodeType::INFO_FUNC);
    TInfoFunc info_func;
    info_func.__set_int_value(100);
    info_func.__set_str_value("");
    node.__set_info_func(info_func);

    VExprSPtr expr;
    auto st = VExpr::create_expr(node, expr);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->node_type(), TExprNodeType::INFO_FUNC);
}

// ---------------------------------------------------------------------------
// VExpr::create_expr_tree / create_tree_from_thrift error and edge paths
// ---------------------------------------------------------------------------
TEST(VExprCreateTreeTest, EmptyTexprYieldsNullCtx) {
    TExpr texpr; // no nodes
    VExprContextSPtr ctx;
    auto st = VExpr::create_expr_tree(texpr, ctx);
    EXPECT_TRUE(st.ok());
    EXPECT_EQ(ctx, nullptr);
}

TEST(VExprCreateTreeTest, MissingChildNodeFails) {
    int32_t v = 1;
    TExpr texpr;
    TExprNode root = create_texpr_node_from(&v, TYPE_INT);
    // Declares one child but no child node is provided.
    root.__set_num_children(1);
    texpr.nodes.push_back(root);

    VExprContextSPtr ctx;
    auto st = VExpr::create_expr_tree(texpr, ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("Failed to reconstruct expression tree from thrift") !=
                std::string::npos);
}

TEST(VExprCreateTreeTest, PartiallyReconstructedFails) {
    int32_t v = 1;
    TExpr texpr;
    // Root is a leaf (num_children == 0) but an extra dangling node is present.
    TExprNode root = create_texpr_node_from(&v, TYPE_INT);
    root.__set_num_children(0);
    TExprNode extra = create_texpr_node_from(&v, TYPE_INT);
    extra.__set_num_children(0);
    texpr.nodes.push_back(root);
    texpr.nodes.push_back(extra);

    VExprContextSPtr ctx;
    auto st = VExpr::create_expr_tree(texpr, ctx);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("only partially reconstructed") != std::string::npos);
}

TEST(VExprCreateTreeTest, ValidTwoLevelTree) {
    int32_t v = 1;
    TExpr texpr;
    TExprNode root = create_texpr_node_from(&v, TYPE_INT);
    root.__set_num_children(1);
    TExprNode child = create_texpr_node_from(&v, TYPE_INT);
    child.__set_num_children(0);
    texpr.nodes.push_back(root);
    texpr.nodes.push_back(child);

    VExprContextSPtr ctx;
    auto st = VExpr::create_expr_tree(texpr, ctx);
    EXPECT_TRUE(st.ok()) << st.to_string();
    ASSERT_NE(ctx, nullptr);
    EXPECT_EQ(ctx->root()->get_num_children(), 1);
}

// ---------------------------------------------------------------------------
// VExpr::prepare depth-limit path (EXCEEDED_LIMIT)
// ---------------------------------------------------------------------------
TEST(VExprPrepareTest, DepthTooBig) {
    int32_t v = 1;
    auto root = VLiteral::create_shared(create_texpr_node_from(&v, TYPE_INT));
    auto child = VLiteral::create_shared(create_texpr_node_from(&v, TYPE_INT));
    root->add_child(child);

    auto ctx = std::make_shared<VExprContext>(root);

    int saved = config::max_depth_of_expr_tree;
    config::max_depth_of_expr_tree = 1;
    RowDescriptor row_desc;
    auto st = ctx->prepare(nullptr, row_desc);
    config::max_depth_of_expr_tree = saved;

    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("depth of the expression tree is too big") !=
                std::string::npos);
}

} // namespace doris
