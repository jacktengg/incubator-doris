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

#include "exprs/vinfo_func.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <string>

#include "core/column/column.h"
#include "core/column/column_const.h"

namespace doris {

// Build a minimal TExprNode that drives the VInfoFunc constructor.
// Only the fields read by the VExpr base ctor (node.type) and by VInfoFunc
// (node.info_func) are populated. is_nullable is set to false so that
// _data_type is the plain scalar type and get_primitive_type() returns the
// expected primitive type.
static TExprNode make_info_func_node(TPrimitiveType::type ptype, int64_t int_value,
                                     const std::string& str_value) {
    TExprNode node;
    node.node_type = TExprNodeType::INFO_FUNC;
    node.num_children = 0;
    node.__set_is_nullable(false);

    TTypeDesc type_desc;
    TTypeNode type_node;
    type_node.type = TTypeNodeType::SCALAR;
    TScalarType scalar_type;
    scalar_type.__set_type(ptype);
    if (ptype == TPrimitiveType::VARCHAR || ptype == TPrimitiveType::CHAR ||
        ptype == TPrimitiveType::STRING) {
        scalar_type.__set_len(-1);
    }
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    node.__set_type(type_desc);

    TInfoFunc info_func;
    info_func.int_value = int_value;
    info_func.str_value = str_value;
    node.__set_info_func(info_func);

    return node;
}

class VInfoFuncTest : public testing::Test {};

TEST_F(VInfoFuncTest, bigint_info_func) {
    TExprNode node = make_info_func_node(TPrimitiveType::BIGINT, 12345, "");
    VInfoFunc info_func(node);

    EXPECT_EQ(info_func.expr_name(), "vinfofunc expr");

    ColumnPtr result_column;
    const size_t count = 5;
    Status st = info_func.execute_column_impl(nullptr, nullptr, nullptr, count, result_column);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(result_column);
    EXPECT_EQ(result_column->size(), count);
    EXPECT_TRUE(is_column_const(*result_column));
    EXPECT_EQ(result_column->get_int(0), 12345);
    EXPECT_EQ(result_column->get_int(4), 12345);
}

TEST_F(VInfoFuncTest, varchar_info_func) {
    TExprNode node = make_info_func_node(TPrimitiveType::VARCHAR, 0, "doris");
    VInfoFunc info_func(node);

    ColumnPtr result_column;
    const size_t count = 3;
    Status st = info_func.execute_column_impl(nullptr, nullptr, nullptr, count, result_column);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(result_column);
    EXPECT_EQ(result_column->size(), count);
    EXPECT_TRUE(is_column_const(*result_column));
    EXPECT_EQ(result_column->get_data_at(0).to_string(), "doris");
    EXPECT_EQ(result_column->get_data_at(2).to_string(), "doris");
}

} // namespace doris
