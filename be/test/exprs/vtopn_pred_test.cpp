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

#include "exprs/vtopn_pred.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exec/pipeline/thrift_builder.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "runtime/runtime_predicate.h"

namespace doris {

// VTopNPred::get_binary_expr() rewrites a dynamic topn predicate into a plain
// `slot <=/>= literal` (optionally OR'ed with `slot is null`) expr tree, used by
// external/min-max filters. These tests drive its branches directly. BE unit
// tests compile with access control disabled, so the private members
// VTopNPred::_predicate and RuntimePredicate fields are set directly to avoid
// standing up a full RuntimeState/QueryContext.

namespace {

TTopnFilterDesc make_topn_filter_desc(bool is_asc, bool nulls_first) {
    TTopnFilterDesc desc;
    desc.__set_source_node_id(10);
    desc.__set_is_asc(is_asc);
    desc.__set_null_first(nulls_first);
    // The default target expr is an INT slot ref, so RuntimePredicate's type is INT.
    desc.__set_target_node_id_to_target_expr({{20, TRuntimeFilterDescBuilder::get_default_expr()}});
    return desc;
}

std::shared_ptr<VSlotRef> make_int_slot_ref() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_INT));
    node.__set_is_nullable(true);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(0);
    slot_ref.__set_tuple_id(0);
    node.__set_slot_ref(slot_ref);
    return VSlotRef::create_shared(node);
}

std::shared_ptr<VLiteral> make_int_literal() {
    TExprNode node =
            create_texpr_node_from(Field::create_field<TYPE_INT>(5), PrimitiveType::TYPE_INT, 0, 0);
    return VLiteral::create_shared(node);
}

std::shared_ptr<VTopNPred> make_vtopn_pred() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_is_nullable(true);
    return VTopNPred::create_shared(node, /*source_node_id=*/10, VExprContextSPtr {});
}

} // namespace

class VTopNPredTest : public testing::Test {};

// If the child is not a plain slot ref (e.g. `order by abs(col)`), the rewrite
// is not possible and get_binary_expr returns false.
TEST_F(VTopNPredTest, get_binary_expr_non_slot_ref_child) {
    auto vtopn = make_vtopn_pred();
    vtopn->add_child(make_int_literal());

    VExprSPtr new_root;
    EXPECT_FALSE(vtopn->get_binary_expr(new_root));
}

// If the runtime predicate has no value yet, there is nothing to rewrite.
TEST_F(VTopNPredTest, get_binary_expr_no_value) {
    RuntimePredicate predicate(make_topn_filter_desc(/*is_asc=*/true, /*nulls_first=*/false));
    // _has_value defaults to false.

    auto vtopn = make_vtopn_pred();
    vtopn->add_child(make_int_slot_ref());
    vtopn->_predicate = &predicate;

    VExprSPtr new_root;
    EXPECT_FALSE(vtopn->get_binary_expr(new_root));
}

// ASC predicate with a value and nulls_last: produces `slot <= literal`.
TEST_F(VTopNPredTest, get_binary_expr_asc_le) {
    RuntimePredicate predicate(make_topn_filter_desc(/*is_asc=*/true, /*nulls_first=*/false));
    predicate._has_value = true;
    predicate._orderby_extrem = Field::create_field<TYPE_INT>(100);

    auto vtopn = make_vtopn_pred();
    vtopn->add_child(make_int_slot_ref());
    vtopn->_predicate = &predicate;

    VExprSPtr new_root;
    ASSERT_TRUE(vtopn->get_binary_expr(new_root));
    ASSERT_TRUE(new_root != nullptr);
    EXPECT_EQ(new_root->node_type(), TExprNodeType::BINARY_PRED);
    // children: slot ref + literal.
    EXPECT_EQ(new_root->children().size(), 2);
}

// DESC predicate with a value and nulls_first: produces
// `(slot is null) OR (slot >= literal)`.
TEST_F(VTopNPredTest, get_binary_expr_desc_ge_nulls_first) {
    RuntimePredicate predicate(make_topn_filter_desc(/*is_asc=*/false, /*nulls_first=*/true));
    predicate._has_value = true;
    predicate._orderby_extrem = Field::create_field<TYPE_INT>(42);

    auto vtopn = make_vtopn_pred();
    vtopn->add_child(make_int_slot_ref());
    vtopn->_predicate = &predicate;

    VExprSPtr new_root;
    ASSERT_TRUE(vtopn->get_binary_expr(new_root));
    ASSERT_TRUE(new_root != nullptr);
    // Top node is the OR compound predicate.
    EXPECT_EQ(new_root->node_type(), TExprNodeType::COMPOUND_PRED);
    // children: `slot is null` and the `slot >= literal` predicate.
    EXPECT_EQ(new_root->children().size(), 2);
}

} // namespace doris
