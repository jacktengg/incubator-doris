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

#include "exprs/vslot_ref.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/exception.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class VSlotRefTest : public testing::Test {
public:
    void SetUp() override {
        _pool = std::make_unique<ObjectPool>();
        _state = std::make_unique<MockRuntimeState>();
    }

    void TearDown() override {
        _pool.reset();
        _state.reset();
    }

protected:
    // Helper method to create a TExprNode for VSlotRef
    TExprNode create_slot_ref_node(int slot_id, const std::string& label = "") {
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);

        TSlotRef slot_ref;
        slot_ref.__set_slot_id(slot_id);
        node.__set_slot_ref(slot_ref);

        TTypeDesc type_desc;
        TTypeNode type_node;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::STRING);
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        node.__set_type(type_desc);

        if (!label.empty()) {
            node.__set_label(label);
        }

        return node;
    }

    // Helper method to create a SlotDescriptor
    SlotDescriptor* create_slot_descriptor(
            int slot_id, const std::string& col_name,
            DataTypePtr data_type = std::make_shared<DataTypeString>()) {
        auto* slot_desc = _pool->add(new SlotDescriptor());
        slot_desc->_id = SlotId(slot_id);
        slot_desc->_col_name = col_name;
        slot_desc->_type = data_type;
        return slot_desc;
    }

    std::unique_ptr<ObjectPool> _pool;
    std::unique_ptr<MockRuntimeState> _state;
};

// prepare(): _slot_id == -1 -> early return OK
TEST_F(VSlotRefTest, PrepareSlotIdMinusOne) {
    TExprNode node = create_slot_ref_node(-1);
    auto expr = VSlotRef::create_shared(node);
    auto ctx = std::make_shared<VExprContext>(expr);
    RowDescriptor row_desc;

    Status st = expr->prepare(_state.get(), row_desc, ctx.get());
    EXPECT_TRUE(st.ok());
}

// prepare(): slot_desc == nullptr -> INTERNAL_ERROR
TEST_F(VSlotRefTest, PrepareSlotDescNull) {
    // slot id 100 is not registered in the (empty) MockDescriptorTbl1
    TExprNode node = create_slot_ref_node(100);
    auto expr = VSlotRef::create_shared(node);
    auto ctx = std::make_shared<VExprContext>(expr);
    RowDescriptor row_desc;

    Status st = expr->prepare(_state.get(), row_desc, ctx.get());
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
}

// prepare(): _column_id < 0 -> INTERNAL_ERROR
TEST_F(VSlotRefTest, PrepareInvalidColumnId) {
    const int slot_id = 7;
    _state->_mock_desc_tbl->add_slot_descriptor(slot_id, /*col_unique_id=*/1, "my_col", {});

    TExprNode node = create_slot_ref_node(slot_id);
    auto expr = VSlotRef::create_shared(node);
    auto ctx = std::make_shared<VExprContext>(expr);
    // Empty RowDescriptor => get_column_id returns -1.
    RowDescriptor row_desc;

    Status st = expr->prepare(_state.get(), row_desc, ctx.get());
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
}

// execute(): _column_id >= block->columns() -> INTERNAL_ERROR
TEST_F(VSlotRefTest, ExecuteColumnIdOutOfRange) {
    auto* slot_desc = create_slot_descriptor(1, "col_a");
    VSlotRef vslot(slot_desc);
    vslot._column_id = 0;
    vslot._column_name = &slot_desc->col_name();

    Block block; // empty block, columns() == 0
    int result_column_id = -1;
    Status st = vslot.execute(nullptr, &block, &result_column_id);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
}

// execute_column_impl(): _column_id >= block->columns() -> INTERNAL_ERROR
TEST_F(VSlotRefTest, ExecuteColumnImplColumnIdOutOfRange) {
    auto* slot_desc = create_slot_descriptor(2, "col_b");
    VSlotRef vslot(slot_desc);
    vslot._column_id = 0;
    vslot._column_name = &slot_desc->col_name();

    Block block; // empty block, columns() == 0
    ColumnPtr result_column;
    Status st = vslot.execute_column_impl(nullptr, &block, nullptr, 0, result_column);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
}

// execute_type(): _column_id >= block->columns() -> throws doris::Exception
TEST_F(VSlotRefTest, ExecuteTypeColumnIdOutOfRange) {
    auto* slot_desc = create_slot_descriptor(3, "col_c");
    VSlotRef vslot(slot_desc);
    vslot._column_id = 0;
    vslot._column_name = &slot_desc->col_name();

    Block block; // empty block, columns() == 0
    EXPECT_THROW({ (void)vslot.execute_type(&block); }, doris::Exception);
}

} // namespace doris
