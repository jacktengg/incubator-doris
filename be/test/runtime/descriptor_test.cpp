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

#include <gen_cpp/Descriptors_constants.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/descriptors.pb.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "runtime/descriptors.h"
#include "testutil/desc_tbl_builder.h"

namespace doris {

class SlotDescriptorTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

protected:
    // Helper method to create a basic TSlotDescriptor
    TSlotDescriptor create_basic_slot_descriptor(int slot_id = 1,
                                                 const std::string& col_name = "test_col") {
        TSlotDescriptor tdesc;
        tdesc.__set_id(slot_id);
        tdesc.__set_parent(1);
        tdesc.__set_slotType(create_string_type());
        tdesc.__set_columnPos(0);
        tdesc.__set_colName(col_name);
        tdesc.__set_col_unique_id(slot_id);
        tdesc.__set_slotIdx(0);
        tdesc.__set_isMaterialized(true);
        tdesc.__set_is_key(false);
        tdesc.__set_nullIndicatorBit(0);
        return tdesc;
    }

    // Helper method to create a TTypeDesc for string type
    TTypeDesc create_string_type() {
        TTypeDesc type_desc;
        TTypeNode type_node;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::STRING);
        type_node.__set_type(TTypeNodeType::SCALAR);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        return type_desc;
    }

    // Helper method to create a TExpr with nodes
    TExpr create_virtual_column_expr(TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL) {
        TExpr expr;
        TExprNode node;
        node.__set_node_type(node_type);
        node.__set_type(create_string_type());
        expr.nodes.push_back(node);
        return expr;
    }
};

TEST_F(SlotDescriptorTest, BasicConstructor) {
    // Test basic constructor without virtual column expression
    TSlotDescriptor tdesc = create_basic_slot_descriptor();

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_EQ(slot_desc.col_unique_id(), 1);
        EXPECT_FALSE(slot_desc.is_key());
        EXPECT_EQ(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, CanonicalTypesWithoutPostConstructionConversion) {
    auto tdesc = create_basic_slot_descriptor();
    auto& scalar = tdesc.slotType.types[0].scalar_type;
    scalar.__set_type(TPrimitiveType::DECIMALV2);
    scalar.__set_precision(18);
    scalar.__set_scale(2);
    SlotDescriptor slot(tdesc);
    auto type = slot.type();
    ASSERT_TRUE(type->is_nullable());
    const auto& decimal = assert_cast<const DataTypeDecimalV2&>(*remove_nullable(type));
    EXPECT_EQ(decimal.get_precision(), 27);
    EXPECT_EQ(decimal.get_scale(), 9);
    EXPECT_EQ(decimal.get_original_precision(), 18);
    EXPECT_EQ(decimal.get_original_scale(), 2);

    PSlotDescriptor pdesc;
    slot.to_protobuf(&pdesc);
    SlotDescriptor restored(pdesc);
    EXPECT_TRUE(restored.type()->equals(*type));

    scalar.__set_type(TPrimitiveType::BINARY);
    SlotDescriptor binary(tdesc);
    EXPECT_EQ(binary.type()->get_primitive_type(), TYPE_STRING);
    EXPECT_TRUE(binary.type()->is_nullable());

    scalar.__set_type(TPrimitiveType::VARBINARY);
    scalar.__set_len(32);
    SlotDescriptor varbinary(tdesc);
    EXPECT_EQ(varbinary.type()->get_primitive_type(), TYPE_VARBINARY);
}

TEST_F(SlotDescriptorTest, VirtualColumnExprValid) {
    // Test constructor with valid virtual column expression
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::FUNCTION_CALL);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_NE(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, VirtualColumnExprEmptyNodes) {
    // Test constructor with empty virtual column expression nodes - should throw exception
    TSlotDescriptor tdesc = create_basic_slot_descriptor(1, "virtual_col");
    TExpr virtual_expr;
    // Empty nodes list
    virtual_expr.nodes.clear();
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_THROW({ SlotDescriptor slot_desc(tdesc); }, doris::Exception);

    // Test the specific exception message
    try {
        SlotDescriptor slot_desc(tdesc);
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        std::string error_msg = e.what();
        EXPECT_TRUE(error_msg.find("Virtual column expr node is empty") != std::string::npos);
        EXPECT_TRUE(error_msg.find("virtual_col") != std::string::npos);
        EXPECT_TRUE(error_msg.find("col_unique_id: 1") != std::string::npos);
    }
}

TEST_F(SlotDescriptorTest, VirtualColumnExprSlotRefNode) {
    // Test constructor with SLOT_REF node type - should throw exception
    TSlotDescriptor tdesc = create_basic_slot_descriptor(2, "slot_ref_col");
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::SLOT_REF);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_THROW({ SlotDescriptor slot_desc(tdesc); }, doris::Exception);

    // Test the specific exception message
    try {
        SlotDescriptor slot_desc(tdesc);
        FAIL() << "Expected doris::Exception";
    } catch (const doris::Exception& e) {
        std::string error_msg = e.what();
        EXPECT_TRUE(error_msg.find("Virtual column expr node is slot ref") != std::string::npos);
        EXPECT_TRUE(error_msg.find("slot_ref_col") != std::string::npos);
        EXPECT_TRUE(error_msg.find("col_unique_id: 2") != std::string::npos);
    }
}

TEST_F(SlotDescriptorTest, VirtualColumnExprVirtualSlotRefNode) {
    // Test constructor with VIRTUAL_SLOT_REF node type - should be valid
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::VIRTUAL_SLOT_REF);
    tdesc.__set_virtual_column_expr(virtual_expr);

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
        EXPECT_NE(slot_desc.get_virtual_column_expr(), nullptr);
    });
}

TEST_F(SlotDescriptorTest, OptionalFields) {
    // Test constructor with optional fields set
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    tdesc.__set_is_auto_increment(true);
    tdesc.__set_col_default_value("default_value");

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
    });
}

TEST_F(SlotDescriptorTest, OptionalFieldsNotSet) {
    // Test constructor with optional fields not set
    TSlotDescriptor tdesc = create_basic_slot_descriptor();
    // Don't set is_auto_increment and col_default_value

    EXPECT_NO_THROW({
        SlotDescriptor slot_desc(tdesc);
        EXPECT_EQ(slot_desc.id(), 1);
        EXPECT_EQ(slot_desc.col_name(), "test_col");
    });
}

TEST_F(SlotDescriptorTest, DebugString) {
    // Test debug string output for virtual and non-virtual columns
    TSlotDescriptor tdesc1 = create_basic_slot_descriptor(1, "normal_col");
    SlotDescriptor slot_desc1(tdesc1);
    std::string debug_str1 = slot_desc1.debug_string();
    EXPECT_TRUE(debug_str1.find("normal_col") != std::string::npos);
    EXPECT_TRUE(debug_str1.find("is_virtual=false") != std::string::npos);

    TSlotDescriptor tdesc2 = create_basic_slot_descriptor(2, "virtual_col");
    TExpr virtual_expr = create_virtual_column_expr(TExprNodeType::FUNCTION_CALL);
    tdesc2.__set_virtual_column_expr(virtual_expr);
    SlotDescriptor slot_desc2(tdesc2);
    std::string debug_str2 = slot_desc2.debug_string();
    EXPECT_TRUE(debug_str2.find("virtual_col") != std::string::npos);
    EXPECT_TRUE(debug_str2.find("is_virtual=true") != std::string::npos);
}

TEST(TupleDescriptorTest, GetColumnId) {
    ObjectPool pool;
    DescriptorTblBuilder builder(&pool);
    builder.declare_tuple() << std::make_shared<DataTypeInt32>()
                            << std::make_shared<DataTypeInt64>();
    auto* desc_tbl = builder.build();
    auto* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    ASSERT_EQ(tuple_desc->slots().size(), 2);
    EXPECT_EQ(tuple_desc->get_column_id(tuple_desc->slots()[0]->id()), 0);
    EXPECT_EQ(tuple_desc->get_column_id(tuple_desc->slots()[1]->id()), 1);
    EXPECT_EQ(tuple_desc->get_column_id(100), -1);
}

TEST(RowDescriptorTest, MultiTupleLayoutAndSingleTupleCount) {
    ObjectPool pool;
    DescriptorTblBuilder builder(&pool);
    builder.declare_tuple() << std::make_shared<DataTypeInt32>();
    builder.declare_tuple() << std::make_shared<DataTypeInt64>()
                            << std::make_shared<DataTypeInt32>();
    auto* desc_tbl = builder.build();

    RowDescriptor single(desc_tbl->get_tuple_descriptor(1));
    EXPECT_EQ(single.num_slots(), 2);

    RowDescriptor row(*desc_tbl, {0, 1});
    EXPECT_EQ(row.num_slots(), 3);
    ASSERT_EQ(row.tuple_descriptors().size(), 2);
    EXPECT_EQ(row.tuple_descriptors()[0], desc_tbl->get_tuple_descriptor(0));
    EXPECT_EQ(row.tuple_descriptors()[1], desc_tbl->get_tuple_descriptor(1));
    EXPECT_EQ(row.get_column_id(desc_tbl->get_tuple_descriptor(1)->slots()[0]->id()), 1);
    EXPECT_EQ(row.get_column_id(desc_tbl->get_tuple_descriptor(1)->slots()[1]->id()), 2);

    RowDescriptor copied(row);
    EXPECT_EQ(copied.num_slots(), row.num_slots());
    EXPECT_EQ(copied.tuple_descriptors(), row.tuple_descriptors());
}

TEST_F(SlotDescriptorTest, AccessPathsPreservedThroughProtobuf) {
    TColumnAccessPath data_path;
    data_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    data_path.__set_type(TAccessPathType::DATA);
    TDataAccessPath data_payload;
    data_payload.__set_path({"s", "field"});
    data_path.__set_data_access_path(data_payload);

    TColumnAccessPath meta_path;
    meta_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    meta_path.__set_type(TAccessPathType::META);
    TMetaAccessPath meta_payload;
    meta_payload.__set_path({"s", "field", "NULL"});
    meta_path.__set_meta_access_path(meta_payload);

    TColumnAccessPath legacy_path;
    legacy_path.__set_type(TAccessPathType::DATA);
    data_payload.__set_path({"s", "legacy", "NULL"});
    legacy_path.__set_data_access_path(data_payload);

    TColumnAccessPath explicit_legacy_path;
    explicit_legacy_path.__set_version(g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    explicit_legacy_path.__set_type(TAccessPathType::DATA);
    data_payload.__set_path({"s", "legacy", "VALUES"});
    explicit_legacy_path.__set_data_access_path(data_payload);

    TSlotDescriptor thrift_descriptor = create_basic_slot_descriptor();
    thrift_descriptor.__set_all_access_paths(
            {data_path, meta_path, legacy_path, explicit_legacy_path});
    thrift_descriptor.__set_predicate_access_paths({meta_path});

    SlotDescriptor original(thrift_descriptor);
    PSlotDescriptor protobuf_descriptor;
    original.to_protobuf(&protobuf_descriptor);
    ASSERT_EQ(protobuf_descriptor.all_access_paths_size(), 4);
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(0).has_version());
    EXPECT_EQ(protobuf_descriptor.all_access_paths(0).version(),
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_TYPED);
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(1).has_version());
    EXPECT_FALSE(protobuf_descriptor.all_access_paths(2).has_version());
    EXPECT_TRUE(protobuf_descriptor.all_access_paths(3).has_version());
    EXPECT_EQ(protobuf_descriptor.all_access_paths(3).version(),
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
    SlotDescriptor round_trip(protobuf_descriptor);

    EXPECT_EQ(round_trip.all_access_paths(), thrift_descriptor.all_access_paths);
    EXPECT_EQ(round_trip.predicate_access_paths(), thrift_descriptor.predicate_access_paths);
    EXPECT_FALSE(round_trip.all_access_paths()[1].__isset.data_access_path);
    EXPECT_FALSE(round_trip.predicate_access_paths()[0].__isset.data_access_path);
    EXPECT_FALSE(round_trip.all_access_paths()[2].__isset.version);
    EXPECT_TRUE(round_trip.all_access_paths()[3].__isset.version);
    EXPECT_EQ(round_trip.all_access_paths()[3].version,
              g_Descriptors_constants.TCOLUMN_ACCESS_PATH_VERSION_LEGACY);
}

TEST(TupleDescriptorTest, CollidingExternalTableIds) {
    // Util.genIdByName("hive", "default", name) generates these IDs in the FE.
    // DescriptorToThriftConverterTest verifies the names against the real ID generator.
    // Both IDs have signed low 32 bits of -1916665440. An int32 map key merges them.
    constexpr int64_t table_ids[] = {789217397117682080LL, 7889889164609585568LL};
    const char* table_names[] = {"orders_25394", "orders_47299"};

    // Two table descriptors and two tuple references reproduce the binding error.
    // No slots, rows, or Hive service are needed. Both tables belong to the same plan.
    TDescriptorTable thrift_table;
    thrift_table.__set_full_width_table_ids(true);
    for (int i = 0; i < 2; ++i) {
        TTableDescriptor table;
        table.__set_id(table_ids[i]);
        table.__set_tableType(TTableType::HIVE_TABLE);
        table.__set_tableName(table_names[i]);
        table.__set_dbName("default");
        thrift_table.tableDescriptors.push_back(table);
        TTupleDescriptor tuple;
        tuple.__set_id(i);
        tuple.__set_tableId(table_ids[i]);
        thrift_table.tupleDescriptors.push_back(tuple);
    }

    ObjectPool pool;
    DescriptorTbl* descriptors = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&pool, thrift_table, &descriptors).ok());
    for (int i = 0; i < 2; ++i) {
        const auto* tuple = descriptors->get_tuple_descriptor(i);
        ASSERT_NE(tuple, nullptr);
        const auto* table = tuple->table_desc();
        ASSERT_NE(table, nullptr);
        EXPECT_EQ(table->table_id(), table_ids[i]);
        EXPECT_EQ(table->name(), table_names[i]);
    }
    EXPECT_NE(descriptors->get_tuple_descriptor(0)->table_desc(),
              descriptors->get_tuple_descriptor(1)->table_desc());

    // An old FE sends -1916665440 for both references. Reject this ambiguous input.
    // Choosing either table would bind at least one tuple to the wrong table.
    thrift_table.__isset.full_width_table_ids = false;
    for (auto& tuple : thrift_table.tupleDescriptors) {
        tuple.__set_tableId(static_cast<int32_t>(tuple.tableId));
    }
    auto status = DescriptorTbl::create(&pool, thrift_table, &descriptors);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Ambiguous truncated table ID -1916665440"),
              std::string::npos);
}

TEST(TupleDescriptorTest, FullWidthTableIdsAndLegacyTuple) {
    // Two descriptors with equal low 32 bits used to overwrite one another.
    TDescriptorTable thrift_table;
    thrift_table.__set_full_width_table_ids(true);
    for (int64_t id : {int64_t {1}, int64_t {4294967297}}) {
        TTableDescriptor table;
        table.__set_id(id);
        table.__set_tableType(TTableType::OLAP_TABLE);
        thrift_table.tableDescriptors.push_back(table);
        TTupleDescriptor tuple;
        tuple.__set_id(static_cast<int>(thrift_table.tupleDescriptors.size()));
        tuple.__set_tableId(id);
        thrift_table.tupleDescriptors.push_back(tuple);
    }
    ObjectPool pool;
    DescriptorTbl* table = nullptr;
    ASSERT_TRUE(DescriptorTbl::create(&pool, thrift_table, &table).ok());
    EXPECT_EQ(table->get_tuple_descriptor(0)->table_desc()->table_id(), 1);
    EXPECT_EQ(table->get_tuple_descriptor(1)->table_desc()->table_id(), 4294967297LL);

    // The same low ID is ambiguous for a legacy sender, even though it has an exact match.
    thrift_table.__isset.full_width_table_ids = false;
    EXPECT_FALSE(DescriptorTbl::create(&pool, thrift_table, &table).ok());
    // An older FE truncated only the tuple reference. A unique full ID remains recoverable.
    thrift_table.tableDescriptors.erase(thrift_table.tableDescriptors.begin());
    thrift_table.tupleDescriptors.resize(1);
    thrift_table.tupleDescriptors[0].__set_tableId(4294967297LL);
    // A full-width reference is unambiguous even when the optional flag is absent.
    ASSERT_TRUE(DescriptorTbl::create(&pool, thrift_table, &table).ok());
    EXPECT_EQ(table->get_tuple_descriptor(0)->table_desc()->table_id(), 4294967297LL);
    thrift_table.tupleDescriptors[0].__set_tableId(1);
    ASSERT_TRUE(DescriptorTbl::create(&pool, thrift_table, &table).ok());
    EXPECT_EQ(table->get_tuple_descriptor(0)->table_desc()->table_id(), 4294967297LL);

    TTableDescriptor collision = thrift_table.tableDescriptors[0];
    collision.__set_id(8589934593LL);
    thrift_table.tableDescriptors.push_back(collision);
    EXPECT_FALSE(DescriptorTbl::create(&pool, thrift_table, &table).ok());
}

TEST(TupleDescriptorTest, UnknownTableIdsDoNotUseFullWidthFallback) {
    // One table is sufficient. A full-width sender's reference 1 is unknown even
    // though it matches the low bits of 4294967297. Only a legacy sender may recover it.
    TDescriptorTable thrift_table;
    TTableDescriptor table;
    table.__set_id(4294967297LL);
    table.__set_tableType(TTableType::OLAP_TABLE);
    thrift_table.tableDescriptors.push_back(table);
    TTupleDescriptor tuple;
    tuple.__set_id(0);
    tuple.__set_tableId(1);
    thrift_table.tupleDescriptors.push_back(tuple);
    ObjectPool pool;
    DescriptorTbl* descriptors = nullptr;
    thrift_table.__set_full_width_table_ids(true);
    auto status = DescriptorTbl::create(&pool, thrift_table, &descriptors);
    EXPECT_NE(status.to_string().find("Unknown table ID 1"), std::string::npos);
    // An absent reference must fail for both sender versions, rather than bind null.
    thrift_table.tupleDescriptors[0].__set_tableId(2);
    for (bool full_width : {false, true}) {
        thrift_table.__isset.full_width_table_ids = full_width;
        status = DescriptorTbl::create(&pool, thrift_table, &descriptors);
        EXPECT_NE(status.to_string().find("Unknown table ID 2"), std::string::npos);
    }
}

} // namespace doris
