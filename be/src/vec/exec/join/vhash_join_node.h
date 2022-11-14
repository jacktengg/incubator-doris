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

#pragma once

#include <future>
#include <variant>

#include "exprs/runtime_filter_slots.h"
#include "join_op.h"
#include "process_hash_table_probe.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vjoin_node_base.h"

namespace doris {

class ObjectPool;
class IRuntimeFilter;

namespace vectorized {

class SharedHashTableController;

template <typename RowRefListType>
struct SerializedHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <typename RowRefListType>
struct PartitionedSerializedHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <typename HashMethod>
struct IsSerializedHashTableContextTraits {
    constexpr static bool value = false;
};

template <typename Value, typename Mapped>
struct IsSerializedHashTableContextTraits<ColumnsHashing::HashMethodSerialized<Value, Mapped>> {
    constexpr static bool value = true;
};

// T should be UInt32 UInt64 UInt128
template <class T, typename RowRefListType>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

// TODO: use FixedHashTable instead of HashTable
template <typename RowRefListType>
using I8HashTableContext = PrimaryTypeHashTableContext<UInt8, RowRefListType>;
template <typename RowRefListType>
using I16HashTableContext = PrimaryTypeHashTableContext<UInt16, RowRefListType>;
template <typename RowRefListType>
using I32HashTableContext = PrimaryTypeHashTableContext<UInt32, RowRefListType>;
template <typename RowRefListType>
using I64HashTableContext = PrimaryTypeHashTableContext<UInt64, RowRefListType>;
template <typename RowRefListType>
using I128HashTableContext = PrimaryTypeHashTableContext<UInt128, RowRefListType>;
template <typename RowRefListType>
using I256HashTableContext = PrimaryTypeHashTableContext<UInt256, RowRefListType>;

// T should be UInt32 UInt64 UInt128
template <class T, typename RowRefListType>
struct PartitionedPrimaryTypeHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};
template <typename RowRefListType>
using I32PartitionedHashTableContext =
        PartitionedPrimaryTypeHashTableContext<UInt32, RowRefListType>;
template <typename RowRefListType>
using I64PartitionedHashTableContext =
        PartitionedPrimaryTypeHashTableContext<UInt64, RowRefListType>;
template <typename RowRefListType>
using I128PartitionedHashTableContext =
        PartitionedPrimaryTypeHashTableContext<UInt128, RowRefListType>;
template <typename RowRefListType>
using I256PartitionedHashTableContext =
        PartitionedPrimaryTypeHashTableContext<UInt256, RowRefListType>;

template <class T, bool has_null, typename RowRefListType>
struct FixedKeyHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <bool has_null, typename RowRefListType>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null, RowRefListType>;

template <class T, bool has_null, typename RowRefListType>
struct PartitionedFixedKeyHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    HashTable* hash_table_ptr = &hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <bool has_null, typename RowRefListType>
using I64PartitionedFixedKeyHashTableContext =
        PartitionedFixedKeyHashTableContext<UInt64, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I128PartitionedFixedKeyHashTableContext =
        PartitionedFixedKeyHashTableContext<UInt128, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I256PartitionedFixedKeyHashTableContext =
        PartitionedFixedKeyHashTableContext<UInt256, has_null, RowRefListType>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContext<RowRefList>, I8HashTableContext<RowRefList>,
        I16HashTableContext<RowRefList>, I32HashTableContext<RowRefList>,
        I64HashTableContext<RowRefList>, I128HashTableContext<RowRefList>,
        I256HashTableContext<RowRefList>, I64FixedKeyHashTableContext<true, RowRefList>,
        I64FixedKeyHashTableContext<false, RowRefList>,
        I128FixedKeyHashTableContext<true, RowRefList>,
        I128FixedKeyHashTableContext<false, RowRefList>,
        I256FixedKeyHashTableContext<true, RowRefList>,
        I256FixedKeyHashTableContext<false, RowRefList>,
        SerializedHashTableContext<RowRefListWithFlag>, I8HashTableContext<RowRefListWithFlag>,
        I16HashTableContext<RowRefListWithFlag>, I32HashTableContext<RowRefListWithFlag>,
        I64HashTableContext<RowRefListWithFlag>, I128HashTableContext<RowRefListWithFlag>,
        I256HashTableContext<RowRefListWithFlag>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlag>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlag>,
        SerializedHashTableContext<RowRefListWithFlags>, I8HashTableContext<RowRefListWithFlags>,
        I16HashTableContext<RowRefListWithFlags>, I32HashTableContext<RowRefListWithFlags>,
        I64HashTableContext<RowRefListWithFlags>, I128HashTableContext<RowRefListWithFlags>,
        I256HashTableContext<RowRefListWithFlags>,
        I64FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I64FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I128FixedKeyHashTableContext<false, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<true, RowRefListWithFlags>,
        I256FixedKeyHashTableContext<false, RowRefListWithFlags>,
        // two levle hash table
        PartitionedSerializedHashTableContext<RowRefList>,
        I32PartitionedHashTableContext<RowRefList>, I64PartitionedHashTableContext<RowRefList>,
        I128PartitionedHashTableContext<RowRefList>, I256PartitionedHashTableContext<RowRefList>,
        I64PartitionedFixedKeyHashTableContext<true, RowRefList>,
        I64PartitionedFixedKeyHashTableContext<false, RowRefList>,
        I128PartitionedFixedKeyHashTableContext<true, RowRefList>,
        I128PartitionedFixedKeyHashTableContext<false, RowRefList>,
        I256PartitionedFixedKeyHashTableContext<true, RowRefList>,
        I256PartitionedFixedKeyHashTableContext<false, RowRefList>,
        // RowRefListWithFlag
        PartitionedSerializedHashTableContext<RowRefListWithFlag>,
        I32PartitionedHashTableContext<RowRefListWithFlag>,
        I64PartitionedHashTableContext<RowRefListWithFlag>,
        I128PartitionedHashTableContext<RowRefListWithFlag>,
        I256PartitionedHashTableContext<RowRefListWithFlag>,
        I64PartitionedFixedKeyHashTableContext<true, RowRefListWithFlag>,
        I64PartitionedFixedKeyHashTableContext<false, RowRefListWithFlag>,
        I128PartitionedFixedKeyHashTableContext<true, RowRefListWithFlag>,
        I128PartitionedFixedKeyHashTableContext<false, RowRefListWithFlag>,
        I256PartitionedFixedKeyHashTableContext<true, RowRefListWithFlag>,
        I256PartitionedFixedKeyHashTableContext<false, RowRefListWithFlag>,
        // RowRefListWithFlags
        PartitionedSerializedHashTableContext<RowRefListWithFlags>,
        I32PartitionedHashTableContext<RowRefListWithFlags>,
        I64PartitionedHashTableContext<RowRefListWithFlags>,
        I128PartitionedHashTableContext<RowRefListWithFlags>,
        I256PartitionedHashTableContext<RowRefListWithFlags>,
        I64PartitionedFixedKeyHashTableContext<true, RowRefListWithFlags>,
        I64PartitionedFixedKeyHashTableContext<false, RowRefListWithFlags>,
        I128PartitionedFixedKeyHashTableContext<true, RowRefListWithFlags>,
        I128PartitionedFixedKeyHashTableContext<false, RowRefListWithFlags>,
        I256PartitionedFixedKeyHashTableContext<true, RowRefListWithFlags>,
        I256PartitionedFixedKeyHashTableContext<false, RowRefListWithFlags>>;

class VExprContext;
class HashJoinNode;

using HashTableCtxVariants =
        std::variant<std::monostate, ProcessHashTableProbe<TJoinOp::INNER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::LEFT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::FULL_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_OUTER_JOIN>,
                     ProcessHashTableProbe<TJoinOp::CROSS_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_SEMI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::RIGHT_ANTI_JOIN>,
                     ProcessHashTableProbe<TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>>;

class HashJoinNode final : public VJoinNodeBase {
public:
    // TODO: Best prefetch step is decided by machine. We should also provide a
    //  SQL hint to allow users to tune by hand.
    static constexpr int PREFETCH_STEP = 64;

    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void add_hash_buckets_info(const std::string& info);
    void add_hash_buckets_filled_info(const std::string& info);

private:
    using VExprContexts = std::vector<VExprContext*>;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    std::unique_ptr<VExprContext*> _vother_join_conjunct_ptr;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;

    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _build_buckets_fill_counter;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _build_side_compute_hash_timer;
    RuntimeProfile::Counter* _build_side_merge_block_timer;

    RuntimeProfile::Counter* _join_filter_timer;

    int64_t _mem_used;

    std::unique_ptr<Arena> _arena;
    std::unique_ptr<HashTableVariants> _hash_table_variants;
    bool _is_first_build_block = true;
    bool _is_convertable_to_partitioned = true;
    std::unique_ptr<HashTableVariants> _partitioned_hash_table_variants;

    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants;

    std::vector<Block> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_build = false;
    bool _probe_ignore_null = false;
    int _probe_index = -1;
    bool _probe_eos = false;

    bool _build_side_ignore_null = false;

    Sizes _probe_key_sz;
    Sizes _build_key_sz;

    // For null aware left anti join, we apply a short circuit strategy.
    // 1. Set _short_circuit_for_null_in_build_side to true if join operator is null aware left anti join.
    // 2. In build phase, we stop building hash table when we meet the first null value and set _short_circuit_for_null_in_probe_side to true.
    // 3. In probe phase, if _short_circuit_for_null_in_probe_side is true, join node returns empty block directly. Otherwise, probing will continue as the same as generic left anti join.
    bool _short_circuit_for_null_in_build_side = false;
    bool _short_circuit_for_null_in_probe_side = false;
    bool _is_broadcast_join = false;
    SharedHashTableController* _shared_hashtable_controller = nullptr;
    VRuntimeFilterSlots* _runtime_filter_slots;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    MutableColumnPtr _tuple_is_null_left_flag_column;
    MutableColumnPtr _tuple_is_null_right_flag_column;

private:
    Status _materialize_build_side(RuntimeState* state) override;

    Status _process_build_block(RuntimeState* state, Block& block, uint8_t offset);

    Status _do_evaluate(Block& block, std::vector<VExprContext*>& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    template <bool BuildSide>
    Status _extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids);

    bool _need_probe_null_map(Block& block, const std::vector<int>& res_col_ids);

    void _set_build_ignore_flag(Block& block, const std::vector<int>& res_col_ids);

    void _hash_table_init();
    void _process_hashtable_ctx_variants_init(RuntimeState* state);

    static constexpr auto _MAX_BUILD_BLOCK_COUNT = 128;

    void _prepare_probe_block();

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block);

    // reset the tuple is null flag column for the next call
    void _reset_tuple_is_null_column();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    void _release_mem();

    template <class HashTableContext>
    friend struct ProcessHashTableBuild;

    template <int JoinOpType>
    friend struct ProcessHashTableProbe;

    template <class HashTableContext>
    friend struct ProcessRuntimeFilterBuild;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>> _inserted_rows;

    std::vector<IRuntimeFilter*> _runtime_filters;
};
} // namespace vectorized
} // namespace doris
