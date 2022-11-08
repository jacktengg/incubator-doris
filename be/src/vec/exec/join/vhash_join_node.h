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

#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "exprs/runtime_filter_slots.h"
#include "vec/common/columns_hashing.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/exec/join/join_op.h"
#include "vec/exec/join/vacquire_list.hpp"
#include "vec/functions/function.h"

namespace doris {
namespace vectorized {

struct SerializedHashTableContextRowRefList {
    using Mapped = RowRefList;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

struct SerializedHashTableContextRowRefListWithFlag {
    using Mapped = RowRefListWithFlag;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

struct SerializedHashTableContextRowRefListWithFlags {
    using Mapped = RowRefListWithFlags;
    using HashTable = HashMap<StringRef, Mapped>;
    using State = ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
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
template <class T>
struct PrimaryTypeHashTableContextRowRefList {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};
template <class T>
struct PrimaryTypeHashTableContextRowRefListWithFlag {
    using Mapped = RowRefListWithFlag;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};
template <class T>
struct PrimaryTypeHashTableContextRowRefListWithFlags {
    using Mapped = RowRefListWithFlags;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State =
            ColumnsHashing::HashMethodOneNumber<typename HashTable::value_type, Mapped, T, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
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
using I8HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt8>;
using I16HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt16>;
using I32HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt32>;
using I64HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt64>;
using I128HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt128>;
using I256HashTableContextRowRefList = PrimaryTypeHashTableContextRowRefList<UInt256>;

using I8HashTableContextRowRefListWithFlag = PrimaryTypeHashTableContextRowRefListWithFlag<UInt8>;
using I16HashTableContextRowRefListWithFlag = PrimaryTypeHashTableContextRowRefListWithFlag<UInt16>;
using I32HashTableContextRowRefListWithFlag = PrimaryTypeHashTableContextRowRefListWithFlag<UInt32>;
using I64HashTableContextRowRefListWithFlag = PrimaryTypeHashTableContextRowRefListWithFlag<UInt64>;
using I128HashTableContextRowRefListWithFlag =
        PrimaryTypeHashTableContextRowRefListWithFlag<UInt128>;
using I256HashTableContextRowRefListWithFlag =
        PrimaryTypeHashTableContextRowRefListWithFlag<UInt256>;

using I8HashTableContextRowRefListWithFlags = PrimaryTypeHashTableContextRowRefListWithFlags<UInt8>;
using I16HashTableContextRowRefListWithFlags =
        PrimaryTypeHashTableContextRowRefListWithFlags<UInt16>;
using I32HashTableContextRowRefListWithFlags =
        PrimaryTypeHashTableContextRowRefListWithFlags<UInt32>;
using I64HashTableContextRowRefListWithFlags =
        PrimaryTypeHashTableContextRowRefListWithFlags<UInt64>;
using I128HashTableContextRowRefListWithFlags =
        PrimaryTypeHashTableContextRowRefListWithFlags<UInt128>;
using I256HashTableContextRowRefListWithFlags =
        PrimaryTypeHashTableContextRowRefListWithFlags<UInt256>;

template <class T, bool has_null>
struct FixedKeyHashTableContextRowRefList {
    using Mapped = RowRefList;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};
template <class T, bool has_null>
struct FixedKeyHashTableContextRowRefListWithFlag {
    using Mapped = RowRefListWithFlag;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};
template <class T, bool has_null>
struct FixedKeyHashTableContextRowRefListWithFlags {
    using Mapped = RowRefListWithFlags;
    using HashTable = HashMap<T, Mapped, HashCRC32<T>>;
    using State = ColumnsHashing::HashMethodKeysFixed<typename HashTable::value_type, T, Mapped,
                                                      has_null, false>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }
};

template <bool has_null>
using I64FixedKeyHashTableContextRowRefList = FixedKeyHashTableContextRowRefList<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContextRowRefList =
        FixedKeyHashTableContextRowRefList<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContextRowRefList =
        FixedKeyHashTableContextRowRefList<UInt256, has_null>;

////////
template <bool has_null>
using I64FixedKeyHashTableContextRowRefListWithFlag =
        FixedKeyHashTableContextRowRefListWithFlag<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContextRowRefListWithFlag =
        FixedKeyHashTableContextRowRefListWithFlag<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContextRowRefListWithFlag =
        FixedKeyHashTableContextRowRefListWithFlag<UInt256, has_null>;

////////
template <bool has_null>
using I64FixedKeyHashTableContextRowRefListWithFlags =
        FixedKeyHashTableContextRowRefListWithFlags<UInt64, has_null>;

template <bool has_null>
using I128FixedKeyHashTableContextRowRefListWithFlags =
        FixedKeyHashTableContextRowRefListWithFlags<UInt128, has_null>;

template <bool has_null>
using I256FixedKeyHashTableContextRowRefListWithFlags =
        FixedKeyHashTableContextRowRefListWithFlags<UInt256, has_null>;

using HashTableVariants = std::variant<
        std::monostate, SerializedHashTableContextRowRefList, I8HashTableContextRowRefList,
        I16HashTableContextRowRefList, I32HashTableContextRowRefList, I64HashTableContextRowRefList,
        I128HashTableContextRowRefList, I256HashTableContextRowRefList,
        I64FixedKeyHashTableContextRowRefList<true>, I64FixedKeyHashTableContextRowRefList<false>,
        I128FixedKeyHashTableContextRowRefList<true>, I128FixedKeyHashTableContextRowRefList<false>,
        I256FixedKeyHashTableContextRowRefList<true>, I256FixedKeyHashTableContextRowRefList<false>,
        SerializedHashTableContextRowRefListWithFlag, I8HashTableContextRowRefListWithFlag,
        I16HashTableContextRowRefListWithFlag, I32HashTableContextRowRefListWithFlag,
        I64HashTableContextRowRefListWithFlag, I128HashTableContextRowRefListWithFlag,
        I256HashTableContextRowRefListWithFlag, I64FixedKeyHashTableContextRowRefListWithFlag<true>,
        I64FixedKeyHashTableContextRowRefListWithFlag<false>,
        I128FixedKeyHashTableContextRowRefListWithFlag<true>,
        I128FixedKeyHashTableContextRowRefListWithFlag<false>,
        I256FixedKeyHashTableContextRowRefListWithFlag<true>,
        I256FixedKeyHashTableContextRowRefListWithFlag<false>,
        SerializedHashTableContextRowRefListWithFlags, I8HashTableContextRowRefListWithFlags,
        I16HashTableContextRowRefListWithFlags, I32HashTableContextRowRefListWithFlags,
        I64HashTableContextRowRefListWithFlags, I128HashTableContextRowRefListWithFlags,
        I256HashTableContextRowRefListWithFlags,
        I64FixedKeyHashTableContextRowRefListWithFlags<true>,
        I64FixedKeyHashTableContextRowRefListWithFlags<false>,
        I128FixedKeyHashTableContextRowRefListWithFlags<true>,
        I128FixedKeyHashTableContextRowRefListWithFlags<false>,
        I256FixedKeyHashTableContextRowRefListWithFlags<true>,
        I256FixedKeyHashTableContextRowRefListWithFlags<false>>;

using JoinOpVariants =
        std::variant<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                     std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>>;

#define APPLY_FOR_JOINOP_VARIANTS(M) \
    M(INNER_JOIN)                    \
    M(LEFT_SEMI_JOIN)                \
    M(LEFT_ANTI_JOIN)                \
    M(LEFT_OUTER_JOIN)               \
    M(FULL_OUTER_JOIN)               \
    M(RIGHT_OUTER_JOIN)              \
    M(CROSS_JOIN)                    \
    M(RIGHT_SEMI_JOIN)               \
    M(RIGHT_ANTI_JOIN)               \
    M(NULL_AWARE_LEFT_ANTI_JOIN)

class VExprContext;
class HashJoinNode;

template <class JoinOpType, bool ignore_null>
struct ProcessHashTableProbe {
    ProcessHashTableProbe(HashJoinNode* join_node, int batch_size);

    // output build side result column
    template <bool have_other_join_conjunct = false>
    void build_side_output_column(MutableColumns& mcol, int column_offset, int column_length,
                                  const std::vector<bool>& output_slot_flags, int size);

    template <bool have_other_join_conjunct = false>
    void probe_side_output_column(MutableColumns& mcol, const std::vector<bool>& output_slot_flags,
                                  int size, int last_probe_index, size_t probe_size,
                                  bool all_match_one);
    // Only process the join with no other join conjunt, because of no other join conjunt
    // the output block struct is same with mutable block. we can do more opt on it and simplify
    // the logic of probe
    // TODO: opt the visited here to reduce the size of hash table
    template <bool need_null_map_for_probe, typename HashTableType>
    Status do_process(HashTableType& hash_table_ctx, ConstNullMapPtr null_map,
                      MutableBlock& mutable_block, Block* output_block, size_t probe_rows);
    // In the presence of other join conjunt, the process of join become more complicated.
    // each matching join column need to be processed by other join conjunt. so the sturct of mutable block
    // and output block may be different
    // The output result is determined by the other join conjunt result and same_to_prev struct
    template <bool need_null_map_for_probe, typename HashTableType>
    Status do_process_with_other_join_conjuncts(HashTableType& hash_table_ctx,
                                                ConstNullMapPtr null_map,
                                                MutableBlock& mutable_block, Block* output_block,
                                                size_t probe_rows);

    // Process full outer join/ right join / right semi/anti join to output the join result
    // in hash table
    template <typename HashTableType>
    Status process_data_in_hashtable(HashTableType& hash_table_ctx, MutableBlock& mutable_block,
                                     Block* output_block, bool* eos);

    vectorized::HashJoinNode* _join_node;
    const int _batch_size;
    const std::vector<Block>& _build_blocks;
    Arena _arena;

    std::vector<uint32_t> _items_counts;
    std::vector<int8_t> _build_block_offsets;
    std::vector<int> _build_block_rows;
    // only need set the tuple is null in RIGHT_OUTER_JOIN and FULL_OUTER_JOIN
    ColumnUInt8::Container* _tuple_is_null_left_flags;
    // only need set the tuple is null in LEFT_OUTER_JOIN and FULL_OUTER_JOIN
    ColumnUInt8::Container* _tuple_is_null_right_flags;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;

    static constexpr int PROBE_SIDE_EXPLODE_RATE = 3;
};

using HashTableCtxVariants = std::variant<
        std::monostate,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>, true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>, true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>, true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                              true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                              true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                              true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>, true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                              true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                              true>,
        ProcessHashTableProbe<
                std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>, true>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::INNER_JOIN>, false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_SEMI_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_ANTI_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::LEFT_OUTER_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::FULL_OUTER_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_OUTER_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::CROSS_JOIN>, false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_SEMI_JOIN>,
                              false>,
        ProcessHashTableProbe<std::integral_constant<TJoinOp::type, TJoinOp::RIGHT_ANTI_JOIN>,
                              false>,
        ProcessHashTableProbe<
                std::integral_constant<TJoinOp::type, TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN>, false>>;

class HashJoinNode : public ::doris::ExecNode {
public:
    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void init_join_op();

    const RowDescriptor& row_desc() const override { return _output_row_desc; }

private:
    using VExprContexts = std::vector<VExprContext*>;

    TJoinOp::type _join_op;

    JoinOpVariants _join_op_variants;
    // probe expr
    VExprContexts _probe_expr_ctxs;
    // build expr
    VExprContexts _build_expr_ctxs;
    // other expr
    std::unique_ptr<VExprContext*> _vother_join_conjunct_ptr;
    // output expr
    VExprContexts _output_expr_ctxs;

    // mark the join column whether support null eq
    std::vector<bool> _is_null_safe_eq_join;

    // mark the build hash table whether it needs to store null value
    std::vector<bool> _store_null_in_hash_table;

    std::vector<uint16_t> _probe_column_disguise_null;
    std::vector<uint16_t> _probe_column_convert_to_null;

    DataTypes _right_table_data_types;
    DataTypes _left_table_data_types;

    RuntimeProfile::Counter* _build_timer;
    RuntimeProfile::Counter* _build_table_timer;
    RuntimeProfile::Counter* _build_expr_call_timer;
    RuntimeProfile::Counter* _build_table_insert_timer;
    RuntimeProfile::Counter* _build_table_expanse_timer;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_expr_call_timer;
    RuntimeProfile::Counter* _probe_next_timer;
    RuntimeProfile::Counter* _build_buckets_counter;
    RuntimeProfile::Counter* _push_down_timer;
    RuntimeProfile::Counter* _push_compute_timer;
    RuntimeProfile::Counter* _build_rows_counter;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::Counter* _search_hashtable_timer;
    RuntimeProfile::Counter* _build_side_output_timer;
    RuntimeProfile::Counter* _probe_side_output_timer;
    RuntimeProfile::Counter* _build_side_compute_hash_timer;
    RuntimeProfile::Counter* _build_side_merge_block_timer;

    RuntimeProfile::Counter* _join_filter_timer;

    int64_t _mem_used;

    Arena _arena;
    HashTableVariants _hash_table_variants;

    HashTableCtxVariants _process_hashtable_ctx_variants;

    std::vector<Block> _build_blocks;
    Block _probe_block;
    ColumnRawPtrs _probe_columns;
    ColumnUInt8::MutablePtr _null_map_column;
    bool _need_null_map_for_probe = false;
    bool _has_set_need_null_map_for_probe = false;
    bool _need_null_map_for_build = false;
    bool _has_set_need_null_map_for_build = false;
    bool _probe_ignore_null = false;
    int _probe_index = -1;
    bool _probe_eos = false;

    bool _build_side_ignore_null = false;

    Sizes _probe_key_sz;
    Sizes _build_key_sz;

    bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    bool _build_unique;          // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_outer_join;

    // For null aware left anti join, we apply a short circuit strategy.
    // 1. Set _short_circuit_for_null_in_build_side to true if join operator is null aware left anti join.
    // 2. In build phase, we stop building hash table when we meet the first null value and set _short_circuit_for_null_in_probe_side to true.
    // 3. In probe phase, if _short_circuit_for_null_in_probe_side is true, join node returns empty block directly. Otherwise, probing will continue as the same as generic left anti join.
    bool _short_circuit_for_null_in_build_side = false;
    bool _short_circuit_for_null_in_probe_side = false;

    Block _join_block;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    RowDescriptor _intermediate_row_desc;
    RowDescriptor _output_row_desc;

    MutableColumnPtr _tuple_is_null_left_flag_column;
    MutableColumnPtr _tuple_is_null_right_flag_column;

private:
    void _hash_table_build(RuntimeState* state, Block& block, ColumnRawPtrs& raw_ptrs,
                           uint8_t offset, ColumnUInt8::MutablePtr& null_map_val);
    Status _extract_join_column_variants(const std::vector<int>& res_col_ids);
    Status _extract_join_column_variants2(Block& block, ColumnUInt8::MutablePtr& null_map,
                                          ColumnRawPtrs& raw_ptrs,
                                          const std::vector<int>& res_col_ids);

    Status _probe_variants();
    Status _process_data_in_hashtable_variants(MutableBlock& mutable_block, Block* output_block,
                                               bool* eos);
    Status _runtime_filter_build_process_variants(RuntimeState* state);
    void hash_table_build_process_variants();

    void _hash_table_create_row_ref_list();
    void _hash_table_create_row_ref_list_with_flag();
    void _hash_table_create_row_ref_list_with_flags();

    void _probe_side_open_thread(RuntimeState* state, std::promise<Status>* status);

    Status _hash_table_build(RuntimeState* state);

    Status _process_build_block(RuntimeState* state, Block& block, uint8_t offset);

    Status _do_evaluate(Block& block, std::vector<VExprContext*>& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    template <bool BuildSide>
    Status _extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids);

    template <bool BuildSide>
    bool _need_null_map(Block& block, const std::vector<int>& res_col_ids);

    void _hash_table_init();
    void _process_hashtable_ctx_variants_init(RuntimeState* state);

    static constexpr auto _MAX_BUILD_BLOCK_COUNT = 128;

    void _prepare_probe_block();

    void _construct_mutable_join_block();

    Status _build_output_block(Block* origin_block, Block* output_block);

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block);

    // reset the tuple is null flag column for the next call
    void _reset_tuple_is_null_column();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    template <class HashTableContext>
    friend struct ProcessHashTableBuild;

    template <class JoinOpType, bool ignore_null>
    friend struct ProcessHashTableProbe;

    template <class HashTableContext>
    friend struct ProcessRuntimeFilterBuild;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>> _inserted_rows;

    std::vector<IRuntimeFilter*> _runtime_filters;
};
} // namespace vectorized
} // namespace doris
