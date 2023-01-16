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
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/hash_table/partitioned_hash_map.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vjoin_node_base.h"

namespace doris {

class ObjectPool;
class IRuntimeFilter;

namespace vectorized {

class SharedHashTableController;

template <typename RowRefListType>
struct SerializedHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<StringRef, Mapped>;
    using State =
            ColumnsHashing::HashMethodSerialized<typename HashTable::value_type, Mapped, true>;
    using Iter = typename HashTable::iterator;

    HashTable hash_table;
    Iter iter;
    bool inited = false;
    std::vector<StringRef> keys;
    size_t keys_memory_usage = 0;

    void serialize_keys(const ColumnRawPtrs& key_columns, size_t num_rows) {
        if (keys.size() < num_rows) {
            keys.resize(num_rows);
        }

        _arena.reset(new Arena());
        keys_memory_usage = 0;
        size_t keys_size = key_columns.size();
        for (size_t i = 0; i < num_rows; ++i) {
            keys[i] = serialize_keys_to_pool_contiguous(i, keys_size, key_columns, *_arena);
        }
        keys_memory_usage = _arena->size();
    }

    void init_once() {
        if (!inited) {
            inited = true;
            iter = hash_table.begin();
        }
    }

private:
    std::unique_ptr<Arena> _arena;
};

// T should be UInt32 UInt64 UInt128
template <class T, typename RowRefListType>
struct PrimaryTypeHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
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

template <class T, bool has_null, typename RowRefListType>
struct FixedKeyHashTableContext {
    using Mapped = RowRefListType;
    using HashTable = PartitionedHashMap<T, Mapped, HashCRC32<T>>;
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

template <bool has_null, typename RowRefListType>
using I64FixedKeyHashTableContext = FixedKeyHashTableContext<UInt64, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I128FixedKeyHashTableContext = FixedKeyHashTableContext<UInt128, has_null, RowRefListType>;

template <bool has_null, typename RowRefListType>
using I256FixedKeyHashTableContext = FixedKeyHashTableContext<UInt256, has_null, RowRefListType>;

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
        I256FixedKeyHashTableContext<false, RowRefListWithFlags>>;

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

class HashJoinNode;

static constexpr int MAX_PARTITION_DEPTH = 16;
class HashJoinPartition {
    friend class HashJoinNode;

public:
    HashJoinPartition(int level) : level_(level) {}

    size_t build_data_size() const {
        size_t data_size = 0;
        for (const auto& block : build_blocks_) {
            data_size += block.bytes();
        }
        return data_size;
    }

    int level() const { return level_; }
    Status spill();

    bool is_spilled() const { return is_spilled_; }

    size_t build_row_count() const { return build_row_count_; }

    size_t probe_row_count() const { return probe_row_count_; }

    void update_build_row_count(size_t rows) { build_row_count_ += rows; }

    void update_probe_row_count(size_t rows) { probe_row_count_ += rows; }

    void close() {
        hash_table_.reset();
        build_blocks_.clear();
        probe_blocks_.clear();
    }

private:
    int level_ = 0;
    HashJoinNode* parent_;
    bool is_spilled_ = false;
    size_t build_row_count_ = 0;
    size_t probe_row_count_ = 0;
    std::vector<Block> probe_blocks_;
    std::vector<Block> build_blocks_;
    std::shared_ptr<HashTableVariants> hash_table_;

    std::deque<int64_t> spilled_build_block_streams_;
    std::deque<int64_t> spilled_probe_block_streams_;
};

using HashJoinPartitionSPtr = std::shared_ptr<HashJoinPartition>;

enum class HashJoinState {
    /// Partitioning the build (right) child's input into the builder's hash partitions.
    PARTITIONING_BUILD,

    /// Processing the probe (left) child's input, probing hash tables and
    /// spilling probe rows into 'probe_hash_partitions_' if necessary.
    PARTITIONING_PROBE,

    /// Processing the spilled probe rows of a single spilled partition
    /// ('input_partition_') that fits in memory.
    PROBING_SPILLED_PARTITION,

    /// Repartitioning the build rows of a single spilled partition ('input_partition_')
    /// into the builder's hash partitions.
    /// Corresponds to PARTITIONING_BUILD but reading from a spilled partition.
    REPARTITIONING_BUILD,

    /// Probing the repartitioned hash partitions of a single spilled partition
    /// ('input_partition_') with the probe rows of that partition.
    /// Corresponds to PARTITIONING_PROBE but reading from a spilled partition.
    REPARTITIONING_PROBE,
};

enum class ProbeState {
    // Processing probe batches and more rows in the current probe batch must be
    // processed.
    PROBING_IN_BATCH,
    // Processing probe batches and no more rows in the current probe batch to process.
    PROBING_END_BATCH,
    // All probe batches have been processed, unmatched build rows need to be outputted
    // from 'output_build_partitions_'.
    // This state is only used if NeedToProcessUnmatchedBuildRows(join_op_) is true.
    OUTPUTTING_UNMATCHED,
    // All input probe rows from the child ExecNode or the current spilled partition have
    // been processed, and all unmatched rows from the build have been output.
    PROBE_COMPLETE,
    // All input has been processed. We need to process builder->null_aware_partition()
    // and output any rows from it.
    // This state is only used if join_op_ is NULL_AWARE_ANTI_JOIN.
    OUTPUTTING_NULL_AWARE,
    // All input has been processed. We need to process null_probe_rows_ and output any
    // rows from it.
    // This state is only used if join_op_ is NULL_AWARE_ANTI_JOIN.
    OUTPUTTING_NULL_PROBE,
    // All output rows have been produced - GetNext() should return eos.
    EOS,
};

class HashJoinNode final : public VJoinNodeBase {
    friend class HashJoinPartition;

private:
    // for spill to disk
    static constexpr size_t BITS_FOR_SUB_TABLE = 4;
    static constexpr size_t HASH_JOIN_HASH_PARTITION_COUNT = 1ULL << BITS_FOR_SUB_TABLE;
    static constexpr size_t SPILL_BUILD_BLOCK_MAX_SIZE = 256UL * 1024UL * 1024UL;
    std::vector<HashJoinPartitionSPtr> hash_partitions_;

    std::vector<HashJoinPartitionSPtr> current_hash_partitions_;
    std::vector<HashJoinPartitionSPtr> spilled_hash_partitions_;
    HashJoinPartitionSPtr current_spilled_partition_;

    bool create_partitioned_mutable_build_blocks_ = true;
    std::unique_ptr<MutableBlock> partitioned_mutable_build_blocks_[HASH_JOIN_HASH_PARTITION_COUNT];

    bool create_partitioned_mutable_probe_blocks_ = true;
    std::unique_ptr<MutableBlock> partitioned_mutable_probe_blocks_[HASH_JOIN_HASH_PARTITION_COUNT];

    std::vector<int> build_col_ids_;

public:
    // TODO: Best prefetch step is decided by machine. We should also provide a
    //  SQL hint to allow users to tune by hand.
    static constexpr int PREFETCH_STEP = 64;

    HashJoinNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~HashJoinNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, Block* block, bool* eos) override;
    Status close(RuntimeState* state) override;
    void add_hash_buckets_info(const std::string& info);
    void add_hash_buckets_filled_info(const std::string& info);

    Status alloc_resource(RuntimeState* state) override;
    void release_resource(RuntimeState* state) override;
    Status sink(doris::RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    bool need_more_input_data() const;
    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;
    Status push(RuntimeState* state, vectorized::Block* input_block, bool eos) override;
    void prepare_for_next() override;

    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool can_sink_write() const {
        if (_should_build_hash_table) {
            return true;
        }
        return _shared_hash_table_context && _shared_hash_table_context->signaled;
    }

    bool should_build_hash_table() const { return _should_build_hash_table; }

private:
    void UpdateState(HashJoinState next_state) {
        // Validate the state transition.
        switch (join_state_) {
        case HashJoinState::PARTITIONING_BUILD:
            DCHECK(next_state == HashJoinState::PARTITIONING_PROBE);
            break;
        case HashJoinState::PARTITIONING_PROBE:
        case HashJoinState::REPARTITIONING_PROBE:
        case HashJoinState::PROBING_SPILLED_PARTITION:
            DCHECK(next_state == HashJoinState::REPARTITIONING_BUILD ||
                   next_state == HashJoinState::PROBING_SPILLED_PARTITION);
            break;
        case HashJoinState::REPARTITIONING_BUILD:
            DCHECK(next_state == HashJoinState::REPARTITIONING_PROBE);
            break;
        default:
            DCHECK(false) << "Invalid state " << static_cast<int>(join_state_);
        }
        join_state_ = next_state;
    }

    Status _create_hash_partitions(int level);
    Status _process_build_data_partitioned(RuntimeState* state);
    Status _sink_partitioned(doris::RuntimeState* state, vectorized::Block& in_block, bool eos);

    static size_t get_partition_from_hash(size_t hash_value) {
        return (hash_value >> (32 - BITS_FOR_SUB_TABLE)) & HASH_JOIN_HASH_PARTITION_COUNT;
    }

    template <class HashTableContext, bool ignore_null, bool short_circuit_for_null>
    Status _partition_build_block(vectorized::Block& block, HashTableContext& hash_table_ctx,
                            bool* has_null_key);

    Status _build_partitioned_hash_tables(RuntimeState* state);
    Status _build_partitioned_hash_table(RuntimeState* state, HashJoinPartitionSPtr& partition,
                                         bool& is_build);
    Status _spill_build_partition();
    Status _spill_probe_row(HashJoinPartitionSPtr& partition, Block& block, size_t row);

    Status _prepare_for_partitioned_probe();

    Status _get_next_partitioned(RuntimeState* state, Block* output_block, bool* eos);

    Status _get_next_probe_block(RuntimeState* state, bool* eos);
    Status _get_next_probe_block_from_child(RuntimeState* state, bool* eos);
    Status _get_next_probe_block_spilled(RuntimeState* state, bool* eos);

    Status _process_probe_block(RuntimeState* state, Block* output_block, bool* eos);
    Status _finished_probing_partitions(RuntimeState* state);

    Status _begin_spilled_probe(RuntimeState* state);
    Status _repartition_build_input(RuntimeState* state, HashJoinPartitionSPtr& partition);

    HashJoinState join_state_ = HashJoinState::PARTITIONING_BUILD;
    ProbeState probe_state_ = ProbeState::PROBE_COMPLETE;

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
    RuntimeProfile::Counter* _build_table_convert_timer;
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

    RuntimeProfile::Counter* _build_blocks_memory_usage;
    RuntimeProfile::Counter* _hash_table_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _build_arena_memory_usage;
    RuntimeProfile::HighWaterMarkCounter* _probe_arena_memory_usage;

    RuntimeProfile* _build_phase_profile;

    std::shared_ptr<Arena> _arena;

    // maybe share hash table with other fragment instances
    std::shared_ptr<HashTableVariants> _hash_table_variants;

    std::unique_ptr<HashTableCtxVariants> _process_hashtable_ctx_variants;

    // for full/right outer join
    ForwardIterator<RowRefListWithFlag> _outer_join_pull_visited_iter;

    std::shared_ptr<std::vector<Block>> _build_blocks;
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

    bool _is_broadcast_join = false;
    bool _should_build_hash_table = true;
    std::shared_ptr<SharedHashTableController> _shared_hashtable_controller = nullptr;
    VRuntimeFilterSlots* _runtime_filter_slots = nullptr;

    std::vector<SlotId> _hash_output_slot_ids;
    std::vector<bool> _left_output_slot_flags;
    std::vector<bool> _right_output_slot_flags;

    uint8_t _build_block_idx = 0;
    int64_t _build_side_mem_used = 0;
    int64_t _build_side_last_mem_used = 0;
    MutableBlock _build_side_mutable_block;

    SharedHashTableContextPtr _shared_hash_table_context = nullptr;

    Status _materialize_build_side(RuntimeState* state) override;

    Status _process_build_block(RuntimeState* state, Block& block, uint8_t offset);

    Status _do_evaluate(Block& block, std::vector<VExprContext*>& exprs,
                        RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    template <bool BuildSide>
    Status _extract_join_column(Block& block, ColumnUInt8::MutablePtr& null_map,
                                ColumnRawPtrs& raw_ptrs, const std::vector<int>& res_col_ids);

    bool _need_probe_null_map(Block& block, const std::vector<int>& res_col_ids);

    void _set_build_ignore_flag(Block& block, const std::vector<int>& res_col_ids);

    void _hash_table_init(RuntimeState* state);
    void _process_hashtable_ctx_variants_init(RuntimeState* state);

    static constexpr auto _MAX_BUILD_BLOCK_COUNT = 128;

    void _prepare_probe_block();

    static std::vector<uint16_t> _convert_block_to_null(Block& block);

    void _release_mem();

    // add tuple is null flag column to Block for filter conjunct and output expr
    void _add_tuple_is_null_column(Block* block) override;

    template <class HashTableContext>
    friend struct ProcessHashTableBuild;

    template <int JoinOpType>
    friend struct ProcessHashTableProbe;

    template <class HashTableContext>
    friend struct ProcessRuntimeFilterBuild;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
    std::unordered_map<const Block*, std::vector<int>> _inserted_rows;

    std::vector<IRuntimeFilter*> _runtime_filters;

    RuntimeProfile* block_spill_profile_;
};
} // namespace vectorized
} // namespace doris
