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

#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_filter_mgr.h"
#include "util/defer_op.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/template_helpers.hpp"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
static constexpr int PREFETCH_STEP = 64;

template <class JoinOpType, bool ignore_null>
template <bool need_null_map_for_probe, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::do_process_with_other_join_conjuncts(
        HashTableType& hash_table_ctx, ConstNullMapPtr null_map, MutableBlock& mutable_block,
        Block* output_block, size_t probe_rows) {
    auto& probe_index = _join_node->_probe_index;
    auto& probe_raw_ptrs = _join_node->_probe_columns;
    if (probe_index == 0 && _items_counts.size() < probe_rows) {
        _items_counts.resize(probe_rows);
    }
    if (_build_block_rows.size() < probe_rows * PROBE_SIDE_EXPLODE_RATE) {
        _build_block_rows.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
        _build_block_offsets.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
    }

    using KeyGetter = typename HashTableType::State;
    using Mapped = typename HashTableType::Mapped;
    if constexpr (std::is_same_v<Mapped, RowRefListWithFlags>) {
        constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                                   JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;
        KeyGetter key_getter(probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);

        int right_col_idx = _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        auto& mcol = mutable_block.mutable_columns();
        // use in right join to change visited state after
        // exec the vother join conjunt
        std::vector<bool*> visited_map;
        visited_map.reserve(1.2 * _batch_size);

        std::vector<bool> same_to_prev;
        same_to_prev.reserve(1.2 * _batch_size);

        int current_offset = 0;

        bool all_match_one = true;
        int last_probe_index = probe_index;
        while (probe_index < probe_rows) {
            // ignore null rows
            if constexpr (ignore_null && need_null_map_for_probe) {
                if ((*null_map)[probe_index]) {
                    if constexpr (probe_all) {
                        _items_counts[probe_index++] = (uint32_t)1;
                        same_to_prev.emplace_back(false);
                        visited_map.emplace_back(nullptr);
                        // only full outer / left outer need insert the data of right table
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                        } else {
                            _build_block_offsets.emplace_back(-1);
                            _build_block_rows.emplace_back(-1);
                        }
                        ++current_offset;
                    } else {
                        _items_counts[probe_index++] = (uint32_t)0;
                    }
                    all_match_one = false;
                    continue;
                }
            }

            auto last_offset = current_offset;
            auto find_result =
                    !need_null_map_for_probe
                            ? key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena)
                    : (*null_map)[probe_index]
                            ? decltype(key_getter.find_key(hash_table_ctx.hash_table, probe_index,
                                                           _arena)) {nullptr, false}
                            : key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena);
            if (probe_index + PREFETCH_STEP < probe_rows)
                key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                                   probe_index + PREFETCH_STEP, _arena);
            if (find_result.is_found()) {
                auto& mapped = find_result.get_mapped();
                auto origin_offset = current_offset;
                // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
                // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
                if (mapped.get_row_count() == 1) {
                    if (LIKELY(current_offset < _build_block_rows.size())) {
                        _build_block_offsets[current_offset] = mapped.block_offset;
                        _build_block_rows[current_offset] = mapped.row_num;
                    } else {
                        _build_block_offsets.emplace_back(mapped.block_offset);
                        _build_block_rows.emplace_back(mapped.row_num);
                    }
                    ++current_offset;
                    visited_map.emplace_back(&mapped.visited);
                } else {
                    for (auto it = mapped.begin(); it.ok(); ++it) {
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = it->block_offset;
                            _build_block_rows[current_offset] = it->row_num;
                        } else {
                            _build_block_offsets.emplace_back(it->block_offset);
                            _build_block_rows.emplace_back(it->row_num);
                        }
                        ++current_offset;
                        visited_map.emplace_back(&it->visited);
                    }
                }
                same_to_prev.emplace_back(false);
                for (int i = 0; i < current_offset - origin_offset - 1; ++i) {
                    same_to_prev.emplace_back(true);
                }
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                                 JoinOpType::value == TJoinOp::FULL_OUTER_JOIN ||
                                 JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                                 JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                same_to_prev.emplace_back(false);
                visited_map.emplace_back(nullptr);
                // only full outer / left outer need insert the data of right table
                // left anti use -1 use a default value
                if (LIKELY(current_offset < _build_block_rows.size())) {
                    _build_block_offsets[current_offset] = -1;
                    _build_block_rows[current_offset] = -1;
                } else {
                    _build_block_offsets.emplace_back(-1);
                    _build_block_rows.emplace_back(-1);
                }
                ++current_offset;
            } else {
                // other join, no nothing
            }
            uint32_t count = (uint32_t)(current_offset - last_offset);
            _items_counts[probe_index++] = count;
            all_match_one &= (count == 1);
            if (current_offset >= _batch_size && !all_match_one) {
                break;
            }
        }

        {
            SCOPED_TIMER(_build_side_output_timer);
            build_side_output_column<true>(mcol, right_col_idx, right_col_len,
                                           _join_node->_right_output_slot_flags, current_offset);
        }
        {
            SCOPED_TIMER(_probe_side_output_timer);
            probe_side_output_column<true>(mcol, _join_node->_left_output_slot_flags,
                                           current_offset, last_probe_index,
                                           probe_index - last_probe_index, all_match_one);
        }
        output_block->swap(mutable_block.to_block());

        // dispose the other join conjunt exec
        if (output_block->rows()) {
            int result_column_id = -1;
            int orig_columns = output_block->columns();
            (*_join_node->_vother_join_conjunct_ptr)->execute(output_block, &result_column_id);

            auto column = output_block->get_by_position(result_column_id).column;
            if constexpr (JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                          JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                auto null_map_column = ColumnVector<UInt8>::create(column->size(), 0);
                auto* __restrict null_map_data = null_map_column->get_data().data();

                for (int i = 0; i < column->size(); ++i) {
                    auto join_hit = visited_map[i] != nullptr;
                    auto other_hit = column->get_bool(i);

                    if (!other_hit) {
                        for (size_t j = 0; j < right_col_len; ++j) {
                            typeid_cast<ColumnNullable*>(
                                    std::move(*output_block->get_by_position(j + right_col_idx)
                                                       .column)
                                            .assume_mutable()
                                            .get())
                                    ->get_null_map_data()[i] = true;
                        }
                    }
                    null_map_data[i] = !join_hit || !other_hit;

                    if (join_hit) {
                        *visited_map[i] |= other_hit;
                        filter_map.push_back(other_hit || !same_to_prev[i] ||
                                             (!column->get_bool(i - 1) && filter_map.back()));
                        // Here to keep only hit join conjunt and other join conjunt is true need to be output.
                        // if not, only some key must keep one row will output will null right table column
                        if (same_to_prev[i] && filter_map.back() && !column->get_bool(i - 1))
                            filter_map[i - 1] = false;
                    } else {
                        filter_map.push_back(true);
                    }
                }

                for (int i = 0; i < column->size(); ++i) {
                    if (filter_map[i]) {
                        _tuple_is_null_right_flags->emplace_back(null_map_data[i]);
                    }
                }
                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) {
                    filter_map.emplace_back(column->get_bool(0));
                }
                for (int i = 1; i < column->size(); ++i) {
                    if (column->get_bool(i) || (same_to_prev[i] && filter_map[i - 1])) {
                        // Only last same element is true, output last one
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                                 JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                auto new_filter_column = ColumnVector<UInt8>::create();
                auto& filter_map = new_filter_column->get_data();

                if (!column->empty()) {
                    // Both equal conjuncts and other conjuncts are true
                    filter_map.emplace_back(column->get_bool(0) && visited_map[0]);
                }
                for (int i = 1; i < column->size(); ++i) {
                    if ((visited_map[i] && column->get_bool(i)) ||
                        (same_to_prev[i] && filter_map[i - 1])) {
                        // When either of two conditions is meet:
                        // 1. Both equal conjuncts and other conjuncts are true or same_to_prev
                        // 2. This row is joined from the same build side row as the previous row
                        // Set filter_map[i] to true and filter_map[i - 1] to false if same_to_prev[i]
                        // is true.
                        filter_map.push_back(true);
                        filter_map[i - 1] = !same_to_prev[i] && filter_map[i - 1];
                    } else {
                        filter_map.push_back(false);
                    }
                }

                // Same to the semi join, but change the last value to opposite value
                for (int i = 1; i < same_to_prev.size(); ++i) {
                    if (!same_to_prev[i]) filter_map[i - 1] = !filter_map[i - 1];
                }
                filter_map[same_to_prev.size() - 1] = !filter_map[same_to_prev.size() - 1];

                output_block->get_by_position(result_column_id).column =
                        std::move(new_filter_column);
            } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                 JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    *visited_map[i] |= column->get_bool(i);
                }
            } else if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN) {
                auto filter_size = 0;
                for (int i = 0; i < column->size(); ++i) {
                    DCHECK(visited_map[i]);
                    auto result = column->get_bool(i);
                    *visited_map[i] |= result;
                    filter_size += result;
                }
                _tuple_is_null_left_flags->resize_fill(filter_size, 0);
            } else {
                // inner join do nothing
            }

            if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                          JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN) {
                output_block->clear();
            } else {
                if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN ||
                              JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                              JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                    orig_columns = right_col_idx;
                }
                Block::filter_block(output_block, result_column_id, orig_columns);
            }
        }

        return Status::OK();
    } else {
        LOG(FATAL) << "Invalid RowRefList";
        return Status::InvalidArgument("Invalid RowRefList");
    }
}
template <class JoinOpType, bool ignore_null>
template <typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::process_data_in_hashtable(
        HashTableType& hash_table_ctx, MutableBlock& mutable_block, Block* output_block,
        bool* eos) {
    using Mapped = typename HashTableType::Mapped;
    if constexpr (std::is_same_v<Mapped, RowRefListWithFlag> ||
                  std::is_same_v<Mapped, RowRefListWithFlags>) {
        hash_table_ctx.init_once();
        auto& mcol = mutable_block.mutable_columns();

        bool right_semi_anti_without_other =
                _join_node->_is_right_semi_anti && !_join_node->_have_other_join_conjunct;
        int right_col_idx =
                right_semi_anti_without_other ? 0 : _join_node->_left_table_data_types.size();
        int right_col_len = _join_node->_right_table_data_types.size();

        auto& iter = hash_table_ctx.iter;
        auto block_size = 0;

        auto insert_from_hash_table = [&](uint8_t offset, uint32_t row_num) {
            block_size++;
            for (size_t j = 0; j < right_col_len; ++j) {
                auto& column = *_build_blocks[offset].get_by_position(j).column;
                mcol[j + right_col_idx]->insert_from(column, row_num);
            }
        };

        for (; iter != hash_table_ctx.hash_table.end() && block_size < _batch_size; ++iter) {
            auto& mapped = iter->get_second();
            if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                if (mapped.visited) {
                    for (auto it = mapped.begin(); it.ok(); ++it) {
                        if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN) {
                            insert_from_hash_table(it->block_offset, it->row_num);
                        }
                    }
                } else {
                    for (auto it = mapped.begin(); it.ok(); ++it) {
                        if constexpr (JoinOpType::value != TJoinOp::RIGHT_SEMI_JOIN) {
                            insert_from_hash_table(it->block_offset, it->row_num);
                        }
                    }
                }
            } else {
                for (auto it = mapped.begin(); it.ok(); ++it) {
                    if constexpr (JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN) {
                        if (it->visited) insert_from_hash_table(it->block_offset, it->row_num);
                    } else {
                        if (!it->visited) insert_from_hash_table(it->block_offset, it->row_num);
                    }
                }
            }
        }

        // just resize the left table column in case with other conjunct to make block size is not zero
        if (_join_node->_is_right_semi_anti && _join_node->_have_other_join_conjunct) {
            auto target_size = mcol[right_col_idx]->size();
            for (int i = 0; i < right_col_idx; ++i) {
                mcol[i]->resize(target_size);
            }
        }

        // right outer join / full join need insert data of left table
        if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN ||
                      JoinOpType::value == TJoinOp::FULL_OUTER_JOIN) {
            for (int i = 0; i < right_col_idx; ++i) {
                assert_cast<ColumnNullable*>(mcol[i].get())->insert_many_defaults(block_size);
            }
            _tuple_is_null_left_flags->resize_fill(block_size, 1);
        }
        *eos = iter == hash_table_ctx.hash_table.end();
        output_block->swap(
                mutable_block.to_block(right_semi_anti_without_other ? right_col_idx : 0));
        return Status::OK();
    } else {
        LOG(FATAL) << "Invalid RowRefList";
        return Status::InvalidArgument("Invalid RowRefList");
    }
}
template <class JoinOpType, bool ignore_null>
template <bool need_null_map_for_probe, typename HashTableType>
Status ProcessHashTableProbe<JoinOpType, ignore_null>::do_process(HashTableType& hash_table_ctx,
                                                                  ConstNullMapPtr null_map,
                                                                  MutableBlock& mutable_block,
                                                                  Block* output_block,
                                                                  size_t probe_rows) {
    auto& probe_index = _join_node->_probe_index;
    auto& probe_raw_ptrs = _join_node->_probe_columns;
    if (probe_index == 0 && _items_counts.size() < probe_rows) {
        _items_counts.resize(probe_rows);
    }

    if (_build_block_rows.size() < probe_rows * PROBE_SIDE_EXPLODE_RATE) {
        _build_block_rows.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
        _build_block_offsets.resize(probe_rows * PROBE_SIDE_EXPLODE_RATE);
    }
    using KeyGetter = typename HashTableType::State;
    using Mapped = typename HashTableType::Mapped;

    int right_col_idx =
            _join_node->_is_right_semi_anti ? 0 : _join_node->_left_table_data_types.size();
    int right_col_len = _join_node->_right_table_data_types.size();

    KeyGetter key_getter(probe_raw_ptrs, _join_node->_probe_key_sz, nullptr);
    auto& mcol = mutable_block.mutable_columns();
    int current_offset = 0;

    constexpr auto is_right_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                             JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN;

    constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                               JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

    bool all_match_one = true;
    int last_probe_index = probe_index;
    {
        SCOPED_TIMER(_search_hashtable_timer);
        while (probe_index < probe_rows) {
            if constexpr (ignore_null && need_null_map_for_probe) {
                if ((*null_map)[probe_index]) {
                    if constexpr (probe_all) {
                        _items_counts[probe_index++] = (uint32_t)1;
                        // only full outer / left outer need insert the data of right table
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                        } else {
                            _build_block_offsets.emplace_back(-1);
                            _build_block_rows.emplace_back(-1);
                        }
                        ++current_offset;
                    } else {
                        _items_counts[probe_index++] = (uint32_t)0;
                    }
                    all_match_one = false;
                    continue;
                }
            }
            int last_offset = current_offset;
            auto find_result =
                    !need_null_map_for_probe
                            ? key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena)
                    : (*null_map)[probe_index]
                            ? decltype(key_getter.find_key(hash_table_ctx.hash_table, probe_index,
                                                           _arena)) {nullptr, false}
                            : key_getter.find_key(hash_table_ctx.hash_table, probe_index, _arena);
            if (probe_index + PREFETCH_STEP < probe_rows)
                key_getter.template prefetch<true>(hash_table_ctx.hash_table,
                                                   probe_index + PREFETCH_STEP, _arena);

            if constexpr (JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                          JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN) {
                if (!find_result.is_found()) {
                    ++current_offset;
                }
            } else if constexpr (JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN) {
                if (find_result.is_found()) {
                    ++current_offset;
                }
            } else {
                if (find_result.is_found()) {
                    auto& mapped = find_result.get_mapped();
                    // TODO: Iterators are currently considered to be a heavy operation and have a certain impact on performance.
                    // We should rethink whether to use this iterator mode in the future. Now just opt the one row case
                    if (mapped.get_row_count() == 1) {
                        if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                            mapped.visited = true;
                        }

                        if constexpr (!is_right_semi_anti_join) {
                            if (LIKELY(current_offset < _build_block_rows.size())) {
                                _build_block_offsets[current_offset] = mapped.block_offset;
                                _build_block_rows[current_offset] = mapped.row_num;
                            } else {
                                _build_block_offsets.emplace_back(mapped.block_offset);
                                _build_block_rows.emplace_back(mapped.row_num);
                            }
                            ++current_offset;
                        }
                    } else {
                        for (auto it = mapped.begin(); it.ok(); ++it) {
                            if constexpr (!is_right_semi_anti_join) {
                                if (LIKELY(current_offset < _build_block_rows.size())) {
                                    _build_block_offsets[current_offset] = it->block_offset;
                                    _build_block_rows[current_offset] = it->row_num;
                                } else {
                                    _build_block_offsets.emplace_back(it->block_offset);
                                    _build_block_rows.emplace_back(it->row_num);
                                }
                                ++current_offset;
                            }
                        }
                        if constexpr (std::is_same_v<Mapped, RowRefListWithFlag>) {
                            mapped.visited = true;
                        }
                    }
                } else {
                    if constexpr (probe_all) {
                        // only full outer / left outer need insert the data of right table
                        if (LIKELY(current_offset < _build_block_rows.size())) {
                            _build_block_offsets[current_offset] = -1;
                            _build_block_rows[current_offset] = -1;
                        } else {
                            _build_block_offsets.emplace_back(-1);
                            _build_block_rows.emplace_back(-1);
                        }
                        ++current_offset;
                    }
                }
            }

            uint32_t count = (uint32_t)(current_offset - last_offset);
            _items_counts[probe_index++] = count;
            all_match_one &= (count == 1);
            if (current_offset >= _batch_size && !all_match_one) {
                break;
            }
        }
    }

    {
        SCOPED_TIMER(_build_side_output_timer);
        build_side_output_column(mcol, right_col_idx, right_col_len,
                                 _join_node->_right_output_slot_flags, current_offset);
    }

    if constexpr (JoinOpType::value != TJoinOp::RIGHT_SEMI_JOIN &&
                  JoinOpType::value != TJoinOp::RIGHT_ANTI_JOIN) {
        SCOPED_TIMER(_probe_side_output_timer);
        probe_side_output_column(mcol, _join_node->_left_output_slot_flags, current_offset,
                                 last_probe_index, probe_index - last_probe_index, all_match_one);
    }

    output_block->swap(mutable_block.to_block());

    return Status::OK();
}

template <class JoinOpType, bool ignore_null>
ProcessHashTableProbe<JoinOpType, ignore_null>::ProcessHashTableProbe(HashJoinNode* join_node,
                                                                      int batch_size)
        : _join_node(join_node),
          _batch_size(batch_size),
          _build_blocks(join_node->_build_blocks),
          _tuple_is_null_left_flags(join_node->_is_outer_join
                                            ? &(reinterpret_cast<ColumnUInt8&>(
                                                        *join_node->_tuple_is_null_left_flag_column)
                                                        .get_data())
                                            : nullptr),
          _tuple_is_null_right_flags(
                  join_node->_is_outer_join
                          ? &(reinterpret_cast<ColumnUInt8&>(
                                      *join_node->_tuple_is_null_right_flag_column)
                                      .get_data())
                          : nullptr),
          _rows_returned_counter(join_node->_rows_returned_counter),
          _search_hashtable_timer(join_node->_search_hashtable_timer),
          _build_side_output_timer(join_node->_build_side_output_timer),
          _probe_side_output_timer(join_node->_probe_side_output_timer) {}

template <class JoinOpType, bool ignore_null>
template <bool have_other_join_conjunct>
void ProcessHashTableProbe<JoinOpType, ignore_null>::build_side_output_column(
        MutableColumns& mcol, int column_offset, int column_length,
        const std::vector<bool>& output_slot_flags, int size) {
    constexpr auto is_semi_anti_join = JoinOpType::value == TJoinOp::RIGHT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::RIGHT_SEMI_JOIN ||
                                       JoinOpType::value == TJoinOp::LEFT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN ||
                                       JoinOpType::value == TJoinOp::LEFT_SEMI_JOIN;

    constexpr auto probe_all = JoinOpType::value == TJoinOp::LEFT_OUTER_JOIN ||
                               JoinOpType::value == TJoinOp::FULL_OUTER_JOIN;

    if constexpr (!is_semi_anti_join || have_other_join_conjunct) {
        if (_build_blocks.size() == 1) {
            for (int i = 0; i < column_length; i++) {
                auto& column = *_build_blocks[0].get_by_position(i).column;
                if (output_slot_flags[i]) {
                    mcol[i + column_offset]->insert_indices_from(column, _build_block_rows.data(),
                                                                 _build_block_rows.data() + size);
                } else {
                    mcol[i + column_offset]->insert_many_defaults(size);
                }
            }
        } else {
            for (int i = 0; i < column_length; i++) {
                if (output_slot_flags[i]) {
                    for (int j = 0; j < size; j++) {
                        if constexpr (probe_all) {
                            if (_build_block_offsets[j] == -1) {
                                DCHECK(mcol[i + column_offset]->is_nullable());
                                assert_cast<ColumnNullable*>(mcol[i + column_offset].get())
                                        ->insert_default();
                            } else {
                                auto& column = *_build_blocks[_build_block_offsets[j]]
                                                        .get_by_position(i)
                                                        .column;
                                mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                            }
                        } else {
                            if (_build_block_offsets[j] == -1) {
                                // the only case to reach here:
                                // 1. left anti join with other conjuncts, and
                                // 2. equal conjuncts does not match
                                // since nullptr is emplaced back to visited_map,
                                // the output value of the build side does not matter,
                                // just insert default value
                                mcol[i + column_offset]->insert_default();
                            } else {
                                auto& column = *_build_blocks[_build_block_offsets[j]]
                                                        .get_by_position(i)
                                                        .column;
                                mcol[i + column_offset]->insert_from(column, _build_block_rows[j]);
                            }
                        }
                    }
                } else {
                    mcol[i + column_offset]->insert_many_defaults(size);
                }
            }
        }
    }

    // Dispose right tuple is null flags columns
    if constexpr (probe_all && !have_other_join_conjunct) {
        _tuple_is_null_right_flags->resize(size);
        auto* __restrict null_data = _tuple_is_null_right_flags->data();
        for (int i = 0; i < size; ++i) {
            null_data[i] = _build_block_rows[i] == -1;
        }
    }
}

template <class JoinOpType, bool ignore_null>
template <bool have_other_join_conjunct>
void ProcessHashTableProbe<JoinOpType, ignore_null>::probe_side_output_column(
        MutableColumns& mcol, const std::vector<bool>& output_slot_flags, int size,
        int last_probe_index, size_t probe_size, bool all_match_one) {
    auto& probe_block = _join_node->_probe_block;
    for (int i = 0; i < output_slot_flags.size(); ++i) {
        if (output_slot_flags[i]) {
            auto& column = probe_block.get_by_position(i).column;
            if (all_match_one) {
                DCHECK_EQ(probe_size, column->size() - last_probe_index);
                mcol[i]->insert_range_from(*column, last_probe_index, probe_size);
            } else {
                DCHECK_GE(_items_counts.size(), last_probe_index + probe_size);
                column->replicate(&_items_counts[0], size, *mcol[i], last_probe_index, probe_size);
            }
        } else {
            mcol[i]->insert_many_defaults(size);
        }
    }

    if constexpr (JoinOpType::value == TJoinOp::RIGHT_OUTER_JOIN && !have_other_join_conjunct) {
        _tuple_is_null_left_flags->resize_fill(size, 0);
    }
}

void HashJoinNode::_process_hashtable_ctx_variants_init(RuntimeState* state) {
    std::visit(
            [&](auto&& join_op_variants, auto probe_ignore_null) {
                using JoinOpType = std::decay_t<decltype(join_op_variants)>;
                _process_hashtable_ctx_variants
                        .emplace<ProcessHashTableProbe<JoinOpType, probe_ignore_null>>(
                                this, state->batch_size());
            },
            _join_op_variants, make_bool_variant(_probe_ignore_null));
}

Status HashJoinNode::_process_data_in_hashtable_variants(MutableBlock& mutable_join_block,
                                                         Block* output_block, bool* eos) {
    Status st;
    std::visit(
            [&](auto&& arg, auto&& process_hashtable_ctx) {
                using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                    using HashTableCtxType = std::decay_t<decltype(arg)>;
                    if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                        st = process_hashtable_ctx.process_data_in_hashtable(
                                arg, mutable_join_block, output_block, eos);
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash table";
                    }
                } else {
                    LOG(FATAL) << "FATAL: uninited hash table";
                }
            },
            _hash_table_variants, _process_hashtable_ctx_variants);

    return st;
}

template <bool BuildSide>
bool HashJoinNode::_need_null_map(Block& block, const std::vector<int>& res_col_ids) {
    DCHECK_EQ(_build_expr_ctxs.size(), _probe_expr_ctxs.size());
    for (size_t i = 0; i < _build_expr_ctxs.size(); ++i) {
        if (!_is_null_safe_eq_join[i]) {
            auto column = block.get_by_position(res_col_ids[i]).column.get();
            if constexpr (BuildSide) {
                if (check_and_get_column<ColumnNullable>(*column)) {
                    if (!_store_null_in_hash_table[i]) {
                        return true;
                    }
                }
            } else {
                if (check_and_get_column<ColumnNullable>(*column)) {
                    return true;
                }
            }
        }
    }
    return false;
}

Status HashJoinNode::_process_build_block(RuntimeState* state, Block& block, uint8_t offset) {
    SCOPED_TIMER(_build_table_timer);
    size_t rows = block.rows();
    if (UNLIKELY(rows == 0)) {
        return Status::OK();
    }
    COUNTER_UPDATE(_build_rows_counter, rows);

    ColumnRawPtrs raw_ptrs(_build_expr_ctxs.size());

    ColumnUInt8::MutablePtr null_map_val;
    std::vector<int> res_col_ids(_build_expr_ctxs.size());
    RETURN_IF_ERROR(_do_evaluate(block, _build_expr_ctxs, *_build_expr_call_timer, res_col_ids));
    if (_join_op == TJoinOp::LEFT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
        _convert_block_to_null(block);
    }
    // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
    //  so we have to initialize this flag by the first build block.
    if (!_has_set_need_null_map_for_build) {
        _has_set_need_null_map_for_build = true;
        _need_null_map_for_build =
                _short_circuit_for_null_in_build_side || _need_null_map<true>(block, res_col_ids);
    }
    if (_need_null_map_for_build) {
        null_map_val = ColumnUInt8::create();
        null_map_val->get_data().assign(rows, (uint8_t)0);
    }

    // Get the key column that needs to be built
    Status st = _extract_join_column_variants2(block, null_map_val, raw_ptrs, res_col_ids);
    _hash_table_build(state, block, raw_ptrs, offset, null_map_val);

    return st;
}

Status HashJoinNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    INIT_AND_SCOPE_GET_NEXT_SPAN(state->get_tracer(), _get_next_span, "HashJoinNode::get_next");
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_TIMER(_probe_timer);

    if (_short_circuit_for_null_in_probe_side) {
        // If we use a short-circuit strategy for null value in build side (e.g. if join operator is
        // NULL_AWARE_LEFT_ANTI_JOIN), we should return empty block directly.
        *eos = true;
        return Status::OK();
    }
    size_t probe_rows = _probe_block.rows();
    if ((probe_rows == 0 || _probe_index == probe_rows) && !_probe_eos) {
        _probe_index = 0;
        _prepare_probe_block();

        do {
            SCOPED_TIMER(_probe_next_timer);
            RETURN_IF_ERROR_AND_CHECK_SPAN(
                    child(0)->get_next_after_projects(state, &_probe_block, &_probe_eos),
                    child(0)->get_next_span(), _probe_eos);
        } while (_probe_block.rows() == 0 && !_probe_eos);

        probe_rows = _probe_block.rows();
        if (probe_rows != 0) {
            COUNTER_UPDATE(_probe_rows_counter, probe_rows);
            int probe_expr_ctxs_sz = _probe_expr_ctxs.size();
            _probe_columns.resize(probe_expr_ctxs_sz);

            std::vector<int> res_col_ids(probe_expr_ctxs_sz);
            RETURN_IF_ERROR(_do_evaluate(_probe_block, _probe_expr_ctxs, *_probe_expr_call_timer,
                                         res_col_ids));
            if (_join_op == TJoinOp::RIGHT_OUTER_JOIN || _join_op == TJoinOp::FULL_OUTER_JOIN) {
                _probe_column_convert_to_null = _convert_block_to_null(_probe_block);
            }
            // TODO: Now we are not sure whether a column is nullable only by ExecNode's `row_desc`
            //  so we have to initialize this flag by the first probe block.
            if (!_has_set_need_null_map_for_probe) {
                _has_set_need_null_map_for_probe = true;
                _need_null_map_for_probe = _need_null_map<false>(_probe_block, res_col_ids);
            }
            if (_need_null_map_for_probe) {
                if (_null_map_column == nullptr) {
                    _null_map_column = ColumnUInt8::create();
                }
                _null_map_column->get_data().assign(probe_rows, (uint8_t)0);
            }

            RETURN_IF_ERROR(_extract_join_column_variants(res_col_ids));
        }
    }

    Status st;
    _join_block.clear_column_data();
    MutableBlock mutable_join_block(&_join_block);
    Block temp_block;

    if (_probe_index < _probe_block.rows()) {
        DCHECK(_has_set_need_null_map_for_probe);
        std::visit(
                [&](auto&& arg, auto&& process_hashtable_ctx, auto have_other_join_conjunct,
                    auto need_null_map_for_probe) {
                    using HashTableProbeType = std::decay_t<decltype(process_hashtable_ctx)>;
                    if constexpr (!std::is_same_v<HashTableProbeType, std::monostate>) {
                        using HashTableCtxType = std::decay_t<decltype(arg)>;
                        if constexpr (have_other_join_conjunct) {
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx
                                             .template do_process_with_other_join_conjuncts<
                                                     need_null_map_for_probe>(
                                                     arg,
                                                     need_null_map_for_probe
                                                             ? &_null_map_column->get_data()
                                                             : nullptr,
                                                     mutable_join_block, &temp_block, probe_rows);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        } else {
                            if constexpr (!std::is_same_v<HashTableCtxType, std::monostate>) {
                                st = process_hashtable_ctx
                                             .template do_process<need_null_map_for_probe>(
                                                     arg,
                                                     need_null_map_for_probe
                                                             ? &_null_map_column->get_data()
                                                             : nullptr,
                                                     mutable_join_block, &temp_block, probe_rows);
                            } else {
                                LOG(FATAL) << "FATAL: uninited hash table";
                            }
                        }
                    } else {
                        LOG(FATAL) << "FATAL: uninited hash probe";
                    }
                },
                _hash_table_variants, _process_hashtable_ctx_variants,
                make_bool_variant(_have_other_join_conjunct),
                make_bool_variant(_need_null_map_for_probe));
    } else if (_probe_eos) {
        if (_is_right_semi_anti || (_is_outer_join && _join_op != TJoinOp::LEFT_OUTER_JOIN)) {
            st = _process_data_in_hashtable_variants(mutable_join_block, &temp_block, eos);
        } else {
            *eos = true;
            return Status::OK();
        }
    } else {
        return Status::OK();
    }

    _add_tuple_is_null_column(&temp_block);
    {
        SCOPED_TIMER(_join_filter_timer);
        RETURN_IF_ERROR(
                VExprContext::filter_block(_vconjunct_ctx_ptr, &temp_block, temp_block.columns()));
    }
    RETURN_IF_ERROR(_build_output_block(&temp_block, output_block));
    _reset_tuple_is_null_column();
    reached_limit(output_block, eos);

    return st;
}

} // namespace doris::vectorized