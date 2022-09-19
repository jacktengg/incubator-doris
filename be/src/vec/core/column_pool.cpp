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

#include <sanitizer/asan_interface.h>

#include <list>
#include <mutex>

#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "util/cpu_info.h"
#include "util/spinlock.h"
#include "vec/core/column_pool.h"
#include "vec/core/block.h"

namespace doris {

namespace vectorized {

using namespace std;

ColumnAllocator* ColumnAllocator::_s_instance = nullptr;

class ColumnPoolArena;

template <typename T>
class ColumnPool2 {

    friend class ColumnPoolArena;

private:
    // void set_mem_tracker(MemTracker* mem_tracker) { _mem_tracker = mem_tracker; }
    // MemTracker* mem_tracker() { return _mem_tracker; }
    ColumnPool2() = default;

    bool has_free_column() const {
        return !_column_list.empty();
    }

    template <typename... Args>
    MutableColumnPtr get_column(size_t block_size, Args&&... args) {
        LOG(INFO) << "ColumnPool2::get_column(";
        if (!_column_list.empty()) {
            T* col = _column_list.back();
            _column_list.pop_back();
            return T::from_raw_ptr(col, true);
        } else {
            return T::create_pooled(block_size, std::forward<Args>(args)...);
        }
    }

    void return_column(T* col, size_t block_size) {
        LOG(INFO) << "ColumnPool2::return_column(";
        col->clear();
        if (UNLIKELY(col->capacity() > block_size)) {
            delete col;
            return;
        }
        _column_list.push_back(col);
    }

private:

    std::vector<T*> _column_list;
};

class ColumnPoolArena {
    friend class ColumnAllocator;

private:
    static constexpr int TRY_LOCK_TIMES = 3;
    SpinLock _lock;

private:
    template<typename T>
    static ColumnPool2<T>* column_pool_instance() {
        static ColumnPool2<T> p;
        return &p;
    }

    Block* new_block(int core_id, const std::vector<SlotDescriptor*>& slots, std::size_t block_size) {
#define M_DECIMAL(NAME)   \
    case TYPE_##NAME: {   \
        if (config::enable_decimalv3) { \
            using T = PrimitiveTypeTraits<TYPE_##NAME>::ColumnType; \
            column_ptr = \
                T::create_pooled(block_size, block_size, slot_desc->type().scale); \
        } else { \
            column_ptr = \
                ColumnDecimal128::create_pooled(block_size, block_size, slot_desc->type().scale); \
            column_ptr->set_decimalv2_type(); \
        } \
        break; \
    }

#define APPLY_FOR_DECIMAL_TYPE(M_DECIMAL) \
    M_DECIMAL(DECIMALV2)                  \
    M_DECIMAL(DECIMAL32)                  \
    M_DECIMAL(DECIMAL64)                  \
    M_DECIMAL(DECIMAL128)

#define M(NAME)          \
    case TYPE_##NAME: {  \
        using T = PrimitiveTypeTraits<TYPE_##NAME>::ColumnType; \
        column_ptr = T::create_pooled(block_size); \
        break; \
    }

#define APPLY_FOR_PRIMITIVE_TYPE(M) \
    M(TINYINT)                      \
    M(SMALLINT)                     \
    M(INT)                          \
    M(BIGINT)                       \
    M(LARGEINT)                     \
    M(CHAR)                         \
    M(DATE)                         \
    M(DATETIME)                     \
    M(DATEV2)                       \
    M(DATETIMEV2)                   \
    M(VARCHAR)                      \
    M(STRING)                       \
    M(HLL)                          \
    M(BOOLEAN)

        Block* block = new Block();
        for (const auto slot_desc : slots) {
            MutableColumnPtr column_ptr;

            switch (slot_desc->type().type) {

                APPLY_FOR_DECIMAL_TYPE(M_DECIMAL)

                APPLY_FOR_PRIMITIVE_TYPE(M)

                default: {
                    VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slot_desc->col_name()
                                  << "]";
                    break;
                }
            }

            if (slot_desc->is_nullable()) {
                column_ptr->set_core_id(core_id);
                column_ptr = std::move(ColumnNullable::create_pooled(block_size, std::move(column_ptr), ColumnUInt8::create()));
            }
            column_ptr->set_core_id(core_id);
            column_ptr->reserve(block_size);
            block->insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                                slot_desc->col_name()));
        }
#undef M_DECIMAL

#undef M

        return block;
    }

    Block* get_block(int core_id, const std::vector<SlotDescriptor*>& slots, std::size_t block_size) {

#define M_DECIMAL(NAME)   \
    case TYPE_##NAME: {   \
        if (config::enable_decimalv3) { \
            column_ptr = \
                ColumnPoolArena::column_pool_instance<PrimitiveTypeTraits<TYPE_##NAME>::ColumnType>()->get_column( \
                    block_size, block_size, slot_desc->type().scale); \
        } else { \
            column_ptr = ColumnPoolArena::column_pool_instance<ColumnDecimal128>()->get_column( \
                block_size, block_size, slot_desc->type().scale); \
            column_ptr->set_decimalv2_type(); \
        } \
        break; \
    }

#define APPLY_FOR_DECIMAL_TYPE(M_DECIMAL) \
    M_DECIMAL(DECIMALV2)                  \
    M_DECIMAL(DECIMAL32)                  \
    M_DECIMAL(DECIMAL64)                  \
    M_DECIMAL(DECIMAL128)

#define M(NAME)          \
    case TYPE_##NAME: {  \
        column_ptr = ColumnPoolArena::column_pool_instance<PrimitiveTypeTraits<TYPE_##NAME>::ColumnType>()->get_column(block_size); \
        break; \
    }

#define APPLY_FOR_PRIMITIVE_TYPE(M) \
    M(TINYINT)                      \
    M(SMALLINT)                     \
    M(INT)                          \
    M(BIGINT)                       \
    M(LARGEINT)                     \
    M(CHAR)                         \
    M(DATE)                         \
    M(DATETIME)                     \
    M(DATEV2)                       \
    M(DATETIMEV2)                   \
    M(VARCHAR)                      \
    M(STRING)                       \
    M(HLL)                          \
    M(BOOLEAN)

        Block* block = nullptr;

        for (int i = 0; i < TRY_LOCK_TIMES; ++i) {
            if (_lock.try_lock()) {

                block = new Block();
                for (const auto slot_desc : slots) {
                    MutableColumnPtr column_ptr;

                    switch (slot_desc->type().type) {

                        APPLY_FOR_DECIMAL_TYPE(M_DECIMAL)

                        APPLY_FOR_PRIMITIVE_TYPE(M)

                        default: {
                            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slot_desc->col_name()
                                          << "]";
                            break;
                        }
                    }

                    if (slot_desc->is_nullable()) {
                        column_ptr->set_core_id(core_id);
                        column_ptr = std::move(ColumnNullable::create_pooled(block_size, std::move(column_ptr), ColumnUInt8::create()));
                    }
                    column_ptr->set_core_id(core_id);
                    column_ptr->reserve(block_size);
                    block->insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name()));
                }
            }
        }

#undef M_DECIMAL

#undef M

        if (nullptr == block) {
            block = new_block(core_id, slots, block_size);
        }
        return block;
    }

    template <typename T>
    void return_column(T* col, std::size_t block_size) {
        ColumnPoolArena::column_pool_instance<T>()->return_column(col, block_size);
    }
};

template <typename T>
inline void return_pooled_column2(T* col, std::size_t block_size) {
    ColumnAllocator::instance()->return_pooled_column(col, block_size);
}

void ColumnAllocator::init_instance(std::size_t reserve_limit) {
    if (_s_instance != nullptr) return;
    _s_instance = new ColumnAllocator(reserve_limit);
}

ColumnAllocator::ColumnAllocator(size_t reserve_limit)
    : _reserve_bytes_limit(reserve_limit),
      _reserved_bytes(0),
      _arenas(CpuInfo::get_max_num_cores()) {
    for (int i = 0; i < _arenas.size(); ++i) {
        _arenas[i].reset(new ColumnPoolArena());
    }
}

template <typename T>
void ColumnAllocator::return_pooled_column(T* col, std::size_t block_size) {
    int core_id = col->get_core_id();
    DCHECK(core_id != -1);
    _arenas[core_id]->return_column(col, block_size);
}

Block* ColumnAllocator::allocate_block(const std::vector<SlotDescriptor*>& slots, std::size_t block_size) {
    int core_id = CpuInfo::get_current_core();
    return _arenas[core_id]->get_block(core_id, slots, block_size);
}

} // namespace vectorized
} // namespace doris