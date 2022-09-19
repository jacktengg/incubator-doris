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
#include "vec/core/block_column_pool.h"
#include "vec/core/block.h"

namespace doris {

namespace vectorized {

using namespace std;

BlockPool* BlockPool::_s_instance = nullptr;

template <typename T>
class ColumnArena {
    int TRY_LOCK_TIMES = 3;

    SpinLock _lock;
    std::vector<T*> _column_list;

public:
    template <typename... Args>
    MutableColumnPtr get_column(size_t block_size, Args&&... args) {
        if (_column_list.empty()) {
            return T::create_pooled(block_size, std::forward<Args>(args)...);
        } else {
            for (int i = 0; i < TRY_LOCK_TIMES; ++i) {
                if (_lock.try_lock()) {
                    if (_column_list.empty()) {
                        _lock.unlock();
                        return T::create_pooled(block_size, std::forward<Args>(args)...);
                    } else {
                        T* col = _column_list.back();
                        _column_list.pop_back();
                        // ASAN_UNPOISON_MEMORY_REGION(col, size);
                        _lock.unlock();
                        return T::from_raw_ptr(col, true);
                    }
                }
            }
            return T::create_pooled(block_size, std::forward<Args>(args)...);
        }
    }

    void return_column(T* col, size_t block_size) {
        // Poison this chunk to make asan can detect invalid access
        // ASAN_POISON_MEMORY_REGION(col, size);
        std::lock_guard<SpinLock> l(_lock);
        _column_list.push_back(col);
    }
};

template <typename T>
class ColumnPool2 {

private:
    ColumnPool2()
        : _arenas(CpuInfo::get_max_num_cores()) {

        for (int i = 0; i < _arenas.size(); ++i) {
            _arenas[i].reset(new ColumnArena<T>());
        }
    }

public:
    // void set_mem_tracker(MemTracker* mem_tracker) { _mem_tracker = mem_tracker; }
    // MemTracker* mem_tracker() { return _mem_tracker; }

    static ColumnPool2* instance() {
        static ColumnPool2 p;
        return &p;
    }

    template <typename... Args>
    MutableColumnPtr get_column(size_t block_size, Args&&... args) {
        LOG(INFO) << "ColumnPool2::get_column(";
        int core_id = CpuInfo::get_current_core();
        auto col = _arenas[core_id]->get_column(block_size, std::forward<Args>(args)...);
        return col;
    }

    void return_column(T* col, size_t block_size) {
        LOG(INFO) << "ColumnPool2::return_column(";
        col->clear();
        if (UNLIKELY(col->capacity() > block_size)) {
            delete col;
            return;
        }
        auto bytes = col->byte_size();
        int core_id = CpuInfo::get_current_core();
        _arenas[core_id]->return_column(col, bytes);
    }

private:

    ~ColumnPool2() = default;

    // each core has a ColumnArena
    std::vector<std::unique_ptr<ColumnArena<T>>> _arenas;
};

template <typename T, typename... Args>
inline MutableColumnPtr get_pooled_column2(std::size_t block_size, Args&&... args) {
    return ColumnPool2<T>::instance()->template get_column(block_size, std::forward<Args>(args)...);
}

template <typename T>
inline void return_pooled_column2(T* col, std::size_t block_size) {
    ColumnPool2<T>::instance()->return_column(col, block_size);
}

void BlockPool::init_instance(std::size_t reserve_limit) {
    if (_s_instance != nullptr) return;
    _s_instance = new BlockPool(reserve_limit);
}

BlockPool::BlockPool(size_t reserve_limit)
    : _reserve_bytes_limit(reserve_limit) {
}

Block* BlockPool::allocate_block(const std::vector<SlotDescriptor*>& slots, std::size_t block_size) {
    Block* block = new Block();

    for (const auto slot_desc : slots) {
        MutableColumnPtr column_ptr;

        switch (slot_desc->type().type) {

#define M_DECIMAL(NAME)   \
    case TYPE_##NAME: {   \
        using DecimalValueType = PrimitiveTypeTraits<TYPE_##NAME>::CppType; \
        auto data_type = assert_cast<DataTypeDecimal<DecimalValueType>>(slot_desc->get_data_type_ptr()); \
        if (config::enable_decimalv3) { \
            column_ptr = \
                get_pooled_column2<PrimitiveTypeTraits<TYPE_##NAME>::ColumnType>( \
                    block_size, data_type->get_scale()); \
        } else { \
            column_ptr = get_pooled_column2<ColumnDecimal128>( \
                block_size, data_type->get_scale()); \
            column_ptr->set_decimalv2_type(); \
        } \
        break; \
    }

#define APPLY_FOR_DECIMAL_TYPE(M_DECIMAL) \
    M_DECIMAL(DECIMAL32)                  \
    M_DECIMAL(DECIMAL64)                  \
    M_DECIMAL(DECIMAL128)                 \
    M_DECIMAL(DECIMALV2)                  \
            APPLY_FOR_DECIMAL_TYPE(M_DECIMAL)

#undef M_DECIMAL

#define M(NAME)          \
    case TYPE_##NAME: {  \
        column_ptr = get_pooled_column2<PrimitiveTypeTraits<TYPE_##NAME>::ColumnType>(block_size); \
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

            APPLY_FOR_PRIMITIVE_TYPE(M)
#undef M

        default: {
            VLOG_CRITICAL << "Unsupported Normalize Slot [ColName=" << slot_desc->col_name()
                          << "]";
            break;
        }

        if (slot_desc->is_nullable()) {
            column_ptr = std::move(ColumnNullable::create(std::move(column_ptr), ColumnUInt8::create()));
        }
        column_ptr->reserve(block_size);
        block->insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                            slot_desc->col_name()));
        } // end of switch
    }

    return block;
}

} // namespace vectorized
} // namespace doris