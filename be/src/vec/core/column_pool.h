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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace doris {

class SlotDescriptor;

namespace vectorized {
class Block;
class ColumnPoolArena;

class ColumnAllocator {
public:
    static void init_instance(std::size_t reserve_limit);

    static ColumnAllocator* instance() { return _s_instance; }

    Block* allocate_block(const std::vector<SlotDescriptor*>& slots, std::size_t block_size);

    template <typename T>
    inline void return_pooled_column(T* col, std::size_t block_size);

private:
    ColumnAllocator(std::size_t reserve_limit);

private:
    static ColumnAllocator* _s_instance;
    size_t _reserve_bytes_limit;
    std::atomic<int64_t> _reserved_bytes;
    std::vector<std::unique_ptr<ColumnPoolArena>> _arenas;

};

} // namespace vectorized
} // namespace doris