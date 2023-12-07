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
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "olap/data_dir.h"
#include "runtime/exec_env.h"
#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {
class RuntimeProfile;
class DataDir;

namespace vectorized {

class Block;

using flush_stream_callback = std::function<void(const Status&)>;

class SpillStream;

class SpillableBlock {
public:
    SpillableBlock() : spilled_(false) {}
    SpillableBlock(Block&& block) : block_(block), spilled_(false) {}
    Block& get_block() { return block_; }

private:
    friend class SpillStream;

    Block block_;
    std::atomic_bool spilled_ = false;
    // int fd_ = -1;
    size_t spill_data_size_ = 0;
    int64_t offset_ = -1;
};
using SpillableBlockSPtr = std::shared_ptr<SpillableBlock>;
enum class SpillState : uint8_t { NOT_SPILL, SPILLING, SPILL_FINISHED };
class SpillStream {
public:
    SpillStream(int64_t stream_id, doris::DataDir* data_dir, std::string spill_dir,
                size_t batch_rows, size_t batch_bytes, RuntimeProfile* profile);

    void unpin();

    // add rows in block to the spill stream
    Status add_rows(Block* block, const std::vector<uint32_t>& rows, bool pin, bool eos);

    // add blocks to blocks_
    Status add_blocks(std::vector<Block>&& blocks, bool pin);

    Status add_block(Block&& block, bool pin);

    Status done_write();

    bool has_next() const { return !eos_; }

    Status get_next(Block* block, bool* eos, bool async = true);

    Status get_spill_state(SpillState& state);

    bool is_reading();

    bool is_spilled() const { return spilled_; }

    int64_t id() const { return stream_id_; }

    int64_t total_bytes() const { return total_bytes_; }

    Status seek_for_read(size_t block_index);

    DataDir* get_data_dir() const { return data_dir_; }
    const std::string& get_spill_root_dir() const { return data_dir_->path(); }

    const std::string& get_spill_dir() const { return spill_dir_; }

    size_t get_written_bytes() const { return writer_->get_written_bytes(); }

    Status prepare_spill();

    // void spill();

    Status spill_block(const Block& block, bool eof);

    // Status spill_async_and_wait();

    void end_spill(const Status& status);

    Status read_current_block_sync(Block* block, bool* eos);

private:
    friend class SpillStreamManager;

    Status prepare();

    void close();

    bool _block_reach_limit() const {
        return mutable_block_->rows() > batch_rows_ || mutable_block_->bytes() > batch_bytes_;
    }

    SpillableBlockSPtr _get_next_spilled_block();

    Status _get_next_spilled_async();

    Status _get_next_spilled_sync(Block* block);

    ThreadPool* io_thread_pool_;
    std::unique_ptr<MutableBlock> mutable_block_;
    int64_t stream_id_;
    std::atomic_bool closed_ = false;
    doris::DataDir* data_dir_ = nullptr;
    std::string spill_dir_;
    int fd_ = -1;
    size_t batch_rows_;
    size_t batch_bytes_;
    int64_t total_rows_ = 0;
    int64_t total_bytes_ = 0;
    std::atomic_bool spilled_ = false;
    std::atomic_bool eos_ = false;

    // protect in_mem_blocks_, dirty_blocks_ and spilled_blocks_
    std::mutex lock_;
    std::unique_ptr<std::promise<Status>> spill_promise_;
    std::future<Status> spill_future_;
    std::unique_ptr<std::promise<Status>> read_promise_;
    std::future<Status> read_future_;
    std::deque<SpillableBlockSPtr> in_mem_blocks_;
    std::deque<SpillableBlockSPtr> dirty_blocks_;
    std::deque<SpillableBlockSPtr> spilled_blocks_;

    SpillWriterUPtr writer_;
    SpillReaderUPtr reader_;

    RuntimeProfile* profile_ = nullptr;
};
using SpillStreamSPtr = std::shared_ptr<SpillStream>;
} // namespace vectorized
} // namespace doris