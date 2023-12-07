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

#include "vec/spill/spill_stream.h"

#include <glog/logging.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/spill/spill_reader.h"
#include "vec/spill/spill_writer.h"

namespace doris {

namespace vectorized {
Status SpillStream::prepare() {
    writer_.reset(new SpillWriter(stream_id_, batch_rows_, spill_dir_, profile_));

    reader_.reset(new SpillReader(stream_id_, writer_->get_file_path(), profile_));
    return Status::OK();
}

void SpillStream::close() {
    if (closed_) {
        return;
    }
    LOG(WARNING) << "spill stream closing: " << stream_id_;
    closed_ = true;
    if (spill_promise_) {
        spill_future_.wait();
        spill_promise_.reset();
    }
    if (read_promise_) {
        read_future_.wait();
        read_promise_.reset();
    }

    (void)writer_->close();
    (void)reader_->close();
}
Status SpillStream::add_rows(Block* block, const std::vector<uint32_t>& rows, bool pin, bool eos) {
    if (mutable_block_ == nullptr) {
        mutable_block_ = MutableBlock::create_unique(block->clone_empty());
    }

    total_rows_ += rows.size();

    auto pre_bytes_ = mutable_block_->bytes();
    RETURN_IF_CATCH_EXCEPTION(mutable_block_->add_rows(block, &rows[0], &rows[0] + rows.size()));
    auto new_bytes = mutable_block_->bytes();
    total_bytes_ += new_bytes - pre_bytes_;
    if (_block_reach_limit() || eos) {
        auto new_block = mutable_block_->to_block();
        {
            std::lock_guard l(lock_);
            if (pin) {
                in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
            } else {
                dirty_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
            }
        }
        mutable_block_ = MutableBlock::create_unique(block->clone_empty());
    }
    return Status::OK();
}

Status SpillStream::add_blocks(std::vector<Block>&& blocks, bool pin) {
    std::lock_guard l(lock_);
    if (pin) {
        for (auto& block : blocks) {
            in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
        }
    } else {
        for (auto& block : blocks) {
            dirty_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
        }
    }
    return Status::OK();
}
Status SpillStream::add_block(Block&& block, bool pin) {
    std::lock_guard l(lock_);
    if (pin) {
        in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
    } else {
        dirty_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(block)));
    }
    return Status::OK();
}

Status SpillStream::done_write() {
    if (mutable_block_ && mutable_block_->rows() > 0) {
        auto new_block = mutable_block_->to_block();
        {
            std::lock_guard l(lock_);
            in_mem_blocks_.push_back(std::make_shared<SpillableBlock>(std::move(new_block)));
        }
    }
    mutable_block_.reset();
    return Status::OK();
}

void SpillStream::unpin() {
    std::lock_guard l(lock_);
    dirty_blocks_.insert(dirty_blocks_.end(), in_mem_blocks_.cbegin(), in_mem_blocks_.cend());
    in_mem_blocks_.clear();
}

void SpillStream::spill() {
    DCHECK(!spill_promise_);

    Status status;

    spilled_ = true;
    spill_promise_ = std::make_unique<std::promise<Status>>();
    spill_future_ = spill_promise_->get_future();
    LOG(WARNING) << "spill stream start spilling: stream " << stream_id_;

    status = writer_->open();
    if (!status.ok()) {
        spill_promise_->set_value(status);
        LOG(WARNING) << "spill stream open failed: " << stream_id_;
        return;
    }
    fd_ = writer_->get_fd();

    std::lock_guard l(lock_);
    for (auto& block : dirty_blocks_) {
        if (closed_) {
            LOG(WARNING) << "spill stream closed: " << stream_id_;
            break;
        }
        block->spilled_ = true;
        block->fd_ = fd_;
        block->offset_ = writer_->get_written_bytes();
        status = writer_->write(block->block_, block->spill_data_size_);
        if (!status.ok()) {
            spill_promise_->set_value(status);
            LOG(WARNING) << "spill stream write failed: " << stream_id_;
            return;
        }
        block->block_.swap(Block());
        spilled_blocks_.emplace_back(block);
    }
    dirty_blocks_.clear();
    status = writer_->close();
    spill_promise_->set_value(status);
    LOG(WARNING) << "spill stream spill finished: stream " << stream_id_;
}

bool SpillStream::is_spilling() {
    if (spill_promise_) {
        LOG(WARNING) << "check spill stream: " << stream_id_;
        auto status = spill_future_.wait_for(std::chrono::milliseconds(10));
        if (status == std::future_status::ready) {
            auto status = spill_future_.get();
            LOG(WARNING) << "spill stream spilling ready: " << stream_id_ << ", status: " << status;
            spill_promise_.reset();
            return false;
        } else {
            LOG(WARNING) << "spill stream spilling NOT ready: " << stream_id_;
            return true;
        }
    }
    LOG(WARNING) << "spill stream is not spilling: " << stream_id_;
    return false;
}
size_t SpillStream::spillable_data_size() {
    size_t size = 0;
    std::lock_guard l(lock_);
    for (const auto& block : in_mem_blocks_) {
        size += block->block_.allocated_bytes();
    }
    for (const auto& block : dirty_blocks_) {
        size += block->block_.allocated_bytes();
    }
    return size;
}

Status SpillStream::get_next(Block* block, bool* eos, bool async) {
    std::lock_guard l(lock_);
    if (in_mem_blocks_.empty() && dirty_blocks_.empty() && spilled_blocks_.empty()) {
        *eos = true;
        eos_ = true;
        LOG(WARNING) << "SpillStream::get_next, no blocks: " << stream_id_;
        return Status::OK();
    }
    if (!in_mem_blocks_.empty()) {
        *block = std::move(in_mem_blocks_.front()->get_block());
        in_mem_blocks_.pop_front();
        LOG(WARNING) << "SpillStream::get_next, get in mem block: " << stream_id_;
        return Status::OK();
    } else if (!dirty_blocks_.empty()) {
        *block = std::move(dirty_blocks_.front()->get_block());
        dirty_blocks_.pop_front();
        LOG(WARNING) << "SpillStream::get_next, get dirty block: " << stream_id_;
        return Status::OK();
    } else {
        // initiate async read
        if (async) {
            RETURN_IF_ERROR(_read_async());
            LOG(WARNING) << "SpillStream::get_next, reading spilled block: " << stream_id_;
            return Status::WaitForIO("reading spilled blocks");
        } else {
            LOG(WARNING) << "SpillStream::get_next, read sync: " << stream_id_;
            return _read_sync(block);
        }
    }
}

Status SpillStream::get_next_sync(Block* block, bool* eos) {
    return get_next(block, eos, false);
}
SpillableBlockSPtr SpillStream::_get_next_spilled_block() {
    if (spilled_blocks_.empty()) {
        return nullptr;
    }
    auto block = spilled_blocks_.front();
    spilled_blocks_.pop_front();
    return block;
}

Status SpillStream::_read_sync(Block* block) {
    RETURN_IF_ERROR(reader_->open());

    Status st;
    auto spilled_block = _get_next_spilled_block();
    if (spilled_block) {
        st = reader_->read_at_offset(spilled_block->offset_, spilled_block->spill_data_size_,
                                     &spilled_block->block_);
        RETURN_IF_ERROR(st);
        *block = spilled_block->get_block();
    }

    return Status::OK();
}

bool SpillStream::is_reading() {
    if (read_promise_) {
        auto status = read_future_.wait_for(std::chrono::milliseconds(10));
        if (status == std::future_status::ready) {
            auto status = read_future_.get();
            LOG(WARNING) << "spill stream read finished, status: " << status;
            read_promise_.reset();
            return false;
        } else {
            LOG(WARNING) << "spill stream read NOT ready";
            return true;
        }
    }
    LOG(WARNING) << "spill stream is not reading";
    return false;
}

Status SpillStream::_read_async() {
    DCHECK(!read_promise_);
    RETURN_IF_ERROR(reader_->open());

    read_promise_ = std::make_unique<std::promise<Status>>();
    read_future_ = read_promise_->get_future();
    auto status = io_thread_pool_->submit_func([this] {
        Status st;
        auto spilled_block = _get_next_spilled_block();
        while (spilled_block && !closed_) {
            st = reader_->read_at_offset(spilled_block->offset_, spilled_block->spill_data_size_,
                                         &spilled_block->block_);
            if (!st.ok()) {
                break;
            }
            {
                std::lock_guard l(lock_);
                in_mem_blocks_.push_back(spilled_block);
            }
            spilled_block = _get_next_spilled_block();
        }
        read_promise_->set_value(st);
    });
    return status;
}

bool SpillStream::has_in_memory_blocks() {
    std::lock_guard l(lock_);
    return !in_mem_blocks_.empty() || !dirty_blocks_.empty();
}

} // namespace vectorized
} // namespace doris