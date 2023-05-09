#include "vec/spill/BlockSpill.h"
namespace doris::vectorized {
Status BlockSpiller::prepare(RuntimeState* state) {
    if (opts_.partition_count_ > 0) {
        writer_ = std::make_unique<PartitionedBlockSpillWriter>(this, opts_.partition_count_);
    } else {
        writer_ = std::make_unique<RawBlockSpillWriter>(this);
    }
    writer_->prepare(state);
    return Status::OK();
}

Status BlockSpiller::append_block(RuntimeState* state, ThreadPool* thread_pool, Block* block) {
    return writer_->append_block(state, thread_pool, block);
}

Status BlockSpiller::prepare_for_restore() {
    writer_->prepare_for_restore();
    return Status::OK();
}

Status BlockSpiller::get_next(Block* block, bool* eos) {
    return Status::OK();
}

Status RawBlockSpillWriter::prepare(RuntimeState* state) {
    if (options().is_ordered_) {
        spill_mem_table_ = std::make_unique<OrderedSpillMemTable>();
    } else {
        spill_mem_table_ = std::make_unique<UnOrderedSpillMemTable>();
    }
    return Status::OK();
}

Status RawBlockSpillWriter::append_block(RuntimeState* state, ThreadPool* thread_pool,
                                         Block* block) {
    // 1. append block to spill_mem_table_

    // 2. flush if mem table is full
    if (need_flush()) {
        RETURN_IF_ERROR(flush(state, thread_pool));
    }
    return Status::OK();
}

Status RawBlockSpillWriter::flush(RuntimeState* state, ThreadPool* thread_pool) {
    // 1. sort current block
    spill_mem_table_->pre_flush();

    // 2. flush to disk asynchronously
    RETURN_IF_ERROR(thread_pool->submit_func([this] {
        // serialize block and write to disk
    }));

    return Status::OK();
}

Status OrderedSpillMemTable::_partial_sort() {
    return Status::OK();
}

Status RawBlockSpillWriter::prepare_for_restore() {
    // build merge sort tree
    // k-way merge
    return Status::OK();
}

Status PartitionedBlockSpillWriter::prepare(RuntimeState* state) {
    _init_partitions();
    return Status::OK();
}

Status PartitionedBlockSpillWriter::_init_partitions() {
    for (int i = 0; i < partition_count_; ++i) {
        spill_partitions_.emplace_back(std::make_unique<SpillPartition>());
        spill_partitions_.back().get()->spill_writer_ = std::make_unique<RawBlockSpillWriter>();
    }
    return Status::OK();
}

Status PartitionedBlockSpillWriter::append_block(RuntimeState* state, ThreadPool* thread_pool,
                                                 Block* block) {
    // split block into partitions

    if (need_flush()) {
        RETURN_IF_ERROR(flush(state, thread_pool));
    }
    return Status::OK();
}

Status PartitionedBlockSpillWriter::flush(RuntimeState* state, ThreadPool* thread_pool) {
    return Status::OK();
}
} // namespace doris::vectorized