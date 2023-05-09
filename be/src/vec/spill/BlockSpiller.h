#include "common/status.h"
namespace doris::vectorized {

class Block;

struct SpillOptions {
    SpillOptions() : SpillOptions(-1, false) {}
    SpillOptions(int partition_count, bool split_if_partition_size_exceed)
            : is_ordered_(false),
              partition_count_(partition_count),
              split_if_partition_size_exceed_(split_if_partition_size_exceed) {}

    SpillOptions(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
           std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first)
            : is_ordered_(true),
              partition_count_(-1),
              split_if_partition_size_exceed_(false),
              vsort_exec_exprs_(vsort_exec_exprs),
              limit_(limit),
              offset_(offset),
              is_asc_order_(is_asc_order),
              nulls_first_(nulls_first) {}

    // whether sort data block before spill to disk
    bool is_ordered_ = false;
    int partition_count_;
    bool split_if_partition_size_exceed_;
    // maximum data bytes to keep in memory before spill to disk
    // make it a session variable
    size_t spill_data_block_bytes_ = 1024 * 1024 * 100; // 100M
    // maximum data size of a spill partition
    // make it a session variable
    size_t max_partition_bytes_ = 1024 * 1024 * 1024; // 1G

    SortDescription sort_description_;
    VSortExecExprs& vsort_exec_exprs_;
    std::vector<bool>& is_asc_order_;
    std::vector<bool>& nulls_first_;
    int limit_;
    int64_t offset_;
};

// The main class to provide spill block interfaces.
// Set proper spill options to specify whether to sort before spill,
// whether to partition data blocks, the maximum size of data to buffer
// in memory before flush, the maximum size of the spilled file size and etc.
class BlockSpiller {
public:
    BlockSpiller(SpillOptions opts) : opts_(opts) {}

    Status prepare(RuntimeState* state);

    // append a block, partition it if necessary, and spill to disk
    // if reach memory limit
    Status append_block(RuntimeState* state, ThreadPool* thread_pool, Block* block);

    // prepare to read back spilled data
    Status prepare_for_restore();

    Status get_next(Block* block, bool* eos);

    const SpillOptions options() const { return opts_; }

private:
    SpillOptions opts_;

    std::unique_ptr<BlockSpillWriter> writer_;
    std::unique_ptr<BlockSpillReader> reader_;
};

class SpillMemTable {
public:
    virtual Status pre_flush() = 0;

private:
    size_t max_bytes_in_mem_ = 0;
};
using SpillMemTablePtr = std::unique_ptr<SpillMemTable>;

class OrderedSpillMemTable : public SpillMemTable {
public:
    Status pre_flush() override { return _partial_sort(); }

private:
    // sort block_ before flush
    Status _partial_sort();

    Block block_;
    SortDescription sort_description_;
    VSortExecExprs& vsort_exec_exprs_;
    std::vector<bool>& is_asc_order_;
    std::vector<bool>& nulls_first_;
};

class UnOrderedSpillMemTable : public SpillMemTable {
public:
    Status pre_flush() override { return Status::OK(); }
};

class BlockSpillWriter {
public:
    BlockSpillWriter(BlockSpiller* spiller) : spiller_(spiller) {}
    virtual Status prepare(RuntimeState* state) = 0;
    virtual Status append_block(RuntimeState* state, ThreadPool* thread_pool, Block* block) = 0;
    virtual Status flush(RuntimeState* state, ThreadPool* thread_pool) = 0;
    virtual Status get_spill_partitions(std::vector<const SpillPartition*>* partitions) = 0;
    virtual Status prepare_for_restore() = 0;

    bool need_flush() { return unspilled_bytes > opts_.spill_data_block_bytes_; }

    const SpillOptions options() const { return spiller_->options(); }

protected:
    BlockSpiller* spiller_;

    SpillOptions opts_;
    // std::unique_ptr<MemTracker> _mem_tracker;
    size_t unspilled_bytes = 0;
};

class RawBlockSpillWriter : public BlockSpillWriter {
public:
    RawBlockSpillWriter(BlockSpiller* spiller) : BlockSpillWriter(spiller) {}
    Status prepare(RuntimeState* state) override;
    Status append_block(RuntimeState* state, ThreadPool* thread_pool, Block* block) override;
    Status flush(RuntimeState* state, ThreadPool* thread_pool) override;

    Status prepare_for_restore() override;

    Status get_spill_partitions(std::vector<const SpillPartition*>* partitions) override {
        return Status::OK();
    }

private:
    SpillMemTablePtr spill_mem_table_;
    std::vector<std::string> spilled_files_;
};

struct SpillPartition {
    bool spilled_ = false;
    std::unique_ptr<RawBlockSpillWriter> spill_writer_;
};
using SpillPartitionPtr = std::unique_ptr<SpillPartition>;

class PartitionedBlockSpillWriter : public BlockSpillWriter {
public:
    PartitionedBlockSpillWriter(BlockSpiller* spiller, int partition_count)
        : BlockSpillWriter(spiller), partition_count_(partition_count) {}

    Status prepare(RuntimeState* state) override;

    Status append_block(RuntimeState* state, ThreadPool* thread_pool, Block* block) override;

    Status flush(RuntimeState* state, ThreadPool* thread_pool) override;

    Status prepare_for_restore() override;

    Status get_spill_partitions(std::vector<const SpillPartition*>& partitions) override {
        for (const auto& partition : spill_partitions_) {
            partitions.push_back(partition.get());
        }
        return Status::OK();
    }

private:
    Status _init_partitions();

    std::vector<SpillPartitionPtr> spill_partitions_;
    int partition_count_;
};

class BlockSpillReader {

};

} // namespace doris::vectorized