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

#include <cstdint>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/compute/exec/partition_util.h"
#include "arrow/compute/exec/spilling_util.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/light_array.h"

namespace arrow {
namespace compute {

/// \brief A container that accumulates batches until they are ready to
///        be processed.
class AccumulationQueue {
 public:
  AccumulationQueue() = default;
  ~AccumulationQueue() = default;

  // We should never be copying ExecBatch around
  AccumulationQueue(const AccumulationQueue&) = delete;
  AccumulationQueue& operator=(const AccumulationQueue&) = delete;

  AccumulationQueue(AccumulationQueue&& that);
  AccumulationQueue& operator=(AccumulationQueue&& that);

  void Concatenate(AccumulationQueue&& that);
  void InsertBatch(ExecBatch batch);
  void SetBatch(size_t idx, ExecBatch batch);
  size_t batch_count() const { return batches_.size(); }
  bool empty() const { return batches_.empty(); }
  size_t CalculateRowCount() const;

  // Resizes the accumulation queue to contain size batches. The
  // new batches will be empty and have length 0, but they will be
  // usable (useful for concurrent modification of the AccumulationQueue
  // of separate elements).
  void Resize(size_t size) { batches_.resize(size); }
  void Clear();
  ExecBatch& operator[](size_t i) { return batches_[i]; }
  const ExecBatch& operator[](size_t i) const { return batches_[i]; }

 private:
  std::vector<ExecBatch> batches_;
};

/// Accumulates batches in a queue that can be spilled to disk if needed
///
/// Each batch is partitioned by the lower bits of the hash column (which must be present)
/// and rows are initially accumulated in batch builders (one per partition).  As a batch
/// builder fills up the completed batch is put into an in-memory accumulation queue (per
/// partition).
///
/// When memory pressure is encountered the spilling queue's "spill cursor" can be
/// advanced.  This will cause a partition to be spilled to disk.  Any future data
/// arriving for that partition will go immediately to disk (after accumulating a full
/// batch in the batch builder). Note that hashes are spilled separately from batches and
/// have their own cursor. We assume that the Batch cursor is advanced faster than the
/// spill cursor. Hashes are spilled separately to enable building a Bloom filter for
/// spilled partitions.
///
/// Later, data is retrieved one partition at a time.  Partitions that are in-memory will
/// be delivered immediately in new thread tasks.  Partitions that are on disk will be
/// read from disk and delivered as they arrive.
///
/// This class assumes that data is fully accumulated before it is read-back. As such, do
/// not call InsertBatch after calling GetPartition.
class SpillingAccumulationQueue {
 public:
  // Number of partitions must be a power of two, since we assign partitions by
  // looking at bottom few bits.
  static constexpr int kLogNumPartitions = 6;
  static constexpr int kNumPartitions = 1 << kLogNumPartitions;
  Status Init(QueryContext* ctx);
  // Assumes that the final column in batch contains 64-bit hashes of the columns.
  Status InsertBatch(size_t thread_index, ExecBatch batch);
  // Runs `on_batch` on each batch in the SpillingAccumulationQueue for the given
  // partition. Each batch will have its own task. Once all batches have had their
  // on_batch function run, `on_finished` will be called.
  Status GetPartition(size_t thread_index, size_t partition_idx,
                      std::function<Status(size_t, size_t, ExecBatch)>
                          on_batch,  // thread_index, batch_index, batch
                      std::function<Status(size_t)> on_finished);

  // Returns hashes of the given partition and batch index.
  // partition MUST be at least hash_cursor, as if partition < hash_cursor,
  // these hashes will have been deleted.
  const uint64_t* GetHashes(size_t partition_idx, size_t batch_idx);
  inline size_t batch_count(size_t partition_idx) const {
    const Partition& partition = partitions_[partition_idx];
    size_t num_full_batches = partition_idx >= spilling_cursor_
                                  ? partition.queue.batch_count()
                                  : partition.file.num_batches();

    return num_full_batches + (partition.builder.num_rows() > 0);
  }

  inline size_t row_count(size_t partition_idx, size_t batch_idx) const {
    const Partition& partition = partitions_[partition_idx];
    if (batch_idx < partition.hash_queue.batch_count())
      return partition.hash_queue[batch_idx].length;
    else
      return partition.builder.num_rows();
  }

  static inline constexpr size_t partition_id(uint64_t hash) {
    // Hash Table uses the top bits of the hash, so it is important
    // to use the bottom bits of the hash for spilling to avoid
    // a huge number of hash collisions per partition.
    return static_cast<size_t>(hash & (kNumPartitions - 1));
  }

  // Returns the row count for the partition if it is still in-memory.
  // Returns 0 if the partition has already been spilled.
  size_t CalculatePartitionRowCount(size_t partition) const;

  // Spills the next partition of batches to disk and returns true,
  // or returns false if too many partitions have been spilled.
  // The QueryContext's bytes_in_flight will be increased by the
  // number of bytes spilled (unless the disk IO was very fast and
  // the bytes_in_flight got reduced again).
  //
  // We expect that we always advance the SpillCursor faster than the
  // HashCursor, and only advance the HashCursor when we've exhausted
  // partitions for the SpillCursor.
  Result<bool> AdvanceSpillCursor();
  // Same as AdvanceSpillCursor but spills the hashes for the partition.
  Result<bool> AdvanceHashCursor();
  inline size_t spill_cursor() const { return spilling_cursor_.load(); }
  inline size_t hash_cursor() const { return hash_cursor_.load(); }

 private:
  std::atomic<size_t> spilling_cursor_{0};  // denotes the first in-memory partition
  std::atomic<size_t> hash_cursor_{0};

  QueryContext* ctx_;
  PartitionLocks partition_locks_;

  struct Partition {
    AccumulationQueue queue;
    AccumulationQueue hash_queue;
    ExecBatchBuilder builder;
    SpillFile file;
    int task_group_read;
    std::function<Status(size_t, size_t, ExecBatch)> read_back_fn;
    std::function<Status(size_t)> on_finished;
  };

  Partition partitions_[kNumPartitions];
};

}  // namespace compute
}  // namespace arrow
