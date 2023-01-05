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
#include "arrow/compute/light_array.h"
#include "arrow/compute/exec/partition_util.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/exec/spilling_util.h"

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
  ExecBatch& operator[](size_t i) { return batches_[i]; };
  const ExecBatch &operator[] (size_t i) const { return batches_[i]; };

 private:
  std::vector<ExecBatch> batches_;
};

class SpillingAccumulationQueue
{
public:
    // Number of partitions must be a power of two, since we assign partitions by
    // looking at bottom few bits. 
    static constexpr int kLogNumPartitions = 6;
    static constexpr int kNumPartitions = 1 << kLogNumPartitions;
    Status Init(QueryContext *ctx);
    // Assumes that the final column in batch contains 64-bit hashes of the columns.
    Status InsertBatch(
        size_t thread_index,
        ExecBatch batch);
    Status GetPartition(
        size_t thread_index,
        size_t partition,
        std::function<Status(size_t, size_t, ExecBatch)> on_batch, // thread_index, batch_index, batch
        std::function<Status(size_t)> on_finished);

    // Returns hashes of the given partition and batch index. 
    // partition MUST be at least hash_cursor, as if partition < hash_cursor,
    // these hashes will have been deleted. 
    const uint64_t *GetHashes(size_t partition, size_t batch_idx);
    inline size_t batch_count(size_t partition) const
    {
        size_t num_full_batches = partition >= spilling_cursor_
            ? queues_[partition].batch_count()
            : files_[partition].num_batches();

        return num_full_batches + (builders_[partition].num_rows() > 0);
    }
    inline size_t row_count(size_t partition, size_t batch_idx) const
    {
        if(batch_idx < hash_queues_[partition].batch_count())
            return hash_queues_[partition][batch_idx].length;
        else
            return builders_[partition].num_rows();
    }

    static inline constexpr size_t partition_id(uint64_t hash)
    {
        // Hash Table uses the top bits of the hash, so we really really
        // need to use the bottom bits of the hash for spilling to avoid
        // a huge number of hash collisions per partition. 
        return static_cast<size_t>(hash & (kNumPartitions - 1));
    }

    size_t CalculatePartitionRowCount(size_t partition) const;

    Result<bool> AdvanceSpillCursor();
    Result<bool> AdvanceHashCursor();
    inline size_t spill_cursor() const { return spilling_cursor_.load(); };
    inline size_t hash_cursor() const { return hash_cursor_.load(); };

private:
    std::atomic<size_t> spilling_cursor_{0}; // denotes the first in-memory partition
    std::atomic<size_t> hash_cursor_{0};

    QueryContext* ctx_;
    PartitionLocks partition_locks_;

    AccumulationQueue queues_[kNumPartitions];
    AccumulationQueue hash_queues_[kNumPartitions];

    ExecBatchBuilder builders_[kNumPartitions];

    SpillFile files_[kNumPartitions];

    int task_group_read_[kNumPartitions];
    std::function<Status(size_t, size_t, ExecBatch)> read_back_fn_[kNumPartitions];
    std::function<Status(size_t)> on_finished_[kNumPartitions];
};

}  // namespace compute
}  // namespace arrow
