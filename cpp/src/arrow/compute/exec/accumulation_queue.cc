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

#include "arrow/compute/exec/accumulation_queue.h"
#include "arrow/compute/exec/key_hash.h"
#include "arrow/util/atomic_util.h"

namespace arrow {
namespace compute {

AccumulationQueue::AccumulationQueue(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  that.Clear();
}

AccumulationQueue& AccumulationQueue::operator=(AccumulationQueue&& that) {
  this->batches_ = std::move(that.batches_);
  that.Clear();
  return *this;
}

void AccumulationQueue::Concatenate(AccumulationQueue&& that) {
  this->batches_.reserve(this->batches_.size() + that.batches_.size());
  std::move(that.batches_.begin(), that.batches_.end(),
            std::back_inserter(this->batches_));
  that.Clear();
}

void AccumulationQueue::InsertBatch(ExecBatch batch) {
  batches_.emplace_back(std::move(batch));
}

void AccumulationQueue::SetBatch(size_t idx, ExecBatch batch) {
  ARROW_DCHECK(idx < batches_.size());
  batches_[idx] = std::move(batch);
}

size_t AccumulationQueue::CalculateRowCount() const {
  size_t count = 0;
  for (const ExecBatch& b : batches_) count += static_cast<size_t>(b.length);
  return count;
}

void AccumulationQueue::Clear() { batches_.clear(); }

Status SpillingAccumulationQueue::Init(QueryContext* ctx) {
  ctx_ = ctx;
  partition_locks_.Init(ctx_->max_concurrency(), kNumPartitions);
  for (size_t ipart = 0; ipart < kNumPartitions; ipart++) {
    task_group_read_[ipart] = ctx_->RegisterTaskGroup(
        [this, ipart](size_t thread_index, int64_t batch_index) {
          return read_back_fn_[ipart](thread_index, static_cast<size_t>(batch_index),
                                      std::move(queues_[ipart][batch_index]));
        },
        [this, ipart](size_t thread_index) { return on_finished_[ipart](thread_index); });
  }
  return Status::OK();
}

Status SpillingAccumulationQueue::InsertBatch(size_t thread_index, ExecBatch batch) {
  Datum& hash_datum = batch.values.back();
  const uint64_t* hashes =
      reinterpret_cast<const uint64_t*>(hash_datum.array()->buffers[1]->data());
  // `permutation` stores the indices of rows in the input batch sorted by partition.
  std::vector<uint16_t> permutation(batch.length);
  uint16_t part_starts[kNumPartitions + 1];
  PartitionSort::Eval(
      batch.length, kNumPartitions, part_starts,
      /*partition_id=*/[&](int64_t i) { return partition_id(hashes[i]); },
      /*output_fn=*/
      [&permutation](int64_t input_pos, int64_t output_pos) {
        permutation[output_pos] = static_cast<uint16_t>(input_pos);
      });

  int unprocessed_partition_ids[kNumPartitions];
  RETURN_NOT_OK(partition_locks_.ForEachPartition(
      thread_index, unprocessed_partition_ids,
      /*is_prtn_empty=*/
      [&](int part_id) { return part_starts[part_id + 1] == part_starts[part_id]; },
      /*partition=*/
      [&](int locked_part_id_int) {
        size_t locked_part_id = static_cast<size_t>(locked_part_id_int);
        uint64_t num_total_rows_to_append =
            part_starts[locked_part_id + 1] - part_starts[locked_part_id];

        size_t offset = static_cast<size_t>(part_starts[locked_part_id]);
        while (num_total_rows_to_append > 0) {
          int num_rows_to_append =
              std::min(static_cast<int>(num_total_rows_to_append),
                       static_cast<int>(ExecBatchBuilder::num_rows_max() -
                                        builders_[locked_part_id].num_rows()));

          RETURN_NOT_OK(builders_[locked_part_id].AppendSelected(
              ctx_->memory_pool(), batch, num_rows_to_append, permutation.data() + offset,
              batch.num_values()));

          if (builders_[locked_part_id].is_full()) {
            ExecBatch batch = builders_[locked_part_id].Flush();
            Datum hash = std::move(batch.values.back());
            batch.values.pop_back();
            ExecBatch hash_batch({std::move(hash)}, batch.length);
            if (locked_part_id < spilling_cursor_)
              RETURN_NOT_OK(files_[locked_part_id].SpillBatch(ctx_, std::move(batch)));
            else
              queues_[locked_part_id].InsertBatch(std::move(batch));

            if (locked_part_id >= hash_cursor_)
              hash_queues_[locked_part_id].InsertBatch(std::move(hash_batch));
          }
          offset += num_rows_to_append;
          num_total_rows_to_append -= num_rows_to_append;
        }
        return Status::OK();
      }));
  return Status::OK();
}

const uint64_t* SpillingAccumulationQueue::GetHashes(size_t partition, size_t batch_idx) {
  ARROW_DCHECK(partition >= hash_cursor_.load());
  if (batch_idx > hash_queues_[partition].batch_count()) {
    const Datum& datum = hash_queues_[partition][batch_idx].values[0];
    return reinterpret_cast<const uint64_t*>(datum.array()->buffers[1]->data());
  } else {
    size_t hash_idx = builders_[partition].num_cols();
    KeyColumnArray kca = builders_[partition].column(hash_idx - 1);
    return reinterpret_cast<const uint64_t*>(kca.data(1));
  }
}

Status SpillingAccumulationQueue::GetPartition(
    size_t thread_index, size_t partition,
    std::function<Status(size_t, size_t, ExecBatch)> on_batch,
    std::function<Status(size_t)> on_finished) {
  bool is_in_memory = partition >= spilling_cursor_.load();
  if (builders_[partition].num_rows() > 0) {
    ExecBatch batch = builders_[partition].Flush();
    Datum hash = std::move(batch.values.back());
    batch.values.pop_back();
    if (is_in_memory) {
      ExecBatch hash_batch({std::move(hash)}, batch.length);
      hash_queues_[partition].InsertBatch(std::move(hash_batch));
      queues_[partition].InsertBatch(std::move(batch));
    } else {
      RETURN_NOT_OK(on_batch(thread_index,
                             /*batch_index=*/queues_[partition].batch_count(),
                             std::move(batch)));
    }
  }

  if (is_in_memory) {
    ARROW_DCHECK(partition >= hash_cursor_.load());
    read_back_fn_[partition] = std::move(on_batch);
    on_finished_[partition] = std::move(on_finished);
    return ctx_->StartTaskGroup(task_group_read_[partition],
                                queues_[partition].batch_count());
  }

  return files_[partition].ReadBackBatches(
      ctx_, on_batch,
      [this, partition, finished = std::move(on_finished)](size_t thread_index) {
        RETURN_NOT_OK(files_[partition].Cleanup());
        return finished(thread_index);
      });
}

size_t SpillingAccumulationQueue::CalculatePartitionRowCount(size_t partition) const {
  return builders_[partition].num_rows() + queues_[partition].CalculateRowCount();
}

Result<bool> SpillingAccumulationQueue::AdvanceSpillCursor() {
  size_t to_spill = spilling_cursor_.fetch_add(1);
  if (to_spill >= kNumPartitions) {
    ARROW_DCHECK(to_spill < 1000 * 1000 * 1000)
        << "You've tried to advance the spill cursor over a billion times, you might "
           "have a problem";
    return false;
  }

  auto lock = partition_locks_.AcquirePartitionLock(static_cast<int>(to_spill));
  size_t num_batches = queues_[to_spill].batch_count();
  for (size_t i = 0; i < num_batches; i++)
    RETURN_NOT_OK(files_[to_spill].SpillBatch(ctx_, std::move(queues_[to_spill][i])));
  return true;
}

Result<bool> SpillingAccumulationQueue::AdvanceHashCursor() {
  size_t to_spill = hash_cursor_.fetch_add(1);
  if (to_spill >= kNumPartitions) {
    ARROW_DCHECK(to_spill < 1000 * 1000 * 1000)
        << "You've tried to advance the spill cursor over a billion times, you might "
           "have a problem";
    return false;
  }

  auto lock = partition_locks_.AcquirePartitionLock(static_cast<int>(to_spill));
  hash_queues_[to_spill].Clear();
  return true;
}
}  // namespace compute
}  // namespace arrow
