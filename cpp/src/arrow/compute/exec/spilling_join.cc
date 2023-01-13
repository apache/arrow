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

#include <memory>

#include "arrow/compute/exec/spilling_join.h"
#include "arrow/util/atomic_util.h"

namespace arrow {
namespace compute {
void PartitionedBloomFilter::Find(int64_t hardware_flags, int64_t num_rows,
                                  const uint64_t* hashes, uint8_t* bv) {
  if (in_memory) return in_memory->Find(hardware_flags, num_rows, hashes, bv);

  for (int64_t i = 0; i < num_rows; i++) {
    uint64_t hash = hashes[i];
    size_t partition = SpillingAccumulationQueue::partition_id(hashes[i]);
    bool found = partitions[partition] ? partitions[partition]->Find(hash) : true;
    bit_util::SetBitTo(bv, i, found);
  }
}

Status SpillingHashJoin::Init(QueryContext* ctx, JoinType join_type, size_t num_threads,
                              SchemaProjectionMaps<HashJoinProjection>* proj_map_left,
                              SchemaProjectionMaps<HashJoinProjection>* proj_map_right,
                              std::vector<JoinKeyCmp>* key_cmp, Expression* filter,
                              PartitionedBloomFilter* bloom_filter,
                              CallbackRecord callback_record, bool is_swiss) {
  ctx_ = ctx;
  num_threads_ = num_threads;
  callbacks_ = std::move(callback_record);
  bloom_filter_ = bloom_filter;
  is_swiss_ = is_swiss;

  HashJoinImpl::CallbackRecord join_callbacks;
  join_callbacks.register_task_group = callbacks_.register_task_group;
  join_callbacks.start_task_group = callbacks_.start_task_group;
  join_callbacks.output_batch = callbacks_.output_batch;
  join_callbacks.finished = [this](int64_t num_total_batches) {
    return this->OnCollocatedJoinFinished(num_total_batches);
  };

  builder_ = BloomFilterBuilder::Make(num_threads_ == 1
                                          ? BloomFilterBuildStrategy::SINGLE_THREADED
                                          : BloomFilterBuildStrategy::PARALLEL);
  RETURN_NOT_OK(build_accumulator_.Init(ctx));
  RETURN_NOT_OK(probe_accumulator_.Init(ctx));

  for (size_t i = 0; i < SpillingAccumulationQueue::kNumPartitions; i++) {
    ARROW_ASSIGN_OR_RAISE(
        impls_[i], is_swiss_ ? HashJoinImpl::MakeSwiss() : HashJoinImpl::MakeBasic());
    RETURN_NOT_OK(impls_[i]->Init(ctx_, join_type, num_threads, proj_map_left,
                                  proj_map_right, key_cmp, filter, join_callbacks));

    task_group_bloom_[i] = callbacks_.register_task_group(
        [this](size_t thread_index, int64_t task_id) {
          return PushBloomFilterBatch(thread_index, task_id);
        },
        [this](size_t thread_index) { return OnBloomFilterFinished(thread_index); });
  }
  return Status::OK();
}

Status SpillingHashJoin::CheckSpilling(size_t thread_index, ExecBatch& batch) {
  size_t size_of_batch = static_cast<size_t>(batch.TotalBufferSize());
  size_t max_batch_size = arrow::util::AtomicMax(max_batch_size_, size_of_batch);

  // Spilling algorithm proven to not use more than
  // (SpillThreshold + NumThreads * BatchSize) memory.
  // Thus we want to spill when (SpillThreshold + NumThreads * BatchSize) = k * MaxMemory
  // with some fuzz factor k (which is 0.8 here because that's what I decided).
  // Thus SpillThreshold = k * MaxMemory - NumThreads * BatchSize.
  constexpr float kFuzzFactor = 0.8f;
  size_t max_memory = static_cast<size_t>(kFuzzFactor * ctx_->options().max_memory_bytes);
  size_t spill_threshold = static_cast<size_t>(std::max(
      static_cast<int64_t>(kFuzzFactor * max_memory - num_threads_ * max_batch_size),
      static_cast<int64_t>(0)));
  size_t bytes_allocated = static_cast<size_t>(ctx_->memory_pool()->bytes_allocated());
  size_t bytes_inflight = ctx_->GetCurrentTempFileIO();

  size_t backpressure_threshold = spill_threshold / 2;
  if (bytes_allocated > backpressure_threshold) {
    if (int32_t expected = 0; backpressure_counter_.compare_exchange_strong(expected, 1))
      callbacks_.pause_probe_side(1);
  }
  if ((bytes_allocated - bytes_inflight) > spill_threshold) {
    RETURN_NOT_OK(AdvanceSpillCursor(thread_index));
  }
  return Status::OK();
}

Status SpillingHashJoin::AdvanceSpillCursor(size_t thread_index) {
  if (bool expected = false;
      !spilling_.load() && spilling_.compare_exchange_strong(expected, true))
    return callbacks_.start_spilling(thread_index);

  ARROW_ASSIGN_OR_RAISE(bool probe_advanced, probe_accumulator_.AdvanceSpillCursor());
  if (probe_advanced) return Status::OK();

  ARROW_ASSIGN_OR_RAISE(bool build_advanced, build_accumulator_.AdvanceSpillCursor());
  if (build_advanced) return Status::OK();

  ARROW_ASSIGN_OR_RAISE(bool probe_hash_advanced, probe_accumulator_.AdvanceHashCursor());
  if (probe_hash_advanced) return Status::OK();

  ARROW_ASSIGN_OR_RAISE(bool build_hash_advanced, build_accumulator_.AdvanceHashCursor());
  if (build_hash_advanced) return Status::OK();

  // Pray we don't run out of memory
  ARROW_LOG(WARNING)
      << "Memory limits for query exceeded but all data from hash join spilled to disk";
  return Status::OK();
}

Status SpillingHashJoin::OnBuildSideBatch(size_t thread_index, ExecBatch batch) {
  return build_accumulator_.InsertBatch(thread_index, std::move(batch));
}

Status SpillingHashJoin::OnBuildSideFinished(size_t thread_index) {
  return BuildPartitionedBloomFilter(thread_index);
}

// Note about Bloom filter implementation:
// Currently, we disable a partition for a Bloom filter based on the size of
// the hashes for that partition. Instead, we should be disabling based on
// the size of the bloom filter itself, since a Bloom filter would use about
// 8-16 bits per value instead of 64 bits per value.
Status SpillingHashJoin::BuildPartitionedBloomFilter(size_t thread_index) {
  // Disable Bloom filter if bloom_filter_ = nullptr by advancing to past
  // the final Bloom filter
  partition_idx_ = (bloom_filter_ == nullptr) ? SpillingAccumulationQueue::kNumPartitions
                                              : build_accumulator_.hash_cursor();
  return BuildNextBloomFilter(thread_index);
}

Status SpillingHashJoin::PushBloomFilterBatch(size_t thread_index, int64_t batch_id) {
  const uint64_t* hashes =
      build_accumulator_.GetHashes(partition_idx_, static_cast<size_t>(batch_id));
  size_t num_rows =
      build_accumulator_.row_count(partition_idx_, static_cast<size_t>(batch_id));
  return builder_->PushNextBatch(thread_index, static_cast<int64_t>(num_rows), hashes);
}

Status SpillingHashJoin::BuildNextBloomFilter(size_t thread_index) {
  size_t num_rows = build_accumulator_.CalculatePartitionRowCount(partition_idx_);
  size_t num_batches = build_accumulator_.batch_count(partition_idx_);

  // partition_idx_ is incremented in the callback for the taskgroup
  bloom_filter_->partitions[partition_idx_] = std::make_unique<BlockedBloomFilter>();

  RETURN_NOT_OK(builder_->Begin(num_threads_, ctx_->cpu_info()->hardware_flags(),
                                ctx_->memory_pool(), num_rows, num_batches,
                                bloom_filter_->partitions[partition_idx_].get()));

  return callbacks_.start_task_group(task_group_bloom_[partition_idx_],
                                     build_accumulator_.batch_count(partition_idx_));
}

Status SpillingHashJoin::OnBloomFilterFinished(size_t thread_index) {
  if (++partition_idx_ >= SpillingAccumulationQueue::kNumPartitions)
    return OnPartitionedBloomFilterFinished(thread_index);
  return BuildNextBloomFilter(thread_index);
}

Status SpillingHashJoin::OnPartitionedBloomFilterFinished(size_t thread_index) {
  RETURN_NOT_OK(callbacks_.bloom_filter_finished(thread_index));
  backpressure_counter_.store(2);
  callbacks_.resume_probe_side(/*backpressure_counter=*/2);
  if (bloom_or_probe_finished_.exchange(true)) return StartCollocatedJoins(thread_index);
  return Status::OK();
}

Status SpillingHashJoin::OnBloomFiltersReceived(size_t thread_index) {
  bloom_ready_.store(true, std::memory_order_release);
  return Status::OK();
}

Status SpillingHashJoin::OnProbeSideBatch(size_t thread_index, ExecBatch batch) {
  if (bloom_ready_.load()) {
    RETURN_NOT_OK(callbacks_.apply_bloom_filter(thread_index, &batch));
  }
  return probe_accumulator_.InsertBatch(thread_index, std::move(batch));
}

Status SpillingHashJoin::OnProbeSideFinished(size_t thread_index) {
  if (bloom_or_probe_finished_.exchange(true)) return StartCollocatedJoins(thread_index);
  return Status::OK();
}

Status SpillingHashJoin::StartCollocatedJoins(size_t thread_index) {
  // We start reading from the back to take advantage of any caches with the SSD
  // that may be in place (i.e. read back the most-recently-written stuff).
  partition_idx_ = SpillingAccumulationQueue::kNumPartitions;
  return BeginNextCollocatedJoin(thread_index);
}

Status SpillingHashJoin::BeginNextCollocatedJoin(size_t thread_index) {
  partition_idx_ -= 1;
  build_queue_.Resize(build_accumulator_.batch_count(partition_idx_));
  return build_accumulator_.GetPartition(
      thread_index, partition_idx_,
      /*on_batch*/
      [this](size_t thread_index, size_t batch_idx, ExecBatch batch) {
        build_queue_.SetBatch(batch_idx, std::move(batch));
        return Status::OK();
      },
      /*on_finished=*/
      [this](size_t thread_index) { return BuildHashTable(thread_index); });
}

// A possible optimization here is to swap the build and probe side if the probe side is
// smaller (we want the smaller side to be the hash table side). We know how much we wrote
// to disk for each side, so it could be a big win.
Status SpillingHashJoin::BuildHashTable(size_t thread_index) {
  RETURN_NOT_OK(impls_[partition_idx_]->BuildHashTable(
      thread_index, std::move(build_queue_),
      [this](size_t thread_index) { return OnHashTableFinished(thread_index); }));
  return Status::OK();
}

Status SpillingHashJoin::OnHashTableFinished(size_t thread_index) {
  return probe_accumulator_.GetPartition(
      thread_index, partition_idx_,
      [this](size_t thread_index, size_t batch_idx, ExecBatch batch) {
        return OnProbeSideBatchReadBack(thread_index, batch_idx, std::move(batch));
      },
      [this](size_t thread_index) { return OnProbingFinished(thread_index); });
}

Status SpillingHashJoin::OnProbeSideBatchReadBack(size_t thread_index, size_t batch_idx,
                                                  ExecBatch batch) {
  ARROW_DCHECK(bloom_ready_.load());
  RETURN_NOT_OK(callbacks_.add_probe_side_hashes(thread_index, &batch));
  RETURN_NOT_OK(callbacks_.apply_bloom_filter(thread_index, &batch));
  return impls_[partition_idx_]->ProbeSingleBatch(thread_index, std::move(batch));
}

Status SpillingHashJoin::OnProbingFinished(size_t thread_index) {
  return impls_[partition_idx_]->ProbingFinished(thread_index);
}

Status SpillingHashJoin::OnCollocatedJoinFinished(int64_t num_batches) {
  total_batches_outputted_ += num_batches;
  if (partition_idx_ > 0) return BeginNextCollocatedJoin(ctx_->GetThreadIndex());
  return callbacks_.finished(total_batches_outputted_);
}
}  // namespace compute
}  // namespace arrow
