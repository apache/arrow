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

#include "arrow/acero/bloom_filter.h"
#include <random>
#include "arrow/acero/util.h"       // PREFETCH
#include "arrow/util/bit_util.h"    // Log2
#include "arrow/util/bitmap_ops.h"  // CountSetBits

namespace arrow {
namespace acero {

BloomFilterMasks::BloomFilterMasks() {
  std::seed_seq seed{0, 0, 0, 0, 0, 0, 0, 0};
  std::mt19937 re(seed);
  std::uniform_int_distribution<uint64_t> rd;
  auto random = [&re, &rd](int min_value, int max_value) {
    return min_value + rd(re) % (max_value - min_value + 1);
  };

  memset(masks_, 0, kTotalBytes);

  // Prepare the first mask
  //
  int num_bits_set = static_cast<int>(random(kMinBitsSet, kMaxBitsSet));
  for (int i = 0; i < num_bits_set; ++i) {
    for (;;) {
      int bit_pos = static_cast<int>(random(0, kBitsPerMask - 1));
      if (!bit_util::GetBit(masks_, bit_pos)) {
        bit_util::SetBit(masks_, bit_pos);
        break;
      }
    }
  }

  int64_t num_bits_total = kNumMasks + kBitsPerMask - 1;

  // The value num_bits_set will be updated in each iteration of the loop to
  // represent the number of bits set in the entire mask directly preceding the
  // currently processed bit.
  //
  for (int64_t i = kBitsPerMask; i < num_bits_total; ++i) {
    // The value of the lowest bit of the previous mask that will be removed
    // from the current mask as we move to the next position in the bit vector
    // of masks.
    //
    int bit_leaving = bit_util::GetBit(masks_, i - kBitsPerMask) ? 1 : 0;

    // Next bit has to be 1 because of minimum bits in a mask requirement
    //
    if (bit_leaving == 1 && num_bits_set == kMinBitsSet) {
      bit_util::SetBit(masks_, i);
      continue;
    }

    // Next bit has to be 0 because of maximum bits in a mask requirement
    //
    if (bit_leaving == 0 && num_bits_set == kMaxBitsSet) {
      continue;
    }

    // Next bit can be random. Use the expected number of bits set in a mask
    // as a probability of 1.
    //
    if (random(0, kBitsPerMask * 2 - 1) < kMinBitsSet + kMaxBitsSet) {
      bit_util::SetBit(masks_, i);
      if (bit_leaving == 0) {
        ++num_bits_set;
      }
    } else {
      if (bit_leaving == 1) {
        --num_bits_set;
      }
    }
  }
}

BloomFilterMasks BlockedBloomFilter::masks_;

Status BlockedBloomFilter::CreateEmpty(int64_t num_rows_to_insert, MemoryPool* pool) {
  // Compute the size
  //
  constexpr int64_t min_num_bits_per_key = 8;
  constexpr int64_t min_num_bits = 512;
  int64_t desired_num_bits =
      std::max(min_num_bits, num_rows_to_insert * min_num_bits_per_key);
  int log_num_bits = bit_util::Log2(desired_num_bits);

  log_num_blocks_ = log_num_bits - 6;
  num_blocks_ = 1ULL << log_num_blocks_;

  // Allocate and zero out bit vector
  //
  int64_t buffer_size = num_blocks_ * sizeof(uint64_t);
  ARROW_ASSIGN_OR_RAISE(buf_, AllocateBuffer(buffer_size, pool));
  blocks_ = reinterpret_cast<uint64_t*>(buf_->mutable_data());
  memset(blocks_, 0, buffer_size);

  return Status::OK();
}

template <typename T>
void BlockedBloomFilter::InsertImp(int64_t num_rows, const T* hashes) {
  for (int64_t i = 0; i < num_rows; ++i) {
    Insert(hashes[i]);
  }
}

void BlockedBloomFilter::Insert(int64_t hardware_flags, int64_t num_rows,
                                const uint32_t* hashes) {
  int64_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    num_processed = Insert_avx2(num_rows, hashes);
  }
#endif
  InsertImp(num_rows - num_processed, hashes + num_processed);
}

void BlockedBloomFilter::Insert(int64_t hardware_flags, int64_t num_rows,
                                const uint64_t* hashes) {
  int64_t num_processed = 0;
#if defined(ARROW_HAVE_AVX2)
  if (hardware_flags & arrow::internal::CpuInfo::AVX2) {
    num_processed = Insert_avx2(num_rows, hashes);
  }
#endif
  InsertImp(num_rows - num_processed, hashes + num_processed);
}

template <typename T>
void BlockedBloomFilter::FindImp(int64_t num_rows, const T* hashes,
                                 uint8_t* result_bit_vector, bool enable_prefetch) const {
  int64_t num_processed = 0;
  uint64_t bits = 0ULL;

  if (enable_prefetch && UsePrefetch()) {
    constexpr int kPrefetchIterations = 16;
    for (int64_t i = 0; i < num_rows - kPrefetchIterations; ++i) {
      PREFETCH(blocks_ + block_id(hashes[i + kPrefetchIterations]));
      uint64_t result = Find(hashes[i]) ? 1ULL : 0ULL;
      bits |= result << (i & 63);
      if ((i & 63) == 63) {
        reinterpret_cast<uint64_t*>(result_bit_vector)[i / 64] = bits;
        bits = 0ULL;
      }
    }
    num_processed = num_rows - kPrefetchIterations;
  }

  for (int64_t i = num_processed; i < num_rows; ++i) {
    uint64_t result = Find(hashes[i]) ? 1ULL : 0ULL;
    bits |= result << (i & 63);
    if ((i & 63) == 63) {
      reinterpret_cast<uint64_t*>(result_bit_vector)[i / 64] = bits;
      bits = 0ULL;
    }
  }

  for (int i = 0; i < bit_util::CeilDiv(num_rows % 64, 8); ++i) {
    result_bit_vector[num_rows / 64 * 8 + i] = static_cast<uint8_t>(bits >> (i * 8));
  }
}

void BlockedBloomFilter::Find(int64_t hardware_flags, int64_t num_rows,
                              const uint32_t* hashes, uint8_t* result_bit_vector,
                              bool enable_prefetch) const {
  int64_t num_processed = 0;

#if defined(ARROW_HAVE_AVX2)
  if (!(enable_prefetch && UsePrefetch()) &&
      (hardware_flags & arrow::internal::CpuInfo::AVX2)) {
    num_processed = Find_avx2(num_rows, hashes, result_bit_vector);
    // Make sure that the results in bit vector for the remaining rows start at
    // a byte boundary.
    //
    num_processed -= (num_processed % 8);
  }
#endif

  ARROW_DCHECK(num_processed % 8 == 0);
  FindImp(num_rows - num_processed, hashes + num_processed,
          result_bit_vector + num_processed / 8, enable_prefetch);
}

void BlockedBloomFilter::Find(int64_t hardware_flags, int64_t num_rows,
                              const uint64_t* hashes, uint8_t* result_bit_vector,
                              bool enable_prefetch) const {
  int64_t num_processed = 0;

#if defined(ARROW_HAVE_AVX2)
  if (!(enable_prefetch && UsePrefetch()) &&
      (hardware_flags & arrow::internal::CpuInfo::AVX2)) {
    num_processed = Find_avx2(num_rows, hashes, result_bit_vector);
    num_processed -= (num_processed % 8);
  }
#endif

  ARROW_DCHECK(num_processed % 8 == 0);
  FindImp(num_rows - num_processed, hashes + num_processed,
          result_bit_vector + num_processed / 8, enable_prefetch);
}

void BlockedBloomFilter::Fold() {
  // Keep repeating until one of the stop conditions checked inside the loop
  // is met
  for (;;) {
    // If we reached the minimum size of blocked Bloom filter then stop
    constexpr int log_num_blocks_min = 4;
    if (log_num_blocks_ <= log_num_blocks_min) {
      break;
    }

    int64_t num_bits = num_blocks_ * 64;

    // Calculate the number of bits set in this blocked Bloom filter
    int64_t num_bits_set = 0;
    int batch_size_max = 65536;
    for (int64_t i = 0; i < num_bits; i += batch_size_max) {
      int batch_size =
          static_cast<int>(std::min(num_bits - i, static_cast<int64_t>(batch_size_max)));
      num_bits_set +=
          arrow::internal::CountSetBits(reinterpret_cast<const uint8_t*>(blocks_) + i / 8,
                                        /*offset=*/0, batch_size);
    }

    // If at least 1/4 of bits is set then stop
    if (4 * num_bits_set >= num_bits) {
      break;
    }

    // Decide how many times to fold at once.
    // The resulting size should not be less than log_num_bits_min.
    int num_folds = 1;

    while ((log_num_blocks_ - num_folds) > log_num_blocks_min &&
           (4 * num_bits_set) < (num_bits >> num_folds)) {
      ++num_folds;
    }

    // Actual update to block Bloom filter bits
    SingleFold(num_folds);
  }
}

void BlockedBloomFilter::SingleFold(int num_folds) {
  // Calculate number of slices and size of a slice
  //
  int64_t num_slices = 1LL << num_folds;
  int64_t num_slice_blocks = (num_blocks_ >> num_folds);
  uint64_t* target_slice = blocks_;

  // OR bits of all the slices and store result in the first slice
  //
  for (int64_t slice = 1; slice < num_slices; ++slice) {
    const uint64_t* source_slice = blocks_ + slice * num_slice_blocks;
    for (int i = 0; i < num_slice_blocks; ++i) {
      target_slice[i] |= source_slice[i];
    }
  }

  log_num_blocks_ -= num_folds;
  num_blocks_ = 1ULL << log_num_blocks_;
}

int BlockedBloomFilter::NumHashBitsUsed() const {
  constexpr int num_bits_for_mask = (BloomFilterMasks::kLogNumMasks + 6);
  int num_bits_for_block = log_num_blocks();
  return num_bits_for_mask + num_bits_for_block;
}

bool BlockedBloomFilter::IsSameAs(const BlockedBloomFilter* other) const {
  if (log_num_blocks_ != other->log_num_blocks_ || num_blocks_ != other->num_blocks_) {
    return false;
  }
  if (memcmp(blocks_, other->blocks_, num_blocks_ * sizeof(uint64_t)) != 0) {
    return false;
  }
  return true;
}

int64_t BlockedBloomFilter::NumBitsSet() const {
  return arrow::internal::CountSetBits(reinterpret_cast<const uint8_t*>(blocks_),
                                       /*offset=*/0, (1LL << log_num_blocks()) * 64);
}

Status BloomFilterBuilder_SingleThreaded::Begin(size_t /*num_threads*/,
                                                int64_t hardware_flags, MemoryPool* pool,
                                                int64_t num_rows, int64_t /*num_batches*/,
                                                BlockedBloomFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;

  RETURN_NOT_OK(build_target->CreateEmpty(num_rows, pool));

  return Status::OK();
}

Status BloomFilterBuilder_SingleThreaded::PushNextBatch(size_t /*thread_index*/,
                                                        int64_t num_rows,
                                                        const uint32_t* hashes) {
  PushNextBatchImp(num_rows, hashes);
  return Status::OK();
}

Status BloomFilterBuilder_SingleThreaded::PushNextBatch(size_t /*thread_index*/,
                                                        int64_t num_rows,
                                                        const uint64_t* hashes) {
  PushNextBatchImp(num_rows, hashes);
  return Status::OK();
}

template <typename T>
void BloomFilterBuilder_SingleThreaded::PushNextBatchImp(int64_t num_rows,
                                                         const T* hashes) {
  build_target_->Insert(hardware_flags_, num_rows, hashes);
}

Status BloomFilterBuilder_Parallel::Begin(size_t num_threads, int64_t hardware_flags,
                                          MemoryPool* pool, int64_t num_rows,
                                          int64_t /*num_batches*/,
                                          BlockedBloomFilter* build_target) {
  hardware_flags_ = hardware_flags;
  build_target_ = build_target;

  constexpr int kMaxLogNumPrtns = 8;
  log_num_prtns_ = std::min(kMaxLogNumPrtns, bit_util::Log2(num_threads));

  thread_local_states_.resize(num_threads);
  prtn_locks_.Init(num_threads, 1 << log_num_prtns_);

  RETURN_NOT_OK(build_target->CreateEmpty(num_rows, pool));

  return Status::OK();
}

Status BloomFilterBuilder_Parallel::PushNextBatch(size_t thread_id, int64_t num_rows,
                                                  const uint32_t* hashes) {
  PushNextBatchImp(thread_id, num_rows, hashes);
  return Status::OK();
}

Status BloomFilterBuilder_Parallel::PushNextBatch(size_t thread_id, int64_t num_rows,
                                                  const uint64_t* hashes) {
  PushNextBatchImp(thread_id, num_rows, hashes);
  return Status::OK();
}

template <typename T>
void BloomFilterBuilder_Parallel::PushNextBatchImp(size_t thread_id, int64_t num_rows,
                                                   const T* hashes) {
  // Partition IDs are calculated using the higher bits of the block ID.  This
  // ensures that each block is contained entirely within a partition and prevents
  // concurrent access to a block.
  constexpr int kLogBlocksKeptTogether = 7;
  constexpr int kPrtnIdBitOffset =
      BloomFilterMasks::kLogNumMasks + 6 + kLogBlocksKeptTogether;

  const int log_num_prtns_max =
      std::max(0, build_target_->log_num_blocks() - kLogBlocksKeptTogether);
  const int log_num_prtns_mod = std::min(log_num_prtns_, log_num_prtns_max);
  int num_prtns = 1 << log_num_prtns_mod;

  ThreadLocalState& local_state = thread_local_states_[thread_id];
  local_state.partition_ranges.resize(num_prtns + 1);
  local_state.partitioned_hashes_64.resize(num_rows);
  local_state.unprocessed_partition_ids.resize(num_prtns);
  uint16_t* partition_ranges = local_state.partition_ranges.data();
  uint64_t* partitioned_hashes = local_state.partitioned_hashes_64.data();
  int* unprocessed_partition_ids = local_state.unprocessed_partition_ids.data();

  PartitionSort::Eval(
      num_rows, num_prtns, partition_ranges,
      [=](int64_t row_id) {
        return (hashes[row_id] >> (kPrtnIdBitOffset)) & (num_prtns - 1);
      },
      [=](int64_t row_id, int output_pos) {
        partitioned_hashes[output_pos] = hashes[row_id];
      });

  int num_unprocessed_partitions = 0;
  for (int i = 0; i < num_prtns; ++i) {
    bool is_prtn_empty = (partition_ranges[i + 1] == partition_ranges[i]);
    if (!is_prtn_empty) {
      unprocessed_partition_ids[num_unprocessed_partitions++] = i;
    }
  }
  while (num_unprocessed_partitions > 0) {
    int locked_prtn_id;
    int locked_prtn_id_pos;
    prtn_locks_.AcquirePartitionLock(thread_id, num_unprocessed_partitions,
                                     unprocessed_partition_ids,
                                     /*limit_retries=*/false, /*max_retries=*/-1,
                                     &locked_prtn_id, &locked_prtn_id_pos);
    build_target_->Insert(
        hardware_flags_,
        partition_ranges[locked_prtn_id + 1] - partition_ranges[locked_prtn_id],
        partitioned_hashes + partition_ranges[locked_prtn_id]);
    prtn_locks_.ReleasePartitionLock(locked_prtn_id);
    if (locked_prtn_id_pos < num_unprocessed_partitions - 1) {
      unprocessed_partition_ids[locked_prtn_id_pos] =
          unprocessed_partition_ids[num_unprocessed_partitions - 1];
    }
    --num_unprocessed_partitions;
  }
}

void BloomFilterBuilder_Parallel::CleanUp() {
  thread_local_states_.clear();
  prtn_locks_.CleanUp();
}

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::Make(
    BloomFilterBuildStrategy strategy) {
  switch (strategy) {
    case BloomFilterBuildStrategy::SINGLE_THREADED: {
      std::unique_ptr<BloomFilterBuilder> impl{new BloomFilterBuilder_SingleThreaded()};
      return impl;
    }
    case BloomFilterBuildStrategy::PARALLEL: {
      std::unique_ptr<BloomFilterBuilder> impl{new BloomFilterBuilder_Parallel()};
      return impl;
    }
  }
  return nullptr;
}

}  // namespace acero
}  // namespace arrow
