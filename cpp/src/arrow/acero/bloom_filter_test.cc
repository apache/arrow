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

#include <gmock/gmock-matchers.h>

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <thread>
#include <unordered_set>
#include "arrow/acero/bloom_filter.h"
#include "arrow/acero/task_util.h"
#include "arrow/acero/test_util_internal.h"
#include "arrow/acero/util.h"
#include "arrow/compute/key_hash.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/cpu_info.h"

namespace arrow {

using compute::Hashing32;
using compute::Hashing64;

namespace acero {

constexpr int kBatchSizeMax = 32 * 1024;
Status BuildBloomFilter_Serial(
    std::unique_ptr<BloomFilterBuilder>& builder, int64_t num_rows, int64_t num_batches,
    std::function<void(int64_t, int, uint32_t*)> get_hash32_impl,
    std::function<void(int64_t, int, uint64_t*)> get_hash64_impl,
    BlockedBloomFilter* target) {
  std::vector<uint32_t> hashes32(kBatchSizeMax);
  std::vector<uint64_t> hashes64(kBatchSizeMax);
  for (int64_t i = 0; i < num_batches; i++) {
    size_t thread_index = 0;
    int batch_size = static_cast<int>(
        std::min(num_rows - i * kBatchSizeMax, static_cast<int64_t>(kBatchSizeMax)));
    if (target->NumHashBitsUsed() > 32) {
      uint64_t* hashes = hashes64.data();
      get_hash64_impl(i * kBatchSizeMax, batch_size, hashes);
      RETURN_NOT_OK(builder->PushNextBatch(thread_index, batch_size, hashes));
    } else {
      uint32_t* hashes = hashes32.data();
      get_hash32_impl(i * kBatchSizeMax, batch_size, hashes);
      RETURN_NOT_OK(builder->PushNextBatch(thread_index, batch_size, hashes));
    }
  }
  return Status::OK();
}

Status BuildBloomFilter_Parallel(
    std::unique_ptr<BloomFilterBuilder>& builder, size_t num_threads, int64_t num_rows,
    int64_t num_batches, std::function<void(int64_t, int, uint32_t*)> get_hash32_impl,
    std::function<void(int64_t, int, uint64_t*)> get_hash64_impl,
    BlockedBloomFilter* target) {
  ThreadIndexer thread_indexer;
  std::unique_ptr<TaskScheduler> scheduler = TaskScheduler::Make();
  std::vector<std::vector<uint32_t>> thread_local_hashes32(num_threads);
  std::vector<std::vector<uint64_t>> thread_local_hashes64(num_threads);
  for (std::vector<uint32_t>& h : thread_local_hashes32) h.resize(kBatchSizeMax);
  for (std::vector<uint64_t>& h : thread_local_hashes64) h.resize(kBatchSizeMax);

  std::condition_variable cv;
  std::mutex mutex;
  auto group = scheduler->RegisterTaskGroup(
      [&](size_t thread_index, int64_t task_id) -> Status {
        int batch_size = static_cast<int>(std::min(num_rows - task_id * kBatchSizeMax,
                                                   static_cast<int64_t>(kBatchSizeMax)));
        if (target->NumHashBitsUsed() > 32) {
          uint64_t* hashes = thread_local_hashes64[thread_index].data();
          get_hash64_impl(task_id * kBatchSizeMax, batch_size, hashes);
          RETURN_NOT_OK(builder->PushNextBatch(thread_index, batch_size, hashes));
        } else {
          uint32_t* hashes = thread_local_hashes32[thread_index].data();
          get_hash32_impl(task_id * kBatchSizeMax, batch_size, hashes);
          RETURN_NOT_OK(builder->PushNextBatch(thread_index, batch_size, hashes));
        }
        return Status::OK();
      },
      [&](size_t thread_index) -> Status {
        std::unique_lock<std::mutex> lk(mutex);
        cv.notify_all();
        return Status::OK();
      });
  scheduler->RegisterEnd();
  auto tp = arrow::internal::GetCpuThreadPool();
  RETURN_NOT_OK(scheduler->StartScheduling(
      0,
      [&](std::function<Status(size_t)> func) -> Status {
        return tp->Spawn([&, func]() {
          size_t tid = thread_indexer();
          ARROW_DCHECK_OK(func(tid));
        });
      },
      static_cast<int>(num_threads), false));
  {
    std::unique_lock<std::mutex> lk(mutex);
    RETURN_NOT_OK(scheduler->StartTaskGroup(0, group, num_batches));
    cv.wait(lk);
  }
  return Status::OK();
}

Status BuildBloomFilter(BloomFilterBuildStrategy strategy, int64_t hardware_flags,
                        MemoryPool* pool, int64_t num_rows,
                        std::function<void(int64_t, int, uint32_t*)> get_hash32_impl,
                        std::function<void(int64_t, int, uint64_t*)> get_hash64_impl,
                        BlockedBloomFilter* target) {
  int64_t num_batches = bit_util::CeilDiv(num_rows, kBatchSizeMax);

  bool is_serial = strategy == BloomFilterBuildStrategy::SINGLE_THREADED;
  auto builder = BloomFilterBuilder::Make(strategy);

  size_t num_threads = is_serial ? 1 : arrow::GetCpuThreadPoolCapacity();
  RETURN_NOT_OK(builder->Begin(num_threads, hardware_flags, pool, num_rows,
                               bit_util::CeilDiv(num_rows, kBatchSizeMax), target));

  if (is_serial)
    RETURN_NOT_OK(BuildBloomFilter_Serial(builder, num_rows, num_batches,
                                          std::move(get_hash32_impl),
                                          std::move(get_hash64_impl), target));
  else
    RETURN_NOT_OK(BuildBloomFilter_Parallel(builder, num_threads, num_rows, num_batches,
                                            std::move(get_hash32_impl),
                                            std::move(get_hash64_impl), target));
  builder->CleanUp();

  return Status::OK();
}

// In order to simulate what would happen if there were many duplicate keys as the build
// input of Bloom filter, we use the same sequence of generated hashes multiple times.
//
// This helper function treats input hashes as a ring buffer, that can be viewed as an
// infinite sequence of hashes (for every integer we can pick a hash from the buffer
// taking integer index module buffer size).
// Then it outputs a requested window of hashes from that infinite sequence.
//
template <typename T>
void TestBloomSmallHashHelper(int64_t num_input_hashes, const T* input_hashes,
                              int64_t first_row, int64_t num_rows, T* output_hashes) {
  int64_t first_row_clamped = first_row % num_input_hashes;
  int64_t num_rows_processed = 0;
  while (num_rows_processed < num_rows) {
    int64_t num_rows_next =
        std::min(num_rows - num_rows_processed, num_input_hashes - first_row_clamped);
    memcpy(output_hashes + num_rows_processed, input_hashes + first_row_clamped,
           num_rows_next * sizeof(T));
    first_row_clamped = 0;
    num_rows_processed += num_rows_next;
  }
}

// FPR (false positives rate) - fraction of false positives relative to the sum
// of false positives and true negatives.
//
// Output FPR and build and probe cost.
//
void TestBloomSmall(BloomFilterBuildStrategy strategy, int64_t num_build,
                    int num_build_copies, bool use_simd, bool enable_prefetch) {
  int64_t hardware_flags = use_simd ? ::arrow::internal::CpuInfo::AVX2 : 0;

  // Generate input keys
  //
  int64_t num_probe = 4 * num_build;
  Random64Bit rnd(/*seed=*/0);
  std::vector<uint64_t> unique_keys;
  std::unordered_set<uint64_t> unique_keys_set;
  for (int64_t i = 0; i < num_build + num_probe; ++i) {
    uint64_t value;
    for (;;) {
      value = rnd.next();
      if (unique_keys_set.find(value) == unique_keys_set.end()) {
        break;
      }
    }
    unique_keys.push_back(value);
    unique_keys_set.insert(value);
  }
  unique_keys_set.clear();

  // Generate input hashes
  //
  std::vector<uint32_t> hashes32;
  std::vector<uint64_t> hashes64;
  hashes32.resize(unique_keys.size());
  hashes64.resize(unique_keys.size());
  int batch_size_max = 1024;
  for (size_t i = 0; i < unique_keys.size(); i += batch_size_max) {
    int batch_size = static_cast<int>(
        std::min(unique_keys.size() - i, static_cast<size_t>(batch_size_max)));
    constexpr int key_length = sizeof(uint64_t);
    Hashing32::HashFixed(hardware_flags, /*combine_hashes=*/false, batch_size, key_length,
                         reinterpret_cast<const uint8_t*>(unique_keys.data() + i),
                         hashes32.data() + i, nullptr);
    Hashing64::HashFixed(
        /*combine_hashes=*/false, batch_size, key_length,
        reinterpret_cast<const uint8_t*>(unique_keys.data() + i), hashes64.data() + i);
  }

  MemoryPool* pool = default_memory_pool();

  // Build the filter
  //
  BlockedBloomFilter reference;
  BlockedBloomFilter bloom;

  ASSERT_OK(BuildBloomFilter(
      BloomFilterBuildStrategy::SINGLE_THREADED, hardware_flags, pool, num_build,
      [hashes32](int64_t first_row, int num_rows, uint32_t* output_hashes) {
        memcpy(output_hashes, hashes32.data() + first_row, num_rows * sizeof(uint32_t));
      },
      [hashes64](int64_t first_row, int num_rows, uint64_t* output_hashes) {
        memcpy(output_hashes, hashes64.data() + first_row, num_rows * sizeof(uint64_t));
      },
      &reference));

  ASSERT_OK(BuildBloomFilter(
      strategy, hardware_flags, pool, num_build * num_build_copies,
      [hashes32, num_build](int64_t first_row, int num_rows, uint32_t* output_hashes) {
        TestBloomSmallHashHelper<uint32_t>(num_build, hashes32.data(), first_row,
                                           num_rows, output_hashes);
      },
      [hashes64, num_build](int64_t first_row, int num_rows, uint64_t* output_hashes) {
        TestBloomSmallHashHelper<uint64_t>(num_build, hashes64.data(), first_row,
                                           num_rows, output_hashes);
      },
      &bloom));

  int log_before = bloom.log_num_blocks();

  if (num_build_copies > 1) {
    reference.Fold();
    bloom.Fold();
  } else {
    if (strategy != BloomFilterBuildStrategy::SINGLE_THREADED) {
      ASSERT_TRUE(reference.IsSameAs(&bloom));
    }
  }

  int log_after = bloom.log_num_blocks();

  float fraction_of_bits_set = static_cast<float>(bloom.NumBitsSet()) /
                               static_cast<float>(64LL << bloom.log_num_blocks());

  ARROW_SCOPED_TRACE("log_before = ", log_before, " log_after = ", log_after,
                     " percent_bits_set = ", 100.0f * fraction_of_bits_set);

  // Verify no false negatives and false positive rate is
  // within reason
  int64_t false_positives = 0;
  for (int64_t i = 0; i < num_build + num_probe; ++i) {
    bool found;
    if (bloom.NumHashBitsUsed() > 32) {
      found = bloom.Find(hashes64[i]);
    } else {
      found = bloom.Find(hashes32[i]);
    }
    // Every build key should be found
    if (i < num_build) {
      ASSERT_TRUE(found);
    } else if (found) {
      false_positives++;
    }
  }

  double fpr = static_cast<double>(false_positives) / num_probe;
  // Ideally this should be less than 0.05 but we check 0.1 here to avoid false failures
  // due to rounding issues or minor inconsistencies in the theory
  ASSERT_LT(fpr, 0.1) << "False positive rate for bloom filter was higher than expected";
}

template <typename T>
void TestBloomLargeHashHelper(int64_t hardware_flags, int64_t block,
                              const std::vector<uint64_t>& first_in_block,
                              int64_t first_row, int num_rows, T* output_hashes) {
  // Largest 63-bit prime
  constexpr uint64_t prime = 0x7FFFFFFFFFFFFFE7ULL;

  constexpr int mini_batch_size = 1024;
  uint64_t keys[mini_batch_size];
  int64_t ikey = first_row / block * block;
  uint64_t key = first_in_block[first_row / block];
  while (ikey < first_row) {
    key += prime;
    ++ikey;
  }
  for (int ibase = 0; ibase < num_rows;) {
    int next_batch_size = std::min(num_rows - ibase, mini_batch_size);
    for (int i = 0; i < next_batch_size; ++i) {
      keys[i] = key;
      key += prime;
    }

    constexpr int key_length = sizeof(uint64_t);
    if (sizeof(T) == sizeof(uint32_t)) {
      Hashing32::HashFixed(hardware_flags, false, next_batch_size, key_length,
                           reinterpret_cast<const uint8_t*>(keys),
                           reinterpret_cast<uint32_t*>(output_hashes) + ibase, nullptr);
    } else {
      Hashing64::HashFixed(false, next_batch_size, key_length,
                           reinterpret_cast<const uint8_t*>(keys),
                           reinterpret_cast<uint64_t*>(output_hashes) + ibase);
    }

    ibase += next_batch_size;
  }
}

// Test with larger size Bloom filters (use large prime with arithmetic
// sequence modulo 2^64).
//
void TestBloomLarge(BloomFilterBuildStrategy strategy, int64_t num_build, bool use_simd,
                    bool enable_prefetch) {
  int64_t hardware_flags = use_simd ? ::arrow::internal::CpuInfo::AVX2 : 0;

  // Largest 63-bit prime
  constexpr uint64_t prime = 0x7FFFFFFFFFFFFFE7ULL;

  // Generate input keys
  //
  int64_t num_probe = 4 * num_build;
  const int64_t block = 1024;
  std::vector<uint64_t> first_in_block;
  first_in_block.resize(bit_util::CeilDiv(num_build + num_probe, block));
  uint64_t current = prime;
  for (int64_t i = 0; i < num_build + num_probe; ++i) {
    if (i % block == 0) {
      first_in_block[i / block] = current;
    }
    current += prime;
  }

  MemoryPool* pool = default_memory_pool();

  // Build the filter
  //
  BlockedBloomFilter reference;
  BlockedBloomFilter bloom;

  for (int ibuild = 0; ibuild < 2; ++ibuild) {
    if (ibuild == 0 && strategy == BloomFilterBuildStrategy::SINGLE_THREADED) {
      continue;
    }
    ASSERT_OK(BuildBloomFilter(
        ibuild == 0 ? BloomFilterBuildStrategy::SINGLE_THREADED : strategy,
        hardware_flags, pool, num_build,
        [hardware_flags, &first_in_block](int64_t first_row, int num_rows,
                                          uint32_t* output_hashes) {
          const int64_t block = 1024;
          TestBloomLargeHashHelper(hardware_flags, block, first_in_block, first_row,
                                   num_rows, output_hashes);
        },
        [hardware_flags, &first_in_block](int64_t first_row, int num_rows,
                                          uint64_t* output_hashes) {
          const int64_t block = 1024;
          TestBloomLargeHashHelper(hardware_flags, block, first_in_block, first_row,
                                   num_rows, output_hashes);
        },
        ibuild == 0 ? &reference : &bloom));
  }

  if (strategy != BloomFilterBuildStrategy::SINGLE_THREADED) {
    ASSERT_TRUE(reference.IsSameAs(&bloom));
  }

  std::vector<uint32_t> hashes32;
  std::vector<uint64_t> hashes64;
  std::vector<uint8_t> result_bit_vector;
  hashes32.resize(block);
  hashes64.resize(block);
  result_bit_vector.resize(bit_util::BytesForBits(block));

  // Verify no false negatives and measure false positives.
  // Measure FPR and performance.
  //
  int64_t num_negatives_build = 0LL;
  int64_t num_negatives_probe = 0LL;

  for (int64_t i = 0; i < num_build + num_probe;) {
    int64_t first_row = i < num_build ? i : num_build + ((i - num_build) % num_probe);
    int64_t last_row = i < num_build ? num_build : num_build + num_probe;
    int64_t next_batch_size = std::min(last_row - first_row, block);
    if (bloom.NumHashBitsUsed() > 32) {
      TestBloomLargeHashHelper(hardware_flags, block, first_in_block, first_row,
                               static_cast<int>(next_batch_size), hashes64.data());
      bloom.Find(hardware_flags, next_batch_size, hashes64.data(),
                 result_bit_vector.data(), enable_prefetch);
    } else {
      TestBloomLargeHashHelper(hardware_flags, block, first_in_block, first_row,
                               static_cast<int>(next_batch_size), hashes32.data());
      bloom.Find(hardware_flags, next_batch_size, hashes32.data(),
                 result_bit_vector.data(), enable_prefetch);
    }
    uint64_t num_negatives = 0ULL;
    for (int iword = 0; iword < next_batch_size / 64; ++iword) {
      uint64_t word = reinterpret_cast<const uint64_t*>(result_bit_vector.data())[iword];
      num_negatives += ARROW_POPCOUNT64(~word);
    }
    if (next_batch_size % 64 > 0) {
      uint64_t word = reinterpret_cast<const uint64_t*>(
          result_bit_vector.data())[next_batch_size / 64];
      uint64_t mask = (1ULL << (next_batch_size % 64)) - 1;
      word |= ~mask;
      num_negatives += ARROW_POPCOUNT64(~word);
    }
    if (i < num_build) {
      num_negatives_build += num_negatives;
    } else {
      num_negatives_probe += num_negatives;
    }
    i += next_batch_size;
  }

  ASSERT_EQ(num_negatives_build, 0);
  int64_t probe_positives = num_probe - num_negatives_probe;
  double fpr = probe_positives / static_cast<double>(num_probe);
  // Ideally this should be less than 0.05 but we check 0.1 here to avoid false failures
  // due to rounding issues or minor inconsistencies in the theory
  ASSERT_LT(fpr, 0.1) << "False positive rate for bloom filter was higher than expected";
}

TEST(BloomFilter, Basic) {
  std::vector<int64_t> num_build;
#if defined(THREAD_SANITIZER) || defined(ARROW_VALGRIND)
  constexpr int log_min = 8;
  constexpr int log_max = 9;
#else
  constexpr int log_min = 8;
  constexpr int log_max = 16;
#endif
  for (int log_num_build = log_min; log_num_build < log_max; ++log_num_build) {
    constexpr int num_intermediate_points = 2;
    for (int i = 0; i < num_intermediate_points; ++i) {
      int64_t num_left = 1LL << log_num_build;
      int64_t num_right = 1LL << (log_num_build + 1);
      num_build.push_back((num_left * (num_intermediate_points - i) + num_right * i) /
                          num_intermediate_points);
    }
  }
#ifndef ARROW_VALGRIND
  num_build.push_back(1LL << log_max);
  constexpr int log_large = 22;
  num_build.push_back(1LL << log_large);
#endif

  constexpr int num_param_sets = 3;
  struct {
    bool use_avx2;
    bool enable_prefetch;
    bool insert_multiple_copies;
  } params[num_param_sets];
  for (int i = 0; i < num_param_sets; ++i) {
    params[i].use_avx2 = (i == 1);
    params[i].enable_prefetch = (i == 2);
    params[i].insert_multiple_copies = (i == 3);
  }

  std::vector<BloomFilterBuildStrategy> strategy;
  strategy.push_back(BloomFilterBuildStrategy::SINGLE_THREADED);
#ifndef ARROW_VALGRIND
  strategy.push_back(BloomFilterBuildStrategy::PARALLEL);
#endif

  static constexpr int64_t min_rows_for_large = 2 * 1024 * 1024;

  for (size_t istrategy = 0; istrategy < strategy.size(); ++istrategy) {
    for (int iparam_set = 0; iparam_set < num_param_sets; ++iparam_set) {
      ARROW_SCOPED_TRACE("%s ", params[iparam_set].use_avx2                 ? "AVX2"
                                : params[iparam_set].enable_prefetch        ? "PREFETCH"
                                : params[iparam_set].insert_multiple_copies ? "FOLDING"
                                                                            : "REGULAR");
      for (size_t inum_build = 0; inum_build < num_build.size(); ++inum_build) {
        ARROW_SCOPED_TRACE("num_build ", static_cast<int>(num_build[inum_build]));
        if (num_build[inum_build] >= min_rows_for_large) {
          TestBloomLarge(strategy[istrategy], num_build[inum_build],
                         params[iparam_set].use_avx2, params[iparam_set].enable_prefetch);

        } else {
          TestBloomSmall(strategy[istrategy], num_build[inum_build],
                         params[iparam_set].insert_multiple_copies ? 8 : 1,
                         params[iparam_set].use_avx2, params[iparam_set].enable_prefetch);
        }
      }
    }
  }
}

#ifndef ARROW_VALGRIND
TEST(BloomFilter, Scaling) {
  std::vector<int64_t> num_build;
  num_build.push_back(1000000);
  num_build.push_back(4000000);

  std::vector<BloomFilterBuildStrategy> strategy;
  strategy.push_back(BloomFilterBuildStrategy::PARALLEL);

  for (bool use_avx2 : {false, true}) {
    for (size_t istrategy = 0; istrategy < strategy.size(); ++istrategy) {
      for (size_t inum_build = 0; inum_build < num_build.size(); ++inum_build) {
        ARROW_SCOPED_TRACE("num_build = ", static_cast<int>(num_build[inum_build]));
        ARROW_SCOPED_TRACE("strategy = ",
                           strategy[istrategy] == BloomFilterBuildStrategy::PARALLEL
                               ? "PARALLEL"
                               : "SINGLE_THREADED");
        ARROW_SCOPED_TRACE("avx2 = ", use_avx2 ? "AVX2" : "SCALAR");
        TestBloomLarge(strategy[istrategy], num_build[inum_build], use_avx2,
                       /*enable_prefetch=*/false);
      }
    }
  }
}
#endif

}  // namespace acero
}  // namespace arrow
