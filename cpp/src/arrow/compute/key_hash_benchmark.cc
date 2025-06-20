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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/hashing.h"

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/key_hash_internal.h"
#include "arrow/compute/util_internal.h"

namespace arrow {
namespace internal {

namespace {
// copied from scalar_string_benchmark
constexpr auto kSeed = 0x94378165;

static random::RandomArrayGenerator hashing_rng(kSeed);
}  // namespace

static void KeyHashIntegers32(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int32(10000, 0, std::numeric_limits<int32_t>::max());

  // initialize the stack allocator
  util::TempVectorStack stack_memallocator;
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // prepare the execution context for Hashing32
  compute::LightContext hash_ctx;
  hash_ctx.hardware_flags = compute::default_exec_context()->cpu_info()->hardware_flags();
  hash_ctx.stack = &stack_memallocator;

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(test_vals->length() * sizeof(int32_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(test_vals->data(), 0, test_vals->length()));

    compute::Hashing32::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint32_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int32_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

static void KeyHashIntegers64(benchmark::State& state) {  // NOLINT non-const reference
  auto test_vals = hashing_rng.Int64(10000, 0, std::numeric_limits<int64_t>::max());

  // initialize the stack allocator
  util::TempVectorStack stack_memallocator;
  ASSERT_OK(
      stack_memallocator.Init(compute::default_exec_context()->memory_pool(),
                              3 * sizeof(int32_t) * util::MiniBatch::kMiniBatchLength));

  // prepare the execution context for Hashing32
  compute::LightContext hash_ctx;
  hash_ctx.hardware_flags = compute::default_exec_context()->cpu_info()->hardware_flags();
  hash_ctx.stack = &stack_memallocator;

  // allocate memory for results
  ASSERT_OK_AND_ASSIGN(std::unique_ptr<Buffer> hash_buffer,
                       AllocateBuffer(test_vals->length() * sizeof(int64_t)));

  // run the benchmark
  while (state.KeepRunning()) {
    // Prepare input data structure for propagation to hash function
    ASSERT_OK_AND_ASSIGN(
        compute::KeyColumnArray input_keycol,
        compute::ColumnArrayFromArrayData(test_vals->data(), 0, test_vals->length()));

    compute::Hashing64::HashMultiColumn(
        {input_keycol}, &hash_ctx,
        reinterpret_cast<uint64_t*>(hash_buffer->mutable_data()));

    // benchmark::DoNotOptimize(hash_buffer);
  }

  state.SetBytesProcessed(state.iterations() * test_vals->length() * sizeof(int64_t));
  state.SetItemsProcessed(state.iterations() * test_vals->length());
}

// ----------------------------------------------------------------------
// Benchmark declarations

// Directly uses "KeyHash" hash functions from key_hash.h (xxHash-like)
BENCHMARK(KeyHashIntegers32);
BENCHMARK(KeyHashIntegers64);

}  // namespace internal
}  // namespace arrow
