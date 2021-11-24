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

#include <benchmark/benchmark.h>

#include "arrow/array.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

#include "arrow/compute/api_vector.h"

namespace arrow {
namespace compute {

using ::arrow::internal::checked_pointer_cast;

static constexpr random::SeedType kRandomSeed = 0xabcdef;
static constexpr random::SeedType kLongLength = 16384;

static std::shared_ptr<Array> MakeReplacements(random::RandomArrayGenerator* generator,
                                               const BooleanArray& mask) {
  int64_t count = 0;
  for (int64_t i = 0; i < mask.length(); i++) {
    count += mask.Value(i) && mask.IsValid(i);
  }
  return generator->Int64(count, /*min=*/-65536, /*max=*/65536, /*null_probability=*/0.1);
}

static void ReplaceWithMaskLowSelectivityBench(
    benchmark::State& state) {  // NOLINT non-const reference
  random::RandomArrayGenerator generator(kRandomSeed);
  const int64_t len = state.range(0);
  const int64_t offset = state.range(1);

  auto values =
      generator.Int64(len, /*min=*/-65536, /*max=*/65536, /*null_probability=*/0.1)
          ->Slice(offset);
  auto mask = checked_pointer_cast<BooleanArray>(
      generator.Boolean(len, /*true_probability=*/0.1, /*null_probability=*/0.1)
          ->Slice(offset));
  auto replacements = MakeReplacements(&generator, *mask);

  for (auto _ : state) {
    ABORT_NOT_OK(ReplaceWithMask(values, mask, replacements));
  }
  state.SetBytesProcessed(state.iterations() * (len - offset) * 8);
}

static void ReplaceWithMaskHighSelectivityBench(
    benchmark::State& state) {  // NOLINT non-const reference
  random::RandomArrayGenerator generator(kRandomSeed);
  const int64_t len = state.range(0);
  const int64_t offset = state.range(1);

  auto values =
      generator.Int64(len, /*min=*/-65536, /*max=*/65536, /*null_probability=*/0.1)
          ->Slice(offset);
  auto mask = checked_pointer_cast<BooleanArray>(
      generator.Boolean(len, /*true_probability=*/0.9, /*null_probability=*/0.1)
          ->Slice(offset));
  auto replacements = MakeReplacements(&generator, *mask);

  for (auto _ : state) {
    ABORT_NOT_OK(ReplaceWithMask(values, mask, replacements));
  }
  state.SetBytesProcessed(state.iterations() * (len - offset) * 8);
}

BENCHMARK(ReplaceWithMaskLowSelectivityBench)->Args({kLongLength, 0});
BENCHMARK(ReplaceWithMaskLowSelectivityBench)->Args({kLongLength, 99});
BENCHMARK(ReplaceWithMaskHighSelectivityBench)->Args({kLongLength, 0});
BENCHMARK(ReplaceWithMaskHighSelectivityBench)->Args({kLongLength, 99});

}  // namespace compute
}  // namespace arrow
