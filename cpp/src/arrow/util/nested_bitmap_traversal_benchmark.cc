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

#include "benchmark/benchmark.h"

#include <cstdint>
#include <cstdlib>
#include <memory>

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/buffer.h"
#include "arrow/memory_pool.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow::internal {

struct NestedBitmapTraversalBenchmark {
  benchmark::State& state;
  MemoryPool* memory_pool;
  int64_t parent_length;
  std::shared_ptr<ListArray> list_array;
  std::shared_ptr<Buffer> outer_bitmap;
  int64_t expected;

  explicit NestedBitmapTraversalBenchmark(benchmark::State& state)
      : state(state), memory_pool(default_memory_pool()), parent_length(1 << 20) {
    constexpr double kValuesNullProbability = 0.1;
    random::RandomArrayGenerator rng(/*seed=*/0);
    const auto parent_null_probability = 1. / static_cast<double>(state.range(0));
    const auto avg_list_length = state.range(1);

    auto child_values =
        rng.Int8(parent_length * avg_list_length, 0, 100, kValuesNullProbability);
    list_array = std::static_pointer_cast<ListArray>(
        rng.List(*child_values, parent_length, parent_null_probability,
                 /*force_empty_nulls=*/false));
    outer_bitmap = rng.NullBitmap(parent_length, parent_null_probability);
    const auto& values = static_cast<const Int8Array&>(*list_array->values());

    expected = 0;
    for (int64_t i = 0; i < parent_length; ++i) {
      if (bit_util::GetBit(outer_bitmap->data(), i) && list_array->IsValid(i)) {
        const int64_t child_start = list_array->value_offset(i);
        const int64_t child_end = list_array->value_offset(i + 1);
        expected += CountValues(values, child_start, child_end - child_start);
      }
    }
  }

  static int64_t CountValues(const Int8Array& values, int64_t offset, int64_t length) {
    const uint8_t* values_bitmap = values.null_bitmap_data();
    return CountSetBits(values_bitmap, values.offset() + offset, length);
  }

  void CheckResult(int64_t result) const {
    if (result != expected) {
      std::abort();
    }
  }

  void BenchBitmapAnd() {
    const uint8_t* outer_bitmap_data = outer_bitmap->data();
    const uint8_t* list_bitmap = list_array->null_bitmap_data();
    const auto& values = static_cast<const Int8Array&>(*list_array->values());
    for (auto _ : state) {
      std::shared_ptr<Buffer> visible_bitmap;
      ABORT_NOT_OK(BitmapAnd(memory_pool, outer_bitmap_data, /*left_offset=*/0,
                             list_bitmap, /*right_offset=*/0, parent_length,
                             /*out_offset=*/0)
                       .Value(&visible_bitmap));

      int64_t result = 0;
      VisitSetBitRunsVoid(
          visible_bitmap->data(), /*offset=*/0, parent_length,
          [&](int64_t position, int64_t length) {
            const int64_t child_start = list_array->value_offset(position);
            const int64_t child_end = list_array->value_offset(position + length);
            result += CountValues(values, child_start, child_end - child_start);
          });
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * parent_length);
  }

  void BenchVisitTwoBitBlocks() {
    const uint8_t* outer_bitmap_data = outer_bitmap->data();
    const uint8_t* list_bitmap = list_array->null_bitmap_data();
    const auto& values = static_cast<const Int8Array&>(*list_array->values());
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoBitBlocksVoid(
          outer_bitmap_data, /*left_offset=*/0, list_bitmap, /*right_offset=*/0,
          parent_length,
          [&](int64_t position) {
            const int64_t child_start = list_array->value_offset(position);
            const int64_t child_end = list_array->value_offset(position + 1);
            result += CountValues(values, child_start, child_end - child_start);
          },
          [] {});
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * parent_length);
  }

  void BenchVisitTwoBitRuns() {
    const uint8_t* outer_bitmap_data = outer_bitmap->data();
    const uint8_t* list_bitmap = list_array->null_bitmap_data();
    const auto& values = static_cast<const Int8Array&>(*list_array->values());
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoBitRunsVoid(
          outer_bitmap_data, /*left_offset=*/0, list_bitmap,
          /*right_offset=*/0, parent_length,
          [&](int64_t position, int64_t length, bool set) {
            if (set) {
              const int64_t child_start = list_array->value_offset(position);
              const int64_t child_end = list_array->value_offset(position + length);
              result += CountValues(values, child_start, child_end - child_start);
            }
          },
          memory_pool);
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * parent_length);
  }

  void BenchVisitTwoSetBitRuns() {
    const uint8_t* outer_bitmap_data = outer_bitmap->data();
    const uint8_t* list_bitmap = list_array->null_bitmap_data();
    const auto& values = static_cast<const Int8Array&>(*list_array->values());
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoSetBitRunsVoid(
          outer_bitmap_data, /*left_offset=*/0, list_bitmap,
          /*right_offset=*/0, parent_length,
          [&](int64_t position, int64_t length) {
            const int64_t child_start = list_array->value_offset(position);
            const int64_t child_end = list_array->value_offset(position + length);
            result += CountValues(values, child_start, child_end - child_start);
          },
          memory_pool);
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * parent_length);
  }
};

static void NestedBitmapAndCount(benchmark::State& state) {
  NestedBitmapTraversalBenchmark(state).BenchBitmapAnd();
}

static void NestedVisitTwoBitBlocksCount(benchmark::State& state) {
  NestedBitmapTraversalBenchmark(state).BenchVisitTwoBitBlocks();
}

static void NestedVisitTwoBitRunsCount(benchmark::State& state) {
  NestedBitmapTraversalBenchmark(state).BenchVisitTwoBitRuns();
}

static void NestedVisitTwoSetBitRunsCount(benchmark::State& state) {
  NestedBitmapTraversalBenchmark(state).BenchVisitTwoSetBitRuns();
}

static void SetArgs(benchmark::internal::Benchmark* benchmark) {
  benchmark->ArgsProduct({benchmark::CreateRange(2, 1 << 16, 8), {1, 4, 16}});
}

BENCHMARK(NestedBitmapAndCount)->Apply(SetArgs);
BENCHMARK(NestedVisitTwoBitBlocksCount)->Apply(SetArgs);
BENCHMARK(NestedVisitTwoBitRunsCount)->Apply(SetArgs);
BENCHMARK(NestedVisitTwoSetBitRunsCount)->Apply(SetArgs);

}  // namespace arrow::internal
