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

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow::internal {

struct NestedBitmapTraversalBenchmark {
  benchmark::State& state;
  int64_t bitmap_length;
  std::shared_ptr<StructArray> outer_struct;
  std::shared_ptr<StructArray> inner_struct;
  std::shared_ptr<Array> values;
  const Int8Array* values_int8;
  int64_t expected;

  explicit NestedBitmapTraversalBenchmark(benchmark::State& state)
      : state(state), bitmap_length(1 << 20) {
    random::RandomArrayGenerator rng(/*seed=*/0);
    const auto parent_null_probability = 1. / static_cast<double>(state.range(0));
    const auto values_null_probability = 1. / static_cast<double>(state.range(1));

    values = rng.Int8(bitmap_length, 0, 100, values_null_probability);
    values_int8 = static_cast<const Int8Array*>(values.get());
    auto inner_null_bitmap = rng.NullBitmap(bitmap_length, parent_null_probability);
    auto outer_null_bitmap = rng.NullBitmap(bitmap_length, parent_null_probability);
    ABORT_NOT_OK(
        StructArray::Make({values}, {field("b", int8())}, std::move(inner_null_bitmap))
            .Value(&inner_struct));
    ABORT_NOT_OK(StructArray::Make({inner_struct}, {field("a", inner_struct->type())},
                                   std::move(outer_null_bitmap))
                     .Value(&outer_struct));

    expected = 0;
    for (int64_t i = 0; i < bitmap_length; ++i) {
      if (outer_struct->IsValid(i) && inner_struct->IsValid(i) &&
          values_int8->IsValid(i)) {
        ++expected;
      }
    }
  }

  void CheckResult(int64_t result) const {
    if (result != expected) {
      std::abort();
    }
  }

  void BenchVisitTwoBitBlocks() {
    const auto* left_bitmap = outer_struct->null_bitmap_data();
    const auto* right_bitmap = inner_struct->null_bitmap_data();
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoBitBlocksVoid(
          left_bitmap, /*left_offset=*/0, right_bitmap, /*right_offset=*/0, bitmap_length,
          [&](int64_t position) {
            if (!values_int8->IsNull(position)) {
              ++result;
            }
          },
          [] {});
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * bitmap_length);
  }

  void BenchVisitTwoBitRuns() {
    const auto* left_bitmap = outer_struct->null_bitmap_data();
    const auto* right_bitmap = inner_struct->null_bitmap_data();
    const auto* values_bitmap = values_int8->null_bitmap_data();
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoBitRunsVoid(left_bitmap, /*left_offset=*/0, right_bitmap,
                          /*right_offset=*/0, bitmap_length,
                          [&](int64_t position, int64_t length, bool set) {
                            if (set) {
                              result +=
                                  CountSetBits(values_bitmap,
                                               values_int8->offset() + position, length);
                            }
                          });
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * bitmap_length);
  }

  void BenchVisitTwoSetBitRuns() {
    const auto* left_bitmap = outer_struct->null_bitmap_data();
    const auto* right_bitmap = inner_struct->null_bitmap_data();
    const auto* values_bitmap = values_int8->null_bitmap_data();
    for (auto _ : state) {
      int64_t result = 0;
      VisitTwoSetBitRunsVoid(
          left_bitmap, /*left_offset=*/0, right_bitmap,
          /*right_offset=*/0, bitmap_length, [&](int64_t position, int64_t length) {
            result +=
                CountSetBits(values_bitmap, values_int8->offset() + position, length);
          });
      CheckResult(result);
    }
    state.SetItemsProcessed(state.iterations() * bitmap_length);
  }
};

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
  benchmark->ArgsProduct(
      {benchmark::CreateRange(2, 1 << 16, 8), benchmark::CreateRange(2, 1 << 16, 8)});
}

BENCHMARK(NestedVisitTwoBitBlocksCount)->Apply(SetArgs);
BENCHMARK(NestedVisitTwoBitRunsCount)->Apply(SetArgs);
BENCHMARK(NestedVisitTwoSetBitRunsCount)->Apply(SetArgs);

}  // namespace arrow::internal
