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

#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"

#include "arrow/compute/row/grouper.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;
constexpr int64_t kRound = 16;
constexpr double true_and_unique_probability = 0.2;

static ExecBatch MakeRandomExecBatch(const DataTypeVector& types, int64_t num_rows,
                                     double null_probability,
                                     int64_t alignment = kDefaultBufferAlignment,
                                     MemoryPool* memory_pool = nullptr) {
  random::RandomArrayGenerator rng(kSeed);
  auto num_types = static_cast<int>(types.size());

  // clang-format off
  // For unique probability:
  // The proportion of Unique determines the number of groups.
  // 1. In most scenarios, unique has a small proportion and exists
  // 2. In GroupBy/HashJoin are sometimes used for deduplication and
  // in that use case the key is mostly unique
  auto metadata = key_value_metadata(
      {
        "null_probability",
        "true_probability",  // for boolean type
        "unique"             // for string type
      },
      {
          internal::ToChars(null_probability),
          internal::ToChars(true_and_unique_probability),
          internal::ToChars(static_cast<int32_t>(num_rows *
                            true_and_unique_probability))
      });
  // clang-format on

  std::vector<Datum> values;
  values.resize(num_types);
  for (int i = 0; i < num_types; ++i) {
    auto field = ::arrow::field("", types[i], metadata);
    values[i] = rng.ArrayOf(*field, num_rows, alignment, memory_pool);
  }

  return ExecBatch(std::move(values), num_rows);
}

static void GrouperBenchmark(benchmark::State& state, const ExecSpan& span,
                             ExecContext* ctx = nullptr) {
  uint32_t num_groups = 0;
  for (auto _ : state) {
    ASSIGN_OR_ABORT(auto grouper, Grouper::Make(span.GetTypes(), ctx));
    for (int i = 0; i < kRound; ++i) {
      ASSIGN_OR_ABORT(auto group_ids, grouper->Consume(span));
    }
    num_groups = grouper->num_groups();
  }

  state.SetItemsProcessed(state.iterations() * kRound * span.length);
  state.counters["num_groups"] = num_groups;
  state.counters["uniqueness"] = static_cast<double>(num_groups) / (kRound * span.length);
}

static void GrouperWithMultiTypes(benchmark::State& state, const DataTypeVector& types) {
  auto ctx = default_exec_context();

  RegressionArgs args(state, false);
  const int64_t num_rows = args.size;
  const double null_proportion = args.null_proportion;

  auto exec_batch = MakeRandomExecBatch(types, num_rows, null_proportion,
                                        kDefaultBufferAlignment, ctx->memory_pool());
  ExecSpan exec_span(exec_batch);
  ASSIGN_OR_ABORT(auto grouper, Grouper::Make(exec_span.GetTypes(), ctx));
  GrouperBenchmark(state, exec_span, ctx);
}

void SetArgs(benchmark::internal::Benchmark* bench) {
  BenchmarkSetArgsWithSizes(bench, {1 << 10, 1 << 12});
}

// This benchmark is mainly to ensure that the construction of our underlying
// RowTable and the performance of the comparison operations in the lower-level
// compare_internal can be tracked (we have not systematically tested these
// underlying operations before).
//
// It mainly covers:
// 1. Basics types, including the impact of null ratio on performance (comparison
//    operations will compare null values separately.)
//
// 2. Combination types which will break the CPU-pipeline in column comparision.
//    Examples: https://github.com/apache/arrow/pull/41036#issuecomment-2048721547
//
// 3. Combination types requiring column resorted. These combinations are
//    essentially to test the impact of RowTableEncoder's sorting function on
//    input columns on the performance of CompareColumnsToRows
//    Examples: https://github.com/apache/arrow/pull/40998#issuecomment-2039204161

// basic types
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean}", {boolean()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32}", {int32()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int64}", {int64()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{utf8}", {utf8()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{fixed_size_binary(32)}",
                  {fixed_size_binary(32)})
    ->Apply(SetArgs);

// combination types
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean, utf8}", {boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, int32}", {int32(), int32()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int64, int32}", {int64(), int32()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean, int64, utf8}",
                  {boolean(), int64(), utf8()})
    ->Apply(SetArgs);

// combination types requiring column resorted
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, boolean, utf8}",
                  {int32(), boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, int64, boolean, utf8}",
                  {int32(), int64(), boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes,
                  "{utf8, int32, int64, fixed_size_binary(32), boolean}",
                  {utf8(), int32(), int64(), fixed_size_binary(32), boolean()})
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
