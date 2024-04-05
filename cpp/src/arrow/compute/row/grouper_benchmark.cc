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

#include "arrow/util/key_value_metadata.h"
#include "arrow/util/string.h"
#include "benchmark/benchmark.h"

#include "arrow/compute/row/grouper.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/benchmark_util.h"

namespace arrow {
namespace compute {

constexpr auto kSeed = 0x0ff1ce;
constexpr int64_t kRound = 256;

static ExecBatch MakeRandomExecBatch(const DataTypeVector& types, int64_t num_rows,
                                     double null_probability,
                                     int64_t alignment = kDefaultBufferAlignment,
                                     MemoryPool* memory_pool = nullptr) {
  random::RandomArrayGenerator rng(kSeed);
  auto num_types = static_cast<int>(types.size());

  // clang-format off
  auto metadata = key_value_metadata(
      {
        "null_probability",
        "true_probability",
        "unique"
      },
      {
          internal::ToChars(null_probability),
          internal::ToChars(null_probability),                     // for boolean type
          internal::ToChars(static_cast<int32_t>(num_rows * 0.5))  // for string type
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
  for (auto _ : state) {
    ASSIGN_OR_ABORT(auto grouper, Grouper::Make(span.GetTypes(), ctx));
    for (int i = 0; i < kRound; ++i) {
      ASSIGN_OR_ABORT(auto group_ids, grouper->Consume(span));
    }
  }

  state.SetItemsProcessed(state.iterations() * kRound * span.length);
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

// basic type column
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean}", {boolean()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32}", {int32()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int64}", {int64()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{utf8}", {utf8()})->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{fixed_size_binary(128)}",
                  {fixed_size_binary(128)})
    ->Apply(SetArgs);

// multi types' columns
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean, utf8}", {boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, int32}", {int32(), int32()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, int64}", {int32(), int64()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{boolean, int64, utf8}",
                  {boolean(), int64(), utf8()})
    ->Apply(SetArgs);

// multi types' columns with column resorted
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, boolean, utf8}",
                  {int32(), boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes, "{int32, int64, boolean, utf8}",
                  {int32(), int64(), boolean(), utf8()})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(GrouperWithMultiTypes,
                  "{utf8, int32, int64, fixed_size_binary(128), boolean}",
                  {utf8(), int32(), int64(), fixed_size_binary(128), boolean()})
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
