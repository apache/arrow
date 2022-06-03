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

#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

static constexpr int64_t total_batch_size = 1e6;

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (auto batch_size = 1; batch_size <= 1e6; batch_size *= 10) {
    auto num_batches = total_batch_size / batch_size;
    bench->ArgNames({"batch_size, num_batches"})
        ->Args({batch_size, num_batches})
        ->UseRealTime();
  }
}

/*
should be able to vary
1. batch size
2. expression complexity
*/
static void ExecuteScalarProjectionOverhead(benchmark::State& state, Expression expr) {
  const auto batch_size = static_cast<int32_t>(state.range(0));
  const auto num_batches = static_cast<int32_t>(state.range(1));

  auto data = MakeRandomBatches(schema({field("i64", int64()), field("bool", boolean())}),
                                num_batches, batch_size);
  ExecContext ctx;

  for (auto _ : state) {
    ASSERT_OK_AND_ASSIGN(auto plan, ExecPlan::Make());
    // CountOptions options(CountOptions::ONLY_VALID);
    AsyncGenerator<util::optional<ExecBatch>> sink_gen;
    ASSERT_OK(Declaration::Sequence(
                  {
                      {"source",
                       SourceNodeOptions{data.schema,
                                         data.gen(/*parallel=*/false, /*slow=*/false)},
                       "custom_source_label"},
                      {"project", ProjectNodeOptions{{
                                      field_ref("bool"),
                                      expr,
                                  }}},
                      {"sink", SinkNodeOptions{&sink_gen}, "custom_sink_label"},
                  })
                  .AddToPlan(plan.get()));
    ASSERT_FINISHES_OK(StartAndCollect(plan.get(), sink_gen));
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * batch_size),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

auto complex_expression =
    and_(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
auto simple_expression = call("negate", {field_ref("i64")});
auto zero_copy_expression = call("cast", {field_ref("i64")},
                                 compute::CastOptions::Safe(timestamp(TimeUnit::NANO)));
auto ref_only_expression = field_ref("i64");

BENCHMARK_CAPTURE(ExecuteScalarProjectionOverhead, complex_expression, complex_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(ExecuteScalarProjectionOverhead, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ExecuteScalarProjectionOverhead, zero_copy_expression,
                  zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ExecuteScalarProjectionOverhead, ref_only_expression,
                  ref_only_expression)
    ->Apply(SetArgs);
}  // namespace compute
}  // namespace arrow
