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
#include <condition_variable>
#include <mutex>

#include "benchmark/benchmark.h"

#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/benchmark_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

static constexpr int64_t kTotalBatchSize = 1000000;

static void FilterOverhead(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  std::vector<arrow::compute::Declaration> filter_node_dec = {{
      "filter",
      FilterNodeOptions{expr}
  }};
  BenchmarkNodeOverhead(
      state,
      ctx,
      expr,
      num_batches,
      batch_size,
      data,
      filter_node_dec
  );
}

static void FilterOverheadIsolated(benchmark::State& state, Expression expr) {
  ExecContext ctx(default_memory_pool(), arrow::internal::GetCpuThreadPool());
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;
  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  FilterNodeOptions options = FilterNodeOptions{expr};
  BenchmarkIsolatedNodeOverhead(
      state,
      ctx,
      expr,
      num_batches,
      batch_size,
      data,
      "filter",
      options
  );
}

arrow::compute::Expression complex_expression =
    less(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
arrow::compute::Expression simple_expression =
    less(call("negate", {field_ref("i64")}), literal(0));
arrow::compute::Expression ref_only_expression = field_ref("bool");

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"batch_size"})
      ->RangeMultiplier(10)
      ->Range(1000, kTotalBatchSize)
      ->UseRealTime();
}

BENCHMARK_CAPTURE(FilterOverheadIsolated, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(FilterOverhead, complex_expression, complex_expression)->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, simple_expression, simple_expression)->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
