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
#include "arrow/compute/exec/benchmark_util.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/exec/task_util.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/future_util.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

static constexpr int64_t kTotalBatchSize = 1000000;

static void ProjectionOverhead(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  std::vector<arrow::compute::Declaration> project_node_dec = {
      {"project", ProjectNodeOptions{{expr}}}};
  ASSERT_OK(
      BenchmarkNodeOverhead(state, num_batches, batch_size, data, project_node_dec));
}

static void ProjectionOverheadIsolated(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  ProjectNodeOptions options = ProjectNodeOptions{{expr}};
  ASSERT_OK(BenchmarkIsolatedNodeOverhead(state, expr, num_batches, batch_size, data,
                                          "project", options));
}

arrow::compute::Expression complex_expression =
    and_(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
arrow::compute::Expression simple_expression = call("negate", {field_ref("i64")});
arrow::compute::Expression zero_copy_expression = call(
    "cast", {field_ref("i64")}, compute::CastOptions::Safe(timestamp(TimeUnit::NANO)));
arrow::compute::Expression ref_only_expression = field_ref("i64");

void SetArgs(benchmark::internal::Benchmark* bench) {
  bench->ArgNames({"batch_size"})
      ->RangeMultiplier(10)
      ->Range(1000, kTotalBatchSize)
      ->UseRealTime();
}

BENCHMARK_CAPTURE(ProjectionOverheadIsolated, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverheadIsolated, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(ProjectionOverhead, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, zero_copy_expression, zero_copy_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(ProjectionOverhead, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

}  // namespace compute
}  // namespace arrow
