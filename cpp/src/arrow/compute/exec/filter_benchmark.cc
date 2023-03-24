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

#include "arrow/compute/exec/benchmark_util.h"
#include "arrow/compute/exec/options.h"
#include "arrow/compute/expression.h"
#include "arrow/record_batch.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

constexpr int64_t kTotalBatchSize = 1000000;
constexpr auto kSeed = 0x94378165;

// Will return batches of size length, with fields as specified.
// null_probability controls the likelihood that an element within the batch is null,
// across all fields. bool_true_probability controls the likelihood that an element
// belonging to a boolean field is true.

static std::shared_ptr<arrow::RecordBatch> GetBatchesWithNullProbability(
    const FieldVector& fields, int64_t length, double null_probability,
    double bool_true_probability = 0.5) {
  std::vector<std::shared_ptr<Array>> arrays(fields.size());
  auto rand = random::RandomArrayGenerator(kSeed);
  for (size_t i = 0; i < fields.size(); i++) {
    const auto& field = fields[i];
    if (field.get()->name() == "bool") {
      arrays[i] = rand.Boolean(length, bool_true_probability, null_probability);
    } else {
      arrays[i] = rand.ArrayOf(field.get()->type(), length, null_probability);
    }
  }
  return RecordBatch::Make(schema(fields), length, std::move(arrays));
}

BatchesWithSchema MakeRandomBatchesWithNullProbability(
    std::shared_ptr<Schema> schema, int num_batches, int batch_size,
    double null_probability = 0.5, double bool_true_probability = 0.5) {
  BatchesWithSchema out;
  out.batches.resize(num_batches);

  for (int i = 0; i < num_batches; ++i) {
    out.batches[i] = ExecBatch(*GetBatchesWithNullProbability(
        schema->fields(), batch_size, null_probability, bool_true_probability));
    out.batches[i].values.emplace_back(i);
  }
  out.schema = std::move(schema);
  return out;
}

static void FilterOverhead(benchmark::State& state, std::vector<Expression> expr_vector) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const double null_prob = state.range(1) / 100.0;
  const double bool_true_probability = state.range(2) / 100.0;
  const int32_t num_batches = kTotalBatchSize / batch_size;

  arrow::compute::BatchesWithSchema data = MakeRandomBatchesWithNullProbability(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size,
      null_prob, bool_true_probability);
  std::vector<arrow::compute::Declaration> filter_node_dec;
  for (Expression expr : expr_vector) {
    filter_node_dec.push_back({"filter", FilterNodeOptions(expr)});
  }
  ASSERT_OK(BenchmarkNodeOverhead(state, num_batches, batch_size, data, filter_node_dec));
}

static void FilterOverheadIsolated(benchmark::State& state, Expression expr) {
  const int32_t batch_size = static_cast<int32_t>(state.range(0));
  const int32_t num_batches = kTotalBatchSize / batch_size;
  arrow::compute::BatchesWithSchema data = MakeRandomBatches(
      schema({field("i64", int64()), field("bool", boolean())}), num_batches, batch_size);
  FilterNodeOptions options = FilterNodeOptions{expr};
  ASSERT_OK(BenchmarkIsolatedNodeOverhead(state, expr, num_batches, batch_size, data,
                                          "filter", options));
}

arrow::compute::Expression complex_expression =
    less(less(field_ref("i64"), literal(20)), greater(field_ref("i64"), literal(0)));
arrow::compute::Expression simple_expression =
    less(call("negate", {field_ref("i64")}), literal(0));
arrow::compute::Expression ref_only_expression = field_ref("bool");

arrow::compute::Expression is_not_null_expression =
    not_(is_null(field_ref("bool"), false));
arrow::compute::Expression is_true_expression = equal(field_ref("bool"), literal(true));
arrow::compute::Expression is_not_null_and_true_expression =
    and_(is_not_null_expression, is_true_expression);

void SetArgs(benchmark::internal::Benchmark* bench) {
  for (int batch_size = 1000; batch_size <= kTotalBatchSize; batch_size *= 10) {
    bench->ArgNames({"batch_size", "null_prob", "bool_true_prob"})
        ->Args({batch_size, 0, 50})
        ->UseRealTime();
  }
}

void SetSelectivityArgs(benchmark::internal::Benchmark* bench) {
  for (int batch_size = 1000; batch_size <= kTotalBatchSize; batch_size *= 10) {
    for (double null_prob : {0.1, 0.5, 0.75, 1.0}) {
      bench->ArgNames({"batch_size", "null_prob", "bool_true_prob"})
          ->Args({batch_size, static_cast<int64_t>(null_prob * 100.0), 50})
          ->UseRealTime();
    }
  }
}

void SetMultiPassArgs(benchmark::internal::Benchmark* bench) {
  for (int batch_size = 1000; batch_size <= kTotalBatchSize; batch_size *= 10) {
    for (double null_prob : {0.0, 0.25, 0.5, 0.75}) {
      // we keep the number of selected elements constant for all benchmarks.
      double bool_true_prob = 0.25 / (1 - null_prob);
      bench->ArgNames({"batch_size", "null_prob", "bool_true_prob"})
          ->Args({batch_size, static_cast<int64_t>(null_prob * 100.0),
                  static_cast<int64_t>(bool_true_prob * 100)})
          ->UseRealTime();
    }
  }
}

BENCHMARK_CAPTURE(FilterOverheadIsolated, complex_expression, complex_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, simple_expression, simple_expression)
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverheadIsolated, ref_only_expression, ref_only_expression)
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(FilterOverhead, complex_expression, {complex_expression})
    ->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, simple_expression, {simple_expression})->Apply(SetArgs);
BENCHMARK_CAPTURE(FilterOverhead, ref_only_expression, {ref_only_expression})
    ->Apply(SetArgs);

BENCHMARK_CAPTURE(FilterOverhead, selectivity_benchmark, {is_not_null_expression})
    ->Apply(SetSelectivityArgs);

BENCHMARK_CAPTURE(FilterOverhead, not_null_to_is_true_multipass_benchmark,
                  {is_not_null_expression, is_true_expression})
    ->Apply(SetMultiPassArgs);
BENCHMARK_CAPTURE(FilterOverhead, not_null_and_is_true_singlepass_benchmark,
                  {is_not_null_and_true_expression})
    ->Apply(SetMultiPassArgs);

}  // namespace compute
}  // namespace arrow
