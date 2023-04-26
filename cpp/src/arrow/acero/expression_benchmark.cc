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

#include <thread>

#include "arrow/acero/test_util_internal.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/expression.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {

using compute::and_;
using compute::call;
using compute::Expression;
using compute::field_ref;
using compute::literal;

namespace acero {

std::shared_ptr<Scalar> ninety_nine_dict =
    DictionaryScalar::Make(MakeScalar(0), ArrayFromJSON(int64(), "[99]"));

static void BindAndEvaluate(benchmark::State& state, Expression expr) {
  ExecContext ctx;
  auto struct_type = struct_({
      field("int", int64()),
      field("float", float64()),
  });
  auto dataset_schema = schema({
      field("int_arr", int64()),
      field("struct_arr", struct_type),
      field("int_scalar", int64()),
      field("struct_scalar", struct_type),
  });
  ExecBatch input(
      {
          Datum(ArrayFromJSON(int64(), "[0, 2, 4, 8]")),
          Datum(ArrayFromJSON(struct_type,
                              "[[0, 2.0], [4, 8.0], [16, 32.0], [64, 128.0]]")),
          Datum(ScalarFromJSON(int64(), "16")),
          Datum(ScalarFromJSON(struct_type, "[32, 64.0]")),
      },
      /*length=*/4);

  for (auto _ : state) {
    ASSIGN_OR_ABORT(auto bound, expr.Bind(*dataset_schema));
    ABORT_NOT_OK(ExecuteScalarExpression(bound, input, &ctx).status());
  }
}

// A benchmark of SimplifyWithGuarantee using expressions arising from partitioning.
static void SimplifyFilterWithGuarantee(benchmark::State& state, Expression filter,
                                        Expression guarantee) {
  auto dataset_schema = schema({field("a", int64()), field("b", int64())});
  ASSIGN_OR_ABORT(filter, filter.Bind(*dataset_schema));

  for (auto _ : state) {
    ABORT_NOT_OK(SimplifyWithGuarantee(filter, guarantee));
  }
}

static void ExecuteScalarExpressionOverhead(benchmark::State& state, Expression expr) {
  const auto rows_per_batch = static_cast<int32_t>(state.range(0));
  const auto num_batches = 1000000 / rows_per_batch;

  ExecContext ctx;
  auto dataset_schema = schema({
      field("x", int64()),
  });
  std::vector<ExecBatch> inputs(num_batches);
  for (auto& batch : inputs) {
    batch = ExecBatch({Datum(ConstantArrayGenerator::Int64(rows_per_batch, /*value=*/5))},
                      /*length=*/rows_per_batch);
  }

  ASSIGN_OR_ABORT(auto bound, expr.Bind(*dataset_schema));
  for (auto _ : state) {
    for (int it = 0; it < num_batches; ++it)
      ABORT_NOT_OK(ExecuteScalarExpression(bound, inputs[it], &ctx).status());
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * rows_per_batch),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

/// \brief Baseline benchmarks are implemented in pure C++ without arrow for performance
/// comparision.
template <typename BenchmarkType>
void ExecuteScalarExpressionBaseline(benchmark::State& state) {
  const auto rows_per_batch = static_cast<int32_t>(state.range(0));
  const auto num_batches = 1000000 / rows_per_batch;

  std::vector<std::vector<int64_t>> inputs(num_batches,
                                           std::vector<int64_t>(rows_per_batch, 5));
  BenchmarkType benchmark(rows_per_batch);

  for (auto _ : state) {
    for (int it = 0; it < num_batches; ++it) benchmark.Exec(inputs[it]);
  }
  state.counters["rows_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches * rows_per_batch),
      benchmark::Counter::kIsRate);

  state.counters["batches_per_second"] = benchmark::Counter(
      static_cast<double>(state.iterations() * num_batches), benchmark::Counter::kIsRate);
}

auto to_int64 = compute::CastOptions::Safe(int64());
// A fully simplified filter.
auto filter_simple_negative = and_(equal(field_ref("a"), literal(int64_t(99))),
                                   equal(field_ref("b"), literal(int64_t(98))));
auto filter_simple_positive = and_(equal(field_ref("a"), literal(int64_t(99))),
                                   equal(field_ref("b"), literal(int64_t(99))));
// A filter with casts inserted due to converting between the
// assumed-by-default type and the inferred partition schema.
auto filter_cast_negative =
    and_(equal(call("cast", {field_ref("a")}, to_int64), literal(99)),
         equal(call("cast", {field_ref("b")}, to_int64), literal(98)));
auto filter_cast_positive =
    and_(equal(call("cast", {field_ref("a")}, to_int64), literal(99)),
         equal(call("cast", {field_ref("b")}, to_int64), literal(99)));

// An unencoded partition expression for "a=99/b=99".
auto guarantee = and_(equal(field_ref("a"), literal(int64_t(99))),
                      equal(field_ref("b"), literal(int64_t(99))));

// A partition expression for "a=99/b=99" that uses dictionaries (inferred by default).
auto guarantee_dictionary = and_(equal(field_ref("a"), literal(ninety_nine_dict)),
                                 equal(field_ref("b"), literal(ninety_nine_dict)));

auto complex_expression =
    and_(less(field_ref("x"), literal(20)), greater(field_ref("x"), literal(0)));
auto complex_integer_expression =
    call("multiply", {call("add", {field_ref("x"), literal(20)}),
                      call("add", {field_ref("x"), literal(-3)})});
auto simple_expression = call("negate", {field_ref("x")});
auto zero_copy_expression =
    call("cast", {field_ref("x")}, compute::CastOptions::Safe(timestamp(TimeUnit::NANO)));
auto ref_only_expression = field_ref("x");

// Negative queries (partition expressions that fail the filter)
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, negative_filter_simple_guarantee_simple,
                  filter_simple_negative, guarantee);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, negative_filter_cast_guarantee_simple,
                  filter_cast_negative, guarantee);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee,
                  negative_filter_simple_guarantee_dictionary, filter_simple_negative,
                  guarantee_dictionary);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, negative_filter_cast_guarantee_dictionary,
                  filter_cast_negative, guarantee_dictionary);
// Positive queries (partition expressions that pass the filter)
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, positive_filter_simple_guarantee_simple,
                  filter_simple_positive, guarantee);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, positive_filter_cast_guarantee_simple,
                  filter_cast_positive, guarantee);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee,
                  positive_filter_simple_guarantee_dictionary, filter_simple_positive,
                  guarantee_dictionary);
BENCHMARK_CAPTURE(SimplifyFilterWithGuarantee, positive_filter_cast_guarantee_dictionary,
                  filter_cast_positive, guarantee_dictionary);

BENCHMARK_CAPTURE(BindAndEvaluate, simple_array, field_ref("int_arr"));
BENCHMARK_CAPTURE(BindAndEvaluate, simple_scalar, field_ref("int_scalar"));
BENCHMARK_CAPTURE(BindAndEvaluate, nested_array,
                  field_ref(FieldRef("struct_arr", "float")));
BENCHMARK_CAPTURE(BindAndEvaluate, nested_scalar,
                  field_ref(FieldRef("struct_scalar", "float")));

/// \brief Baseline benchmark for complex_expression implemented without arrow
struct ComplexExpressionBaseline {
 public:
  ComplexExpressionBaseline(size_t input_size) {
    /* hack - cuts off a few elemets if the input size is not a multiple of 64 for
     * simplicity. We can't use std::vector<bool> here since it slows down things
     * massively */
    less_20.resize(input_size / 64);
    greater_0.resize(input_size / 64);
    output.resize(input_size / 64);
  }
  void Exec(const std::vector<int64_t>& input) {
    size_t input_size = input.size();

    for (size_t index = 0; index < input_size / 64; index++) {
      size_t value = 0;
      for (size_t bit = 0; bit < 64; bit++) {
        value |= input[index * 64 + bit] > 0;
        value <<= 1;
      }
      greater_0[index] = value;
    }
    for (size_t index = 0; index < input_size / 64; index++) {
      size_t value = 0;
      for (size_t bit = 0; bit < 64; bit++) {
        value |= input[index * 64 + bit] < 20;
        value <<= 1;
      }
      less_20[index] = value;
    }

    for (size_t index = 0; index < input_size / 64; index++) {
      output[index] = greater_0[index] & less_20[index];
    }
  }

 private:
  std::vector<int64_t> greater_0;
  std::vector<int64_t> less_20;
  std::vector<int64_t> output;
};

/// \brief Baseline benchmark for simple_expression implemented without arrow
struct SimpleExpressionBaseline {
  SimpleExpressionBaseline(size_t input_size) { output.resize(input_size); }
  void Exec(const std::vector<int64_t>& input) {
    size_t input_size = input.size();

    for (size_t index = 0; index < input_size; index++) {
      output[index] = -input[index];
    }
  }
  std::vector<int64_t> output;
};

BENCHMARK_CAPTURE(ExecuteScalarExpressionOverhead, complex_integer_expression,
                  complex_expression)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();

BENCHMARK_CAPTURE(ExecuteScalarExpressionOverhead, complex_expression, complex_expression)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
BENCHMARK_TEMPLATE(ExecuteScalarExpressionBaseline, ComplexExpressionBaseline)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
BENCHMARK_CAPTURE(ExecuteScalarExpressionOverhead, simple_expression, simple_expression)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
BENCHMARK_TEMPLATE(ExecuteScalarExpressionBaseline, SimpleExpressionBaseline)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
BENCHMARK_CAPTURE(ExecuteScalarExpressionOverhead, zero_copy_expression,
                  zero_copy_expression)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
BENCHMARK_CAPTURE(ExecuteScalarExpressionOverhead, ref_only_expression,
                  ref_only_expression)
    ->ArgNames({"rows_per_batch"})
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(),
                       std::thread::hardware_concurrency())
    ->UseRealTime();
}  // namespace acero
}  // namespace arrow
