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
#include "arrow/compute/exec/expression.h"
#include "arrow/compute/exec/test_util.h"
#include "arrow/dataset/partition.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"

namespace arrow {
namespace compute {

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

}  // namespace compute
}  // namespace arrow
