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

#include <stdlib.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type_fwd.h"
#include "benchmark/benchmark.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tests/timed_evaluate.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

using arrow::random::RandomArrayGenerator;

int num_records = 1 * MILLION;
int num_batches = 16 * THOUSAND;

int32_t int32_t_min = std::numeric_limits<int32_t>::min();
int32_t int32_t_max = std::numeric_limits<int32_t>::max();
int64_t int64_t_min = std::numeric_limits<int64_t>::min();
int64_t int64_t_max = std::numeric_limits<int64_t>::max();

RandomArrayGenerator rag(0);

static void TimedTestAdd3(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_sum = field("add", int64());

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      int64());
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, int64());

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Int64(num_batches, int64_t_min, int64_t_max, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_OK(status);
}

static void TimedTestBigNested(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a < 10)
  //   10
  // else if (a < 20)
  //   20
  // ..
  // ..
  // else if (a < 190)
  //   190
  // else
  //   200
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto top_node = TreeExprBuilder::MakeLiteral(200);
  for (int thresh = 190; thresh > 0; thresh -= 10) {
    auto literal = TreeExprBuilder::MakeLiteral(thresh);
    auto condition =
        TreeExprBuilder::MakeFunction("less_than", {node_a, literal}, boolean());
    auto if_node = TreeExprBuilder::MakeIf(condition, literal, top_node, int32());
    top_node = if_node;
  }
  auto expr = TreeExprBuilder::MakeExpression(top_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Int64(num_batches, 0, 250, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int32Type, int32_t>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestExtractYear(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", arrow::date64());
  auto schema = arrow::schema({field0});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int64());

  // Build expression
  auto expr = TreeExprBuilder::MakeExpression("extractYear", {field0}, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Int64(num_batches, int64_t_min, int64_t_max, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Date64Type, int64_t>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterAdd2(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // Build expression
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field0)},
      int64());
  auto less_than = TreeExprBuilder::MakeFunction(
      "less_than", {sum, TreeExprBuilder::MakeField(field2)}, boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Int64(num_batches, int64_t_min, int64_t_max, 0));
    }
  }

  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterLike(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // build expression.
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto pattern_node = TreeExprBuilder::MakeStringLiteral("%yellow%");
  auto like_yellow =
      TreeExprBuilder::MakeFunction("like", {node_a, pattern_node}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(like_yellow);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.String(num_batches, 0, 32, 0));
    }
  }

  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestCastFloatFromString(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool = arrow::default_memory_pool();

  auto field_result = field("res", arrow::float64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto fn = TreeExprBuilder::MakeFunction("castFLOAT8", {node_a}, arrow::float64());
  auto expr = TreeExprBuilder::MakeExpression(fn, field_result);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Float32String(num_batches, 0, 1, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestCastIntFromString(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool = arrow::default_memory_pool();

  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto fn = TreeExprBuilder::MakeFunction("castINT", {node_a}, int32());
  auto expr = TreeExprBuilder::MakeExpression(fn, field_result);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] = std::make_shared<ArrayPtr>(
          rag.Int32String(num_batches, int32_t_min, int32_t_max, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestAllocs(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int32());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto length = TreeExprBuilder::MakeFunction("octet_length", {upper}, int32());
  auto expr = TreeExprBuilder::MakeExpression(length, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.String(num_batches, 64, 64, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestOutputStringAllocs(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("abcdefghijklmnopqrstuvwxyz", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();
  // output field
  auto field_res = field("res", utf8());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto length = TreeExprBuilder::MakeFunction("octet_length", {upper}, int32());
  auto expr = TreeExprBuilder::MakeExpression(upper, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.String(num_batches, 64, 64, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  ASSERT_OK((TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool_, num_records, num_batches, state)));
}
// following two tests are for benchmark optimization of
// in expr. will be used in follow-up PRs to optimize in expr.

static void TimedTestMultiOr(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // booleanOr(a = string1, a = string2, ..)
  auto node_a = TreeExprBuilder::MakeField(field_a);

  NodeVector boolean_functions;
  for (int thresh = 1; thresh <= 32; thresh++) {
    auto literal =
        TreeExprBuilder::MakeStringLiteral(rag.String(1, 0, 250, 0)->ToString());
    auto condition = TreeExprBuilder::MakeFunction("equal", {node_a, literal}, boolean());
    boolean_functions.push_back(condition);
  }

  auto boolean_or = TreeExprBuilder::MakeOr(boolean_functions);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.String(num_batches, 0, 250, 0));
    }
  }

  ProjectEvaluator evaluator(projector);
  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool_, 100 * THOUSAND, 16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void TimedTestInExpr(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // a in (string1, string2, ..)
  auto node_a = TreeExprBuilder::MakeField(field_a);

  std::unordered_set<std::string> values;
  for (int thresh = 1; thresh <= 32; thresh++) {
    values.insert(rag.String(1, 0, 250, 0)->ToString());
  }
  auto boolean_or = TreeExprBuilder::MakeInExpressionString(node_a, values);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.String(num_batches, 0, 250, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, arrays, pool_, 100 * THOUSAND, 16 * THOUSAND, state);

  ASSERT_OK(status);
}

static void DoDecimalAdd3(benchmark::State& state, int32_t precision, int32_t scale,
                          bool large = false) {
  // schema for input fields
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field0 = field("f0", decimal_type);
  auto field1 = field("f1", decimal_type);
  auto field2 = field("f2", decimal_type);
  auto schema = arrow::schema({field0, field1, field2});

  Decimal128TypePtr add2_type;
  auto status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                               {decimal_type, decimal_type}, &add2_type);

  Decimal128TypePtr output_type;
  status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                          {add2_type, decimal_type}, &output_type);

  // output field
  auto field_sum = field("add", output_type);

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      add2_type);
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, output_type);

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Decimal128(decimal_type, num_batches, 0, 64, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  status = TimedEvaluate<arrow::Decimal128Type, arrow::Decimal128>(
      schema, evaluator, arrays, arrow::default_memory_pool(), 1 * MILLION, 16 * THOUSAND,
      state);
  ASSERT_OK(status);
}

static void DoDecimalAdd2(benchmark::State& state, int32_t precision, int32_t scale,
                          bool large = false) {
  // schema for input fields
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field0 = field("f0", decimal_type);
  auto field1 = field("f1", decimal_type);
  auto schema = arrow::schema({field0, field1});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(
      DecimalTypeUtil::kOpAdd, {decimal_type, decimal_type}, &output_type);

  // output field
  auto field_sum = field("add", output_type);

  // Build expression
  auto sum = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {sum}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  int num_fields = schema->num_fields();
  std::shared_ptr<ArrayPtr> arrays[num_fields * NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    for (int col = 0; col < num_fields; col++) {
      arrays[col * NUM_BATCHES + i] =
          std::make_shared<ArrayPtr>(rag.Decimal128(decimal_type, num_batches, 0, 64, 0));
    }
  }

  ProjectEvaluator evaluator(projector);

  status = TimedEvaluate<arrow::Decimal128Type, arrow::Decimal128>(
      schema, evaluator, arrays, arrow::default_memory_pool(), 1 * MILLION, 16 * THOUSAND,
      state);
  ASSERT_OK(status);
}

static void DecimalAdd2Fast(benchmark::State& state) {
  // use lesser precision to test the fast-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision - 6, 18);
}

static void DecimalAdd2LeadingZeroes(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 6);
}

static void DecimalAdd2LeadingZeroesWithDiv(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 18);
}

static void DecimalAdd2Large(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 18, true);
}

static void DecimalAdd3Fast(benchmark::State& state) {
  // use lesser precision to test the fast-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision - 6, 18);
}

static void DecimalAdd3LeadingZeroes(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 6);
}

static void DecimalAdd3LeadingZeroesWithDiv(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 18);
}

static void DecimalAdd3Large(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 18, true);
}

BENCHMARK(TimedTestAdd3)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestBigNested)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestExtractYear)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterAdd2)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterLike)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCastFloatFromString)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCastIntFromString)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestAllocs)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestOutputStringAllocs)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestMultiOr)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestInExpr)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2Fast)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2LeadingZeroes)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2LeadingZeroesWithDiv)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2Large)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3Fast)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3LeadingZeroes)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3LeadingZeroesWithDiv)->MinTime(1.0)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3Large)->MinTime(1.0)->Unit(benchmark::kMicrosecond);

}  // namespace gandiva
