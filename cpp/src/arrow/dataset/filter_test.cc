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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/api.h"
#include "arrow/dataset/api.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arrow {
namespace dataset {

class ExpressionsTest : public ::testing::Test {
 public:
  void AssertSimplifiesTo(OperatorExpression expr, const Expression& given,
                          const Expression& expected) {
    auto simplified = expr.Assume(given);
    ASSERT_OK(simplified.status());
    if (!simplified.ValueOrDie()->Equals(expected)) {
      FAIL() << "  simplification of: " << expr.ToString() << std::endl
             << "              given: " << given.ToString() << std::endl
             << "           expected: " << expected.ToString() << std::endl
             << "                was: " << simplified.ValueOrDie()->ToString();
    }
  }

  template <typename... T>
  void AssertOperandsAre(OperatorExpression expr, ExpressionType::type type,
                         T... expected_operands) {
    ASSERT_EQ(expr.type(), type);
    ASSERT_EQ(expr.operands().size(), sizeof...(T));
    std::shared_ptr<Expression> expected_operand_ptrs[] = {expected_operands.Copy()...};

    for (size_t i = 0; i < sizeof...(T); ++i) {
      ASSERT_TRUE(expr.operands()[i]->Equals(expected_operand_ptrs[i]));
    }
  }

  std::shared_ptr<ScalarExpression> always = ScalarExpression::Make(true);
  std::shared_ptr<ScalarExpression> never = ScalarExpression::Make(false);
};

TEST_F(ExpressionsTest, Equality) {
  using namespace string_literals;

  ASSERT_TRUE("a"_.Equals("a"_));
  ASSERT_FALSE("a"_.Equals("b"_));

  ASSERT_TRUE(("b"_ == 3).Equals("b"_ == 3));
  ASSERT_FALSE(("b"_ == 3).Equals("b"_ < 3));
  ASSERT_FALSE(("b"_ == 3).Equals("b"_));

  // ordering matters
  ASSERT_FALSE(("b"_ > 2 and "b"_ < 3).Equals("b"_ < 3 and "b"_ > 2));
}

TEST_F(ExpressionsTest, Simplification) {
  using namespace string_literals;

  // chained "and" expressions are flattened
  auto multi_and = "b"_ > 5 and "b"_ < 10 and "b"_ != 7;
  AssertOperandsAre(multi_and, ExpressionType::AND, "b"_ > 5, "b"_ < 10, "b"_ != 7);

  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ == 3, *never);
  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ == 6, *always);
  AssertSimplifiesTo("b"_ > 5, "b"_ == 3 or "b"_ == 6, "b"_ > 5);
  AssertSimplifiesTo("b"_ > 7, "b"_ == 3 or "b"_ == 6, *never);
  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ > 6 and "b"_ < 13, "b"_ < 10);

  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ > 6, *never);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ == 3, *always);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ > 3, "b"_ == 4);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ >= 3, "b"_ == 3 or "b"_ == 4);
}

class FilterTest : public ::testing::Test {
 public:
  void AssertFilter(OperatorExpression expr, std::vector<std::shared_ptr<Field>> fields,
                    std::string batch_json) {
    // expected filter result is in the "in" field
    fields.push_back(field("in", boolean()));

    auto batch_array = ArrayFromJSON(struct_(std::move(fields)), std::move(batch_json));
    std::shared_ptr<RecordBatch> batch;
    ASSERT_OK(RecordBatch::FromStructArray(batch_array, &batch));

    auto expected_filter = batch->GetColumnByName("in");

    std::shared_ptr<BooleanArray> filter;
    ASSERT_OK(EvaluateExpression(&ctx_, expr, *batch, &filter));

    ASSERT_ARRAYS_EQUAL(*expected_filter, *filter);
  }

  arrow::compute::FunctionContext ctx_;
};

TEST_F(FilterTest, Basics) {
  using namespace string_literals;

  AssertFilter("a"_ == 0 and "b"_ > 0.0 and "b"_ < 1.0,
               {field("a", int64()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.3, "in": 1},
      {"a": 1, "b":  0.2, "in": 0},
      {"a": 2, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.1, "in": 1},
      {"a": 0, "b":  1.0, "in": 0}
  ])");
}

}  // namespace dataset
}  // namespace arrow
