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
             << "              given: " << expr.ToString() << std::endl
             << "           expected: " << expected.ToString() << std::endl
             << "                was: " << simplified.ValueOrDie()->ToString();
    }
  }

  std::shared_ptr<ScalarExpression> always = ScalarExpression::Make(true);
  std::shared_ptr<ScalarExpression> never = ScalarExpression::Make(false);
};

TEST_F(ExpressionsTest, Basics) {
  using namespace string_literals;

  ASSERT_TRUE("a"_.Equals("a"_));
  ASSERT_FALSE("a"_.Equals("b"_));

  ASSERT_TRUE(("b"_ == 3).Equals("b"_ == 3));
  ASSERT_FALSE(("b"_ == 3).Equals("b"_ < 3));
  ASSERT_FALSE(("b"_ == 3).Equals("b"_));

  AssertSimplifiesTo("b"_ == 3, "b"_ > 5 and "b"_ < 10, *never);
  AssertSimplifiesTo("b"_ > 3, "b"_ > 5 and "b"_ < 10, *always);
}

TEST(FilterTest, Basics) {
  using namespace string_literals;

  auto batch = RecordBatch::FromStructArray(
      ArrayFromJSON(struct_({field("a", int64()), field("b", float64())}), R"([
      {"a": 0, "b": -0.1},
      {"a": 0, "b": 0.3},
      {"a": 1, "b": 0.2},
      {"a": 2, "b": -0.1},
      {"a": 0, "b": 0.1},
      {"a": 0, "b": 1.0}
  ])"));

  arrow::compute::FunctionContext ctx;
  std::shared_ptr<BooleanArray> filter;
  ASSERT_OK(ExpressionFilter(MakeShared("a"_ == 0 and "b"_ > 0.0 and "b"_ < 1.0))
                .Execute(&ctx, *batch, &filter));

  auto expected_filter = ArrayFromJSON(boolean(), "[0, 1, 0, 0, 1, 0]");
  ASSERT_ARRAYS_EQUAL(*expected_filter, *filter);
}

}  // namespace dataset
}  // namespace arrow
