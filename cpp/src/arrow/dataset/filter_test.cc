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

#include "arrow/dataset/filter.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/compute/api.h"
#include "arrow/compute/context.h"
#include "arrow/compute/kernels/take.h"
#include "arrow/dataset/test_util.h"
#include "arrow/record_batch.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace dataset {

// clang-format off
using string_literals::operator"" _;
// clang-format on

using internal::checked_cast;
using internal::checked_pointer_cast;

using E = TestExpression;

class ExpressionsTest : public ::testing::Test {
 public:
  void AssertSimplifiesTo(const Expression& expr, const Expression& given,
                          const Expression& expected) {
    ASSERT_OK_AND_ASSIGN(auto expr_type, expr.Validate(*schema_));
    ASSERT_OK_AND_ASSIGN(auto given_type, given.Validate(*schema_));
    ASSERT_OK_AND_ASSIGN(auto expected_type, expected.Validate(*schema_));

    EXPECT_TRUE(expr_type->Equals(expected_type));
    EXPECT_TRUE(given_type->Equals(boolean()));

    auto simplified = expr.Assume(given);
    ASSERT_EQ(E{simplified}, E{expected})
        << "  simplification of: " << expr.ToString() << std::endl
        << "              given: " << given.ToString() << std::endl;
  }

  std::shared_ptr<Schema> schema_ =
      schema({field("a", int32()), field("b", int32()), field("f", float64())});
  std::shared_ptr<ScalarExpression> always = scalar(true);
  std::shared_ptr<ScalarExpression> never = scalar(false);
};

TEST_F(ExpressionsTest, Equality) {
  ASSERT_EQ(E{"a"_}, E{"a"_});
  ASSERT_NE(E{"a"_}, E{"b"_});

  ASSERT_EQ(E{"b"_ == 3}, E{"b"_ == 3});
  ASSERT_NE(E{"b"_ == 3}, E{"b"_ < 3});
  ASSERT_NE(E{"b"_ == 3}, E{"b"_});

  // ordering matters
  ASSERT_EQ(E{"b"_ == 3}, E{"b"_ == 3});
  ASSERT_NE(E{"b"_ == 3}, E{"b"_ < 3});
  ASSERT_NE(E{"b"_ == 3}, E{"b"_});

  ASSERT_EQ(E("b"_ > 2 and "b"_ < 3), E("b"_ > 2 and "b"_ < 3));
  ASSERT_NE(E("b"_ > 2 and "b"_ < 3), E("b"_ < 3 and "b"_ > 2));
}

TEST_F(ExpressionsTest, SimplificationOfCompoundQuery) {
  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ == 3, *never);
  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ == 6, *always);

  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ > 6, *never);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ == 3, *always);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ > 3, "b"_ == 4);
  AssertSimplifiesTo("b"_ == 3 or "b"_ == 4, "b"_ >= 3, "b"_ == 3 or "b"_ == 4);

  AssertSimplifiesTo("f"_ > 0.5 and "f"_ < 1.5, not("f"_ < 0.0 or "f"_ > 1.0),
                     "f"_ > 0.5);

  AssertSimplifiesTo("b"_ == 4, "a"_ == 0, "b"_ == 4);

  AssertSimplifiesTo("a"_ == 3 or "b"_ == 4, "a"_ == 0, "b"_ == 4);
}

TEST_F(ExpressionsTest, SimplificationAgainstCompoundCondition) {
  AssertSimplifiesTo("b"_ > 5, "b"_ == 3 or "b"_ == 6, "b"_ > 5);
  AssertSimplifiesTo("b"_ > 7, "b"_ == 3 or "b"_ == 6, *never);
  AssertSimplifiesTo("b"_ > 5 and "b"_ < 10, "b"_ > 6 and "b"_ < 13, "b"_ < 10);
}

TEST_F(ExpressionsTest, SimplificationToNull) {
  auto null = scalar(std::make_shared<BooleanScalar>());
  auto null32 = scalar(std::make_shared<Int32Scalar>());

  AssertSimplifiesTo(*equal(field_ref("b"), null32), "b"_ == 3, *null);
  AssertSimplifiesTo(*not_equal(field_ref("b"), null32), "b"_ == 3, *null);

  // Kleene logic applies here
  AssertSimplifiesTo(*not_equal(field_ref("b"), null32) and "b"_ > 3, "b"_ == 3, *never);
  AssertSimplifiesTo(*not_equal(field_ref("b"), null32) and "b"_ > 2, "b"_ == 3, *null);
  AssertSimplifiesTo(*not_equal(field_ref("b"), null32) or "b"_ > 3, "b"_ == 3, *null);
  AssertSimplifiesTo(*not_equal(field_ref("b"), null32) or "b"_ > 2, "b"_ == 3, *always);
}

class FilterTest : public ::testing::Test {
 public:
  FilterTest() { evaluator_ = std::make_shared<TreeEvaluator>(default_memory_pool()); }

  Result<Datum> DoFilter(const Expression& expr,
                         std::vector<std::shared_ptr<Field>> fields,
                         std::string batch_json,
                         std::shared_ptr<BooleanArray>* expected_mask = nullptr) {
    // expected filter result is in the "in" field
    fields.push_back(field("in", boolean()));
    auto batch = RecordBatchFromJSON(schema(fields), batch_json);
    if (expected_mask) {
      *expected_mask = checked_pointer_cast<BooleanArray>(batch->GetColumnByName("in"));
    }

    ASSERT_OK_AND_ASSIGN(auto expr_type, expr.Validate(*batch->schema()));
    EXPECT_TRUE(expr_type->Equals(boolean()));

    return evaluator_->Evaluate(expr, *batch);
  }

  void AssertFilter(const std::shared_ptr<Expression>& expr,
                    std::vector<std::shared_ptr<Field>> fields,
                    const std::string& batch_json) {
    AssertFilter(*expr, std::move(fields), batch_json);
  }

  void AssertFilter(const Expression& expr, std::vector<std::shared_ptr<Field>> fields,
                    const std::string& batch_json) {
    std::shared_ptr<BooleanArray> expected_mask;
    auto mask_res =
        DoFilter(expr, std::move(fields), std::move(batch_json), &expected_mask);
    ASSERT_OK(mask_res.status());

    auto mask = std::move(mask_res).ValueOrDie();
    ASSERT_TRUE(mask.type()->Equals(null()) || mask.type()->Equals(boolean()));

    if (mask.is_array()) {
      ASSERT_ARRAYS_EQUAL(*expected_mask, *mask.make_array());
      return;
    }

    ASSERT_TRUE(mask.is_scalar());
    auto mask_scalar = mask.scalar();
    if (!mask_scalar->is_valid) {
      ASSERT_EQ(expected_mask->null_count(), expected_mask->length());
      return;
    }

    TypedBufferBuilder<bool> builder;
    ASSERT_OK(builder.Append(expected_mask->length(),
                             checked_cast<const BooleanScalar&>(*mask_scalar).value));

    std::shared_ptr<Buffer> values;
    ASSERT_OK(builder.Finish(&values));

    ASSERT_ARRAYS_EQUAL(*expected_mask, BooleanArray(expected_mask->length(), values));
  }

  std::shared_ptr<ExpressionEvaluator> evaluator_;
};

TEST_F(FilterTest, Trivial) {
  // Note that we should expect these trivial expressions will never be evaluated against
  // record batches; since they're trivial, evaluation is not necessary.
  AssertFilter(scalar(true), {field("a", int32()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": 1},
      {"a": 0, "b":  0.3, "in": 1},
      {"a": 1, "b":  0.2, "in": 1},
      {"a": 2, "b": -0.1, "in": 1},
      {"a": 0, "b":  0.1, "in": 1},
      {"a": 0, "b": null, "in": 1},
      {"a": 0, "b":  1.0, "in": 1}
  ])");

  AssertFilter(scalar(false), {field("a", int32()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.3, "in": 0},
      {"a": 1, "b":  0.2, "in": 0},
      {"a": 2, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.1, "in": 0},
      {"a": 0, "b": null, "in": 0},
      {"a": 0, "b":  1.0, "in": 0}
  ])");

  AssertFilter(*scalar(std::shared_ptr<Scalar>(new BooleanScalar)),
               {field("a", int32()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": null},
      {"a": 0, "b":  0.3, "in": null},
      {"a": 1, "b":  0.2, "in": null},
      {"a": 2, "b": -0.1, "in": null},
      {"a": 0, "b":  0.1, "in": null},
      {"a": 0, "b": null, "in": null},
      {"a": 0, "b":  1.0, "in": null}
  ])");
}

TEST_F(FilterTest, Basics) {
  AssertFilter("a"_ == 0 and "b"_ > 0.0 and "b"_ < 1.0,
               {field("a", int32()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.3, "in": 1},
      {"a": 1, "b":  0.2, "in": 0},
      {"a": 2, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.1, "in": 1},
      {"a": 0, "b": null, "in": null},
      {"a": 0, "b":  1.0, "in": 0}
  ])");

  AssertFilter("a"_ != 0 and "b"_ > 0.1, {field("a", int32()), field("b", float64())},
               R"([
      {"a": 0, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.3, "in": 0},
      {"a": 1, "b":  0.2, "in": 1},
      {"a": 2, "b": -0.1, "in": 0},
      {"a": 0, "b":  0.1, "in": 0},
      {"a": 0, "b": null, "in": null},
      {"a": 0, "b":  1.0, "in": 0}
  ])");
}

TEST_F(FilterTest, ConditionOnAbsentColumn) {
  AssertFilter("a"_ == 0 and "b"_ > 0.0 and "b"_ < 1.0 and "absent"_ == 0,
               {field("a", int32()), field("b", float64())}, R"([
      {"a": 0, "b": -0.1, "in": null},
      {"a": 0, "b":  0.3, "in": null},
      {"a": 1, "b":  0.2, "in": null},
      {"a": 2, "b": -0.1, "in": null},
      {"a": 0, "b":  0.1, "in": null},
      {"a": 0, "b": null, "in": null},
      {"a": 0, "b":  1.0, "in": null}
  ])");
}

TEST_F(FilterTest, DISABLED_KleeneTruthTables) {
  // FIXME(bkietz) enable this test after ARROW-6396
  // TODO(bkietz) also test various ranks against each other
  AssertFilter("a"_ and "b"_, {field("a", boolean()), field("b", boolean())}, R"([
    {"a":null,  "b":null,  "in":null},
    {"a":null,  "b":true,  "in":null},
    {"a":null,  "b":false, "in":false},

    {"a":true,  "b":true,  "in":true},
    {"a":true,  "b":false, "in":false},

    {"a":false,  "b":false,  "in":false}
  ])");

  AssertFilter("a"_ and "b"_, {field("a", boolean()), field("b", boolean())}, R"([
    {"a":null,  "b":null,  "in":null},
    {"a":null,  "b":true,  "in":true},
    {"a":null,  "b":false, "in":null},

    {"a":true,  "b":true,  "in":true},
    {"a":true,  "b":false, "in":true},

    {"a":false,  "b":false,  "in":false}
  ])");
}

class TakeExpression : public CustomExpression {
 public:
  TakeExpression(std::shared_ptr<Expression> operand, std::shared_ptr<Array> dictionary)
      : operand_(std::move(operand)), dictionary_(std::move(dictionary)) {}

  std::string ToString() const override {
    return dictionary_->ToString() + "[" + operand_->ToString() + "]";
  }

  std::shared_ptr<Expression> Copy() const override {
    return std::make_shared<TakeExpression>(*this);
  }

  bool Equals(const Expression& other) const override {
    // in a real CustomExpression this would need to be more sophisticated
    return other.type() == ExpressionType::CUSTOM && ToString() == other.ToString();
  }

  Result<std::shared_ptr<DataType>> Validate(const Schema& schema) const override {
    ARROW_ASSIGN_OR_RAISE(auto operand_type, operand_->Validate(schema));
    if (!is_integer(operand_type->id())) {
      return Status::TypeError("Take indices must be integral, not ", *operand_type);
    }
    return dictionary_->type();
  }

  class Evaluator : public TreeEvaluator {
   public:
    using TreeEvaluator::TreeEvaluator;

    using TreeEvaluator::Evaluate;

    Result<compute::Datum> Evaluate(const CustomExpression& expr,
                                    const RecordBatch& batch) const override {
      const auto& take_expr = checked_cast<const TakeExpression&>(expr);
      ARROW_ASSIGN_OR_RAISE(auto indices, Evaluate(*take_expr.operand_, batch));

      if (indices.kind() == Datum::SCALAR) {
        std::shared_ptr<Array> indices_array;
        RETURN_NOT_OK(MakeArrayFromScalar(default_memory_pool(), *indices.scalar(),
                                          batch.num_rows(), &indices_array));
        indices = compute::Datum(indices_array->data());
      }

      DCHECK_EQ(indices.kind(), Datum::ARRAY);
      compute::Datum out;
      compute::FunctionContext ctx{default_memory_pool()};
      RETURN_NOT_OK(compute::Take(&ctx, compute::Datum(take_expr.dictionary_->data()),
                                  indices, compute::TakeOptions(), &out));
      return std::move(out);
    }
  };

 private:
  std::shared_ptr<Expression> operand_;
  std::shared_ptr<Array> dictionary_;
};

TEST_F(ExpressionsTest, TakeAssumeYieldsNothing) {
  auto dict = ArrayFromJSON(float64(), "[0.0, 0.25, 0.5, 0.75, 1.0]");
  auto take_b_is_half = (TakeExpression(field_ref("b"), dict) == 0.5);

  // no special Assume logic was provided for TakeExpression so we should just ignore it
  // (logically the below *could* be simplified to false but we haven't implemented that)
  AssertSimplifiesTo(take_b_is_half, "b"_ == 3, take_b_is_half);

  // custom expressions will not interfere with simplification of other subexpressions and
  // can be dropped if other subexpressions simplify trivially

  // ("b"_ > 5).Assume("b"_ == 3) simplifies to false regardless of take, so the and will
  // be false
  AssertSimplifiesTo("b"_ > 5 and take_b_is_half, "b"_ == 3, *never);

  // ("b"_ > 5).Assume("b"_ == 6) simplifies to true regardless of take, so it can be
  // dropped
  AssertSimplifiesTo("b"_ > 5 and take_b_is_half, "b"_ == 6, take_b_is_half);

  // ("b"_ > 5).Assume("b"_ == 6) simplifies to true regardless of take, so the or will be
  // true
  AssertSimplifiesTo(take_b_is_half or "b"_ > 5, "b"_ == 6, *always);

  // ("b"_ > 5).Assume("b"_ == 3) simplifies to true regardless of take, so it can be
  // dropped
  AssertSimplifiesTo(take_b_is_half or "b"_ > 5, "b"_ == 3, take_b_is_half);
}

TEST_F(FilterTest, EvaluateTakeExpression) {
  evaluator_ = std::make_shared<TakeExpression::Evaluator>(default_memory_pool());

  auto dict = ArrayFromJSON(float64(), "[0.0, 0.25, 0.5, 0.75, 1.0]");

  AssertFilter(TakeExpression(field_ref("b"), dict) == 0.5,
               {field("b", int32()), field("f", float64())}, R"([
      {"b": 3, "f": -0.1, "in": 0},
      {"b": 2, "f":  0.3, "in": 1},
      {"b": 1, "f":  0.2, "in": 0},
      {"b": 2, "f": -0.1, "in": 1},
      {"b": 4, "f":  0.1, "in": 0},
      {"b": null, "f": 0.0, "in": null},
      {"b": 0, "f":  1.0, "in": 0}
  ])");
}

void AssertFieldsInExpression(std::shared_ptr<Expression> expr,
                              std::vector<std::string> expected) {
  EXPECT_THAT(FieldsInExpression(expr), testing::ContainerEq(expected));
}

TEST(FieldsInExpressionTest, Basic) {
  AssertFieldsInExpression(scalar(true), {});

  AssertFieldsInExpression(("a"_).Copy(), {"a"});
  AssertFieldsInExpression(("a"_ == 1).Copy(), {"a"});
  AssertFieldsInExpression(("a"_ == "b"_).Copy(), {"a", "b"});

  AssertFieldsInExpression(("a"_ == 1 || "a"_ == 2).Copy(), {"a", "a"});
  AssertFieldsInExpression(("a"_ == 1 || "b"_ == 2).Copy(), {"a", "b"});
  AssertFieldsInExpression((not("a"_ == 1) && ("b"_ == 2 || not("c"_ < 3))).Copy(),
                           {"a", "b", "c"});
}

}  // namespace dataset
}  // namespace arrow
