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

#include <arrow/array.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/kernels/test_util.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

namespace arrow {
namespace compute {

void CheckIfElseOutputArray(const Datum& cond, const Datum& left, const Datum& right,
                            const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum datum_out, IfElse(cond, left, right));
  std::shared_ptr<Array> result = datum_out.make_array();
  ASSERT_OK(result->ValidateFull());
  std::shared_ptr<Array> expected_ = expected.make_array();
  AssertArraysEqual(*expected.make_array(), *result, /*verbose=*/true);

  ASSERT_EQ(result->data()->null_count, expected_->data()->null_count);
}

void CheckIfElseOutputAAA(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::string& left, const std::string& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutputArray(cond_, left_, right_, expected_);
}

void CheckIfElseOutputAAS(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::string& left, const std::shared_ptr<Scalar>& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutputArray(cond_, left_, right, expected_);
}

void CheckIfElseOutputASA(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::shared_ptr<Scalar>& left, const std::string& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutputArray(cond_, left, right_, expected_);
}

void CheckIfElseOutputASS(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::shared_ptr<Scalar>& left,
                          const std::shared_ptr<Scalar>& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutputArray(cond_, left, right, expected_);
}

class TestIfElseKernel : public ::testing::Test {};

template <typename Type>
class TestIfElsePrimitive : public ::testing::Test {};

using PrimitiveTypes = ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type,
                                        Int32Type, UInt32Type, Int64Type, UInt64Type,
                                        FloatType, DoubleType, Date32Type, Date64Type>;

TYPED_TEST_SUITE(TestIfElsePrimitive, PrimitiveTypes);

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSizeRand) {
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  auto type = TypeTraits<TypeParam>::type_singleton();

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;
  auto cond = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
  auto left = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  typename TypeTraits<TypeParam>::BuilderType builder;

  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    if (cond->Value(i)) {
      ASSERT_OK(builder.Append(left->Value(i)));
    } else {
      ASSERT_OK(builder.Append(right->Value(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutputArray(cond, left, right, expected_data);
}

/*
 * Legend:
 * C - Cond, L - Left, R - Right
 * 1 - All valid (or valid scalar), 0 - Could have nulls (or invalid scalar)
 */
TYPED_TEST(TestIfElsePrimitive, IfElseFixedSize) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  // -------- All arrays ---------
  // empty
  CheckIfElseOutputAAA(type, "[]", "[]", "[]", "[]");
  // CLR = 111
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[1, 2, 3, 4]", "[5, 6, 7, 8]",
                       "[1, 2, 3, 8]");
  // CLR = 011
  CheckIfElseOutputAAA(type, "[true, true, null, false]", "[1, 2, 3, 4]", "[5, 6, 7, 8]",
                       "[1, 2, null, 8]");
  // CLR = 101
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[1, null, 3, 4]",
                       "[5, 6, 7, 8]", "[1, null, 3, 8]");
  // CLR = 001
  CheckIfElseOutputAAA(type, "[true, true, null, false]", "[1, null, 3, 4]",
                       "[5, 6, 7, 8]", "[1, null, null, 8]");
  // CLR = 110
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[1, 2, 3, 4]",
                       "[5, 6, 7, null]", "[1, 2, 3, null]");
  // CLR = 010
  CheckIfElseOutputAAA(type, "[null, true, true, false]", "[1, 2, 3, 4]",
                       "[5, 6, 7, null]", "[null, 2, 3, null]");
  // CLR = 100
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[1, 2, null, null]",
                       "[null, 6, 7, null]", "[1, 2, null, null]");
  // CLR = 000
  CheckIfElseOutputAAA(type, "[null, true, true, false]", "[1, 2, null, null]",
                       "[null, 6, 7, null]", "[null, 2, null, null]");

  // -------- Cond - Array, Left- Array, Right - Scalar ---------

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> valid_scalar, MakeScalar(type, 100));
  std::shared_ptr<Scalar> null_scalar = MakeNullScalar(type);

  // empty
  CheckIfElseOutputAAS(type, "[]", "[]", valid_scalar, "[]");

  // CLR = 111
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[1, 2, 3, 4]", valid_scalar,
                       "[1, 2, 3, 100]");
  // CLR = 011
  CheckIfElseOutputAAS(type, "[true, true, null, false]", "[1, 2, 3, 4]", valid_scalar,
                       "[1, 2, null, 100]");
  // CLR = 101
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[1, null, 3, 4]", valid_scalar,
                       "[1, null, 3, 100]");
  // CLR = 001
  CheckIfElseOutputAAS(type, "[true, true, null, false]", "[1, null, 3, 4]", valid_scalar,
                       "[1, null, null, 100]");
  // CLR = 110
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[1, 2, 3, 4]", null_scalar,
                       "[1, 2, 3, null]");
  // CLR = 010
  CheckIfElseOutputAAS(type, "[null, true, true, false]", "[1, 2, 3, 4]", null_scalar,
                       "[null, 2, 3, null]");
  // CLR = 100
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[1, 2, null, null]",
                       null_scalar, "[1, 2, null, null]");
  // CLR = 000
  CheckIfElseOutputAAS(type, "[null, true, true, false]", "[1, 2, null, null]",
                       null_scalar, "[null, 2, null, null]");

  // -------- Cond - Array, Left- Scalar, Right - Array ---------
  // empty
  CheckIfElseOutputASA(type, "[]", valid_scalar, "[]", "[]");

  // CLR = 111
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar, "[1, 2, 3, 4]",
                       "[100, 100, 100, 4]");
  // CLR = 011
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar, "[1, 2, 3, 4]",
                       "[100, 100, null, 4]");
  // CLR = 110
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar,
                       "[1, null, 3, null]", "[100, 100, 100, null]");
  // CLR = 010
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar,
                       "[1, null, 3, null]", "[100, 100, null, null]");
  // CLR = 101
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar, "[1, 2, 3, 4]",
                       "[null, null, null, 4]");
  // CLR = 001
  CheckIfElseOutputASA(type, "[null, true, true, false]", null_scalar, "[1, 2, 3, 4]",
                       "[null, null, null, 4]");
  // CLR = 100
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar, "[1, 2, null, 4]",
                       "[null, null, null, 4]");
  // CLR = 000
  CheckIfElseOutputASA(type, "[true, true, null, false]", null_scalar, "[1, 2, null, 4]",
                       "[null, null, null, 4]");

  // -------- Cond - Array, Left- Scalar, Right - Scalar ---------
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> valid_scalar1, MakeScalar(type, 111));

  // empty
  CheckIfElseOutputASS(type, "[]", valid_scalar, valid_scalar1, "[]");

  // CLR = 111
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, valid_scalar1,
                       "[100, 100, 100, 111]");
  // CLR = 011
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, valid_scalar1,
                       "[100, 100, null, 111]");
  // CLR = 010
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, null_scalar,
                       "[100, 100, null, null]");
  // CLR = 110
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, null_scalar,
                       "[100, 100, 100, null]");
  // CLR = 101
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, 111]");
  // CLR = 001
  CheckIfElseOutputASS(type, "[null, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, 111]");
  // CLR = 100
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");
  // CLR = 000
  CheckIfElseOutputASS(type, "[true, true, null, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");
}

TEST_F(TestIfElseKernel, IfElseBoolean) {
  auto type = boolean();
  // No Nulls
  CheckIfElseOutputAAA(type, "[]", "[]", "[]", "[]");

  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[false, false, false, false]",
                       "[true, true, true, true]", "[false, false, false, true]");

  CheckIfElseOutputAAA(type, "[true, true, null, false]", "[false, false, false, false]",
                       "[true, true, true, true]", "[false, false, null, true]");

  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[true, false, null, null]",
                       "[null, false, true, null]", "[true, false, null, null]");

  random::RandomArrayGenerator rand(/*seed=*/0);
  int64_t len = 1000;
  auto cond = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
  auto left = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));
  auto right = std::static_pointer_cast<BooleanArray>(
      rand.ArrayOf(type, len, /*null_probability=*/0.01));

  BooleanBuilder builder;
  for (int64_t i = 0; i < len; ++i) {
    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
        (!cond->Value(i) && !right->IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    if (cond->Value(i)) {
      ASSERT_OK(builder.Append(left->Value(i)));
    } else {
      ASSERT_OK(builder.Append(right->Value(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  CheckIfElseOutputArray(cond, left, right, expected_data);
}

TEST_F(TestIfElseKernel, IfElseNull) {
  CheckIfElseOutputAAA(null(), "[null, null, null, null]", "[null, null, null, null]",
                       "[null, null, null, null]", "[null, null, null, null]");
}

}  // namespace compute
}  // namespace arrow
