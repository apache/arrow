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

void CheckIfElseOutput(const Datum& cond, const Datum& left, const Datum& right,
                       const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum datum_out, IfElse(cond, left, right));
  if (datum_out.is_array()) {
    std::shared_ptr<Array> result = datum_out.make_array();
    ASSERT_OK(result->ValidateFull());
    std::shared_ptr<Array> expected_ = expected.make_array();
    AssertArraysEqual(*expected_, *result, /*verbose=*/true);
  } else {  // expecting scalar
    const std::shared_ptr<Scalar>& result = datum_out.scalar();
    const std::shared_ptr<Scalar>& expected_ = expected.scalar();
    AssertScalarsEqual(*expected_, *result, /*verbose=*/true);
  }
}

void CheckIfElseOutputAAA(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::string& left, const std::string& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond_, left_, right_, expected_);
}

void CheckIfElseOutputAAS(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::string& left, const std::shared_ptr<Scalar>& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond_, left_, right, expected_);
}

void CheckIfElseOutputASA(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::shared_ptr<Scalar>& left, const std::string& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond_, left, right_, expected_);
}

void CheckIfElseOutputASS(const std::shared_ptr<DataType>& type, const std::string& cond,
                          const std::shared_ptr<Scalar>& left,
                          const std::shared_ptr<Scalar>& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond_, left, right, expected_);
}

void CheckIfElseOutputSAA(const std::shared_ptr<DataType>& type,
                          const std::shared_ptr<Scalar>& cond, const std::string& left,
                          const std::string& right, const std::string& expected) {
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond, left_, right_, expected_);
}

void CheckIfElseOutputSAS(const std::shared_ptr<DataType>& type,
                          const std::shared_ptr<Scalar>& cond, const std::string& left,
                          const std::shared_ptr<Scalar>& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond, left_, right, expected_);
}

void CheckIfElseOutputSSA(const std::shared_ptr<DataType>& type,
                          const std::shared_ptr<Scalar>& cond,
                          const std::shared_ptr<Scalar>& left, const std::string& right,
                          const std::string& expected) {
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutput(cond, left, right_, expected_);
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

  CheckIfElseOutput(cond, left, right, expected_data);
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

  // -------- Cond - Scalar, Left- Array, Right - Array ---------
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> bool_true, MakeScalar(boolean(), true));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> bool_false, MakeScalar(boolean(), false));
  std::shared_ptr<Scalar> bool_null = MakeNullScalar(boolean());

  // empty
  CheckIfElseOutputSAA(type, bool_true, "[]", "[]", "[]");
  // CLR = 111
  CheckIfElseOutputSAA(type, bool_true, "[1, 2, 3, 4]", "[5, 6, 7, 8]", "[1, 2, 3, 4]");
  // CLR = 011
  CheckIfElseOutputSAA(type, bool_null, "[1, 2, 3, 4]", "[5, 6, 7, 8]",
                       "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSAA(type, bool_false, "[1, null, 3, 4]", "[5, 6, 7, 8]",
                       "[5, 6, 7, 8]");
  // CLR = 001
  CheckIfElseOutputSAA(type, bool_null, "[1, null, 3, 4]", "[5, 6, 7, 8]",
                       "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSAA(type, bool_false, "[1, 2, 3, 4]", "[5, 6, 7, null]",
                       "[5, 6, 7, null]");
  // CLR = 010
  CheckIfElseOutputSAA(type, bool_null, "[1, 2, 3, 4]", "[5, 6, 7, null]",
                       "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSAA(type, bool_true, "[1, 2, null, null]", "[null, 6, 7, null]",
                       "[1, 2, null, null]");
  // CLR = 000
  CheckIfElseOutputSAA(type, bool_null, "[1, 2, null, null]", "[null, 6, 7, null]",
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Array, Right - Scalar ---------
  // empty
  CheckIfElseOutputSAS(type, bool_true, "[]", valid_scalar, "[]");

  // CLR = 111
  CheckIfElseOutputSAS(type, bool_true, "[1, 2, 3, 4]", valid_scalar, "[1, 2, 3, 4]");
  // CLR = 011
  CheckIfElseOutputSAS(type, bool_null, "[1, 2, 3, 4]", valid_scalar,
                       "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSAS(type, bool_false, "[1, null, 3, 4]", valid_scalar,
                       "[100, 100, 100, 100]");
  // CLR = 001
  CheckIfElseOutputSAS(type, bool_null, "[1, null, 3, 4]", valid_scalar,
                       "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSAS(type, bool_true, "[1, 2, 3, 4]", null_scalar, "[1, 2, 3, 4]");
  // CLR = 010
  CheckIfElseOutputSAS(type, bool_null, "[1, 2, 3, 4]", null_scalar,
                       "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSAS(type, bool_false, "[1, 2, null, null]", null_scalar,
                       "[null, null, null, null]");
  // CLR = 000
  CheckIfElseOutputSAS(type, bool_null, "[1, 2, null, null]", null_scalar,
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Scalar, Right - Array ---------
  // empty
  CheckIfElseOutputSSA(type, bool_true, valid_scalar, "[]", "[]");

  // CLR = 111
  CheckIfElseOutputSSA(type, bool_true, valid_scalar, "[1, 2, 3, 4]",
                       "[100, 100, 100, 100]");
  // CLR = 011
  CheckIfElseOutputSSA(type, bool_null, valid_scalar, "[1, 2, 3, 4]",
                       "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSSA(type, bool_false, valid_scalar, "[1, null, 3, null]",
                       "[1, null, 3, null]");
  // CLR = 010
  CheckIfElseOutputSSA(type, bool_null, valid_scalar, "[1, null, 3, null]",
                       "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSSA(type, bool_true, null_scalar, "[1, 2, 3, 4]",
                       "[null, null, null, null]");
  // CLR = 001
  CheckIfElseOutputSSA(type, bool_null, null_scalar, "[1, 2, 3, 4]",
                       "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSSA(type, bool_false, null_scalar, "[1, 2, null, 4]",
                       "[1, 2, null, 4]");
  // CLR = 000
  CheckIfElseOutputSSA(type, bool_null, null_scalar, "[1, 2, null, 4]",
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Scalar, Right - Scalar ---------

  // CLR = 111
  CheckIfElseOutput(bool_false, valid_scalar, valid_scalar1, valid_scalar1);
  // CLR = 011
  CheckIfElseOutput(bool_null, valid_scalar, valid_scalar1, null_scalar);
  // CLR = 110
  CheckIfElseOutput(bool_true, valid_scalar, null_scalar, valid_scalar);
  // CLR = 010
  CheckIfElseOutput(bool_null, valid_scalar, null_scalar, null_scalar);
  // CLR = 101
  CheckIfElseOutput(bool_false, null_scalar, valid_scalar1, valid_scalar1);
  // CLR = 001
  CheckIfElseOutput(bool_null, null_scalar, valid_scalar1, null_scalar);
  // CLR = 100
  CheckIfElseOutput(bool_true, null_scalar, null_scalar, null_scalar);
  // CLR = 000
  CheckIfElseOutput(bool_null, null_scalar, null_scalar, null_scalar);
}

TEST_F(TestIfElseKernel, IfElseBoolean) {
  auto type = boolean();

  // -------- All arrays ---------
  // empty
  CheckIfElseOutputAAA(type, "[]", "[]", "[]", "[]");
  // CLR = 111
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[false, false, false, false]",
                       "[true, true, true, true]", "[false, false, false, true]");
  // CLR = 011
  CheckIfElseOutputAAA(type, "[true, true, null, false]", "[false, false, false, false]",
                       "[true, true, true, true]", "[false, false, null, true]");
  // CLR = 101
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[false, null, false, false]",
                       "[true, true, true, true]", "[false, null, false, true]");
  // CLR = 001
  CheckIfElseOutputAAA(type, "[true, true, null, false]", "[false, null, false, false]",
                       "[true, true, true, true]", "[false, null, null, true]");
  // CLR = 110
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[false, false, false, false]",
                       "[true, true, true, null]", "[false, false, false, null]");
  // CLR = 010
  CheckIfElseOutputAAA(type, "[null, true, true, false]", "[false, false, false, false]",
                       "[true, true, true, null]", "[null, false, false, null]");
  // CLR = 100
  CheckIfElseOutputAAA(type, "[true, true, true, false]", "[false, false, null, null]",
                       "[null, true, true, null]", "[false, false, null, null]");
  // CLR = 000
  CheckIfElseOutputAAA(type, "[null, true, true, false]", "[false, false, null, null]",
                       "[null, true, true, null]", "[null, false, null, null]");

  // -------- Cond - Array, Left- Array, Right - Scalar ---------

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> valid_scalar, MakeScalar(type, false));
  std::shared_ptr<Scalar> null_scalar = MakeNullScalar(type);

  // empty
  CheckIfElseOutputAAS(type, "[]", "[]", valid_scalar, "[]");

  // CLR = 111
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[false, false, false, false]",
                       valid_scalar, "[false, false, false, false]");
  // CLR = 011
  CheckIfElseOutputAAS(type, "[true, true, null, false]", "[false, false, false, false]",
                       valid_scalar, "[false, false, null, false]");
  // CLR = 101
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[false, null, false, false]",
                       valid_scalar, "[false, null, false, false]");
  // CLR = 001
  CheckIfElseOutputAAS(type, "[true, true, null, false]", "[false, null, false, false]",
                       valid_scalar, "[false, null, null, false]");
  // CLR = 110
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[false, false, false, false]",
                       null_scalar, "[false, false, false, null]");
  // CLR = 010
  CheckIfElseOutputAAS(type, "[null, true, true, false]", "[false, false, false, false]",
                       null_scalar, "[null, false, false, null]");
  // CLR = 100
  CheckIfElseOutputAAS(type, "[true, true, true, false]", "[false, false, null, null]",
                       null_scalar, "[false, false, null, null]");
  // CLR = 000
  CheckIfElseOutputAAS(type, "[null, true, true, false]", "[false, false, null, null]",
                       null_scalar, "[null, false, null, null]");

  // -------- Cond - Array, Left- Scalar, Right - Array ---------
  // empty
  CheckIfElseOutputASA(type, "[]", valid_scalar, "[]", "[]");

  // CLR = 111
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar,
                       "[false, false, false, false]", "[false, false, false, false]");
  // CLR = 011
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar,
                       "[false, false, false, false]", "[false, false, null, false]");
  // CLR = 110
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar,
                       "[false, null, false, null]", "[false, false, false, null]");
  // CLR = 010
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar,
                       "[false, null, false, null]", "[false, false, null, null]");
  // CLR = 101
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar,
                       "[false, false, false, false]", "[null, null, null, false]");
  // CLR = 001
  CheckIfElseOutputASA(type, "[null, true, true, false]", null_scalar,
                       "[false, false, false, false]", "[null, null, null, false]");
  // CLR = 100
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar,
                       "[false, false, null, false]", "[null, null, null, false]");
  // CLR = 000
  CheckIfElseOutputASA(type, "[true, true, null, false]", null_scalar,
                       "[false, false, null, false]", "[null, null, null, false]");

  // -------- Cond - Array, Left- Scalar, Right - Scalar ---------
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> valid_scalar1, MakeScalar(type, true));

  // empty
  CheckIfElseOutputASS(type, "[]", valid_scalar, valid_scalar1, "[]");

  // CLR = 111
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, valid_scalar1,
                       "[false, false, false, true]");
  // CLR = 011
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, valid_scalar1,
                       "[false, false, null, true]");
  // CLR = 010
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, null_scalar,
                       "[false, false, null, null]");
  // CLR = 110
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, null_scalar,
                       "[false, false, false, null]");
  // CLR = 101
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, true]");
  // CLR = 001
  CheckIfElseOutputASS(type, "[null, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, true]");
  // CLR = 100
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");
  // CLR = 000
  CheckIfElseOutputASS(type, "[true, true, null, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Array, Right - Array ---------
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> bool_true, MakeScalar(type, true));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Scalar> bool_false, MakeScalar(type, false));
  std::shared_ptr<Scalar> bool_null = MakeNullScalar(type);

  // empty
  CheckIfElseOutputSAA(type, bool_true, "[]", "[]", "[]");
  // CLR = 111
  CheckIfElseOutputSAA(type, bool_true, "[false, false, false, false]",
                       "[true, true, true, true]", "[false, false, false, false]");
  // CLR = 011
  CheckIfElseOutputSAA(type, bool_null, "[false, false, false, false]",
                       "[true, true, true, true]", "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSAA(type, bool_false, "[false, null, false, false]",
                       "[true, true, true, true]", "[true, true, true, true]");
  // CLR = 001
  CheckIfElseOutputSAA(type, bool_null, "[false, null, false, false]",
                       "[true, true, true, true]", "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSAA(type, bool_false, "[false, false, false, false]",
                       "[true, true, true, null]", "[true, true, true, null]");
  // CLR = 010
  CheckIfElseOutputSAA(type, bool_null, "[false, false, false, false]",
                       "[true, true, true, null]", "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSAA(type, bool_true, "[false, false, null, null]",
                       "[null, true, true, null]", "[false, false, null, null]");
  // CLR = 000
  CheckIfElseOutputSAA(type, bool_null, "[false, false, null, null]",
                       "[null, true, true, null]", "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Array, Right - Scalar ---------
  // empty
  CheckIfElseOutputSAS(type, bool_true, "[]", valid_scalar, "[]");

  // CLR = 111
  CheckIfElseOutputSAS(type, bool_true, "[false, false, false, false]", valid_scalar,
                       "[false, false, false, false]");
  // CLR = 011
  CheckIfElseOutputSAS(type, bool_null, "[false, false, false, false]", valid_scalar,
                       "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSAS(type, bool_false, "[false, null, false, false]", valid_scalar,
                       "[false, false, false, false]");
  // CLR = 001
  CheckIfElseOutputSAS(type, bool_null, "[false, null, false, false]", valid_scalar,
                       "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSAS(type, bool_true, "[false, false, false, false]", null_scalar,
                       "[false, false, false, false]");
  // CLR = 010
  CheckIfElseOutputSAS(type, bool_null, "[false, false, false, false]", null_scalar,
                       "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSAS(type, bool_false, "[false, false, null, null]", null_scalar,
                       "[null, null, null, null]");
  // CLR = 000
  CheckIfElseOutputSAS(type, bool_null, "[false, false, null, null]", null_scalar,
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Scalar, Right - Array ---------
  // empty
  CheckIfElseOutputSSA(type, bool_true, valid_scalar, "[]", "[]");

  // CLR = 111
  CheckIfElseOutputSSA(type, bool_true, valid_scalar, "[false, false, false, false]",
                       "[false, false, false, false]");
  // CLR = 011
  CheckIfElseOutputSSA(type, bool_null, valid_scalar, "[false, false, false, false]",
                       "[null, null, null, null]");
  // CLR = 110
  CheckIfElseOutputSSA(type, bool_false, valid_scalar, "[false, null, false, null]",
                       "[false, null, false, null]");
  // CLR = 010
  CheckIfElseOutputSSA(type, bool_null, valid_scalar, "[false, null, false, null]",
                       "[null, null, null, null]");
  // CLR = 101
  CheckIfElseOutputSSA(type, bool_true, null_scalar, "[false, false, false, false]",
                       "[null, null, null, null]");
  // CLR = 001
  CheckIfElseOutputSSA(type, bool_null, null_scalar, "[false, false, false, false]",
                       "[null, null, null, null]");
  // CLR = 100
  CheckIfElseOutputSSA(type, bool_false, null_scalar, "[false, false, null, false]",
                       "[false, false, null, false]");
  // CLR = 000
  CheckIfElseOutputSSA(type, bool_null, null_scalar, "[false, false, null, false]",
                       "[null, null, null, null]");

  // -------- Cond - Scalar, Left- Scalar, Right - Scalar ---------

  // CLR = 111
  CheckIfElseOutput(bool_false, valid_scalar, valid_scalar1, valid_scalar1);
  // CLR = 011
  CheckIfElseOutput(bool_null, valid_scalar, valid_scalar1, null_scalar);
  // CLR = 110
  CheckIfElseOutput(bool_true, valid_scalar, null_scalar, valid_scalar);
  // CLR = 010
  CheckIfElseOutput(bool_null, valid_scalar, null_scalar, null_scalar);
  // CLR = 101
  CheckIfElseOutput(bool_false, null_scalar, valid_scalar1, valid_scalar1);
  // CLR = 001
  CheckIfElseOutput(bool_null, null_scalar, valid_scalar1, null_scalar);
  // CLR = 100
  CheckIfElseOutput(bool_true, null_scalar, null_scalar, null_scalar);
  // CLR = 000
  CheckIfElseOutput(bool_null, null_scalar, null_scalar, null_scalar);
}

TYPED_TEST(TestIfElsePrimitive, IfElseBooleanRand) {
  auto type = boolean();
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

  CheckIfElseOutput(cond, left, right, expected_data);
}

TEST_F(TestIfElseKernel, IfElseNull) {
  CheckIfElseOutputAAA(null(), "[null, null, null, null]", "[null, null, null, null]",
                       "[null, null, null, null]", "[null, null, null, null]");
}

}  // namespace compute
}  // namespace arrow
