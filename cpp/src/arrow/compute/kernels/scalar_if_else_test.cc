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

template <typename T>
void DoIfElseTest(const std::shared_ptr<DataType>& type, const std::array<T, 4>& left,
                  const std::array<T, 4>& right, const std::array<T, 2>& valid_scalars) {
  std::array<std::string, 4> l, r;
  std::array<std::string, 2> v;

  auto to_string = [](const T& i) { return std::to_string(i); };
  std::transform(left.begin(), left.end(), l.begin(), to_string);
  std::transform(right.begin(), right.end(), r.begin(), to_string);
  std::transform(valid_scalars.begin(), valid_scalars.end(), v.begin(), to_string);

  /* -------- All arrays --------- */
  /* empty */
  CheckIfElseOutputAAA(type, "[]", "[]", "[]", "[]");
  /* CLR = 111 */
  CheckIfElseOutputAAA(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + r[3] + "]");
  /* CLR = 011 */
  CheckIfElseOutputAAA(type, "[true, true, null, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + l[0] + ", " + l[1] + ", null, " + r[3] + "]");
  /* CLR = 101 */
  CheckIfElseOutputAAA(type, "[true, true, true, false]",
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + l[0] + ", null, " + l[2] + ", " + r[3] + "]");
  /* CLR = 001 */
  CheckIfElseOutputAAA(type, "[true, true, null, false]",
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + l[0] + ", null, null, " + r[3] + "]");
  /* CLR = 110 */
  CheckIfElseOutputAAA(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", null]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", null]");
  /* CLR = 010 */
  CheckIfElseOutputAAA(type, "[null, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", null]",
                       "[null, " + l[1] + ", " + l[2] + ", null]");
  /* CLR = 100 */
  CheckIfElseOutputAAA(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", null, null]",
                       "[null, " + r[1] + ", " + r[2] + ", null]",
                       "[" + l[0] + ", " + l[1] + ", null, null]");
  /* CLR = 000 */
  CheckIfElseOutputAAA(
      type, "[null, true, true, false]", "[" + l[0] + ", " + l[1] + ", null, null]",
      "[null, " + r[1] + ", " + r[2] + ", null]", "[null, " + l[1] + ", null, null]");

  /* -------- Cond - Array, Left- Array, Right - Scalar --------- */
  ASSERT_OK_AND_ASSIGN(auto valid_scalar, MakeScalar(type, valid_scalars[0]));
  ASSERT_OK_AND_ASSIGN(auto valid_scalar1, MakeScalar(type, valid_scalars[1]));
  auto null_scalar = MakeNullScalar(type);

  /* empty */
  //  CheckIfElseOutputAAS(type, "[]", "[]", valid_scalar, "[]");

  /* CLR = 111 */
  CheckIfElseOutputAAS(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       valid_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + v[0] + "]");
  /* CLR = 011 */
  CheckIfElseOutputAAS(type, "[true, true, null, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       valid_scalar, "[" + l[0] + ", " + l[1] + ", null, " + v[0] + "]");
  /* CLR = 101 */
  CheckIfElseOutputAAS(type, "[true, true, true, false]",
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]", valid_scalar,
                       "[" + l[0] + ", null, " + l[2] + ", " + v[0] + "]");
  /* CLR = 001 */
  CheckIfElseOutputAAS(type, "[true, true, null, false]",
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]", valid_scalar,
                       "[" + l[0] + ", null, null, " + v[0] + "]");
  /* CLR = 110 */
  CheckIfElseOutputAAS(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       null_scalar, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", null]");
  /* CLR = 010 */
  CheckIfElseOutputAAS(type, "[null, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       null_scalar, "[null, " + l[1] + ", " + l[2] + ", null]");
  /* CLR = 100 */
  CheckIfElseOutputAAS(type, "[true, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", null, null]", null_scalar,
                       "[" + l[0] + ", " + l[1] + ", null, null]");
  /* CLR = 000 */
  CheckIfElseOutputAAS(type, "[null, true, true, false]",
                       "[" + l[0] + ", " + l[1] + ", null, null]", null_scalar,
                       "[null, " + l[1] + ", null, null]");

  /* -------- Cond - Array, Left- Scalar, Right - Array --------- */
  /* empty */
  CheckIfElseOutputASA(type, "[]", valid_scalar, "[]", "[]");

  /* CLR = 111 */
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", " + l[3] + "]");
  /* CLR = 011 */
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + v[0] + ", " + v[0] + ", null, " + l[3] + "]");
  /* CLR = 110 */
  CheckIfElseOutputASA(type, "[true, true, true, false]", valid_scalar,
                       "[" + l[0] + ", null, " + l[2] + ", null]",
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", null]");
  /* CLR = 010 */
  CheckIfElseOutputASA(type, "[true, true, null, false]", valid_scalar,
                       "[" + l[0] + ", null, " + l[2] + ", null]",
                       "[" + v[0] + ", " + v[0] + ", null, null]");
  /* CLR = 101 */
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[null, null, null, " + l[3] + "]");
  /* CLR = 001 */
  CheckIfElseOutputASA(type, "[null, true, true, false]", null_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[null, null, null, " + l[3] + "]");
  /* CLR = 100 */
  CheckIfElseOutputASA(type, "[true, true, true, false]", null_scalar,
                       "[" + l[0] + ", " + l[1] + ", null, " + l[3] + "]",
                       "[null, null, null, " + l[3] + "]");
  /* CLR = 000 */
  CheckIfElseOutputASA(type, "[true, true, null, false]", null_scalar,
                       "[" + l[0] + ", " + l[1] + ", null, " + l[3] + "]",
                       "[null, null, null, " + l[3] + "]");

  /* -------- Cond - Array, Left- Scalar, Right - Scalar --------- */
  /* empty */
  CheckIfElseOutputASS(type, "[]", valid_scalar, valid_scalar1, "[]");

  /* CLR = 111 */
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, valid_scalar1,
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", " + v[1] + "]");
  /* CLR = 011 */
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, valid_scalar1,
                       "[" + v[0] + ", " + v[0] + ", null, " + v[1] + "]");
  /* CLR = 010 */
  CheckIfElseOutputASS(type, "[true, true, null, false]", valid_scalar, null_scalar,
                       "[" + v[0] + ", " + v[0] + ", null, null]");
  /* CLR = 110 */
  CheckIfElseOutputASS(type, "[true, true, true, false]", valid_scalar, null_scalar,
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", null]");
  /* CLR = 101 */
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, " + v[1] + "]");
  /* CLR = 001 */
  CheckIfElseOutputASS(type, "[null, true, true, false]", null_scalar, valid_scalar1,
                       "[null, null, null, " + v[1] + "]");
  /* CLR = 100 */
  CheckIfElseOutputASS(type, "[true, true, true, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");
  /* CLR = 000 */
  CheckIfElseOutputASS(type, "[true, true, null, false]", null_scalar, null_scalar,
                       "[null, null, null, null]");

  /* -------- Cond - Scalar, Left- Array, Right - Array --------- */
  ASSERT_OK_AND_ASSIGN(auto bool_true, MakeScalar(boolean(), true));
  ASSERT_OK_AND_ASSIGN(auto bool_false, MakeScalar(boolean(), false));
  auto bool_null = MakeNullScalar(boolean());

  /* empty */
  CheckIfElseOutputSAA(type, bool_true, "[]", "[]", "[]");
  /* CLR = 111 */
  CheckIfElseOutputSAA(type, bool_true,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]");
  /* CLR = 011 */
  CheckIfElseOutputSAA(type, bool_null,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[null, null, null, null]");
  /* CLR = 101 */
  CheckIfElseOutputSAA(type, bool_false,
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]");
  /* CLR = 001 */
  CheckIfElseOutputSAA(type, bool_null,
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + "]",
                       "[null, null, null, null]");
  /* CLR = 110 */
  CheckIfElseOutputSAA(type, bool_false,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", null]",
                       "[" + r[0] + ", " + r[1] + ", " + r[2] + ", null]");
  /* CLR = 010 */
  CheckIfElseOutputSAA(
      type, bool_null, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
      "[" + r[0] + ", " + r[1] + ", " + r[2] + ", null]", "[null, null, null, null]");
  /* CLR = 100 */
  CheckIfElseOutputSAA(type, bool_true, "[" + l[0] + ", " + l[1] + ", null, null]",
                       "[null, " + r[1] + ", " + r[2] + ", null]",
                       "[" + l[0] + ", " + l[1] + ", null, null]");
  /* CLR = 000 */
  CheckIfElseOutputSAA(type, bool_null, "[" + l[0] + ", " + l[1] + ", null, null]",
                       "[null, " + r[1] + ", " + r[2] + ", null]",
                       "[null, null, null, null]");

  /* -------- Cond - Scalar, Left- Array, Right - Scalar --------- */
  /* empty */
  CheckIfElseOutputSAS(type, bool_true, "[]", valid_scalar, "[]");

  /* CLR = 111 */
  CheckIfElseOutputSAS(
      type, bool_true, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
      valid_scalar, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]");
  /* CLR = 011 */
  CheckIfElseOutputSAS(type, bool_null,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       valid_scalar, "[null, null, null, null]");
  /* CLR = 101 */
  CheckIfElseOutputSAS(type, bool_false,
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]", valid_scalar,
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", " + v[0] + "]");
  /* CLR = 001 */
  CheckIfElseOutputSAS(type, bool_null,
                       "[" + l[0] + ", null, " + l[2] + ", " + l[3] + "]", valid_scalar,
                       "[null, null, null, null]");
  /* CLR = 110 */
  CheckIfElseOutputSAS(
      type, bool_true, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
      null_scalar, "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]");
  /* CLR = 010 */
  CheckIfElseOutputSAS(type, bool_null,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       null_scalar, "[null, null, null, null]");
  /* CLR = 100 */
  CheckIfElseOutputSAS(type, bool_false, "[" + l[0] + ", " + l[1] + ", null, null]",
                       null_scalar, "[null, null, null, null]");
  /* CLR = 000 */
  CheckIfElseOutputSAS(type, bool_null, "[" + l[0] + ", " + l[1] + ", null, null]",
                       null_scalar, "[null, null, null, null]");

  /* -------- Cond - Scalar, Left- Scalar, Right - Array --------- */
  /* empty */
  CheckIfElseOutputSSA(type, bool_true, valid_scalar, "[]", "[]");

  /* CLR = 111 */
  CheckIfElseOutputSSA(type, bool_true, valid_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[" + v[0] + ", " + v[0] + ", " + v[0] + ", " + v[0] + "]");
  /* CLR = 011 */
  CheckIfElseOutputSSA(type, bool_null, valid_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[null, null, null, null]");
  /* CLR = 110 */
  CheckIfElseOutputSSA(type, bool_false, valid_scalar,
                       "[" + l[0] + ", null, " + l[2] + ", null]",
                       "[" + l[0] + ", null, " + l[2] + ", null]");
  /* CLR = 010 */
  CheckIfElseOutputSSA(type, bool_null, valid_scalar,
                       "[" + l[0] + ", null, " + l[2] + ", null]",
                       "[null, null, null, null]");
  /* CLR = 101 */
  CheckIfElseOutputSSA(type, bool_true, null_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[null, null, null, null]");
  /* CLR = 001 */
  CheckIfElseOutputSSA(type, bool_null, null_scalar,
                       "[" + l[0] + ", " + l[1] + ", " + l[2] + ", " + l[3] + "]",
                       "[null, null, null, null]");
  /* CLR = 100 */
  CheckIfElseOutputSSA(type, bool_false, null_scalar,
                       "[" + l[0] + ", " + l[1] + ", null, " + l[3] + "]",
                       "[" + l[0] + ", " + l[1] + ", null, " + l[3] + "]");
  /* CLR = 000 */
  CheckIfElseOutputSSA(type, bool_null, null_scalar,
                       "[" + l[0] + ", " + l[1] + ", null, " + l[3] + "]",
                       "[null, null, null, null]");

  /* -------- Cond - Scalar, Left- Scalar, Right - Scalar --------- */

  /* CLR = 111 */
  CheckIfElseOutput(bool_false, valid_scalar, valid_scalar1, valid_scalar1);
  /* CLR = 011 */
  CheckIfElseOutput(bool_null, valid_scalar, valid_scalar1, null_scalar);
  /* CLR = 110 */
  CheckIfElseOutput(bool_true, valid_scalar, null_scalar, valid_scalar);
  /* CLR = 010 */
  CheckIfElseOutput(bool_null, valid_scalar, null_scalar, null_scalar);
  /* CLR = 101 */
  CheckIfElseOutput(bool_false, null_scalar, valid_scalar1, valid_scalar1);
  /* CLR = 001 */
  CheckIfElseOutput(bool_null, null_scalar, valid_scalar1, null_scalar);
  /* CLR = 100 */
  CheckIfElseOutput(bool_true, null_scalar, null_scalar, null_scalar);
  /* CLR = 000 */
  CheckIfElseOutput(bool_null, null_scalar, null_scalar, null_scalar);
}

/*
 * Legend:
 * C - Cond, L - Left, R - Right
 * 1 - All valid (or valid scalar), 0 - Could have nulls (or invalid scalar)
 */
TYPED_TEST(TestIfElsePrimitive, IfElseFixedSize) {
  auto type = TypeTraits<TypeParam>::type_singleton();
  using T = typename TypeTraits<TypeParam>::CType;

  DoIfElseTest<T>(type, {1, 2, 3, 4}, {5, 6, 7, 8}, {100, 111});
}

TEST_F(TestIfElseKernel, IfElseBoolean) {
  auto type = boolean();

  DoIfElseTest<bool>(type, {false, false, false, false}, {true, true, true, true},
                     {false, true});
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
