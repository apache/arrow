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
                            const Datum& expected, bool all_valid = true) {
  ASSERT_OK_AND_ASSIGN(Datum datum_out, IfElse(cond, left, right));
  std::shared_ptr<Array> result = datum_out.make_array();
  ASSERT_OK(result->ValidateFull());
  AssertArraysEqual(*expected.make_array(), *result, /*verbose=*/true);
  if (all_valid) {
    // Check null count of ArrayData is set, not the computed Array.null_count
    ASSERT_EQ(result->data()->null_count, 0);
  }
}

void CheckIfElseOutputArray(const std::shared_ptr<DataType>& type,
                            const std::string& cond, const std::string& left,
                            const std::string& right, const std::string& expected,
                            bool all_valid = true) {
  const std::shared_ptr<Array>& cond_ = ArrayFromJSON(boolean(), cond);
  const std::shared_ptr<Array>& left_ = ArrayFromJSON(type, left);
  const std::shared_ptr<Array>& right_ = ArrayFromJSON(type, right);
  const std::shared_ptr<Array>& expected_ = ArrayFromJSON(type, expected);
  CheckIfElseOutputArray(cond_, left_, right_, expected_, all_valid);
}

class TestIfElseKernel : public ::testing::Test {};

template <typename Type>
class TestIfElsePrimitive : public ::testing::Test {};

using PrimitiveTypes = ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type,
                                        Int32Type, UInt32Type, Int64Type, UInt64Type,
                                        FloatType, DoubleType, Date32Type, Date64Type>;


TYPED_TEST_SUITE(TestIfElsePrimitive, PrimitiveTypes);

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSize) {
  //  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();
  //  auto scalar = std::make_shared<ScalarType>(static_cast<T>(5));
  // No Nulls
  CheckIfElseOutputArray(type, "[]", "[]", "[]", "[]");

  CheckIfElseOutputArray(type, "[true, true, true, false]", "[1, 2, 3, 4]",
                         "[5, 6, 7, 8]", "[1, 2, 3, 8]");

  CheckIfElseOutputArray(type, "[true, true, null, false]", "[1, 2, 3, 4]",
                         "[5, 6, 7, 8]", "[1, 2, null, 8]", false);

  CheckIfElseOutputArray(type, "[true, true, true, false]", "[1, 2, null, null]",
                         "[null, 6, 7, null]", "[1, 2, null, null]", false);

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

  CheckIfElseOutputArray(cond, left, right, expected_data, false);
}

TEST_F(TestIfElseKernel, IfElseBoolean) {
  //  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  //  auto scalar = std::make_shared<ScalarType>(static_cast<T>(5));
  auto type = boolean();
  // No Nulls
  CheckIfElseOutputArray(type, "[]", "[]", "[]", "[]");

  CheckIfElseOutputArray(type, "[true, true, true, false]",
                         "[false, false, false, false]", "[true, true, true, true]",
                         "[false, false, false, true]");

  CheckIfElseOutputArray(type, "[true, true, null, false]",
                         "[false, false, false, false]", "[true, true, true, true]",
                         "[false, false, null, true]", false);

  CheckIfElseOutputArray(type, "[true, true, true, false]", "[true, false, null, null]",
                         "[null, false, true, null]", "[true, false, null, null]", false);

//  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
//  random::RandomArrayGenerator rand(/*seed=*/0);
//  int64_t len = 1000;
//  auto cond = std::static_pointer_cast<BooleanArray>(
//      rand.ArrayOf(boolean(), len, /*null_probability=*/0.01));
//  auto left = std::static_pointer_cast<ArrayType>(
//      rand.ArrayOf(type, len, /*null_probability=*/0.01));
//  auto right = std::static_pointer_cast<ArrayType>(
//      rand.ArrayOf(type, len, /*null_probability=*/0.01));
//
//  typename TypeTraits<TypeParam>::BuilderType builder;
//
//  for (int64_t i = 0; i < len; ++i) {
//    if (!cond->IsValid(i) || (cond->Value(i) && !left->IsValid(i)) ||
//        (!cond->Value(i) && !right->IsValid(i))) {
//      ASSERT_OK(builder.AppendNull());
//      continue;
//    }
//
//    if (cond->Value(i)) {
//      ASSERT_OK(builder.Append(left->Value(i)));
//    } else {
//      ASSERT_OK(builder.Append(right->Value(i)));
//    }
//  }
//  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());
//
//  CheckIfElseOutputArray(cond, left, right, expected_data, false);
}

}  // namespace compute
}  // namespace arrow