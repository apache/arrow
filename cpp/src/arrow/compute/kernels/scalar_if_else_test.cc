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

template <typename Type>
struct DatumWrapper {
  using CType = typename TypeTraits<Type>::CType;
  using ArrayType = typename TypeTraits<Type>::ArrayType;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  util::Variant<std::shared_ptr<ScalarType>, std::shared_ptr<ArrayType>> datum;
  bool is_scalar;

  explicit DatumWrapper(const Datum& datum_) : is_scalar(datum_.is_scalar()) {
    if (is_scalar) {
      datum = std::move(std::static_pointer_cast<ScalarType>(datum_.scalar()));
    } else {
      datum = std::move(std::static_pointer_cast<ArrayType>(datum_.make_array()));
    }
  }

  bool IsValid(int64_t i) const {
    return is_scalar ? util::get<std::shared_ptr<ScalarType>>(datum)->is_valid
                     : util::get<std::shared_ptr<ArrayType>>(datum)->IsValid(i);
  }

  CType Value(int64_t i) const {
    return is_scalar ? util::get<std::shared_ptr<ScalarType>>(datum)->value
                     : util::get<std::shared_ptr<ArrayType>>(datum)->Value(i);
  }
};

template <typename Type>
void GenerateExpected(const Datum& cond, const Datum& left, const Datum& right,
                      Datum* out) {
  int64_t len = cond.is_array() ? cond.length()
                                : left.is_array() ? left.length()
                                                  : right.is_array() ? right.length() : 1;

  DatumWrapper<BooleanType> cond_(cond);
  DatumWrapper<Type> left_(left);
  DatumWrapper<Type> right_(right);

  int64_t i = 0;

  // if all scalars
  if (cond.is_scalar() && left.is_scalar() && right.is_scalar()) {
    if (!cond_.IsValid(i) || (cond_.Value(i) && !left_.IsValid(i)) ||
        (!cond_.Value(i) && !right_.IsValid(i))) {
      *out = MakeNullScalar(left.type());
      return;
    }

    if (cond_.Value(i)) {
      *out = left;
      return;
    } else {
      *out = right;
      return;
    }
  }

  typename TypeTraits<Type>::BuilderType builder;

  for (; i < len; ++i) {
    if (!cond_.IsValid(i) || (cond_.Value(i) && !left_.IsValid(i)) ||
        (!cond_.Value(i) && !right_.IsValid(i))) {
      ASSERT_OK(builder.AppendNull());
      continue;
    }

    if (cond_.Value(i)) {
      ASSERT_OK(builder.Append(left_.Value(i)));
    } else {
      ASSERT_OK(builder.Append(right_.Value(i)));
    }
  }
  ASSERT_OK_AND_ASSIGN(auto expected_data, builder.Finish());

  *out = expected_data;
}

TYPED_TEST(TestIfElsePrimitive, IfElseFixedSizeGen) {
  auto type = TypeTraits<TypeParam>::type_singleton();

  std::vector<Datum> cond_datums{ArrayFromJSON(boolean(), "[true, true, true, false]"),
                                 ArrayFromJSON(boolean(), "[true, null, true, false]"),
                                 MakeScalar(boolean(), true).ValueOrDie(),
                                 MakeNullScalar(boolean())};

  std::vector<Datum> left_datums{
      ArrayFromJSON(type, "[1, 2, 3, 4]"), ArrayFromJSON(type, "[1, 2, null, 4]"),
      MakeScalar(type, 100).ValueOrDie(), MakeNullScalar(type)};

  std::vector<Datum> right_datums{
      ArrayFromJSON(type, "[5, 6, 7, 8]"), ArrayFromJSON(type, "[5, 6, 7, null]"),
      MakeScalar(type, 111).ValueOrDie(), MakeNullScalar(type)};

  for (auto&& cond : cond_datums) {
    for (auto&& left : left_datums) {
      for (auto&& right : right_datums) {
        Datum exp;
        GenerateExpected<TypeParam>(cond, left, right, &exp);
        CheckIfElseOutput(cond, left, right, exp);
      }
    }
  }
}

TEST_F(TestIfElseKernel, IfElseBooleanGen) {
  auto type = boolean();

  std::vector<Datum> cond_datums{ArrayFromJSON(boolean(), "[true, true, true, false]"),
                                 ArrayFromJSON(boolean(), "[true, true, null, false]"),
                                 MakeScalar(boolean(), true).ValueOrDie(),
                                 MakeNullScalar(boolean())};

  std::vector<Datum> left_datums{ArrayFromJSON(type, "[false, false, false, false]"),
                                 ArrayFromJSON(type, "[false, false, null, false]"),
                                 MakeScalar(type, false).ValueOrDie(),
                                 MakeNullScalar(type)};

  std::vector<Datum> right_datums{ArrayFromJSON(type, "[true, true, true, true]"),
                                  ArrayFromJSON(type, "[true, true, true, null]"),
                                  MakeScalar(type, true).ValueOrDie(),
                                  MakeNullScalar(type)};

  for (auto&& cond : cond_datums) {
    for (auto&& left : left_datums) {
      for (auto&& right : right_datums) {
        Datum exp;
        GenerateExpected<BooleanType>(cond, left, right, &exp);
        CheckIfElseOutput(cond, left, right, exp);
      }
    }
  }
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
  CheckIfElseOutput(ArrayFromJSON(boolean(), "[null, null, null, null]"),
                    ArrayFromJSON(null(), "[null, null, null, null]"),
                    ArrayFromJSON(null(), "[null, null, null, null]"),
                    ArrayFromJSON(null(), "[null, null, null, null]"));
}

TEST_F(TestIfElseKernel, IfElseWithOffset) {
  auto cond = ArrayFromJSON(boolean(), "[null, true, false]")->Slice(1, 2);
  auto left = ArrayFromJSON(int64(), "[10, 11]");
  auto right = ArrayFromJSON(int64(), "[1, 2]");
  auto expected = ArrayFromJSON(int64(), "[10, 2]");
  CheckIfElseOutput(cond, left, right, expected);
}

}  // namespace compute
}  // namespace arrow
