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

#include <algorithm>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::BitmapReader;

namespace compute {

using util::string_view;

template <typename ArrowType>
static void ValidateCompare(CompareOptions options, const Datum& lhs, const Datum& rhs,
                            const Datum& expected) {
  ASSERT_OK_AND_ASSIGN(Datum result, Compare(lhs, rhs, options));
  AssertArraysEqual(*expected.make_array(), *result.make_array(),
                    /*verbose=*/true);
}

template <typename ArrowType>
static void ValidateCompare(CompareOptions options, const char* lhs_str, const Datum& rhs,
                            const char* expected_str) {
  auto lhs = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), lhs_str);
  auto expected = ArrayFromJSON(TypeTraits<BooleanType>::type_singleton(), expected_str);
  ValidateCompare<ArrowType>(options, lhs, rhs, expected);
}

template <typename ArrowType>
static void ValidateCompare(CompareOptions options, const Datum& lhs, const char* rhs_str,
                            const char* expected_str) {
  auto rhs = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), rhs_str);
  auto expected = ArrayFromJSON(TypeTraits<BooleanType>::type_singleton(), expected_str);
  ValidateCompare<ArrowType>(options, lhs, rhs, expected);
}

template <typename ArrowType>
static void ValidateCompare(CompareOptions options, const char* lhs_str,
                            const char* rhs_str, const char* expected_str) {
  auto lhs = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), lhs_str);
  auto rhs = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), rhs_str);
  auto expected = ArrayFromJSON(TypeTraits<BooleanType>::type_singleton(), expected_str);
  ValidateCompare<ArrowType>(options, lhs, rhs, expected);
}

template <typename T>
static inline bool SlowCompare(CompareOperator op, const T& lhs, const T& rhs) {
  switch (op) {
    case EQUAL:
      return lhs == rhs;
    case NOT_EQUAL:
      return lhs != rhs;
    case GREATER:
      return lhs > rhs;
    case GREATER_EQUAL:
      return lhs >= rhs;
    case LESS:
      return lhs < rhs;
    case LESS_EQUAL:
      return lhs <= rhs;
    default:
      return false;
  }
}

template <typename ArrowType>
Datum SimpleScalarArrayCompare(CompareOptions options, const Datum& lhs,
                               const Datum& rhs) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;

  bool swap = lhs.is_array();
  auto array = std::static_pointer_cast<ArrayType>((swap ? lhs : rhs).make_array());
  auto value = std::static_pointer_cast<ScalarType>((swap ? rhs : lhs).scalar())->value;

  std::vector<bool> bitmap(array->length());
  for (int64_t i = 0; i < array->length(); i++) {
    bitmap[i] = swap ? SlowCompare(options.op, array->Value(i), value)
                     : SlowCompare(options.op, value, array->Value(i));
  }

  std::shared_ptr<Array> result;

  if (array->null_count() == 0) {
    ArrayFromVector<BooleanType>(bitmap, &result);
  } else {
    std::vector<bool> null_bitmap(array->length());
    auto reader =
        BitmapReader(array->null_bitmap_data(), array->offset(), array->length());
    for (int64_t i = 0; i < array->length(); i++, reader.Next()) {
      null_bitmap[i] = reader.IsSet();
    }
    ArrayFromVector<BooleanType>(null_bitmap, bitmap, &result);
  }

  return Datum(result);
}

template <>
Datum SimpleScalarArrayCompare<StringType>(CompareOptions options, const Datum& lhs,
                                           const Datum& rhs) {
  bool swap = lhs.is_array();
  auto array = std::static_pointer_cast<StringArray>((swap ? lhs : rhs).make_array());
  auto value = util::string_view(
      *std::static_pointer_cast<StringScalar>((swap ? rhs : lhs).scalar())->value);

  std::vector<bool> bitmap(array->length());
  for (int64_t i = 0; i < array->length(); i++) {
    bitmap[i] = swap ? SlowCompare(options.op, array->GetView(i), value)
                     : SlowCompare(options.op, value, array->GetView(i));
  }

  std::shared_ptr<Array> result;

  if (array->null_count() == 0) {
    ArrayFromVector<BooleanType>(bitmap, &result);
  } else {
    std::vector<bool> null_bitmap(array->length());
    auto reader =
        BitmapReader(array->null_bitmap_data(), array->offset(), array->length());
    for (int64_t i = 0; i < array->length(); i++, reader.Next()) {
      null_bitmap[i] = reader.IsSet();
    }
    ArrayFromVector<BooleanType>(null_bitmap, bitmap, &result);
  }

  return Datum(result);
}

template <typename ArrayType>
std::vector<bool> NullBitmapFromArrays(const ArrayType& lhs, const ArrayType& rhs) {
  auto left_lambda = [&lhs](int64_t i) {
    return lhs.null_count() == 0 ? true : lhs.IsValid(i);
  };

  auto right_lambda = [&rhs](int64_t i) {
    return rhs.null_count() == 0 ? true : rhs.IsValid(i);
  };

  const int64_t length = lhs.length();
  std::vector<bool> null_bitmap(length);

  for (int64_t i = 0; i < length; i++) {
    null_bitmap[i] = left_lambda(i) && right_lambda(i);
  }

  return null_bitmap;
}

template <typename ArrowType>
Datum SimpleArrayArrayCompare(CompareOptions options, const Datum& lhs,
                              const Datum& rhs) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  auto l_array = std::static_pointer_cast<ArrayType>(lhs.make_array());
  auto r_array = std::static_pointer_cast<ArrayType>(rhs.make_array());
  const int64_t length = l_array->length();

  std::vector<bool> bitmap(length);
  for (int64_t i = 0; i < length; i++) {
    bitmap[i] = SlowCompare(options.op, l_array->Value(i), r_array->Value(i));
  }

  std::shared_ptr<Array> result;

  if (l_array->null_count() == 0 && r_array->null_count() == 0) {
    ArrayFromVector<BooleanType>(bitmap, &result);
  } else {
    std::vector<bool> null_bitmap = NullBitmapFromArrays(*l_array, *r_array);
    ArrayFromVector<BooleanType>(null_bitmap, bitmap, &result);
  }

  return Datum(result);
}

template <>
Datum SimpleArrayArrayCompare<StringType>(CompareOptions options, const Datum& lhs,
                                          const Datum& rhs) {
  auto l_array = std::static_pointer_cast<StringArray>(lhs.make_array());
  auto r_array = std::static_pointer_cast<StringArray>(rhs.make_array());
  const int64_t length = l_array->length();

  std::vector<bool> bitmap(length);
  for (int64_t i = 0; i < length; i++) {
    bitmap[i] = SlowCompare(options.op, l_array->GetView(i), r_array->GetView(i));
  }

  std::shared_ptr<Array> result;

  if (l_array->null_count() == 0 && r_array->null_count() == 0) {
    ArrayFromVector<BooleanType>(bitmap, &result);
  } else {
    std::vector<bool> null_bitmap = NullBitmapFromArrays(*l_array, *r_array);
    ArrayFromVector<BooleanType>(null_bitmap, bitmap, &result);
  }

  return Datum(result);
}

template <typename ArrowType>
void ValidateCompare(CompareOptions options, const Datum& lhs, const Datum& rhs) {
  Datum result;

  bool has_scalar = lhs.is_scalar() || rhs.is_scalar();
  Datum expected = has_scalar ? SimpleScalarArrayCompare<ArrowType>(options, lhs, rhs)
                              : SimpleArrayArrayCompare<ArrowType>(options, lhs, rhs);

  ValidateCompare<ArrowType>(options, lhs, rhs, expected);
}

template <typename ArrowType>
class TestNumericCompareKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericCompareKernel, NumericArrowTypes);
TYPED_TEST(TestNumericCompareKernel, SimpleCompareArrayScalar) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using CType = typename TypeTraits<TypeParam>::CType;

  Datum one(std::make_shared<ScalarType>(CType(1)));

  CompareOptions eq(CompareOperator::EQUAL);
  ValidateCompare<TypeParam>(eq, "[]", one, "[]");
  ValidateCompare<TypeParam>(eq, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(eq, "[0,0,1,1,2,2]", one, "[0,0,1,1,0,0]");
  ValidateCompare<TypeParam>(eq, "[0,1,2,3,4,5]", one, "[0,1,0,0,0,0]");
  ValidateCompare<TypeParam>(eq, "[5,4,3,2,1,0]", one, "[0,0,0,0,1,0]");
  ValidateCompare<TypeParam>(eq, "[null,0,1,1]", one, "[null,0,1,1]");

  CompareOptions neq(CompareOperator::NOT_EQUAL);
  ValidateCompare<TypeParam>(neq, "[]", one, "[]");
  ValidateCompare<TypeParam>(neq, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(neq, "[0,0,1,1,2,2]", one, "[1,1,0,0,1,1]");
  ValidateCompare<TypeParam>(neq, "[0,1,2,3,4,5]", one, "[1,0,1,1,1,1]");
  ValidateCompare<TypeParam>(neq, "[5,4,3,2,1,0]", one, "[1,1,1,1,0,1]");
  ValidateCompare<TypeParam>(neq, "[null,0,1,1]", one, "[null,1,0,0]");

  CompareOptions gt(CompareOperator::GREATER);
  ValidateCompare<TypeParam>(gt, "[]", one, "[]");
  ValidateCompare<TypeParam>(gt, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(gt, "[0,0,1,1,2,2]", one, "[0,0,0,0,1,1]");
  ValidateCompare<TypeParam>(gt, "[0,1,2,3,4,5]", one, "[0,0,1,1,1,1]");
  ValidateCompare<TypeParam>(gt, "[4,5,6,7,8,9]", one, "[1,1,1,1,1,1]");
  ValidateCompare<TypeParam>(gt, "[null,0,1,1]", one, "[null,0,0,0]");

  CompareOptions gte(CompareOperator::GREATER_EQUAL);
  ValidateCompare<TypeParam>(gte, "[]", one, "[]");
  ValidateCompare<TypeParam>(gte, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(gte, "[0,0,1,1,2,2]", one, "[0,0,1,1,1,1]");
  ValidateCompare<TypeParam>(gte, "[0,1,2,3,4,5]", one, "[0,1,1,1,1,1]");
  ValidateCompare<TypeParam>(gte, "[4,5,6,7,8,9]", one, "[1,1,1,1,1,1]");
  ValidateCompare<TypeParam>(gte, "[null,0,1,1]", one, "[null,0,1,1]");

  CompareOptions lt(CompareOperator::LESS);
  ValidateCompare<TypeParam>(lt, "[]", one, "[]");
  ValidateCompare<TypeParam>(lt, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(lt, "[0,0,1,1,2,2]", one, "[1,1,0,0,0,0]");
  ValidateCompare<TypeParam>(lt, "[0,1,2,3,4,5]", one, "[1,0,0,0,0,0]");
  ValidateCompare<TypeParam>(lt, "[4,5,6,7,8,9]", one, "[0,0,0,0,0,0]");
  ValidateCompare<TypeParam>(lt, "[null,0,1,1]", one, "[null,1,0,0]");

  CompareOptions lte(CompareOperator::LESS_EQUAL);
  ValidateCompare<TypeParam>(lte, "[]", one, "[]");
  ValidateCompare<TypeParam>(lte, "[null]", one, "[null]");
  ValidateCompare<TypeParam>(lte, "[0,0,1,1,2,2]", one, "[1,1,1,1,0,0]");
  ValidateCompare<TypeParam>(lte, "[0,1,2,3,4,5]", one, "[1,1,0,0,0,0]");
  ValidateCompare<TypeParam>(lte, "[4,5,6,7,8,9]", one, "[0,0,0,0,0,0]");
  ValidateCompare<TypeParam>(lte, "[null,0,1,1]", one, "[null,1,1,1]");
}

TYPED_TEST(TestNumericCompareKernel, SimpleCompareScalarArray) {
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using CType = typename TypeTraits<TypeParam>::CType;

  Datum one(std::make_shared<ScalarType>(CType(1)));

  CompareOptions eq(CompareOperator::EQUAL);
  ValidateCompare<TypeParam>(eq, one, "[]", "[]");
  ValidateCompare<TypeParam>(eq, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(eq, one, "[0,0,1,1,2,2]", "[0,0,1,1,0,0]");
  ValidateCompare<TypeParam>(eq, one, "[0,1,2,3,4,5]", "[0,1,0,0,0,0]");
  ValidateCompare<TypeParam>(eq, one, "[5,4,3,2,1,0]", "[0,0,0,0,1,0]");
  ValidateCompare<TypeParam>(eq, one, "[null,0,1,1]", "[null,0,1,1]");

  CompareOptions neq(CompareOperator::NOT_EQUAL);
  ValidateCompare<TypeParam>(neq, one, "[]", "[]");
  ValidateCompare<TypeParam>(neq, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(neq, one, "[0,0,1,1,2,2]", "[1,1,0,0,1,1]");
  ValidateCompare<TypeParam>(neq, one, "[0,1,2,3,4,5]", "[1,0,1,1,1,1]");
  ValidateCompare<TypeParam>(neq, one, "[5,4,3,2,1,0]", "[1,1,1,1,0,1]");
  ValidateCompare<TypeParam>(neq, one, "[null,0,1,1]", "[null,1,0,0]");

  CompareOptions gt(CompareOperator::GREATER);
  ValidateCompare<TypeParam>(gt, one, "[]", "[]");
  ValidateCompare<TypeParam>(gt, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(gt, one, "[0,0,1,1,2,2]", "[1,1,0,0,0,0]");
  ValidateCompare<TypeParam>(gt, one, "[0,1,2,3,4,5]", "[1,0,0,0,0,0]");
  ValidateCompare<TypeParam>(gt, one, "[4,5,6,7,8,9]", "[0,0,0,0,0,0]");
  ValidateCompare<TypeParam>(gt, one, "[null,0,1,1]", "[null,1,0,0]");

  CompareOptions gte(CompareOperator::GREATER_EQUAL);
  ValidateCompare<TypeParam>(gte, one, "[]", "[]");
  ValidateCompare<TypeParam>(gte, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(gte, one, "[0,0,1,1,2,2]", "[1,1,1,1,0,0]");
  ValidateCompare<TypeParam>(gte, one, "[0,1,2,3,4,5]", "[1,1,0,0,0,0]");
  ValidateCompare<TypeParam>(gte, one, "[4,5,6,7,8,9]", "[0,0,0,0,0,0]");
  ValidateCompare<TypeParam>(gte, one, "[null,0,1,1]", "[null,1,1,1]");

  CompareOptions lt(CompareOperator::LESS);
  ValidateCompare<TypeParam>(lt, one, "[]", "[]");
  ValidateCompare<TypeParam>(lt, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(lt, one, "[0,0,1,1,2,2]", "[0,0,0,0,1,1]");
  ValidateCompare<TypeParam>(lt, one, "[0,1,2,3,4,5]", "[0,0,1,1,1,1]");
  ValidateCompare<TypeParam>(lt, one, "[4,5,6,7,8,9]", "[1,1,1,1,1,1]");
  ValidateCompare<TypeParam>(lt, one, "[null,0,1,1]", "[null,0,0,0]");

  CompareOptions lte(CompareOperator::LESS_EQUAL);
  ValidateCompare<TypeParam>(lte, one, "[]", "[]");
  ValidateCompare<TypeParam>(lte, one, "[null]", "[null]");
  ValidateCompare<TypeParam>(lte, one, "[0,0,1,1,2,2]", "[0,0,1,1,1,1]");
  ValidateCompare<TypeParam>(lte, one, "[0,1,2,3,4,5]", "[0,1,1,1,1,1]");
  ValidateCompare<TypeParam>(lte, one, "[4,5,6,7,8,9]", "[1,1,1,1,1,1]");
  ValidateCompare<TypeParam>(lte, one, "[null,0,1,1]", "[null,0,1,1]");
}

TYPED_TEST(TestNumericCompareKernel, TestNullScalar) {
  /* Ensure that null scalar broadcast to all null results. */
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;

  Datum null(std::make_shared<ScalarType>());
  EXPECT_FALSE(null.scalar()->is_valid);

  CompareOptions eq(CompareOperator::EQUAL);
  ValidateCompare<TypeParam>(eq, "[]", null, "[]");
  ValidateCompare<TypeParam>(eq, null, "[]", "[]");
  ValidateCompare<TypeParam>(eq, "[null]", null, "[null]");
  ValidateCompare<TypeParam>(eq, null, "[null]", "[null]");
  ValidateCompare<TypeParam>(eq, null, "[1,2,3]", "[null, null, null]");
}

TYPED_TEST_SUITE(TestNumericCompareKernel, NumericArrowTypes);

template <typename Type>
struct CompareRandomNumeric {
  static void Test(const std::shared_ptr<DataType>& type) {
    using ScalarType = typename TypeTraits<Type>::ScalarType;
    using CType = typename TypeTraits<Type>::CType;
    auto rand = random::RandomArrayGenerator(0x5416447);
    const int64_t length = 1000;
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
        auto data =
            rand.Numeric<typename Type::PhysicalType>(length, 0, 100, null_probability);

        auto data1 =
            rand.Numeric<typename Type::PhysicalType>(length, 0, 100, null_probability);
        auto data2 =
            rand.Numeric<typename Type::PhysicalType>(length, 0, 100, null_probability);

        // Create view of data as the type (e.g. timestamp)
        auto array1 = Datum(*data1->View(type));
        auto array2 = Datum(*data2->View(type));
        auto fifty = Datum(std::make_shared<ScalarType>(CType(50), type));
        auto options = CompareOptions(op);

        ValidateCompare<Type>(options, array1, fifty);
        ValidateCompare<Type>(options, fifty, array1);
        ValidateCompare<Type>(options, array1, array2);
      }
    }
  }
};

TEST(TestCompareKernel, PrimitiveRandomTests) {
  TestRandomPrimitiveCTypes<CompareRandomNumeric>();
}

TYPED_TEST(TestNumericCompareKernel, SimpleCompareArrayArray) {
  /* Ensure that null scalar broadcast to all null results. */
  CompareOptions eq(CompareOperator::EQUAL);
  ValidateCompare<TypeParam>(eq, "[]", "[]", "[]");
  ValidateCompare<TypeParam>(eq, "[null]", "[null]", "[null]");
  ValidateCompare<TypeParam>(eq, "[1]", "[1]", "[1]");
  ValidateCompare<TypeParam>(eq, "[1]", "[2]", "[0]");
  ValidateCompare<TypeParam>(eq, "[null]", "[1]", "[null]");
  ValidateCompare<TypeParam>(eq, "[1]", "[null]", "[null]");

  CompareOptions lte(CompareOperator::LESS_EQUAL);
  ValidateCompare<TypeParam>(lte, "[1,2,3,4,5]", "[2,3,4,5,6]", "[1,1,1,1,1]");
}

TEST(TestCompareTimestamps, Basics) {
  const char* example1_json = R"(["1970-01-01","2000-02-29","1900-02-28"])";
  const char* example2_json = R"(["1970-01-02","2000-02-01","1900-02-28"])";

  auto CheckArrayCase = [&](std::shared_ptr<DataType> type, CompareOperator op,
                            const char* expected_json) {
    auto lhs = ArrayFromJSON(type, example1_json);
    auto rhs = ArrayFromJSON(type, example2_json);
    auto expected = ArrayFromJSON(boolean(), expected_json);
    ASSERT_OK_AND_ASSIGN(Datum result, Compare(lhs, rhs, CompareOptions(op)));
    AssertArraysEqual(*expected, *result.make_array(), /*verbose=*/true);
  };

  auto seconds = timestamp(TimeUnit::SECOND);
  auto millis = timestamp(TimeUnit::MILLI);
  auto micros = timestamp(TimeUnit::MICRO);
  auto nanos = timestamp(TimeUnit::NANO);

  CheckArrayCase(seconds, CompareOperator::EQUAL, "[false, false, true]");
  CheckArrayCase(seconds, CompareOperator::NOT_EQUAL, "[true, true, false]");
  CheckArrayCase(seconds, CompareOperator::LESS, "[true, false, false]");
  CheckArrayCase(seconds, CompareOperator::LESS_EQUAL, "[true, false, true]");
  CheckArrayCase(seconds, CompareOperator::GREATER, "[false, true, false]");
  CheckArrayCase(seconds, CompareOperator::GREATER_EQUAL, "[false, true, true]");

  // Check that comparisons with tz-aware timestamps work fine
  auto seconds_utc = timestamp(TimeUnit::SECOND, "utc");
  CheckArrayCase(seconds_utc, CompareOperator::EQUAL, "[false, false, true]");
}

class TestStringCompareKernel : public ::testing::Test {};

TEST_F(TestStringCompareKernel, SimpleCompareArrayScalar) {
  Datum one(std::make_shared<StringScalar>("one"));

  CompareOptions eq(CompareOperator::EQUAL);
  ValidateCompare<StringType>(eq, "[]", one, "[]");
  ValidateCompare<StringType>(eq, "[null]", one, "[null]");
  ValidateCompare<StringType>(eq, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[0,0,1,1,0,0]");
  ValidateCompare<StringType>(
      eq, "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]", one, "[0,1,0,0,0,0]");
  ValidateCompare<StringType>(
      eq, "[\"five\",\"four\",\"three\",\"two\",\"one\",\"zero\"]", one, "[0,0,0,0,1,0]");
  ValidateCompare<StringType>(eq, "[null,\"zero\",\"one\",\"one\"]", one, "[null,0,1,1]");

  CompareOptions neq(CompareOperator::NOT_EQUAL);
  ValidateCompare<StringType>(neq, "[]", one, "[]");
  ValidateCompare<StringType>(neq, "[null]", one, "[null]");
  ValidateCompare<StringType>(neq, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[1,1,0,0,1,1]");
  ValidateCompare<StringType>(neq,
                              "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]",
                              one, "[1,0,1,1,1,1]");
  ValidateCompare<StringType>(neq,
                              "[\"five\",\"four\",\"three\",\"two\",\"one\",\"zero\"]",
                              one, "[1,1,1,1,0,1]");
  ValidateCompare<StringType>(neq, "[null,\"zero\",\"one\",\"one\"]", one,
                              "[null,1,0,0]");

  CompareOptions gt(CompareOperator::GREATER);
  ValidateCompare<StringType>(gt, "[]", one, "[]");
  ValidateCompare<StringType>(gt, "[null]", one, "[null]");
  ValidateCompare<StringType>(gt, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[1,1,0,0,1,1]");
  ValidateCompare<StringType>(
      gt, "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]", one, "[1,0,1,1,0,0]");
  ValidateCompare<StringType>(gt,
                              "[\"four\",\"five\",\"six\",\"seven\",\"eight\",\"nine\"]",
                              one, "[0,0,1,1,0,0]");
  ValidateCompare<StringType>(gt, "[null,\"zero\",\"one\",\"one\"]", one, "[null,1,0,0]");

  CompareOptions gte(CompareOperator::GREATER_EQUAL);
  ValidateCompare<StringType>(gte, "[]", one, "[]");
  ValidateCompare<StringType>(gte, "[null]", one, "[null]");
  ValidateCompare<StringType>(gte, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[1,1,1,1,1,1]");
  ValidateCompare<StringType>(gte,
                              "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]",
                              one, "[1,1,1,1,0,0]");
  ValidateCompare<StringType>(gte,
                              "[\"four\",\"five\",\"six\",\"seven\",\"eight\",\"nine\"]",
                              one, "[0,0,1,1,0,0]");
  ValidateCompare<StringType>(gte, "[null,\"zero\",\"one\",\"one\"]", one,
                              "[null,1,1,1]");

  CompareOptions lt(CompareOperator::LESS);
  ValidateCompare<StringType>(lt, "[]", one, "[]");
  ValidateCompare<StringType>(lt, "[null]", one, "[null]");
  ValidateCompare<StringType>(lt, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[0,0,0,0,0,0]");
  ValidateCompare<StringType>(
      lt, "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]", one, "[0,0,0,0,1,1]");
  ValidateCompare<StringType>(lt,
                              "[\"four\",\"five\",\"six\",\"seven\",\"eight\",\"nine\"]",
                              one, "[1,1,0,0,1,1]");
  ValidateCompare<StringType>(lt, "[null,\"zero\",\"one\",\"one\"]", one, "[null,0,0,0]");

  CompareOptions lte(CompareOperator::LESS_EQUAL);
  ValidateCompare<StringType>(lte, "[]", one, "[]");
  ValidateCompare<StringType>(lte, "[null]", one, "[null]");
  ValidateCompare<StringType>(lte, "[\"zero\",\"zero\",\"one\",\"one\",\"two\",\"two\"]",
                              one, "[0,0,1,1,0,0]");
  ValidateCompare<StringType>(lte,
                              "[\"zero\",\"one\",\"two\",\"three\",\"four\",\"five\"]",
                              one, "[0,1,0,0,1,1]");
  ValidateCompare<StringType>(lte,
                              "[\"four\",\"five\",\"six\",\"seven\",\"eight\",\"nine\"]",
                              one, "[1,1,0,0,1,1]");
  ValidateCompare<StringType>(lte, "[null,\"zero\",\"one\",\"one\"]", one,
                              "[null,0,1,1]");
}

TEST_F(TestStringCompareKernel, RandomCompareArrayScalar) {
  using ScalarType = typename TypeTraits<StringType>::ScalarType;

  auto rand = random::RandomArrayGenerator(0x5416447);
  for (size_t i = 3; i < 10; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
        const int64_t length = static_cast<int64_t>(1ULL << i);
        auto array = Datum(rand.String(length, 0, 16, null_probability));
        auto hello = Datum(std::make_shared<ScalarType>("hello"));
        auto options = CompareOptions(op);
        ValidateCompare<StringType>(options, array, hello);
        ValidateCompare<StringType>(options, hello, array);
      }
    }
  }
}

TEST_F(TestStringCompareKernel, RandomCompareArrayArray) {
  auto rand = random::RandomArrayGenerator(0x5416447);
  for (size_t i = 3; i < 5; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto op : {EQUAL, NOT_EQUAL, GREATER, LESS_EQUAL}) {
        const int64_t length = static_cast<int64_t>(1ULL << i);
        auto lhs = Datum(rand.String(length << i, 0, 16, null_probability));
        auto rhs = Datum(rand.String(length << i, 0, 16, null_probability));
        auto options = CompareOptions(op);
        ValidateCompare<StringType>(options, lhs, rhs);
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow
