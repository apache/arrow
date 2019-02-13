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

#include <string>
#include <type_traits>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/sum.h"
#include "arrow/compute/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

using std::shared_ptr;
using std::vector;

namespace arrow {
namespace compute {

template <typename Type, typename Enable = void>
struct DatumEqual {
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {}
};

template <typename Type>
struct DatumEqual<Type, typename std::enable_if<IsFloatingPoint<Type>::Value>::type> {
  static constexpr double kArbitraryDoubleErrorBound = 1.0;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = static_cast<const ScalarType*>(lhs.scalar().get());
      auto right = static_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(left->type->id(), right->type->id());
      ASSERT_NEAR(left->value, right->value, kArbitraryDoubleErrorBound);
    }
  }
};

template <typename Type>
struct DatumEqual<Type, typename std::enable_if<!IsFloatingPoint<Type>::value>::type> {
  using ScalarType = typename TypeTraits<Type>::ScalarType;
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      auto left = static_cast<const ScalarType*>(lhs.scalar().get());
      auto right = static_cast<const ScalarType*>(rhs.scalar().get());
      ASSERT_EQ(left->type->id(), right->type->id());
      ASSERT_EQ(left->value, right->value);
    }
  }
};

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const Array& input, Datum expected) {
  using OutputType = typename FindAccumulatorType<ArrowType>::Type;
  Datum result;
  ASSERT_OK(Sum(ctx, input, &result));
  DatumEqual<OutputType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const char* json, Datum expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum<ArrowType>(ctx, *array, expected);
}

template <typename ArrowType>
static Datum DummySum(const Array& array) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using SumScalarType = typename TypeTraits<SumType>::ScalarType;

  typename SumType::c_type sum = 0;
  int64_t count = 0;

  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();
  const auto bitmap = array.null_bitmap_data();
  for (int64_t i = 0; i < array.length(); i++) {
    if (BitUtil::GetBit(bitmap, i)) {
      sum += values[i];
      count++;
    }
  }

  if (count > 0) {
    return Datum(std::make_shared<SumScalarType>(sum));
  } else {
    return Datum(std::make_shared<SumScalarType>(0, false));
  }
}

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const Array& array) {
  ValidateSum<ArrowType>(ctx, array, DummySum<ArrowType>(array));
}

template <typename ArrowType>
class TestSumKernelNumeric : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestSumKernelNumeric, NumericArrowTypes);
TYPED_TEST(TestSumKernelNumeric, SimpleSum) {
  using SumType = typename FindAccumulatorType<TypeParam>::Type;
  using ScalarType = typename TypeTraits<SumType>::ScalarType;
  using T = typename TypeParam::c_type;

  ValidateSum<TypeParam>(&this->ctx_, "[]",
                         Datum(std::make_shared<ScalarType>(0, false)));

  ValidateSum<TypeParam>(&this->ctx_, "[0, 1, 2, 3, 4, 5]",
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  const T expected_result = static_cast<T>(14);
  ValidateSum<TypeParam>(&this->ctx_, "[1, null, 3, null, 3, null, 7]",
                         Datum(std::make_shared<ScalarType>(expected_result)));
}

template <typename ArrowType>
class TestRandomSumKernelNumeric : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestRandomSumKernelNumeric, NumericArrowTypes);
TYPED_TEST(TestRandomSumKernelNumeric, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  for (size_t i = 5; i < 14; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto length_offset : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_offset;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateSum<TypeParam>(&this->ctx_, *array);
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow
