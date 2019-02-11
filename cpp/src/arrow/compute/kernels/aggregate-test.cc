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

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

using std::shared_ptr;
using std::vector;

namespace arrow {
namespace compute {

template <typename CType, typename Enable = void>
struct DatumEqual {
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {}
};

template <typename CType>
struct DatumEqual<CType,
                  typename std::enable_if<std::is_floating_point<CType>::value>::type> {
  static constexpr double kArbitraryDoubleErrorBound = 1.0;

  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      ASSERT_EQ(lhs.scalar().kind(), rhs.scalar().kind());
      ASSERT_NEAR(util::get<CType>(lhs.scalar().value),
                  util::get<CType>(rhs.scalar().value), kArbitraryDoubleErrorBound);
    }
  }
};

template <typename CType>
struct DatumEqual<CType,
                  typename std::enable_if<!std::is_floating_point<CType>::value>::type> {
  static void EnsureEqual(const Datum& lhs, const Datum& rhs) {
    ASSERT_EQ(lhs.kind(), rhs.kind());
    if (lhs.kind() == Datum::SCALAR) {
      ASSERT_EQ(lhs.scalar().kind(), rhs.scalar().kind());
      ASSERT_EQ(util::get<CType>(lhs.scalar().value),
                util::get<CType>(rhs.scalar().value));
    }
  }
};

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const Array& input, Datum expected) {
  using CType = typename ArrowType::c_type;
  using SumType = typename FindAccumulatorType<CType>::Type;

  Datum result;
  ASSERT_OK(Sum(ctx, input, &result));
  DatumEqual<SumType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const char* json, Datum expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum<ArrowType>(ctx, *array, expected);
}

template <typename ArrowType>
static Datum DummySum(const Array& array) {
  using CType = typename ArrowType::c_type;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using SumType = typename FindAccumulatorType<CType>::Type;

  SumType sum = 0;
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

  return (count > 0) ? Datum(Scalar(sum)) : Datum();
}

template <typename ArrowType>
void ValidateSum(FunctionContext* ctx, const Array& array) {
  ValidateSum<ArrowType>(ctx, array, DummySum<ArrowType>(array));
}

template <typename ArrowType>
class TestSumKernelNumeric : public ComputeFixture, public TestBase {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>
    NumericArrowTypes;

TYPED_TEST_CASE(TestSumKernelNumeric, NumericArrowTypes);
TYPED_TEST(TestSumKernelNumeric, SimpleSum) {
  using CType = typename TypeParam::c_type;
  using SumType = typename FindAccumulatorType<CType>::Type;

  ValidateSum<TypeParam>(&this->ctx_, "[]", Datum());

  ValidateSum<TypeParam>(&this->ctx_, "[0, 1, 2, 3, 4, 5]",
                         Datum(Scalar(static_cast<SumType>(5 * 6 / 2))));

  // Avoid this tests for (U)Int8Type
  if (sizeof(CType) > 1)
    ValidateSum<TypeParam>(&this->ctx_, "[1000, null, 300, null, 30, null, 7]",
                           Datum(Scalar(static_cast<SumType>(1337))));
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
