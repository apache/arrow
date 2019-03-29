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
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/count.h"
#include "arrow/compute/kernels/mean.h"
#include "arrow/compute/kernels/sum-internal.h"
#include "arrow/compute/kernels/sum.h"
#include "arrow/compute/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

///
/// Sum
///

template <typename ArrowType>
using SumResult =
    std::pair<typename FindAccumulatorType<ArrowType>::Type::c_type, size_t>;

template <typename ArrowType>
static SumResult<ArrowType> NaiveSumPartial(const Array& array) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ResultType = SumResult<ArrowType>;

  ResultType result;

  auto data = array.data();
  internal::BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();
  for (int64_t i = 0; i < array.length(); i++) {
    if (reader.IsSet()) {
      result.first += values[i];
      result.second++;
    }

    reader.Next();
  }

  return result;
}

template <typename ArrowType>
static Datum NaiveSum(const Array& array) {
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using SumScalarType = typename TypeTraits<SumType>::ScalarType;

  auto result = NaiveSumPartial<ArrowType>(array);
  bool is_valid = result.second > 0;

  return Datum(std::make_shared<SumScalarType>(result.first, is_valid));
}

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
void ValidateSum(FunctionContext* ctx, const Array& array) {
  ValidateSum<ArrowType>(ctx, array, NaiveSum<ArrowType>(array));
}

template <typename ArrowType>
class TestNumericSumKernel : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestNumericSumKernel, SimpleSum) {
  using SumType = typename FindAccumulatorType<TypeParam>::Type;
  using ScalarType = typename TypeTraits<SumType>::ScalarType;
  using T = typename TypeParam::c_type;

  ValidateSum<TypeParam>(&this->ctx_, "[]",
                         Datum(std::make_shared<ScalarType>(0, false)));

  ValidateSum<TypeParam>(&this->ctx_, "[null]",
                         Datum(std::make_shared<ScalarType>(0, false)));

  ValidateSum<TypeParam>(&this->ctx_, "[0, 1, 2, 3, 4, 5]",
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  const T expected_result = static_cast<T>(14);
  ValidateSum<TypeParam>(&this->ctx_, "[1, null, 3, null, 3, null, 7]",
                         Datum(std::make_shared<ScalarType>(expected_result)));
}

template <typename ArrowType>
class TestRandomNumericSumKernel : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestRandomNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericSumKernel, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  for (size_t i = 3; i < 14; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateSum<TypeParam>(&this->ctx_, *array);
      }
    }
  }
}

TYPED_TEST(TestRandomNumericSumKernel, RandomSliceArraySum) {
  auto arithmetic = ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(),
                                  "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]");
  ValidateSum<TypeParam>(&this->ctx_, *arithmetic);
  for (size_t i = 1; i < 15; i++) {
    auto slice = arithmetic->Slice(i, 16);
    ValidateSum<TypeParam>(&this->ctx_, *slice);
  }

  // Trigger ConsumeSparse with different slice offsets.
  auto rand = random::RandomArrayGenerator(0xfa432643);
  const int64_t length = 1U << 6;
  auto array = rand.Numeric<TypeParam>(length, 0, 10, 0.5);
  for (size_t i = 1; i < 16; i++) {
    for (size_t j = 1; j < 16; j++) {
      auto slice = array->Slice(i, length - j);
      ValidateSum<TypeParam>(&this->ctx_, *slice);
    }
  }
}

///
/// Mean
///

template <typename ArrowType>
static Datum NaiveMean(const Array& array) {
  using MeanScalarType = typename TypeTraits<DoubleType>::ScalarType;

  const auto result = NaiveSumPartial<ArrowType>(array);
  const double mean = static_cast<double>(result.first) /
                      static_cast<double>(result.second ? result.second : 1UL);
  const bool is_valid = result.second > 0;

  return Datum(std::make_shared<MeanScalarType>(mean, is_valid));
}

template <typename ArrowType>
void ValidateMean(FunctionContext* ctx, const Array& input, Datum expected) {
  using OutputType = typename FindAccumulatorType<DoubleType>::Type;

  Datum result;
  ASSERT_OK(Mean(ctx, input, &result));
  DatumEqual<OutputType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateMean(FunctionContext* ctx, const char* json, Datum expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateMean<ArrowType>(ctx, *array, expected);
}

template <typename ArrowType>
void ValidateMean(FunctionContext* ctx, const Array& array) {
  ValidateMean<ArrowType>(ctx, array, NaiveMean<ArrowType>(array));
}

template <typename ArrowType>
class TestMeanKernelNumeric : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestMeanKernelNumeric, NumericArrowTypes);
TYPED_TEST(TestMeanKernelNumeric, SimpleMean) {
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

  ValidateMean<TypeParam>(&this->ctx_, "[]",
                          Datum(std::make_shared<ScalarType>(0.0, false)));

  ValidateMean<TypeParam>(&this->ctx_, "[null]",
                          Datum(std::make_shared<ScalarType>(0.0, false)));

  ValidateMean<TypeParam>(&this->ctx_, "[1, null, 1]",
                          Datum(std::make_shared<ScalarType>(1.0)));

  ValidateMean<TypeParam>(&this->ctx_, "[1, 2, 3, 4, 5, 6, 7, 8]",
                          Datum(std::make_shared<ScalarType>(4.5)));

  ValidateMean<TypeParam>(&this->ctx_, "[0, 0, 0, 0, 0, 0, 0, 0]",
                          Datum(std::make_shared<ScalarType>(0.0)));

  ValidateMean<TypeParam>(&this->ctx_, "[1, 1, 1, 1, 1, 1, 1, 1]",
                          Datum(std::make_shared<ScalarType>(1.0)));
}

template <typename ArrowType>
class TestRandomNumericMeanKernel : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestRandomNumericMeanKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericMeanKernel, RandomArrayMean) {
  auto rand = random::RandomArrayGenerator(0x8afc055);
  for (size_t i = 3; i < 14; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateMean<TypeParam>(&this->ctx_, *array);
      }
    }
  }
}

///
/// Count
///
//
using CountPair = std::pair<int64_t, int64_t>;

static CountPair NaiveCount(const Array& array) {
  CountPair count;

  count.first = array.length() - array.null_count();
  count.second = array.null_count();

  return count;
}

void ValidateCount(FunctionContext* ctx, const Array& input, CountPair expected) {
  CountOptions all = CountOptions(CountOptions::COUNT_ALL);
  CountOptions nulls = CountOptions(CountOptions::COUNT_NULL);
  Datum result;

  ASSERT_OK(Count(ctx, all, input, &result));
  AssertDatumsEqual(result, Datum(expected.first));

  ASSERT_OK(Count(ctx, nulls, input, &result));
  AssertDatumsEqual(result, Datum(expected.second));
}

template <typename ArrowType>
void ValidateCount(FunctionContext* ctx, const char* json, CountPair expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateCount(ctx, *array, expected);
}

void ValidateCount(FunctionContext* ctx, const Array& input) {
  ValidateCount(ctx, input, NaiveCount(input));
}

template <typename ArrowType>
class TestCountKernel : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestCountKernel, NumericArrowTypes);
TYPED_TEST(TestCountKernel, SimpleCount) {
  ValidateCount<TypeParam>(&this->ctx_, "[]", {0, 0});
  ValidateCount<TypeParam>(&this->ctx_, "[null]", {0, 1});
  ValidateCount<TypeParam>(&this->ctx_, "[1, null, 2]", {2, 1});
  ValidateCount<TypeParam>(&this->ctx_, "[null, null, null]", {0, 3});
  ValidateCount<TypeParam>(&this->ctx_, "[1, 2, 3, 4, 5, 6, 7, 8, 9]", {9, 0});
}

template <typename ArrowType>
class TestRandomNumericCountKernel : public ComputeFixture, public TestBase {};

TYPED_TEST_CASE(TestRandomNumericCountKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericCountKernel, RandomArrayCount) {
  auto rand = random::RandomArrayGenerator(0x1205643);
  for (size_t i = 3; i < 14; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateCount(&this->ctx_, *array);
      }
    }
  }
}

}  // namespace compute
}  // namespace arrow
