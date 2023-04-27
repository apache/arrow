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
#include <limits>
#include <memory>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/compute/registry.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"

#include "arrow/testing/gtest_util.h"
#include "arrow/testing/matchers.h"
#include "arrow/testing/random.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::BitmapReader;
using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

using internal::FindAccumulatorType;

//
// Sum
//

template <typename ArrowType>
using SumResult =
    std::pair<typename FindAccumulatorType<ArrowType>::Type::c_type, size_t>;

template <typename ArrowType>
static SumResult<ArrowType> NaiveSumPartial(const Array& array) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using ResultType = SumResult<ArrowType>;

  ResultType result;

  auto data = array.data();
  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();

  if (array.null_count() != 0) {
    BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
    for (int64_t i = 0; i < array.length(); i++) {
      if (reader.IsSet()) {
        result.first += values[i];
        result.second++;
      }

      reader.Next();
    }
  } else {
    for (int64_t i = 0; i < array.length(); i++) {
      result.first += values[i];
      result.second++;
    }
  }

  return result;
}

template <typename ArrowType>
static Datum NaiveSum(const Array& array) {
  using SumType = typename FindAccumulatorType<ArrowType>::Type;
  using SumScalarType = typename TypeTraits<SumType>::ScalarType;

  auto result = NaiveSumPartial<ArrowType>(array);
  bool is_valid = result.second > 0;

  if (!is_valid) return Datum(std::make_shared<SumScalarType>());
  return Datum(std::make_shared<SumScalarType>(result.first));
}

void ValidateSum(
    const Datum input, Datum expected,
    const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
  ASSERT_OK_AND_ASSIGN(Datum result, Sum(input, options));
  AssertDatumsApproxEqual(expected, result, /*verbose=*/true);
}

template <typename ArrowType>
void ValidateSum(
    const char* json, Datum expected,
    const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum(*array, expected, options);
}

template <typename ArrowType>
void ValidateSum(
    const std::vector<std::string>& json, Datum expected,
    const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
  auto array = ChunkedArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum(array, expected, options);
}

template <typename ArrowType>
void ValidateSum(const Array& array, const ScalarAggregateOptions& options =
                                         ScalarAggregateOptions::Defaults()) {
  ValidateSum(array, NaiveSum<ArrowType>(array), options);
}

using UnaryOp = Result<Datum>(const Datum&, const ScalarAggregateOptions&, ExecContext*);

template <UnaryOp& Op, typename ScalarAggregateOptions, typename ScalarType>
void ValidateBooleanAgg(const std::string& json,
                        const std::shared_ptr<ScalarType>& expected,
                        const ScalarAggregateOptions& options) {
  SCOPED_TRACE(json);
  auto array = ArrayFromJSON(boolean(), json);
  ASSERT_OK_AND_ASSIGN(Datum result, Op(array, options, nullptr));
  AssertScalarsEqual(*expected, *result.scalar(), /*verbose=*/true);
}

TEST(TestBooleanAggregation, Sum) {
  const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults();
  ValidateBooleanAgg<Sum>("[]", std::make_shared<UInt64Scalar>(), options);
  ValidateBooleanAgg<Sum>("[null]", std::make_shared<UInt64Scalar>(), options);
  ValidateBooleanAgg<Sum>("[null, false]", std::make_shared<UInt64Scalar>(0), options);
  ValidateBooleanAgg<Sum>("[true]", std::make_shared<UInt64Scalar>(1), options);
  ValidateBooleanAgg<Sum>("[true, false, true]", std::make_shared<UInt64Scalar>(2),
                          options);
  ValidateBooleanAgg<Sum>("[true, false, true, true, null]",
                          std::make_shared<UInt64Scalar>(3), options);

  const ScalarAggregateOptions& options_min_count_zero =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
  ValidateBooleanAgg<Sum>("[]", std::make_shared<UInt64Scalar>(0),
                          options_min_count_zero);
  ValidateBooleanAgg<Sum>("[null]", std::make_shared<UInt64Scalar>(0),
                          options_min_count_zero);

  std::string json = "[true, null, false, null]";
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(1),
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(1),
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(),
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3));
  ValidateBooleanAgg<Sum>("[]", std::make_shared<UInt64Scalar>(0),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Sum>("[]", std::make_shared<UInt64Scalar>(),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));

  json = "[true, false]";
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(1),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(1),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/2));
  ValidateBooleanAgg<Sum>(json, std::make_shared<UInt64Scalar>(),
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));

  EXPECT_THAT(Sum(MakeScalar(true)),
              ResultWith(Datum(std::make_shared<UInt64Scalar>(1))));
  EXPECT_THAT(Sum(MakeScalar(false)),
              ResultWith(Datum(std::make_shared<UInt64Scalar>(0))));
  EXPECT_THAT(Sum(MakeNullScalar(boolean())),
              ResultWith(Datum(MakeNullScalar(uint64()))));
  EXPECT_THAT(Sum(MakeNullScalar(boolean()),
                  ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
              ResultWith(ScalarFromJSON(uint64(), "0")));
  EXPECT_THAT(Sum(MakeNullScalar(boolean()),
                  ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
              ResultWith(ScalarFromJSON(uint64(), "null")));
}

TEST(TestBooleanAggregation, Product) {
  const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults();
  ValidateBooleanAgg<Product>("[]", std::make_shared<UInt64Scalar>(), options);
  ValidateBooleanAgg<Product>("[null]", std::make_shared<UInt64Scalar>(), options);
  ValidateBooleanAgg<Product>("[null, false]", std::make_shared<UInt64Scalar>(0),
                              options);
  ValidateBooleanAgg<Product>("[true]", std::make_shared<UInt64Scalar>(1), options);
  ValidateBooleanAgg<Product>("[true, false, true]", std::make_shared<UInt64Scalar>(0),
                              options);
  ValidateBooleanAgg<Product>("[true, false, true, true, null]",
                              std::make_shared<UInt64Scalar>(0), options);

  const ScalarAggregateOptions& options_min_count_zero =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
  ValidateBooleanAgg<Product>("[]", std::make_shared<UInt64Scalar>(1),
                              options_min_count_zero);
  ValidateBooleanAgg<Product>("[null]", std::make_shared<UInt64Scalar>(1),
                              options_min_count_zero);

  const char* json = "[true, null, true, null]";
  ValidateBooleanAgg<Product>(
      json, std::make_shared<UInt64Scalar>(1),
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateBooleanAgg<Product>(
      json, std::make_shared<UInt64Scalar>(1),
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2));
  ValidateBooleanAgg<Product>(
      json, std::make_shared<UInt64Scalar>(),
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3));
  ValidateBooleanAgg<Product>(
      "[]", std::make_shared<UInt64Scalar>(1),
      ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Product>(
      json, std::make_shared<UInt64Scalar>(),
      ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Product>(
      "[]", std::make_shared<UInt64Scalar>(),
      ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));
  ValidateBooleanAgg<Product>(
      json, std::make_shared<UInt64Scalar>(),
      ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));

  EXPECT_THAT(Product(MakeScalar(true)),
              ResultWith(Datum(std::make_shared<UInt64Scalar>(1))));
  EXPECT_THAT(Product(MakeScalar(false)),
              ResultWith(Datum(std::make_shared<UInt64Scalar>(0))));
  EXPECT_THAT(Product(MakeNullScalar(boolean())),
              ResultWith(Datum(MakeNullScalar(uint64()))));
  EXPECT_THAT(Product(MakeNullScalar(boolean()),
                      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
              ResultWith(ScalarFromJSON(uint64(), "1")));
  EXPECT_THAT(Product(MakeNullScalar(boolean()),
                      ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
              ResultWith(ScalarFromJSON(uint64(), "null")));
}

TEST(TestBooleanAggregation, Mean) {
  const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults();
  ValidateBooleanAgg<Mean>("[]", std::make_shared<DoubleScalar>(), options);
  ValidateBooleanAgg<Mean>("[null]", std::make_shared<DoubleScalar>(), options);
  ValidateBooleanAgg<Mean>("[null, false]", std::make_shared<DoubleScalar>(0), options);
  ValidateBooleanAgg<Mean>("[true]", std::make_shared<DoubleScalar>(1), options);
  ValidateBooleanAgg<Mean>("[true, false, true, false]",
                           std::make_shared<DoubleScalar>(0.5), options);
  ValidateBooleanAgg<Mean>("[true, null]", std::make_shared<DoubleScalar>(1), options);
  ValidateBooleanAgg<Mean>("[true, null, false, true, true]",
                           std::make_shared<DoubleScalar>(0.75), options);
  ValidateBooleanAgg<Mean>("[true, null, false, false, false]",
                           std::make_shared<DoubleScalar>(0.25), options);

  const ScalarAggregateOptions& options_min_count_zero =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
  ValidateBooleanAgg<Mean>("[]", std::make_shared<DoubleScalar>(NAN),
                           options_min_count_zero);
  ValidateBooleanAgg<Mean>("[null]", std::make_shared<DoubleScalar>(NAN),
                           options_min_count_zero);

  const char* json = "[true, null, false, null]";
  ValidateBooleanAgg<Mean>(json, std::make_shared<DoubleScalar>(0.5),
                           ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateBooleanAgg<Mean>(json, std::make_shared<DoubleScalar>(0.5),
                           ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2));
  ValidateBooleanAgg<Mean>(json, std::make_shared<DoubleScalar>(),
                           ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3));
  ValidateBooleanAgg<Mean>("[]", std::make_shared<DoubleScalar>(NAN),
                           ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Mean>(json, std::make_shared<DoubleScalar>(),
                           ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateBooleanAgg<Mean>("[]", std::make_shared<DoubleScalar>(),
                           ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));
  ValidateBooleanAgg<Mean>(json, std::make_shared<DoubleScalar>(),
                           ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3));

  EXPECT_THAT(Mean(MakeScalar(true)), ResultWith(ScalarFromJSON(float64(), "1.0")));
  EXPECT_THAT(Mean(MakeScalar(false)), ResultWith(ScalarFromJSON(float64(), "0.0")));
  EXPECT_THAT(Mean(MakeNullScalar(boolean())),
              ResultWith(ScalarFromJSON(float64(), "null")));
  ASSERT_OK_AND_ASSIGN(
      auto result, Mean(MakeNullScalar(boolean()),
                        ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)));
  AssertDatumsApproxEqual(result, ScalarFromJSON(float64(), "NaN"), /*detailed=*/true);
  EXPECT_THAT(Mean(MakeNullScalar(boolean()),
                   ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
              ResultWith(ScalarFromJSON(float64(), "null")));
}

template <typename ArrowType>
class TestNumericSumKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestNumericSumKernel, SimpleSum) {
  using SumType = typename FindAccumulatorType<TypeParam>::Type;
  using ScalarType = typename TypeTraits<SumType>::ScalarType;
  using InputScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using T = typename TypeParam::c_type;

  ValidateSum<TypeParam>("[]", Datum(std::make_shared<ScalarType>()));

  ValidateSum<TypeParam>("[null]", Datum(std::make_shared<ScalarType>()));

  ValidateSum<TypeParam>("[0, 1, 2, 3, 4, 5]",
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  std::vector<std::string> chunks = {"[0, 1, 2, 3, 4, 5]"};
  ValidateSum<TypeParam>(chunks,
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  chunks = {"[0, 1, 2]", "[3, 4, 5]"};
  ValidateSum<TypeParam>(chunks,
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  chunks = {"[0, 1, 2]", "[]", "[3, 4, 5]"};
  ValidateSum<TypeParam>(chunks,
                         Datum(std::make_shared<ScalarType>(static_cast<T>(5 * 6 / 2))));

  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  ValidateSum<TypeParam>("[]", Datum(std::make_shared<ScalarType>(static_cast<T>(0))),
                         options);
  ValidateSum<TypeParam>("[null]", Datum(std::make_shared<ScalarType>(static_cast<T>(0))),
                         options);
  chunks = {};
  ValidateSum<TypeParam>(chunks, Datum(std::make_shared<ScalarType>(static_cast<T>(0))),
                         options);

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0);
  ValidateSum<TypeParam>("[]", Datum(std::make_shared<ScalarType>(static_cast<T>(0))),
                         options);
  ValidateSum<TypeParam>("[null]", Datum(std::make_shared<ScalarType>()), options);
  ValidateSum<TypeParam>("[1, null, 3, null, 3, null, 7]",
                         Datum(std::make_shared<ScalarType>()), options);
  ValidateSum<TypeParam>("[1, null, 3, null, 3, null, 7]",
                         Datum(std::make_shared<ScalarType>(14)));

  EXPECT_THAT(Sum(Datum(std::make_shared<InputScalarType>(static_cast<T>(5)))),
              ResultWith(Datum(std::make_shared<ScalarType>(static_cast<T>(5)))));
  EXPECT_THAT(Sum(MakeNullScalar(TypeTraits<TypeParam>::type_singleton())),
              ResultWith(Datum(MakeNullScalar(TypeTraits<SumType>::type_singleton()))));
}

TYPED_TEST(TestNumericSumKernel, ScalarAggregateOptions) {
  using SumType = typename FindAccumulatorType<TypeParam>::Type;
  using ScalarType = typename TypeTraits<SumType>::ScalarType;
  using InputScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using T = typename TypeParam::c_type;

  const T expected_result = static_cast<T>(14);
  auto null_result = Datum(std::make_shared<ScalarType>());
  auto zero_result = Datum(std::make_shared<ScalarType>(static_cast<T>(0)));
  auto result = Datum(std::make_shared<ScalarType>(expected_result));
  const char* json = "[1, null, 3, null, 3, null, 7]";

  ValidateSum<TypeParam>("[]", zero_result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0));
  ValidateSum<TypeParam>("[null]", zero_result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0));
  ValidateSum<TypeParam>(json, result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3));
  ValidateSum<TypeParam>(json, result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4));
  ValidateSum<TypeParam>(json, null_result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5));
  ValidateSum<TypeParam>("[]", null_result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateSum<TypeParam>("[null]", null_result,
                         ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateSum<TypeParam>("[]", zero_result,
                         ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateSum<TypeParam>(json, null_result,
                         ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));

  EXPECT_THAT(Sum(Datum(std::make_shared<InputScalarType>(static_cast<T>(5))),
                  ScalarAggregateOptions(/*skip_nulls=*/false)),
              ResultWith(Datum(std::make_shared<ScalarType>(static_cast<T>(5)))));
  EXPECT_THAT(Sum(Datum(std::make_shared<InputScalarType>(static_cast<T>(5))),
                  ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
              ResultWith(Datum(MakeNullScalar(TypeTraits<SumType>::type_singleton()))));
  EXPECT_THAT(Sum(MakeNullScalar(TypeTraits<TypeParam>::type_singleton()),
                  ScalarAggregateOptions(/*skip_nulls=*/false)),
              ResultWith(Datum(MakeNullScalar(TypeTraits<SumType>::type_singleton()))));
}

template <typename ArrowType>
class TestRandomNumericSumKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericSumKernel, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  // Test size up to 1<<13 (8192).
  for (size_t i = 3; i < 14; i += 2) {
    for (auto null_probability : {0.0, 0.001, 0.1, 0.5, 0.999, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateSum<TypeParam>(*array, options);
      }
    }
  }
}

TYPED_TEST_SUITE(TestRandomNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericSumKernel, RandomArraySumOverflow) {
  using CType = typename TypeParam::c_type;
  using SumCType = typename FindAccumulatorType<TypeParam>::Type::c_type;
  if (sizeof(CType) == sizeof(SumCType)) {
    // Skip if accumulator type is same to original type
    return;
  }

  CType max = std::numeric_limits<CType>::max();
  CType min = std::numeric_limits<CType>::min();
  int64_t length = 1024;

  auto rand = random::RandomArrayGenerator(0x5487655);
  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Test overflow on the original type
    auto array = rand.Numeric<TypeParam>(length, max - 200, max - 100, null_probability);
    ValidateSum<TypeParam>(*array, options);
    array = rand.Numeric<TypeParam>(length, min + 100, min + 200, null_probability);
    ValidateSum<TypeParam>(*array, options);
  }
}

TYPED_TEST(TestRandomNumericSumKernel, RandomSliceArraySum) {
  auto arithmetic = ArrayFromJSON(TypeTraits<TypeParam>::type_singleton(),
                                  "[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]");
  ValidateSum<TypeParam>(*arithmetic);
  for (size_t i = 1; i < 15; i++) {
    auto slice = arithmetic->Slice(i, 16);
    ValidateSum<TypeParam>(*slice);
  }

  // Trigger ConsumeSparse with different slice offsets.
  auto rand = random::RandomArrayGenerator(0xfa432643);
  const int64_t length = 1U << 5;
  auto array = rand.Numeric<TypeParam>(length, 0, 10, 0.5);
  for (size_t i = 1; i < 16; i++) {
    for (size_t j = 1; j < 16; j++) {
      auto slice = array->Slice(i, length - j);
      ValidateSum<TypeParam>(*slice);
    }
  }
}

// Test round-off error
class TestSumKernelRoundOff : public ::testing::Test {};

TEST_F(TestSumKernelRoundOff, Basics) {
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

  // array = np.arange(321000, dtype='float64')
  // array -= np.mean(array)
  // array *= arrray
  double index = 0;
  ASSERT_OK_AND_ASSIGN(
      auto array, ArrayFromBuilderVisitor(
                      float64(), 321000, [&](NumericBuilder<DoubleType>* builder) {
                        builder->UnsafeAppend((index - 160499.5) * (index - 160499.5));
                        ++index;
                      }));

  // reference value from numpy.sum()
  ASSERT_OK_AND_ASSIGN(Datum result, Sum(array));
  auto sum = checked_cast<const ScalarType*>(result.scalar().get());
  ASSERT_EQ(sum->value, 2756346749973250.0);
}

TEST(TestDecimalSumKernel, SimpleSum) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    EXPECT_THAT(Sum(ArrayFromJSON(ty, R"([])")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
    EXPECT_THAT(Sum(ArrayFromJSON(ty, R"([null])")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
    EXPECT_THAT(
        Sum(ArrayFromJSON(ty, R"(["0.00", "1.01", "2.02", "3.03", "4.04", "5.05"])")),
        ResultWith(ScalarFromJSON(ty, R"("15.15")")));
    Datum chunks =
        ChunkedArrayFromJSON(ty, {R"(["0.00", "1.01", "2.02", "3.03", "4.04", "5.05"])"});
    EXPECT_THAT(Sum(chunks), ResultWith(ScalarFromJSON(ty, R"("15.15")")));
    chunks = ChunkedArrayFromJSON(
        ty, {R"(["0.00", "1.01", "2.02"])", R"(["3.03", "4.04", "5.05"])"});
    EXPECT_THAT(Sum(chunks), ResultWith(ScalarFromJSON(ty, R"("15.15")")));
    chunks = ChunkedArrayFromJSON(
        ty, {R"(["0.00", "1.01", "2.02"])", "[]", R"(["3.03", "4.04", "5.05"])"});
    EXPECT_THAT(Sum(chunks), ResultWith(ScalarFromJSON(ty, R"("15.15")")));

    ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
    EXPECT_THAT(Sum(ArrayFromJSON(ty, R"([])"), options),
                ResultWith(ScalarFromJSON(ty, R"("0.00")")));
    EXPECT_THAT(Sum(ArrayFromJSON(ty, R"([null])"), options),
                ResultWith(ScalarFromJSON(ty, R"("0.00")")));
    chunks = ChunkedArrayFromJSON(ty, {});
    EXPECT_THAT(Sum(chunks, options), ResultWith(ScalarFromJSON(ty, R"("0.00")")));

    EXPECT_THAT(
        Sum(ArrayFromJSON(ty, R"(["1.01", null, "3.03", null, "5.05", null, "7.07"])"),
            options),
        ResultWith(ScalarFromJSON(ty, R"("16.16")")));

    EXPECT_THAT(Sum(ScalarFromJSON(ty, R"("5.05")")),
                ResultWith(ScalarFromJSON(ty, R"("5.05")")));
    EXPECT_THAT(Sum(ScalarFromJSON(ty, R"(null)")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
    EXPECT_THAT(Sum(ScalarFromJSON(ty, R"(null)"), options),
                ResultWith(ScalarFromJSON(ty, R"("0.00")")));
  }
}

TEST(TestDecimalSumKernel, ScalarAggregateOptions) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    Datum null = ScalarFromJSON(ty, R"(null)");
    Datum zero = ScalarFromJSON(ty, R"("0.00")");
    Datum result = ScalarFromJSON(ty, R"("14.14")");
    Datum arr =
        ArrayFromJSON(ty, R"(["1.01", null, "3.03", null, "3.03", null, "7.07"])");

    EXPECT_THAT(Sum(ArrayFromJSON(ty, "[]"),
                    ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
                ResultWith(zero));
    EXPECT_THAT(Sum(ArrayFromJSON(ty, "[null]"),
                    ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
                ResultWith(zero));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3)),
                ResultWith(result));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4)),
                ResultWith(result));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5)),
                ResultWith(null));
    EXPECT_THAT(Sum(ArrayFromJSON(ty, "[]"),
                    ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
                ResultWith(null));
    EXPECT_THAT(Sum(ArrayFromJSON(ty, "[null]"),
                    ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
                ResultWith(null));

    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
                ResultWith(null));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
                ResultWith(null));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
                ResultWith(null));

    arr = ArrayFromJSON(ty, R"(["1.01", "3.03", "3.03", "7.07"])");
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
                ResultWith(result));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
                ResultWith(result));
    EXPECT_THAT(Sum(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
                ResultWith(null));

    EXPECT_THAT(Sum(ScalarFromJSON(ty, R"("5.05")"),
                    ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(ScalarFromJSON(ty, R"("5.05")")));
    EXPECT_THAT(Sum(ScalarFromJSON(ty, R"("5.05")"),
                    ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
                ResultWith(null));
    EXPECT_THAT(Sum(null, ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(null));
  }
}

TEST(TestNullSumKernel, Basics) {
  auto ty = null();
  Datum null_result = std::make_shared<Int64Scalar>();
  Datum zero_result = std::make_shared<Int64Scalar>(0);

  EXPECT_THAT(Sum(ScalarFromJSON(ty, "null")), ResultWith(null_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[]")), ResultWith(null_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[null]")), ResultWith(null_result));
  EXPECT_THAT(Sum(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"})),
              ResultWith(null_result));

  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  EXPECT_THAT(Sum(ScalarFromJSON(ty, "null"), options), ResultWith(zero_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[]"), options), ResultWith(zero_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[null]"), options), ResultWith(zero_result));
  EXPECT_THAT(Sum(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
              ResultWith(zero_result));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0);
  EXPECT_THAT(Sum(ScalarFromJSON(ty, "null"), options), ResultWith(null_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[]"), options), ResultWith(zero_result));
  EXPECT_THAT(Sum(ArrayFromJSON(ty, "[null]"), options), ResultWith(null_result));
  EXPECT_THAT(Sum(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
              ResultWith(null_result));
}

//
// Product
//

template <typename ArrowType>
class TestNumericProductKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericProductKernel, NumericArrowTypes);
TYPED_TEST(TestNumericProductKernel, SimpleProduct) {
  using ProductType = typename FindAccumulatorType<TypeParam>::Type;
  using T = typename TypeParam::c_type;
  using ProductT = typename ProductType::c_type;

  Datum null_result(std::make_shared<typename TypeTraits<ProductType>::ScalarType>());

  auto ty = TypeTraits<TypeParam>::type_singleton();

  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]")), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]")), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[0, 1, 2, 3, 4, 5]")),
              ResultWith(Datum(static_cast<ProductT>(0))));
  Datum chunks = ChunkedArrayFromJSON(ty, {"[1, 2, 3, 4, 5]"});
  EXPECT_THAT(Product(chunks), ResultWith(Datum(static_cast<ProductT>(120))));
  chunks = ChunkedArrayFromJSON(ty, {"[1, 2]", "[3, 4, 5]"});
  EXPECT_THAT(Product(chunks), ResultWith(Datum(static_cast<ProductT>(120))));
  chunks = ChunkedArrayFromJSON(ty, {"[1, 2]", "[]", "[3, 4, 5]"});
  EXPECT_THAT(Product(chunks), ResultWith(Datum(static_cast<ProductT>(120))));

  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]"), options), Datum(static_cast<ProductT>(1)));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]"), options),
              Datum(static_cast<ProductT>(1)));
  chunks = ChunkedArrayFromJSON(ty, {});
  EXPECT_THAT(Product(chunks, options), Datum(static_cast<ProductT>(1)));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0);
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]"), options),
              ResultWith(Datum(static_cast<ProductT>(1))));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]"), options), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[1, null, 3, null, 3, null, 7]"), options),
              ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[1, null, 3, null, 3, null, 7]")),
              Datum(static_cast<ProductT>(63)));

  EXPECT_THAT(Product(Datum(static_cast<T>(5))),
              ResultWith(Datum(static_cast<ProductT>(5))));
  EXPECT_THAT(Product(MakeNullScalar(TypeTraits<TypeParam>::type_singleton())),
              ResultWith(null_result));
}

TYPED_TEST(TestNumericProductKernel, ScalarAggregateOptions) {
  using ProductType = typename FindAccumulatorType<TypeParam>::Type;
  using T = typename TypeParam::c_type;
  using ProductT = typename ProductType::c_type;

  Datum null_result(std::make_shared<typename TypeTraits<ProductType>::ScalarType>());
  Datum one_result(static_cast<ProductT>(1));
  Datum result(static_cast<ProductT>(63));

  auto ty = TypeTraits<TypeParam>::type_singleton();
  Datum empty = ArrayFromJSON(ty, "[]");
  Datum null = ArrayFromJSON(ty, "[null]");
  Datum arr = ArrayFromJSON(ty, "[1, null, 3, null, 3, null, 7]");

  EXPECT_THAT(
      Product(empty, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
      ResultWith(one_result));
  EXPECT_THAT(Product(null, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
              ResultWith(one_result));
  EXPECT_THAT(Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3)),
              ResultWith(result));
  EXPECT_THAT(Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4)),
              ResultWith(result));
  EXPECT_THAT(Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5)),
              ResultWith(null_result));
  EXPECT_THAT(
      Product(empty, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
      ResultWith(null_result));
  EXPECT_THAT(Product(null, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
              ResultWith(null_result));
  EXPECT_THAT(
      Product(empty, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
      ResultWith(one_result));
  EXPECT_THAT(Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
              ResultWith(null_result));

  EXPECT_THAT(
      Product(Datum(static_cast<T>(5)), ScalarAggregateOptions(/*skip_nulls=*/false)),
      ResultWith(Datum(static_cast<ProductT>(5))));
  EXPECT_THAT(Product(Datum(static_cast<T>(5)),
                      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
              ResultWith(null_result));
  EXPECT_THAT(Product(MakeNullScalar(TypeTraits<TypeParam>::type_singleton()),
                      ScalarAggregateOptions(/*skip_nulls=*/false)),
              ResultWith(null_result));
}

TEST(TestDecimalProductKernel, SimpleProduct) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    Datum null = ScalarFromJSON(ty, R"(null)");

    EXPECT_THAT(Product(ArrayFromJSON(ty, R"([])")), ResultWith(null));
    EXPECT_THAT(Product(ArrayFromJSON(ty, R"([null])")), ResultWith(null));
    EXPECT_THAT(
        Product(ArrayFromJSON(ty, R"(["0.00", "1.00", "2.00", "3.00", "4.00", "5.00"])")),
        ResultWith(ScalarFromJSON(ty, R"("0.00")")));
    Datum chunks =
        ChunkedArrayFromJSON(ty, {R"(["1.00", "2.00", "3.00", "4.00", "5.00"])"});
    EXPECT_THAT(Product(chunks), ResultWith(ScalarFromJSON(ty, R"("120.00")")));
    chunks =
        ChunkedArrayFromJSON(ty, {R"(["1.00", "2.00"])", R"(["-3.00", "4.00", "5.00"])"});
    EXPECT_THAT(Product(chunks), ResultWith(ScalarFromJSON(ty, R"("-120.00")")));
    chunks = ChunkedArrayFromJSON(
        ty, {R"(["1.00", "2.00"])", R"([])", R"(["-3.00", "4.00", "-5.00"])"});
    EXPECT_THAT(Product(chunks), ResultWith(ScalarFromJSON(ty, R"("120.00")")));

    const ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);

    EXPECT_THAT(Product(ArrayFromJSON(ty, R"([])"), options),
                ResultWith(ScalarFromJSON(ty, R"("1.00")")));
    EXPECT_THAT(Product(ArrayFromJSON(ty, R"([null])"), options),
                ResultWith(ScalarFromJSON(ty, R"("1.00")")));
    chunks = ChunkedArrayFromJSON(ty, {});
    EXPECT_THAT(Product(chunks, options), ResultWith(ScalarFromJSON(ty, R"("1.00")")));

    EXPECT_THAT(Product(ArrayFromJSON(
                            ty, R"(["1.00", null, "-3.00", null, "3.00", null, "7.00"])"),
                        options),
                ResultWith(ScalarFromJSON(ty, R"("-63.00")")));

    EXPECT_THAT(Product(ScalarFromJSON(ty, R"("5.00")")),
                ResultWith(ScalarFromJSON(ty, R"("5.00")")));
    EXPECT_THAT(Product(null), ResultWith(null));
  }
}

TEST(TestDecimalProductKernel, ScalarAggregateOptions) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    Datum null = ScalarFromJSON(ty, R"(null)");
    Datum one = ScalarFromJSON(ty, R"("1.00")");
    Datum result = ScalarFromJSON(ty, R"("63.00")");

    Datum empty = ArrayFromJSON(ty, R"([])");
    Datum null_arr = ArrayFromJSON(ty, R"([null])");
    Datum arr =
        ArrayFromJSON(ty, R"(["1.00", null, "3.00", null, "3.00", null, "7.00"])");

    EXPECT_THAT(
        Product(empty, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
        ResultWith(one));
    EXPECT_THAT(
        Product(null_arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
        ResultWith(one));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3)),
        ResultWith(result));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4)),
        ResultWith(result));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5)),
        ResultWith(null));
    EXPECT_THAT(
        Product(empty, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
        ResultWith(null));
    EXPECT_THAT(
        Product(null_arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
        ResultWith(null));

    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
        ResultWith(null));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
        ResultWith(null));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
        ResultWith(null));

    arr = ArrayFromJSON(ty, R"(["1.00", "3.00", "3.00", "7.00"])");
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
        ResultWith(result));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
        ResultWith(result));
    EXPECT_THAT(
        Product(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
        ResultWith(null));

    EXPECT_THAT(Product(ScalarFromJSON(ty, R"("5.00")"),
                        ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(ScalarFromJSON(ty, R"("5.00")")));
    EXPECT_THAT(Product(ScalarFromJSON(ty, R"("5.00")"),
                        ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
                ResultWith(null));
    EXPECT_THAT(Product(null, ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(null));
  }
}

TEST(TestProductKernel, Overflow) {
  EXPECT_THAT(Product(ArrayFromJSON(int64(), "[8589934592, 8589934593]")),
              ResultWith(Datum(static_cast<int64_t>(8589934592))));
}

TEST(TestNullProductKernel, Basics) {
  auto ty = null();
  Datum null_result = std::make_shared<Int64Scalar>();
  Datum one_result = std::make_shared<Int64Scalar>(1);

  EXPECT_THAT(Product(ScalarFromJSON(ty, "null")), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]")), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]")), ResultWith(null_result));
  EXPECT_THAT(Product(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"})),
              ResultWith(null_result));

  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  EXPECT_THAT(Product(ScalarFromJSON(ty, "null"), options), ResultWith(one_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]"), options), ResultWith(one_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]"), options), ResultWith(one_result));
  EXPECT_THAT(
      Product(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
      ResultWith(one_result));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0);
  EXPECT_THAT(Product(ScalarFromJSON(ty, "null"), options), ResultWith(null_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[]"), options), ResultWith(one_result));
  EXPECT_THAT(Product(ArrayFromJSON(ty, "[null]"), options), ResultWith(null_result));
  EXPECT_THAT(
      Product(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
      ResultWith(null_result));
}

//
// Count
//

using CountPair = std::pair<int64_t, int64_t>;

static CountPair NaiveCount(const Array& array) {
  CountPair count;

  count.first = array.length() - array.null_count();
  count.second = array.null_count();

  return count;
}

void ValidateCount(const Array& input, CountPair expected) {
  CountOptions non_null;
  CountOptions nulls(CountOptions::ONLY_NULL);
  CountOptions all(CountOptions::ALL);

  ASSERT_OK_AND_ASSIGN(Datum result, Count(input, non_null));
  AssertDatumsEqual(result, Datum(expected.first));

  ASSERT_OK_AND_ASSIGN(result, Count(input, nulls));
  AssertDatumsEqual(result, Datum(expected.second));

  ASSERT_OK_AND_ASSIGN(result, Count(input, all));
  AssertDatumsEqual(result, Datum(expected.first + expected.second));
}

template <typename ArrowType>
void ValidateCount(const char* json, CountPair expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateCount(*array, expected);
}

void ValidateCount(const Array& input) { ValidateCount(input, NaiveCount(input)); }

template <typename ArrowType>
class TestCountKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestCountKernel, NumericArrowTypes);
TYPED_TEST(TestCountKernel, SimpleCount) {
  ValidateCount<TypeParam>("[]", {0, 0});
  ValidateCount<TypeParam>("[null]", {0, 1});
  ValidateCount<TypeParam>("[1, null, 2]", {2, 1});
  ValidateCount<TypeParam>("[null, null, null]", {0, 3});
  ValidateCount<TypeParam>("[1, 2, 3, 4, 5, 6, 7, 8, 9]", {9, 0});

  auto ty = TypeTraits<TypeParam>::type_singleton();
  EXPECT_THAT(Count(MakeNullScalar(ty)), ResultWith(Datum(int64_t(0))));
  EXPECT_THAT(Count(MakeNullScalar(ty), CountOptions(CountOptions::ONLY_NULL)),
              ResultWith(Datum(int64_t(1))));
  EXPECT_THAT(Count(*MakeScalar(ty, 1)), ResultWith(Datum(int64_t(1))));
  EXPECT_THAT(Count(*MakeScalar(ty, 1), CountOptions(CountOptions::ONLY_NULL)),
              ResultWith(Datum(int64_t(0))));

  CountOptions all(CountOptions::ALL);
  EXPECT_THAT(Count(MakeNullScalar(ty), all), ResultWith(Datum(int64_t(1))));
  EXPECT_THAT(Count(*MakeScalar(ty, 1), all), ResultWith(Datum(int64_t(1))));
}

template <typename ArrowType>
class TestRandomNumericCountKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericCountKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericCountKernel, RandomArrayCount) {
  auto rand = random::RandomArrayGenerator(0x1205643);
  for (size_t i = 3; i < 10; i++) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.25, 0.5, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateCount(*array);
      }
    }
  }
}

//
// Count Distinct
//

class TestCountDistinctKernel : public ::testing::Test {
 protected:
  Datum Expected(int64_t value) { return MakeScalar(static_cast<int64_t>(value)); }

  void Check(Datum input, int64_t expected_all, bool has_nulls = true) {
    int64_t expected_valid = has_nulls ? expected_all - 1 : expected_all;
    int64_t expected_null = has_nulls ? 1 : 0;
    CheckScalar("count_distinct", {input}, Expected(expected_valid), &only_valid);
    CheckScalar("count_distinct", {input}, Expected(expected_null), &only_null);
    CheckScalar("count_distinct", {input}, Expected(expected_all), &all);
  }

  void Check(const std::shared_ptr<DataType>& type, std::string_view json,
             int64_t expected_all, bool has_nulls = true) {
    Check(ArrayFromJSON(type, json), expected_all, has_nulls);
  }

  void Check(const std::shared_ptr<DataType>& type, std::string_view json) {
    auto input = ScalarFromJSON(type, json);
    auto zero = ResultWith(Expected(0));
    auto one = ResultWith(Expected(1));
    // non null scalar
    EXPECT_THAT(CallFunction("count_distinct", {input}, &only_valid), one);
    EXPECT_THAT(CallFunction("count_distinct", {input}, &only_null), zero);
    EXPECT_THAT(CallFunction("count_distinct", {input}, &all), one);
    // null scalar
    input = MakeNullScalar(input->type);
    EXPECT_THAT(CallFunction("count_distinct", {input}, &only_valid), zero);
    EXPECT_THAT(CallFunction("count_distinct", {input}, &only_null), one);
    EXPECT_THAT(CallFunction("count_distinct", {input}, &all), one);
  }

  void CheckChunkedArr(const std::shared_ptr<DataType>& type,
                       const std::vector<std::string>& json, int64_t expected_all,
                       bool has_nulls = true) {
    Check(ChunkedArrayFromJSON(type, json), expected_all, has_nulls);
  }

  CountOptions only_valid{CountOptions::ONLY_VALID};
  CountOptions only_null{CountOptions::ONLY_NULL};
  CountOptions all{CountOptions::ALL};
};

TEST_F(TestCountDistinctKernel, AllChunkedArrayTypesWithNulls) {
  // Boolean
  CheckChunkedArr(boolean(), {"[]", "[]"}, 0, /*has_nulls=*/false);
  CheckChunkedArr(boolean(), {"[true, null]", "[false, null, false]", "[true]"}, 3);

  // Number
  for (auto ty : NumericTypes()) {
    CheckChunkedArr(ty, {"[1, 1, null, 2]", "[5, 8, 9, 9, null, 10]", "[6, 6, 8, 9, 10]"},
                    8);
    CheckChunkedArr(ty, {"[1, 1, 8, 2]", "[5, 8, 9, 9, 10]", "[10, 6, 6]"}, 7,
                    /*has_nulls=*/false);
  }

  // Date
  CheckChunkedArr(date32(), {"[0, 11016]", "[0, null, 14241, 14241, null]"}, 4);
  CheckChunkedArr(date64(), {"[0, null]", "[0, null, 0, 0, 1262217600000]"}, 3);

  // Time
  CheckChunkedArr(time32(TimeUnit::SECOND), {"[ 0, 11, 0, null]", "[14, 14, null]"}, 4);
  CheckChunkedArr(time32(TimeUnit::MILLI), {"[ 0, 11000, 0]", "[null, 11000, 11000]"}, 3);

  CheckChunkedArr(time64(TimeUnit::MICRO), {"[84203999999, 0, null, 84203999999]", "[0]"},
                  3);
  CheckChunkedArr(time64(TimeUnit::NANO),
                  {"[11715003000000, 0, null, 0, 0]", "[0, 0, null]"}, 3);

  // Timestamp & Duration
  for (auto u : TimeUnit::values()) {
    CheckChunkedArr(duration(u), {"[123456789, null, 987654321]", "[123456789, null]"},
                    3);

    CheckChunkedArr(duration(u),
                    {"[123456789, 987654321, 123456789, 123456789]", "[123456789]"}, 2,
                    /*has_nulls=*/false);

    auto ts =
        std::vector<std::string>{R"(["2009-12-31T04:20:20", "2009-12-31T04:20:20"])",
                                 R"(["2020-01-01", null])", R"(["2020-01-01", null])"};
    CheckChunkedArr(timestamp(u), ts, 3);
    CheckChunkedArr(timestamp(u, "Pacific/Marquesas"), ts, 3);
  }

  // Interval
  CheckChunkedArr(month_interval(), {"[9012, 5678, null, 9012]", "[5678, null, 9012]"},
                  3);
  CheckChunkedArr(day_time_interval(),
                  {"[[0, 1], [0, 1]]", "[null, [0, 1], [1234, 5678]]"}, 3);
  CheckChunkedArr(month_day_nano_interval(),
                  {"[[0, 1, 2]]", "[[0, 1, 2], null, [0, 1, 2]]"}, 2);

  // Binary & String & Fixed binary
  auto samples = std::vector<std::string>{
      R"([null, "abc", null])", R"(["abc", "abc", "cba"])", R"(["bca", "cba", null])"};

  CheckChunkedArr(binary(), samples, 4);
  CheckChunkedArr(large_binary(), samples, 4);
  CheckChunkedArr(utf8(), samples, 4);
  CheckChunkedArr(large_utf8(), samples, 4);
  CheckChunkedArr(fixed_size_binary(3), samples, 4);

  // Decimal
  samples = {R"(["12345.679", "98765.421"])", R"([null, "12345.679", "98765.421"])"};
  CheckChunkedArr(decimal128(21, 3), samples, 3);
  CheckChunkedArr(decimal256(13, 3), samples, 3);
}

TEST_F(TestCountDistinctKernel, AllArrayTypesWithNulls) {
  // Boolean
  Check(boolean(), "[]", 0, /*has_nulls=*/false);
  Check(boolean(), "[true, null, false, null, false, true]", 3);
  // Number
  for (auto ty : NumericTypes()) {
    Check(ty, "[1, 1, null, 2, 5, 8, 9, 9, null, 10, 6, 6]", 8);
    Check(ty, "[1, 1, 8, 2, 5, 8, 9, 9, 10, 10, 6, 6]", 7, /*has_nulls=*/false);
  }
  // Date
  Check(date32(), "[0, 11016, 0, null, 14241, 14241, null]", 4);
  Check(date64(), "[0, null, 0, null, 0, 0, 1262217600000]", 3);
  // Time
  Check(time32(TimeUnit::SECOND), "[0, 11, 0, null, 14, 14, null]", 4);
  Check(time32(TimeUnit::MILLI), "[0, 11000, 0, null, 11000, 11000]", 3);
  Check(time64(TimeUnit::MICRO), "[84203999999,  0, null, 84203999999, 0]", 3);
  Check(time64(TimeUnit::NANO), "[11715003000000,  0, null, 0, 0]", 3);
  // Timestamp & Duration
  for (auto u : TimeUnit::values()) {
    Check(duration(u), "[123456789, null, 987654321, 123456789, null]", 3);
    Check(duration(u), "[123456789, 987654321, 123456789, 123456789]", 2,
          /*has_nulls=*/false);
    auto ts = R"(["2009-12-31T04:20:20", "2020-01-01", null, "2009-12-31T04:20:20"])";
    Check(timestamp(u), ts, 3);
    Check(timestamp(u, "Pacific/Marquesas"), ts, 3);
  }
  // Interval
  Check(month_interval(), "[9012, 5678, null, 9012, 5678, null, 9012]", 3);
  Check(day_time_interval(), "[[0, 1], [0, 1], null, [0, 1], [1234, 5678]]", 3);
  Check(month_day_nano_interval(), "[[0, 1, 2], [0, 1, 2], null, [0, 1, 2]]", 2);
  // Binary & String & Fixed binary
  auto samples = R"([null, "abc", null, "abc", "abc", "cba", "bca", "cba", null])";
  Check(binary(), samples, 4);
  Check(large_binary(), samples, 4);
  Check(utf8(), samples, 4);
  Check(large_utf8(), samples, 4);
  Check(fixed_size_binary(3), samples, 4);
  // Decimal
  samples = R"(["12345.679", "98765.421", null, "12345.679", "98765.421"])";
  Check(decimal128(21, 3), samples, 3);
  Check(decimal256(13, 3), samples, 3);
}

TEST_F(TestCountDistinctKernel, AllScalarTypesWithNulls) {
  // Boolean
  Check(boolean(), "true");
  // Number
  for (auto ty : NumericTypes()) {
    Check(ty, "91");
  }
  // Date
  Check(date32(), "11016");
  Check(date64(), "1262217600000");
  // Time
  Check(time32(TimeUnit::SECOND), "14");
  Check(time32(TimeUnit::MILLI), "11000");
  Check(time64(TimeUnit::MICRO), "84203999999");
  Check(time64(TimeUnit::NANO), "11715003000000");
  // Timestamp & Duration
  for (auto u : TimeUnit::values()) {
    Check(duration(u), "987654321");
    Check(duration(u), "123456789");
    auto ts = R"("2009-12-31T04:20:20")";
    Check(timestamp(u), ts);
    Check(timestamp(u, "Pacific/Marquesas"), ts);
  }
  // Interval
  Check(month_interval(), "5678");
  Check(day_time_interval(), "[1234, 5678]");
  Check(month_day_nano_interval(), "[0, 1, 2]");
  // Binary & String & Fixed binary
  auto sample = R"("cba")";
  Check(binary(), sample);
  Check(large_binary(), sample);
  Check(utf8(), sample);
  Check(large_utf8(), sample);
  Check(fixed_size_binary(3), sample);
  // Decimal
  sample = R"("98765.421")";
  Check(decimal128(21, 3), sample);
  Check(decimal256(13, 3), sample);
}

TEST_F(TestCountDistinctKernel, Random) {
  UInt32Builder builder;
  std::unordered_set<uint32_t> memo;
  auto visit_null = []() { return Status::OK(); };
  auto visit_value = [&](uint32_t arg) {
    const bool inserted = memo.insert(arg).second;
    if (inserted) {
      return builder.Append(arg);
    }
    return Status::OK();
  };
  auto rand = random::RandomArrayGenerator(0x1205643);
  auto arr = rand.Numeric<UInt32Type>(1024, 0, 100, 0.0)->data();
  auto r = VisitArraySpanInline<UInt32Type>(*arr, visit_value, visit_null);
  auto input = builder.Finish().ValueOrDie();
  Check(input, memo.size(), false);
}

//
// Mean
//

template <typename ArrowType>
static Datum NaiveMean(const Array& array) {
  using MeanScalarType = typename TypeTraits<DoubleType>::ScalarType;

  const auto result = NaiveSumPartial<ArrowType>(array);
  const double mean = static_cast<double>(result.first) /
                      static_cast<double>(result.second ? result.second : 1UL);
  const bool is_valid = result.second > 0;

  if (!is_valid) return Datum(std::make_shared<MeanScalarType>());
  return Datum(std::make_shared<MeanScalarType>(mean));
}

template <typename ArrowType>
void ValidateMean(const Array& input, Datum expected,
                  const ScalarAggregateOptions& options) {
  ASSERT_OK_AND_ASSIGN(Datum result, Mean(input, options, nullptr));
  AssertDatumsApproxEqual(expected, result, /*verbose=*/true);
}

template <typename ArrowType>
void ValidateMean(
    const std::string& json, Datum expected,
    const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateMean<ArrowType>(*array, expected, options);
}

template <typename ArrowType>
void ValidateMean(const Array& array, const ScalarAggregateOptions& options =
                                          ScalarAggregateOptions::Defaults()) {
  ValidateMean<ArrowType>(array, NaiveMean<ArrowType>(array), options);
}

template <typename ArrowType>
class TestNumericMeanKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericMeanKernel, NumericArrowTypes);
TYPED_TEST(TestNumericMeanKernel, SimpleMean) {
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;
  using InputScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using T = typename TypeParam::c_type;

  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);

  ValidateMean<TypeParam>("[]", Datum(std::make_shared<ScalarType>(NAN)), options);

  ValidateMean<TypeParam>("[null]", Datum(std::make_shared<ScalarType>(NAN)), options);

  ValidateMean<TypeParam>("[]", Datum(std::make_shared<ScalarType>()));

  ValidateMean<TypeParam>("[null]", Datum(std::make_shared<ScalarType>()));

  ValidateMean<TypeParam>("[1, null, 1]", Datum(std::make_shared<ScalarType>(1.0)));

  ValidateMean<TypeParam>("[1, 2, 3, 4, 5, 6, 7, 8]",
                          Datum(std::make_shared<ScalarType>(4.5)));

  ValidateMean<TypeParam>("[0, 0, 0, 0, 0, 0, 0, 0]",
                          Datum(std::make_shared<ScalarType>(0.0)));

  ValidateMean<TypeParam>("[1, 1, 1, 1, 1, 1, 1, 1]",
                          Datum(std::make_shared<ScalarType>(1.0)));

  EXPECT_THAT(Mean(Datum(std::make_shared<InputScalarType>(static_cast<T>(5)))),
              ResultWith(Datum(std::make_shared<ScalarType>(5.0))));
  EXPECT_THAT(Mean(MakeNullScalar(TypeTraits<TypeParam>::type_singleton())),
              ResultWith(Datum(MakeNullScalar(float64()))));
}

TYPED_TEST(TestNumericMeanKernel, ScalarAggregateOptions) {
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;
  using InputScalarType = typename TypeTraits<TypeParam>::ScalarType;
  using T = typename TypeParam::c_type;
  auto expected_result = Datum(std::make_shared<ScalarType>(3));
  auto null_result = Datum(std::make_shared<ScalarType>());
  auto nan_result = Datum(std::make_shared<ScalarType>(NAN));
  std::string json = "[1, null, 2, 2, null, 7]";

  ValidateMean<TypeParam>("[]", nan_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0));
  ValidateMean<TypeParam>("[null]", nan_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0));
  ValidateMean<TypeParam>("[]", null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateMean<TypeParam>("[null]", null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1));
  ValidateMean<TypeParam>(json, expected_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0));
  ValidateMean<TypeParam>(json, expected_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3));
  ValidateMean<TypeParam>(json, expected_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4));
  ValidateMean<TypeParam>(json, null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5));

  ValidateMean<TypeParam>("[]", nan_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateMean<TypeParam>("[null]", null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));
  ValidateMean<TypeParam>(json, null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0));

  ValidateMean<TypeParam>("[]", null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
  ValidateMean<TypeParam>("[null]", null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
  ValidateMean<TypeParam>(json, null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));

  json = "[1, 2, 2, 7]";
  ValidateMean<TypeParam>(json, expected_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1));
  ValidateMean<TypeParam>(json, expected_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4));
  ValidateMean<TypeParam>(json, null_result,
                          ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5));

  EXPECT_THAT(Mean(Datum(std::make_shared<InputScalarType>(static_cast<T>(5))),
                   ScalarAggregateOptions(/*skip_nulls=*/false)),
              ResultWith(Datum(std::make_shared<ScalarType>(5.0))));
  EXPECT_THAT(Mean(Datum(std::make_shared<InputScalarType>(static_cast<T>(5))),
                   ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
              ResultWith(Datum(MakeNullScalar(float64()))));
  EXPECT_THAT(Mean(MakeNullScalar(TypeTraits<TypeParam>::type_singleton()),
                   ScalarAggregateOptions(/*skip_nulls=*/false)),
              ResultWith(Datum(MakeNullScalar(float64()))));
}

template <typename ArrowType>
class TestRandomNumericMeanKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericMeanKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericMeanKernel, RandomArrayMean) {
  auto rand = random::RandomArrayGenerator(0x8afc055);
  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  // Test size up to 1<<13 (8192).
  for (size_t i = 3; i < 14; i += 2) {
    for (auto null_probability : {0.0, 0.001, 0.1, 0.5, 0.999, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateMean<TypeParam>(*array, options);
      }
    }
  }
}

TYPED_TEST_SUITE(TestRandomNumericMeanKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericMeanKernel, RandomArrayMeanOverflow) {
  using CType = typename TypeParam::c_type;
  using SumCType = typename FindAccumulatorType<TypeParam>::Type::c_type;
  if (sizeof(CType) == sizeof(SumCType)) {
    // Skip if accumulator type is same to original type
    return;
  }

  CType max = std::numeric_limits<CType>::max();
  CType min = std::numeric_limits<CType>::min();
  int64_t length = 1024;

  auto rand = random::RandomArrayGenerator(0x8afc055);
  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Test overflow on the original type
    auto array = rand.Numeric<TypeParam>(length, max - 200, max - 100, null_probability);
    ValidateMean<TypeParam>(*array, options);
    array = rand.Numeric<TypeParam>(length, min + 100, min + 200, null_probability);
    ValidateMean<TypeParam>(*array, options);
  }
}

TEST(TestDecimalMeanKernel, SimpleMean) {
  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);

  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    // Decimal doesn't have NaN
    EXPECT_THAT(Mean(ArrayFromJSON(ty, R"([])"), options),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
    EXPECT_THAT(Mean(ArrayFromJSON(ty, R"([null])"), options),
                ResultWith(ScalarFromJSON(ty, R"(null)")));

    EXPECT_THAT(Mean(ArrayFromJSON(ty, R"([])")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
    EXPECT_THAT(Mean(ArrayFromJSON(ty, R"([null])")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));

    EXPECT_THAT(Mean(ArrayFromJSON(ty, R"(["1.01", null, "1.01"])")),
                ResultWith(ScalarFromJSON(ty, R"("1.01")")));

    // Check rounding
    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty, R"(["1.01", "2.02", "3.03", "4.04", "5.05", "6.06", "7.07", "8.08"])")),
        // 4.545 unrounded
        ResultWith(ScalarFromJSON(ty, R"("4.55")")));
    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty,
            R"(["-1.01", "-2.02", "-3.03", "-4.04", "-5.05", "-6.06", "-7.07", "-8.08"])")),
        // -4.545 unrounded
        ResultWith(ScalarFromJSON(ty, R"("-4.55")")));
    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty, R"(["1.01", "2.02", "3.00", "4.04", "5.05", "6.06", "7.07", "8.08"])")),
        // 4.54125 unrounded
        ResultWith(ScalarFromJSON(ty, R"("4.54")")));
    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty,
            R"(["-1.01", "-2.02", "-3.00", "-4.04", "-5.05", "-6.06", "-7.07", "-8.08"])")),
        // -4.54125 unrounded
        ResultWith(ScalarFromJSON(ty, R"("-4.54")")));

    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty, R"(["0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00", "0.00"])")),
        ResultWith(ScalarFromJSON(ty, R"("0.00")")));
    EXPECT_THAT(
        Mean(ArrayFromJSON(
            ty, R"(["1.01", "1.01", "1.01", "1.01", "1.01", "1.01", "1.01", "1.01"])")),
        ResultWith(ScalarFromJSON(ty, R"("1.01")")));

    EXPECT_THAT(Mean(ScalarFromJSON(ty, R"("5.05")")),
                ResultWith(ScalarFromJSON(ty, R"("5.05")")));
    EXPECT_THAT(Mean(ScalarFromJSON(ty, R"(null)")),
                ResultWith(ScalarFromJSON(ty, R"(null)")));
  }

  for (const auto& ty : {decimal128(3, -2), decimal256(3, -2)}) {
    // Check rounding
    EXPECT_THAT(
        Mean(DecimalArrayFromJSON(
            ty,
            R"(["101E2", "202E2", "303E2", "404E2", "505E2", "606E2", "707E2", "808E2"])")),
        // 45450 unrounded
        ResultWith(DecimalScalarFromJSON(ty, R"("455E2")")));
    EXPECT_THAT(
        Mean(DecimalArrayFromJSON(
            ty,
            R"(["-101E2", "-202E2", "-303E2", "-404E2", "-505E2", "-606E2", "-707E2", "-808E2"])")),
        // -45450 unrounded
        ResultWith(DecimalScalarFromJSON(ty, R"("-455E2")")));
    EXPECT_THAT(
        Mean(DecimalArrayFromJSON(
            ty,
            R"(["101E2", "202E2", "300E2", "404E2", "505E2", "606E2", "707E2", "808E2"])")),
        // 45412.5 unrounded
        ResultWith(DecimalScalarFromJSON(ty, R"("454E2")")));
    EXPECT_THAT(
        Mean(DecimalArrayFromJSON(
            ty,
            R"(["-101E2", "-202E2", "-300E2", "-404E2", "-505E2", "-606E2", "-707E2", "-808E2"])")),
        // -45412.5 unrounded
        ResultWith(DecimalScalarFromJSON(ty, R"("-454E2")")));
  }
}

TEST(TestDecimalMeanKernel, ScalarAggregateOptions) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    Datum result = ScalarFromJSON(ty, R"("3.03")");
    Datum null = ScalarFromJSON(ty, R"(null)");
    Datum arr = ArrayFromJSON(ty, R"(["1.01", null, "2.02", "2.02", null, "7.07"])");

    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"),
                     ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"),
                     ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"),
                     ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"),
                     ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1)),
                null);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0)),
                result);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/3)),
                result);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/4)),
                result);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5)),
                null);

    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"),
                     ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"),
                     ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"),
                     ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1)),
                null);
    EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"),
                     ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1)),
                null);

    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
                null);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
                null);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
                null);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
                null);

    arr = ArrayFromJSON(ty, R"(["1.01", "2.02", "2.02", "7.07"])");
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0)),
                result);
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/3)),
                ResultWith(result));
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4)),
                ResultWith(result));
    EXPECT_THAT(Mean(arr, ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5)),
                ResultWith(null));

    EXPECT_THAT(Mean(ScalarFromJSON(ty, R"("5.05")"),
                     ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(ScalarFromJSON(ty, R"("5.05")")));
    EXPECT_THAT(Mean(ScalarFromJSON(ty, R"("5.05")"),
                     ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/2)),
                ResultWith(null));
    EXPECT_THAT(Mean(null, ScalarAggregateOptions(/*skip_nulls=*/false)),
                ResultWith(null));
  }
}

TEST(TestNullMeanKernel, Basics) {
  auto ty = null();
  Datum null_result = std::make_shared<DoubleScalar>();
  Datum zero_result = std::make_shared<DoubleScalar>(0);

  EXPECT_THAT(Mean(ScalarFromJSON(ty, "null")), ResultWith(null_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]")), ResultWith(null_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]")), ResultWith(null_result));
  EXPECT_THAT(Mean(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"})),
              ResultWith(null_result));

  ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  EXPECT_THAT(Mean(ScalarFromJSON(ty, "null"), options), ResultWith(zero_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"), options), ResultWith(zero_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"), options), ResultWith(zero_result));
  EXPECT_THAT(Mean(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
              ResultWith(zero_result));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/0);
  EXPECT_THAT(Mean(ScalarFromJSON(ty, "null"), options), ResultWith(null_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[]"), options), ResultWith(zero_result));
  EXPECT_THAT(Mean(ArrayFromJSON(ty, "[null]"), options), ResultWith(null_result));
  EXPECT_THAT(Mean(ChunkedArrayFromJSON(ty, {"[null]", "[]", "[null, null]"}), options),
              ResultWith(null_result));
}

//
// First / Last
//

template <typename ArrowType>
class TestFirstLastKernel : public ::testing::Test {
  using Traits = TypeTraits<ArrowType>;
  using ArrayType = typename Traits::ArrayType;
  using c_type = typename ArrowType::c_type;
  using ScalarType = typename Traits::ScalarType;

 public:
  void AssertFirstLastIs(const Datum& array, c_type expected_first, c_type expected_last,
                         const ScalarAggregateOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, CallFunction("first", {array}, &options));
    const auto& out_first = out.scalar_as<ScalarType>();
    ASSERT_EQ(expected_first, out_first.value);

    ASSERT_OK_AND_ASSIGN(out, CallFunction("last", {array}, &options));
    const auto& out_last = out.scalar_as<ScalarType>();
    ASSERT_EQ(expected_last, out_last.value);
  }

  void AssertFirstLastIsNull(const Datum& array, const ScalarAggregateOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, First(array, options));
    const auto& out_first = out.scalar_as<ScalarType>();
    ASSERT_FALSE(out_first.is_valid);

    ASSERT_OK_AND_ASSIGN(out, Last(array, options));
    const auto& out_last = out.scalar_as<ScalarType>();
    ASSERT_FALSE(out_last.is_valid);
  }

  void AssertFirstLastIsNull(const std::string& json,
                             const ScalarAggregateOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertFirstLastIsNull(array, options);
  }

  void AssertFirstLastIsNull(const std::vector<std::string>& json,
                             const ScalarAggregateOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertFirstLastIsNull(array, options);
  }

  void AssertFirstLastIs(const std::string& json, c_type expected_first,
                         c_type expected_last, const ScalarAggregateOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertFirstLastIs(array, expected_first, expected_last, options);
  }

  void AssertFirstLastIs(const std::vector<std::string>& json, c_type expected_min,
                         c_type expected_max, const ScalarAggregateOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertFirstLastIs(array, expected_min, expected_max, options);
  }

  std::shared_ptr<DataType> type_singleton() {
    return default_type_instance<ArrowType>();
  }
};

class TestBooleanFirstLastKernel : public TestFirstLastKernel<BooleanType> {};

template <typename ArrowType>
class TestNumericFirstLastKernel : public TestFirstLastKernel<ArrowType> {};

template <typename ArrowType>
class TestTemporalFirstLastKernel : public TestFirstLastKernel<ArrowType> {};

TEST_F(TestBooleanFirstLastKernel, Basics) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input0 = {"[]", "[]"};
  std::vector<std::string> chunked_input1 = {"[null, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[null, true]", "[false, null]"};
  std::vector<std::string> chunked_input4 = {"[false, null]", "[null, true]"};
  auto ty = struct_({field("first", boolean()), field("last", boolean())});

  this->AssertFirstLastIsNull("[]", options);
  this->AssertFirstLastIsNull("[null, null, null]", options);
  this->AssertFirstLastIsNull(chunked_input0, options);
  this->AssertFirstLastIs(chunked_input1, true, true, options);
  this->AssertFirstLastIs(chunked_input2, false, false, options);
  this->AssertFirstLastIs(chunked_input3, true, false, options);
  this->AssertFirstLastIs(chunked_input4, false, true, options);

  options.skip_nulls = false;
  this->AssertFirstLastIsNull("[]", options);
  this->AssertFirstLastIsNull("[null, null, null]", options);
  this->AssertFirstLastIsNull(chunked_input0, options);
  this->AssertFirstLastIsNull(chunked_input1, options);
  this->AssertFirstLastIs(chunked_input2, false, false, options);
  this->AssertFirstLastIsNull(chunked_input3, options);
  this->AssertFirstLastIs(chunked_input4, false, true, options);
}

TYPED_TEST_SUITE(TestNumericFirstLastKernel, NumericArrowTypes);
TYPED_TEST(TestNumericFirstLastKernel, Basics) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 8, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[null, null, null, 7]",
                                             "[null, 8, null, 3, 4, null]"};
  std::vector<std::string> chunked_input3 = {"[null, null, null]", "[null, null]"};
  auto item_ty = default_type_instance<TypeParam>();

  this->AssertFirstLastIs("[5, 1, 2, 3, 4]", 5, 4, options);
  this->AssertFirstLastIs("[5, null, 2, 3, null]", 5, 3, options);
  this->AssertFirstLastIs(chunked_input1, 5, 4, options);
  this->AssertFirstLastIs(chunked_input1[1], 9, 4, options);
  this->AssertFirstLastIs(chunked_input2, 7, 4, options);
  this->AssertFirstLastIsNull(chunked_input3[0], options);
  this->AssertFirstLastIsNull(chunked_input3, options);

  options.skip_nulls = false;
  this->AssertFirstLastIsNull(chunked_input2[1], options);
  this->AssertFirstLastIsNull(chunked_input2, options);
}

TYPED_TEST_SUITE(TestTemporalFirstLastKernel, TemporalArrowTypes);
TYPED_TEST(TestTemporalFirstLastKernel, Basics) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 8, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[null, null, null, null]",
                                             "[null, 8, null, 3 ,4, null]"};
  auto item_ty = default_type_instance<TypeParam>();

  this->AssertFirstLastIs("[5, 1, 2, 3, 4]", 5, 4, options);
  this->AssertFirstLastIs("[5, null, 2, 3, null]", 5, 3, options);
  this->AssertFirstLastIs(chunked_input1, 5, 4, options);
  this->AssertFirstLastIs(chunked_input2, 8, 4, options);

  options.skip_nulls = false;
  this->AssertFirstLastIsNull(chunked_input2, options);
}

template <typename ArrowType>
class TestBaseBinaryFirstLastKernel : public ::testing::Test {};
TYPED_TEST_SUITE(TestBaseBinaryFirstLastKernel, BaseBinaryArrowTypes);
TYPED_TEST(TestBaseBinaryFirstLastKernel, Basics) {
  std::vector<std::string> chunked_input1 = {R"(["cc", "", "aa", "b", "c"])",
                                             R"(["d", "", null, "b", null])"};
  std::vector<std::string> chunked_input2 = {R"(["aa", null, "aa", "b", "c"])",
                                             R"(["d", "", "aa", "b", "bb"])"};
  std::vector<std::string> chunked_input3 = {R"(["bb", "", "aa", "b", null])",
                                             R"(["d", "", null, "b", "aa"])"};
  auto ty = std::make_shared<TypeParam>();
  Datum null = ScalarFromJSON(ty, R"(null)");

  // SKIP nulls by default
  EXPECT_THAT(First(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(First(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(First(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(ty, R"("cc")")));
  EXPECT_THAT(First(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(ty, R"("cc")")));
  EXPECT_THAT(First(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(ty, R"("bb")")));

  EXPECT_THAT(Last(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(Last(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(Last(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(ty, R"("c")")));
  EXPECT_THAT(Last(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(ty, R"("b")")));
  EXPECT_THAT(Last(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(ty, R"("aa")")));

  EXPECT_THAT(Last(MakeNullScalar(ty)), ResultWith(null));
}

TEST(TestFixedSizeBinaryFirstLastKernel, Basics) {
  auto ty = fixed_size_binary(2);
  std::vector<std::string> chunked_input1 = {R"(["cd", "aa", "ab", "bb", "cc"])",
                                             R"(["da", "aa", null, "bb", "bb"])"};
  std::vector<std::string> chunked_input2 = {R"([null, null, null, null, null])",
                                             R"(["dd", "aa", "ab", "bb", "aa"])"};
  std::vector<std::string> chunked_input3 = {R"(["aa", "aa", "ab", "bb", null])",
                                             R"([null, null, null, null, null])"};
  Datum null = ScalarFromJSON(ty, R"(null)");

  // SKIP nulls by default
  EXPECT_THAT(First(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(First(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(First(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(ty, R"("cd")")));
  EXPECT_THAT(First(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(ty, R"("cd")")));
  EXPECT_THAT(First(ChunkedArrayFromJSON(ty, chunked_input2)),
              ResultWith(ScalarFromJSON(ty, R"("dd")")));
  EXPECT_THAT(First(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(ty, R"("aa")")));

  EXPECT_THAT(Last(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(Last(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(Last(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(ty, R"("cc")")));
  EXPECT_THAT(Last(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(ty, R"("bb")")));
  EXPECT_THAT(Last(ChunkedArrayFromJSON(ty, chunked_input2)),
              ResultWith(ScalarFromJSON(ty, R"("aa")")));
  EXPECT_THAT(Last(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(ty, R"("bb")")));
}

//
// Min / Max
//

template <typename ArrowType>
class TestPrimitiveMinMaxKernel : public ::testing::Test {
  using Traits = TypeTraits<ArrowType>;
  using ArrayType = typename Traits::ArrayType;
  using c_type = typename ArrowType::c_type;
  using ScalarType = typename Traits::ScalarType;

 public:
  void AssertMinMaxIs(const Datum& array, c_type expected_min, c_type expected_max,
                      const ScalarAggregateOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array, options));
    const StructScalar& value = out.scalar_as<StructScalar>();

    {
      const auto& out_min = checked_cast<const ScalarType&>(*value.value[0]);
      ASSERT_EQ(expected_min, out_min.value);

      const auto& out_max = checked_cast<const ScalarType&>(*value.value[1]);
      ASSERT_EQ(expected_max, out_max.value);
    }

    {
      ASSERT_OK_AND_ASSIGN(out, CallFunction("min", {array}, &options));
      const auto& out_min = out.scalar_as<ScalarType>();
      ASSERT_EQ(expected_min, out_min.value);

      ASSERT_OK_AND_ASSIGN(out, CallFunction("max", {array}, &options));
      const auto& out_max = out.scalar_as<ScalarType>();
      ASSERT_EQ(expected_max, out_max.value);
    }
  }

  void AssertMinMaxIs(const std::string& json, c_type expected_min, c_type expected_max,
                      const ScalarAggregateOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertMinMaxIs(array, expected_min, expected_max, options);
  }

  void AssertMinMaxIs(const std::vector<std::string>& json, c_type expected_min,
                      c_type expected_max, const ScalarAggregateOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertMinMaxIs(array, expected_min, expected_max, options);
  }

  void AssertMinMaxIsNull(const Datum& array, const ScalarAggregateOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array, options));
    for (const auto& val : out.scalar_as<StructScalar>().value) {
      ASSERT_FALSE(val->is_valid);
    }
  }

  void AssertMinMaxIsNull(const std::string& json,
                          const ScalarAggregateOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertMinMaxIsNull(array, options);
  }

  void AssertMinMaxIsNull(const std::vector<std::string>& json,
                          const ScalarAggregateOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertMinMaxIsNull(array, options);
  }

  std::shared_ptr<DataType> type_singleton() {
    return default_type_instance<ArrowType>();
  }
};

template <typename ArrowType>
class TestIntegerMinMaxKernel : public TestPrimitiveMinMaxKernel<ArrowType> {};

template <typename ArrowType>
class TestFloatingMinMaxKernel : public TestPrimitiveMinMaxKernel<ArrowType> {};

class TestBooleanMinMaxKernel : public TestPrimitiveMinMaxKernel<BooleanType> {};

TEST_F(TestBooleanMinMaxKernel, Basics) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input0 = {"[]", "[]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[true, null]", "[null, false]"};
  auto ty = struct_({field("min", boolean()), field("max", boolean())});

  // SKIP nulls by default
  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null, null, null]", options);
  this->AssertMinMaxIs("[false, false, false]", false, false, options);
  this->AssertMinMaxIs("[false, false, false, null]", false, false, options);
  this->AssertMinMaxIs("[true, null, true, true]", true, true, options);
  this->AssertMinMaxIs("[true, null, true, true]", true, true, options);
  this->AssertMinMaxIs("[true, null, false, true]", false, true, options);
  this->AssertMinMaxIsNull(chunked_input0, options);
  this->AssertMinMaxIs(chunked_input1, true, true, options);
  this->AssertMinMaxIs(chunked_input2, false, false, options);
  this->AssertMinMaxIs(chunked_input3, false, true, options);

  Datum null_min_max = ScalarFromJSON(ty, "[null, null]");
  Datum true_min_max = ScalarFromJSON(ty, "[true, true]");
  Datum false_min_max = ScalarFromJSON(ty, "[false, false]");
  EXPECT_THAT(MinMax(MakeNullScalar(boolean())), ResultWith(null_min_max));
  EXPECT_THAT(MinMax(MakeScalar(true)), ResultWith(true_min_max));
  EXPECT_THAT(MinMax(MakeScalar(false)), ResultWith(false_min_max));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1);
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null, null, null]", options);
  this->AssertMinMaxIsNull("[false, null, false]", options);
  this->AssertMinMaxIsNull("[true, null]", options);
  this->AssertMinMaxIs("[true, true, true]", true, true, options);
  this->AssertMinMaxIs("[false, false]", false, false, options);
  this->AssertMinMaxIs("[false, true]", false, true, options);
  this->AssertMinMaxIsNull(chunked_input0, options);
  this->AssertMinMaxIsNull(chunked_input1, options);
  this->AssertMinMaxIs(chunked_input2, false, false, options);
  this->AssertMinMaxIsNull(chunked_input3, options);

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/2);
  EXPECT_THAT(MinMax(MakeNullScalar(boolean()), options), ResultWith(null_min_max));
  EXPECT_THAT(MinMax(MakeScalar(true), options), ResultWith(null_min_max));

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null]", options);
}

TYPED_TEST_SUITE(TestIntegerMinMaxKernel, PhysicalIntegralArrowTypes);
TYPED_TEST(TestIntegerMinMaxKernel, Basics) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 8, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[5, null, 2, 3, 4]", "[9, 1, 2, 3, 4]"};
  std::vector<std::string> chunked_input3 = {"[5, 1, 2, 3, null]", "[9, 1, null, 3, 4]"};
  auto item_ty = default_type_instance<TypeParam>();
  auto ty = struct_({field("min", item_ty), field("max", item_ty)});

  // SKIP nulls by default
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null, null, null]", options);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, null, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);
  this->AssertMinMaxIs(chunked_input2, 1, 9, options);
  this->AssertMinMaxIs(chunked_input3, 1, 9, options);

  Datum null_min_max(std::make_shared<StructScalar>(
      ScalarVector{MakeNullScalar(item_ty), MakeNullScalar(item_ty)}, ty));
  auto one_scalar = *MakeScalar(item_ty, static_cast<typename TypeParam::c_type>(1));
  Datum one_min_max(
      std::make_shared<StructScalar>(ScalarVector{one_scalar, one_scalar}, ty));
  EXPECT_THAT(MinMax(MakeNullScalar(item_ty)), ResultWith(null_min_max));
  EXPECT_THAT(MinMax(one_scalar), ResultWith(one_min_max));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIsNull(chunked_input1, options);
  this->AssertMinMaxIsNull(chunked_input2, options);
  this->AssertMinMaxIsNull(chunked_input3, options);

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIsNull(chunked_input1, options);
}

TYPED_TEST_SUITE(TestFloatingMinMaxKernel, RealArrowTypes);
TYPED_TEST(TestFloatingMinMaxKernel, Floats) {
  ScalarAggregateOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 1, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[5, null, 2, 3, 4]", "[9, 1, 2, 3, 4]"};
  std::vector<std::string> chunked_input3 = {"[5, 1, 2, 3, null]", "[9, 1, null, 3, 4]"};
  auto item_ty = TypeTraits<TypeParam>::type_singleton();
  auto ty = struct_({field("min", item_ty), field("max", item_ty)});

  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, null, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs("[5, Inf, 2, 3, 4]", 2.0, INFINITY, options);
  this->AssertMinMaxIs("[5, NaN, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs("[5, -Inf, 2, 3, 4]", -INFINITY, 5, options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);
  this->AssertMinMaxIs(chunked_input2, 1, 9, options);
  this->AssertMinMaxIs(chunked_input3, 1, 9, options);

  Datum null_min_max(std::make_shared<StructScalar>(
      ScalarVector{MakeNullScalar(item_ty), MakeNullScalar(item_ty)}, ty));
  auto one_scalar = *MakeScalar(item_ty, static_cast<typename TypeParam::c_type>(1));
  Datum one_min_max(
      std::make_shared<StructScalar>(ScalarVector{one_scalar, one_scalar}, ty));
  EXPECT_THAT(MinMax(MakeNullScalar(item_ty)), ResultWith(null_min_max));
  EXPECT_THAT(MinMax(one_scalar), ResultWith(one_min_max));

  options = ScalarAggregateOptions(/*skip_nulls=*/false);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, -Inf, 2, 3, 4]", -INFINITY, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIsNull("[5, -Inf, null, 3, 4]", options);
  this->AssertMinMaxIsNull(chunked_input1, options);
  this->AssertMinMaxIsNull(chunked_input2, options);
  this->AssertMinMaxIsNull(chunked_input3, options);

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null]", options);

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1);
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null]", options);

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/5);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  this->AssertMinMaxIsNull(chunked_input1, options);
}

TYPED_TEST(TestFloatingMinMaxKernel, DefaultOptions) {
  auto values = ArrayFromJSON(this->type_singleton(), "[0, 1, 2, 3, 4]");

  ASSERT_OK_AND_ASSIGN(auto no_options_provided, CallFunction("min_max", {values}));

  auto default_options = ScalarAggregateOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto explicit_defaults,
                       CallFunction("min_max", {values}, &default_options));

  AssertDatumsEqual(explicit_defaults, no_options_provided);
}

TEST(TestDecimalMinMaxKernel, Decimals) {
  for (const auto& item_ty : {decimal128(5, 2), decimal256(5, 2)}) {
    auto ty = struct_({field("min", item_ty), field("max", item_ty)});

    Datum chunked_input1 =
        ChunkedArrayFromJSON(item_ty, {R"(["5.10", "1.23", "2.00", "3.45", "4.56"])",
                                       R"(["9.42", "1.01", null, "3.14", "4.00"])"});
    Datum chunked_input2 =
        ChunkedArrayFromJSON(item_ty, {R"(["5.10", null, "2.00", "3.45", "4.56"])",
                                       R"(["9.42", "1.01", "2.52", "3.14", "4.00"])"});
    Datum chunked_input3 =
        ChunkedArrayFromJSON(item_ty, {R"(["5.10", "1.23", "2.00", "3.45", null])",
                                       R"(["9.42", "1.01", null, "3.14", "4.00"])"});

    ScalarAggregateOptions options;

    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["5.10", "-1.23", "2.00", "3.45", "4.56"])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": "-1.23", "max": "5.10"})")));
    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["-5.10", "-1.23", "-2.00", "-3.45", "-4.56"])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": "-5.10", "max": "-1.23"})")));
    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["5.10", null, "2.00", "3.45", "4.56"])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": "2.00", "max": "5.10"})")));

    EXPECT_THAT(MinMax(chunked_input1, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": "1.01", "max": "9.42"})")));
    EXPECT_THAT(MinMax(chunked_input2, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": "1.01", "max": "9.42"})")));
    EXPECT_THAT(MinMax(chunked_input3, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": "1.01", "max": "9.42"})")));

    EXPECT_THAT(CallFunction("min", {chunked_input1}, &options),
                ResultWith(ScalarFromJSON(item_ty, R"("1.01")")));
    EXPECT_THAT(CallFunction("max", {chunked_input1}, &options),
                ResultWith(ScalarFromJSON(item_ty, R"("9.42")")));

    EXPECT_THAT(MinMax(ScalarFromJSON(item_ty, "null"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(ScalarFromJSON(item_ty, R"("1.00")"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": "1.00", "max": "1.00"})")));

    options = ScalarAggregateOptions(/*skip_nulls=*/false);
    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["5.10", "-1.23", "2.00", "3.45", "4.56"])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": "-1.23", "max": "5.10"})")));
    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["5.10", null, "2.00", "3.45", "4.56"])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(chunked_input1, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(chunked_input2, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(chunked_input3, options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));

    options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/0);
    EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, R"([])"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, R"([null])"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));

    options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/5);
    EXPECT_THAT(MinMax(ArrayFromJSON(
                           item_ty, R"(["5.10", "-1.23", "2.00", "3.45", "4.56", null])"),
                       options),
                ResultWith(ScalarFromJSON(ty, R"({"min": "-1.23", "max": "5.10"})")));
    EXPECT_THAT(
        MinMax(ArrayFromJSON(item_ty, R"(["5.10", "-1.23", "2.00", "3.45", null])"),
               options),
        ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));

    options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/1);
    EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, R"([])"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
    EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, R"([null])"), options),
                ResultWith(ScalarFromJSON(ty, R"({"min": null, "max": null})")));
  }
}

TEST(TestNullMinMaxKernel, Basics) {
  auto item_ty = null();
  auto ty = struct_({field("min", item_ty), field("max", item_ty)});
  Datum result = ScalarFromJSON(ty, "[null, null]");
  EXPECT_THAT(MinMax(ScalarFromJSON(item_ty, "null")), ResultWith(result));
  EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, "[]")), ResultWith(result));
  EXPECT_THAT(MinMax(ArrayFromJSON(item_ty, "[null]")), ResultWith(result));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(item_ty, {"[null]", "[]", "[null, null]"})),
              ResultWith(result));
}

template <typename ArrowType>
class TestBaseBinaryMinMaxKernel : public ::testing::Test {};
TYPED_TEST_SUITE(TestBaseBinaryMinMaxKernel, BaseBinaryArrowTypes);
TYPED_TEST(TestBaseBinaryMinMaxKernel, Basics) {
  std::vector<std::string> chunked_input1 = {R"(["cc", "", "aa", "b", "c"])",
                                             R"(["d", "", null, "b", "c"])"};
  std::vector<std::string> chunked_input2 = {R"(["cc", null, "aa", "b", "c"])",
                                             R"(["d", "", "aa", "b", "c"])"};
  std::vector<std::string> chunked_input3 = {R"(["cc", "", "aa", "b", null])",
                                             R"(["d", "", null, "b", "c"])"};
  auto ty = std::make_shared<TypeParam>();
  auto res_ty = struct_({field("min", ty), field("max", ty)});
  Datum null = ScalarFromJSON(res_ty, R"([null, null])");

  // SKIP nulls by default
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "cc"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0])),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "cc"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "d"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input2)),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "d"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "d"])")));

  EXPECT_THAT(MinMax(MakeNullScalar(ty)), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("one")")),
              ResultWith(ScalarFromJSON(res_ty, R"(["one", "one"])")));

  ScalarAggregateOptions options(/*skip_nulls=*/false);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "cc"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(null));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("one")"), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["one", "one"])")));

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/9);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "d"])")));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("one")"), options), ResultWith(null));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["", "cc"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(null));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("one")"), options), ResultWith(null));
}

TEST(TestFixedSizeBinaryMinMaxKernel, Basics) {
  auto ty = fixed_size_binary(2);
  std::vector<std::string> chunked_input1 = {R"(["cd", "aa", "ab", "bb", "cc"])",
                                             R"(["da", "aa", null, "bb", "cc"])"};
  std::vector<std::string> chunked_input2 = {R"(["cd", null, "ab", "bb", "cc"])",
                                             R"(["da", "aa", "ab", "bb", "cc"])"};
  std::vector<std::string> chunked_input3 = {R"(["cd", "aa", "ab", "bb", null])",
                                             R"(["da", "aa", null, "bb", "cc"])"};
  auto res_ty = struct_({field("min", ty), field("max", ty)});
  Datum null = ScalarFromJSON(res_ty, R"([null, null])");

  // SKIP nulls by default
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, R"([])")), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, R"([null, null, null])")), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0])),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "cd"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0])),
              ResultWith(ScalarFromJSON(res_ty, R"(["ab", "cd"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1)),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "da"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input2)),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "da"])")));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input3)),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "da"])")));

  EXPECT_THAT(MinMax(MakeNullScalar(ty)), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("aa")")),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "aa"])")));

  ScalarAggregateOptions options(/*skip_nulls=*/false);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "cd"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(null));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("aa")"), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "aa"])")));

  options = ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/9);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "da"])")));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("aa")"), options), ResultWith(null));

  options = ScalarAggregateOptions(/*skip_nulls=*/false, /*min_count=*/4);
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input1[0]), options),
              ResultWith(ScalarFromJSON(res_ty, R"(["aa", "cd"])")));
  EXPECT_THAT(MinMax(ArrayFromJSON(ty, chunked_input2[0]), options), ResultWith(null));
  EXPECT_THAT(MinMax(ChunkedArrayFromJSON(ty, chunked_input1), options),
              ResultWith(null));
  EXPECT_THAT(MinMax(MakeNullScalar(ty), options), ResultWith(null));
  EXPECT_THAT(MinMax(ScalarFromJSON(ty, R"("aa")"), options), ResultWith(null));
}

template <typename ArrowType>
struct MinMaxResult {
  using T = typename ArrowType::c_type;

  T min = 0;
  T max = 0;
  bool is_valid = false;
};

template <typename ArrowType>
static enable_if_integer<ArrowType, MinMaxResult<ArrowType>> NaiveMinMax(
    const Array& array) {
  using T = typename ArrowType::c_type;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  MinMaxResult<ArrowType> result;

  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();

  if (array.length() <= array.null_count()) {  // All null values
    return result;
  }

  T min = std::numeric_limits<T>::max();
  T max = std::numeric_limits<T>::min();
  if (array.null_count() != 0) {  // Some values are null
    BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
    for (int64_t i = 0; i < array.length(); i++) {
      if (reader.IsSet()) {
        min = std::min(min, values[i]);
        max = std::max(max, values[i]);
      }
      reader.Next();
    }
  } else {  // All true values
    for (int64_t i = 0; i < array.length(); i++) {
      min = std::min(min, values[i]);
      max = std::max(max, values[i]);
    }
  }

  result.min = min;
  result.max = max;
  result.is_valid = true;
  return result;
}

template <typename ArrowType>
static enable_if_floating_point<ArrowType, MinMaxResult<ArrowType>> NaiveMinMax(
    const Array& array) {
  using T = typename ArrowType::c_type;
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;

  MinMaxResult<ArrowType> result;

  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();

  if (array.length() <= array.null_count()) {  // All null values
    return result;
  }

  T min = std::numeric_limits<T>::infinity();
  T max = -std::numeric_limits<T>::infinity();
  if (array.null_count() != 0) {  // Some values are null
    BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
    for (int64_t i = 0; i < array.length(); i++) {
      if (reader.IsSet()) {
        min = std::fmin(min, values[i]);
        max = std::fmax(max, values[i]);
      }
      reader.Next();
    }
  } else {  // All true values
    for (int64_t i = 0; i < array.length(); i++) {
      min = std::fmin(min, values[i]);
      max = std::fmax(max, values[i]);
    }
  }

  result.min = min;
  result.max = max;
  result.is_valid = true;
  return result;
}

template <typename ArrowType>
void ValidateMinMax(const Array& array, const ScalarAggregateOptions& options) {
  using Traits = TypeTraits<ArrowType>;
  using ScalarType = typename Traits::ScalarType;

  ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array, options));
  const StructScalar& value = out.scalar_as<StructScalar>();

  auto expected = NaiveMinMax<ArrowType>(array);
  const auto& out_min = checked_cast<const ScalarType&>(*value.value[0]);
  const auto& out_max = checked_cast<const ScalarType&>(*value.value[1]);

  if (expected.is_valid) {
    ASSERT_TRUE(out_min.is_valid);
    ASSERT_TRUE(out_max.is_valid);
    ASSERT_EQ(expected.min, out_min.value);
    ASSERT_EQ(expected.max, out_max.value);
  } else {  // All null values
    ASSERT_FALSE(out_min.is_valid);
    ASSERT_FALSE(out_max.is_valid);
  }
}

template <typename ArrowType>
class TestRandomNumericMinMaxKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericMinMaxKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericMinMaxKernel, RandomArrayMinMax) {
  auto rand = random::RandomArrayGenerator(0x8afc055);
  const ScalarAggregateOptions& options =
      ScalarAggregateOptions(/*skip_nulls=*/true, /*min_count=*/1);
  // Test size up to 1<<11 (2048).
  for (size_t i = 3; i < 12; i += 2) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.5, 0.99, 1.0}) {
      int64_t base_length = (1UL << i) + 2;
      auto array = rand.Numeric<TypeParam>(base_length, 0, 100, null_probability);
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        ValidateMinMax<TypeParam>(*array->Slice(0, length), options);
      }
    }
  }
}

//
// Any
//

class TestAnyKernel : public ::testing::Test {
 public:
  void AssertAnyIs(const Datum& array, const std::shared_ptr<BooleanScalar>& expected,
                   const ScalarAggregateOptions& options) {
    SCOPED_TRACE(options.ToString());
    ASSERT_OK_AND_ASSIGN(Datum out, Any(array, options, nullptr));
    const BooleanScalar& out_any = out.scalar_as<BooleanScalar>();
    AssertScalarsEqual(*expected, out_any, /*verbose=*/true);
  }

  void AssertAnyIs(
      const std::string& json, const std::shared_ptr<BooleanScalar>& expected,
      const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
    SCOPED_TRACE(json);
    auto array = ArrayFromJSON(boolean(), json);
    AssertAnyIs(array, expected, options);
  }

  void AssertAnyIs(
      const std::vector<std::string>& json,
      const std::shared_ptr<BooleanScalar>& expected,
      const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
    auto array = ChunkedArrayFromJSON(boolean(), json);
    AssertAnyIs(array, expected, options);
  }
};

TEST_F(TestAnyKernel, Basics) {
  auto true_value = std::make_shared<BooleanScalar>(true);
  auto false_value = std::make_shared<BooleanScalar>(false);
  auto null_value = std::make_shared<BooleanScalar>();
  null_value->is_valid = false;

  std::vector<std::string> chunked_input0 = {"[]", "[true]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[false, null]", "[null, false]"};
  std::vector<std::string> chunked_input4 = {"[true, null]", "[null, false]"};

  const ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  this->AssertAnyIs("[]", false_value, options);
  this->AssertAnyIs("[false]", false_value, options);
  this->AssertAnyIs("[true, false]", true_value, options);
  this->AssertAnyIs("[null, null, null]", false_value, options);
  this->AssertAnyIs("[false, false, false]", false_value, options);
  this->AssertAnyIs("[false, false, false, null]", false_value, options);
  this->AssertAnyIs("[true, null, true, true]", true_value, options);
  this->AssertAnyIs("[false, null, false, true]", true_value, options);
  this->AssertAnyIs("[true, null, false, true]", true_value, options);
  this->AssertAnyIs(chunked_input0, true_value, options);
  this->AssertAnyIs(chunked_input1, true_value, options);
  this->AssertAnyIs(chunked_input2, false_value, options);
  this->AssertAnyIs(chunked_input3, false_value, options);
  this->AssertAnyIs(chunked_input4, true_value, options);

  EXPECT_THAT(Any(Datum(true), options), ResultWith(Datum(true)));
  EXPECT_THAT(Any(Datum(false), options), ResultWith(Datum(false)));
  EXPECT_THAT(Any(Datum(null_value), options), ResultWith(Datum(false)));

  const ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  this->AssertAnyIs("[]", false_value, keep_nulls);
  this->AssertAnyIs("[false]", false_value, keep_nulls);
  this->AssertAnyIs("[true, false]", true_value, keep_nulls);
  this->AssertAnyIs("[null, null, null]", null_value, keep_nulls);
  this->AssertAnyIs("[false, false, false]", false_value, keep_nulls);
  this->AssertAnyIs("[false, false, false, null]", null_value, keep_nulls);
  this->AssertAnyIs("[true, null, true, true]", true_value, keep_nulls);
  this->AssertAnyIs("[false, null, false, true]", true_value, keep_nulls);
  this->AssertAnyIs("[true, null, false, true]", true_value, keep_nulls);
  this->AssertAnyIs(chunked_input0, true_value, keep_nulls);
  this->AssertAnyIs(chunked_input1, true_value, keep_nulls);
  this->AssertAnyIs(chunked_input2, false_value, keep_nulls);
  this->AssertAnyIs(chunked_input3, null_value, keep_nulls);
  this->AssertAnyIs(chunked_input4, true_value, keep_nulls);

  EXPECT_THAT(Any(Datum(true), keep_nulls), ResultWith(Datum(true)));
  EXPECT_THAT(Any(Datum(false), keep_nulls), ResultWith(Datum(false)));
  EXPECT_THAT(Any(Datum(null_value), keep_nulls), ResultWith(Datum(null_value)));

  const ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/2);
  this->AssertAnyIs("[]", null_value, min_count);
  this->AssertAnyIs("[false]", null_value, min_count);
  this->AssertAnyIs("[true, false]", true_value, min_count);
  this->AssertAnyIs("[null, null, null]", null_value, min_count);
  this->AssertAnyIs("[false, false, false]", false_value, min_count);
  this->AssertAnyIs("[false, false, false, null]", false_value, min_count);
  this->AssertAnyIs("[true, null, true, true]", true_value, min_count);
  this->AssertAnyIs("[false, null, false, true]", true_value, min_count);
  this->AssertAnyIs("[true, null, false, true]", true_value, min_count);
  this->AssertAnyIs(chunked_input0, null_value, min_count);
  this->AssertAnyIs(chunked_input1, true_value, min_count);
  this->AssertAnyIs(chunked_input2, false_value, min_count);
  this->AssertAnyIs(chunked_input3, false_value, min_count);
  this->AssertAnyIs(chunked_input4, true_value, min_count);

  EXPECT_THAT(Any(Datum(true), min_count), ResultWith(Datum(null_value)));
  EXPECT_THAT(Any(Datum(false), min_count), ResultWith(Datum(null_value)));
  EXPECT_THAT(Any(Datum(null_value), min_count), ResultWith(Datum(null_value)));
}

//
// All
//

class TestAllKernel : public ::testing::Test {
 public:
  void AssertAllIs(const Datum& array, const std::shared_ptr<BooleanScalar>& expected,
                   const ScalarAggregateOptions& options) {
    SCOPED_TRACE(options.ToString());
    ASSERT_OK_AND_ASSIGN(Datum out, All(array, options, nullptr));
    const BooleanScalar& out_all = out.scalar_as<BooleanScalar>();
    AssertScalarsEqual(*expected, out_all, /*verbose=*/true);
  }

  void AssertAllIs(
      const std::string& json, const std::shared_ptr<BooleanScalar>& expected,
      const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
    SCOPED_TRACE(json);
    auto array = ArrayFromJSON(boolean(), json);
    AssertAllIs(array, expected, options);
  }

  void AssertAllIs(
      const std::vector<std::string>& json,
      const std::shared_ptr<BooleanScalar>& expected,
      const ScalarAggregateOptions& options = ScalarAggregateOptions::Defaults()) {
    auto array = ChunkedArrayFromJSON(boolean(), json);
    AssertAllIs(array, expected, options);
  }
};

TEST_F(TestAllKernel, Basics) {
  auto true_value = std::make_shared<BooleanScalar>(true);
  auto false_value = std::make_shared<BooleanScalar>(false);
  auto null_value = std::make_shared<BooleanScalar>();
  null_value->is_valid = false;

  std::vector<std::string> chunked_input0 = {"[]", "[true]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[false, null]", "[null, false]"};
  std::vector<std::string> chunked_input4 = {"[true, null]", "[null, false]"};
  std::vector<std::string> chunked_input5 = {"[false, null]", "[null, true]"};

  const ScalarAggregateOptions options(/*skip_nulls=*/true, /*min_count=*/0);
  this->AssertAllIs("[]", true_value, options);
  this->AssertAllIs("[false]", false_value, options);
  this->AssertAllIs("[true, false]", false_value, options);
  this->AssertAllIs("[null, null, null]", true_value, options);
  this->AssertAllIs("[false, false, false]", false_value, options);
  this->AssertAllIs("[false, false, false, null]", false_value, options);
  this->AssertAllIs("[true, null, true, true]", true_value, options);
  this->AssertAllIs("[false, null, false, true]", false_value, options);
  this->AssertAllIs("[true, null, false, true]", false_value, options);
  this->AssertAllIs(chunked_input0, true_value, options);
  this->AssertAllIs(chunked_input1, true_value, options);
  this->AssertAllIs(chunked_input2, false_value, options);
  this->AssertAllIs(chunked_input3, false_value, options);
  this->AssertAllIs(chunked_input4, false_value, options);
  this->AssertAllIs(chunked_input5, false_value, options);

  EXPECT_THAT(All(Datum(true), options), ResultWith(Datum(true)));
  EXPECT_THAT(All(Datum(false), options), ResultWith(Datum(false)));
  EXPECT_THAT(All(Datum(null_value), options), ResultWith(Datum(true)));

  const ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
  this->AssertAllIs("[]", true_value, keep_nulls);
  this->AssertAllIs("[false]", false_value, keep_nulls);
  this->AssertAllIs("[true, false]", false_value, keep_nulls);
  this->AssertAllIs("[null, null, null]", null_value, keep_nulls);
  this->AssertAllIs("[false, false, false]", false_value, keep_nulls);
  this->AssertAllIs("[false, false, false, null]", false_value, keep_nulls);
  this->AssertAllIs("[true, null, true, true]", null_value, keep_nulls);
  this->AssertAllIs("[false, null, false, true]", false_value, keep_nulls);
  this->AssertAllIs("[true, null, false, true]", false_value, keep_nulls);
  this->AssertAllIs(chunked_input0, true_value, keep_nulls);
  this->AssertAllIs(chunked_input1, null_value, keep_nulls);
  this->AssertAllIs(chunked_input2, false_value, keep_nulls);
  this->AssertAllIs(chunked_input3, false_value, keep_nulls);
  this->AssertAllIs(chunked_input4, false_value, keep_nulls);
  this->AssertAllIs(chunked_input5, false_value, keep_nulls);

  EXPECT_THAT(All(Datum(true), keep_nulls), ResultWith(Datum(true)));
  EXPECT_THAT(All(Datum(false), keep_nulls), ResultWith(Datum(false)));
  EXPECT_THAT(All(Datum(null_value), keep_nulls), ResultWith(Datum(null_value)));

  const ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/2);
  this->AssertAllIs("[]", null_value, min_count);
  this->AssertAllIs("[false]", null_value, min_count);
  this->AssertAllIs("[true, false]", false_value, min_count);
  this->AssertAllIs("[null, null, null]", null_value, min_count);
  this->AssertAllIs("[false, false, false]", false_value, min_count);
  this->AssertAllIs("[false, false, false, null]", false_value, min_count);
  this->AssertAllIs("[true, null, true, true]", true_value, min_count);
  this->AssertAllIs("[false, null, false, true]", false_value, min_count);
  this->AssertAllIs("[true, null, false, true]", false_value, min_count);
  this->AssertAllIs(chunked_input0, null_value, min_count);
  this->AssertAllIs(chunked_input1, true_value, min_count);
  this->AssertAllIs(chunked_input2, false_value, min_count);
  this->AssertAllIs(chunked_input3, false_value, min_count);
  this->AssertAllIs(chunked_input4, false_value, min_count);
  this->AssertAllIs(chunked_input5, false_value, min_count);

  EXPECT_THAT(All(Datum(true), min_count), ResultWith(Datum(null_value)));
  EXPECT_THAT(All(Datum(false), min_count), ResultWith(Datum(null_value)));
  EXPECT_THAT(All(Datum(null_value), min_count), ResultWith(Datum(null_value)));
}

//
// Index
//

void CheckIndex(Datum array, const std::shared_ptr<Scalar>& value, int64_t expected) {
  IndexOptions options(value);
  ASSERT_OK_AND_ASSIGN(Datum out, Index(array, options));
  const Int64Scalar& out_index = out.scalar_as<Int64Scalar>();
  ASSERT_EQ(out_index.value, expected);
}

template <typename ArrowType>
class TestIndexKernel : public ::testing::Test {
 public:
  using ScalarType = typename TypeTraits<ArrowType>::ScalarType;
  void AssertIndexIs(const std::string& json, const std::shared_ptr<ScalarType>& value,
                     int64_t expected) {
    SCOPED_TRACE("Value: " + value->ToString());
    SCOPED_TRACE("Input: " + json);
    auto array = ArrayFromJSON(type_singleton(), json);
    CheckIndex(array, value, expected);
  }

  void AssertIndexIs(const std::vector<std::string>& json,
                     const std::shared_ptr<ScalarType>& value, int64_t expected) {
    SCOPED_TRACE("Value: " + value->ToString());
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    SCOPED_TRACE("Input: " + array->ToString());
    CheckIndex(array, value, expected);
  }

  std::shared_ptr<DataType> type_singleton() { return std::make_shared<ArrowType>(); }
};

template <typename ArrowType>
class TestNumericIndexKernel : public TestIndexKernel<ArrowType> {
 public:
  using CType = typename TypeTraits<ArrowType>::CType;
};
TYPED_TEST_SUITE(TestNumericIndexKernel, NumericArrowTypes);
TYPED_TEST(TestNumericIndexKernel, Basics) {
  std::vector<std::string> chunked_input0 = {"[]", "[0]"};
  std::vector<std::string> chunked_input1 = {"[1, 0, null]", "[0, 0]"};
  std::vector<std::string> chunked_input2 = {"[1, 1, 1]", "[1, 0]", "[0, 1]"};
  std::vector<std::string> chunked_input3 = {"[1, 1, 1]", "[1, 1]"};
  std::vector<std::string> chunked_input4 = {"[1, 1, 1]", "[1, 1]", "[0]"};

  auto value = std::make_shared<typename TestFixture::ScalarType>(
      static_cast<typename TestFixture::CType>(0));
  auto null_value = std::make_shared<typename TestFixture::ScalarType>(
      static_cast<typename TestFixture::CType>(0));
  null_value->is_valid = false;

  this->AssertIndexIs("[]", value, -1);
  this->AssertIndexIs("[0]", value, 0);
  this->AssertIndexIs("[1, 2, 3, 4]", value, -1);
  this->AssertIndexIs("[1, 2, 3, 4, 0]", value, 4);
  this->AssertIndexIs("[null, null, null]", value, -1);
  this->AssertIndexIs("[null, null, null]", null_value, -1);
  this->AssertIndexIs("[0, null, null]", null_value, -1);
  this->AssertIndexIs(chunked_input0, value, 0);
  this->AssertIndexIs(chunked_input1, value, 1);
  this->AssertIndexIs(chunked_input2, value, 4);
  this->AssertIndexIs(chunked_input3, value, -1);
  this->AssertIndexIs(chunked_input4, value, 5);

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Function 'index' cannot be called without options"),
      CallFunction("index", {ArrayFromJSON(this->type_singleton(), "[0]")}));
}
TYPED_TEST(TestNumericIndexKernel, Random) {
  constexpr auto kChunks = 4;
  auto rand = random::RandomArrayGenerator(0x5487655);
  auto value = std::make_shared<typename TestFixture::ScalarType>(
      static_cast<typename TestFixture::CType>(0));

  // Test chunked array sizes from 32 to 2048
  for (size_t i = 3; i <= 9; i += 2) {
    const int64_t chunk_length = static_cast<int64_t>(1) << i;
    ArrayVector chunks;
    for (int i = 0; i < kChunks; i++) {
      chunks.push_back(
          rand.ArrayOf(this->type_singleton(), chunk_length, /*null_probability=*/0.1));
    }
    ChunkedArray chunked_array(std::move(chunks));

    int64_t expected = -1;
    int64_t index = 0;
    for (auto chunk : chunked_array.chunks()) {
      auto typed_chunk = arrow::internal::checked_pointer_cast<
          typename TypeTraits<TypeParam>::ArrayType>(chunk);
      for (auto value : *typed_chunk) {
        if (value.has_value() &&
            value.value() == static_cast<typename TestFixture::CType>(0)) {
          expected = index;
          break;
        }
        index++;
      }
      if (expected >= 0) break;
    }

    CheckIndex(Datum(chunked_array), value, expected);
  }
}

template <typename ArrowType>
class TestDateTimeIndexKernel : public TestIndexKernel<ArrowType> {};
TYPED_TEST_SUITE(TestDateTimeIndexKernel, TemporalArrowTypes);
TYPED_TEST(TestDateTimeIndexKernel, Basics) {
  auto type = this->type_singleton();
  auto value = std::make_shared<typename TestFixture::ScalarType>(42, type);
  auto null_value = std::make_shared<typename TestFixture::ScalarType>(42, type);
  null_value->is_valid = false;

  this->AssertIndexIs("[]", value, -1);
  this->AssertIndexIs("[42]", value, 0);
  this->AssertIndexIs("[84, 84, 84, 84]", value, -1);
  this->AssertIndexIs("[84, 84, 84, 84, 42]", value, 4);
  this->AssertIndexIs("[null, null, null]", value, -1);
  this->AssertIndexIs("[null, null, null]", null_value, -1);
  this->AssertIndexIs("[42, null, null]", null_value, -1);
}

template <typename ArrowType>
class TestBooleanIndexKernel : public TestIndexKernel<ArrowType> {};
TYPED_TEST_SUITE(TestBooleanIndexKernel, ::testing::Types<BooleanType>);
TYPED_TEST(TestBooleanIndexKernel, Basics) {
  auto value = std::make_shared<typename TestFixture::ScalarType>(true);
  auto null_value = std::make_shared<typename TestFixture::ScalarType>(true);
  null_value->is_valid = false;

  this->AssertIndexIs("[]", value, -1);
  this->AssertIndexIs("[true]", value, 0);
  this->AssertIndexIs("[false, false, false, false]", value, -1);
  this->AssertIndexIs("[false, false, false, false, true]", value, 4);
  this->AssertIndexIs("[null, null, null]", value, -1);
  this->AssertIndexIs("[null, null, null]", null_value, -1);
  this->AssertIndexIs("[true, null, null]", null_value, -1);
}

template <typename ArrowType>
class TestStringIndexKernel : public TestIndexKernel<ArrowType> {};
TYPED_TEST_SUITE(TestStringIndexKernel, BaseBinaryArrowTypes);
TYPED_TEST(TestStringIndexKernel, Basics) {
  auto buffer = Buffer::FromString("foo");
  auto value = std::make_shared<typename TestFixture::ScalarType>(buffer);
  auto null_value = std::make_shared<typename TestFixture::ScalarType>(buffer);
  null_value->is_valid = false;

  this->AssertIndexIs(R"([])", value, -1);
  this->AssertIndexIs(R"(["foo"])", value, 0);
  this->AssertIndexIs(R"(["bar", "bar", "bar", "bar"])", value, -1);
  this->AssertIndexIs(R"(["bar", "bar", "bar", "bar", "foo"])", value, 4);
  this->AssertIndexIs(R"([null, null, null])", value, -1);
  this->AssertIndexIs(R"([null, null, null])", null_value, -1);
  this->AssertIndexIs(R"(["foo", null, null])", null_value, -1);
}

TEST(TestIndexKernel, FixedSizeBinary) {
  auto ty = fixed_size_binary(3);
  auto buffer = Buffer::FromString("foo");
  auto value = std::make_shared<FixedSizeBinaryScalar>(buffer, ty);
  auto null_value = std::make_shared<FixedSizeBinaryScalar>(buffer, ty);
  null_value->is_valid = false;

  CheckIndex(ArrayFromJSON(ty, R"([])"), value, -1);
  CheckIndex(ArrayFromJSON(ty, R"(["foo"])"), value, 0);
  CheckIndex(ArrayFromJSON(ty, R"(["bar", "bar", "bar", "bar"])"), value, -1);
  CheckIndex(ArrayFromJSON(ty, R"(["bar", "bar", "bar", "bar", "foo"])"), value, 4);
  CheckIndex(ArrayFromJSON(ty, R"([null, null, null])"), value, -1);
  CheckIndex(ArrayFromJSON(ty, R"([null, null, null])"), null_value, -1);
  CheckIndex(ArrayFromJSON(ty, R"(["foo", null, null])"), null_value, -1);
}

TEST(TestIndexKernel, Decimal) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    std::shared_ptr<Scalar> value, null_value;
    if (ty->id() == Type::DECIMAL128) {
      value = std::make_shared<Decimal128Scalar>(Decimal128(123), ty);
      null_value = std::make_shared<Decimal128Scalar>(ty);
    } else {
      value = std::make_shared<Decimal256Scalar>(Decimal256(123), ty);
      null_value = std::make_shared<Decimal256Scalar>(ty);
    }

    CheckIndex(ArrayFromJSON(ty, R"([])"), value, -1);
    CheckIndex(ArrayFromJSON(ty, R"(["1.23"])"), value, 0);
    CheckIndex(ArrayFromJSON(ty, R"(["9.99", "9.99", "9.99", "9.99"])"), value, -1);
    CheckIndex(ArrayFromJSON(ty, R"(["9.99", "9.99", "9.99", "9.99", "1.23"])"), value,
               4);
    CheckIndex(ArrayFromJSON(ty, R"([null, null, null])"), value, -1);
    CheckIndex(ArrayFromJSON(ty, R"([null, null, null])"), null_value, -1);
    CheckIndex(ArrayFromJSON(ty, R"(["1.23", null, null])"), null_value, -1);
  }
}

TEST(TestIndexKernel, Null) {
  auto ty = null();
  auto value = std::make_shared<NullScalar>();

  CheckIndex(ArrayFromJSON(ty, R"([])"), value, -1);
  CheckIndex(ArrayFromJSON(ty, R"([null])"), value, -1);
  CheckIndex(ArrayFromJSON(ty, R"([null, null, null, null])"), value, -1);
}

TEST(TestIndexKernel, Errors) {
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr(
          "Expected IndexOptions.value to be of type string, but got int32"),
      Index(ArrayFromJSON(utf8(), R"(["a"])"),
            IndexOptions(ScalarFromJSON(int32(), "1"))));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      TypeError,
      ::testing::HasSubstr("Expected IndexOptions.value to be of type timestamp[ns], "
                           "but got timestamp[ms]"),
      Index(ArrayFromJSON(timestamp(TimeUnit::NANO), R"(["2020-01-01"])"),
            IndexOptions(ScalarFromJSON(timestamp(TimeUnit::MILLI), R"("2020-01-01")"))));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      Invalid, ::testing::HasSubstr("Must provide IndexOptions.value"),
      Index(ArrayFromJSON(utf8(), R"(["a"])"), IndexOptions(nullptr)));
}

//
// Mode
//

template <typename CType>
void CheckModes(const Datum& array, const ModeOptions options,
                const std::vector<CType>& expected_modes,
                const std::vector<int64_t>& expected_counts) {
  ARROW_SCOPED_TRACE("Mode Options: ", options.ToString());
  ASSERT_OK_AND_ASSIGN(Datum out, Mode(array, options));
  ValidateOutput(out);
  const StructArray out_array(out.array());
  ASSERT_EQ(out_array.length(), expected_modes.size());
  ASSERT_EQ(out_array.num_fields(), 2);

  const CType* out_modes = out_array.field(0)->data()->GetValues<CType>(1);
  const int64_t* out_counts = out_array.field(1)->data()->GetValues<int64_t>(1);
  for (int i = 0; i < out_array.length(); ++i) {
    // equal or nan equal
    ASSERT_TRUE((expected_modes[i] == out_modes[i]) ||
                (expected_modes[i] != expected_modes[i] && out_modes[i] != out_modes[i]))
        << "  Actual Value: " << out_modes[i] << "\n"
        << "Expected Value: " << expected_modes[i];
    ASSERT_EQ(expected_counts[i], out_counts[i]);
  }
}

template <>
void CheckModes<bool>(const Datum& array, const ModeOptions options,
                      const std::vector<bool>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
  ARROW_SCOPED_TRACE("Mode Options: ", options.ToString());
  ASSERT_OK_AND_ASSIGN(Datum out, Mode(array, options));
  ValidateOutput(out);
  const StructArray out_array(out.array());
  ASSERT_EQ(out_array.length(), expected_modes.size());
  ASSERT_EQ(out_array.num_fields(), 2);

  const uint8_t* out_modes = out_array.field(0)->data()->GetValues<uint8_t>(1);
  const int64_t* out_counts = out_array.field(1)->data()->GetValues<int64_t>(1);
  for (int i = 0; i < out_array.length(); ++i) {
    ASSERT_EQ(expected_modes[i], bit_util::GetBit(out_modes, i));
    ASSERT_EQ(expected_counts[i], out_counts[i]);
  }
}

template <typename T>
class TestPrimitiveModeKernel : public ::testing::Test {
 public:
  using ArrowType = T;
  using Traits = TypeTraits<ArrowType>;
  using CType = typename ArrowType::c_type;

  void AssertModesAre(const Datum& array, const ModeOptions options,
                      const std::vector<CType>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    CheckModes(array, options, expected_modes, expected_counts);
  }

  void AssertModesAre(const std::string& json, const int n,
                      const std::vector<CType>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesAre(array, ModeOptions(n), expected_modes, expected_counts);
  }

  void AssertModesAre(const std::string& json, const ModeOptions options,
                      const std::vector<CType>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesAre(array, options, expected_modes, expected_counts);
  }

  void AssertModeIs(const Datum& array, CType expected_mode, int64_t expected_count) {
    AssertModesAre(array, ModeOptions(1), {expected_mode}, {expected_count});
  }

  void AssertModeIs(const std::string& json, CType expected_mode,
                    int64_t expected_count) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModeIs(array, expected_mode, expected_count);
  }

  void AssertModeIs(const std::vector<std::string>& json, CType expected_mode,
                    int64_t expected_count) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertModeIs(chunked, expected_mode, expected_count);
  }

  void AssertModesEmpty(const Datum& array, ModeOptions options) {
    ASSERT_OK_AND_ASSIGN(Datum out, Mode(array, options));
    auto out_array = out.make_array();
    ValidateOutput(*out_array);
    ASSERT_EQ(out.array()->length, 0);
  }

  void AssertModesEmpty(const std::string& json, int n = 1) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesEmpty(array, ModeOptions(n));
  }

  void AssertModesEmpty(const std::vector<std::string>& json, int n = 1) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertModesEmpty(chunked, ModeOptions(n));
  }

  void AssertModesEmpty(const std::string& json, ModeOptions options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesEmpty(array, options);
  }

  std::shared_ptr<DataType> type_singleton() { return Traits::type_singleton(); }
};

template <typename ArrowType>
class TestIntegerModeKernel : public TestPrimitiveModeKernel<ArrowType> {};

template <typename ArrowType>
class TestFloatingModeKernel : public TestPrimitiveModeKernel<ArrowType> {};

class TestBooleanModeKernel : public TestPrimitiveModeKernel<BooleanType> {};

class TestInt8ModeKernelValueRange : public TestPrimitiveModeKernel<Int8Type> {};

class TestInt32ModeKernel : public TestPrimitiveModeKernel<Int32Type> {};

TEST_F(TestBooleanModeKernel, Basics) {
  this->AssertModeIs("[false, false]", false, 2);
  this->AssertModeIs("[false, false, true, true, true]", true, 3);
  this->AssertModeIs("[true, false, false, true, true]", true, 3);
  this->AssertModeIs("[false, false, true, true, true, false]", false, 3);

  this->AssertModeIs("[true, null, false, false, null, true, null, null, true]", true, 3);
  this->AssertModesEmpty("[null, null, null]");
  this->AssertModesEmpty("[]");

  this->AssertModeIs({"[true, false]", "[true, true]", "[false, false]"}, false, 3);
  this->AssertModeIs({"[true, null]", "[]", "[null, false]"}, false, 1);
  this->AssertModesEmpty({"[null, null]", "[]", "[null]"});

  this->AssertModesAre("[true, false]", 2, {false, true}, {1, 1});
  this->AssertModesAre("[false, false, true, true, true, false]", 2, {false, true},
                       {3, 3});
  this->AssertModesAre("[true, null, false, false, null, true, null, null, true]", 100,
                       {true, false}, {3, 2});
  this->AssertModesEmpty({"[null, null]", "[]", "[null]"}, 4);

  auto in_ty = boolean();
  this->AssertModesAre("[true, false, false, null]", ModeOptions(/*n=*/1), {false}, {2});
  this->AssertModesEmpty("[true, false, false, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesAre("[true, false, false, null]",
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3),
                       {false}, {2});
  this->AssertModesEmpty("[false, false, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3));
  this->AssertModesAre("[true, false, false]",
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3),
                       {false}, {2});
  this->AssertModesEmpty("[true, false, false, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesEmpty("[true, false]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesAre(ScalarFromJSON(in_ty, "true"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {true}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "true"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "true"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));

  this->AssertModesAre(ScalarFromJSON(in_ty, "true"), ModeOptions(/*n=*/1), {true}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"), ModeOptions(/*n=*/1));
}

TYPED_TEST_SUITE(TestIntegerModeKernel, IntegralArrowTypes);
TYPED_TEST(TestIntegerModeKernel, Basics) {
  this->AssertModeIs("[5, 1, 1, 5, 5]", 5, 3);
  this->AssertModeIs("[5, 1, 1, 5, 5, 1]", 1, 3);
  this->AssertModeIs("[127, 0, 127, 127, 0, 1, 0, 127]", 127, 4);

  this->AssertModeIs("[null, null, 2, null, 1]", 1, 1);
  this->AssertModesEmpty("[null, null, null]");
  this->AssertModesEmpty("[]");

  this->AssertModeIs({"[5]", "[1, 1, 5]", "[5]"}, 5, 3);
  this->AssertModeIs({"[5]", "[1, 1, 5]", "[5, 1]"}, 1, 3);
  this->AssertModesEmpty({"[null, null]", "[]", "[null]"});

  this->AssertModesAre("[127, 0, 127, 127, 0, 1, 0, 127]", 2, {127, 0}, {4, 3});
  this->AssertModesAre("[null, null, 2, null, 1]", 3, {1, 2}, {1, 1});
  this->AssertModesEmpty("[null, null, null]", 10);

  auto in_ty = this->type_singleton();

  this->AssertModesAre("[1, 2, 2, null]", ModeOptions(/*n=*/1), {2}, {2});
  this->AssertModesEmpty("[1, 2, 2, null]", ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesAre("[1, 2, 2, null]",
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3), {2},
                       {2});
  this->AssertModesEmpty("[2, 2, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3));
  this->AssertModesAre(
      "[1, 2, 2]", ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3), {2}, {2});
  this->AssertModesEmpty("[1, 2, 2, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesEmpty("[1, 2]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesAre(ScalarFromJSON(in_ty, "1"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {1}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "1"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "1"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));

  this->AssertModesAre(ScalarFromJSON(in_ty, "5"), ModeOptions(/*n=*/1), {5}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"), ModeOptions(/*n=*/1));
}

TYPED_TEST_SUITE(TestFloatingModeKernel, RealArrowTypes);
TYPED_TEST(TestFloatingModeKernel, Floats) {
  this->AssertModeIs("[5, 1, 1, 5, 5]", 5, 3);
  this->AssertModeIs("[5, 1, 1, 5, 5, 1]", 1, 3);
  this->AssertModeIs("[Inf, 100, Inf, 100, Inf]", INFINITY, 3);
  this->AssertModeIs("[Inf, -Inf, Inf, -Inf]", -INFINITY, 2);

  this->AssertModeIs("[null, null, 2, null, 1]", 1, 1);
  this->AssertModeIs("[NaN, NaN, 1, null, 1]", 1, 2);

  this->AssertModesEmpty("[null, null, null]");
  this->AssertModesEmpty("[]");

  this->AssertModeIs("[NaN, NaN, 1]", NAN, 2);
  this->AssertModeIs("[NaN, NaN, null]", NAN, 2);
  this->AssertModeIs("[NaN, NaN, NaN]", NAN, 3);

  this->AssertModeIs({"[Inf, 100]", "[Inf, 100]", "[Inf]"}, INFINITY, 3);
  this->AssertModeIs({"[NaN, 1]", "[NaN, 1]", "[NaN]"}, NAN, 3);
  this->AssertModesEmpty({"[null, null]", "[]", "[null]"});

  this->AssertModesAre("[Inf, 100, Inf, 100, Inf]", 2, {INFINITY, 100}, {3, 2});
  this->AssertModesAre("[NaN, NaN, 1, null, 1, 2, 2]", 3, {1, 2, NAN}, {2, 2, 2});

  auto in_ty = this->type_singleton();

  this->AssertModesAre("[1, 2, 2, null]", ModeOptions(/*n=*/1), {2}, {2});
  this->AssertModesEmpty("[1, 2, 2, null]", ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesAre("[1, 2, 2, null]",
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3), {2},
                       {2});
  this->AssertModesEmpty("[2, 2, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3));
  this->AssertModesAre(
      "[1, 2, 2]", ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3), {2}, {2});
  this->AssertModesEmpty("[1, 2, 2, null]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesEmpty("[1, 2]",
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3));
  this->AssertModesAre(ScalarFromJSON(in_ty, "1"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {1}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "1"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "1"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"),
                         ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2));

  this->AssertModesAre(ScalarFromJSON(in_ty, "5"), ModeOptions(/*n=*/1), {5}, {1});
  this->AssertModesEmpty(ScalarFromJSON(in_ty, "null"), ModeOptions(/*n=*/1));
}

template <typename ArrowType>
class TestDecimalModeKernel : public ::testing::Test {
 public:
  using CType = typename TypeTraits<ArrowType>::CType;

  void AssertModesAre(const Datum& array, const ModeOptions options,
                      const std::vector<std::string>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    CheckModes<CType>(array, options, values(expected_modes), expected_counts);
  }

  CType value(const std::string& s) const {
    EXPECT_OK_AND_ASSIGN(auto out, CType::FromString(s));
    return out;
  }

  std::vector<CType> values(const std::vector<std::string>& strings) const {
    std::vector<CType> values;
    for (const auto& s : strings) {
      values.push_back(value(s));
    }
    return values;
  }

  std::shared_ptr<DataType> type_instance() { return std::make_shared<ArrowType>(4, 2); }
};

TYPED_TEST_SUITE(TestDecimalModeKernel, DecimalArrowTypes);

TYPED_TEST(TestDecimalModeKernel, Decimals) {
  auto ty = this->type_instance();
  this->AssertModesAre(ArrayFromJSON(ty, R"(["5.01", "-1.42", "-1.42", "5.01", "5.01"])"),
                       ModeOptions(1), {"5.01"}, {3});
  this->AssertModesAre(
      ArrayFromJSON(ty, R"(["5.01", "-1.42", "-1.42", "5.01", "5.01", "-1.42"])"),
      ModeOptions(1), {"-1.42"}, {3});
  this->AssertModesAre(
      ArrayFromJSON(ty, R"(["5.01", "-1.42", "-1.42", "5.01", "5.01", "-1.42"])"),
      ModeOptions(2), {"-1.42", "5.01"}, {3, 3});

  this->AssertModesAre(ArrayFromJSON(ty, "[]"), ModeOptions(1), {}, {});

  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00", "-2.00", null])"),
                       ModeOptions(/*n=*/1), {"-2.00"}, {2});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00", "-2.00", null])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {}, {});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00", "-2.00", null])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3),
                       {"-2.00"}, {2});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["-2.00", "-2.00", null])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/3), {},
                       {});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00", "-2.00"])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3),
                       {"-2.00"}, {2});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00", "-2.00", null])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3), {},
                       {});
  this->AssertModesAre(ArrayFromJSON(ty, R"(["1.86", "-2.00"])"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/3), {},
                       {});

  this->AssertModesAre(ScalarFromJSON(ty, R"("0.00")"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {"0.00"}, {1});
  this->AssertModesAre(ScalarFromJSON(ty, "null"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false), {}, {});
  this->AssertModesAre(ScalarFromJSON(ty, R"("0.00")"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2), {},
                       {});
  this->AssertModesAre(ScalarFromJSON(ty, "null"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/true, /*min_count=*/2), {},
                       {});
  this->AssertModesAre(ScalarFromJSON(ty, R"("0.00")"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2), {},
                       {});
  this->AssertModesAre(ScalarFromJSON(ty, "null"),
                       ModeOptions(/*n=*/1, /*skip_nulls=*/false, /*min_count=*/2), {},
                       {});
  this->AssertModesAre(ScalarFromJSON(ty, R"("5.00")"), ModeOptions(/*n=*/1), {"5.00"},
                       {1});
  this->AssertModesAre(ScalarFromJSON(ty, "null"), ModeOptions(/*n=*/1), {}, {});
}

TEST_F(TestInt8ModeKernelValueRange, Basics) {
  this->AssertModeIs("[0, 127, -128, -128]", -128, 2);
  this->AssertModeIs("[127, 127, 127]", 127, 3);
}

template <typename ArrowType>
struct ModeResult {
  using T = typename ArrowType::c_type;

  T mode = std::numeric_limits<T>::min();
  int64_t count = 0;
};

template <typename ArrowType>
ModeResult<ArrowType> NaiveMode(const Array& array) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  using CTYPE = typename ArrowType::c_type;

  std::unordered_map<CTYPE, int64_t> value_counts;

  const auto& array_numeric = reinterpret_cast<const ArrayType&>(array);
  const auto values = array_numeric.raw_values();
  BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
  for (int64_t i = 0; i < array.length(); ++i) {
    if (reader.IsSet()) {
      ++value_counts[values[i]];
    }
    reader.Next();
  }

  ModeResult<ArrowType> result;
  for (const auto& value_count : value_counts) {
    auto value = value_count.first;
    auto count = value_count.second;
    if (count > result.count || (count == result.count && value < result.mode)) {
      result.count = count;
      result.mode = value;
    }
  }

  return result;
}

template <typename ArrowType, typename CTYPE = typename ArrowType::c_type>
void VerifyMode(const std::shared_ptr<Array>& array) {
  auto expected = NaiveMode<ArrowType>(*array);
  ASSERT_OK_AND_ASSIGN(Datum out, Mode(array));
  const StructArray out_array(out.array());
  ValidateOutput(out_array);
  ASSERT_EQ(out_array.length(), 1);
  ASSERT_EQ(out_array.num_fields(), 2);

  const CTYPE* out_modes = out_array.field(0)->data()->GetValues<CTYPE>(1);
  const int64_t* out_counts = out_array.field(1)->data()->GetValues<int64_t>(1);
  ASSERT_EQ(out_modes[0], expected.mode);
  ASSERT_EQ(out_counts[0], expected.count);
}

template <typename ArrowType, typename CTYPE = typename ArrowType::c_type>
void CheckModeWithRange(CTYPE range_min, CTYPE range_max) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  // 32K items (>= counting mode cutoff) within range, 10% null
  auto array = rand.Numeric<ArrowType>(32 * 1024, range_min, range_max, 0.1);
  VerifyMode<ArrowType>(array);
}

template <typename ArrowType, typename CTYPE = typename ArrowType::c_type>
void CheckModeWithRangeSliced(CTYPE range_min, CTYPE range_max) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  auto array = rand.Numeric<ArrowType>(32 * 1024, range_min, range_max, 0.1);

  const int64_t array_size = array->length();
  const std::vector<std::array<int64_t, 2>> offset_size{
      {0, 40},
      {array_size - 40, 40},
      {array_size / 3, array_size / 6},
      {array_size * 9 / 10, array_size / 10},
  };
  for (const auto& os : offset_size) {
    VerifyMode<ArrowType>(array->Slice(os[0], os[1]));
  }
}

TEST_F(TestInt32ModeKernel, SmallValueRange) {
  // Small value range => should exercise counter-based Mode implementation
  CheckModeWithRange<ArrowType>(-100, 100);
}

TEST_F(TestInt32ModeKernel, LargeValueRange) {
  // Large value range => should exercise sorter-based Mode implementation
  CheckModeWithRange<ArrowType>(-10000000, 10000000);
}

TEST_F(TestInt32ModeKernel, Sliced) {
  CheckModeWithRangeSliced<ArrowType>(-100, 100);
  CheckModeWithRangeSliced<ArrowType>(-10000000, 10000000);
}

//
// Variance/Stddev
//

void CheckVarStd(const Datum& array, const VarianceOptions& options,
                 double expected_var) {
  ASSERT_OK_AND_ASSIGN(Datum out_var, Variance(array, options));
  ASSERT_OK_AND_ASSIGN(Datum out_std, Stddev(array, options));
  auto var = checked_cast<const DoubleScalar*>(out_var.scalar().get());
  auto std = checked_cast<const DoubleScalar*>(out_std.scalar().get());
  ASSERT_TRUE(var->is_valid && std->is_valid);
  // Near zero these macros don't work as well
  // (and MinGW can give results slightly off from zero)
  if (std::abs(expected_var) < 1e-20) {
    ASSERT_NEAR(std->value * std->value, var->value, 1e-20);
    ASSERT_NEAR(var->value, expected_var, 1e-20);
  } else {
    ASSERT_DOUBLE_EQ(std->value * std->value, var->value);
    ASSERT_DOUBLE_EQ(var->value, expected_var);  // < 4ULP
  }
}

template <typename ArrowType>
class TestPrimitiveVarStdKernel : public ::testing::Test {
 public:
  using Traits = TypeTraits<ArrowType>;
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

  void AssertVarStdIs(const Array& array, const VarianceOptions& options,
                      double expected_var) {
    CheckVarStd(array, options, expected_var);
  }

  void AssertVarStdIs(const std::shared_ptr<ChunkedArray>& array,
                      const VarianceOptions& options, double expected_var) {
    CheckVarStd(array, options, expected_var);
  }

  void AssertVarStdIs(const std::string& json, const VarianceOptions& options,
                      double expected_var) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertVarStdIs(*array, options, expected_var);
  }

  void AssertVarStdIs(const std::vector<std::string>& json,
                      const VarianceOptions& options, double expected_var) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertVarStdIs(chunked, options, expected_var);
  }

  void AssertVarStdIsInvalid(const Array& array, const VarianceOptions& options) {
    AssertVarStdIsInvalidInternal(array, options);
  }

  void AssertVarStdIsInvalid(const std::shared_ptr<ChunkedArray>& array,
                             const VarianceOptions& options) {
    AssertVarStdIsInvalidInternal(array, options);
  }

  void AssertVarStdIsInvalid(const std::string& json, const VarianceOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertVarStdIsInvalid(*array, options);
  }

  void AssertVarStdIsInvalid(const std::vector<std::string>& json,
                             const VarianceOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertVarStdIsInvalid(array, options);
  }

  std::shared_ptr<DataType> type_singleton() { return Traits::type_singleton(); }

 private:
  void AssertVarStdIsInvalidInternal(const Datum& array, const VarianceOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out_var, Variance(array, options));
    ASSERT_OK_AND_ASSIGN(Datum out_std, Stddev(array, options));
    auto var = checked_cast<const ScalarType*>(out_var.scalar().get());
    auto std = checked_cast<const ScalarType*>(out_std.scalar().get());
    ASSERT_FALSE(var->is_valid || std->is_valid);
  }
};

template <typename ArrowType>
class TestNumericVarStdKernel : public TestPrimitiveVarStdKernel<ArrowType> {};

// Reference value from numpy.var
TYPED_TEST_SUITE(TestNumericVarStdKernel, NumericArrowTypes);
TYPED_TEST(TestNumericVarStdKernel, Basics) {
  VarianceOptions options;  // ddof = 0, population variance/stddev

  this->AssertVarStdIs("[100]", options, 0);
  this->AssertVarStdIs("[1, 2, 3]", options, 0.6666666666666666);
  this->AssertVarStdIs("[null, 1, 2, null, 3]", options, 0.6666666666666666);

  std::vector<std::string> chunks;
  chunks = {"[]", "[1]", "[2]", "[null]", "[3]"};
  this->AssertVarStdIs(chunks, options, 0.6666666666666666);
  chunks = {"[1, 2, 3]", "[4, 5, 6]", "[7, 8]"};
  this->AssertVarStdIs(chunks, options, 5.25);
  chunks = {"[1, 2, 3, 4, 5, 6, 7]", "[8]"};
  this->AssertVarStdIs(chunks, options, 5.25);

  this->AssertVarStdIsInvalid("[null, null, null]", options);
  this->AssertVarStdIsInvalid("[]", options);
  this->AssertVarStdIsInvalid("[]", options);

  options.ddof = 1;  // sample variance/stddev

  this->AssertVarStdIs("[1, 2]", options, 0.5);

  chunks = {"[1]", "[2]"};
  this->AssertVarStdIs(chunks, options, 0.5);
  chunks = {"[1, 2, 3]", "[4, 5, 6]", "[7, 8]"};
  this->AssertVarStdIs(chunks, options, 6.0);
  chunks = {"[1, 2, 3, 4, 5, 6, 7]", "[8]"};
  this->AssertVarStdIs(chunks, options, 6.0);

  this->AssertVarStdIsInvalid("[100]", options);
  this->AssertVarStdIsInvalid("[100, null, null]", options);
  chunks = {"[100]", "[null]", "[]"};
  this->AssertVarStdIsInvalid(chunks, options);

  auto ty = this->type_singleton();
  EXPECT_THAT(Stddev(*MakeScalar(ty, 5)), ResultWith(Datum(0.0)));
  EXPECT_THAT(Variance(*MakeScalar(ty, 5)), ResultWith(Datum(0.0)));
  EXPECT_THAT(Stddev(*MakeScalar(ty, 5), options),
              ResultWith(Datum(MakeNullScalar(float64()))));
  EXPECT_THAT(Variance(*MakeScalar(ty, 5), options),
              ResultWith(Datum(MakeNullScalar(float64()))));
  EXPECT_THAT(Stddev(MakeNullScalar(ty)), ResultWith(Datum(MakeNullScalar(float64()))));
  EXPECT_THAT(Variance(MakeNullScalar(ty)), ResultWith(Datum(MakeNullScalar(float64()))));

  // skip_nulls and min_count
  options.ddof = 0;
  options.min_count = 3;
  this->AssertVarStdIs("[1, 2, 3]", options, 0.6666666666666666);
  this->AssertVarStdIsInvalid("[1, 2, null]", options);

  options.min_count = 0;
  options.skip_nulls = false;
  this->AssertVarStdIs("[1, 2, 3]", options, 0.6666666666666666);
  this->AssertVarStdIsInvalid("[1, 2, 3, null]", options);

  options.min_count = 4;
  options.skip_nulls = false;
  this->AssertVarStdIsInvalid("[1, 2, 3]", options);
  this->AssertVarStdIsInvalid("[1, 2, 3, null]", options);
}

// Test numerical stability
template <typename ArrowType>
class TestVarStdKernelStability : public TestPrimitiveVarStdKernel<ArrowType> {};

typedef ::testing::Types<Int32Type, UInt32Type, Int64Type, UInt64Type, DoubleType>
    VarStdStabilityTypes;

TYPED_TEST_SUITE(TestVarStdKernelStability, VarStdStabilityTypes);
TYPED_TEST(TestVarStdKernelStability, Basics) {
  VarianceOptions options{1};  // ddof = 1
  this->AssertVarStdIs("[100000004, 100000007, 100000013, 100000016]", options, 30.0);
  this->AssertVarStdIs("[1000000004, 1000000007, 1000000013, 1000000016]", options, 30.0);
  if (!is_unsigned_integer_type<TypeParam>::value) {
    this->AssertVarStdIs("[-1000000016, -1000000013, -1000000007, -1000000004]", options,
                         30.0);
  }
}

// Test numerical stability of variance merging code
class TestVarStdKernelMergeStability : public TestPrimitiveVarStdKernel<DoubleType> {};

TEST_F(TestVarStdKernelMergeStability, Basics) {
  VarianceOptions options{1};  // ddof = 1

#ifndef __MINGW32__  // MinGW has precision issues
  // XXX: The reference value from numpy is actually wrong due to floating
  // point limits. The correct result should equals variance(90, 0) = 4050.
  std::vector<std::string> chunks = {"[40000008000000490]", "[40000008000000400]"};
  this->AssertVarStdIs(chunks, options, 3904.0);
#endif
}

// Test round-off error
template <typename ArrowType>
class TestVarStdKernelRoundOff : public TestPrimitiveVarStdKernel<ArrowType> {};

typedef ::testing::Types<Int32Type, Int64Type, FloatType, DoubleType> VarStdRoundOffTypes;

TYPED_TEST_SUITE(TestVarStdKernelRoundOff, VarStdRoundOffTypes);
TYPED_TEST(TestVarStdKernelRoundOff, Basics) {
  // build array: np.arange(321000, dtype='xxx')
  typename TypeParam::c_type value = 0;
  ASSERT_OK_AND_ASSIGN(
      auto array, ArrayFromBuilderVisitor(TypeTraits<TypeParam>::type_singleton(), 321000,
                                          [&](NumericBuilder<TypeParam>* builder) {
                                            builder->UnsafeAppend(value++);
                                          }));

  // reference value from numpy.var()
  this->AssertVarStdIs(*array, VarianceOptions{0}, 8586749999.916667);
}

// Test integer arithmetic code
class TestVarStdKernelInt32 : public TestPrimitiveVarStdKernel<Int32Type> {};

TEST_F(TestVarStdKernelInt32, Basics) {
  VarianceOptions options{1};
  this->AssertVarStdIs("[-2147483648, -2147483647, -2147483646]", options, 1.0);
  this->AssertVarStdIs("[2147483645, 2147483646, 2147483647]", options, 1.0);
  this->AssertVarStdIs("[-2147483648, -2147483648, 2147483647]", options,
                       6.148914688373205e+18);
}

class TestVarStdKernelUInt32 : public TestPrimitiveVarStdKernel<UInt32Type> {};

TEST_F(TestVarStdKernelUInt32, Basics) {
  VarianceOptions options{1};
  this->AssertVarStdIs("[4294967293, 4294967294, 4294967295]", options, 1.0);
  this->AssertVarStdIs("[0, 0, 4294967295]", options, 6.148914688373205e+18);
}

// https://en.wikipedia.org/wiki/Kahan_summation_algorithm
void KahanSum(double& sum, double& adjust, double addend) {
  double y = addend - adjust;
  double t = sum + y;
  adjust = (t - sum) - y;
  sum = t;
}

// Calculate reference variance with Welford's online algorithm + Kahan summation
// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
// XXX: not stable for long array with very small `stddev / average`
template <typename ArrayType>
std::pair<double, double> WelfordVar(const ArrayType& array) {
  const auto values = array.raw_values();
  BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
  double count = 0, mean = 0, m2 = 0;
  double mean_adjust = 0, m2_adjust = 0;
  for (int64_t i = 0; i < array.length(); ++i) {
    if (reader.IsSet()) {
      ++count;
      double delta = static_cast<double>(values[i]) - mean;
      KahanSum(mean, mean_adjust, delta / count);
      double delta2 = static_cast<double>(values[i]) - mean;
      KahanSum(m2, m2_adjust, delta * delta2);
    }
    reader.Next();
  }
  return std::make_pair(m2 / count, m2 / (count - 1));
}

// Test random chunked array
template <typename ArrowType>
class TestVarStdKernelRandom : public TestPrimitiveVarStdKernel<ArrowType> {};

using VarStdRandomTypes =
    ::testing::Types<Int32Type, UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>;

TYPED_TEST_SUITE(TestVarStdKernelRandom, VarStdRandomTypes);

TYPED_TEST(TestVarStdKernelRandom, Basics) {
#if defined(__MINGW32__) && !defined(__MINGW64__)
  if (TypeParam::type_id == Type::FLOAT) {
    GTEST_SKIP() << "Precision issues on MinGW32 with float32";
  }
#endif
  // Cut array into small chunks
  constexpr int array_size = 5000;
  constexpr int chunk_size_max = 50;
  constexpr int chunk_count = array_size / chunk_size_max;

  std::shared_ptr<Array> array;
  auto rand = random::RandomArrayGenerator(0x5487656);
  if (is_floating_type<TypeParam>::value) {
    array = rand.Numeric<TypeParam>(array_size, -10000.0, 100000.0, 0.1);
  } else {
    using CType = typename TypeParam::c_type;
    constexpr CType min = std::numeric_limits<CType>::min();
    constexpr CType max = std::numeric_limits<CType>::max();
    array = rand.Numeric<TypeParam>(array_size, min, max, 0.1);
  }
  auto chunk_size_array = rand.Numeric<Int32Type>(chunk_count, 0, chunk_size_max);
  const int* chunk_size = chunk_size_array->data()->GetValues<int>(1);
  int total_size = 0;

  ArrayVector array_vector;
  for (int i = 0; i < chunk_count; ++i) {
    array_vector.emplace_back(array->Slice(total_size, chunk_size[i]));
    total_size += chunk_size[i];
  }
  auto chunked = *ChunkedArray::Make(array_vector);

  double var_population, var_sample;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  auto typed_array = checked_pointer_cast<ArrayType>(array->Slice(0, total_size));
  std::tie(var_population, var_sample) = WelfordVar(*typed_array);

  this->AssertVarStdIs(chunked, VarianceOptions{0}, var_population);
  this->AssertVarStdIs(chunked, VarianceOptions{1}, var_sample);
}

// This test is too heavy to run in CI, should be checked manually
#if 0
class TestVarStdKernelIntegerLength : public TestPrimitiveVarStdKernel<Int32Type> {};

TEST_F(TestVarStdKernelIntegerLength, Basics) {
  constexpr int32_t min = std::numeric_limits<int32_t>::min();
  constexpr int32_t max = std::numeric_limits<int32_t>::max();
  auto rand = random::RandomArrayGenerator(0x5487657);
  // large data volume
  auto array = rand.Numeric<Int32Type>(4000000000, min, max, 0.1);
  // biased distribution
  // auto array = rand.Numeric<Int32Type>(4000000000, min, min + 100000, 0.1);

  double var_population, var_sample;
  auto int32_array = checked_pointer_cast<Int32Array>(array);
  std::tie(var_population, var_sample) = WelfordVar(*int32_array);

  this->AssertVarStdIs(*array, VarianceOptions{0}, var_population);
  this->AssertVarStdIs(*array, VarianceOptions{1}, var_sample);
}
#endif

TEST(TestVarStdKernel, Decimal) {
  // Effectively treated as double, sanity check results here
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    CheckVarStd(ArrayFromJSON(ty, R"(["1.00"])"), VarianceOptions(), 0);
    CheckVarStd(ArrayFromJSON(ty, R"([null, "1.00", "2.00", "3.00"])"), VarianceOptions(),
                0.6666666666666666);
    CheckVarStd(ScalarFromJSON(ty, R"("1.00")"), VarianceOptions(), 0);
    CheckVarStd(ArrayFromJSON(ty, R"([null, "1.00", "2.00"])"),
                VarianceOptions(/*ddof=*/1), 0.5);
  }
}

//
// Quantile
//

template <typename ArrowType>
class TestPrimitiveQuantileKernel : public ::testing::Test {
 public:
  using Traits = TypeTraits<ArrowType>;
  using CType = typename ArrowType::c_type;

  void AssertQuantilesAre(const Datum& array, QuantileOptions options,
                          const std::vector<std::vector<Datum>>& expected) {
    ASSERT_EQ(options.q.size(), expected.size());

    for (size_t i = 0; i < this->interpolations_.size(); ++i) {
      options.interpolation = this->interpolations_[i];
      ARROW_SCOPED_TRACE("Quantile Options: ", options.ToString());

      ASSERT_OK_AND_ASSIGN(Datum out, Quantile(array, options));
      const auto& out_array = out.make_array();
      ValidateOutput(*out_array);
      ASSERT_EQ(out_array->length(), options.q.size());
      ASSERT_EQ(out_array->null_count(), 0);
      AssertTypeEqual(out_array->type(), expected[0][i].type());

      if (out_array->type()->Equals(float64())) {
        const double* quantiles = out_array->data()->GetValues<double>(1);
        for (int64_t j = 0; j < out_array->length(); ++j) {
          const auto& numeric_scalar =
              checked_pointer_cast<DoubleScalar>(expected[j][i].scalar());
          ASSERT_TRUE((quantiles[j] == numeric_scalar->value) ||
                      (std::isnan(quantiles[j]) && std::isnan(numeric_scalar->value)))
              << "  Actual Value: " << quantiles[j] << "\n"
              << "Expected Value: " << numeric_scalar->value;
        }
      } else {
        AssertTypeEqual(out_array->type(), type_singleton());
        const CType* quantiles = out_array->data()->GetValues<CType>(1);
        for (int64_t j = 0; j < out_array->length(); ++j) {
          const auto& numeric_scalar =
              checked_pointer_cast<NumericScalar<ArrowType>>(expected[j][i].scalar());
          ASSERT_EQ(quantiles[j], numeric_scalar->value);
        }
      }
    }
  }

  void AssertQuantilesAre(const std::string& json, const std::vector<double>& q,
                          const std::vector<std::vector<Datum>>& expected) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertQuantilesAre(array, QuantileOptions{q}, expected);
  }

  void AssertQuantilesAre(const std::vector<std::string>& json,
                          const std::vector<double>& q,
                          const std::vector<std::vector<Datum>>& expected) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertQuantilesAre(chunked, QuantileOptions{q}, expected);
  }

  void AssertQuantileIs(const Datum& array, double q,
                        const std::vector<Datum>& expected) {
    AssertQuantilesAre(array, QuantileOptions{q}, {expected});
  }

  void AssertQuantileIs(const std::string& json, double q,
                        const std::vector<Datum>& expected) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertQuantileIs(array, q, expected);
  }

  void AssertQuantileIs(const std::vector<std::string>& json, double q,
                        const std::vector<Datum>& expected) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertQuantileIs(chunked, q, expected);
  }

  void AssertQuantilesEmpty(const Datum& array, const std::vector<double>& q) {
    QuantileOptions options{q};
    for (auto interpolation : this->interpolations_) {
      options.interpolation = interpolation;
      ASSERT_OK_AND_ASSIGN(Datum out, Quantile(array, options));
      auto out_array = out.make_array();
      ValidateOutput(*out_array);
      ASSERT_EQ(out.array()->length, q.size());
      ASSERT_EQ(out.array()->null_count, q.size());
    }
  }

  void AssertQuantilesEmpty(const std::string& json, const std::vector<double>& q) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertQuantilesEmpty(array, q);
  }

  void AssertQuantilesEmpty(const std::vector<std::string>& json,
                            const std::vector<double>& q) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertQuantilesEmpty(chunked, q);
  }

  std::shared_ptr<DataType> type_singleton() { return Traits::type_singleton(); }

  std::vector<enum QuantileOptions::Interpolation> interpolations_ = {
      QuantileOptions::LINEAR, QuantileOptions::LOWER, QuantileOptions::HIGHER,
      QuantileOptions::NEAREST, QuantileOptions::MIDPOINT};
};

#define INTYPE(x) Datum(static_cast<typename TypeParam::c_type>(x))
#define DOUBLE(x) Datum(static_cast<double>(x))
// output type per interplation: linear, lower, higher, nearest, midpoint
#define O(a, b, c, d, e) \
  { DOUBLE(a), INTYPE(b), INTYPE(c), INTYPE(d), DOUBLE(e) }

template <typename ArrowType>
class TestIntegerQuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {};

TYPED_TEST_SUITE(TestIntegerQuantileKernel, IntegralArrowTypes);
TYPED_TEST(TestIntegerQuantileKernel, Basics) {
  // reference values from numpy
  // ordered by interpolation method: {linear, lower, higher, nearest, midpoint}
  this->AssertQuantileIs("[1]", 0.1, O(1, 1, 1, 1, 1));
  this->AssertQuantileIs("[1, 2]", 0.5, O(1.5, 1, 2, 1, 1.5));
  this->AssertQuantileIs("[3, 5, 2, 9, 0, 1, 8]", 0.5, O(3, 3, 3, 3, 3));
  this->AssertQuantileIs("[3, 5, 2, 9, 0, 1, 8]", 0.33, O(1.98, 1, 2, 2, 1.5));
  this->AssertQuantileIs("[3, 5, 2, 9, 0, 1, 8]", 0.9, O(8.4, 8, 9, 8, 8.5));
  this->AssertQuantilesAre("[3, 5, 2, 9, 0, 1, 8]", {0.5, 0.9},
                           {O(3, 3, 3, 3, 3), O(8.4, 8, 9, 8, 8.5)});
  this->AssertQuantilesAre("[3, 5, 2, 9, 0, 1, 8]", {1, 0.5},
                           {O(9, 9, 9, 9, 9), O(3, 3, 3, 3, 3)});
  this->AssertQuantileIs("[3, 5, 2, 9, 0, 1, 8]", 0, O(0, 0, 0, 0, 0));
  this->AssertQuantileIs("[3, 5, 2, 9, 0, 1, 8]", 1, O(9, 9, 9, 9, 9));

  this->AssertQuantileIs("[5, null, null, 3, 9, null, 8, 1, 2, 0]", 0.21,
                         O(1.26, 1, 2, 1, 1.5));
  this->AssertQuantilesAre("[5, null, null, 3, 9, null, 8, 1, 2, 0]", {0.5, 0.9},
                           {O(3, 3, 3, 3, 3), O(8.4, 8, 9, 8, 8.5)});
  this->AssertQuantilesAre("[5, null, null, 3, 9, null, 8, 1, 2, 0]", {0.9, 0.5},
                           {O(8.4, 8, 9, 8, 8.5), O(3, 3, 3, 3, 3)});

  this->AssertQuantileIs({"[5]", "[null, null]", "[3, 9, null]", "[8, 1, 2, 0]"}, 0.33,
                         O(1.98, 1, 2, 2, 1.5));
  this->AssertQuantilesAre({"[5]", "[null, null]", "[3, 9, null]", "[8, 1, 2, 0]"},
                           {0.21, 1}, {O(1.26, 1, 2, 1, 1.5), O(9, 9, 9, 9, 9)});

  this->AssertQuantilesEmpty("[]", {0.5});
  this->AssertQuantilesEmpty("[null, null, null]", {0.1, 0.2});
  this->AssertQuantilesEmpty({"[null, null]", "[]", "[null]"}, {0.3, 0.4});

  auto ty = this->type_singleton();

  QuantileOptions keep_nulls(/*q=*/0.5, QuantileOptions::LINEAR, /*skip_nulls=*/false,
                             /*min_count=*/0);
  QuantileOptions min_count(/*q=*/0.5, QuantileOptions::LINEAR, /*skip_nulls=*/true,
                            /*min_count=*/3);
  QuantileOptions keep_nulls_min_count(/*q=*/0.5, QuantileOptions::LINEAR,
                                       /*skip_nulls=*/false, /*min_count=*/3);
  auto not_empty = ResultWith(ArrayFromJSON(float64(), "[3.0]"));
  auto empty = ResultWith(ArrayFromJSON(float64(), "[null]"));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), min_count), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), min_count), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), keep_nulls_min_count),
              not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), keep_nulls_min_count),
              empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls_min_count), empty);

  for (const auto interpolation : this->interpolations_) {
    QuantileOptions options({0.0, 0.5, 1.0}, interpolation);
    auto expected_ty = (interpolation == QuantileOptions::LINEAR ||
                        interpolation == QuantileOptions::MIDPOINT)
                           ? float64()
                           : ty;
    EXPECT_THAT(Quantile(*MakeScalar(ty, 1), options),
                ResultWith(ArrayFromJSON(expected_ty, "[1, 1, 1]")));
    EXPECT_THAT(Quantile(MakeNullScalar(ty), options),
                ResultWith(ArrayFromJSON(expected_ty, "[null, null, null]")));
  }
}

template <typename ArrowType>
class TestFloatingQuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {};

#ifndef __MINGW32__
TYPED_TEST_SUITE(TestFloatingQuantileKernel, RealArrowTypes);
TYPED_TEST(TestFloatingQuantileKernel, Floats) {
  // ordered by interpolation method: {linear, lower, higher, nearest, midpoint}
  this->AssertQuantileIs("[-9, 7, Inf, -Inf, 2, 11]", 0.5, O(4.5, 2, 7, 2, 4.5));
  this->AssertQuantileIs("[-9, 7, Inf, -Inf, 2, 11]", 0.1,
                         O(-INFINITY, -INFINITY, -9, -INFINITY, -INFINITY));
  this->AssertQuantileIs("[-9, 7, Inf, -Inf, 2, 11]", 0.9,
                         O(INFINITY, 11, INFINITY, 11, INFINITY));
  this->AssertQuantilesAre("[-9, 7, Inf, -Inf, 2, 11]", {0.3, 0.6},
                           {O(-3.5, -9, 2, 2, -3.5), O(7, 7, 7, 7, 7)});
  this->AssertQuantileIs("[-Inf, Inf]", 0.2, O(NAN, -INFINITY, INFINITY, -INFINITY, NAN));

  this->AssertQuantileIs("[NaN, -9, 7, Inf, null, null, -Inf, NaN, 2, 11]", 0.5,
                         O(4.5, 2, 7, 2, 4.5));
  this->AssertQuantilesAre("[null, -9, 7, Inf, NaN, NaN, -Inf, null, 2, 11]", {0.3, 0.6},
                           {O(-3.5, -9, 2, 2, -3.5), O(7, 7, 7, 7, 7)});
  this->AssertQuantilesAre("[null, -9, 7, Inf, NaN, NaN, -Inf, null, 2, 11]", {0.6, 0.3},
                           {O(7, 7, 7, 7, 7), O(-3.5, -9, 2, 2, -3.5)});

  this->AssertQuantileIs({"[NaN, -9, 7, Inf]", "[null, NaN]", "[-Inf, NaN, 2, 11]"}, 0.5,
                         O(4.5, 2, 7, 2, 4.5));
  this->AssertQuantilesAre({"[null, -9, 7, Inf]", "[NaN, NaN]", "[-Inf, null, 2, 11]"},
                           {0.3, 0.6}, {O(-3.5, -9, 2, 2, -3.5), O(7, 7, 7, 7, 7)});

  this->AssertQuantilesEmpty("[]", {0.5, 0.6});
  this->AssertQuantilesEmpty("[null, NaN, null]", {0.1});
  this->AssertQuantilesEmpty({"[NaN, NaN]", "[]", "[null]"}, {0.3, 0.4});

  auto ty = this->type_singleton();

  QuantileOptions keep_nulls(/*q=*/0.5, QuantileOptions::LINEAR, /*skip_nulls=*/false,
                             /*min_count=*/0);
  QuantileOptions min_count(/*q=*/0.5, QuantileOptions::LINEAR, /*skip_nulls=*/true,
                            /*min_count=*/3);
  QuantileOptions keep_nulls_min_count(/*q=*/0.5, QuantileOptions::LINEAR,
                                       /*skip_nulls=*/false, /*min_count=*/3);
  auto not_empty = ResultWith(ArrayFromJSON(float64(), "[3.0]"));
  auto empty = ResultWith(ArrayFromJSON(float64(), "[null]"));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), keep_nulls), not_empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), min_count), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), min_count), not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5]"), keep_nulls_min_count),
              not_empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 2, 4, 5, null]"), keep_nulls_min_count),
              empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5]"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1, 5, null]"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "3"), keep_nulls_min_count), empty);
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls_min_count), empty);

  for (const auto interpolation : this->interpolations_) {
    QuantileOptions options({0.0, 0.5, 1.0}, interpolation);
    auto expected_ty = (interpolation == QuantileOptions::LINEAR ||
                        interpolation == QuantileOptions::MIDPOINT)
                           ? float64()
                           : ty;
    EXPECT_THAT(Quantile(*MakeScalar(ty, 1), options),
                ResultWith(ArrayFromJSON(expected_ty, "[1, 1, 1]")));
    EXPECT_THAT(Quantile(MakeNullScalar(ty), options),
                ResultWith(ArrayFromJSON(expected_ty, "[null, null, null]")));
  }
}

class TestInt8QuantileKernel : public TestPrimitiveQuantileKernel<Int8Type> {};

// Test histogram approach
TEST_F(TestInt8QuantileKernel, Int8) {
  using TypeParam = Int8Type;
  this->AssertQuantilesAre(
      "[127, -128, null, -128, 66, -88, 127]", {0, 0.3, 0.7, 1},
      {O(-128, -128, -128, -128, -128), O(-108, -128, -88, -88, -108),
       O(96.5, 66, 127, 127, 96.5), O(127, 127, 127, 127, 127)});
  this->AssertQuantilesAre(
      {"[null]", "[-88, 127]", "[]", "[66, -128, null, -128]", "[127]"}, {0, 0.3, 0.7, 1},
      {O(-128, -128, -128, -128, -128), O(-108, -128, -88, -88, -108),
       O(96.5, 66, 127, 127, 96.5), O(127, 127, 127, 127, 127)});
}
#endif

class TestInt64QuantileKernel : public TestPrimitiveQuantileKernel<Int64Type> {};

// Test big int64 numbers cannot be precisely presented by double
TEST_F(TestInt64QuantileKernel, Int64) {
  using TypeParam = Int64Type;
  this->AssertQuantileIs(
      "[9223372036854775806, 9223372036854775807]", 0.5,
      O(9.223372036854776e+18, 9223372036854775806, 9223372036854775807,
        9223372036854775806, 9.223372036854776e+18));
}

#undef INTYPE
#undef DOUBLE
#undef O

#if !defined(__MINGW32__) && !defined(__i386__)
template <typename ArrowType>
class TestRandomQuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {
  using CType = typename ArrowType::c_type;

 public:
  void CheckQuantiles(int64_t array_size, int64_t num_quantiles) {
    std::shared_ptr<Array> array;
    std::vector<double> quantiles;
    // small value range to exercise input array with equal values and histogram approach
    GenerateTestData(array_size, num_quantiles, -100, 200, &array, &quantiles);

    this->AssertQuantilesAre(array, QuantileOptions{quantiles},
                             NaiveQuantile(array, quantiles, this->interpolations_));
  }

  void CheckQuantilesSliced(int64_t array_size, int64_t num_quantiles) {
    std::shared_ptr<Array> array;
    std::vector<double> quantiles;
    GenerateTestData(array_size, num_quantiles, -100, 200, &array, &quantiles);

    const std::vector<std::array<int64_t, 2>> offset_size{
        {0, array_size - 1},
        {1, array_size - 1},
        {array_size / 3, array_size / 2},
        {array_size * 9 / 10, array_size / 10},
    };
    for (const auto& os : offset_size) {
      auto sliced = array->Slice(os[0], os[1]);
      this->AssertQuantilesAre(sliced, QuantileOptions{quantiles},
                               NaiveQuantile(sliced, quantiles, this->interpolations_));
    }
  }

  void CheckTDigests(const std::vector<int>& chunk_sizes, int64_t num_quantiles) {
    std::shared_ptr<ChunkedArray> chunked;
    std::vector<double> quantiles;
    GenerateChunked(chunk_sizes, num_quantiles, &chunked, &quantiles);

    VerifyTDigest(chunked, quantiles);
  }

  void CheckTDigestsSliced(const std::vector<int>& chunk_sizes, int64_t num_quantiles) {
    std::shared_ptr<ChunkedArray> chunked;
    std::vector<double> quantiles;
    GenerateChunked(chunk_sizes, num_quantiles, &chunked, &quantiles);

    const int64_t size = chunked->length();
    const std::vector<std::array<int64_t, 2>> offset_size{
        {0, size - 1},
        {1, size - 1},
        {size / 3, size / 2},
        {size * 9 / 10, size / 10},
    };
    for (const auto& os : offset_size) {
      VerifyTDigest(chunked->Slice(os[0], os[1]), quantiles);
    }
  }

 private:
  void GenerateTestData(int64_t array_size, int64_t num_quantiles, int min, int max,
                        std::shared_ptr<Array>* array, std::vector<double>* quantiles) {
    auto rand = random::RandomArrayGenerator(0x5487658);
    if (is_floating_type<ArrowType>::value) {
      *array = rand.Float64(array_size, min, max, /*null_prob=*/0.1, /*nan_prob=*/0.2);
    } else {
      *array = rand.Int64(array_size, min, max, /*null_prob=*/0.1);
    }

    random_real(num_quantiles, 0x5487658, 0.0, 1.0, quantiles);
    // make sure to exercise 0 and 1 quantiles
    *std::min_element(quantiles->begin(), quantiles->end()) = 0;
    *std::max_element(quantiles->begin(), quantiles->end()) = 1;
  }

  void GenerateChunked(const std::vector<int>& chunk_sizes, int64_t num_quantiles,
                       std::shared_ptr<ChunkedArray>* chunked,
                       std::vector<double>* quantiles) {
    int total_size = 0;
    for (int size : chunk_sizes) {
      total_size += size;
    }
    std::shared_ptr<Array> array;
    GenerateTestData(total_size, num_quantiles, 100, 123456789, &array, quantiles);

    total_size = 0;
    ArrayVector array_vector;
    for (int size : chunk_sizes) {
      array_vector.emplace_back(array->Slice(total_size, size));
      total_size += size;
    }
    *chunked = ChunkedArray::Make(array_vector).ValueOrDie();
  }

  void VerifyTDigest(const std::shared_ptr<ChunkedArray>& chunked,
                     std::vector<double>& quantiles) {
    TDigestOptions options(quantiles);
    ASSERT_OK_AND_ASSIGN(Datum out, TDigest(chunked, options));
    const auto& out_array = out.make_array();
    ValidateOutput(*out_array);
    ASSERT_EQ(out_array->length(), quantiles.size());
    ASSERT_EQ(out_array->null_count(), 0);
    AssertTypeEqual(out_array->type(), float64());

    // linear interpolated exact quantile as reference
    std::vector<std::vector<Datum>> exact =
        NaiveQuantile(*chunked, quantiles, {QuantileOptions::LINEAR});
    const double* approx = out_array->data()->GetValues<double>(1);
    for (size_t i = 0; i < quantiles.size(); ++i) {
      const auto& exact_scalar = checked_pointer_cast<DoubleScalar>(exact[i][0].scalar());
      const double tolerance = std::fabs(exact_scalar->value) * 0.05;
      EXPECT_NEAR(approx[i], exact_scalar->value, tolerance) << quantiles[i];
    }
  }

  std::vector<std::vector<Datum>> NaiveQuantile(
      const std::shared_ptr<Array>& array, const std::vector<double>& quantiles,
      const std::vector<enum QuantileOptions::Interpolation>& interpolations) {
    return NaiveQuantile(ChunkedArray(array), quantiles, interpolations);
  }

  std::vector<std::vector<Datum>> NaiveQuantile(
      const ChunkedArray& chunked, const std::vector<double>& quantiles,
      const std::vector<enum QuantileOptions::Interpolation>& interpolations) {
    // copy and sort input chunked array
    int64_t index = 0;
    std::vector<CType> input(chunked.length() - chunked.null_count());
    for (const auto& array : chunked.chunks()) {
      const CType* values = array->data()->GetValues<CType>(1);
      const auto bitmap = array->null_bitmap_data();
      for (int64_t i = 0; i < array->length(); ++i) {
        if ((!bitmap || bit_util::GetBit(bitmap, array->data()->offset + i)) &&
            !std::isnan(static_cast<double>(values[i]))) {
          input[index++] = values[i];
        }
      }
    }
    input.resize(index);
    std::sort(input.begin(), input.end());

    std::vector<std::vector<Datum>> output(quantiles.size(),
                                           std::vector<Datum>(interpolations.size()));
    for (uint64_t i = 0; i < interpolations.size(); ++i) {
      const auto interp = interpolations[i];
      for (uint64_t j = 0; j < quantiles.size(); ++j) {
        output[j][i] = GetQuantile(input, quantiles[j], interp);
      }
    }
    return output;
  }

  Datum GetQuantile(const std::vector<CType>& input, double q,
                    enum QuantileOptions::Interpolation interp) {
    const double index = (input.size() - 1) * q;
    const uint64_t lower_index = static_cast<uint64_t>(index);
    const double fraction = index - lower_index;

    switch (interp) {
      case QuantileOptions::LOWER:
        return Datum(input[lower_index]);
      case QuantileOptions::HIGHER:
        return Datum(input[lower_index + (fraction != 0)]);
      case QuantileOptions::NEAREST:
        if (fraction < 0.5) {
          return Datum(input[lower_index]);
        } else if (fraction > 0.5) {
          return Datum(input[lower_index + 1]);
        } else {
          return Datum(input[lower_index + (lower_index & 1)]);
        }
      case QuantileOptions::LINEAR:
        if (fraction == 0) {
          return Datum(input[lower_index] * 1.0);
        } else {
          return Datum(fraction * input[lower_index + 1] +
                       (1 - fraction) * input[lower_index]);
        }
      case QuantileOptions::MIDPOINT:
        if (fraction == 0) {
          return Datum(input[lower_index] * 1.0);
        } else {
          return Datum(input[lower_index] / 2.0 + input[lower_index + 1] / 2.0);
        }
      default:
        return Datum(NAN);
    }
  }
};

class TestRandomInt64QuantileKernel : public TestRandomQuantileKernel<Int64Type> {};

TEST_F(TestRandomInt64QuantileKernel, Normal) {
  // exercise copy and sort approach: size < 65536
  this->CheckQuantiles(/*array_size=*/10000, /*num_quantiles=*/100);
}

TEST_F(TestRandomInt64QuantileKernel, Overlapped) {
  // much more quantiles than array size => many overlaps
  this->CheckQuantiles(/*array_size=*/999, /*num_quantiles=*/9999);
}

TEST_F(TestRandomInt64QuantileKernel, Histogram) {
  // exercise histogram approach: size >= 65536, range <= 65536
  this->CheckQuantiles(/*array_size=*/80000, /*num_quantiles=*/100);
}

TEST_F(TestRandomInt64QuantileKernel, Sliced) {
  this->CheckQuantilesSliced(1000, 10);   // sort
  this->CheckQuantilesSliced(66000, 10);  // count
}

class TestRandomFloatQuantileKernel : public TestRandomQuantileKernel<DoubleType> {};

TEST_F(TestRandomFloatQuantileKernel, Exact) {
  this->CheckQuantiles(/*array_size=*/1000, /*num_quantiles=*/100);
}

TEST_F(TestRandomFloatQuantileKernel, TDigest) {
  this->CheckTDigests(/*chunk_sizes=*/{12345, 6789, 8765, 4321}, /*num_quantiles=*/100);
}

TEST_F(TestRandomFloatQuantileKernel, Sliced) {
  this->CheckQuantilesSliced(1000, 10);
  this->CheckTDigestsSliced({200, 600}, 10);
}
#endif

TEST(TestQuantileKernel, AllNullsOrNaNs) {
  const std::vector<std::vector<std::string>> tests = {
      {"[]"},
      {"[null, null]", "[]", "[null]"},
      {"[NaN]", "[NaN, NaN]", "[]"},
      {"[null, NaN, null]"},
      {"[NaN, NaN]", "[]", "[null]"},
  };

  for (const auto& json : tests) {
    auto chunked = ChunkedArrayFromJSON(float64(), json);
    ASSERT_OK_AND_ASSIGN(Datum out, Quantile(chunked, QuantileOptions()));
    auto out_array = out.make_array();
    ValidateOutput(*out_array);
    AssertArraysEqual(*ArrayFromJSON(float64(), "[null]"), *out_array, /*verbose=*/true);
  }
}

TEST(TestQuantileKernel, Decimal) {
  auto check = [](const std::shared_ptr<Array>& input, QuantileOptions options,
                  const std::shared_ptr<Array>& expected) {
    ASSERT_OK_AND_ASSIGN(Datum out, Quantile(input, options));
    auto out_array = out.make_array();
    ValidateOutput(*out_array);
    AssertArraysEqual(*expected, *out_array, /*verbose=*/true);
  };
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    check(ArrayFromJSON(ty, R"(["1.00", "5.00", null])"),
          QuantileOptions(0.5, QuantileOptions::LINEAR),
          ArrayFromJSON(float64(), R"([3.00])"));
    check(ArrayFromJSON(ty, R"(["1.00", "2.00", "5.00"])"),
          QuantileOptions(0.5, QuantileOptions::NEAREST),
          ArrayFromJSON(ty, R"(["2.00"])"));
  }
}

TEST(TestQuantileKernel, Scalar) {
  for (const auto& ty : {float64(), int64(), uint64()}) {
    QuantileOptions options(std::vector<double>{0.0, 0.5, 1.0});
    EXPECT_THAT(Quantile(*MakeScalar(ty, 1), options),
                ResultWith(ArrayFromJSON(float64(), "[1.0, 1.0, 1.0]")));
    EXPECT_THAT(Quantile(MakeNullScalar(ty), options),
                ResultWith(ArrayFromJSON(float64(), "[null, null, null]")));
  }
}

TEST(TestQuantileKernel, Options) {
  auto ty = float64();
  QuantileOptions keep_nulls(/*q=*/0.5, QuantileOptions::LINEAR,
                             /*skip_nulls=*/false, /*min_count=*/0);
  QuantileOptions min_count(/*q=*/0.5, QuantileOptions::LINEAR,
                            /*skip_nulls=*/true, /*min_count=*/3);
  QuantileOptions keep_nulls_min_count(/*q=*/0.5, QuantileOptions::NEAREST,
                                       /*skip_nulls=*/false, /*min_count=*/3);

  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, 3.0]"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "1.0"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[1.0]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[null]")));

  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), min_count),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, null]"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "1.0"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));

  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, 3.0]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "1.0"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(Quantile(ScalarFromJSON(ty, "null"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
}

TEST(TestTDigestKernel, AllNullsOrNaNs) {
  const std::vector<std::vector<std::string>> tests = {
      {"[]"},
      {"[null, null]", "[]", "[null]"},
      {"[NaN]", "[NaN, NaN]", "[]"},
      {"[null, NaN, null]"},
      {"[NaN, NaN]", "[]", "[null]"},
  };

  for (const auto& json : tests) {
    auto chunked = ChunkedArrayFromJSON(float64(), json);
    ASSERT_OK_AND_ASSIGN(Datum out, TDigest(chunked, TDigestOptions()));
    auto out_array = out.make_array();
    ValidateOutput(*out_array);
    AssertArraysEqual(*ArrayFromJSON(float64(), "[null]"), *out_array, /*verbose=*/true);
  }
}

TEST(TestTDigestKernel, Decimal) {
  for (const auto& ty : {decimal128(3, 2), decimal256(3, 2)}) {
    ASSERT_OK_AND_ASSIGN(auto decimal_array,
                         TDigest(ArrayFromJSON(ty, R"(["1.00", "2.00", "3.25"])")));
    ASSERT_OK_AND_ASSIGN(auto float_array,
                         TDigest(ArrayFromJSON(float64(), "[1, 2, 3.25]")));
    AssertArraysApproxEqual(*float_array.make_array(), *decimal_array.make_array(),
                            /*verbose=*/true);
  }
}

TEST(TestTDigestKernel, Scalar) {
  for (const auto& ty : {float64(), int64(), uint64()}) {
    TDigestOptions options(std::vector<double>{0.0, 0.5, 1.0});
    EXPECT_THAT(TDigest(*MakeScalar(ty, 1), options),
                ResultWith(ArrayFromJSON(float64(), "[1, 1, 1]")));
    EXPECT_THAT(TDigest(MakeNullScalar(ty), options),
                ResultWith(ArrayFromJSON(float64(), "[null, null, null]")));
  }
}

TEST(TestTDigestKernel, Options) {
  auto ty = float64();
  TDigestOptions keep_nulls(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                            /*skip_nulls=*/false, /*min_count=*/0);
  TDigestOptions min_count(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                           /*skip_nulls=*/true, /*min_count=*/3);
  TDigestOptions keep_nulls_min_count(/*q=*/0.5, /*delta=*/100, /*buffer_size=*/500,
                                      /*skip_nulls=*/false, /*min_count=*/3);

  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, 3.0]"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "1.0"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[1.0]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "null"), keep_nulls),
              ResultWith(ArrayFromJSON(ty, "[null]")));

  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), min_count),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, null]"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "1.0"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "null"), min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));

  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, 3.0]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[2.0]")));
  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ArrayFromJSON(ty, "[1.0, 2.0, 3.0, null]"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "1.0"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
  EXPECT_THAT(TDigest(ScalarFromJSON(ty, "null"), keep_nulls_min_count),
              ResultWith(ArrayFromJSON(ty, "[null]")));
}

TEST(TestTDigestKernel, ApproximateMedian) {
  // This is a wrapper for TDigest
  for (const auto& ty : {float64(), int64(), uint16()}) {
    ScalarAggregateOptions keep_nulls(/*skip_nulls=*/false, /*min_count=*/0);
    ScalarAggregateOptions min_count(/*skip_nulls=*/true, /*min_count=*/3);
    ScalarAggregateOptions keep_nulls_min_count(/*skip_nulls=*/false, /*min_count=*/3);

    EXPECT_THAT(
        CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, 3]")}, &keep_nulls),
        ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, 3, null]")},
                             &keep_nulls),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(
        CallFunction("approximate_median",
                     {ChunkedArrayFromJSON(ty, {"[1, 2]", "[]", "[3]"})}, &keep_nulls),
        ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median",
                             {ChunkedArrayFromJSON(ty, {"[1, 2]", "[null]", "[3]"})},
                             &keep_nulls),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(
        CallFunction("approximate_median", {ScalarFromJSON(ty, "1")}, &keep_nulls),
        ResultWith(ScalarFromJSON(float64(), "1.0")));
    EXPECT_THAT(
        CallFunction("approximate_median", {ScalarFromJSON(ty, "null")}, &keep_nulls),
        ResultWith(ScalarFromJSON(float64(), "null")));

    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, 3, null]")},
                             &min_count),
                ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, null]")},
                             &min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(
        CallFunction("approximate_median",
                     {ChunkedArrayFromJSON(ty, {"[1, 2]", "[]", "[3]"})}, &keep_nulls),
        ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median",
                             {ChunkedArrayFromJSON(ty, {"[1, 2]", "[null]", "[3]"})},
                             &keep_nulls),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(CallFunction("approximate_median", {ScalarFromJSON(ty, "1")}, &min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(
        CallFunction("approximate_median", {ScalarFromJSON(ty, "null")}, &min_count),
        ResultWith(ScalarFromJSON(float64(), "null")));

    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, 3]")},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2]")},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(CallFunction("approximate_median",
                             {ChunkedArrayFromJSON(ty, {"[1, 2]", "[]", "[3]"})},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "2.0")));
    EXPECT_THAT(CallFunction("approximate_median",
                             {ChunkedArrayFromJSON(ty, {"[1, 2]", "[null]", "[3]"})},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(CallFunction("approximate_median", {ArrayFromJSON(ty, "[1, 2, 3, null]")},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(CallFunction("approximate_median", {ScalarFromJSON(ty, "1")},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
    EXPECT_THAT(CallFunction("approximate_median", {ScalarFromJSON(ty, "null")},
                             &keep_nulls_min_count),
                ResultWith(ScalarFromJSON(float64(), "null")));
  }
}

}  // namespace compute
}  // namespace arrow
