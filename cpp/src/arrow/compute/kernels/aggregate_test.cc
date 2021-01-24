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
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/aggregate_internal.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {

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
    internal::BitmapReader reader(array.null_bitmap_data(), array.offset(),
                                  array.length());
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

template <typename ArrowType>
void ValidateSum(const Array& input, Datum expected) {
  using OutputType = typename FindAccumulatorType<ArrowType>::Type;

  ASSERT_OK_AND_ASSIGN(Datum result, Sum(input));
  DatumEqual<OutputType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateSum(const std::shared_ptr<ChunkedArray>& input, Datum expected) {
  using OutputType = typename FindAccumulatorType<ArrowType>::Type;

  ASSERT_OK_AND_ASSIGN(Datum result, Sum(input));
  DatumEqual<OutputType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateSum(const char* json, Datum expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum<ArrowType>(*array, expected);
}

template <typename ArrowType>
void ValidateSum(const std::vector<std::string>& json, Datum expected) {
  auto array = ChunkedArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateSum<ArrowType>(array, expected);
}

template <typename ArrowType>
void ValidateSum(const Array& array) {
  ValidateSum<ArrowType>(array, NaiveSum<ArrowType>(array));
}

using UnaryOp = Result<Datum>(const Datum&, ExecContext*);

template <UnaryOp& Op, typename ScalarType>
void ValidateBooleanAgg(const std::string& json,
                        const std::shared_ptr<ScalarType>& expected) {
  auto array = ArrayFromJSON(boolean(), json);
  auto exp = Datum(expected);
  ASSERT_OK_AND_ASSIGN(Datum result, Op(array, nullptr));
  ASSERT_TRUE(result.Equals(exp));
}

TEST(TestBooleanAggregation, Sum) {
  ValidateBooleanAgg<Sum>("[]", std::make_shared<UInt64Scalar>());
  ValidateBooleanAgg<Sum>("[null]", std::make_shared<UInt64Scalar>());
  ValidateBooleanAgg<Sum>("[null, false]", std::make_shared<UInt64Scalar>(0));
  ValidateBooleanAgg<Sum>("[true]", std::make_shared<UInt64Scalar>(1));
  ValidateBooleanAgg<Sum>("[true, false, true]", std::make_shared<UInt64Scalar>(2));
  ValidateBooleanAgg<Sum>("[true, false, true, true, null]",
                          std::make_shared<UInt64Scalar>(3));
}

TEST(TestBooleanAggregation, Mean) {
  ValidateBooleanAgg<Mean>("[]", std::make_shared<DoubleScalar>());
  ValidateBooleanAgg<Mean>("[null]", std::make_shared<DoubleScalar>());
  ValidateBooleanAgg<Mean>("[null, false]", std::make_shared<DoubleScalar>(0));
  ValidateBooleanAgg<Mean>("[true]", std::make_shared<DoubleScalar>(1));
  ValidateBooleanAgg<Mean>("[true, false, true, false]",
                           std::make_shared<DoubleScalar>(0.5));
  ValidateBooleanAgg<Mean>("[true, null]", std::make_shared<DoubleScalar>(1));
  ValidateBooleanAgg<Mean>("[true, null, false, true, true]",
                           std::make_shared<DoubleScalar>(0.75));
  ValidateBooleanAgg<Mean>("[true, null, false, false, false]",
                           std::make_shared<DoubleScalar>(0.25));
}

template <typename ArrowType>
class TestNumericSumKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestNumericSumKernel, SimpleSum) {
  using SumType = typename FindAccumulatorType<TypeParam>::Type;
  using ScalarType = typename TypeTraits<SumType>::ScalarType;
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

  chunks = {};
  ValidateSum<TypeParam>(chunks,
                         Datum(std::make_shared<ScalarType>()));  // null

  const T expected_result = static_cast<T>(14);
  ValidateSum<TypeParam>("[1, null, 3, null, 3, null, 7]",
                         Datum(std::make_shared<ScalarType>(expected_result)));
}

template <typename ArrowType>
class TestRandomNumericSumKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericSumKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericSumKernel, RandomArraySum) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  // Test size up to 1<<13 (8192).
  for (size_t i = 3; i < 14; i += 2) {
    for (auto null_probability : {0.0, 0.001, 0.1, 0.5, 0.999, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateSum<TypeParam>(*array);
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
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Test overflow on the original type
    auto array = rand.Numeric<TypeParam>(length, max - 200, max - 100, null_probability);
    ValidateSum<TypeParam>(*array);
    array = rand.Numeric<TypeParam>(length, min + 100, min + 200, null_probability);
    ValidateSum<TypeParam>(*array);
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
  CountOptions all = CountOptions(CountOptions::COUNT_NON_NULL);
  CountOptions nulls = CountOptions(CountOptions::COUNT_NULL);

  ASSERT_OK_AND_ASSIGN(Datum result, Count(input, all));
  AssertDatumsEqual(result, Datum(expected.first));

  ASSERT_OK_AND_ASSIGN(result, Count(input, nulls));
  AssertDatumsEqual(result, Datum(expected.second));
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
void ValidateMean(const Array& input, Datum expected) {
  using OutputType = typename FindAccumulatorType<DoubleType>::Type;

  ASSERT_OK_AND_ASSIGN(Datum result, Mean(input));
  DatumEqual<OutputType>::EnsureEqual(result, expected);
}

template <typename ArrowType>
void ValidateMean(const char* json, Datum expected) {
  auto array = ArrayFromJSON(TypeTraits<ArrowType>::type_singleton(), json);
  ValidateMean<ArrowType>(*array, expected);
}

template <typename ArrowType>
void ValidateMean(const Array& array) {
  ValidateMean<ArrowType>(array, NaiveMean<ArrowType>(array));
}

template <typename ArrowType>
class TestMeanKernelNumeric : public ::testing::Test {};

TYPED_TEST_SUITE(TestMeanKernelNumeric, NumericArrowTypes);
TYPED_TEST(TestMeanKernelNumeric, SimpleMean) {
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

  ValidateMean<TypeParam>("[]", Datum(std::make_shared<ScalarType>()));

  ValidateMean<TypeParam>("[null]", Datum(std::make_shared<ScalarType>()));

  ValidateMean<TypeParam>("[1, null, 1]", Datum(std::make_shared<ScalarType>(1.0)));

  ValidateMean<TypeParam>("[1, 2, 3, 4, 5, 6, 7, 8]",
                          Datum(std::make_shared<ScalarType>(4.5)));

  ValidateMean<TypeParam>("[0, 0, 0, 0, 0, 0, 0, 0]",
                          Datum(std::make_shared<ScalarType>(0.0)));

  ValidateMean<TypeParam>("[1, 1, 1, 1, 1, 1, 1, 1]",
                          Datum(std::make_shared<ScalarType>(1.0)));
}

template <typename ArrowType>
class TestRandomNumericMeanKernel : public ::testing::Test {};

TYPED_TEST_SUITE(TestRandomNumericMeanKernel, NumericArrowTypes);
TYPED_TEST(TestRandomNumericMeanKernel, RandomArrayMean) {
  auto rand = random::RandomArrayGenerator(0x8afc055);
  // Test size up to 1<<13 (8192).
  for (size_t i = 3; i < 14; i += 2) {
    for (auto null_probability : {0.0, 0.001, 0.1, 0.5, 0.999, 1.0}) {
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        auto array = rand.Numeric<TypeParam>(length, 0, 100, null_probability);
        ValidateMean<TypeParam>(*array);
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
  for (auto null_probability : {0.0, 0.1, 0.5, 1.0}) {
    // Test overflow on the original type
    auto array = rand.Numeric<TypeParam>(length, max - 200, max - 100, null_probability);
    ValidateMean<TypeParam>(*array);
    array = rand.Numeric<TypeParam>(length, min + 100, min + 200, null_probability);
    ValidateMean<TypeParam>(*array);
  }
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
                      const MinMaxOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array, options));
    const StructScalar& value = out.scalar_as<StructScalar>();

    const auto& out_min = checked_cast<const ScalarType&>(*value.value[0]);
    ASSERT_EQ(expected_min, out_min.value);

    const auto& out_max = checked_cast<const ScalarType&>(*value.value[1]);
    ASSERT_EQ(expected_max, out_max.value);
  }

  void AssertMinMaxIs(const std::string& json, c_type expected_min, c_type expected_max,
                      const MinMaxOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertMinMaxIs(array, expected_min, expected_max, options);
  }

  void AssertMinMaxIs(const std::vector<std::string>& json, c_type expected_min,
                      c_type expected_max, const MinMaxOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertMinMaxIs(array, expected_min, expected_max, options);
  }

  void AssertMinMaxIsNull(const Datum& array, const MinMaxOptions& options) {
    ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array, options));

    const StructScalar& value = out.scalar_as<StructScalar>();
    for (const auto& val : value.value) {
      ASSERT_FALSE(val->is_valid);
    }
  }

  void AssertMinMaxIsNull(const std::string& json, const MinMaxOptions& options) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertMinMaxIsNull(array, options);
  }

  void AssertMinMaxIsNull(const std::vector<std::string>& json,
                          const MinMaxOptions& options) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertMinMaxIsNull(array, options);
  }

  std::shared_ptr<DataType> type_singleton() { return Traits::type_singleton(); }
};

template <typename ArrowType>
class TestIntegerMinMaxKernel : public TestPrimitiveMinMaxKernel<ArrowType> {};

template <typename ArrowType>
class TestFloatingMinMaxKernel : public TestPrimitiveMinMaxKernel<ArrowType> {};

class TestBooleanMinMaxKernel : public TestPrimitiveMinMaxKernel<BooleanType> {};

TEST_F(TestBooleanMinMaxKernel, Basics) {
  MinMaxOptions options;
  std::vector<std::string> chunked_input0 = {"[]", "[]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[true, null]", "[null, false]"};

  // SKIP nulls by default
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

  options = MinMaxOptions(MinMaxOptions::EMIT_NULL);
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
}

TYPED_TEST_SUITE(TestIntegerMinMaxKernel, IntegralArrowTypes);
TYPED_TEST(TestIntegerMinMaxKernel, Basics) {
  MinMaxOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 1, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[5, null, 2, 3, 4]", "[9, 1, 2, 3, 4]"};
  std::vector<std::string> chunked_input3 = {"[5, 1, 2, 3, null]", "[9, 1, null, 3, 4]"};

  // SKIP nulls by default
  this->AssertMinMaxIsNull("[]", options);
  this->AssertMinMaxIsNull("[null, null, null]", options);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, null, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);
  this->AssertMinMaxIs(chunked_input2, 1, 9, options);
  this->AssertMinMaxIs(chunked_input3, 1, 9, options);

  options = MinMaxOptions(MinMaxOptions::EMIT_NULL);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  // output null
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  // output null
  this->AssertMinMaxIsNull(chunked_input1, options);
  this->AssertMinMaxIsNull(chunked_input2, options);
  this->AssertMinMaxIsNull(chunked_input3, options);
}

TYPED_TEST_SUITE(TestFloatingMinMaxKernel, RealArrowTypes);
TYPED_TEST(TestFloatingMinMaxKernel, Floats) {
  MinMaxOptions options;
  std::vector<std::string> chunked_input1 = {"[5, 1, 2, 3, 4]", "[9, 1, null, 3, 4]"};
  std::vector<std::string> chunked_input2 = {"[5, null, 2, 3, 4]", "[9, 1, 2, 3, 4]"};
  std::vector<std::string> chunked_input3 = {"[5, 1, 2, 3, null]", "[9, 1, null, 3, 4]"};

  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, null, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs("[5, Inf, 2, 3, 4]", 2.0, INFINITY, options);
  this->AssertMinMaxIs("[5, NaN, 2, 3, 4]", 2, 5, options);
  this->AssertMinMaxIs("[5, -Inf, 2, 3, 4]", -INFINITY, 5, options);
  this->AssertMinMaxIs(chunked_input1, 1, 9, options);
  this->AssertMinMaxIs(chunked_input2, 1, 9, options);
  this->AssertMinMaxIs(chunked_input3, 1, 9, options);

  options = MinMaxOptions(MinMaxOptions::EMIT_NULL);
  this->AssertMinMaxIs("[5, 1, 2, 3, 4]", 1, 5, options);
  this->AssertMinMaxIs("[5, -Inf, 2, 3, 4]", -INFINITY, 5, options);
  // output null
  this->AssertMinMaxIsNull("[5, null, 2, 3, 4]", options);
  // output null
  this->AssertMinMaxIsNull("[5, -Inf, null, 3, 4]", options);
  // output null
  this->AssertMinMaxIsNull(chunked_input1, options);
  this->AssertMinMaxIsNull(chunked_input2, options);
  this->AssertMinMaxIsNull(chunked_input3, options);
}

TYPED_TEST(TestFloatingMinMaxKernel, DefaultOptions) {
  auto values = ArrayFromJSON(this->type_singleton(), "[0, 1, 2, 3, 4]");

  ASSERT_OK_AND_ASSIGN(auto no_options_provided, CallFunction("min_max", {values}));

  auto default_options = MinMaxOptions::Defaults();
  ASSERT_OK_AND_ASSIGN(auto explicit_defaults,
                       CallFunction("min_max", {values}, &default_options));

  AssertDatumsEqual(explicit_defaults, no_options_provided);
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
    internal::BitmapReader reader(array.null_bitmap_data(), array.offset(),
                                  array.length());
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
    internal::BitmapReader reader(array.null_bitmap_data(), array.offset(),
                                  array.length());
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
void ValidateMinMax(const Array& array) {
  using Traits = TypeTraits<ArrowType>;
  using ScalarType = typename Traits::ScalarType;

  ASSERT_OK_AND_ASSIGN(Datum out, MinMax(array));
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
  // Test size up to 1<<11 (2048).
  for (size_t i = 3; i < 12; i += 2) {
    for (auto null_probability : {0.0, 0.01, 0.1, 0.5, 0.99, 1.0}) {
      int64_t base_length = (1UL << i) + 2;
      auto array = rand.Numeric<TypeParam>(base_length, 0, 100, null_probability);
      for (auto length_adjust : {-2, -1, 0, 1, 2}) {
        int64_t length = (1UL << i) + length_adjust;
        ValidateMinMax<TypeParam>(*array->Slice(0, length));
      }
    }
  }
}

//
// Any
//

class TestPrimitiveAnyKernel : public ::testing::Test {
 public:
  void AssertAnyIs(const Datum& array, bool expected) {
    ASSERT_OK_AND_ASSIGN(Datum out, Any(array));
    const BooleanScalar& out_any = out.scalar_as<BooleanScalar>();
    const auto expected_any = static_cast<const BooleanScalar>(expected);
    ASSERT_EQ(out_any, expected_any);
  }

  void AssertAnyIs(const std::string& json, bool expected) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertAnyIs(array, expected);
  }

  void AssertAnyIs(const std::vector<std::string>& json, bool expected) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertAnyIs(array, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<BooleanType>::type_singleton();
  }
};

class TestAnyKernel : public TestPrimitiveAnyKernel {};

TEST_F(TestAnyKernel, Basics) {
  std::vector<std::string> chunked_input0 = {"[]", "[true]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[false, null]", "[null, false]"};
  std::vector<std::string> chunked_input4 = {"[true, null]", "[null, false]"};

  this->AssertAnyIs("[]", false);
  this->AssertAnyIs("[false]", false);
  this->AssertAnyIs("[true, false]", true);
  this->AssertAnyIs("[null, null, null]", false);
  this->AssertAnyIs("[false, false, false]", false);
  this->AssertAnyIs("[false, false, false, null]", false);
  this->AssertAnyIs("[true, null, true, true]", true);
  this->AssertAnyIs("[false, null, false, true]", true);
  this->AssertAnyIs("[true, null, false, true]", true);
  this->AssertAnyIs(chunked_input0, true);
  this->AssertAnyIs(chunked_input1, true);
  this->AssertAnyIs(chunked_input2, false);
  this->AssertAnyIs(chunked_input3, false);
  this->AssertAnyIs(chunked_input4, true);
}

//
// All
//

class TestPrimitiveAllKernel : public ::testing::Test {
 public:
  void AssertAllIs(const Datum& array, bool expected) {
    ASSERT_OK_AND_ASSIGN(Datum out, All(array));
    const BooleanScalar& out_all = out.scalar_as<BooleanScalar>();
    const auto expected_all = static_cast<const BooleanScalar>(expected);
    ASSERT_EQ(out_all, expected_all);
  }

  void AssertAllIs(const std::string& json, bool expected) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertAllIs(array, expected);
  }

  void AssertAllIs(const std::vector<std::string>& json, bool expected) {
    auto array = ChunkedArrayFromJSON(type_singleton(), json);
    AssertAllIs(array, expected);
  }

  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<BooleanType>::type_singleton();
  }
};

class TestAllKernel : public TestPrimitiveAllKernel {};

TEST_F(TestAllKernel, Basics) {
  std::vector<std::string> chunked_input0 = {"[]", "[true]"};
  std::vector<std::string> chunked_input1 = {"[true, true, null]", "[true, null]"};
  std::vector<std::string> chunked_input2 = {"[false, false, false]", "[false]"};
  std::vector<std::string> chunked_input3 = {"[false, null]", "[null, false]"};
  std::vector<std::string> chunked_input4 = {"[true, null]", "[null, false]"};
  std::vector<std::string> chunked_input5 = {"[false, null]", "[null, true]"};

  this->AssertAllIs("[]", true);
  this->AssertAllIs("[false]", false);
  this->AssertAllIs("[true, false]", false);
  this->AssertAllIs("[null, null, null]", true);
  this->AssertAllIs("[false, false, false]", false);
  this->AssertAllIs("[false, false, false, null]", false);
  this->AssertAllIs("[true, null, true, true]", true);
  this->AssertAllIs("[false, null, false, true]", false);
  this->AssertAllIs("[true, null, false, true]", false);
  this->AssertAllIs(chunked_input0, true);
  this->AssertAllIs(chunked_input1, true);
  this->AssertAllIs(chunked_input2, false);
  this->AssertAllIs(chunked_input3, false);
  this->AssertAllIs(chunked_input4, false);
  this->AssertAllIs(chunked_input5, false);
}

//
// Mode
//

template <typename T>
class TestPrimitiveModeKernel : public ::testing::Test {
 public:
  using ArrowType = T;
  using Traits = TypeTraits<ArrowType>;
  using CType = typename ArrowType::c_type;

  void AssertModesAre(const Datum& array, const int n,
                      const std::vector<CType>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    ASSERT_OK_AND_ASSIGN(Datum out, Mode(array, ModeOptions{n}));
    ASSERT_OK(out.make_array()->ValidateFull());
    const StructArray out_array(out.array());
    ASSERT_EQ(out_array.length(), expected_modes.size());
    ASSERT_EQ(out_array.num_fields(), 2);

    const CType* out_modes = out_array.field(0)->data()->GetValues<CType>(1);
    const int64_t* out_counts = out_array.field(1)->data()->GetValues<int64_t>(1);
    for (int i = 0; i < out_array.length(); ++i) {
      // equal or nan equal
      ASSERT_TRUE(
          (expected_modes[i] == out_modes[i]) ||
          (expected_modes[i] != expected_modes[i] && out_modes[i] != out_modes[i]));
      ASSERT_EQ(expected_counts[i], out_counts[i]);
    }
  }

  void AssertModesAre(const std::string& json, const int n,
                      const std::vector<CType>& expected_modes,
                      const std::vector<int64_t>& expected_counts) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesAre(array, n, expected_modes, expected_counts);
  }

  void AssertModeIs(const Datum& array, CType expected_mode, int64_t expected_count) {
    AssertModesAre(array, 1, {expected_mode}, {expected_count});
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

  void AssertModesEmpty(const Datum& array, int n) {
    ASSERT_OK_AND_ASSIGN(Datum out, Mode(array, ModeOptions{n}));
    ASSERT_OK(out.make_array()->ValidateFull());
    ASSERT_EQ(out.array()->length, 0);
  }

  void AssertModesEmpty(const std::string& json, int n = 1) {
    auto array = ArrayFromJSON(type_singleton(), json);
    AssertModesEmpty(array, n);
  }

  void AssertModesEmpty(const std::vector<std::string>& json, int n = 1) {
    auto chunked = ChunkedArrayFromJSON(type_singleton(), json);
    AssertModesEmpty(chunked, n);
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

  this->AssertModesAre("[false, false, true, true, true, false]", 2, {false, true},
                       {3, 3});
  this->AssertModesAre("[true, null, false, false, null, true, null, null, true]", 100,
                       {true, false}, {3, 2});
  this->AssertModesEmpty({"[null, null]", "[]", "[null]"}, 4);
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
  internal::BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
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
void CheckModeWithRange(CTYPE range_min, CTYPE range_max) {
  auto rand = random::RandomArrayGenerator(0x5487655);
  // 32K items (>= counting mode cutoff) within range, 10% null
  auto array = rand.Numeric<ArrowType>(32 * 1024, range_min, range_max, 0.1);

  auto expected = NaiveMode<ArrowType>(*array);
  ASSERT_OK_AND_ASSIGN(Datum out, Mode(array));
  ASSERT_OK(out.make_array()->ValidateFull());
  const StructArray out_array(out.array());
  ASSERT_EQ(out_array.length(), 1);
  ASSERT_EQ(out_array.num_fields(), 2);

  const CTYPE* out_modes = out_array.field(0)->data()->GetValues<CTYPE>(1);
  const int64_t* out_counts = out_array.field(1)->data()->GetValues<int64_t>(1);
  ASSERT_EQ(out_modes[0], expected.mode);
  ASSERT_EQ(out_counts[0], expected.count);
}

TEST_F(TestInt32ModeKernel, SmallValueRange) {
  // Small value range => should exercise counter-based Mode implementation
  CheckModeWithRange<ArrowType>(-100, 100);
}

TEST_F(TestInt32ModeKernel, LargeValueRange) {
  // Large value range => should exercise hashmap-based Mode implementation
  CheckModeWithRange<ArrowType>(-10000000, 10000000);
}

//
// Variance/Stddev
//

template <typename ArrowType>
class TestPrimitiveVarStdKernel : public ::testing::Test {
 public:
  using Traits = TypeTraits<ArrowType>;
  using ScalarType = typename TypeTraits<DoubleType>::ScalarType;

  void AssertVarStdIs(const Array& array, const VarianceOptions& options,
                      double expected_var) {
    AssertVarStdIsInternal(array, options, expected_var);
  }

  void AssertVarStdIs(const std::shared_ptr<ChunkedArray>& array,
                      const VarianceOptions& options, double expected_var) {
    AssertVarStdIsInternal(array, options, expected_var);
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
  void AssertVarStdIsInternal(const Datum& array, const VarianceOptions& options,
                              double expected_var) {
    ASSERT_OK_AND_ASSIGN(Datum out_var, Variance(array, options));
    ASSERT_OK_AND_ASSIGN(Datum out_std, Stddev(array, options));
    auto var = checked_cast<const ScalarType*>(out_var.scalar().get());
    auto std = checked_cast<const ScalarType*>(out_std.scalar().get());
    ASSERT_TRUE(var->is_valid && std->is_valid);
    ASSERT_DOUBLE_EQ(std->value * std->value, var->value);
    ASSERT_DOUBLE_EQ(var->value, expected_var);  // < 4ULP
  }

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
  internal::BitmapReader reader(array.null_bitmap_data(), array.offset(), array.length());
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

typedef ::testing::Types<Int32Type, UInt32Type, Int64Type, UInt64Type, FloatType,
                         DoubleType>
    VarStdRandomTypes;

TYPED_TEST_SUITE(TestVarStdKernelRandom, VarStdRandomTypes);
TYPED_TEST(TestVarStdKernelRandom, Basics) {
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
  auto typed_array = std::static_pointer_cast<ArrayType>(array->Slice(0, total_size));
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
  auto int32_array = std::static_pointer_cast<Int32Array>(array);
  std::tie(var_population, var_sample) = WelfordVar(*int32_array);

  this->AssertVarStdIs(*array, VarianceOptions{0}, var_population);
  this->AssertVarStdIs(*array, VarianceOptions{1}, var_sample);
}
#endif

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

      ASSERT_OK_AND_ASSIGN(Datum out, Quantile(array, options));
      const auto& out_array = out.make_array();
      ASSERT_OK(out_array->ValidateFull());
      ASSERT_EQ(out_array->length(), options.q.size());
      ASSERT_EQ(out_array->null_count(), 0);
      ASSERT_EQ(out_array->type(), expected[0][i].type());

      if (out_array->type() == float64()) {
        const double* quantiles = out_array->data()->GetValues<double>(1);
        for (int64_t j = 0; j < out_array->length(); ++j) {
          const auto& numeric_scalar =
              std::static_pointer_cast<DoubleScalar>(expected[j][i].scalar());
          ASSERT_TRUE((quantiles[j] == numeric_scalar->value) ||
                      (std::isnan(quantiles[j]) && std::isnan(numeric_scalar->value)));
        }
      } else {
        ASSERT_EQ(out_array->type(), type_singleton());
        const CType* quantiles = out_array->data()->GetValues<CType>(1);
        for (int64_t j = 0; j < out_array->length(); ++j) {
          const auto& numeric_scalar =
              std::static_pointer_cast<NumericScalar<ArrowType>>(expected[j][i].scalar());
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
      ASSERT_OK(out.make_array()->ValidateFull());
      ASSERT_EQ(out.array()->length, 0);
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

  std::vector<enum QuantileOptions::Interpolation> interpolations_{
      QuantileOptions::LINEAR, QuantileOptions::LOWER, QuantileOptions::HIGHER,
      QuantileOptions::NEAREST, QuantileOptions::MIDPOINT};
};

template <typename ArrowType>
class TestIntegerQuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {};

template <typename ArrowType>
class TestFloatingQuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {};

template <typename ArrowType>
class TestInt64QuantileKernel : public TestPrimitiveQuantileKernel<ArrowType> {};

#define INTYPE(x) Datum(static_cast<typename TypeParam::c_type>(x))
#define DOUBLE(x) Datum(static_cast<double>(x))
// output type per interplation: linear, lower, higher, nearest, midpoint
#define O(a, b, c, d, e) \
  { DOUBLE(a), INTYPE(b), INTYPE(c), INTYPE(d), DOUBLE(e) }

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
}

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
}
#endif

// Test big int64 numbers cannot be precisely presented by double
TYPED_TEST_SUITE(TestInt64QuantileKernel, Int64Type);
TYPED_TEST(TestInt64QuantileKernel, Int64) {
  this->AssertQuantileIs(
      "[9223372036854775806, 9223372036854775807]", 0.5,
      O(9.223372036854776e+18, 9223372036854775806, 9223372036854775807,
        9223372036854775806, 9.223372036854776e+18));
}

#undef INTYPE
#undef DOUBLE
#undef O

#ifndef __MINGW32__
class TestRandomQuantileKernel : public TestPrimitiveQuantileKernel<Int32Type> {
 public:
  void CheckQuantiles(int64_t array_size, int64_t num_quantiles) {
    auto rand = random::RandomArrayGenerator(0x5487658);
    // set a small value range to exercise input array with equal values
    const auto array = rand.Numeric<Int32Type>(array_size, -100, 200, 0.1);

    std::vector<double> quantiles;
    random_real(num_quantiles, 0x5487658, 0.0, 1.0, &quantiles);
    // make sure to exercise 0 and 1 quantiles
    *std::min_element(quantiles.begin(), quantiles.end()) = 0;
    *std::max_element(quantiles.begin(), quantiles.end()) = 1;

    this->AssertQuantilesAre(array, QuantileOptions{quantiles},
                             NaiveQuantile(*array, quantiles));
  }

 private:
  std::vector<std::vector<Datum>> NaiveQuantile(const Array& array,
                                                const std::vector<double>& quantiles) {
    // copy and sort input array
    std::vector<int32_t> input(array.length() - array.null_count());
    const int32_t* values = array.data()->GetValues<int32_t>(1);
    const auto bitmap = array.null_bitmap_data();
    int64_t index = 0;
    for (int64_t i = 0; i < array.length(); ++i) {
      if (BitUtil::GetBit(bitmap, i)) {
        input[index++] = values[i];
      }
    }
    std::sort(input.begin(), input.end());

    std::vector<std::vector<Datum>> output(quantiles.size(),
                                           std::vector<Datum>(interpolations_.size()));
    for (uint64_t i = 0; i < interpolations_.size(); ++i) {
      const auto interp = interpolations_[i];
      for (uint64_t j = 0; j < quantiles.size(); ++j) {
        output[j][i] = GetQuantile(input, quantiles[j], interp);
      }
    }
    return output;
  }

  Datum GetQuantile(const std::vector<int32_t>& input, double q,
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
          return Datum(static_cast<double>(input[lower_index]));
        } else {
          return Datum(fraction * input[lower_index + 1] +
                       (1 - fraction) * input[lower_index]);
        }
      case QuantileOptions::MIDPOINT:
        if (fraction == 0) {
          return Datum(static_cast<double>(input[lower_index]));
        } else {
          return Datum(input[lower_index] / 2.0 + input[lower_index + 1] / 2.0);
        }
      default:
        return Datum(NAN);
    }
  }
};

TEST_F(TestRandomQuantileKernel, Normal) {
  this->CheckQuantiles(/*array_size=*/10000, /*num_quantiles=*/100);
}

TEST_F(TestRandomQuantileKernel, Overlapped) {
  // much more quantiles than array size => many overlaps
  this->CheckQuantiles(/*array_size=*/999, /*num_quantiles=*/9999);
}
#endif

}  // namespace compute
}  // namespace arrow
