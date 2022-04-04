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
#include <cstdint>
#include <cstdio>
#include <functional>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/array/builder_decimal.h"
#include "arrow/compute/api_vector.h"
#include "arrow/buffer.h"
#include "arrow/testing/gtest_util.h"  // IntegralArrowTypes
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

#include "arrow/ipc/json_simple.h"

namespace arrow {
namespace compute {

using CumulativeTypes =
    testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type, Int8Type, Int16Type,
                   Int32Type, Int64Type, FloatType, DoubleType>;

template <typename T, typename OptionsType>
class TestCumulativeOp : public ::testing::Test {
 public:
  using ArrowType = T;
  using ArrowScalar = typename TypeTraits<T>::ScalarType;
  using CType = typename TypeTraits<T>::CType;

 protected:
  std::shared_ptr<DataType> type_singleton() { return default_type_instance<T>(); }

  std::shared_ptr<Array> array(const std::string& values) {
    return ArrayFromJSON(type_singleton(), values);
  }

  std::shared_ptr<ChunkedArray> chunked_array(const std::vector<std::string>& values) {
    return ChunkedArrayFromJSON(type_singleton(), values);
  }

  template <typename V = T>
  enable_if_t<!is_floating_type<V>::value, void> Assert(
      const std::string func, const std::shared_ptr<Array>& input,
      const std::shared_ptr<Array>& expected, const OptionsType& options) {
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func, {Datum(input)}, &options));

    AssertArraysEqual(*expected, *result.make_array(), false, EqualOptions::Defaults());
  }

  template <typename V = T>
  enable_if_t<!is_floating_type<V>::value, void> Assert(
      const std::string func, const std::shared_ptr<ChunkedArray>& input,
      const std::shared_ptr<ChunkedArray>& expected, const CumulativeGenericOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunction(func, {Datum(input)}, &options, nullptr));

    AssertChunkedEqual(*expected, result);
  }

  template <typename V = T>
  enable_if_floating_point<V> Assert(const std::string func,
                                     const std::shared_ptr<Array>& input,
                                     const std::shared_ptr<Array>& expected,
                                     const OptionsType& options) {
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction(func, {Datum(input)}, &options));

    AssertArraysApproxEqual(*expected, *result.make_array(), false,
                            EqualOptions::Defaults());
  }

  template <typename V = T>
  enable_if_floating_point<V> Assert(const std::string func,
                                     const std::shared_ptr<ChunkedArray>& input,
                                     const std::shared_ptr<ChunkedArray>& expected,
                                     const CumulativeGenericOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunction(func, {Datum(input)}, &options, nullptr));

    AssertChunkedApproxEquivalent(*expected, *result.chunks(), EqualOptions::Defaults());
  }
};

template <typename T>
class TestCumulativeSum : public TestCumulativeOp<T, CumulativeSumOptions> {
 public:
  using OptionsType = CumulativeSumOptions;
  using ArrowType = typename TestCumulativeOp<T, OptionsType>::ArrowType;
  using ArrowScalar = typename TestCumulativeOp<T, OptionsType>::ArrowScalar;
  using CType = typename TestCumulativeOp<T, OptionsType>::CType;

 protected:
  template <typename U = T>
  enable_if_parameter_free<U, OptionsType> generate_options(
      CType start = 0, bool skip_nulls = false, check_overflow = false) {
    return OptionsType(std::make_shared<ArrowScalar>(start), skip_nulls, check_overflow);
  }

  template <typename U = T>
  enable_if_t<is_time_type<U>::value || is_timestamp_type<U>::value,
              OptionsType>
  generate_options(CType start = 0, bool skip_nulls = false, check_overflow = false) {
    TimeUnit::type unit;
    switch (ArrowType::type_id) {
      case Type::TIME64:
        unit = TimeUnit::NANO;
        break;
      default:
        unit = TimeUnit::SECOND;
        break;
    }
    return OptionsType(std::make_shared<ArrowScalar>(start, unit),
                                    skip_nulls, check_overflow);
  }

  void Assert(const std::string& values, const std::string& expected,
              const OptionsType& options) {
    auto values_arr = TestCumulativeOp<T, OptionsType>::array(values);
    auto expected_arr = TestCumulativeOp<T, OptionsType>::array(expected);
    auto func_name = options.check_overflow ? "cumulative_sum_checked" : "cumulative_sum";
    TestCumulativeOp<T, OptionsType>::Assert(func_name, values_arr, expected_arr,
                                             options);
  }

  void Assert(const std::vector<std::string>& values,
              const std::vector<std::string>& expected,
              const CumulativeGenericOptions& options) {
    auto values_arr = TestCumulativeOp<T>::chunked_array(values);
    auto expected_arr = TestCumulativeOp<T>::chunked_array(expected);
    TestCumulativeOp<T>::Assert("cumulative_sum", values_arr, expected_arr, options);
  }
};

TYPED_TEST_SUITE(TestCumulativeSum, NumericArrowTypes);

TYPED_TEST(TestCumulativeSum, NoStartNoSkipNoNulls) {
  CumulativeSumOptions options = this->generate_options();
  auto empty = "[]";
  auto values = "[1, 2, 3, 4, 5, 6]";
  auto expected = "[1, 3, 6, 10, 15, 21]";
  auto chunked_values = {"[1, 2, 3]", "[4, 5, 6]"};
  auto chunked_expected = {"[1, 3, 6]", "[10, 15, 21]"};
  this->Assert(empty, empty, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, NoStartNoSkipHasNulls) {
  CumulativeSumOptions options = this->generate_options();
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  auto values = "[1, 2, null, 4, null, 6]";
  auto expected = "[1, 3, null, null, null, null]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, NoStartDoSkipNoNulls) {
  CumulativeSumOptions options = this->generate_options(0, true);
  auto empty = "[]";
  auto values = "[1, 2, 3, 4, 5, 6]";
  auto expected = "[1, 3, 6, 10, 15, 21]";
  this->Assert(empty, empty, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, NoStartDoSkipHasNulls) {
  CumulativeSumOptions options = this->generate_options(0, true);
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  auto values = "[1, 2, null, 4, null, 6]";
  auto expected = "[1, 3, null, 7, null, 13]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, HasStartNoSkipNoNulls) {
  CumulativeSumOptions options = this->generate_options(10);
  auto empty = "[]";
  auto values = "[1, 2, 3, 4, 5, 6]";
  auto expected = "[11, 13, 16, 20, 25, 31]";
  this->Assert(empty, empty, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, HasStartNoSkipHasNulls) {
  CumulativeSumOptions options = this->generate_options(10);
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  auto values = "[1, 2, null, 4, null, 6]";
  auto expected = "[11, 13, null, null, null, null]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, HasStartDoSkipNoNulls) {
  CumulativeSumOptions options = this->generate_options(10, true);
  auto empty = "[]";
  auto values = "[1, 2, 3, 4, 5, 6]";
  auto expected = "[11, 13, 16, 20, 25, 31]";
  this->Assert(empty, empty, options);
  this->Assert(values, expected, options);
}

TYPED_TEST(TestCumulativeSum, HasStartDoSkipHasNulls) {
  CumulativeSumOptions options = this->generate_options(10, true);
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  auto values = "[1, 2, null, 4, null, 6]";
  auto expected = "[11, 13, null, 17, null, 23]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);
  this->Assert(values, expected, options);
}

}  // namespace compute
}  // namespace arrow
