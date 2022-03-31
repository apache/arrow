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

//  Date32Type, Date64Type, Time32Type, Time64Type,
//  TimestampType, DurationType, MonthIntervalType>;

template <typename T>
class TestCumulativeOp : public ::testing::Test {
 public:
  using ArrowType = T;
  using ArrowScalar = typename TypeTraits<T>::ScalarType;
  using CType = typename TypeTraits<T>::CType;

 protected:
  std::shared_ptr<DataType> type_singleton() { return default_type_instance<T>(); }

  std::shared_ptr<Array> array(const std::string& value) {
    return ArrayFromJSON(type_singleton(), value);
  }

  void Assert(const std::string func, const std::shared_ptr<Array>& input,
              const std::shared_ptr<Array>& expected,
              const CumulativeGenericOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunction(func, {Datum(input)}, &options, nullptr));

    AssertArraysEqual(*expected, *result.make_array(), false, EqualOptions::Defaults());
  }
};

template <typename T>
class TestCumulativeSum : public TestCumulativeOp<T> {
 public:
  using ArrowType = typename TestCumulativeOp<T>::ArrowType;
  using ArrowScalar = typename TestCumulativeOp<T>::ArrowScalar;
  using CType = typename TestCumulativeOp<T>::CType;

 protected:
  template <typename U = T>
  enable_if_parameter_free<U, CumulativeGenericOptions> generate_options(
      CType start = 0, bool skip_nulls = false) {
    return CumulativeGenericOptions(std::make_shared<ArrowScalar>(start), skip_nulls);
  }

  template <typename U = T>
  enable_if_t<is_time_type<U>::value || is_timestamp_type<U>::value,
              CumulativeGenericOptions>
  generate_options(CType start = 0, bool skip_nulls = false) {
    TimeUnit::type unit;
    switch (ArrowType::type_id) {
      case Type::TIME64:
        unit = TimeUnit::NANO;
        break;
      default:
        unit = TimeUnit::SECOND;
        break;
    }
    return CumulativeGenericOptions(std::make_shared<ArrowScalar>(start, unit),
                                    skip_nulls);
  }

  void Assert(const std::string& values, const std::string& expected,
              const CumulativeGenericOptions& options) {
    auto values_arr = TestCumulativeOp<T>::array(values);
    auto expected_arr = TestCumulativeOp<T>::array(expected);
    TestCumulativeOp<T>::Assert("cumulative_sum", values_arr, expected_arr, options);
  }
};

TYPED_TEST_SUITE(TestCumulativeSum, CumulativeTypes);

TYPED_TEST(TestCumulativeSum, NoStartNoSkipNoNulls) {
  CumulativeGenericOptions options = this->generate_options();
  auto empty = "[]";
  this->Assert(empty, empty, options);

  if (this->type_singleton()->id() == Type::DATE64) {
    auto values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
    auto expected = "[0, 86400000, 259200000, 518400000, 864000000, 1296000000]";
    this->Assert(values, expected, options);
  } else {
    auto values = "[1, 2, 3, 4, 5, 6]";
    auto expected = "[1, 3, 6, 10, 15, 21]";
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, NoStartNoSkipHasNulls) {
  CumulativeGenericOptions options = this->generate_options();
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);

  if (this->type_singleton()->id() == Type::DATE64) {
    auto values = "[0, 86400000, null, 259200000, null, 432000000]";
    auto expected = "[0, 86400000, null, null, null, null]";
    this->Assert(values, expected, options);
  } else {
    auto values = "[1, 2, null, 4, null, 6]";
    auto expected = "[1, 3, null, null, null, null]";
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, NoStartDoSkipNoNulls) {
  CumulativeGenericOptions options = this->generate_options(0, true);
  auto empty = "[]";
  this->Assert(empty, empty, options);

  if (this->type_singleton()->id() == Type::DATE64) {
    auto values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
    auto expected = "[0, 86400000, 259200000, 518400000, 864000000, 1296000000]";
    this->Assert(values, expected, options);
  } else {
    auto values = "[1, 2, 3, 4, 5, 6]";
    auto expected = "[1, 3, 6, 10, 15, 21]";
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, NoStartDoSkipHasNulls) {
  CumulativeGenericOptions options = this->generate_options(0, true);
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";
  this->Assert(one_null, one_null, options);
  this->Assert(three_null, three_null, options);

  if (this->type_singleton()->id() == Type::DATE64) {
    auto values = "[0, 86400000, null, 259200000, null, 432000000]";
    auto expected = "[0, 86400000, null, 345600000, null, 777600000]";
    this->Assert(values, expected, options);
  } else {
    auto values = "[1, 2, null, 4, null, 6]";
    auto expected = "[1, 3, null, 7, null, 13]";
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, HasStartNoSkipNoNulls) {
  auto empty = "[]";

  if (this->type_singleton()->id() == Type::DATE64) {
    // CumulativeGenericOptions options = this->generate_options(86400000);
    // auto values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
    // auto expected = "[86400000, 172800000, 345600000, 604800000, 950400000,
    // 1382400000]";
    // this->Assert(empty, empty, options);
    // this->Assert(values, expected, options);
  } else {
    CumulativeGenericOptions options = this->generate_options(10);
    auto values = "[1, 2, 3, 4, 5, 6]";
    auto expected = "[11, 13, 16, 20, 25, 31]";
    this->Assert(empty, empty, options);
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, HasStartNoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";

  if (this->type_singleton()->id() == Type::DATE64) {
    // CumulativeGenericOptions options = this->generate_options(86400000);
    // auto values = "[0, 86400000, null, 259200000, null, 432000000]";
    // auto expected = "[86400000, 172800000, null, null, null, null]";
    // this->Assert(one_null, one_null, options);
    // this->Assert(three_null, three_null, options);
    // this->Assert(values, expected, options);
  } else {
    CumulativeGenericOptions options = this->generate_options(10);
    auto values = "[1, 2, null, 4, null, 6]";
    auto expected = "[11, 13, null, null, null, null]";
    this->Assert(one_null, one_null, options);
    this->Assert(three_null, three_null, options);
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, HasStartDoSkipNoNulls) {
  auto empty = "[]";

  if (this->type_singleton()->id() == Type::DATE64) {
    // CumulativeGenericOptions options = this->generate_options(86400000, true);
    // auto values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
    // auto expected = "[86400000, 172800000, 345600000, 604800000, 950400000,
    // 1382400000]";
    // this->Assert(empty, empty, options);
    // this->Assert(values, expected, options);
  } else {
    CumulativeGenericOptions options = this->generate_options(10, true);
    auto values = "[1, 2, 3, 4, 5, 6]";
    auto expected = "[11, 13, 16, 20, 25, 31]";
    this->Assert(empty, empty, options);
    this->Assert(values, expected, options);
  }
}

TYPED_TEST(TestCumulativeSum, HasStartDoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_null = "[null, null, null]";

  if (this->type_singleton()->id() == Type::DATE64) {
    // CumulativeGenericOptions options = this->generate_options(86400000, true);
    // auto values = "[0, 86400000, null, 259200000, null, 432000000]";
    // auto expected = "[86400000, 172800000, null, 432000000, null, 864000000]";
    // this->Assert(one_null, one_null, options);
    // this->Assert(three_null, three_null, options);
    // this->Assert(values, expected, options);
  } else {
    CumulativeGenericOptions options = this->generate_options(10, true);
    auto values = "[1, 2, null, 4, null, 6]";
    auto expected = "[11, 13, null, 17, null, 23]";
    this->Assert(one_null, one_null, options);
    this->Assert(three_null, three_null, options);
    this->Assert(values, expected, options);
  }
}

}  // namespace compute
}  // namespace arrow
