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
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"  // IntegralArrowTypes
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

#include "arrow/ipc/json_simple.h"

namespace arrow {
namespace compute {

using DurationTypes = testing::Types<DurationType, MonthIntervalType>;

class TestCumulativeOp : public ::testing::Test {
 protected:
  void Assert(const std::string func, const std::shared_ptr<Array>& expected,
              const std::shared_ptr<Array>& input,
              const CumulativeGenericOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto result,
                         CallFunction(func, {Datum(input)}, &options, nullptr));

    AssertArraysEqual(*expected, *result.make_array(), false, EqualOptions::Defaults());
  }
};

template <typename T>
class TestCumulativeSumIntegral : public TestCumulativeOp {
 public:
 using ArrowType = T;

 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

template <typename T>
class TestCumulativeSumFloating : public TestCumulativeOp {
 public:
 using ArrowType = T;

 protected:
  std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

class TestCumulativeSumTemporal : public TestCumulativeOp {
 protected:
  void Assert(std::shared_ptr<DataType>& type, std::string& values, std::string& expected,
              CumulativeGenericOptions& options) {
    auto values_array = ArrayFromJSON(type, values);
    auto expected_array = ArrayFromJSON(type, expected);
    TestCumulativeOp::Assert("cumulative_sum", values_array, expected_array, options);
  }
};

TYPED_TEST_SUITE(TestCumulativeSumIntegral, IntegralArrowTypes);
TYPED_TEST_SUITE(TestCumulativeSumFloating, RealArrowTypes);
// TYPED_TEST_SUITE(TestCumulativeSumTemporal, TemporalArrowTypes);
// TYPED_TEST_SUITE(TestCumulativeSumDecimal, DecimalArrowTypes);

TYPED_TEST(TestCumulativeSumIntegral, NoStartNoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0)};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, 6, 10, 15, 21]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, NoStartNoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0)};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, null, null, null, null]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, NoStartDoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0), true};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, 6, 10, 15, 21]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, NoStartDoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0), true};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, null, 7, null, 13]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, HasStartNoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10)};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, 16, 20, 25, 31]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, HasStartNoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10)};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected =
      ArrayFromJSON(this->type_singleton(), "[11, 13, null, null, null, null]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, HasStartDoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10), true};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, 16, 20, 25, 31]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumIntegral, HasStartDoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10), true};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, null, 17, null, 23]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, NoStartNoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0)};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, 6, 10, 15, 21]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, NoStartNoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0)};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, null, null, null, null]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, NoStartDoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0), true};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, 6, 10, 15, 21]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, NoStartDoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(0), true};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 3, null, 7, null, 13]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, HasStartNoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10)};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, 16, 20, 25, 31]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, HasStartNoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10)};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected =
      ArrayFromJSON(this->type_singleton(), "[11, 13, null, null, null, null]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, HasStartDoSkipNoNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10), true};

  auto empty = ArrayFromJSON(this->type_singleton(), "[]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, 3, 4, 5, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, 16, 20, 25, 31]");

  this->Assert("cumulative_sum", empty, empty, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TYPED_TEST(TestCumulativeSumFloating, HasStartDoSkipHasNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeGenericOptions options{std::make_shared<NumericScalar<ArrowType>>(10), true};

  auto one_null = ArrayFromJSON(this->type_singleton(), "[null]");
  auto multiple_nulls = ArrayFromJSON(this->type_singleton(), "[null, null, null]");
  auto values = ArrayFromJSON(this->type_singleton(), "[1, 2, null, 4, null, 6]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[11, 13, null, 17, null, 23]");

  this->Assert("cumulative_sum", one_null, one_null, options);
  this->Assert("cumulative_sum", multiple_nulls, multiple_nulls, options);
  this->Assert("cumulative_sum", expected, values, options);
}

TEST_F(TestCumulativeSumTemporal, NoStartNoSkipNoNulls) {
  auto empty = "[]";
  auto time_values = "[1, 2, 3, 4, 5, 6]";
  auto time_expected = "[1, 3, 6, 10, 15, 21]";
  auto date_values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
  auto date_expected = "[0, 86400000, 259200000, 518400000, 864000000, 1296000000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(0)};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)0)};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)0)};

  this->Assert(date32(), empty, empty, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), empty, empty, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, NoStartNoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_nulls = "[null, null, null]";
  auto time_values = "[1, 2, null, 4, null, 6]";
  auto time_expected = "[1, 3, null, null, null, null]";
  auto date_values = "[0, 86400000, null, 256200000, null, 432000000]";
  auto date_expected = "[0, 86400000, null, null, null, null]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(0)};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)0)};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)0)};

  this->Assert(date32(), one_null, one_null, date_options);
  this->Assert(date32(), three_nulls, three_nulls, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), one_null, one_null, date_options);
  this->Assert(date64(), three_nulls, three_nulls, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time32((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time64((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(timestamp((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, NoStartDoSkipNoNulls) {
  auto empty = "[]";
  auto time_values = "[1, 2, 3, 4, 5, 6]";
  auto time_expected = "[1, 3, 6, 10, 15, 21]";
  auto date_values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
  auto date_expected = "[0, 86400000, 259200000, 518400000, 864000000, 1296000000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(0), true};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)0), true};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)0), true};

  this->Assert(date32(), empty, empty, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), empty, empty, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, NoStartDoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_nulls = "[null, null, null]";
  auto time_values = "[1, 2, null, 4, null, 6]";
  auto time_expected = "[1, 3, null, 7, null, 13]";
  auto date_values = "[0, 86400000, null, 256200000, null, 432000000]";
  auto date_expected = "[0, 86400000, null, 345600000, null, 777600000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(0), true};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)0), true};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)0), true};

  this->Assert(date32(), one_null, one_null, date_options);
  this->Assert(date32(), three_nulls, three_nulls, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), one_null, one_null, date_options);
  this->Assert(date64(), three_nulls, three_nulls, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time32((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time64((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(timestamp((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, HasStartNoSkipNoNulls) {
  auto empty = "[]";
  auto time_values = "[1, 2, 3, 4, 5, 6]";
  auto time_expected = "[11, 13, 16, 20, 25, 31]";
  auto date_values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
  auto date_expected = "[86400000, 172800000, 345600000, 604800000, 950400000, 1382400000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(86400000)};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)10)};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)10)};

  this->Assert(date32(), empty, empty, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), empty, empty, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, HasStartNoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_nulls = "[null, null, null]";
  auto time_values = "[1, 2, null, 4, null, 6]";
  auto time_expected = "[11, 13, null, null, null, null]";
  auto date_values = "[0, 86400000, null, 256200000, null, 432000000]";
  auto date_expected = "[86400000, 172800000, null, null, null, null]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(86400000)};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)10)};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)10)};

  this->Assert(date32(), one_null, one_null, date_options);
  this->Assert(date32(), three_nulls, three_nulls, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), one_null, one_null, date_options);
  this->Assert(date64(), three_nulls, three_nulls, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time32((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time64((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(timestamp((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, HasStartDoSkipNoNulls) {
  auto empty = "[]";
  auto time_values = "[1, 2, 3, 4, 5, 6]";
  auto time_expected = "[11, 13, 16, 20, 25, 31]";
  auto date_values = "[0, 86400000, 172800000, 259200000, 345600000, 432000000]";
  auto date_expected = "[86400000, 172800000, 345600000, 604800000, 950400000, 1382400000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(86400000), true};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)10), true};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)10), true};

  this->Assert(date32(), empty, empty, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), empty, empty, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), empty, empty, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

TEST_F(TestCumulativeSumTemporal, HasStartDoSkipHasNulls) {
  auto one_null = "[null]";
  auto three_nulls = "[null, null, null]";
  auto time_values = "[1, 2, null, 4, null, 6]";
  auto time_expected = "[11, 13, null, 17, null, 23]";
  auto date_values = "[0, 86400000, null, 256200000, null, 432000000]";
  auto date_expected = "[86400000, 172800000, null, 429000000, null, 861000000]";

  CumulativeGenericOptions date_options{std::make_shared<DateScalar>(86400000), true};
  CumulativeGenericOptions time_options{std::make_shared<TimeScalar>((TimeUnit::type)10), true};
  CumulativeGenericOptions timestamp_options{std::make_shared<TimestampScalar>((TimeUnit::type)10), true};

  this->Assert(date32(), one_null, one_null, date_options);
  this->Assert(date32(), three_nulls, three_nulls, date_options);
  this->Assert(date32(), date_values, date_expected, date_options);
  this->Assert(date64(), one_null, one_null, date_options);
  this->Assert(date64(), three_nulls, three_nulls, date_options);
  this->Assert(date64(), date_values, date_expected, date_options);

  this->Assert(time32((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time32((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time32((TimeUnit::type)0), time_values, time_expected, time_options);
  this->Assert(time64((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(time64((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(time64((TimeUnit::type)0), time_values, time_expected, time_options);

  this->Assert(timestamp((TimeUnit::type)0), one_null, one_null, time_options);
  this->Assert(timestamp((TimeUnit::type)0), three_nulls, three_nulls, time_options);
  this->Assert(timestamp((TimeUnit::type)0), time_values, time_expected, time_options);
}

}  // namespace compute
}  // namespace arrow
