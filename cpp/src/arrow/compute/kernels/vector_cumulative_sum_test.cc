// Licensed to the Apache Software Foundation (ASF) under one // or more contributor license agreements.  See the NOTICE file // distributed with this work for additional information // regarding copyright ownership.  The ASF licenses this file
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
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"

#include "arrow/ipc/json_simple.h"

namespace arrow {
namespace compute {

using DurationTypes = testing::Types<DurationType, MonthIntervalType>;

template <typename T>
class TestCumulativeSum : public ::testing::Test {
 public:
  using ArrowType = T;
  // using CType = TypeTraits<Type>::CType;

 protected:
  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void SetUp() override {}

  void Assert(const std::shared_ptr<Array>& expected, const std::shared_ptr<Array>& input,
                                const CumulativeSumOptions& options) {
    ASSERT_OK_AND_ASSIGN(auto result, CallFunction("cumulative_sum", {Datum(input)}, &options, nullptr));
    AssertArraysEqual(*expected, *result.make_array(), false, EqualOptions::Defaults());
  }
};

template <typename T>
class TestCumulativeSumIntegral : public TestCumulativeSum<T> {};

TYPED_TEST_SUITE(TestCumulativeSumIntegral, IntegralArrowTypes);
// TYPED_TEST_SUITE(TestCumulativeSumFloating, RealArrowTypes);
// TYPED_TEST_SUITE(TestCumulativeSumTemporal, TemporalArrowTypes);
// TYPED_TEST_SUITE(TestCumulativeSumDecimal, DecimalArrowTypes);

TYPED_TEST(TestCumulativeSumIntegral, NoStartNoSkipNulls) {
  using ArrowType = typename TestFixture::ArrowType;

  CumulativeSumOptions options{std::make_shared<NumericScalar<ArrowType>>(0)};
  // CumulativeSumOptions options;

  auto values = ArrayFromJSON(this->type_singleton(), "[1, 3]");
  auto expected = ArrayFromJSON(this->type_singleton(), "[1, 4]");
  this->Assert(expected, values, options);
}

// TYPED_TEST(TestCumulativeSumIntegral, HasStartNoSkipNulls) {}
// TYPED_TEST(TestCumulativeSumIntegral, NoStartSkipNulls) {}
// TYPED_TEST(TestCumulativeSumIntegral, HasStartSkipNulls) {}


}  // namespace compute
}  // namespace arrow
