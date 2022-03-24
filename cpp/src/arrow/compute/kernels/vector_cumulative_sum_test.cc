// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// #include <algorithm>
// #include <cstdint>
// #include <cstdio>
// #include <functional>
// #include <locale>
// #include <memory>
// #include <stdexcept>
// #include <string>
// #include <utility>
// #include <vector>

// #include <gtest/gtest.h>

// #include "arrow/array.h"
// #include "arrow/array/builder_decimal.h"
// #include "arrow/buffer.h"
// #include "arrow/chunked_array.h"
// #include "arrow/compute/api_vector.h"
// #include "arrow/status.h"
// #include "arrow/testing/util.h"
// #include "arrow/type.h"
// #include "arrow/type_fwd.h"
// #include "arrow/type_traits.h"
// #include "arrow/util/checked_cast.h"
// #include "arrow/util/decimal.h"

// #include "arrow/compute/api.h"
// #include "arrow/compute/kernels/test_util.h"

// #include "arrow/ipc/json_simple.h"

// namespace arrow {
// namespace compute {

// using IntegralTypes = testing::Types<UInt8Type, UInt16Type, UInt32Type, UInt64Type,
//                                      Int8Type, Int16Type, Int32Type, Int64Type>;

// using FloatingTypes = testing::Types<FloatType, DoubleType>;

// using TimeTypes = testing::Types<Date32Type, Date64Type, Time32Type, Time64Type,
//                                  TimestampType, DurationType, MonthIntervalType>;

// template <typename Type>
// class TestCumulativeSum : public ::testing::Test {
//   using CType = TypeTraits<Type>::CType;

//  protected:
//   CumulativeSumOptions no_start_no_skip_nulls();
//   CumulativeSumOptions has_start_no_skip_nulls(10);
//   CumulativeSumOptions no_start_skip_nulls(0, true);
//   CumulativeSumOptions has_start_skip_nulls(10, true);

//   void SetUp() override {}

//   void AssertValidCumulativeSum(const Array& expected, const Array& input,
//                                 const CumulativeSumOptions options) {
//     ASSERT_OK_AND_ASSIGN(auto result, CumulativeSum(input, options, nullptr));
//     AssertArraysEqual(expected, *result, false, EqualOptions::Defaults());
//   }
// };

// TYPED_TEST_SUITE(TestCumulativeSumIntegral, IntegralTypes);
// TYPED_TEST_SUITE(TestCumulativeSumFloating, FloatingTypes);
// TYPED_TEST_SUITE(TestCumulativeSumTemporal, TimeTypes);

// TYPED_TEST(TestCumulativeSumIntegral, NoStartNoSkipNulls) {}
// TYPED_TEST(TestCumulativeSumIntegral, HasStartNoSkipNulls) {}
// TYPED_TEST(TestCumulativeSumIntegral, NoStartSkipNulls) {}
// TYPED_TEST(TestCumulativeSumIntegral, HasStartSkipNulls) {}


// }  // namespace compute
// }  // namespace arrow
