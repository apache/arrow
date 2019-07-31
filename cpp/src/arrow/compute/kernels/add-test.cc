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
#include <string>
#include <type_traits>
#include <utility>

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/add.h"
#include "arrow/compute/test-util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

template <typename ResultType, typename LhsType, typename RhsType>
class TestAddKernel : public ComputeFixture, public TestBase {
 private:
  void AssertAddArrays(const std::shared_ptr<Array> lhs, const std::shared_ptr<Array> rhs,
                       const std::shared_ptr<Array> expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Add(&this->ctx_, *lhs, *rhs, &actual));
    ASSERT_OK(ValidateArray(*actual));
    AssertArraysEqual(*expected, *actual);
  }

 protected:
  virtual void AssertAdd(const std::string& lhs, const std::string& rhs,
                         const std::string& expected) {
    auto lhs_type = TypeTraits<LhsType>::type_singleton();
    auto rhs_type = TypeTraits<RhsType>::type_singleton();
    auto result_type = TypeTraits<ResultType>::type_singleton();
    AssertAddArrays(ArrayFromJSON(lhs_type, lhs), ArrayFromJSON(rhs_type, rhs),
                    ArrayFromJSON(result_type, expected));
  }
};

#define ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(T1, T2, T3)                                \
  class TestAddKernel##T1##T2##T3 : public TestAddKernel<T1##Type, T2##Type, T3##Type> { \
  };                                                                                     \
  TEST_F(TestAddKernel##T1##T2##T3, TestAdd) {                                           \
    this->AssertAdd("[2, 5, 1]", "[5, 3, 12]", "[7, 8, 13]");                            \
  }

ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int32, Int32, Int32)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(UInt32, UInt32, UInt32)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int32, UInt32, Int32)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int32, Int32, UInt32)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int64, Int64, UInt32)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(UInt64, UInt64, UInt64)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(UInt64, UInt8, UInt64)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int8, UInt8, Int8)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int16, UInt8, Int16)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Int16, UInt16, Int8)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(UInt16, UInt8, UInt16)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Double, Double, UInt16)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Double, Float, Double)
ARITHMETIC_KERNEL_TYPE_INFERENCE_TEST(Float, Float, UInt16)

}  // namespace compute
}  // namespace arrow
