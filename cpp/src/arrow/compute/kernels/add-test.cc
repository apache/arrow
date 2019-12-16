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
#include "arrow/compute/test_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"

namespace arrow {
namespace compute {

template <typename ArrowType>
class TestArithmeticKernel : public ComputeFixture, public TestBase {
 private:
  void AssertAddArrays(const std::shared_ptr<Array> lhs, const std::shared_ptr<Array> rhs,
                       const std::shared_ptr<Array> expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Add(&this->ctx_, *lhs, *rhs, &actual));
    ASSERT_OK(actual->ValidateFull());
    AssertArraysEqual(*expected, *actual);
  }

 protected:
  virtual void AssertAdd(const std::string& lhs, const std::string& rhs,
                         const std::string& expected) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertAddArrays(ArrayFromJSON(type, lhs), ArrayFromJSON(type, rhs),
                    ArrayFromJSON(type, expected));
  }
};

template <typename ArrowType>
class TestArithmeticKernelForReal : public TestArithmeticKernel<ArrowType> {};
TYPED_TEST_CASE(TestArithmeticKernelForReal, RealArrowTypes);

template <typename ArrowType>
class TestArithmeticKernelForIntegral : public TestArithmeticKernel<ArrowType> {};
TYPED_TEST_CASE(TestArithmeticKernelForIntegral, IntegralArrowTypes);

TYPED_TEST(TestArithmeticKernelForReal, SortReal) {
  this->AssertAdd("[]", "[]", "[]");

  this->AssertAdd("[3.4, 2.6, 6.3]", "[1, 0, 2]", "[4.4, 2.6, 8.3]");

  this->AssertAdd("[1.1, 2.4, 3.5, 4.3, 5.1, 6.8, 7.3]", "[0, 1, 2, 3, 4, 5, 6]",
                  "[1.1, 3.4, 5.5, 7.3, 9.1, 11.8, 13.3]");

  this->AssertAdd("[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]",
                  "[13, 11, 9, 7, 5, 3, 1]");

  this->AssertAdd("[10.4, 12, 4.2, 50, 50.3, 32, 11]", "[2, 0, 6, 1, 5, 3, 4]",
                  "[12.4, 12, 10.2, 51, 55.3, 35, 15]");

  this->AssertAdd("[null, 1, 3.3, null, 2, 5.3]", "[1, 4, 2, 5, 0, 3]",
                  "[null, 5, 5.3, null, 2, 8.3]");
}

TYPED_TEST(TestArithmeticKernelForIntegral, SortIntegral) {
  this->AssertAdd("[]", "[]", "[]");

  this->AssertAdd("[3, 2, 6]", "[1, 0, 2]", "[4, 2, 8]");

  this->AssertAdd("[1, 2, 3, 4, 5, 6, 7]", "[0, 1, 2, 3, 4, 5, 6]",
                  "[1, 3, 5, 7, 9, 11, 13]");

  this->AssertAdd("[7, 6, 5, 4, 3, 2, 1]", "[6, 5, 4, 3, 2, 1, 0]",
                  "[13, 11, 9, 7, 5, 3, 1]");

  this->AssertAdd("[10, 12, 4, 50, 50, 32, 11]", "[2, 0, 6, 1, 5, 3, 4]",
                  "[12, 12, 10, 51, 55, 35, 15]");

  this->AssertAdd("[null, 1, 3, null, 2, 5]", "[1, 4, 2, 5, 0, 3]",
                  "[null, 5, 5, null, 2, 8]");
}

}  // namespace compute
}  // namespace arrow
