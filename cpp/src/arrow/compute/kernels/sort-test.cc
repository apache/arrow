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

#include <memory>
#include <vector>

#include "arrow/compute/context.h"
#include "arrow/compute/kernels/sort.h"
#include "arrow/compute/test-util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/testing/util.h"
#include "arrow/type_traits.h"
namespace arrow {
namespace compute {

template <typename ArrowType>
class TestSortKernel : public ComputeFixture, public TestBase {
 protected:
  void AssertSortArrays(const std::shared_ptr<Array> values,
                        const std::shared_ptr<Array> expected) {
    std::shared_ptr<Array> actual;
    ASSERT_OK(arrow::compute::Sort(&this->ctx_, *values, &actual));
    ASSERT_OK(ValidateArray(*actual));
    AssertArraysEqual(*expected, *actual);
  }

  virtual void AssertSort(const std::string& values, const std::string& expected) {
    auto type = TypeTraits<ArrowType>::type_singleton();
    AssertSortArrays(ArrayFromJSON(type, values), ArrayFromJSON(uint64(), expected));
  }
};

class TestSortKernelWithInt32 : public TestSortKernel<Int32Type> {
 protected:
  void AssertSort(const std::string& values, const std::string& expected) override {
    TestSortKernel<Int32Type>::AssertSort(values, expected);
  }
};

TEST_F(TestSortKernelWithInt32, SortInt32) {
  this->AssertSort("[]", "[]");

  this->AssertSort("[3,2,6]", "[1, 0, 2]");

  this->AssertSort("[1,2,3,4,5,6,7]", "[0,1,2,3,4,5,6]");

  this->AssertSort("[7,6,5,4,3,2,1]", "[6,5,4,3,2,1,0]");

  this->AssertSort("[10,12,4,50,100,32,11]", "[2,0,6,1,5,3,4]");
}

}  // namespace compute
}  // namespace arrow
