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

#include <gtest/gtest.h>

#include "arrow/array.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/testing/gtest_common.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace compute {

class TestValidityKernels : public ::testing::Test {
 protected:
  // XXX Since IsValid and IsNull don't touch any buffers but the null bitmap
  // testing multiple types seems redundant.
  using ArrowType = BooleanType;

  using CType = typename ArrowType::c_type;

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }

  void AssertUnary(Datum arg, Datum expected) {
    ASSERT_OK_AND_ASSIGN(auto actual, func_(arg, nullptr));
    ASSERT_EQ(actual.kind(), expected.kind());
    if (actual.kind() == Datum::ARRAY) {
      ASSERT_OK(actual.make_array()->ValidateFull());
      AssertArraysApproxEqual(*expected.make_array(), *actual.make_array());
    } else {
      AssertScalarsEqual(*expected.scalar(), *actual.scalar());
    }
  }

  void AssertUnary(const std::string& arg_json, const std::string& expected_json) {
    AssertUnary(ArrayFromJSON(type_singleton(), arg_json),
                ArrayFromJSON(type_singleton(), expected_json));
  }

  using UnaryFunction = std::function<Result<Datum>(const Datum&, ExecContext*)>;
  UnaryFunction func_;
};

TEST_F(TestValidityKernels, ArrayIsValid) {
  func_ = arrow::compute::IsValid;

  this->AssertUnary("[]", "[]");
  this->AssertUnary("[null]", "[false]");
  this->AssertUnary("[1]", "[true]");
  this->AssertUnary("[null, 1, 0, null]", "[false, true, true, false]");
}

TEST_F(TestValidityKernels, ArrayIsValidBufferPassthruOptimization) {
  Datum arg = ArrayFromJSON(boolean(), "[null, 1, 0, null]");
  ASSERT_OK_AND_ASSIGN(auto validity, arrow::compute::IsValid(arg));
  ASSERT_EQ(validity.array()->buffers[1], arg.array()->buffers[0]);
}

TEST_F(TestValidityKernels, ScalarIsValid) {
  func_ = arrow::compute::IsValid;

  AssertUnary(Datum(19.7), Datum(true));
  AssertUnary(MakeNullScalar(float64()), Datum(false));
}

TEST_F(TestValidityKernels, ArrayIsNull) {
  func_ = arrow::compute::IsNull;

  this->AssertUnary("[]", "[]");
  this->AssertUnary("[null]", "[true]");
  this->AssertUnary("[1]", "[false]");
  this->AssertUnary("[null, 1, 0, null]", "[true, false, false, true]");
}

TEST_F(TestValidityKernels, DISABLED_ScalarIsNull) {
  func_ = arrow::compute::IsNull;

  AssertUnary(Datum(19.7), Datum(false));
  AssertUnary(MakeNullScalar(float64()), Datum(true));
}

}  // namespace compute
}  // namespace arrow
