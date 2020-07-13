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

  static std::shared_ptr<DataType> type_singleton() {
    return TypeTraits<ArrowType>::type_singleton();
  }
};

TEST_F(TestValidityKernels, ArrayIsValid) {
  CheckScalarUnary("is_valid", type_singleton(), "[]", type_singleton(), "[]");
  CheckScalarUnary("is_valid", type_singleton(), "[null]", type_singleton(), "[false]");
  CheckScalarUnary("is_valid", type_singleton(), "[1]", type_singleton(), "[true]");
  CheckScalarUnary("is_valid", type_singleton(), "[null, 1, 0, null]", type_singleton(),
                   "[false, true, true, false]");
}

TEST_F(TestValidityKernels, IsValidIsNullNullType) {
  CheckScalarUnary("is_null", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[true, true, true, true, true]"));
  CheckScalarUnary("is_valid", std::make_shared<NullArray>(5),
                   ArrayFromJSON(boolean(), "[false, false, false, false, false]"));
}

TEST_F(TestValidityKernels, ArrayIsValidBufferPassthruOptimization) {
  Datum arg = ArrayFromJSON(boolean(), "[null, 1, 0, null]");
  ASSERT_OK_AND_ASSIGN(auto validity, arrow::compute::IsValid(arg));
  ASSERT_EQ(validity.array()->buffers[1], arg.array()->buffers[0]);
}

TEST_F(TestValidityKernels, ScalarIsValid) {
  CheckScalarUnary("is_valid", MakeScalar(19.7), MakeScalar(true));
  CheckScalarUnary("is_valid", MakeNullScalar(float64()), MakeScalar(false));
}

TEST_F(TestValidityKernels, ArrayIsNull) {
  CheckScalarUnary("is_null", type_singleton(), "[]", type_singleton(), "[]");
  CheckScalarUnary("is_null", type_singleton(), "[null]", type_singleton(), "[true]");
  CheckScalarUnary("is_null", type_singleton(), "[1]", type_singleton(), "[false]");
  CheckScalarUnary("is_null", type_singleton(), "[null, 1, 0, null]", type_singleton(),
                   "[true, false, false, true]");
}

TEST_F(TestValidityKernels, IsNullSetsZeroNullCount) {
  auto arr = ArrayFromJSON(int32(), "[1, 2, 3, 4]");
  std::shared_ptr<ArrayData> result = (*IsNull(arr)).array();
  ASSERT_EQ(result->null_count, 0);
}

TEST_F(TestValidityKernels, ScalarIsNull) {
  CheckScalarUnary("is_null", MakeScalar(19.7), MakeScalar(false));
  CheckScalarUnary("is_null", MakeNullScalar(float64()), MakeScalar(true));
}

}  // namespace compute
}  // namespace arrow
