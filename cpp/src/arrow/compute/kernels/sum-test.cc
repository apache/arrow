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
#include <string>

#include "arrow/array.h"
#include "arrow/test-common.h"
#include "arrow/test-util.h"
#include "arrow/type.h"

#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/sum.h"
#include "arrow/compute/test-util.h"

using std::shared_ptr;
using std::vector;

namespace arrow {
namespace compute {

template <typename Type, typename CType>
void CheckSum(FunctionContext* ctx, const Array& input, CType expected) {
  Datum result;
  ASSERT_OK(Sum(ctx, input, &result));

  // Ensure Datum is Scalar of proper type.
  ASSERT_EQ(result.kind(), Datum::SCALAR);
  auto type = TypeTraits<Type>::type_singleton();
  ASSERT_EQ(result.scalar().kind(), type->id());

  ASSERT_EQ(util::get<CType>(result.scalar().value), expected);
}

template <typename Type, typename CType>
void CheckSum(FunctionContext* ctx, const std::string& json, CType expected) {
  Datum result;
  auto array = ArrayFromJSON(TypeTraits<Type>::type_singleton(), json);

  CheckSum<Type, CType>(ctx, *array, expected);
}

template <typename Type>
class TestSumKernelNumeric : public ComputeFixture, public TestBase {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType>
    NumericTypes;

TYPED_TEST_CASE(TestSumKernelNumeric, NumericTypes);

TYPED_TEST(TestSumKernelNumeric, SimpleSum) {
  using CType = typename TypeParam::c_type;

  CheckSum<TypeParam, CType>(&this->ctx_, "[0, 1, 2, 3, 4, 5]", 5 * 6 / 2);

  // Avoid this tests for (U)Int8Type
  if (sizeof(CType) > 1)
    CheckSum<TypeParam, CType>(&this->ctx_, "[1000, null, 300, null, 30, null, 7]", 1337);
}

}  // namespace compute
}  // namespace arrow
