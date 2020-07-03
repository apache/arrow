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
#include <iosfwd>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/compute/api.h"
#include "arrow/compute/kernels/test_util.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

template <typename Type, typename T = typename TypeTraits<Type>::c_type>
void CheckFillNull(const std::shared_ptr<DataType>& type, const std::vector<T>& in_values,
                   const std::vector<bool>& in_is_valid, const Datum fill_value,
                   const std::vector<T>& out_values,
                   const std::vector<bool>& out_is_valid) {
  std::shared_ptr<Array> input = _MakeArray<Type, T>(type, in_values, in_is_valid);
  std::shared_ptr<Array> expected = _MakeArray<Type, T>(type, out_values, out_is_valid);

  ASSERT_OK_AND_ASSIGN(Datum datum_out, FillNull(input, fill_value));
  std::shared_ptr<Array> result = datum_out.make_array();
  ASSERT_OK(result->ValidateFull());
  AssertArraysEqual(*expected, *result, /*verbose=*/true);
}

class TestFillNullKernel : public ::testing::Test {};

template <typename Type>
class TestFillNullPrimitive : public ::testing::Test {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, Date32Type, Date64Type>
    PrimitiveTypes;

TYPED_TEST_SUITE(TestFillNullPrimitive, PrimitiveTypes);

TYPED_TEST(TestFillNullPrimitive, FillNull) {
  using T = typename TypeParam::c_type;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();
  auto scalar = std::make_shared<ScalarType>(static_cast<T>(5));
  // No Nulls
  CheckFillNull<TypeParam, T>(type, {2, 4, 7, 9}, {true, true, true, true}, Datum(scalar),
                              {2, 4, 7, 9}, {true, true, true, true});
  // Some Nulls
  CheckFillNull<TypeParam, T>(type, {2, 4, 7, 8}, {false, true, false, true},
                              Datum(scalar), {5, 4, 5, 8}, {true, true, true, true});
  // Empty Array
  CheckFillNull<TypeParam, T>(type, {}, {}, Datum(scalar), {}, {});
}

TEST_F(TestFillNullKernel, FillNullNull) {
  auto datum = Datum(std::make_shared<NullScalar>());
  CheckFillNull<NullType, std::nullptr_t>(null(), {0, 0, 0, 0},
                                          {false, false, false, false}, datum,
                                          {0, 0, 0, 0}, {false, false, false, false});
  CheckFillNull<NullType, std::nullptr_t>(null(), {NULL, NULL, NULL, NULL}, {}, datum,
                                          {NULL, NULL, NULL, NULL}, {});
  CheckFillNull<NullType, std::nullptr_t>(null(), {0, 0, 0, 0},
                                          {false, false, false, false}, datum,
                                          {0, 0, 0, 0}, {false, false, false, false});
  CheckFillNull<NullType, std::nullptr_t>(null(), {NULL, NULL, NULL, NULL}, {}, datum,
                                          {NULL, NULL, NULL, NULL}, {});
}

TEST_F(TestFillNullKernel, FillNullBoolean) {
  auto scalar1 = std::make_shared<BooleanScalar>(false);
  auto scalar2 = std::make_shared<BooleanScalar>(true);
  // no nulls
  CheckFillNull<BooleanType, bool>(boolean(), {true, false, true, false},
                                   {true, true, true, true}, Datum(scalar1),
                                   {true, false, true, false}, {true, true, true, true});
  // some nulls
  CheckFillNull<BooleanType, bool>(boolean(), {true, false, true, false},
                                   {false, true, true, false}, Datum(scalar1),
                                   {false, false, true, false}, {true, true, true, true});
  CheckFillNull<BooleanType, bool>(boolean(), {true, false, true, false},
                                   {false, true, false, false}, Datum(scalar2),
                                   {true, false, true, true}, {true, true, true, true});
}

TEST_F(TestFillNullKernel, FillNullTimeStamp) {
  auto time32_type = time32(TimeUnit::SECOND);
  auto time64_type = time64(TimeUnit::NANO);
  auto scalar1 = Datum(std::make_shared<Time32Scalar>(5, time32_type));
  auto scalar2 = Datum(std::make_shared<Time64Scalar>(6, time64_type));
  // no nulls
  CheckFillNull<Time32Type, int32_t>(time32_type, {2, 1, 6, 9}, {true, true, true, true},
                                     Datum(scalar1), {2, 1, 6, 9},
                                     {true, true, true, true});
  CheckFillNull<Time32Type, int32_t>(time32_type, {2, 1, 6, 9},
                                     {true, false, true, false}, Datum(scalar1),
                                     {2, 5, 6, 5}, {true, true, true, true});
  // some nulls
  CheckFillNull<Time64Type, int64_t>(time64_type, {2, 1, 6, 9}, {true, true, true, true},
                                     scalar2, {2, 1, 6, 9}, {true, true, true, true});
  CheckFillNull<Time64Type, int64_t>(time64_type, {2, 1, 6, 9},
                                     {true, false, true, false}, scalar2, {2, 6, 6, 6},
                                     {true, true, true, true});
}

}  // namespace compute
}  // namespace arrow
