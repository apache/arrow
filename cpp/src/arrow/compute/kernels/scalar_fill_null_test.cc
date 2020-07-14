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
#include <string>

#include <gtest/gtest.h>

#include "arrow/array/array_base.h"
#include "arrow/compute/api.h"
#include "arrow/result.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_compat.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace compute {

void CheckFillNull(const Array& input, const Datum& fill_value, const Array& expected) {
  auto Check = [&](const Array& input, const Array& expected) {
    ASSERT_OK_AND_ASSIGN(Datum datum_out, FillNull(input, fill_value));
    std::shared_ptr<Array> result = datum_out.make_array();
    ASSERT_OK(result->ValidateFull());
    AssertArraysEqual(expected, *result, /*verbose=*/true);
  };

  Check(input, expected);
  if (input.length() > 0) {
    Check(*input.Slice(1), *expected.Slice(1));
  }
}

void CheckFillNull(const std::shared_ptr<DataType>& type, const std::string& in_values,
                   const Datum& fill_value, const std::string& out_values) {
  std::shared_ptr<Array> input = ArrayFromJSON(type, in_values);
  std::shared_ptr<Array> expected = ArrayFromJSON(type, out_values);
  CheckFillNull(*input, fill_value, *expected);
}

class TestFillNullKernel : public ::testing::Test {};

template <typename Type>
class TestFillNullPrimitive : public ::testing::Test {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveTypes;

TEST_F(TestFillNullKernel, FillNullInvalidScalar) {
  auto scalar = std::make_shared<Int8Scalar>(3);
  scalar->is_valid = false;
  CheckFillNull(int8(), "[1, null, 3, 2]", Datum(scalar), "[1, null, 3, 2]");
}

TYPED_TEST_SUITE(TestFillNullPrimitive, PrimitiveTypes);

TYPED_TEST(TestFillNullPrimitive, FillNull) {
  using T = typename TypeParam::c_type;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();
  auto scalar = std::make_shared<ScalarType>(static_cast<T>(5));
  // No Nulls
  CheckFillNull(type, "[2, 4, 7, 9]", Datum(scalar), "[2, 4, 7, 9]");
  // Some Null
  CheckFillNull(type, "[null, 4, null, 8]", Datum(scalar), "[5, 4, 5, 8]");
  // Empty Array
  CheckFillNull(type, "[]", Datum(scalar), "[]");

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto arr = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, 1000, /*null_probability=*/0.01));

  std::shared_ptr<ArrayData> expected_data = arr->data()->Copy();
  expected_data->null_count = 0;
  expected_data->buffers[0] = nullptr;
  expected_data->buffers[1] = *AllocateBuffer(arr->length() * sizeof(T));
  T* out_data = expected_data->GetMutableValues<T>(1);
  for (int64_t i = 0; i < arr->length(); ++i) {
    if (arr->IsValid(i)) {
      out_data[i] = arr->Value(i);
    } else {
      out_data[i] = scalar->value;
    }
  }
  CheckFillNull(*arr, Datum(scalar), ArrayType(expected_data));
}

TEST_F(TestFillNullKernel, FillNullNull) {
  auto datum = Datum(std::make_shared<NullScalar>());
  CheckFillNull(null(), "[null, null, null, null]", datum, "[null, null, null, null]");
}

TEST_F(TestFillNullKernel, FillNullBoolean) {
  auto scalar1 = std::make_shared<BooleanScalar>(false);
  auto scalar2 = std::make_shared<BooleanScalar>(true);
  // no nulls
  CheckFillNull(boolean(), "[true, false, true, false]", Datum(scalar1),
                "[true, false, true, false]");
  // some nulls
  CheckFillNull(boolean(), "[true, false, false, null]", Datum(scalar1),
                "[true, false, false, false]");
  CheckFillNull(boolean(), "[true, null, false, null]", Datum(scalar2),
                "[true, true, false, true]");

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto arr = std::static_pointer_cast<BooleanArray>(
      rand.Boolean(1000, /*true_probability=*/0.5, /*null_probability=*/0.01));

  auto expected_data = arr->data()->Copy();
  expected_data->null_count = 0;
  expected_data->buffers[0] = nullptr;
  expected_data->buffers[1] = *AllocateEmptyBitmap(arr->length());
  uint8_t* out_data = expected_data->buffers[1]->mutable_data();
  for (int64_t i = 0; i < arr->length(); ++i) {
    if (arr->IsValid(i)) {
      BitUtil::SetBitTo(out_data, i, arr->Value(i));
    } else {
      BitUtil::SetBitTo(out_data, i, true);
    }
  }
  CheckFillNull(*arr, Datum(std::make_shared<BooleanScalar>(true)),
                BooleanArray(expected_data));
}

TEST_F(TestFillNullKernel, FillNullTimeStamp) {
  auto time32_type = time32(TimeUnit::SECOND);
  auto time64_type = time64(TimeUnit::NANO);
  auto scalar1 = std::make_shared<Time32Scalar>(5, time32_type);
  auto scalar2 = std::make_shared<Time64Scalar>(6, time64_type);
  // no nulls
  CheckFillNull(time32_type, "[2, 1, 6, 9]", Datum(scalar1), "[2, 1, 6, 9]");
  CheckFillNull(time64_type, "[2, 1, 6, 9]", Datum(scalar2), "[2, 1, 6, 9]");
  // some nulls
  CheckFillNull(time32_type, "[2, 1, 6, null]", Datum(scalar1), "[2, 1, 6, 5]");
  CheckFillNull(time64_type, "[2, 1, 6, null]", Datum(scalar2), "[2, 1, 6, 6]");
}

}  // namespace compute
}  // namespace arrow
