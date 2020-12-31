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

#include "arrow/compute/api.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::InvertBitmap;

namespace compute {

void CheckReplace(const Array& input, const Array& mask, const Datum& replacement,
                  const Array& expected, const bool ensure_no_nulls = false) {
  auto Check = [&](const Array& input, const Array& mask, const Array& expected) {
    ASSERT_OK_AND_ASSIGN(Datum datum_out, Replace(input, mask, replacement));
    std::shared_ptr<Array> result = datum_out.make_array();
    ASSERT_OK(result->ValidateFull());
    AssertArraysEqual(expected, *result, /*verbose=*/true);
    if (ensure_no_nulls) {
      if (result->null_count() != 0 || result->data()->buffers[0] != nullptr)
        FAIL() << "Result shall have null_count == 0 and validity bitmap == nullptr!";
    }
  };

  Check(input, mask, expected);

  if (input.length() > 0) {
    Check(*input.Slice(1), *mask.Slice(1), *expected.Slice(1));
  }
}

void CheckReplace(const std::shared_ptr<DataType>& type, const std::string& in_values,
                  const std::string& in_mask, const Datum& replacement,
                  const std::string& out_values, const bool ensure_no_nulls = false) {
  std::shared_ptr<Array> input = ArrayFromJSON(type, in_values);
  std::shared_ptr<Array> mask = ArrayFromJSON(boolean(), in_mask);
  std::shared_ptr<Array> expected = ArrayFromJSON(type, out_values);
  CheckReplace(*input, *mask, replacement, *expected, ensure_no_nulls);
}

class TestReplaceKernel : public ::testing::Test {};

template <typename Type>
class TestReplacePrimitive : public ::testing::Test {};

typedef ::testing::Types<Int8Type, UInt8Type, Int16Type, UInt16Type, Int32Type,
                         UInt32Type, Int64Type, UInt64Type, FloatType, DoubleType,
                         Date32Type, Date64Type>
    PrimitiveTypes;

TEST_F(TestReplaceKernel, ReplaceInvalidScalar) {
  auto scalar = std::make_shared<Int8Scalar>(3);
  scalar->is_valid = false;
  CheckReplace(int8(), "[2, 4, 7, 9]", "[true, false, false, false]", Datum(scalar),
               "[2, 4, 7, 9]");
}

TYPED_TEST_SUITE(TestReplacePrimitive, PrimitiveTypes);

TYPED_TEST(TestReplacePrimitive, Replace) {
  using T = typename TypeParam::c_type;
  using ArrayType = typename TypeTraits<TypeParam>::ArrayType;
  using ScalarType = typename TypeTraits<TypeParam>::ScalarType;
  auto type = TypeTraits<TypeParam>::type_singleton();
  auto scalar = std::make_shared<ScalarType>(static_cast<T>(42));

  // No replacement
  CheckReplace(type, "[2, 4, 7, 9, null]", "[false, false, false, false, false]",
               Datum(scalar), "[2, 4, 7, 9, null]");
  // Some replacements
  CheckReplace(type, "[2, 4, 7, 9, null]", "[true, false, true, false, true]",
               Datum(scalar), "[42, 4, 42, 9, 42]", true);
  // Empty Array
  CheckReplace(type, "[]", "[]", Datum(scalar), "[]");

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto arr = std::static_pointer_cast<ArrayType>(
      rand.ArrayOf(type, 1000, /*null_probability=*/0.01));
  // use arr inverted null bits as mask, so expect to replace all null values...
  auto mask_data = std::make_shared<ArrayData>(boolean(), arr->length(), 0);
  mask_data->null_count = 0;
  mask_data->buffers.resize(2);
  mask_data->buffers[0] = nullptr;
  mask_data->buffers[1] = *AllocateEmptyBitmap(arr->length());
  InvertBitmap(arr->data()->buffers[0]->data(), arr->offset(), arr->length(),
               mask_data->buffers[1]->mutable_data(), mask_data->offset);
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
  CheckReplace(*arr, BooleanArray(mask_data), Datum(scalar), ArrayType(expected_data),
               true);
}

TEST_F(TestReplaceKernel, ReplaceNull) {
  auto null_scalar = Datum(MakeNullScalar(boolean()));
  auto true_scalar = Datum(MakeScalar(true));
  // Replace with invalid null value
  CheckReplace(boolean(), "[null, null, null, null]", "[true, true, true, true]",
               /*replacement=*/null_scalar, "[null, null, null, null]");
  // No replacement
  CheckReplace(boolean(), "[null, null, null, null]", "[false, false, false, false]",
               /*replacement=*/true_scalar, "[null, null, null, null]");
  // Some replacements
  CheckReplace(boolean(), "[null, null, null, null]", "[true, false, true, false]",
               /*replacement=*/true_scalar, "[true, null, true, null]");
  // Some replacements with some nulls in mask
  CheckReplace(boolean(), "[null, null, null, null]", "[true, null, true, false]",
               /*replacement=*/true_scalar, "[true, null, true, null]");
  // Replace all
  CheckReplace(boolean(), "[null, null, null, null]", "[true, true, true, true]",
               /*replacement=*/true_scalar, "[true, true, true, true]", true);
}

TEST_F(TestReplaceKernel, ReplaceBoolean) {
  auto scalar1 = std::make_shared<BooleanScalar>(false);
  auto scalar2 = std::make_shared<BooleanScalar>(true);

  // No replacement
  CheckReplace(boolean(), "[true, false, true, false]", "[false, false, false, false]",
               Datum(scalar1), "[true, false, true, false]");
  // Some replacements
  CheckReplace(boolean(), "[true, false, true, false]", "[true, false, true, false]",
               Datum(scalar1), "[false, false, false, false]");
  // Some replacements with nulls in input
  CheckReplace(boolean(), "[true, null, true, null]", "[true, false, true, false]",
               Datum(scalar1), "[false, null, false, null]");
  // Some replacements with nulls in mask
  CheckReplace(boolean(), "[true, false, true, null]", "[true, null, null, false]",
               Datum(scalar1), "[false, false, true, null]");
  // Replace all
  CheckReplace(boolean(), "[true, false, true, null]", "[true, true, true, true]",
               Datum(scalar1), "[false, false, false, false]", true);

  random::RandomArrayGenerator rand(/*seed=*/0);
  auto arr =
      std::static_pointer_cast<BooleanArray>(rand.Boolean(1000,
                                                          /*true_probability=*/0.5,
                                                          /*null_probability=*/0.01));
  // use arr inverted null bits as mask, so expect to replace all null values...
  auto mask_data = std::make_shared<ArrayData>(boolean(), arr->length(), 0);
  mask_data->null_count = 0;
  mask_data->buffers.resize(2);
  mask_data->buffers[0] = nullptr;
  mask_data->buffers[1] = *AllocateEmptyBitmap(arr->length());
  InvertBitmap(arr->data()->buffers[0]->data(), arr->offset(), arr->length(),
               mask_data->buffers[1]->mutable_data(), mask_data->offset);
  auto expected_data = arr->data()->Copy();
  expected_data->null_count = 0;
  expected_data->buffers[0] = nullptr;
  expected_data->buffers[1] = *AllocateEmptyBitmap(arr->length());
  uint8_t* out_data = expected_data->buffers[1]->mutable_data();
  for (int64_t i = 0; i < arr->length(); ++i) {
    if (arr->IsValid(i)) {
      BitUtil::SetBitTo(out_data, i, arr->Value(i));
    } else {
      BitUtil::SetBitTo(out_data, i, scalar1->value);
    }
  }
  CheckReplace(*arr, BooleanArray(mask_data), Datum(scalar1), BooleanArray(expected_data),
               true);
}

TEST_F(TestReplaceKernel, ReplaceTimestamp) {
  auto time32_type = time32(TimeUnit::SECOND);
  auto time64_type = time64(TimeUnit::NANO);
  auto scalar1 = std::make_shared<Time32Scalar>(5, time32_type);
  auto scalar2 = std::make_shared<Time64Scalar>(6, time64_type);
  // No replacement
  CheckReplace(time32_type, "[2, 1, 6, null]", "[false, false, false, false]",
               Datum(scalar1), "[2, 1, 6, null]");
  CheckReplace(time64_type, "[2, 1, 6, null]", "[false, false, false, false]",
               Datum(scalar2), "[2, 1, 6, null]");
  // Some replacements
  CheckReplace(time32_type, "[2, 1, null, 9]", "[true, false, true, false]",
               Datum(scalar1), "[5, 1, 5, 9]", true);
  CheckReplace(time64_type, "[2, 1, 6, null]", "[false, true, false, true]",
               Datum(scalar2), "[2, 6, 6, 6]", true);
}

TEST_F(TestReplaceKernel, ReplaceString) {
  auto type = large_utf8();
  auto scalar = std::make_shared<LargeStringScalar>("arrow");
  // No replacement
  CheckReplace(type, R"(["foo", "bar", null])", "[false, false, false]", Datum(scalar),
               R"(["foo", "bar", null])");
  // Some replacements
  CheckReplace(type, R"(["foo", "bar", null, null])", "[true, false, true, false]",
               Datum(scalar), R"(["arrow", "bar", "arrow", null])");
}

}  // namespace compute
}  // namespace arrow
