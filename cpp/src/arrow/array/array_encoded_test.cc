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

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/array/builder_nested.h"
#include "arrow/chunked_array.h"
#include "arrow/status.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Run-length encoded array tests

namespace {

class TestRunLengthEncodedArray
    : public ::testing::TestWithParam<std::shared_ptr<DataType>> {
 protected:
  std::shared_ptr<DataType> run_ends_type;
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;
  std::shared_ptr<Array> size_values;
  std::shared_ptr<Array> size_only_null;

  virtual void SetUp() override {
    run_ends_type = GetParam();
    string_values = ArrayFromJSON(utf8(), R"(["Hello", "World", null])");
    int32_values = ArrayFromJSON(int32(), "[10, 20, 30]");
    int16_values = ArrayFromJSON(int16(), "[10, 20, 30]");
    size_values = ArrayFromJSON(run_ends_type, "[10, 20, 30]");
    size_only_null = ArrayFromJSON(run_ends_type, "[null, null, null]");
  }
};

TEST_P(TestRunLengthEncodedArray, MakeArray) {
  ASSERT_OK_AND_ASSIGN(auto rle_array,
                       RunLengthEncodedArray::Make(int32_values, string_values, 3));
  auto array_data = rle_array->data();
  auto new_array = MakeArray(array_data);
  ASSERT_ARRAYS_EQUAL(*new_array, *rle_array);
  // should be the exact same ArrayData object
  ASSERT_EQ(new_array->data(), array_data);
  ASSERT_NE(std::dynamic_pointer_cast<RunLengthEncodedArray>(new_array), NULLPTR);
}

TEST_P(TestRunLengthEncodedArray, FromRunEndsAndValues) {
  std::shared_ptr<RunLengthEncodedArray> rle_array;

  ASSERT_OK_AND_ASSIGN(rle_array,
                       RunLengthEncodedArray::Make(size_values, int32_values, 3));
  ASSERT_EQ(rle_array->length(), 3);
  ASSERT_ARRAYS_EQUAL(*rle_array->values_array(), *int32_values);
  ASSERT_ARRAYS_EQUAL(*rle_array->run_ends_array(), *size_values);
  ASSERT_EQ(rle_array->offset(), 0);
  ASSERT_EQ(rle_array->data()->null_count, 0);
  // one dummy buffer, since code may assume there is exactly one buffer
  ASSERT_EQ(rle_array->data()->buffers.size(), 1);

  // explicitly passing offset
  ASSERT_OK_AND_ASSIGN(rle_array,
                       RunLengthEncodedArray::Make(size_values, string_values, 2, 1));
  ASSERT_EQ(rle_array->length(), 2);
  ASSERT_ARRAYS_EQUAL(*rle_array->values_array(), *string_values);
  ASSERT_ARRAYS_EQUAL(*rle_array->run_ends_array(), *size_values);
  ASSERT_EQ(rle_array->offset(), 1);
  // explicitly access null count variable so it is not calculated automatically
  ASSERT_EQ(rle_array->data()->null_count, 0);

  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Run ends array must be int16, int32 or int64 type",
                             RunLengthEncodedArray::Make(string_values, int32_values, 3));
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid, "Invalid: Run ends array cannot contain null values",
      RunLengthEncodedArray::Make(size_only_null, int32_values, 3));
}

TEST_P(TestRunLengthEncodedArray, OffsetLength) {
  auto run_ends = ArrayFromJSON(run_ends_type, "[100, 200, 300, 400, 500]");
  auto values = ArrayFromJSON(utf8(), R"(["Hello", "beautiful", "world", "of", "RLE"])");
  ASSERT_OK_AND_ASSIGN(auto rle_array,
                       RunLengthEncodedArray::Make(run_ends, values, 500));

  ASSERT_EQ(rle_array->GetPhysicalLength(), 5);
  ASSERT_EQ(rle_array->GetPhysicalOffset(), 0);

  auto slice = std::dynamic_pointer_cast<RunLengthEncodedArray>(rle_array->Slice(199, 5));
  ASSERT_EQ(slice->GetPhysicalLength(), 2);
  ASSERT_EQ(slice->GetPhysicalOffset(), 1);

  auto slice2 =
      std::dynamic_pointer_cast<RunLengthEncodedArray>(rle_array->Slice(199, 101));
  ASSERT_EQ(slice2->GetPhysicalLength(), 2);
  ASSERT_EQ(slice2->GetPhysicalOffset(), 1);

  auto slice3 =
      std::dynamic_pointer_cast<RunLengthEncodedArray>(rle_array->Slice(400, 100));
  ASSERT_EQ(slice3->GetPhysicalLength(), 1);
  ASSERT_EQ(slice3->GetPhysicalOffset(), 4);

  auto slice4 =
      std::dynamic_pointer_cast<RunLengthEncodedArray>(rle_array->Slice(0, 150));
  ASSERT_EQ(slice4->GetPhysicalLength(), 2);
  ASSERT_EQ(slice4->GetPhysicalOffset(), 0);

  auto zero_length_at_end =
      std::dynamic_pointer_cast<RunLengthEncodedArray>(rle_array->Slice(500, 0));
  ASSERT_EQ(zero_length_at_end->GetPhysicalLength(), 0);
  ASSERT_EQ(zero_length_at_end->GetPhysicalOffset(), 5);
}

TEST_P(TestRunLengthEncodedArray, Validate) {
  auto run_ends_good = ArrayFromJSON(run_ends_type, "[10, 20, 30, 40]");
  auto values = ArrayFromJSON(utf8(), R"(["A", "B", "C", null])");
  auto malformed_array = ArrayFromJSON(run_ends_type, "[10, 20, 30, 40]");
  malformed_array->data()->buffers.clear();
  auto run_ends_with_zero = ArrayFromJSON(run_ends_type, "[0, 20, 30, 40]");
  auto run_ends_with_null = ArrayFromJSON(run_ends_type, "[0, 20, 30, null]");
  auto run_ends_not_ordered = ArrayFromJSON(run_ends_type, "[10, 20, 40, 40]");
  auto run_ends_too_low = ArrayFromJSON(run_ends_type, "[10, 20, 40, 39]");
  auto empty_ints = ArrayFromJSON(run_ends_type, "[]");
  auto run_ends_require64 = ArrayFromJSON(int64(), "[10, 9223372036854775807]");
  int64_t long_length = 0;
  // for int32+ we need a valid array of length numeric_limits<int32_t>::max() + 1, but
  // for int16, int32 we can't create a run ends value that high
  if (run_ends_type->id() == Type::INT16) {
    long_length = std::numeric_limits<int16_t>::max();
  } else if (run_ends_type->id() == Type::INT32) {
    long_length = std::numeric_limits<int32_t>::max();
  } else {
    long_length = std::numeric_limits<int64_t>::max();
  }
  auto run_ends_long = ArrayFromJSON(
      run_ends_type, std::string("[10, ") + std::to_string(long_length) + "]");

  ASSERT_OK_AND_ASSIGN(auto good_array,
                       RunLengthEncodedArray::Make(run_ends_good, values, 40));
  ASSERT_OK(good_array->ValidateFull());

  ASSERT_OK_AND_ASSIGN(
      auto require64_array,
      RunLengthEncodedArray::Make(run_ends_require64, values, 9223372036854775806));
  ASSERT_OK(require64_array->ValidateFull());

  auto sliced = good_array->Slice(5, 20);
  ASSERT_OK(sliced->ValidateFull());

  auto sliced_at_run_end = good_array->Slice(10, 20);
  ASSERT_OK(sliced_at_run_end->ValidateFull());

  ASSERT_OK_AND_ASSIGN(
      auto sliced_children,
      RunLengthEncodedArray::Make(run_ends_good->Slice(1, 2), values->Slice(1, 3), 15));
  ASSERT_OK(sliced_children->ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunLengthEncodedArray::Make(empty_ints, empty_ints, 0));
  ASSERT_OK(empty_array->ValidateFull());

  auto empty_run_ends = MakeArray(empty_array->data()->Copy());
  empty_run_ends->data()->length = 1;
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      "Invalid: RLE array has non-zero length 1, but run ends array has zero length",
      empty_run_ends->Validate());

  auto offset_length_overflow = MakeArray(good_array->data()->Copy());
  offset_length_overflow->data()->offset = std::numeric_limits<int64_t>::max();
  offset_length_overflow->data()->length = 1;
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: Array of type run_length_encoded<run_ends: ") +
          run_ends_type->ToString() +
          ", values: string> has impossibly large length and offset",
      offset_length_overflow->Validate());

  ASSERT_OK_AND_ASSIGN(auto too_large_for_rle16,
                       RunLengthEncodedArray::Make(run_ends_long, values, 40));
  too_large_for_rle16->data()->offset = std::numeric_limits<int16_t>::max();
  too_large_for_rle16->data()->length = 1;
  if (run_ends_type->id() == Type::INT16) {
    ASSERT_RAISES_WITH_MESSAGE(
        Invalid,
        "Invalid: Offset + length of an RLE array must fit in a value of the run ends "
        "type int16, but offset + length was 32768 while the allowed maximum is 32767",
        too_large_for_rle16->Validate());
  } else {
    ASSERT_OK(too_large_for_rle16->ValidateFull());
  }

  ASSERT_OK_AND_ASSIGN(auto too_large_for_rle32,
                       RunLengthEncodedArray::Make(run_ends_long, values, 40));
  too_large_for_rle32->data()->offset = std::numeric_limits<int32_t>::max();
  too_large_for_rle32->data()->length = 1;
  if (run_ends_type->id() == Type::INT16) {
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Offset + length of an RLE array must fit in a "
                               "value of the run ends type int16, but offset + length "
                               "was 2147483648 while the allowed maximum is 32767",
                               too_large_for_rle32->Validate());
  } else if (run_ends_type->id() == Type::INT32) {
    ASSERT_RAISES_WITH_MESSAGE(Invalid,
                               "Invalid: Offset + length of an RLE array must fit in a "
                               "value of the run ends type int32, but offset + length "
                               "was 2147483648 while the allowed maximum is 2147483647",
                               too_large_for_rle32->Validate());
  } else {
    ASSERT_OK(too_large_for_rle32->ValidateFull());
  }

  auto too_many_children = MakeArray(good_array->data()->Copy());
  too_many_children->data()->child_data.push_back(NULLPTR);
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: Expected 2 child arrays in array of type "
                  "run_length_encoded<run_ends: ") +
          run_ends_type->ToString() + ", values: string>, got 3",
      too_many_children->Validate());

  auto run_ends_nullptr = MakeArray(good_array->data()->Copy());
  run_ends_nullptr->data()->child_data[0] = NULLPTR;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Run ends array is null pointer",
                             run_ends_nullptr->Validate());

  auto values_nullptr = MakeArray(good_array->data()->Copy());
  values_nullptr->data()->child_data[1] = NULLPTR;
  ASSERT_RAISES_WITH_MESSAGE(Invalid, "Invalid: Values array is null pointer",
                             values_nullptr->Validate());

  auto run_ends_string = MakeArray(good_array->data()->Copy());
  run_ends_string->data()->child_data[0] = values->data();
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: Run ends array of run_length_encoded<run_ends: ") +
          run_ends_type->ToString() + ", values: string> must be " +
          run_ends_type->ToString() + ", but is string",
      run_ends_string->Validate());

  auto wrong_type = MakeArray(good_array->data()->Copy());
  wrong_type->data()->type = run_length_encoded(run_ends_type, uint16());
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Parent type says this array encodes uint16 "
                             "values, but values array has type string",
                             wrong_type->Validate());

  ASSERT_OK_AND_ASSIGN(auto run_ends_malformed,
                       RunLengthEncodedArray::Make(malformed_array, values, 40));
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: Run ends array invalid: Invalid: Expected 2 "
                  "buffers in array of type ") +
          run_ends_type->ToString() + ", got 0",
      run_ends_malformed->Validate());

  ASSERT_OK_AND_ASSIGN(auto values_malformed,
                       RunLengthEncodedArray::Make(run_ends_good, malformed_array, 40));
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: Values array invalid: Invalid: Expected 2 buffers "
                  "in array of type ") +
          run_ends_type->ToString() + ", got 0",
      values_malformed->Validate());

  auto null_count = MakeArray(good_array->data()->Copy());
  null_count->data()->null_count = kUnknownNullCount;
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Null count must be 0 for RLE array, but was -1",
                             null_count->Validate());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> run_end_zero_array,
                       RunLengthEncodedArray::Make(run_ends_with_zero, values, 40));
  ASSERT_OK(run_end_zero_array->Validate());
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Run ends array invalid: All run ends must be a "
                             "positive integer but run end 0 is 0",
                             run_end_zero_array->ValidateFull());
  // The whole run ends array has to be valid even if the parent is sliced
  run_end_zero_array = run_end_zero_array->Slice(30, 0);
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Run ends array invalid: All run ends must be a "
                             "positive integer but run end 0 is 0",
                             run_end_zero_array->ValidateFull());

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> run_ends_not_ordered_array,
                       RunLengthEncodedArray::Make(run_ends_not_ordered, values, 40));
  ASSERT_OK(run_ends_not_ordered_array->Validate());
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      "Invalid: Run ends array invalid: Each run end must be greater than the prevous "
      "one, but run end 3 is 40 and run end 2 is 40",
      run_ends_not_ordered_array->ValidateFull());
  // The whole run ends array has to be valid even if the parent is sliced
  run_ends_not_ordered_array = run_ends_not_ordered_array->Slice(30, 0);
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      "Invalid: Run ends array invalid: Each run end must be greater than the prevous "
      "one, but run end 3 is 40 and run end 2 is 40",
      run_ends_not_ordered_array->ValidateFull());

  ASSERT_OK_AND_ASSIGN(auto run_ends_too_low_array,
                       RunLengthEncodedArray::Make(run_ends_too_low, values, 40));
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Last run in run ends array ends at 39 but this "
                             "array requires at least 40 (offset 0, length 40)",
                             run_ends_too_low_array->Validate());

  ASSERT_OK_AND_ASSIGN(auto values_too_short_array,
                       RunLengthEncodedArray::Make(run_ends_good, values->Slice(1), 40));
  ASSERT_OK(values_too_short_array->Validate());
  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Values array needs at least 4 elements to hold "
                             "the runs described by the run ends array, but only has 3",
                             values_too_short_array->ValidateFull());
}

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunLengthEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));
}  // anonymous namespace

}  // namespace arrow
