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
#include "arrow/array/builder_encoded.h"
#include "arrow/array/builder_nested.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/scalar.h"
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

TEST_P(TestRunLengthEncodedArray, Builder) {
  // test data
  auto expected_run_ends =
      ArrayFromJSON(run_ends_type, "[1, 3, 105, 165, 205, 305, 405, 505]");
  auto expected_values = ArrayFromJSON(
      utf8(),
      R"(["unique", null, "common", "common", "appended", "common", "common", "appended"])");
  auto appended_run_ends = ArrayFromJSON(run_ends_type, "[100, 200]");
  auto appended_values = ArrayFromJSON(utf8(), R"(["common", "appended"])");
  ASSERT_OK_AND_ASSIGN(auto appended_array, RunLengthEncodedArray::Make(
                                                appended_run_ends, appended_values, 200));
  auto appended_span = ArraySpan(*appended_array->data());

  // builder
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<ArrayBuilder> builder,
                       MakeBuilder(run_length_encoded(run_ends_type, utf8())));
  auto rle_builder = std::dynamic_pointer_cast<RunLengthEncodedBuilder>(builder);
  ASSERT_NE(rle_builder, NULLPTR);
  ASSERT_OK(builder->AppendScalar(*MakeScalar("unique")));
  ASSERT_OK(builder->AppendNull());
  ASSERT_OK(builder->AppendScalar(*MakeNullScalar(utf8())));
  ASSERT_OK(builder->AppendScalar(*MakeScalar("common"), 100));
  ASSERT_OK(builder->AppendScalar(*MakeScalar("common")));
  ASSERT_OK(builder->AppendScalar(*MakeScalar("common")));
  // append span that starts with the same value as the previous run ends. They are
  // currently not merged for simplicity and performance. This is still a valid rle array
  ASSERT_OK(builder->AppendArraySlice(appended_span, 40, 100));
  ASSERT_OK(builder->AppendArraySlice(appended_span, 0, 100));
  // append one whole array
  ASSERT_OK(builder->AppendArraySlice(appended_span, 0, appended_span.length));
  ASSERT_EQ(builder->length(), 505);
  ASSERT_EQ(*builder->type(), *run_length_encoded(run_ends_type, utf8()));
  ASSERT_OK_AND_ASSIGN(auto array, builder->Finish());
  auto rle_array = std::dynamic_pointer_cast<RunLengthEncodedArray>(array);
  ASSERT_NE(rle_array, NULLPTR);
  ASSERT_ARRAYS_EQUAL(*expected_run_ends, *rle_array->run_ends_array());
  ASSERT_ARRAYS_EQUAL(*expected_values, *rle_array->values_array());
  ASSERT_EQ(array->length(), 505);
  ASSERT_EQ(array->offset(), 0);
}

TEST_P(TestRunLengthEncodedArray, Compare) {
  ASSERT_OK_AND_ASSIGN(auto rle_array,
                       RunLengthEncodedArray::Make(size_values, string_values, 30));

  // second array object that is exactly the same as rle_array
  auto standard_equals = MakeArray(rle_array->data()->Copy());
  ASSERT_ARRAYS_EQUAL(*rle_array, *standard_equals);

  ASSERT_FALSE(rle_array->Slice(0, 29)->Equals(*rle_array->Slice(1, 29)));

  // array that is logically the same as our rle_array, but has 2 small runs for the first
  // value instead of one large
  auto duplicate_run_ends = ArrayFromJSON(run_ends_type, "[5, 10, 20, 30]");
  auto string_values = ArrayFromJSON(utf8(), R"(["Hello", "Hello", "World", null])");
  ASSERT_OK_AND_ASSIGN(auto duplicate_array, RunLengthEncodedArray::Make(
                                                 duplicate_run_ends, string_values, 30));
  ASSERT_ARRAYS_EQUAL(*rle_array, *duplicate_array);

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunLengthEncodedArray::Make(ArrayFromJSON(run_ends_type, "[]"),
                                                   ArrayFromJSON(binary(), "[]"), 0));
  ASSERT_ARRAYS_EQUAL(*empty_array, *MakeArray(empty_array->data()->Copy()));

  // threee different slices that have the value [3, 3, 3, 4, 4, 4, 4]
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_a,
      RunLengthEncodedArray::Make(ArrayFromJSON(run_ends_type, "[2, 5, 12, 58, 60]"),
                                  ArrayFromJSON(int64(), "[1, 2, 3, 4, 5]"), 60));
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_b,
      RunLengthEncodedArray::Make(ArrayFromJSON(run_ends_type, "[81, 86, 99, 100]"),
                                  ArrayFromJSON(int64(), "[2, 3, 4, 5]"), 100));
  ASSERT_OK_AND_ASSIGN(auto different_offsets_c,
                       RunLengthEncodedArray::Make(ArrayFromJSON(run_ends_type, "[3, 7]"),
                                                   ArrayFromJSON(int64(), "[3, 4]"), 7));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7),
                      *different_offsets_b->Slice(83, 7));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7), *different_offsets_c);
  ASSERT_ARRAYS_EQUAL(*different_offsets_b->Slice(83, 7), *different_offsets_c);
}

TEST_P(TestRunLengthEncodedArray, Concatenate) {
  ASSERT_OK_AND_ASSIGN(auto int32_array,
                       RunLengthEncodedArray::Make(size_values, int32_values, 30));
  ASSERT_OK_AND_ASSIGN(auto string_array,
                       RunLengthEncodedArray::Make(size_values, string_values, 30));
  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunLengthEncodedArray::Make(ArrayFromJSON(run_ends_type, "[]"),
                                                   ArrayFromJSON(int32(), "[]"), 0));

  ASSERT_OK_AND_ASSIGN(auto expected_102030_twice,
                       RunLengthEncodedArray::Make(
                           ArrayFromJSON(run_ends_type, "[10, 20, 30, 40, 50, 60]"),
                           ArrayFromJSON(int32(), "[10, 20, 30, 10, 20, 30]"), 60));
  ASSERT_OK_AND_ASSIGN(auto result,
                       Concatenate({int32_array, int32_array}, default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*expected_102030_twice, *result);

  ArrayVector sliced_back_together = {
      int32_array->Slice(0, 1),  int32_array->Slice(5, 5),
      int32_array->Slice(0, 4),  int32_array->Slice(10, 7),
      int32_array->Slice(3, 0),  int32_array->Slice(10, 1),
      int32_array->Slice(18, 7), empty_array,
      int32_array->Slice(25, 5)};
  ASSERT_OK_AND_ASSIGN(result, Concatenate(sliced_back_together, default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*int32_array, *result);

  ASSERT_OK_AND_ASSIGN(result, Concatenate({empty_array, empty_array, empty_array},
                                           default_memory_pool()));
  ASSERT_ARRAYS_EQUAL(*empty_array, *result);

  ASSERT_RAISES_WITH_MESSAGE(
      Invalid,
      std::string("Invalid: arrays to be concatenated must be identically typed, but "
                  "run_length_encoded<run_ends: ") +
          run_ends_type->ToString() +
          ", values: int32> and run_length_encoded<run_ends: " +
          run_ends_type->ToString() + ", values: string> were encountered.",
      Concatenate({int32_array, string_array}, default_memory_pool()));
}

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunLengthEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));
}  // anonymous namespace

}  // namespace arrow
