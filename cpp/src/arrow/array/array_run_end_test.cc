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
// Run-end encoded array tests

namespace {

class TestRunEndEncodedArray
    : public ::testing::TestWithParam<std::shared_ptr<DataType>> {
 protected:
  std::shared_ptr<DataType> run_end_type;
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;
  std::shared_ptr<Array> run_end_values;
  std::shared_ptr<Array> run_end_only_null;

  void SetUp() override {
    run_end_type = GetParam();

    string_values = ArrayFromJSON(utf8(), R"(["Hello", "World", null])");
    int32_values = ArrayFromJSON(int32(), "[10, 20, 30]");
    int16_values = ArrayFromJSON(int16(), "[10, 20, 30]");
    run_end_values = ArrayFromJSON(run_end_type, "[10, 20, 30]");
    run_end_only_null = ArrayFromJSON(run_end_type, "[null, null, null]");
  }
};

TEST_P(TestRunEndEncodedArray, MakeArray) {
  ASSERT_OK_AND_ASSIGN(auto ree_array,
                       RunEndEncodedArray::Make(30, int32_values, string_values));
  auto array_data = ree_array->data();
  auto new_array = MakeArray(array_data);
  ASSERT_ARRAYS_EQUAL(*new_array, *ree_array);
  // Should be the exact same ArrayData object
  ASSERT_EQ(new_array->data(), array_data);
  ASSERT_NE(std::dynamic_pointer_cast<RunEndEncodedArray>(new_array), NULLPTR);
}

TEST_P(TestRunEndEncodedArray, FromRunEndsAndValues) {
  std::shared_ptr<RunEndEncodedArray> ree_array;

  ASSERT_OK_AND_ASSIGN(ree_array,
                       RunEndEncodedArray::Make(30, run_end_values, int32_values));
  ASSERT_EQ(ree_array->length(), 30);
  ASSERT_ARRAYS_EQUAL(*ree_array->values(), *int32_values);
  ASSERT_ARRAYS_EQUAL(*ree_array->run_ends(), *run_end_values);
  ASSERT_EQ(ree_array->offset(), 0);
  ASSERT_EQ(ree_array->data()->null_count, 0);
  // Existing code might assume at least one buffer,
  // so RunEndEncodedArray should be built with one
  ASSERT_EQ(ree_array->data()->buffers.size(), 1);

  // Passing a non-zero logical offset
  ASSERT_OK_AND_ASSIGN(ree_array,
                       RunEndEncodedArray::Make(29, run_end_values, string_values, 1));
  ASSERT_EQ(ree_array->length(), 29);
  ASSERT_ARRAYS_EQUAL(*ree_array->values(), *string_values);
  ASSERT_ARRAYS_EQUAL(*ree_array->run_ends(), *run_end_values);
  ASSERT_EQ(ree_array->data()->null_count, 0);
  ASSERT_EQ(ree_array->offset(), 1);

  ASSERT_RAISES_WITH_MESSAGE(Invalid,
                             "Invalid: Run end type must be int16, int32 or int64",
                             RunEndEncodedArray::Make(30, string_values, int32_values));
  ASSERT_RAISES_WITH_MESSAGE(
      Invalid, "Invalid: Run ends array cannot contain null values",
      RunEndEncodedArray::Make(30, run_end_only_null, int32_values));
}

TEST_P(TestRunEndEncodedArray, FindOffsetAndLength) {
  auto run_ends = ArrayFromJSON(run_end_type, "[100, 200, 300, 400, 500]");
  auto values = ArrayFromJSON(utf8(), R"(["Hello", "beautiful", "world", "of", "REE"])");
  ASSERT_OK_AND_ASSIGN(auto ree_array, RunEndEncodedArray::Make(500, run_ends, values));

  ASSERT_EQ(ree_array->FindPhysicalOffset(), 0);
  ASSERT_EQ(ree_array->FindPhysicalLength(), 5);

  auto slice = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(199, 5));
  ASSERT_EQ(slice->FindPhysicalOffset(), 1);
  ASSERT_EQ(slice->FindPhysicalLength(), 2);

  auto slice2 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(199, 101));
  ASSERT_EQ(slice2->FindPhysicalOffset(), 1);
  ASSERT_EQ(slice2->FindPhysicalLength(), 2);

  auto slice3 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(400, 100));
  ASSERT_EQ(slice3->FindPhysicalOffset(), 4);
  ASSERT_EQ(slice3->FindPhysicalLength(), 1);

  auto slice4 = std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(0, 150));
  ASSERT_EQ(slice4->FindPhysicalOffset(), 0);
  ASSERT_EQ(slice4->FindPhysicalLength(), 2);

  auto zero_length_at_end =
      std::dynamic_pointer_cast<RunEndEncodedArray>(ree_array->Slice(500, 0));
  ASSERT_EQ(zero_length_at_end->FindPhysicalOffset(), 5);
  ASSERT_EQ(zero_length_at_end->FindPhysicalLength(), 0);
}

TEST_P(TestRunEndEncodedArray, Compare) {
  ASSERT_OK_AND_ASSIGN(auto ree_array,
                       RunEndEncodedArray::Make(30, run_end_values, string_values));

  auto copy = MakeArray(ree_array->data()->Copy());
  ASSERT_ARRAYS_EQUAL(*ree_array, *copy);

  ASSERT_FALSE(ree_array->Slice(0, 29)->Equals(*ree_array->Slice(1, 29)));

  // Two same-length slice pairs
  ASSERT_ARRAYS_EQUAL(*ree_array->Slice(0, 9), *ree_array->Slice(1, 9));
  ASSERT_FALSE(ree_array->Slice(5, 9)->Equals(*ree_array->Slice(6, 9)));

  // Array that is logically the same as our ree_array, but has 2
  // small runs for the first value instead of a single larger run
  auto equivalent_run_ends = ArrayFromJSON(run_end_type, "[5, 10, 20, 30]");
  auto string_values = ArrayFromJSON(utf8(), R"(["Hello", "Hello", "World", null])");
  ASSERT_OK_AND_ASSIGN(auto equivalent_array,
                       RunEndEncodedArray::Make(30, equivalent_run_ends, string_values));
  ASSERT_ARRAYS_EQUAL(*ree_array, *equivalent_array);

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunEndEncodedArray::Make(0, ArrayFromJSON(run_end_type, "[]"),
                                                ArrayFromJSON(binary(), "[]")));
  ASSERT_ARRAYS_EQUAL(*empty_array, *MakeArray(empty_array->data()->Copy()));

  // Three different slices that have the value [3, 3, 3, 4, 4, 4, 4]
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_a,
      RunEndEncodedArray::Make(60, ArrayFromJSON(run_end_type, "[2, 5, 12, 58, 60]"),
                               ArrayFromJSON(int64(), "[1, 2, 3, 4, 5]")));
  ASSERT_OK_AND_ASSIGN(
      auto different_offsets_b,
      RunEndEncodedArray::Make(100, ArrayFromJSON(run_end_type, "[81, 86, 99, 100]"),
                               ArrayFromJSON(int64(), "[2, 3, 4, 5]")));
  ASSERT_OK_AND_ASSIGN(auto different_offsets_c,
                       RunEndEncodedArray::Make(7, ArrayFromJSON(run_end_type, "[3, 7]"),
                                                ArrayFromJSON(int64(), "[3, 4]")));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7),
                      *different_offsets_b->Slice(83, 7));
  ASSERT_ARRAYS_EQUAL(*different_offsets_a->Slice(9, 7), *different_offsets_c);
  ASSERT_ARRAYS_EQUAL(*different_offsets_b->Slice(83, 7), *different_offsets_c);
}

}  // anonymous namespace

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunEndEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));

}  // namespace arrow
