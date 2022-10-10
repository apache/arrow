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
#include "arrow/pretty_print.h"
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
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;
  std::shared_ptr<Array> size_values;
  std::shared_ptr<Array> size_only_null;

  virtual void SetUp() override {
    std::shared_ptr<DataType> run_ends_type = GetParam();

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

TEST(RunLengthEncodedArray, OffsetLength) {
  auto run_ends = ArrayFromJSON(int32(), "[100, 200, 300, 400, 500]");
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
}

TEST(RunLengthEncodedArray, Printing) {
  ASSERT_OK_AND_ASSIGN(auto int_array,
                       RunLengthEncodedArray::Make(int32_values, int32_values, 30));
  std::stringstream ss;
  ASSERT_OK(PrettyPrint(*int_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run ends array (offset: 0, logical length: 30)\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]");

  ASSERT_OK_AND_ASSIGN(auto string_array,
                       RunLengthEncodedArray::Make(int32_values, string_values, 30));
  ss = {};
  ASSERT_OK(PrettyPrint(*string_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run ends array (offset: 0, logical length: 30)\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    \"Hello\",\n"
            "    \"World\",\n"
            "    null\n"
            "  ]");

  auto sliced_array = string_array->Slice(15, 6);
  ss = {};
  ASSERT_OK(PrettyPrint(*sliced_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run ends array (offset: 15, logical length: 6)\n"
            "  [\n"
            "    10,\n"
            "    20,\n"
            "    30\n"
            "  ]\n"
            "-- values:\n"
            "  [\n"
            "    \"Hello\",\n"
            "    \"World\",\n"
            "    null\n"
            "  ]");

  ASSERT_OK_AND_ASSIGN(auto empty_array,
                       RunLengthEncodedArray::Make(ArrayFromJSON(int32(), "[]"),
                                                   ArrayFromJSON(binary(), "[]"), 0));

  ss = {};
  ASSERT_OK(PrettyPrint(*empty_array, {}, &ss));
  ASSERT_EQ(ss.str(),
            "\n"
            "-- run ends array (offset: 0, logical length: 0)\n"
            "  []\n"
            "-- values:\n"
            "  []");
}

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunLengthEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));
}  // anonymous namespace

}  // namespace arrow
