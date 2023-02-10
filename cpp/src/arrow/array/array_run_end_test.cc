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
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;
  std::shared_ptr<Array> run_end_values;
  std::shared_ptr<Array> run_end_only_null;

  void SetUp() override {
    std::shared_ptr<DataType> run_end_type = GetParam();

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

}  // anonymous namespace

INSTANTIATE_TEST_SUITE_P(EncodedArrayTests, TestRunEndEncodedArray,
                         ::testing::Values(int16(), int32(), int64()));

}  // namespace arrow
