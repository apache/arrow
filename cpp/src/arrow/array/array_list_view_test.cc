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

#include "arrow/array/array_nested.h"
#include "arrow/array/util.h"
#include "arrow/pretty_print.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// List-view array tests

namespace {

class TestListViewArray : public ::testing::Test {
 public:
  std::shared_ptr<Array> string_values;
  std::shared_ptr<Array> int32_values;
  std::shared_ptr<Array> int16_values;

  void SetUp() override {
    string_values = ArrayFromJSON(utf8(), R"(["Hello", "World", null])");
    int32_values = ArrayFromJSON(int32(), "[1, 20, 3]");
    int16_values = ArrayFromJSON(int16(), "[10, 2, 30]");
  }

  static std::shared_ptr<Array> Offsets(std::string_view json) {
    return ArrayFromJSON(int32(), json);
  }

  static std::shared_ptr<Array> Sizes(std::string_view json) {
    return ArrayFromJSON(int32(), json);
  }
};

}  // namespace

TEST_F(TestListViewArray, MakeArray) {
  ASSERT_OK_AND_ASSIGN(auto list_view_array,
                       ListViewArray::FromArrays(*Offsets("[0, 0, 1, 2]"),
                                                 *Sizes("[2, 1, 1, 1]"), *string_values));
  auto array_data = list_view_array->data();
  auto new_array = MakeArray(array_data);
  ASSERT_ARRAYS_EQUAL(*new_array, *list_view_array);
  // Should be the exact same ArrayData object
  ASSERT_EQ(new_array->data(), array_data);
  ASSERT_NE(std::dynamic_pointer_cast<ListViewArray>(new_array), NULLPTR);
}

TEST_F(TestListViewArray, FromOffsetsAndSizes) {
  std::shared_ptr<ListViewArray> list_view_array;

  ASSERT_OK_AND_ASSIGN(list_view_array, ListViewArray::FromArrays(
                                            *Offsets("[0, 0, 1, 1000]"),
                                            *Sizes("[2, 1, 1, null]"), *int32_values));
  ASSERT_EQ(list_view_array->length(), 4);
  ASSERT_ARRAYS_EQUAL(*list_view_array->values(), *int32_values);
  ASSERT_EQ(list_view_array->offset(), 0);
  ASSERT_EQ(list_view_array->data()->GetNullCount(), 1);
  ASSERT_EQ(list_view_array->data()->buffers.size(), 3);
}

}  // namespace arrow
