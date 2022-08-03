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

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/rle_util.h"

namespace arrow {
namespace rle_util {

TEST(TestRleUtil, FindPhysicalOffsetTest) {
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1}, 1, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3*4, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3*4, 1), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3*4, 2), 2);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3*4, 3), 3);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3*4, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3*4, 1), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3*4, 2), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3*4, 3), 2);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 4, 6}, 3*4, 3), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4, 5}, 3*4+3, 100), 2);
}


TEST(TestRleUtil, VisitMergedRuns) {
  const auto left_run_ends = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005, 1015, 1020, 1025, 1050]");
  const auto right_run_ends = ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 2005, 2009, 2025, 2050]");
  const std::vector<int32_t> expected_run_lengths = {5, 4, 6, 5, 5, 25};
  const std::vector<int32_t> expected_left_visits = {5, 4, 6, 5, 5};
  const std::vector<int32_t> expected_right_visits = {5, 4, 6, 5, 5};
  const int32_t left_parent_offset = 1000;
  const int32_t left_child_offset = 100;
  const int32_t right_parent_offset = 200;
  const int32_t right_child_offset = 100;

  std::shared_ptr<Array> left_child = std::make_shared<NullArray>(left_child_offset + left_run_ends->length());
  std::shared_ptr<Array> right_child = std::make_shared<NullArray>(right_child_offset + right_run_ends->length());

  left_child = left_child->Slice(left_child_offset);
  right_child = right_child->Slice(right_child_offset);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> left_array, RunLengthEncodedArray::Make(left_run_ends, left_child, 1026));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> right_array, RunLengthEncodedArray::Make(right_run_ends, right_child, 2026));

  left_array = left_array->Slice(left_parent_offset);
  left_array = left_array->Slice(left_parent_offset);

  size_t position = 0;

  rle_util::VisitMergedRuns(ArraySpan(*left_array->data()), ArraySpan(*left_array->data()),
                            [&position, &expected_run_lengths, &expected_left_visits, expected_right_visits](int64_t run_length, int64_t left_index, int64_t right_index) {
    ASSERT_EQ(run_length, expected_run_lengths[position]);
    ASSERT_EQ(left_index, expected_left_visits[position]);
    ASSERT_EQ(run_length, expected_right_visits[position]);
    position++;
  });


  ASSERT_EQ(position, expected_run_lengths.size());


}

}  // namespace rle_util
}  // namespace arrow
