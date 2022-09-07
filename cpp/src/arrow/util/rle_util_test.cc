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
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3, 1), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3}, 3, 2), 2);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3, 0), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3, 1), 0);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3, 2), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 3, 4}, 3, 3), 2);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 4, 6}, 3, 3), 1);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005,
                                                 1015, 1020, 1025, 1050},
                               15, 1000),
            10);

  // out-of-range logical offset should return num_run_ends
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 4, 6}, 3, 6), 3);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 4, 6}, 3, 10000), 3);
  ASSERT_EQ(FindPhysicalOffset((const int32_t[]){2, 4, 6}, 0, 5), 0);
}

TEST(TestRleUtil, ArtificalOffset) {
  const auto values = ArrayFromJSON(int32(), "[1, 2, 3]");
  const auto run_ends = ArrayFromJSON(int32(), "[10, 20, 30]");
  ASSERT_OK_AND_ASSIGN(auto array, RunLengthEncodedArray::Make(run_ends, values, 30));
  AddArtificialOffsetInChildArray(array->data().get(), 100);
  ASSERT_ARRAYS_EQUAL(*values, *array->values_array());
  ASSERT_EQ(array->values_array()->offset(), 100);
}

TEST(TestRleUtil, MergedRunsInterator) {
  const auto left_run_ends = ArrayFromJSON(
      int32(), "[1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005, 1015, 1020, 1025, 30000]");
  const auto right_run_ends =
      ArrayFromJSON(int32(), "[1, 2, 3, 4, 5, 2005, 2009, 2025, 2050]");
  const std::vector<int32_t> expected_run_lengths = {5, 4, 6, 5, 5, 25};
  const std::vector<int32_t> expected_left_visits = {110, 111, 111, 112, 113, 114};
  const std::vector<int32_t> expected_right_visits = {205, 206, 207, 207, 207, 208};
  const int32_t left_parent_offset = 1000;
  const int32_t left_child_offset = 100;
  const int32_t right_parent_offset = 2000;
  const int32_t right_child_offset = 200;

  std::shared_ptr<Array> left_child =
      std::make_shared<NullArray>(left_child_offset + left_run_ends->length());
  std::shared_ptr<Array> right_child =
      std::make_shared<NullArray>(right_child_offset + right_run_ends->length());

  left_child = left_child->Slice(left_child_offset, 50);
  right_child = right_child->Slice(right_child_offset);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> left_array,
                       RunLengthEncodedArray::Make(left_run_ends, left_child, 1050));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> right_array,
                       RunLengthEncodedArray::Make(right_run_ends, right_child, 2050));
  left_array = left_array->Slice(left_parent_offset);
  right_array = right_array->Slice(right_parent_offset);
  ArraySpan left_span(*left_array->data());
  ArraySpan right_span(*right_array->data());

  size_t position = 0;
  size_t logical_position = 0;
  for (auto it = MergedRunsIterator<2>(left_span, right_span);
       it != MergedRunsIterator<2>(); ++it) {
    ASSERT_EQ(it.run_length(), expected_run_lengths[position]);
    ASSERT_EQ(it.index_into_buffer(0), expected_left_visits[position]);
    ASSERT_EQ(it.index_into_buffer(1), expected_right_visits[position]);
    ASSERT_EQ(it.index_into_array(0), expected_left_visits[position] - left_child_offset);
    ASSERT_EQ(it.index_into_array(1),
              expected_right_visits[position] - right_child_offset);
    position++;
    logical_position += it.run_length();
    ASSERT_EQ(it.accumulated_run_length(), logical_position);
  }
  ASSERT_EQ(position, expected_run_lengths.size());

  // left array only
  const std::vector<int32_t> left_only_run_lengths = {5, 10, 5, 5, 25};

  position = 0;
  logical_position = 0;
  for (auto it = MergedRunsIterator<2>(left_span, left_span);
       it != MergedRunsIterator<2>(); ++it) {
    ASSERT_EQ(it.run_length(), left_only_run_lengths[position]);
    ASSERT_EQ(it.index_into_buffer(0), 110 + position);
    ASSERT_EQ(it.index_into_buffer(1), 110 + position);
    ASSERT_EQ(it.index_into_array(0), 10 + position);
    ASSERT_EQ(it.index_into_array(1), 10 + position);
    position++;
    logical_position += it.run_length();
    ASSERT_EQ(it.accumulated_run_length(), logical_position);
  }
  ASSERT_EQ(position, left_only_run_lengths.size());

  position = 0;
  logical_position = 0;
  for (auto it = MergedRunsIterator<1>(left_span); it != MergedRunsIterator<1>(); ++it) {
    ASSERT_EQ(it.run_length(), left_only_run_lengths[position]);
    ASSERT_EQ(it.index_into_buffer(0), 110 + position);
    ASSERT_EQ(it.index_into_array(0), 10 + position);
    position++;
    logical_position += it.run_length();
    ASSERT_EQ(it.accumulated_run_length(), logical_position);
  }
  ASSERT_EQ(position, left_only_run_lengths.size());

  // right array only
  const std::vector<int32_t> right_only_run_lengths = {5, 4, 16, 25};

  position = 0;
  logical_position = 0;
  for (auto it = MergedRunsIterator<2>(right_span, right_span);
       it != MergedRunsIterator<2>(); ++it) {
    ASSERT_EQ(it.run_length(), right_only_run_lengths[position]);
    ASSERT_EQ(it.index_into_buffer(0), 205 + position);
    ASSERT_EQ(it.index_into_buffer(1), 205 + position);
    ASSERT_EQ(it.index_into_array(0), 5 + position);
    ASSERT_EQ(it.index_into_array(1), 5 + position);
    position++;
    logical_position += it.run_length();
    ASSERT_EQ(it.accumulated_run_length(), logical_position);
  }
  ASSERT_EQ(position, right_only_run_lengths.size());

  position = 0;
  logical_position = 0;
  for (auto it = MergedRunsIterator<1>(right_span); it != MergedRunsIterator<1>(); ++it) {
    ASSERT_EQ(it.run_length(), right_only_run_lengths[position]);
    ASSERT_EQ(it.index_into_buffer(0), 205 + position);
    ASSERT_EQ(it.index_into_array(0), 5 + position);
    position++;
    logical_position += it.run_length();
    ASSERT_EQ(it.accumulated_run_length(), logical_position);
  }
  ASSERT_EQ(position, right_only_run_lengths.size());
}

}  // namespace rle_util
}  // namespace arrow
