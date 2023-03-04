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
#include "arrow/type_traits.h"
#include "arrow/util/logging.h"
#include "arrow/util/ree_util.h"

namespace arrow {
namespace ree_util {

template <typename RunEndCType>
struct ReeUtilTest : public ::testing::Test {
  // Re-implementation of FindPhysicalIndex that uses trivial linear-search
  // instead of the more efficient implementation.
  int64_t FindPhysicalIndexTestImpl(const RunEndCType* run_ends, int64_t run_ends_size,
                                    int64_t i, int64_t absolute_offset = 0) {
    for (int64_t j = 0; j < run_ends_size; j++) {
      if (absolute_offset + i < run_ends[j]) {
        return j;
      }
    }
    return run_ends_size;
  }
};
TYPED_TEST_SUITE_P(ReeUtilTest);

TYPED_TEST_P(ReeUtilTest, PhysicalIndex) {
  using RE = TypeParam;  // Run-end type
  const RE run_ends1[] = {1};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends1, 1, 0, 0), 0);
  const RE run_ends124[] = {1, 2, 4};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 0, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 1, 0), 1);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends124, 3, 2, 0), 2);
  const RE run_ends234[] = {2, 3, 4};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 0, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 1, 0), 0);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 2, 0), 1);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends234, 3, 3, 0), 2);
  const RE run_ends246[] = {2, 4, 6};
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 3, 0), 1);

  // Out-of-range logical offset should return run_ends size
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 6, 0), 3);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 3, 1000, 0), 3);
  ASSERT_EQ(internal::FindPhysicalIndex(run_ends246, 0, 5, 0), 0);

  constexpr int64_t kMaxLogicalIndex = 150;
  const RE run_ends[] = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 105, 115, 120, 125, kMaxLogicalIndex};
  for (int64_t i = 0; i <= kMaxLogicalIndex; i++) {
    DCHECK_EQ(internal::FindPhysicalIndex(run_ends, std::size(run_ends), i, 0),
              this->FindPhysicalIndexTestImpl(run_ends, std::size(run_ends), i, 0));
  }
}

TYPED_TEST_P(ReeUtilTest, PhysicalLength) {
  using RE = TypeParam;  // Run-end type

  const RE run_ends0[] = {-1};  // used as zero-length array
  ASSERT_EQ(internal::FindPhysicalLength(run_ends0, 0, 0, 0), 0);

  const RE run_ends1[] = {1};
  ASSERT_EQ(internal::FindPhysicalLength(run_ends1, 1, 1, 0), 1);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends1, 1, 0, 1), 0);

  const RE run_ends124[] = {1, 2, 4};  // run-lengths: 1, 1, 2
  ASSERT_EQ(internal::FindPhysicalLength(run_ends124, 3, 4, 0), 3);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends124, 3, 3, 1), 2);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends124, 3, 2, 2), 1);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends124, 3, 1, 3), 1);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends124, 3, 0, 4), 0);
  const RE run_ends246[] = {2, 4, 6, 7};  // run-lengths: 2, 2, 2, 1
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 7, 0), 4);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 6, 1), 4);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 5, 2), 3);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 4, 3), 3);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 3, 4), 2);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 2, 5), 2);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 1, 6), 1);
  ASSERT_EQ(internal::FindPhysicalLength(run_ends246, 4, 0, 7), 0);
}

TYPED_TEST_P(ReeUtilTest, MergedRunsInterator) {
  // Construct the following two test arrays with a lot of different offsets to test the
  // RLE iterator: left:
  //
  //          child offset: 0
  //                 |
  //                 +---+---+---+---+---+---+---+---+---+----+----+----+----+----+-----+
  // run_ends        | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 |1000|1005|1015|1020|1025|30000|
  // (Int32Array)    +---+---+---+---+---+---+---+---+---+----+----+----+----+----+-----+
  //              ---+---+---+---+---+---+---+---+---+---+----+----+----+----+----+-----+
  // values      ... |   |   |   |   |   |   |   |   |   |    |    |    |    |    |     |
  // (NullArray)  ---+---+---+---+---+---+---+---+---+---+----+----+----+----+----+-----+
  //                 |<--------------- slice of NullArray------------------------------>|
  //                 |                                        |  logical length: 50  |
  //         child offset: 100                                |<-------------------->|
  //                                                          |  physical length: 5  |
  //                                                          |                      |
  //                                                logical offset: 1000
  //                                                physical offset: 10
  //
  // right:
  //           child offset: 0
  //                  |
  //                  +---+---+---+---+---+------+------+------+------+
  //  run_ends        | 1 | 2 | 3 | 4 | 5 | 2005 | 2009 | 2025 | 2050 |
  //  (Int32Array)    +---+---+---+---+---+------+------+------+------+
  //               ---+---+---+---+---+---+------+------+------+------+
  //  values      ... |   |   |   |   |   |      |      |      |      |
  //  (NullArray)  ---+---+---+---+---+---+------+------+------+------+
  //                  |<-------- slice of NullArray------------------>|
  //                  |                        |  logical length: 50  |
  //         child offset: 200                 |<-------------------->|
  //                                           |  physical length: 4
  //                                           |
  //                                 logical offset: 2000
  //                                  physical offset: 5
  //
  const std::shared_ptr<DataType> run_end_type =
      std::make_shared<typename CTypeTraits<TypeParam>::ArrowType>();

  const auto left_run_ends = ArrayFromJSON(
      run_end_type, "[1, 2, 3, 4, 5, 6, 7, 8, 9, 1000, 1005, 1015, 1020, 1025, 30000]");
  const auto right_run_ends =
      ArrayFromJSON(run_end_type, "[1, 2, 3, 4, 5, 2005, 2009, 2025, 2050]");
  const std::vector<int32_t> expected_run_ends = {5, 4, 6, 5, 5, 25};
  const std::vector<int32_t> expected_left_visits = {10, 11, 11, 12, 13, 14};
  const std::vector<int32_t> expected_right_visits = {5, 6, 7, 7, 7, 8};
  const int32_t left_parent_offset = 1000;
  const int32_t left_child_offset = 100;
  const int32_t right_parent_offset = 2000;
  const int32_t right_child_offset = 200;

  std::shared_ptr<Array> left_child =
      std::make_shared<NullArray>(left_child_offset + left_run_ends->length());
  std::shared_ptr<Array> right_child =
      std::make_shared<NullArray>(right_child_offset + right_run_ends->length());

  left_child = left_child->Slice(left_child_offset);
  right_child = right_child->Slice(right_child_offset);

  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> left_array,
                       RunEndEncodedArray::Make(1050, left_run_ends, left_child));
  ASSERT_OK_AND_ASSIGN(std::shared_ptr<Array> right_array,
                       RunEndEncodedArray::Make(2050, right_run_ends, right_child));
  left_array = left_array->Slice(left_parent_offset);
  right_array = right_array->Slice(right_parent_offset);
  const RunEndEncodedArraySpan<TypeParam> left_ree_span(*left_array->data());
  const RunEndEncodedArraySpan<TypeParam> right_ree_span(*right_array->data());

  // Left array on one side and right on the other side
  {
    size_t i = 0;
    size_t logical_pos = 0;
    auto it = MergedRunsIterator(left_ree_span, right_ree_span);
    ASSERT_EQ(it.logical_position(), 0);
    ASSERT_TRUE(!it.isEnd());
    ASSERT_EQ(&it.left(), &left_ree_span);
    ASSERT_EQ(&it.right(), &right_ree_span);
    for (; !it.isEnd(); ++it, ++i) {
      ASSERT_EQ(it.run_length(), expected_run_ends[i]);
      ASSERT_EQ(it.index_into_left_array(), expected_left_visits[i]);
      ASSERT_EQ(it.index_into_right_array(), expected_right_visits[i]);
      ASSERT_EQ(it.logical_position(), logical_pos);
      logical_pos += it.run_length();
    }
    ASSERT_EQ(i, expected_run_ends.size());
  }

  // Left child array on both sides
  const int32_t left_only_run_lengths[] = {5, 10, 5, 5, 25};
  {
    int64_t i = 0;
    int64_t logical_pos = 0;
    auto it = MergedRunsIterator(left_ree_span, left_ree_span);
    ASSERT_EQ(it.logical_position(), 0);
    ASSERT_TRUE(!it.isEnd());
    ASSERT_EQ(&it.left(), &left_ree_span);
    ASSERT_EQ(&it.right(), &left_ree_span);
    for (; !it.isEnd(); ++it, ++i) {
      ASSERT_EQ(it.run_length(), left_only_run_lengths[i]);
      ASSERT_EQ(it.index_into_left_array(), 10 + i);
      ASSERT_EQ(it.index_into_right_array(), 10 + i);
      ASSERT_EQ(it.logical_position(), logical_pos);
      logical_pos += it.run_length();
    }
    ASSERT_EQ(i, std::size(left_only_run_lengths));
  }

  // Stand-alone left array
  {
    int64_t i = 0;
    int64_t logical_pos = 0;
    for (auto it = left_ree_span.begin(); it != left_ree_span.end(); ++it, ++i) {
      ASSERT_EQ(it.run_length(), left_only_run_lengths[i]);
      ASSERT_EQ(it.index_into_array(), 10 + i);
      ASSERT_EQ(it.logical_position(), logical_pos);
      logical_pos += it.run_length();
    }
    ASSERT_EQ(i, std::size(left_only_run_lengths));
  }

  // Right array on both sides
  const int32_t right_only_run_lengths[] = {5, 4, 16, 25};
  {
    int64_t i = 0;
    int64_t logical_pos = 0;
    auto it = MergedRunsIterator(right_ree_span, right_ree_span);
    for (; !it.isEnd(); ++it, ++i) {
      ASSERT_EQ(it.run_length(), right_only_run_lengths[i]);
      ASSERT_EQ(it.index_into_left_array(), 5 + i);
      ASSERT_EQ(it.index_into_right_array(), 5 + i);
      ASSERT_EQ(it.logical_position(), logical_pos);
      logical_pos += it.run_length();
    }
    ASSERT_EQ(i, std::size(right_only_run_lengths));
  }

  {
    int64_t i = 0;
    int64_t logical_pos = 0;
    for (auto it = right_ree_span.begin(); it != right_ree_span.end(); ++it, ++i) {
      ASSERT_EQ(it.run_length(), right_only_run_lengths[i]);
      ASSERT_EQ(it.index_into_array(), 5 + i);
      ASSERT_EQ(it.logical_position(), logical_pos);
      logical_pos += it.run_length();
    }
    ASSERT_EQ(i, std::size(right_only_run_lengths));
  }
}

REGISTER_TYPED_TEST_SUITE_P(ReeUtilTest, PhysicalIndex, PhysicalLength,
                            MergedRunsInterator);

using RunEndsTypes = testing::Types<int16_t, int32_t, int64_t>;
INSTANTIATE_TYPED_TEST_SUITE_P(ReeUtilTest, ReeUtilTest, RunEndsTypes);

}  // namespace ree_util
}  // namespace arrow
