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

#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/functional.h"
#include "arrow/util/sort.h"
#include "arrow/util/string.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace internal {

TEST(StlUtilTest, VectorAddRemoveTest) {
  std::vector<int> values;
  std::vector<int> result = AddVectorElement(values, 0, 100);
  EXPECT_EQ(values.size(), 0);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result[0], 100);

  // Add 200 at index 0 and 300 at the end.
  std::vector<int> result2 = AddVectorElement(result, 0, 200);
  result2 = AddVectorElement(result2, result2.size(), 300);
  EXPECT_EQ(result.size(), 1);
  EXPECT_EQ(result2.size(), 3);
  EXPECT_EQ(result2[0], 200);
  EXPECT_EQ(result2[1], 100);
  EXPECT_EQ(result2[2], 300);

  // Remove 100, 300, 200
  std::vector<int> result3 = DeleteVectorElement(result2, 1);
  EXPECT_EQ(result2.size(), 3);
  EXPECT_EQ(result3.size(), 2);
  EXPECT_EQ(result3[0], 200);
  EXPECT_EQ(result3[1], 300);

  result3 = DeleteVectorElement(result3, 1);
  EXPECT_EQ(result3.size(), 1);
  EXPECT_EQ(result3[0], 200);

  result3 = DeleteVectorElement(result3, 0);
  EXPECT_TRUE(result3.empty());
}

void ExpectSortPermutation(std::vector<std::string> unsorted,
                           std::vector<int64_t> expected_indices,
                           size_t expected_cycle_count) {
  auto actual_indices = ArgSort(unsorted);
  EXPECT_THAT(actual_indices, ::testing::ContainerEq(expected_indices));

  auto sorted = unsorted;
  std::sort(sorted.begin(), sorted.end());

  auto permuted = unsorted;
  EXPECT_EQ(Permute(expected_indices, &permuted), expected_cycle_count);

  EXPECT_THAT(permuted, ::testing::ContainerEq(sorted));
}

TEST(StlUtilTest, ArgSortPermute) {
  std::string f = "foxtrot", a = "alpha", b = "bravo", d = "delta", c = "charlie",
              e = "echo";

  ExpectSortPermutation({a, f}, {0, 1}, 2);
  ExpectSortPermutation({f, a}, {1, 0}, 1);
  ExpectSortPermutation({a, b, c}, {0, 1, 2}, 3);
  ExpectSortPermutation({a, c, b}, {0, 2, 1}, 2);
  ExpectSortPermutation({c, a, b}, {1, 2, 0}, 1);
  ExpectSortPermutation({a, b, c, d, e, f}, {0, 1, 2, 3, 4, 5}, 6);
  ExpectSortPermutation({f, e, d, c, b, a}, {5, 4, 3, 2, 1, 0}, 3);
  ExpectSortPermutation({d, f, e, c, b, a}, {5, 4, 3, 0, 2, 1}, 1);
  ExpectSortPermutation({b, a, c, d, f, e}, {1, 0, 2, 3, 5, 4}, 4);
  ExpectSortPermutation({c, b, a, d, e, f}, {2, 1, 0, 3, 4, 5}, 5);
  ExpectSortPermutation({b, c, a, f, d, e}, {2, 0, 1, 4, 5, 3}, 2);
  ExpectSortPermutation({b, c, d, e, a, f}, {4, 0, 1, 2, 3, 5}, 2);
}

TEST(StlUtilTest, MapEmplaceBack) {
  auto all_good = EmplacedMappedVector<Result<MoveOnlyDataType>>(
      Constructor<MoveOnlyDataType>(), 1, 2, 3);

  ASSERT_EQ(all_good[0].ValueUnsafe(), 1);
  ASSERT_EQ(all_good[1].ValueUnsafe(), 2);
  ASSERT_EQ(all_good[2].ValueUnsafe(), 3);
  ASSERT_EQ(all_good.size(), 3);

  auto some_bad = EmplacedVector<Result<MoveOnlyDataType>>(
      MoveOnlyDataType(1), Status::Invalid("XYZ"), Status::IOError("XYZ"));

  ASSERT_EQ(some_bad[0].ValueUnsafe(), 1);
  ASSERT_TRUE(some_bad[1].status().IsInvalid());
  ASSERT_TRUE(some_bad[2].status().IsIOError());
}

}  // namespace internal
}  // namespace arrow
