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

#include "arrow/testing/gtest_util.h"
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

TEST(StlUtilTest, VectorFlatten) {
  std::vector<int> a{1, 2, 3};
  std::vector<int> b{4, 5, 6};
  std::vector<int> c{7, 8, 9};
  std::vector<std::vector<int>> vecs{a, b, c};
  auto actual = FlattenVectors(vecs);
  std::vector<int> expected{1, 2, 3, 4, 5, 6, 7, 8, 9};
  ASSERT_EQ(expected, actual);
}

static std::string int_to_str(int val) { return std::to_string(val); }

TEST(StlUtilTest, VectorMap) {
  std::vector<int> input{1, 2, 3};
  std::vector<std::string> expected{"1", "2", "3"};

  auto actual = MapVector(int_to_str, input);
  ASSERT_EQ(expected, actual);

  auto bind_fn = std::bind(int_to_str, std::placeholders::_1);
  actual = MapVector(bind_fn, input);
  ASSERT_EQ(expected, actual);

  std::function<std::string(int)> std_fn = int_to_str;
  actual = MapVector(std_fn, input);
  ASSERT_EQ(expected, actual);

  actual = MapVector([](int val) { return std::to_string(val); }, input);
  ASSERT_EQ(expected, actual);
}

TEST(StlUtilTest, VectorMaybeMapFails) {
  std::vector<int> input{1, 2, 3};
  auto mapper = [](int item) -> Result<std::string> {
    if (item == 1) {
      return Status::Invalid("XYZ");
    }
    return std::to_string(item);
  };
  ASSERT_RAISES(Invalid, MaybeMapVector(mapper, input));
}

TEST(StlUtilTest, VectorMaybeMap) {
  std::vector<int> input{1, 2, 3};
  std::vector<std::string> expected{"1", "2", "3"};
  EXPECT_OK_AND_ASSIGN(
      auto actual,
      MaybeMapVector([](int item) -> Result<std::string> { return std::to_string(item); },
                     input));
  ASSERT_EQ(expected, actual);
}

TEST(StlUtilTest, VectorUnwrapOrRaise) {
  // TODO(ARROW-11998) There should be an easier way to construct these vectors
  std::vector<Result<MoveOnlyDataType>> all_good;
  all_good.push_back(Result<MoveOnlyDataType>(MoveOnlyDataType(1)));
  all_good.push_back(Result<MoveOnlyDataType>(MoveOnlyDataType(2)));
  all_good.push_back(Result<MoveOnlyDataType>(MoveOnlyDataType(3)));

  std::vector<Result<MoveOnlyDataType>> some_bad;
  some_bad.push_back(Result<MoveOnlyDataType>(MoveOnlyDataType(1)));
  some_bad.push_back(Result<MoveOnlyDataType>(Status::Invalid("XYZ")));
  some_bad.push_back(Result<MoveOnlyDataType>(Status::IOError("XYZ")));

  EXPECT_OK_AND_ASSIGN(auto unwrapped, UnwrapOrRaise(std::move(all_good)));
  std::vector<MoveOnlyDataType> expected;
  expected.emplace_back(1);
  expected.emplace_back(2);
  expected.emplace_back(3);

  ASSERT_EQ(expected, unwrapped);

  ASSERT_RAISES(Invalid, UnwrapOrRaise(std::move(some_bad)));
}

}  // namespace internal
}  // namespace arrow
