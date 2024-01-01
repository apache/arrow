// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
#include <gtest/gtest.h>
#include "parquet/column_reader.h"

using namespace parquet;

class RowRangesTest : public ::testing::Test {
 protected:
  RowRanges rowRanges;
};

TEST_F(RowRangesTest, SplitAt_EmptySplitPoints_ReturnsOriginalRowRanges) {
  rowRanges.Add(IntervalRange(0, 10));
  std::vector<int64_t> split_points;

  auto result = rowRanges.SplitAt(split_points);

  ASSERT_EQ(result.size(), 1);
  ASSERT_EQ(result[0].GetRanges().size(), 1);
  ASSERT_EQ(result[0][0].start, 0);
  ASSERT_EQ(result[0][0].end, 10);
}

TEST_F(RowRangesTest, SplitAt_SingleSplitPoint_ReturnsTwoRowRanges) {
  rowRanges.Add(IntervalRange(0, 10));
  std::vector<int64_t> split_points = {5};

  auto result = rowRanges.SplitAt(split_points);

  ASSERT_EQ(result.size(), 2);
  ASSERT_EQ(result[0].GetRanges().size(), 1);
  ASSERT_EQ(result[0][0].start, 0);
  ASSERT_EQ(result[0][0].end, 4);
  ASSERT_EQ(result[1].GetRanges().size(), 1);
  ASSERT_EQ(result[1][0].start, 5);
  ASSERT_EQ(result[1][0].end, 10);
}

TEST_F(RowRangesTest, SplitAt_MultipleSplitPoints_ReturnsMultipleRowRanges) {
  rowRanges.Add(IntervalRange(0, 10));
  std::vector<int64_t> split_points = {3, 7};

  auto result = rowRanges.SplitAt(split_points);

  ASSERT_EQ(result.size(), 3);
  ASSERT_EQ(result[0].GetRanges().size(), 1);
  ASSERT_EQ(result[0][0].start, 0);
  ASSERT_EQ(result[0][0].end, 2);
  ASSERT_EQ(result[1].GetRanges().size(), 1);
  ASSERT_EQ(result[1][0].start, 3);
  ASSERT_EQ(result[1][0].end, 6);
  ASSERT_EQ(result[2].GetRanges().size(), 1);
  ASSERT_EQ(result[2][0].start, 7);
  ASSERT_EQ(result[2][0].end, 10);
}

TEST_F(RowRangesTest, SplitAt_MultipleSplitPoints_ReturnWithEmptyRowRanges) {
  rowRanges.Add(IntervalRange(11, 18));
  std::vector<int64_t> split_points = {5, 10, 15, 20};

  auto result = rowRanges.SplitAt(split_points);

  ASSERT_EQ(result.size(), 5);
  ASSERT_EQ(result[0].GetRanges().size(), 0);
  ASSERT_EQ(result[1].GetRanges().size(), 0);
  ASSERT_EQ(result[2].GetRanges().size(), 1);
  ASSERT_EQ(result[2][0].start, 11);
  ASSERT_EQ(result[2][0].end, 14);
  ASSERT_EQ(result[3].GetRanges().size(), 1);
  ASSERT_EQ(result[3][0].start, 15);
  ASSERT_EQ(result[3][0].end, 18);
  ASSERT_EQ(result[4].GetRanges().size(), 0);
}

TEST_F(RowRangesTest, SplitAt_InvalidSplitPoint_ThrowsException) {
  rowRanges.Add(IntervalRange(0, 10));
  std::vector<int64_t> split_points = {-1};

  ASSERT_THROW(rowRanges.SplitAt(split_points), ParquetException);
}

TEST_F(RowRangesTest, SplitAt_UnorderedSplitPoints_ThrowsException) {
  rowRanges.Add(IntervalRange(0, 10));
  std::vector<int64_t> split_points = {5, 3};

  ASSERT_THROW(rowRanges.SplitAt(split_points), ParquetException);
}
