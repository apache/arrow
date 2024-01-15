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
  IntervalRanges row_ranges;
};

TEST_F(RowRangesTest, EmptyRG_ReturnsOriginalRowRanges) {
  row_ranges.Add(IntervalRange(0, 10));
  std::vector<int64_t> rows_per_rg;

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 1);

  auto iter = result[0]->NewIterator();
  auto range = std::get<IntervalRange>(iter->NextRange());
  ASSERT_EQ(range.start, 0);
  ASSERT_EQ(range.end, 10);
  ASSERT_EQ(iter->NextRange().index(), 2);
}

TEST_F(RowRangesTest, SingleRG_ReturnsOriginalRowRanges2) {
  row_ranges.Add(IntervalRange(0, 10));
  std::vector<int64_t> rows_per_rg = {11};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 1);

  auto iter = result[0]->NewIterator();
  auto range = std::get<IntervalRange>(iter->NextRange());
  ASSERT_EQ(range.start, 0);
  ASSERT_EQ(range.end, 10);
  ASSERT_EQ(iter->NextRange().index(), 2);
}

TEST_F(RowRangesTest, ReturnsTwoRowRanges) {
  row_ranges.Add(IntervalRange(0, 10));
  std::vector<int64_t> rows_per_rg = {5, 6};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 2);
  {
    auto iter = result[0]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 4);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[1]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 5);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
}

TEST_F(RowRangesTest, ReturnsMultipleRowRanges) {
  row_ranges.Add(IntervalRange(0, 11));
  std::vector<int64_t> rows_per_rg = {3, 4, 100};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 3);
  {
    auto iter = result[0]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 2);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[1]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 3);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[2]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 4);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
}

TEST_F(RowRangesTest, MultipleInputRange) {
  row_ranges.Add(IntervalRange(0, 10));
  row_ranges.Add(IntervalRange(90, 111));
  row_ranges.Add(IntervalRange(191, 210));

  std::vector<int64_t> rows_per_rg = {100, 100};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 2);
  {
    auto iter = result[0]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 10);

    range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 90);
    ASSERT_EQ(range.end, 99);

    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[1]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 11);

    range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 91);
    ASSERT_EQ(range.end, 99);

    ASSERT_EQ(iter->NextRange().index(), 2);
  }
}

TEST_F(RowRangesTest, MultipleSplitPoints_ReturnWithEmptyRowRanges) {
  row_ranges.Add(IntervalRange(11, 18));
  std::vector<int64_t> rows_per_rg = {5, 5, 5, 5, 5};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 5);
  {
    auto iter = result[0]->NewIterator();
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[1]->NewIterator();
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[2]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 1);
    ASSERT_EQ(range.end, 4);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[3]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 3);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[4]->NewIterator();
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
}

TEST_F(RowRangesTest, RangeExceedRG) {
  row_ranges.Add(IntervalRange(0, 10));
  std::vector<int64_t> rows_per_rg = {5, 3};

  auto result = row_ranges.SplitByRowGroups(rows_per_rg);
  ASSERT_EQ(result.size(), 2);
  {
    auto iter = result[0]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 4);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
  {
    auto iter = result[1]->NewIterator();
    auto range = std::get<IntervalRange>(iter->NextRange());
    ASSERT_EQ(range.start, 0);
    ASSERT_EQ(range.end, 2);
    ASSERT_EQ(iter->NextRange().index(), 2);
  }
}
