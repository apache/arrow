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

#include <vector>

#include "parquet/exception.h"
#include "parquet/row_selection.h"

namespace parquet {

// Test factory methods
TEST(RowSelection, MakeSingleWithCount) {
  auto ranges = RowSelection::MakeSingle(100);
  ASSERT_EQ(ranges.row_count(), 100);
  
  auto iter = ranges.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 99);
  
  // Should be exhausted
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, MakeSingleWithStartEnd) {
  auto ranges = RowSelection::MakeSingle(10, 20);
  ASSERT_EQ(ranges.row_count(), 11);
  
  auto iter = ranges.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ((interval.start + interval.length - 1), 20);
  
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, FromIntervals) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 11},
    {20, 11},
    {40, 11}
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 33);  // 11 + 11 + 11
  
  auto iter = ranges.NewIterator();
  
  // First interval
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 10);
  
  // Second interval
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ((interval.start + interval.length - 1), 30);
  
  // Third interval
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ((interval.start + interval.length - 1), 50);
  
  // Exhausted
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, EmptyRanges) {
  std::vector<RowSelection::IntervalRange> intervals;
  auto ranges = RowSelection::FromIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 0);
  
  auto iter = ranges.NewIterator();
  auto batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

// Test validation
TEST(RowSelection, ValidateValidRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 11},
    {15, 6},
    {25, 6}
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateSingleRange) {
  auto ranges = RowSelection::MakeSingle(100);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateOverlappingRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 11},
    {5, 11}  // Overlaps with previous
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateAdjacentRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 11},
    {11, 10}  // Adjacent but not overlapping
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateInvalidRangeTouching) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 11},
    {10, 11}  // Touches at end/start (overlaps at 10)
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateNotAscendingOrder) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {20, 11},
    {0, 11}  // Not in ascending order
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateInvalidInterval) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {10, -4}  // end < start
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

// Test row_count
TEST(RowSelection, RowCountSingle) {
  auto ranges = RowSelection::MakeSingle(50);
  EXPECT_EQ(ranges.row_count(), 50);
}

TEST(RowSelection, RowCountMultiple) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},    // 10 rows
    {20, 10},  // 10 rows
    {50, 5}   // 5 rows
  };
  
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 25);
}

TEST(RowSelection, RowCountEmpty) {
  std::vector<RowSelection::IntervalRange> intervals;
  auto ranges = RowSelection::FromIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 0);
}

TEST(RowSelection, RowCountSingleRow) {
  auto ranges = RowSelection::MakeSingle(5, 5);
  EXPECT_EQ(ranges.row_count(), 1);
}

// Test Intersect
TEST(RowSelection, IntersectNoOverlap) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  auto rhs = RowSelection::FromIntervals({{40, 11}, {60, 11}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowSelection, IntersectCompleteOverlap) {
  auto lhs = RowSelection::FromIntervals({{0, 101}});
  auto rhs = RowSelection::FromIntervals({{20, 11}, {40, 11}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);  // (30-20+1) + (50-40+1)
  
  auto iter = result.NewIterator();
  
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ((interval.start + interval.length - 1), 30);
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ((interval.start + interval.length - 1), 50);
}

TEST(RowSelection, IntersectPartialOverlap) {
  auto lhs = RowSelection::FromIntervals({{0, 16}, {20, 16}});
  auto rhs = RowSelection::FromIntervals({{10, 16}, {40, 11}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 12);  // (15-10+1) + (25-20+1)
  
  auto iter = result.NewIterator();
  
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ((interval.start + interval.length - 1), 15);
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ((interval.start + interval.length - 1), 25);
}

TEST(RowSelection, IntersectIdentical) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  auto rhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
}

TEST(RowSelection, IntersectWithEmpty) {
  auto lhs = RowSelection::FromIntervals({{0, 11}});
  auto rhs = RowSelection::FromIntervals(std::vector<RowSelection::IntervalRange>{});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowSelection, IntersectComplex) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {15, 11}, {30, 11}, {50, 11}});
  auto rhs = RowSelection::FromIntervals({{5, 8}, {20, 16}, {55, 16}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  
  // Expected intersections:
  // [5, 10] from [0,10] ∩ [5,12] = 6 rows
  // [20, 25] from [15,25] ∩ [20,35] = 6 rows
  // [30, 35] from [30,40] ∩ [20,35] = 6 rows
  // [55, 60] from [50,60] ∩ [55,70] = 6 rows
  EXPECT_EQ(result.row_count(), 24);
}

// Test Union
TEST(RowSelection, UnionNoOverlap) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  auto rhs = RowSelection::FromIntervals({{40, 11}, {60, 11}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 44);  // 11+11+11+11
  
  auto iter = result.NewIterator();
  
  // Should have 4 separate ranges
  for (int i = 0; i < 4; ++i) {
    auto batch = iter->NextRanges();
    EXPECT_TRUE(batch.size() > 0);
  }
  
  auto batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, UnionWithOverlap) {
  auto lhs = RowSelection::FromIntervals({{0, 16}});
  auto rhs = RowSelection::FromIntervals({{10, 16}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 26);  // [0, 25] = 26 rows
  
  auto iter = result.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 25);
}

TEST(RowSelection, UnionAdjacent) {
  auto lhs = RowSelection::FromIntervals({{0, 11}});
  auto rhs = RowSelection::FromIntervals({{11, 10}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 21);  // [0, 20] = 21 rows
  
  // Should merge adjacent ranges
  auto iter = result.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 20);
  
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, UnionWithGap) {
  auto lhs = RowSelection::FromIntervals({{0, 11}});
  auto rhs = RowSelection::FromIntervals({{20, 11}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should have 2 ranges
  auto iter = result.NewIterator();
  
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 10);
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ((interval.start + interval.length - 1), 30);
}

TEST(RowSelection, UnionWithEmpty) {
  auto lhs = RowSelection::FromIntervals({{0, 11}});
  auto rhs = RowSelection::FromIntervals(std::vector<RowSelection::IntervalRange>{});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowSelection, UnionEmptyWithNonEmpty) {
  auto lhs = RowSelection::FromIntervals(std::vector<RowSelection::IntervalRange>{});
  auto rhs = RowSelection::FromIntervals({{0, 11}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowSelection, UnionIdentical) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  auto rhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should still have 2 ranges (merged)
  auto iter = result.NewIterator();
  
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

TEST(RowSelection, UnionComplex) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}, {50, 11}});
  auto rhs = RowSelection::FromIntervals({{5, 11}, {25, 11}, {45, 11}});
  
  auto result = RowSelection::Union(lhs, rhs);
  
  // Expected: [0,15], [20,35], [45,60]
  EXPECT_EQ(result.row_count(), 48);  // 16 + 16 + 16
  
  auto iter = result.NewIterator();
  
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 15);
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ((interval.start + interval.length - 1), 35);
  
  batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  interval = batch[0];
  EXPECT_EQ(interval.start, 45);
  EXPECT_EQ((interval.start + interval.length - 1), 60);
}

TEST(RowSelection, UnionManyOverlapping) {
  auto lhs = RowSelection::FromIntervals({{0, 101}});
  auto rhs = RowSelection::FromIntervals({{50, 101}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 151);  // [0, 150]
  
  auto iter = result.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 150);
}

// Test iterator behavior
TEST(RowSelection, IteratorMultipleIterators) {
  auto ranges = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  
  auto iter1 = ranges.NewIterator();
  auto iter2 = ranges.NewIterator();
  
  // Both iterators should work independently
  auto batch1 = iter1->NextRanges();
  auto batch2 = iter2->NextRanges();
  
  ASSERT_FALSE(batch1.empty());
  ASSERT_FALSE(batch2.empty());
  
  auto interval1 = batch1[0];
  auto interval2 = batch2[0];
  
  EXPECT_EQ(interval1.start, interval2.start);
  EXPECT_EQ((interval1.start + interval1.length - 1), (interval2.start + interval2.length - 1));
}

TEST(RowSelection, IteratorExhaustion) {
  auto ranges = RowSelection::MakeSingle(10);
  auto iter = ranges.NewIterator();
  
  // First call returns the range
  auto batch = iter->NextRanges();
  EXPECT_TRUE(batch.size() > 0);
  
  // Subsequent calls should return End
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
  
  batch = iter->NextRanges();
  EXPECT_TRUE(batch.empty());
}

// Test edge cases
TEST(RowSelection, LargeRanges) {
  auto ranges = RowSelection::MakeSingle(0, 1000000);
  EXPECT_EQ(ranges.row_count(), 1000001);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ZeroStartRange) {
  auto ranges = RowSelection::MakeSingle(0, 0);
  EXPECT_EQ(ranges.row_count(), 1);
  
  auto iter = ranges.NewIterator();
  auto batch = iter->NextRanges();
  ASSERT_FALSE(batch.empty());
  auto interval = batch[0];
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ((interval.start + interval.length - 1), 0);
}

TEST(RowSelection, IntersectAndUnionCommutative) {
  auto lhs = RowSelection::FromIntervals({{0, 11}, {20, 11}});
  auto rhs = RowSelection::FromIntervals({{5, 11}, {25, 11}});
  
  // Intersect should be commutative
  auto intersect1 = RowSelection::Intersect(lhs, rhs);
  auto intersect2 = RowSelection::Intersect(rhs, lhs);
  EXPECT_EQ(intersect1.row_count(), intersect2.row_count());
  
  // Union should be commutative
  auto union1 = RowSelection::Union(lhs, rhs);
  auto union2 = RowSelection::Union(rhs, lhs);
  EXPECT_EQ(union1.row_count(), union2.row_count());
}

}  // namespace parquet
