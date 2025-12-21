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
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 99);
  
  // Should be exhausted
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, MakeSingleWithStartEnd) {
  auto ranges = RowSelection::MakeSingle(10, 20);
  ASSERT_EQ(ranges.row_count(), 11);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ(interval.end, 20);
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, MakeIntervals) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},
    {20, 30},
    {40, 50}
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 33);  // 11 + 11 + 11
  
  auto iter = ranges.NewIterator();
  
  // First interval
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 10);
  
  // Second interval
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
  
  // Third interval
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ(interval.end, 50);
  
  // Exhausted
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, EmptyRanges) {
  std::vector<RowSelection::IntervalRange> intervals;
  auto ranges = RowSelection::MakeIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 0);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

// Test validation
TEST(RowSelection, ValidateValidRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},
    {15, 20},
    {25, 30}
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateSingleRange) {
  auto ranges = RowSelection::MakeSingle(100);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateOverlappingRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},
    {5, 15}  // Overlaps with previous
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateAdjacentRanges) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},
    {11, 20}  // Adjacent but not overlapping
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowSelection, ValidateInvalidRangeTouching) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 10},
    {10, 20}  // Touches at end/start (overlaps at 10)
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateNotAscendingOrder) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {20, 30},
    {0, 10}  // Not in ascending order
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowSelection, ValidateInvalidInterval) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {10, 5}  // end < start
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

// Test row_count
TEST(RowSelection, RowCountSingle) {
  auto ranges = RowSelection::MakeSingle(50);
  EXPECT_EQ(ranges.row_count(), 50);
}

TEST(RowSelection, RowCountMultiple) {
  std::vector<RowSelection::IntervalRange> intervals = {
    {0, 9},    // 10 rows
    {20, 29},  // 10 rows
    {50, 54}   // 5 rows
  };
  
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 25);
}

TEST(RowSelection, RowCountEmpty) {
  std::vector<RowSelection::IntervalRange> intervals;
  auto ranges = RowSelection::MakeIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 0);
}

TEST(RowSelection, RowCountSingleRow) {
  auto ranges = RowSelection::MakeSingle(5, 5);
  EXPECT_EQ(ranges.row_count(), 1);
}

// Test Intersect
TEST(RowSelection, IntersectNoOverlap) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowSelection::MakeIntervals({{40, 50}, {60, 70}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowSelection, IntersectCompleteOverlap) {
  auto lhs = RowSelection::MakeIntervals({{0, 100}});
  auto rhs = RowSelection::MakeIntervals({{20, 30}, {40, 50}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);  // (30-20+1) + (50-40+1)
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ(interval.end, 50);
}

TEST(RowSelection, IntersectPartialOverlap) {
  auto lhs = RowSelection::MakeIntervals({{0, 15}, {20, 35}});
  auto rhs = RowSelection::MakeIntervals({{10, 25}, {40, 50}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 12);  // (15-10+1) + (25-20+1)
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ(interval.end, 15);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 25);
}

TEST(RowSelection, IntersectIdentical) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
}

TEST(RowSelection, IntersectWithEmpty) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}});
  auto rhs = RowSelection::MakeIntervals({});
  
  auto result = RowSelection::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowSelection, IntersectComplex) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {15, 25}, {30, 40}, {50, 60}});
  auto rhs = RowSelection::MakeIntervals({{5, 12}, {20, 35}, {55, 70}});
  
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
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowSelection::MakeIntervals({{40, 50}, {60, 70}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 44);  // 11+11+11+11
  
  auto iter = result.NewIterator();
  
  // Should have 4 separate ranges
  for (int i = 0; i < 4; ++i) {
    auto range = iter->NextRange();
    EXPECT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  }
  
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, UnionWithOverlap) {
  auto lhs = RowSelection::MakeIntervals({{0, 15}});
  auto rhs = RowSelection::MakeIntervals({{10, 25}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 26);  // [0, 25] = 26 rows
  
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 25);
}

TEST(RowSelection, UnionAdjacent) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}});
  auto rhs = RowSelection::MakeIntervals({{11, 20}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 21);  // [0, 20] = 21 rows
  
  // Should merge adjacent ranges
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 20);
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, UnionWithGap) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}});
  auto rhs = RowSelection::MakeIntervals({{20, 30}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should have 2 ranges
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 10);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
}

TEST(RowSelection, UnionWithEmpty) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}});
  auto rhs = RowSelection::MakeIntervals({});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowSelection, UnionEmptyWithNonEmpty) {
  auto lhs = RowSelection::MakeIntervals({});
  auto rhs = RowSelection::MakeIntervals({{0, 10}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowSelection, UnionIdentical) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should still have 2 ranges (merged)
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
}

TEST(RowSelection, UnionComplex) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}, {50, 60}});
  auto rhs = RowSelection::MakeIntervals({{5, 15}, {25, 35}, {45, 55}});
  
  auto result = RowSelection::Union(lhs, rhs);
  
  // Expected: [0,15], [20,35], [45,60]
  EXPECT_EQ(result.row_count(), 48);  // 16 + 16 + 16
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 15);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 35);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 45);
  EXPECT_EQ(interval.end, 60);
}

TEST(RowSelection, UnionManyOverlapping) {
  auto lhs = RowSelection::MakeIntervals({{0, 100}});
  auto rhs = RowSelection::MakeIntervals({{50, 150}});
  
  auto result = RowSelection::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 151);  // [0, 150]
  
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 150);
}

// Test iterator behavior
TEST(RowSelection, IteratorMultipleIterators) {
  auto ranges = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  
  auto iter1 = ranges.NewIterator();
  auto iter2 = ranges.NewIterator();
  
  // Both iterators should work independently
  auto range1 = iter1->NextRange();
  auto range2 = iter2->NextRange();
  
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range1));
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range2));
  
  auto interval1 = std::get<RowSelection::IntervalRange>(range1);
  auto interval2 = std::get<RowSelection::IntervalRange>(range2);
  
  EXPECT_EQ(interval1.start, interval2.start);
  EXPECT_EQ(interval1.end, interval2.end);
}

TEST(RowSelection, IteratorExhaustion) {
  auto ranges = RowSelection::MakeSingle(10);
  auto iter = ranges.NewIterator();
  
  // First call returns the range
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  
  // Subsequent calls should return End
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowSelection::End>(range));
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
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowSelection::IntervalRange>(range));
  auto interval = std::get<RowSelection::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 0);
}

TEST(RowSelection, IntersectAndUnionCommutative) {
  auto lhs = RowSelection::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowSelection::MakeIntervals({{5, 15}, {25, 35}});
  
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
