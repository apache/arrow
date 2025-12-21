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
#include "parquet/row_range.h"

namespace parquet {

// Test factory methods
TEST(RowRanges, MakeSingleWithCount) {
  auto ranges = RowRanges::MakeSingle(100);
  ASSERT_EQ(ranges.row_count(), 100);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 99);
  
  // Should be exhausted
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, MakeSingleWithStartEnd) {
  auto ranges = RowRanges::MakeSingle(10, 20);
  ASSERT_EQ(ranges.row_count(), 11);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ(interval.end, 20);
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, MakeIntervals) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 10},
    {20, 30},
    {40, 50}
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 33);  // 11 + 11 + 11
  
  auto iter = ranges.NewIterator();
  
  // First interval
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 10);
  
  // Second interval
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
  
  // Third interval
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ(interval.end, 50);
  
  // Exhausted
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, EmptyRanges) {
  std::vector<RowRanges::IntervalRange> intervals;
  auto ranges = RowRanges::MakeIntervals(intervals);
  ASSERT_EQ(ranges.row_count(), 0);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

// Test validation
TEST(RowRanges, ValidateValidRanges) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 10},
    {15, 20},
    {25, 30}
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowRanges, ValidateSingleRange) {
  auto ranges = RowRanges::MakeSingle(100);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowRanges, ValidateOverlappingRanges) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 10},
    {5, 15}  // Overlaps with previous
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowRanges, ValidateAdjacentRanges) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 10},
    {11, 20}  // Adjacent but not overlapping
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowRanges, ValidateInvalidRangeTouching) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 10},
    {10, 20}  // Touches at end/start (overlaps at 10)
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowRanges, ValidateNotAscendingOrder) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {20, 30},
    {0, 10}  // Not in ascending order
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

TEST(RowRanges, ValidateInvalidInterval) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {10, 5}  // end < start
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_THROW(ranges.Validate(), ParquetException);
}

// Test row_count
TEST(RowRanges, RowCountSingle) {
  auto ranges = RowRanges::MakeSingle(50);
  EXPECT_EQ(ranges.row_count(), 50);
}

TEST(RowRanges, RowCountMultiple) {
  std::vector<RowRanges::IntervalRange> intervals = {
    {0, 9},    // 10 rows
    {20, 29},  // 10 rows
    {50, 54}   // 5 rows
  };
  
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 25);
}

TEST(RowRanges, RowCountEmpty) {
  std::vector<RowRanges::IntervalRange> intervals;
  auto ranges = RowRanges::MakeIntervals(intervals);
  EXPECT_EQ(ranges.row_count(), 0);
}

TEST(RowRanges, RowCountSingleRow) {
  auto ranges = RowRanges::MakeSingle(5, 5);
  EXPECT_EQ(ranges.row_count(), 1);
}

// Test Intersect
TEST(RowRanges, IntersectNoOverlap) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowRanges::MakeIntervals({{40, 50}, {60, 70}});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowRanges, IntersectCompleteOverlap) {
  auto lhs = RowRanges::MakeIntervals({{0, 100}});
  auto rhs = RowRanges::MakeIntervals({{20, 30}, {40, 50}});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);  // (30-20+1) + (50-40+1)
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 40);
  EXPECT_EQ(interval.end, 50);
}

TEST(RowRanges, IntersectPartialOverlap) {
  auto lhs = RowRanges::MakeIntervals({{0, 15}, {20, 35}});
  auto rhs = RowRanges::MakeIntervals({{10, 25}, {40, 50}});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 12);  // (15-10+1) + (25-20+1)
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 10);
  EXPECT_EQ(interval.end, 15);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 25);
}

TEST(RowRanges, IntersectIdentical) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
}

TEST(RowRanges, IntersectWithEmpty) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}});
  auto rhs = RowRanges::MakeIntervals({});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  EXPECT_EQ(result.row_count(), 0);
}

TEST(RowRanges, IntersectComplex) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {15, 25}, {30, 40}, {50, 60}});
  auto rhs = RowRanges::MakeIntervals({{5, 12}, {20, 35}, {55, 70}});
  
  auto result = RowRanges::Intersect(lhs, rhs);
  
  // Expected intersections:
  // [5, 10] from [0,10] ∩ [5,12] = 6 rows
  // [20, 25] from [15,25] ∩ [20,35] = 6 rows
  // [30, 35] from [30,40] ∩ [20,35] = 6 rows
  // [55, 60] from [50,60] ∩ [55,70] = 6 rows
  EXPECT_EQ(result.row_count(), 24);
}

// Test Union
TEST(RowRanges, UnionNoOverlap) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowRanges::MakeIntervals({{40, 50}, {60, 70}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 44);  // 11+11+11+11
  
  auto iter = result.NewIterator();
  
  // Should have 4 separate ranges
  for (int i = 0; i < 4; ++i) {
    auto range = iter->NextRange();
    EXPECT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  }
  
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, UnionWithOverlap) {
  auto lhs = RowRanges::MakeIntervals({{0, 15}});
  auto rhs = RowRanges::MakeIntervals({{10, 25}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 26);  // [0, 25] = 26 rows
  
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 25);
}

TEST(RowRanges, UnionAdjacent) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}});
  auto rhs = RowRanges::MakeIntervals({{11, 20}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 21);  // [0, 20] = 21 rows
  
  // Should merge adjacent ranges
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 20);
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, UnionWithGap) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}});
  auto rhs = RowRanges::MakeIntervals({{20, 30}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should have 2 ranges
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 10);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 30);
}

TEST(RowRanges, UnionWithEmpty) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}});
  auto rhs = RowRanges::MakeIntervals({});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowRanges, UnionEmptyWithNonEmpty) {
  auto lhs = RowRanges::MakeIntervals({});
  auto rhs = RowRanges::MakeIntervals({{0, 10}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 11);
}

TEST(RowRanges, UnionIdentical) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 22);
  
  // Should still have 2 ranges (merged)
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

TEST(RowRanges, UnionComplex) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}, {50, 60}});
  auto rhs = RowRanges::MakeIntervals({{5, 15}, {25, 35}, {45, 55}});
  
  auto result = RowRanges::Union(lhs, rhs);
  
  // Expected: [0,15], [20,35], [45,60]
  EXPECT_EQ(result.row_count(), 48);  // 16 + 16 + 16
  
  auto iter = result.NewIterator();
  
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 15);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 20);
  EXPECT_EQ(interval.end, 35);
  
  range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 45);
  EXPECT_EQ(interval.end, 60);
}

TEST(RowRanges, UnionManyOverlapping) {
  auto lhs = RowRanges::MakeIntervals({{0, 100}});
  auto rhs = RowRanges::MakeIntervals({{50, 150}});
  
  auto result = RowRanges::Union(lhs, rhs);
  EXPECT_EQ(result.row_count(), 151);  // [0, 150]
  
  auto iter = result.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 150);
}

// Test iterator behavior
TEST(RowRanges, IteratorMultipleIterators) {
  auto ranges = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  
  auto iter1 = ranges.NewIterator();
  auto iter2 = ranges.NewIterator();
  
  // Both iterators should work independently
  auto range1 = iter1->NextRange();
  auto range2 = iter2->NextRange();
  
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range1));
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range2));
  
  auto interval1 = std::get<RowRanges::IntervalRange>(range1);
  auto interval2 = std::get<RowRanges::IntervalRange>(range2);
  
  EXPECT_EQ(interval1.start, interval2.start);
  EXPECT_EQ(interval1.end, interval2.end);
}

TEST(RowRanges, IteratorExhaustion) {
  auto ranges = RowRanges::MakeSingle(10);
  auto iter = ranges.NewIterator();
  
  // First call returns the range
  auto range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  
  // Subsequent calls should return End
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
  
  range = iter->NextRange();
  EXPECT_TRUE(std::holds_alternative<RowRanges::End>(range));
}

// Test edge cases
TEST(RowRanges, LargeRanges) {
  auto ranges = RowRanges::MakeSingle(0, 1000000);
  EXPECT_EQ(ranges.row_count(), 1000001);
  EXPECT_NO_THROW(ranges.Validate());
}

TEST(RowRanges, ZeroStartRange) {
  auto ranges = RowRanges::MakeSingle(0, 0);
  EXPECT_EQ(ranges.row_count(), 1);
  
  auto iter = ranges.NewIterator();
  auto range = iter->NextRange();
  ASSERT_TRUE(std::holds_alternative<RowRanges::IntervalRange>(range));
  auto interval = std::get<RowRanges::IntervalRange>(range);
  EXPECT_EQ(interval.start, 0);
  EXPECT_EQ(interval.end, 0);
}

TEST(RowRanges, IntersectAndUnionCommutative) {
  auto lhs = RowRanges::MakeIntervals({{0, 10}, {20, 30}});
  auto rhs = RowRanges::MakeIntervals({{5, 15}, {25, 35}});
  
  // Intersect should be commutative
  auto intersect1 = RowRanges::Intersect(lhs, rhs);
  auto intersect2 = RowRanges::Intersect(rhs, lhs);
  EXPECT_EQ(intersect1.row_count(), intersect2.row_count());
  
  // Union should be commutative
  auto union1 = RowRanges::Union(lhs, rhs);
  auto union2 = RowRanges::Union(rhs, lhs);
  EXPECT_EQ(union1.row_count(), union2.row_count());
}

}  // namespace parquet
