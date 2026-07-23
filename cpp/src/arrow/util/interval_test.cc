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

#include <vector>

#include <gtest/gtest.h>

#include "arrow/testing/gtest_util.h"
#include "arrow/util/interval.h"
#include "arrow/util/range.h"

namespace arrow::util {

namespace {

std::vector<Interval> IntervalsFromIntVector(const std::vector<int64_t>& raw_intervals) {
  assert(raw_intervals.size() % 2 == 0);
  std::vector<Interval> result;
  for (int64_t i = 0; i < static_cast<int64_t>(raw_intervals.size()); i += 2) {
    result.push_back({raw_intervals[i], raw_intervals[i + 1], -1});
  }
  return result;
}

void AssertIntervalsEqual(const IntervalMerger& interval_merger,
                          const std::vector<int64_t>& raw_expected_intervals) {
  auto merged_intervals = IntervalsFromIntVector(raw_expected_intervals);
  ASSERT_EQ(merged_intervals.size(), interval_merger.size());
  for (auto [expected_interval, actual_interval] :
       ::arrow::internal::Zip(merged_intervals, interval_merger)) {
    ASSERT_EQ(expected_interval, actual_interval);
  }
}

}  // namespace

class TestIntervalMerger : public ::testing::Test {};

class TestIntervalMergerFastPath : public TestIntervalMerger {};

TEST(TestIntervalMergerFastPath, GreaterOrEqualWithLastInsertedIteratorAndJoinable) {
  IntervalMerger interval_merger;
  // Init
  interval_merger.AddInterval(100, 200);

  // For all assertions below, interval.start >= last_inserted_iterator->start.

  // Overlap: interval->end < last_inserted_iterator->end.
  interval_merger.AddInterval(150, 170);
  AssertIntervalsEqual(interval_merger, {100, 200});

  // Overlap: interval->end > last_inserted_iterator->end and
  // std::next(last_inserted_iterator_) is end.
  interval_merger.AddInterval(160, 250);
  AssertIntervalsEqual(interval_merger, {100, 250});

  // No overlap; it is inserted solely to provide a next iterator for [100, 250].
  interval_merger.AddInterval(300, 400);
  AssertIntervalsEqual(interval_merger, {100, 250, 300, 400});

  // Just referring last_inserted_iterator back to the interval [100, 250].
  interval_merger.AddInterval(100, 250);

  // Overlap: interval.end > last_inserted_iterator->end
  // and  interval.end < std::next(last_inserted_iterator_end)->start
  interval_merger.AddInterval(180, 260);
  AssertIntervalsEqual(interval_merger, {100, 260, 300, 400});

  // Overlap: interval.end > last_inserted_iterator->end
  // and  interval.end > std::next(last_inserted_iterator_end)->start
  // and interval.end < std::next(last_inserted_iterator_end)->end
  interval_merger.AddInterval(200, 320);
  AssertIntervalsEqual(interval_merger, {100, 400});

  // No overlap; it is inserted solely to provide a next iterator for [100, 400].
  interval_merger.AddInterval(500, 700);
  AssertIntervalsEqual(interval_merger, {100, 400, 500, 700});

  // Just referring last_inserted_iterator back to the interval [100, 400].
  interval_merger.AddInterval(200, 320);
  // Overlap: interval.end > last_inserted_iterator->end
  // and  interval.end > std::next(last_inserted_iterator_end)->start
  // and interval.end > std::next(last_inserted_iterator_end)->end
  // which leads to taking the slow path.
  interval_merger.AddInterval(210, 800);
  AssertIntervalsEqual(interval_merger, {100, 800});
}

TEST(TestIntervalMergerFastPath, GreaterAndNotJoinableWithLastInsertedIterator) {
  IntervalMerger interval_merger;
  // Init
  interval_merger.AddInterval(300, 500);
  // No overlap, and std::next(last_inserted_iterator) is std::set::end().
  interval_merger.AddInterval(1000, 1200);
  AssertIntervalsEqual(interval_merger, {300, 500, 1000, 1200});

  // Just updating last_inserted_iterator to have a next element.
  interval_merger.AddInterval(700, 800);
  AssertIntervalsEqual(interval_merger, {300, 500, 700, 800, 1000, 1200});

  // Not joinable with last_inserted_iterator or its next.
  interval_merger.AddInterval(900, 950);
  AssertIntervalsEqual(interval_merger, {300, 500, 700, 800, 900, 950, 1000, 1200});

  // Joinable with std::next(last_inserted_iterator).
  // interval.end < std::next(last_inserted_iterator)->end.
  interval_merger.AddInterval(970, 1100);
  AssertIntervalsEqual(interval_merger, {300, 500, 700, 800, 900, 950, 970, 1200});

  // Just updating last_inserted_iterator to have a next element.
  interval_merger.AddInterval(100, 150);
  AssertIntervalsEqual(interval_merger,
                       {100, 150, 300, 500, 700, 800, 900, 950, 970, 1200});

  // Joinable with std::next(last_inserted_iterator).
  // interval.end > std::next(last_inserted_iterator)->end.
  // which leads to taking the slow path.
  interval_merger.AddInterval(170, 600);
  AssertIntervalsEqual(interval_merger,
                       {100, 150, 170, 600, 700, 800, 900, 950, 970, 1200});
}

TEST(TestIntervalMergerFastPath, lessThanAtBegin) {
  IntervalMerger interval_merger;
  // Init
  interval_merger.AddInterval(500, 600);
  // overlap and interval.end < last_inserted_iterator->end
  interval_merger.AddInterval(300, 550);
  AssertIntervalsEqual(interval_merger, {300, 600});

  // For all assertions below, interval.start < last_inserted_iterator->start.

  // Overlap: interval.end > last_inserted_iterator->end
  interval_merger.AddInterval(200, 700);
  AssertIntervalsEqual(interval_merger, {200, 700});

  // No overlap
  interval_merger.AddInterval(100, 150);
  AssertIntervalsEqual(interval_merger, {100, 150, 200, 700});
}

class TestIntervalMergerTryInsertOrMerge : public TestIntervalMerger {};

TEST_F(TestIntervalMergerTryInsertOrMerge, EqualStart) {
  IntervalMerger interval_merger;
  // First use fast path
  interval_merger.AddInterval(500, 600);
  interval_merger.AddInterval(700, 800);
  interval_merger.AddInterval(1100, 1200);
  interval_merger.AddInterval(1300, 1400);
  interval_merger.AddInterval(1500, 1600);
  interval_merger.AddInterval(1700, 1800);

  // Slow path:
  // inserted_interval = std::lower_bound(interval).
  // For all assertions below, interval.start == inserted_interval->start.

  // interval.end < inserted_interval.end
  interval_merger.AddInterval(1300, 1350);
  AssertIntervalsEqual(interval_merger, {500, 600, 700, 800, 1100, 1200, 1300, 1400, 1500,
                                         1600, 1700, 1800});
  // interval.end > inserted_interval.end
  interval_merger.AddInterval(700, 850);
  AssertIntervalsEqual(interval_merger, {500, 600, 700, 850, 1100, 1200, 1300, 1400, 1500,
                                         1600, 1700, 1800});

  // interval.end > inserted_interval.end and
  // interval.end > std::next(inserted_interval.end)
  // which leads to taking the slow path.
  interval_merger.AddInterval(1500, 1750);
  AssertIntervalsEqual(interval_merger,
                       {500, 600, 700, 850, 1100, 1200, 1300, 1400, 1500, 1800});
}

TEST_F(TestIntervalMergerTryInsertOrMerge, InsertOrMergeAtBegin) {
  IntervalMerger interval_merger;
  // First use fast path
  interval_merger.AddInterval(500, 600);
  interval_merger.AddInterval(700, 800);
  interval_merger.AddInterval(1100, 1200);

  // Overlap
  // interval.end < std::set::lower_bound(interval).end
  interval_merger.AddInterval(300, 550);
  AssertIntervalsEqual(interval_merger, {300, 600, 700, 800, 1100, 1200});

  // Update the position of the last_inserted_iterator.
  interval_merger.AddInterval(1100, 1200);
  // interval.end > std::set::lower_bound(interval).end
  interval_merger.AddInterval(200, 650);
  AssertIntervalsEqual(interval_merger, {200, 650, 700, 800, 1100, 1200});

  // Update the position of the last_inserted_iterator.
  interval_merger.AddInterval(1100, 1200);
  // No overlap
  interval_merger.AddInterval(100, 150);
  AssertIntervalsEqual(interval_merger, {100, 150, 200, 650, 700, 800, 1100, 1200});
}

TEST_F(TestIntervalMergerTryInsertOrMerge, InsertOrMergeAtMiddleOrEnd) {
  IntervalMerger interval_merger;
  // First use fast path
  interval_merger.AddInterval(500, 800);
  interval_merger.AddInterval(1200, 1500);
  interval_merger.AddInterval(1700, 2000);
  interval_merger.AddInterval(2200, 2500);
  interval_merger.AddInterval(2700, 3000);
  interval_merger.AddInterval(3200, 3500);

  // overlap with std::prev(std::lower_bound(interval))
  interval_merger.AddInterval(1800, 2100);
  AssertIntervalsEqual(interval_merger, {500, 800, 1200, 1500, 1700, 2100, 2200, 2500,
                                         2700, 3000, 3200, 3500});

  // No overlap with std::prev(std::lower_bound(interval))
  // and std::lower_bound(interval) is equal to std::set::end()
  interval_merger.AddInterval(3700, 3750);
  AssertIntervalsEqual(interval_merger, {500, 800, 1200, 1500, 1700, 2100, 2200, 2500,
                                         2700, 3000, 3200, 3500, 3700, 3750});

  // No overlap with std::prev(std::lower_bound(interval))
  // and overlap with std::lower_bound(interval)
  interval_merger.AddInterval(1600, 1750);
  AssertIntervalsEqual(interval_merger, {500, 800, 1200, 1500, 1600, 2100, 2200, 2500,
                                         2700, 3000, 3200, 3500, 3700, 3750});

  // no overlap with std::prev(std::lower_bound(interval)) and
  // std::lower_bound(interval)
  interval_merger.AddInterval(900, 1000);
  AssertIntervalsEqual(interval_merger, {500, 800, 900, 1000, 1200, 1500, 1600, 2100,
                                         2200, 2500, 2700, 3000, 3200, 3500, 3700, 3750});
}

class TestIntervalMergerJoinRight : public TestIntervalMerger {};

TEST_F(TestIntervalMergerJoinRight, MergeCornerCase) {
  IntervalMerger interval_merger;
  // First use fast path
  interval_merger.AddInterval(1400, 1600);
  interval_merger.AddInterval(900, 1000);
  interval_merger.AddInterval(500, 800);

  // next_it is std::sed::end()
  interval_merger.AddInterval(1200, 1500);
  AssertIntervalsEqual(interval_merger, {500, 800, 900, 1000, 1200, 1600});

  // No overlap between next_it and it
  interval_merger.AddInterval(700, 850);
  AssertIntervalsEqual(interval_merger, {500, 850, 900, 1000, 1200, 1600});

  // Check Corner case of next at end
  interval_merger.AddInterval(870, 1400);
  AssertIntervalsEqual(interval_merger, {500, 850, 870, 1600});

  // Check begin case
  interval_merger.AddInterval(450, 890);
  AssertIntervalsEqual(interval_merger, {450, 1600});
}

TEST_F(TestIntervalMergerJoinRight, MergeCommonCase) {
  IntervalMerger interval_merger;
  // First use fast path
  interval_merger.AddInterval(3000, 4000);
  interval_merger.AddInterval(4000, 5000);
  interval_merger.AddInterval(6000, 7000);

  // Regular JoinRight
  interval_merger.AddInterval(0, 10000);
  AssertIntervalsEqual(interval_merger, {0, 10000});
}

TEST_F(TestIntervalMerger, CompressIntervals) {
  IntervalMerger interval_merger;
  interval_merger.AddInterval(3000, 4000);
  interval_merger.AddInterval(5000, 8000);
  interval_merger.AddInterval(3200, 4500);
  interval_merger.AddInterval(10000, 12000);
  interval_merger.AddInterval(15000, 20000);
  AssertIntervalsEqual(interval_merger,
                       {3000, 4500, 5000, 8000, 10000, 12000, 15000, 20000});

  auto total_size = interval_merger.CompactIntervalAndGetTotalSize();
  ASSERT_EQ(total_size, 11500);

  std::vector<int32_t> offsets{0, 1500, 4500, 6500};

  for (const auto& [interval, offset] :
       ::arrow::internal::Zip(interval_merger, offsets)) {
    ASSERT_EQ(interval.start_offset_in_compacted, offset);
  }
}

TEST_F(TestIntervalMerger, GetCompactedPosition) {
  IntervalMerger interval_merger;
  interval_merger.AddInterval(3000, 4000);
  interval_merger.AddInterval(5000, 8000);
  interval_merger.AddInterval(3200, 4500);
  interval_merger.AddInterval(10000, 12000);
  interval_merger.AddInterval(15000, 20000);
  AssertIntervalsEqual(interval_merger,
                       {3000, 4500, 5000, 8000, 10000, 12000, 15000, 20000});
  interval_merger.CompactIntervalAndGetTotalSize();
  // Offsets are {0, 1500, 4500, 6500}

  // Get Compacted offset from last interval
  ASSERT_EQ(interval_merger.GetCompactedPosition(17000), 8500);

  // Get Compacted offset from the start of interval
  ASSERT_EQ(interval_merger.GetCompactedPosition(5000), 1500);

  // Get Compacted offset from middle of an interval
  ASSERT_EQ(interval_merger.GetCompactedPosition(11000), 5500);
}

}  // namespace arrow::util
