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

#include "parquet/row_selection.h"

#include "arrow/util/bitmap_ops.h"
#include "arrow/util/unreachable.h"
#include "parquet/exception.h"

namespace parquet {

class IteratorImpl : public RowSelection::Iterator {
 public:
  explicit IteratorImpl(const RowSelection& ranges, size_t batch_size = 1)
      : ranges_(ranges.ranges_), index_(0), batch_size_(batch_size) {}

  ~IteratorImpl() override = default;

  ::arrow::util::span<const RowSelection::IntervalRange> NextRange() override {
    if (index_ >= ranges_.size()) {
      return {};
    }
    // Return up to batch_size_ ranges
    size_t remaining = ranges_.size() - index_;
    size_t count = std::min(batch_size_, remaining);
    auto result = ::arrow::util::span<const RowSelection::IntervalRange>(
        ranges_.data() + index_, count);
    index_ += count;
    return result;
  }

 private:
  const std::vector<RowSelection::IntervalRange>& ranges_;
  size_t index_;
  size_t batch_size_;
};

std::unique_ptr<RowSelection::Iterator> RowSelection::NewIterator() const {
  return std::make_unique<IteratorImpl>(*this);
}

void RowSelection::Validate() const {
  int64_t last_end = -1;
  for (const auto& interval : ranges_) {
    if (interval.start <= last_end) {
      throw ParquetException("Row ranges are not in ascending order");
    }
    if (interval.length <= 0) {
      throw ParquetException("Invalid interval range: length must be positive");
    }
    last_end = interval.start + interval.length - 1;
  }
}

int64_t RowSelection::row_count() const {
  int64_t count = 0;
  for (const auto& interval : ranges_) {
    count += interval.length;
  }
  return count;
}

RowSelection RowSelection::Intersect(const RowSelection& lhs, const RowSelection& rhs) {
  RowSelection result;
  
  // Use iterators to get batches
  auto lhs_iter = lhs.NewIterator();
  auto rhs_iter = rhs.NewIterator();
  
  auto lhs_batch = lhs_iter->NextRange();
  auto rhs_batch = rhs_iter->NextRange();
  size_t lhs_idx = 0;
  size_t rhs_idx = 0;

  while (!lhs_batch.empty() && !rhs_batch.empty()) {
    // Get current ranges from batches
    const auto& left = lhs_batch[lhs_idx];
    const auto& right = rhs_batch[rhs_idx];

    int64_t left_end = left.start + left.length - 1;
    int64_t right_end = right.start + right.length - 1;

    // Find overlapping region
    int64_t start = std::max(left.start, right.start);
    int64_t end = std::min(left_end, right_end);

    // If there is an overlap, add it to results
    if (start <= end) {
      result.ranges_.push_back(IntervalRange{start, end - start + 1});
    }

    // Advance the index with smaller end
    if (left_end < right_end) {
      lhs_idx++;
      if (lhs_idx >= lhs_batch.size()) {
        lhs_batch = lhs_iter->NextRange();
        lhs_idx = 0;
      }
    } else {
      rhs_idx++;
      if (rhs_idx >= rhs_batch.size()) {
        rhs_batch = rhs_iter->NextRange();
        rhs_idx = 0;
      }
    }
  }
  result.Validate();
  return result;
}

RowSelection RowSelection::Union(const RowSelection& lhs, const RowSelection& rhs) {
  RowSelection result;
  
  if (lhs.ranges_.empty()) {
    return rhs;
  }
  if (rhs.ranges_.empty()) {
    return lhs;
  }

  // Use iterators to get batches
  auto lhs_iter = lhs.NewIterator();
  auto rhs_iter = rhs.NewIterator();
  
  auto lhs_batch = lhs_iter->NextRange();
  auto rhs_batch = rhs_iter->NextRange();
  size_t lhs_idx = 0;
  size_t rhs_idx = 0;
  
  // Start with whichever range has the smaller start
  IntervalRange current;
  if (lhs_batch[0].start <= rhs_batch[0].start) {
    current = lhs_batch[lhs_idx++];
    if (lhs_idx >= lhs_batch.size()) {
      lhs_batch = lhs_iter->NextRange();
      lhs_idx = 0;
    }
  } else {
    current = rhs_batch[rhs_idx++];
    if (rhs_idx >= rhs_batch.size()) {
      rhs_batch = rhs_iter->NextRange();
      rhs_idx = 0;
    }
  }

  while (!lhs_batch.empty() || !rhs_batch.empty()) {
    IntervalRange next;

    if (rhs_batch.empty()) {
      // Only lhs ranges remain
      next = lhs_batch[lhs_idx++];
      if (lhs_idx >= lhs_batch.size()) {
        lhs_batch = lhs_iter->NextRange();
        lhs_idx = 0;
      }
    } else if (lhs_batch.empty()) {
      // Only rhs ranges remain
      next = rhs_batch[rhs_idx++];
      if (rhs_idx >= rhs_batch.size()) {
        rhs_batch = rhs_iter->NextRange();
        rhs_idx = 0;
      }
    } else {
      // Both have ranges - pick the one with smaller start
      const auto& left = lhs_batch[lhs_idx];
      const auto& right = rhs_batch[rhs_idx];

      if (left.start <= right.start) {
        next = left;
        lhs_idx++;
        if (lhs_idx >= lhs_batch.size()) {
          lhs_batch = lhs_iter->NextRange();
          lhs_idx = 0;
        }
      } else {
        next = right;
        rhs_idx++;
        if (rhs_idx >= rhs_batch.size()) {
          rhs_batch = rhs_iter->NextRange();
          rhs_idx = 0;
        }
      }
    }

    int64_t current_end = current.start + current.length - 1;
    if (current_end + 1 >= next.start) {
      // Concatenate overlapping or adjacent ranges
      int64_t next_end = next.start + next.length - 1;
      int64_t new_end = std::max(current_end, next_end);
      current.length = new_end - current.start + 1;
    } else {
      // Gap between current and next range
      result.ranges_.push_back(current);
      current = next;
    }
  }

  result.ranges_.push_back(current);
  result.Validate();
  return result;
}

RowSelection RowSelection::MakeSingle(int64_t start, int64_t end) {
  RowSelection rowSelection;
  rowSelection.ranges_.push_back(IntervalRange{start, end - start + 1});
  rowSelection.Validate();
  return rowSelection;
}

RowSelection RowSelection::FromIntervals(::arrow::util::span<const IntervalRange> intervals) {
  RowSelection rowSelection;
  rowSelection.ranges_.reserve(intervals.size());
  rowSelection.ranges_.insert(rowSelection.ranges_.end(), intervals.begin(), intervals.end());
  rowSelection.Validate();
  return rowSelection;
}

RowSelection RowSelection::FromIntervals(const std::vector<IntervalRange>& intervals) {
  return FromIntervals(::arrow::util::span<const IntervalRange>(intervals));
}

}  // namespace parquet