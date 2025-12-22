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
  explicit IteratorImpl(const RowSelection& ranges)
      : iter_(ranges.ranges_.cbegin()), end_(ranges.ranges_.cend()) {}

  ~IteratorImpl() override = default;

  std::optional<RowSelection::IntervalRange> NextRange() override {
    if (iter_ == end_) {
      return std::nullopt;
    }
    return *iter_++;
  }

 private:
  decltype(RowSelection::ranges_.cbegin()) iter_;
  decltype(RowSelection::ranges_.cend()) end_;
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
  size_t lhs_idx = 0;
  size_t rhs_idx = 0;

  while (lhs_idx < lhs.ranges_.size() && rhs_idx < rhs.ranges_.size()) {
    const auto& left = lhs.ranges_[lhs_idx];
    const auto& right = rhs.ranges_[rhs_idx];

    int64_t left_end = left.start + left.length - 1;
    int64_t right_end = right.start + right.length - 1;

    // Find overlapping region
    int64_t start = std::max(left.start, right.start);
    int64_t end = std::min(left_end, right_end);

    // If there is an overlap, add it to results
    if (start <= end) {
      result.ranges_.push_back(IntervalRange{start, end - start + 1});
    }

    // Advance the iterator with smaller end
    if (left_end < right_end) {
      lhs_idx++;
    } else {
      rhs_idx++;
    }
  }

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

  size_t lhs_idx = 0;
  size_t rhs_idx = 0;
  
  // Start with whichever range has the smaller start
  IntervalRange current;
  if (lhs.ranges_[0].start <= rhs.ranges_[0].start) {
    current = lhs.ranges_[lhs_idx++];
  } else {
    current = rhs.ranges_[rhs_idx++];
  }

  while (lhs_idx < lhs.ranges_.size() || rhs_idx < rhs.ranges_.size()) {
    IntervalRange next;

    if (rhs_idx >= rhs.ranges_.size()) {
      // Only lhs ranges remain
      next = lhs.ranges_[lhs_idx++];
    } else if (lhs_idx >= lhs.ranges_.size()) {
      // Only rhs ranges remain
      next = rhs.ranges_[rhs_idx++];
    } else {
      // Both have ranges - pick the one with smaller start
      const auto& left = lhs.ranges_[lhs_idx];
      const auto& right = rhs.ranges_[rhs_idx];

      if (left.start <= right.start) {
        next = left;
        lhs_idx++;
      } else {
        next = right;
        rhs_idx++;
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
  return result;
}

RowSelection RowSelection::MakeSingle(int64_t row_count) {
  RowSelection rowSelection;
  rowSelection.ranges_.push_back(IntervalRange{0, row_count});
  return rowSelection;
}

RowSelection RowSelection::MakeSingle(int64_t start, int64_t end) {
  RowSelection rowSelection;
  rowSelection.ranges_.push_back(IntervalRange{start, end - start + 1});
  return rowSelection;
}

RowSelection RowSelection::MakeIntervals(const std::vector<IntervalRange>& intervals) {
  RowSelection rowSelection;
  rowSelection.ranges_.reserve(intervals.size());
  rowSelection.ranges_.insert(rowSelection.ranges_.end(), intervals.cbegin(), intervals.cend());
  return rowSelection;
}

}  // namespace parquet