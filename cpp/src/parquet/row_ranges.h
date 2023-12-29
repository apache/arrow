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

#pragma once

#include <cstdint>
#include <vector>

#include "parquet/exception.h"

namespace parquet {

class RowRanges {
 public:
  /// A range of [from, to]
  struct Range {
    int64_t from;
    int64_t to;

    static Range Union(const Range& left, const Range& right) {
      if (left.from <= right.from) {
        if (left.to + 1 >= right.from) {
          return Range{left.from, std::max(left.to, right.to)};
        }
      } else if (right.to + 1 >= left.from) {
        return Range{right.from, std::max(left.to, right.to)};
      }
      return EMPTY_RANGE;
    }

    static Range Intersection(const Range& left, const Range& right) {
      if (left.from <= right.from) {
        if (left.to >= right.from) {
          return Range{right.from, std::min(left.to, right.to)};
        }
      } else if (right.to >= left.from) {
        return Range{left.from, std::min(left.to, right.to)};
      }
      return EMPTY_RANGE;
    }

    bool IsBefore(Range other) const { return to < other.from; }

    bool IsAfter(Range other) const { return from > other.to; }

    int64_t RowCount() const { return to - from + 1; }

    bool operator==(const Range& other) const {
      if (from == other.from && to == other.to) {
        return true;
      } else {
        return false;
      }
    }

    bool operator!=(const Range& other) const { return !(*this == other); }

    std::string ToString() const {
      std::stringstream ss;
      ss << "[" << from << "," << to << "]";
      return ss.str();
    }
  };

  RowRanges() = default;

  explicit RowRanges(const std::vector<Range>& ranges) {
    if (ranges.empty()) {
      throw ParquetException("Ranges can't be empty");
    }

    for (const auto& range : ranges) {
      Add(range);
    }
  }

  /*
   * Adds a range to the end of the list of ranges. It maintains the disjunct ascending
   * order(*) of the ranges by trying to union the specified range to the last ranges in
   * the list. The specified range shall be larger(*) than the last one or might be
   * overlapped with some of the last ones.
   * (*) [a, b] < [c, d] if b < c
   */
  void Add(const Range& range) {
    CheckRangeIsValid(range);
    auto& range_to_add = const_cast<Range&>(range);
    for (int i = static_cast<int>(ranges_.size() - 1); i >= 0; --i) {
      Range& last = ranges_[i];
      if (last.IsAfter(range)) {
        throw ParquetException("Ranges should be in ascending order");
      }
      const Range& u = Range::Union(last, range_to_add);
      if (u == EMPTY_RANGE) {
        break;
      }
      range_to_add = u;
      ranges_.pop_back();
    }
    ranges_.push_back(range_to_add);
  }

  const std::vector<Range>& GetRanges() const { return ranges_; }

  const Range& GetRange(int i) const {
    if (i >= static_cast<int>(ranges_.size())) {
      throw ParquetException("Index overflow");
    }
    return ranges_[i];
  }

  int64_t GetRowCount() const {
    int64_t row_count = 0;
    for (const Range& range : ranges_) {
      row_count += range.RowCount();
    }
    return row_count;
  }

  bool IsOverlapping(int64_t from, int64_t to) const;

  /**
   * Calculates the intersection of the two specified RowRanges object. Two ranges
   * intersect if they have common elements otherwise the result is empty. <pre> For
   * example: [113, 241] ∩ [221, 340] = [221, 241] while [113, 230] ∩ [231, 340] =
   * &lt;EMPTY&gt;
   * </pre>
   * @param left left RowRanges
   * @param right right RowRanges
   * @return a mutable RowRanges contains all the row indexes that were contained in both
   * of the specified objects
   */
  static RowRanges Intersection(const RowRanges& left, const RowRanges& right) {
    const auto& left_ranges = left.GetRanges();
    const auto& right_ranges = right.GetRanges();
    const int num_right_ranges = static_cast<int>(right_ranges.size());

    RowRanges result;
    int right_index = 0;
    for (const Range& l : left_ranges) {
      for (int i = right_index, n = num_right_ranges; i < n; ++i) {
        Range r = right_ranges[i];
        if (l.IsBefore(r)) {
          break;
        } else if (l.IsAfter(r)) {
          right_index = i + 1;
          continue;
        }
        result.Add(Range::Intersection(l, r));
      }
    }

    return result;
  }

  bool operator==(const RowRanges& other) const { return ranges_ == other.ranges_; }

  bool operator!=(const RowRanges& other) const { return !(*this == other); }

  std::string ToString() const {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < ranges_.size(); i++) {
      if (i == 0) {
        ss << ranges_[i].ToString();
      } else {
        ss << "," << ranges_[i].ToString();
      }
    }
    ss << "]";
    return ss.str();
  }

  static constexpr Range EMPTY_RANGE{std::numeric_limits<int64_t>::max(),
                                     std::numeric_limits<int64_t>::min()};

 private:
  inline void CheckRangeIsValid(const Range& range) {
    if (range.from > range.to) {
      throw ParquetException("Invalid range");
    }
  }

  // Should be in asc order
  std::vector<Range> ranges_;
};

}  // namespace parquet
