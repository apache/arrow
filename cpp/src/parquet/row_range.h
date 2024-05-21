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

// This module contains the logical parquet-cpp types (independent of Thrift
// structures), schema nodes, and related type tools

#pragma once
#include <variant>

#include "parquet/exception.h"

namespace parquet {

// Represent a range to read. The range is inclusive on both ends.
struct IntervalRange {
  IntervalRange(const int64_t start_, const int64_t end_) : start(start_), end(end_) {
    if (start > end) {
      throw ParquetException("Invalid range with start: " + std::to_string(start) +
                             " bigger than end: " + std::to_string(end));
    }
  }

  // inclusive
  int64_t start = -1;
  // inclusive
  int64_t end = -1;
};

class IntervalRangeUtils {
 public:
  static IntervalRange Intersection(const IntervalRange& left,
                                    const IntervalRange& right) {
    if (left.start <= right.start) {
      if (left.end >= right.start) {
        return {right.start, std::min(left.end, right.end)};
      }
    } else if (right.end >= left.start) {
      return {left.start, std::min(left.end, right.end)};
    }
    return {-1, -1};  // Return a default Range object if no intersection range found
  }

  static std::string ToString(const IntervalRange& range) {
    return "(" + std::to_string(range.start) + ", " + std::to_string(range.end) + ")";
  }

  static bool IsValid(const IntervalRange& range) {
    return range.start >= 0 && range.end >= 0 && range.end >= range.start;
  }

  static size_t Count(const IntervalRange& range) {
    if (!IsValid(range)) {
      throw ParquetException("Invalid range: " + ToString(range));
    }
    return range.end - range.start + 1;
  }

  static bool IsBefore(const IntervalRange& self, const IntervalRange& other) {
    return self.end < other.start;
  }

  static bool IsAfter(const IntervalRange& self, const IntervalRange& other) {
    return self.start > other.end;
  }

  static bool IsOverlap(const IntervalRange& self, const IntervalRange& other) {
    return !IsBefore(self, other) && !IsAfter(self, other);
  }
};

struct BitmapRange {
  int64_t offset;
  // zero added to, if there are less than 64 elements left in the column.
  uint64_t bitmap;
};

struct End {};

// Represent a set of ranges to read. The ranges are sorted and non-overlapping.
class RowRanges {
 public:
  virtual ~RowRanges() = default;
  /// \brief Total number of rows in the row ranges.
  virtual size_t num_rows() const = 0;
  /// \brief First row in the ranges
  virtual int64_t first_row() const = 0;
  /// \brief Last row in the ranges
  virtual int64_t last_row() const = 0;
  /// \brief Whether the given range from start to end overlaps with the row ranges.
  virtual bool IsOverlapping(int64_t start, int64_t end) const = 0;
  /// \brief Split the row ranges into sub row ranges according to the
  ///   specified number of rows per sub row ranges. A typical use case is
  ///   to convert file based RowRanges to row group based RowRanges.
  ///
  /// \param num_rows_per_sub_ranges number of rows per sub row range.
  virtual std::vector<std::unique_ptr<RowRanges>> SplitByRowRange(
      const std::vector<int64_t>& num_rows_per_sub_ranges) const = 0;
  /// \brief Readable string representation
  virtual std::string ToString() const = 0;

  class Iterator {
   public:
    virtual std::variant<IntervalRange, BitmapRange, End> NextRange() = 0;
    virtual ~Iterator() = default;
  };
  /// \brief Create an iterator to iterate over the ranges
  virtual std::unique_ptr<Iterator> NewIterator() const = 0;
};

class IntervalRanges : public RowRanges {
 public:
  IntervalRanges();
  explicit IntervalRanges(const IntervalRange& range);
  explicit IntervalRanges(const std::vector<IntervalRange>& ranges);
  std::unique_ptr<Iterator> NewIterator() const override;
  size_t num_rows() const override;
  int64_t first_row() const override;
  int64_t last_row() const override;
  bool IsOverlapping(int64_t start, int64_t end) const override;
  std::string ToString() const override;
  std::vector<std::unique_ptr<RowRanges>> SplitByRowRange(
      const std::vector<int64_t>& num_rows_per_sub_ranges) const override;
  static IntervalRanges Intersection(const IntervalRanges& left,
                                     const IntervalRanges& right);
  void Add(const IntervalRange& range);
  const std::vector<IntervalRange>& GetRanges() const;

 private:
  std::vector<IntervalRange> ranges_;
};

class IntervalRowRangesIterator : public RowRanges::Iterator {
 public:
  explicit IntervalRowRangesIterator(const std::vector<IntervalRange>& ranges);
  ~IntervalRowRangesIterator() override;
  std::variant<IntervalRange, BitmapRange, End> NextRange() override;

 private:
  const std::vector<IntervalRange>& ranges_;
  size_t current_index_ = 0;
};
}  // namespace parquet
