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

#include <variant>
#include <vector>

#include "parquet/platform.h"

namespace parquet {

/// RowRanges is a collection of non-overlapping and ascendingly ordered row ranges.
class PARQUET_EXPORT RowRanges {
 public:
  /// \brief EXPERIMENTAL: A range of contiguous rows represented by an interval.
  struct IntervalRange {
    /// Start row of the range (inclusive).
    int64_t start;
    /// End row of the range (inclusive).
    int64_t end;
  };

  /// \brief EXPERIMENTAL: A range of contiguous rows represented by a bitmap.
  struct BitmapRange {
    /// Start row of the range (inclusive).
    int64_t offset;
    /// Zero appended if there are less than 64 elements.
    uint64_t bitmap;
  };

  /// \brief EXPERIMENTAL: An end marker for the row range iterator.
  struct End {};

  /// \brief EXPERIMENTAL: An iterator for accessing row ranges in order.
  class Iterator {
   public:
    virtual ~Iterator() = default;
    virtual std::variant<IntervalRange, BitmapRange, End> NextRange() = 0;
  };

  /// \brief EXPERIMENTAL: Create a new iterator for accessing row ranges in order.
  std::unique_ptr<Iterator> NewIterator() const;

  /// \brief EXPERIMENTAL: Validate the row ranges.
  /// \throws ParquetException if the row ranges are not in ascending order or
  /// overlapped.
  void Validate() const;

  /// \brief EXPERIMENTAL: Get the total number of rows in the row ranges.
  int64_t row_count() const;

  /// \brief EXPERIMENTAL: Compute the intersection of two row ranges.
  static RowRanges Intersect(const RowRanges& lhs, const RowRanges& rhs);

  /// \brief EXPERIMENTAL: Compute the union of two row ranges.
  static RowRanges Union(const RowRanges& lhs, const RowRanges& rhs);

  /// \brief EXPERIMENTAL: Make a single row range of [0, row_count - 1].
  static RowRanges MakeSingle(int64_t row_count);

  /// \brief EXPERIMENTAL: Make a single row range of [start, end].
  static RowRanges MakeSingle(int64_t start, int64_t end);

  /// \brief EXPERIMENTAL: Make a row range from a list of intervals.
  static RowRanges MakeIntervals(const std::vector<IntervalRange>& intervals);

 private:
  friend class IteratorImpl;
  std::vector<std::variant<IntervalRange, BitmapRange>> ranges_;
};

}  // namespace parquet
