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

#include "parquet/row_range.h"

#include "arrow/util/bitmap_ops.h"
#include "arrow/util/unreachable.h"
#include "parquet/exception.h"

namespace parquet {

class IteratorImpl : public RowRanges::Iterator {
 public:
  explicit IteratorImpl(const RowRanges& ranges)
      : iter_(ranges.ranges_.cbegin()), end_(ranges.ranges_.cend()) {}

  ~IteratorImpl() override = default;

  std::variant<RowRanges::IntervalRange, RowRanges::BitmapRange, RowRanges::End>
  NextRange() override {
    if (iter_ == end_) {
      return RowRanges::End();
    }
    if (std::holds_alternative<RowRanges::IntervalRange>(*iter_)) {
      return std::get<RowRanges::IntervalRange>(*iter_);
    }
    if (std::holds_alternative<RowRanges::BitmapRange>(*iter_)) {
      return std::get<RowRanges::BitmapRange>(*iter_);
    }
    arrow::Unreachable("Invalid row ranges type");
  }

 private:
  decltype(RowRanges::ranges_.cbegin()) iter_;
  decltype(RowRanges::ranges_.cend()) end_;
};

std::unique_ptr<RowRanges::Iterator> RowRanges::NewIterator() const {
  return std::make_unique<IteratorImpl>(*this);
}

void RowRanges::Validate() const {
  int64_t last_end = -1;
  for (const auto& range : ranges_) {
    if (std::holds_alternative<RowRanges::IntervalRange>(range)) {
      const auto& interval = std::get<RowRanges::IntervalRange>(range);
      if (interval.start <= last_end) {
        throw ParquetException("Row ranges are not in ascending order");
      }
      if (interval.end < interval.start) {
        throw ParquetException("Invalid interval range");
      }
      last_end = interval.end;
      continue;
    }
    if (std::holds_alternative<RowRanges::BitmapRange>(range)) {
      const auto& bitmap = std::get<RowRanges::BitmapRange>(range);
      if (bitmap.offset <= last_end) {
        throw ParquetException("Row ranges are not in ascending order");
      }
      last_end = bitmap.offset + sizeof(bitmap.bitmap) - 1;
      continue;
    }
    arrow::Unreachable("Invalid row ranges type");
  }
}

int64_t RowRanges::row_count() const {
  int64_t count = 0;
  for (const auto& range : ranges_) {
    if (std::holds_alternative<RowRanges::IntervalRange>(range)) {
      const auto& interval = std::get<RowRanges::IntervalRange>(range);
      count += interval.end - interval.start + 1;
    }
    if (std::holds_alternative<RowRanges::BitmapRange>(range)) {
      const auto& bitmap = std::get<RowRanges::BitmapRange>(range);
      count += arrow::internal::CountSetBits(
          reinterpret_cast<const uint8_t*>(&bitmap.bitmap), 0, sizeof(bitmap.bitmap));
    }
    arrow::Unreachable("Invalid row ranges type");
  }
  return count;
}

RowRanges RowRanges::Intersect(const RowRanges& lhs, const RowRanges& rhs) {
  RowRanges result;
  auto lhs_iter = lhs.NewIterator();
  auto rhs_iter = rhs.NewIterator();
  auto lhs_range = lhs_iter->NextRange();
  auto rhs_range = rhs_iter->NextRange();

  while (!std::holds_alternative<End>(lhs_range) &&
         !std::holds_alternative<End>(rhs_range)) {
    if (!std::holds_alternative<IntervalRange>(lhs_range) ||
        !std::holds_alternative<IntervalRange>(rhs_range)) {
      throw ParquetException("Bitmap range is not yet supported");
    }

    auto& left = std::get<IntervalRange>(lhs_range);
    auto& right = std::get<IntervalRange>(rhs_range);

    // Find overlapping region
    int64_t start = std::max(left.start, right.start);
    int64_t end = std::min(left.end, right.end);

    // If there is an overlap, add it to results
    if (start <= end) {
      result.ranges_.push_back(IntervalRange{start, end});
    }

    // Advance the iterator with smaller end
    if (left.end < right.end) {
      lhs_range = lhs_iter->NextRange();
    } else {
      rhs_range = rhs_iter->NextRange();
    }
  }

  return result;
}

RowRanges RowRanges::Union(const RowRanges& lhs, const RowRanges& rhs) {
  RowRanges result;
  auto lhs_iter = lhs.NewIterator();
  auto rhs_iter = rhs.NewIterator();
  auto lhs_range = lhs_iter->NextRange();
  auto rhs_range = rhs_iter->NextRange();

  if (std::holds_alternative<End>(lhs_range)) {
    return rhs;
  }
  if (std::holds_alternative<End>(rhs_range)) {
    return lhs;
  }

  if (std::holds_alternative<BitmapRange>(lhs_range)) {
    throw ParquetException("Bitmap range is not yet supported");
  }
  IntervalRange current = std::get<IntervalRange>(lhs_range);
  lhs_range = lhs_iter->NextRange();

  while (!std::holds_alternative<End>(lhs_range) ||
         !std::holds_alternative<End>(rhs_range)) {
    IntervalRange next;

    if (std::holds_alternative<End>(rhs_range)) {
      // Only lhs ranges remain
      if (std::holds_alternative<BitmapRange>(lhs_range)) {
        throw ParquetException("Bitmap range is not yet supported");
      }
      next = std::get<IntervalRange>(lhs_range);
      lhs_range = lhs_iter->NextRange();
    } else if (std::holds_alternative<End>(lhs_range)) {
      // Only rhs ranges remain
      if (std::holds_alternative<BitmapRange>(rhs_range)) {
        throw ParquetException("Bitmap range is not yet supported");
      }
      next = std::get<IntervalRange>(rhs_range);
      rhs_range = rhs_iter->NextRange();
    } else {
      // Both iterators have ranges - pick the one with smaller start
      if (std::holds_alternative<BitmapRange>(lhs_range) ||
          std::holds_alternative<BitmapRange>(rhs_range)) {
        throw ParquetException("Bitmap range is not yet supported");
      }
      const auto& left = std::get<IntervalRange>(lhs_range);
      const auto& right = std::get<IntervalRange>(rhs_range);

      if (left.start <= right.start) {
        next = left;
        lhs_range = lhs_iter->NextRange();
      } else {
        next = right;
        rhs_range = rhs_iter->NextRange();
      }
    }

    if (current.end + 1 >= next.start) {
      // Concatenate overlapping or adjacent ranges
      current.end = std::max(current.end, next.end);
    } else {
      // Gap between current and next range
      result.ranges_.push_back(current);
      current = next;
    }
  }

  result.ranges_.push_back(current);
  return result;
}

RowRanges RowRanges::MakeSingle(int64_t row_count) {
  RowRanges rowRanges;
  rowRanges.ranges_.push_back(IntervalRange{0, row_count - 1});
  return rowRanges;
}

RowRanges RowRanges::MakeSingle(int64_t start, int64_t end) {
  RowRanges rowRanges;
  rowRanges.ranges_.push_back(IntervalRange{start, end});
  return rowRanges;
}

RowRanges RowRanges::MakeIntervals(const std::vector<IntervalRange>& intervals) {
  RowRanges rowRanges;
  rowRanges.ranges_.reserve(intervals.size());
  rowRanges.ranges_.insert(rowRanges.ranges_.end(), intervals.cbegin(), intervals.cend());
  return rowRanges;
}

}  // namespace parquet