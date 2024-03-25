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

#include <variant>

#include "parquet/exception.h"

namespace parquet {
// ----------------------------------------------------------------------
// RowRanges and ins implementations
bool IsValid(const std::vector<IntervalRange>& ranges) {
  if (ranges.size() == 0) return true;
  if (ranges[0].start < 0) {
    return false;
  }
  for (size_t i = 0; i < ranges.size(); i++) {
    if (!IntervalRangeUtils::IsValid(ranges[i])) {
      return false;
    }
  }
  for (size_t i = 1; i < ranges.size(); i++) {
    if (ranges[i].start <= ranges[i - 1].end) {
      return false;
    }
  }
  return true;
}

IntervalRanges::IntervalRanges() = default;

IntervalRanges::IntervalRanges(const IntervalRange& range) {
  ranges_.push_back(range);
  if (!IsValid(ranges_)) {
    throw ParquetException("Invalid range with start: " + std::to_string(range.start) +
                           " and end: " + std::to_string(range.end) +
                           ", keep it monotone and non-interleaving");
  }
}

IntervalRanges::IntervalRanges(const std::vector<IntervalRange>& ranges) {
  this->ranges_ = ranges;
  if (!IsValid(ranges_)) {
    throw ParquetException("Invalid ranges: " + this->IntervalRanges::ToString() +
                           ", keep it monotone and non-interleaving");
  }
}

std::unique_ptr<RowRanges::Iterator> IntervalRanges::NewIterator() const {
  return std::make_unique<IntervalRowRangesIterator>(ranges_);
}

size_t IntervalRanges::num_rows() const {
  size_t cnt = 0;
  for (const IntervalRange& range : ranges_) {
    cnt += IntervalRangeUtils::Count(range);
  }
  return cnt;
}

int64_t IntervalRanges::first_row() const {
  if (ranges_.empty()) {
    throw ParquetException("first_row() called on empty IntervalRanges");
  }
  return ranges_.front().start;
}

int64_t IntervalRanges::last_row() const {
  if (ranges_.empty()) {
    throw ParquetException("last_row() called on empty IntervalRanges");
  }
  return ranges_.back().end;
}

bool IntervalRanges::IsOverlapping(const int64_t start, const int64_t end) const {
  auto searchRange = IntervalRange{start, end};
  auto it = std::lower_bound(ranges_.begin(), ranges_.end(), searchRange,
                             [](const IntervalRange& r1, const IntervalRange& r2) {
                               return IntervalRangeUtils::IsBefore(r1, r2);
                             });
  return it != ranges_.end() && !IntervalRangeUtils::IsAfter(*it, searchRange);
}

std::string IntervalRanges::ToString() const {
  std::string result = "[";
  for (const IntervalRange& range : ranges_) {
    result += IntervalRangeUtils::ToString(range) + ", ";
  }
  if (!ranges_.empty()) {
    result = result.substr(0, result.size() - 2);
  }
  result += "]";
  return result;
}

std::vector<std::unique_ptr<RowRanges>> IntervalRanges::SplitByRowRange(
    const std::vector<int64_t>& num_rows_per_sub_ranges) const {
  if (num_rows_per_sub_ranges.size() <= 1) {
    std::unique_ptr<RowRanges> single =
        std::make_unique<IntervalRanges>(*this);  // return a copy of itself
    auto ret = std::vector<std::unique_ptr<RowRanges>>();
    ret.push_back(std::move(single));
    return ret;
  }

  std::vector<std::unique_ptr<RowRanges>> result;

  IntervalRanges spaces;
  int64_t rows_so_far = 0;
  for (size_t i = 0; i < num_rows_per_sub_ranges.size(); ++i) {
    auto start = rows_so_far;
    rows_so_far += num_rows_per_sub_ranges[i];
    auto end = rows_so_far - 1;
    spaces.Add({start, end});
  }

  // each RG's row range forms a space, we need to adjust RowRanges in each space to
  // zero based.
  for (IntervalRange space : spaces.GetRanges()) {
    auto intersection = Intersection(IntervalRanges(space), *this);

    std::unique_ptr<IntervalRanges> zero_based_ranges =
        std::make_unique<IntervalRanges>();
    for (const IntervalRange& range : intersection.GetRanges()) {
      zero_based_ranges->Add({range.start - space.start, range.end - space.start});
    }
    result.push_back(std::move(zero_based_ranges));
  }

  return result;
}

IntervalRanges IntervalRanges::Intersection(const IntervalRanges& left,
                                            const IntervalRanges& right) {
  IntervalRanges result;

  size_t rightIndex = 0;
  for (const IntervalRange& l : left.ranges_) {
    for (size_t i = rightIndex, n = right.ranges_.size(); i < n; ++i) {
      const IntervalRange& r = right.ranges_[i];
      if (IntervalRangeUtils::IsBefore(l, r)) {
        break;
      } else if (IntervalRangeUtils::IsAfter(l, r)) {
        rightIndex = i + 1;
        continue;
      }
      result.Add(IntervalRangeUtils::Intersection(l, r));
    }
  }

  return result;
}

void IntervalRanges::Add(const IntervalRange& range) {
  const IntervalRange rangeToAdd = range;
  if (ranges_.size() > 1 && rangeToAdd.start <= ranges_.back().end) {
    throw ParquetException("Ranges must be added in order");
  }
  ranges_.push_back(rangeToAdd);
}

const std::vector<IntervalRange>& IntervalRanges::GetRanges() const { return ranges_; }

IntervalRowRangesIterator::IntervalRowRangesIterator(
    const std::vector<IntervalRange>& ranges)
    : ranges_(ranges) {}

IntervalRowRangesIterator::~IntervalRowRangesIterator() {}

std::variant<IntervalRange, BitmapRange, End> IntervalRowRangesIterator::NextRange() {
  if (current_index_ >= ranges_.size()) return End();

  return ranges_[current_index_++];
}
}  // namespace parquet
