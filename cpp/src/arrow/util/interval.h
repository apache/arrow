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

#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <set>
#include <sstream>

namespace arrow::util {

struct Interval {
  mutable int64_t start;
  mutable int64_t end;
  mutable int32_t start_offset_in_compacted = -1;

  bool operator==(const Interval& other) const {
    return other.start == start && other.end == end;
  }

  bool operator!=(const Interval& other) const {
    return other.start != start || other.end != end;
  }
};

struct IntervalComparator {
  bool operator()(const Interval& left, const Interval& right) const {
    return left.start < right.start;
  }
};

class IntervalMerger {
 public:
  using IntervalSet = std::set<Interval, IntervalComparator>;
  using Iterator = std::set<Interval, IntervalComparator>::iterator;

  void AddInterval(const Interval& interval) {
    assert(interval.start < interval.end);
    auto [it, is_slow_path_triggered] = TryFastInsert(interval);
    if (is_slow_path_triggered) {
      last_inserted_iterator = SlowInsert(interval);
    } else {
      last_inserted_iterator = it;
    }
  }

  void AddInterval(int64_t s, int64_t end) { AddInterval({s, end, -1}); }

  int64_t CompactIntervalAndGetTotalSize() {
    int64_t total_size = 0;
    for (auto& it : interval_set_) {
      // Note: This method is used for intervals produced from StringView elements.
      // These intervals cannot produce occupancy in a buffer where start_offset exceeds
      // INT32_MAX.
      assert(total_size <= std::numeric_limits<int32_t>::max());
      it.start_offset_in_compacted = static_cast<int32_t>(total_size);
      total_size += it.end - it.start;
    }
    return total_size;
  }

  // This method should be called After CompactIntervalAndGetTotalSize
  int32_t GetCompactedPosition(int32_t view_offset) const {
    auto it = interval_set_.lower_bound({view_offset, -1, -1});
    if (it == interval_set_.end()) {
      --it;
      // offset from the start of interval
      auto relative_offset = view_offset - it->start;
      return static_cast<int32_t>(relative_offset) + it->start_offset_in_compacted;
    } else if (it->start == view_offset) {
      // this is the case where view_offset refers to the beginning of interval
      return it->start_offset_in_compacted;
    } else {
      --it;
      // offset from the start of interval
      auto relative_offset = view_offset - it->start;
      return static_cast<int32_t>(relative_offset) + it->start_offset_in_compacted;
    }
  }

  IntervalSet::const_iterator begin() const { return interval_set_.cbegin(); }

  IntervalSet::const_iterator end() const { return interval_set_.cend(); }

  std::string ToString() const {
    std::ostringstream sink;
    for (auto& it : interval_set_) {
      sink << it.start << "," << it.end << "," << it.start_offset_in_compacted << "\n";
    }
    return sink.str();
  }

  int64_t size() const { return static_cast<int64_t>(interval_set_.size()); }

 private:
  // Check whether insertion is possible in O(1) time complexity.
  // Returns true if the slow path must be taken.
  // The returned iterator is only valid if false is returned.
  std::pair<Iterator, bool> TryFastInsert(const Interval& interval) {
    if (last_inserted_iterator == interval_set_.end()) {
      auto [it, is_inserted] = interval_set_.insert(interval);
      return {it, false};
    } else if (interval.start >= last_inserted_iterator->start) {
      if (interval.start <= last_inserted_iterator->end) {
        // The interval and last_inserted_iterator are joinable.
        // Attempt to add or merge at last_inserted_iterator.
        if (interval.end <= last_inserted_iterator->end) {
          return {last_inserted_iterator, false};
        } else {
          // The interval and last_inserted_iterator are joinable.
          // interval.end >= last_inserted_iterator->end
          auto next_it = std::next(last_inserted_iterator);
          if (next_it == interval_set_.end()) {
            last_inserted_iterator->end = interval.end;
            return {last_inserted_iterator, false};
          } else if (interval.end < next_it->start) {
            // The interval is not joinable with next_it.
            last_inserted_iterator->end = interval.end;
            return {last_inserted_iterator, false};
          } else if (interval.end <= next_it->end) {
            last_inserted_iterator->end = next_it->end;
            interval_set_.erase(next_it);
            return {last_inserted_iterator, false};
          } else {
            return {interval_set_.end(), true};
          }
        }
      } else {
        // Interval.start > last_inserted_iterator.start
        // Interval and last_inserted_iterator are not joinable; however, it is checked
        // whether insertion is possible.
        auto next_it = std::next(last_inserted_iterator);
        if (next_it == interval_set_.end()) {
          auto it = interval_set_.insert(next_it, interval);
          return {it, false};
        } else if (next_it->start > interval.end) {
          // There is no overlap with either the current or the next iterator.
          auto it = interval_set_.insert(next_it, interval);
          return {it, false};
        } else if (next_it->end >= interval.end) {
          // There is an overlap with the next iterator.
          next_it->start = std::min(next_it->start, interval.start);
          next_it->end = next_it->end;
          return {next_it, false};
        } else {
          return {interval_set_.end(), true};
        }
      }
    } else {
      // interval.start is less than last_inserted_iterator->end.
      // Handling fast insert for this case.
      // This may be impractical due to the many states involved,
      // but it attempts to address some of them here.
      if (last_inserted_iterator == interval_set_.begin()) {
        if (last_inserted_iterator->start <= interval.end) {
          // The interval and last_inserted_iterator are joinable.
          if (last_inserted_iterator->end >= interval.end) {
            last_inserted_iterator->start = interval.start;
            return {last_inserted_iterator, false};
          } else {
            return {interval_set_.end(), true};
          }
        } else {
          auto it = interval_set_.insert(last_inserted_iterator, interval);
          return {it, false};
        }
      } else {
        return {interval_set_.end(), true};
      }
    }
  }

  Iterator SlowInsert(const Interval& interval) {
    auto [it, should_continue] = MergeOrInsertInterval(interval);
    if (should_continue) {
      return JoinRight(it);
    } else {
      return it;
    }
  }

  // Returns true if the merging process should continue.
  std::pair<IntervalSet::iterator, bool> MergeOrInsertInterval(const Interval& interval) {
    // The empty state is handled in the fast path.
    auto it = interval_set_.lower_bound(interval);
    if (it != interval_set_.end() && it->start == interval.start) {
      if (it->end >= interval.end) {
        return {it, false};
      } else {
        it->end = interval.end;
        return {it, true};
      }
    } else if (it == interval_set_.begin()) {
      // Since 'it' is the lower bound, it->start >= interval.start.
      // Check for joinability.
      if (it->start <= interval.end) {
        // they are joinable
        if (it->end >= interval.end) {
          it->start = interval.start;
          return {it, false};
        } else {
          it->end = interval.end;
          it->start = interval.start;
          return {it, true};
        }
      } else {
        it = interval_set_.insert(it, interval);
        return {it, false};
      }
    } else {
      auto prev_it = std::prev(it);
      // Check whether joinable.
      if (interval.start <= prev_it->end) {
        // There is an overlap between prev_it and interval.
        prev_it->end = std::max(interval.end, prev_it->end);
        return {prev_it, true};
      } else if (it == interval_set_.end()) {
        // There is no overlap between interval with prev_it.
        it = interval_set_.insert(it, interval);
        return {it, false};
      } else if (it->start <= interval.end) {
        // There is no overlap between interval with prev_it.
        // There is an overlap between 'it' and interval.
        it->end = std::max(it->end, interval.end);
        // Note that this is safe because it is checked that the
        // interval does not overlap with the previous iterator.
        it->start = interval.start;
        return {it, true};
      } else {
        it = interval_set_.insert(it, interval);
        // It is not joinable with either the previous or current iterator,
        // so the merging process can be finished here.
        return {it, false};
      }
    }
  }

  Iterator JoinRight(Iterator& it) {
    auto next_it = std::next(it);
    if (next_it == interval_set_.end()) {
      return it;
    } else if (next_it->start > it->end) {
      // They are not joinable.
      return it;
    } else {
      auto end_it = interval_set_.upper_bound({it->end, -1});
      // Note that 'it' is joinable with next_it,
      // so the following operation will not result in an invalid state.
      auto prev_end_it = std::prev(end_it);
      it->end = std::max(prev_end_it->end, it->end);
      // Since 'it' and 'next_it' are joinable, 'end_it' refers to at least one element
      // ahead of 'next_it'.
      interval_set_.erase(next_it, end_it);
      return it;
    }
  }

  IntervalSet interval_set_;
  Iterator last_inserted_iterator = interval_set_.end();
};

}  // namespace arrow::util
