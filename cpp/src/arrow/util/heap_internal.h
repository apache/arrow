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

#include <algorithm>
#include <functional>
#include <vector>
#include "arrow/util/macros.h"

namespace arrow {
namespace internal {

// A Heap class, is a simple wrapper to make heap operation simpler.
template <typename T, typename Compare = std::less<T>>
class Heap {
 public:
  Heap() : values_(), comp_() {}
  explicit Heap(const Compare& compare) : values_(), comp_(compare) {}

  ARROW_DISALLOW_COPY_AND_ASSIGN(Heap);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(Heap);

  T top() const { return values_.front(); }

  bool empty() const { return values_.empty(); }

  size_t size() const { return values_.size(); }

  void Push(const T& value) {
    values_.push_back(value);
    std::push_heap(values_.begin(), values_.end(), comp_);
  }

  void Pop() {
    std::pop_heap(values_.begin(), values_.end(), comp_);
    values_.pop_back();
  }

  void ReplaceTop(const T& value) {
    std::pop_heap(values_.begin(), values_.end(), comp_);
    values_.back() = value;
    std::push_heap(values_.begin(), values_.end(), comp_);
  }

 protected:
  std::vector<T> values_;

  Compare comp_;
};

// A Stable Heap class, is a simple wrapper to make heap operation simpler.
template <typename T>
class StableHeap {
 public:
  using Comparator = std::function<int64_t(const T&, const T&)>;
  using HeapItem = std::pair<T, uint64_t>;
  using StableComparator = std::function<bool(const HeapItem&, const HeapItem&)>;

  explicit StableHeap(const Comparator& compare,
                      bool ascending_order_for_duplicates = true)
      : values_(),
        comp_(compare),
        counter_(0),
        ascending_order_for_duplicates_(ascending_order_for_duplicates) {
    this->stable_comp_ = [this](const HeapItem& lhs, const HeapItem& rhs) -> bool {
      auto cmp = this->comp_(lhs.first, rhs.first);
      if ((cmp < 0) || ((cmp == 0) && this->CompareDuplicates(lhs.second, rhs.second)))
        return true;
      return false;
    };
  }

  ARROW_DISALLOW_COPY_AND_ASSIGN(StableHeap);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(StableHeap);

  T top() const { return values_.front().first; }

  bool empty() const { return values_.empty(); }

  size_t size() const { return values_.size(); }

  void Push(const T& value) {
    values_.emplace_back(std::make_pair(value, counter_));
    std::push_heap(values_.begin(), values_.end(), stable_comp_);
    ++counter_;
  }

  void Pop() {
    std::pop_heap(values_.begin(), values_.end(), stable_comp_);
    values_.pop_back();
  }

  void ReplaceTop(const T& value) {
    std::pop_heap(values_.begin(), values_.end(), stable_comp_);
    values_.back() = std::make_pair(value, counter_);
    std::push_heap(values_.begin(), values_.end(), stable_comp_);
    ++counter_;
  }

 protected:
  bool CompareDuplicates(uint64_t left, uint64_t right) {
    if (ascending_order_for_duplicates_) return left < right;
    return right < left;
  }

 private:
  std::vector<HeapItem> values_;

  Comparator comp_;

  uint64_t counter_;

  bool ascending_order_for_duplicates_;

  StableComparator stable_comp_;
};
}  // namespace internal
}  // namespace arrow
