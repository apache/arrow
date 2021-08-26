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
// This class is immutable by design
template <typename T, typename Compare = std::less<T>>
class ARROW_EXPORT Heap {
 public:
  explicit Heap() : values_(), comp_() {}
  explicit Heap(const Compare& compare) : values_(), comp_(compare) {}

  Heap(Heap&&) = default;
  Heap& operator=(Heap&&) = default;

  T* Data() { return values_.data(); }

  // const T& Top() const { return values_.front(); }

  T Top() const { return values_.front(); }

  bool Empty() const { return values_.empty(); }

  size_t Size() const { return values_.size(); }

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

  void SetComparator(const Compare& comp) { comp_ = comp; }

 public:
  ARROW_DISALLOW_COPY_AND_ASSIGN(Heap);

  std::vector<T> values_;

  Compare comp_;
};

}  // namespace internal
}  // namespace arrow