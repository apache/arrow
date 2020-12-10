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

#include <cstddef>
#include <iterator>
#include <utility>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/optional.h"

namespace arrow {
namespace stl {

namespace detail {

template <typename ArrayType>
struct DefaultValueAccessor {
  using ValueType = decltype(std::declval<ArrayType>().GetView(0));

  ValueType operator()(const ArrayType& array, int64_t index) {
    return array.GetView(index);
  }
};

}  // namespace detail

template <typename ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
class ArrayIterator {
 public:
  using value_type = arrow::util::optional<typename ValueAccessor::ValueType>;
  using difference_type = int64_t;
  using iterator_category = std::random_access_iterator_tag;

  // Some algorithms need to default-construct an iterator
  ArrayIterator() : array_(NULLPTR), index_(0) {}

  explicit ArrayIterator(const ArrayType& array, int64_t index = 0)
      : array_(&array), index_(index) {}

  // Value access
  value_type operator*() const {
    return array_->IsNull(index_) ? value_type{} : array_->GetView(index_);
  }

  value_type operator[](difference_type n) const {
    return array_->IsNull(index_ + n) ? value_type{} : array_->GetView(index_ + n);
  }

  int64_t index() const { return index_; }

  // Forward / backward
  ArrayIterator& operator++() {
    ++index_;
    return *this;
  }
  ArrayIterator& operator--() {
    --index_;
    return *this;
  }
  ArrayIterator operator++(int) {
    ArrayIterator tmp(*this);
    ++index_;
    return tmp;
  }
  ArrayIterator operator--(int) {
    ArrayIterator tmp(*this);
    --index_;
    return tmp;
  }

  // Arithmetic
  difference_type operator-(const ArrayIterator& other) const {
    return index_ - other.index_;
  }
  ArrayIterator operator+(difference_type n) const {
    return ArrayIterator(*array_, index_ + n);
  }
  ArrayIterator operator-(difference_type n) const {
    return ArrayIterator(*array_, index_ - n);
  }
  friend inline ArrayIterator operator+(difference_type diff,
                                        const ArrayIterator& other) {
    return ArrayIterator(*other.array_, diff + other.index_);
  }
  friend inline ArrayIterator operator-(difference_type diff,
                                        const ArrayIterator& other) {
    return ArrayIterator(*other.array_, diff - other.index_);
  }
  ArrayIterator& operator+=(difference_type n) {
    index_ += n;
    return *this;
  }
  ArrayIterator& operator-=(difference_type n) {
    index_ -= n;
    return *this;
  }

  // Comparisons
  bool operator==(const ArrayIterator& other) const { return index_ == other.index_; }
  bool operator!=(const ArrayIterator& other) const { return index_ != other.index_; }
  bool operator<(const ArrayIterator& other) const { return index_ < other.index_; }
  bool operator>(const ArrayIterator& other) const { return index_ > other.index_; }
  bool operator<=(const ArrayIterator& other) const { return index_ <= other.index_; }
  bool operator>=(const ArrayIterator& other) const { return index_ >= other.index_; }

 private:
  const ArrayType* array_;
  int64_t index_;
};

}  // namespace stl
}  // namespace arrow

namespace std {

template <typename ArrayType>
struct iterator_traits<::arrow::stl::ArrayIterator<ArrayType>> {
  using IteratorType = ::arrow::stl::ArrayIterator<ArrayType>;
  using difference_type = typename IteratorType::difference_type;
  using value_type = typename IteratorType::value_type;
  using iterator_category = typename IteratorType::iterator_category;
};

}  // namespace std
