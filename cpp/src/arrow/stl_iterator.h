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

#include "arrow/chunked_array.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
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
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::random_access_iterator_tag;

  // Some algorithms need to default-construct an iterator
  ArrayIterator() : array_(NULLPTR), index_(0) {}

  explicit ArrayIterator(const ArrayType& array, int64_t index = 0)
      : array_(&array), index_(index) {}

  // Value access
  value_type operator*() const {
    return !array_ || array_->IsNull(index_) ? value_type{} : array_->GetView(index_);
  }

  value_type operator[](difference_type n) const {
    return !array_ || array_->IsNull(index_ + n) ? value_type{}
                                                 : array_->GetView(index_ + n);
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

template <typename ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
class ChunkedArrayIterator {
 public:
  using value_type = arrow::util::optional<typename ValueAccessor::ValueType>;
  using difference_type = int64_t;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::random_access_iterator_tag;

  // Some algorithms need to default-construct an iterator
  ChunkedArrayIterator() : chunked_array_(NULLPTR), index_(0), current_chunk_index_(0) {}

  explicit ChunkedArrayIterator(const ChunkedArray& chunked_array, int64_t index = 0)
      : chunked_array_(&chunked_array), index_(index) {
    auto chunk_location = GetChunkLocation(this->index_);
    if (chunked_array.length()) {
      current_array_iterator_ = ArrayIterator<ArrayType>(
          *arrow::internal::checked_pointer_cast<ArrayType>(
              chunked_array_->chunk(static_cast<int>(chunk_location.chunk_index))),
          index_);
    } else {
      current_array_iterator_ = {};
    }

    this->current_chunk_index_ = chunk_location.chunk_index;
    current_array_iterator_ -= this->index() - chunk_location.index_in_chunk;
  }

  // Value access
  value_type operator*() const { return *current_array_iterator_; }

  value_type operator[](difference_type n) const {
    auto chunk_location = GetChunkLocation(index_ + n);
    if (current_chunk_index_ == chunk_location.chunk_index) {
      return current_array_iterator_[chunk_location.index_in_chunk -
                                     current_array_iterator_.index()];
    } else {
      ArrayIterator<ArrayType> target_iterator{
          *arrow::internal::checked_pointer_cast<ArrayType>(
              chunked_array_->chunk(static_cast<int>(chunk_location.chunk_index)))};
      return target_iterator[chunk_location.index_in_chunk];
    }
  }

  int64_t index() const { return index_; }

  // Forward / backward
  ChunkedArrayIterator& operator++() {
    (*this) += 1;
    return *this;
  }
  ChunkedArrayIterator& operator--() {
    (*this) -= 1;
    return *this;
  }

  ChunkedArrayIterator operator++(int) {
    ChunkedArrayIterator tmp(*this);
    ++*this;
    return tmp;
  }
  ChunkedArrayIterator operator--(int) {
    ChunkedArrayIterator tmp(*this);
    --*this;
    return tmp;
  }

  // Arithmetic
  difference_type operator-(const ChunkedArrayIterator& other) const {
    return index_ - other.index_;
  }
  ChunkedArrayIterator operator+(difference_type n) const {
    return ChunkedArrayIterator(*chunked_array_, index_ + n);
  }
  ChunkedArrayIterator operator-(difference_type n) const {
    return ChunkedArrayIterator(*chunked_array_, index_ - n);
  }
  friend inline ChunkedArrayIterator operator+(difference_type diff,
                                               const ChunkedArrayIterator& other) {
    return ChunkedArrayIterator(*other.chunked_array_, diff + other.index_);
  }
  friend inline ChunkedArrayIterator operator-(difference_type diff,
                                               const ChunkedArrayIterator& other) {
    return ChunkedArrayIterator(*other.chunked_array_, diff - other.index_);
  }
  ChunkedArrayIterator& operator+=(difference_type n) {
    index_ += n;
    auto chunk_location = GetChunkLocation(index_);
    if (current_chunk_index_ == chunk_location.chunk_index) {
      current_array_iterator_ -=
          current_array_iterator_.index() - chunk_location.index_in_chunk;
    } else {
      current_array_iterator_ = ArrayIterator<ArrayType>(
          *arrow::internal::checked_pointer_cast<ArrayType>(
              chunked_array_->chunk(static_cast<int>(chunk_location.chunk_index))),
          chunk_location.index_in_chunk);
      current_chunk_index_ = chunk_location.chunk_index;
    }
    return *this;
  }
  ChunkedArrayIterator& operator-=(difference_type n) {
    (*this) += -n;
    return *this;
  }

  // Comparisons
  bool operator==(const ChunkedArrayIterator& other) const {
    return index_ == other.index_;
  }
  bool operator!=(const ChunkedArrayIterator& other) const {
    return index_ != other.index_;
  }
  bool operator<(const ChunkedArrayIterator& other) const {
    return index_ < other.index_;
  }
  bool operator>(const ChunkedArrayIterator& other) const {
    return index_ > other.index_;
  }
  bool operator<=(const ChunkedArrayIterator& other) const {
    return index_ <= other.index_;
  }
  bool operator>=(const ChunkedArrayIterator& other) const {
    return index_ >= other.index_;
  }

 private:
  arrow::internal::ChunkLocation GetChunkLocation(int64_t index) const {
    return chunked_array_->chunk_resolver_.Resolve(index);
  }

  const ChunkedArray* chunked_array_;
  int64_t index_;
  int64_t current_chunk_index_;
  ArrayIterator<ArrayType> current_array_iterator_;
};

template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType>
ArrayIterator<ArrayType> Iterate(const Array& array) {
  return stl::ArrayIterator<ArrayType>(&array);
}

template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType>
ChunkedArrayIterator<ArrayType> Iterate(const ChunkedArray& chunked_array) {
  return stl::ChunkedArrayIterator<ArrayType>(chunked_array);
}

}  // namespace stl
}  // namespace arrow

namespace std {

template <typename ArrayType>
struct iterator_traits<::arrow::stl::ArrayIterator<ArrayType>> {
  using IteratorType = ::arrow::stl::ArrayIterator<ArrayType>;
  using difference_type = typename IteratorType::difference_type;
  using value_type = typename IteratorType::value_type;
  using pointer = typename IteratorType::pointer;
  using reference = typename IteratorType::reference;
  using iterator_category = typename IteratorType::iterator_category;
};

}  // namespace std
