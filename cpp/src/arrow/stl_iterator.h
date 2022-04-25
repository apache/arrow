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
    UpdateComponents(*this);
  }

  // Value access
  value_type operator*() const { return *iterators_list_[current_chunk_index_]; }

  value_type operator[](difference_type n) const {
    auto chunk_location = GetChunkLocation(index_ + n);
    return iterators_list_[chunk_location.chunk_index]
                          [chunk_location.index_in_chunk -
                           iterators_list_[chunk_location.chunk_index].index()];
  }

  int64_t index() const { return index_; }

  // Forward / backward
  ChunkedArrayIterator& operator++() {
    ++index_;
    if (iterators_list_[current_chunk_index_].index() ==
        chunked_array_->chunk(static_cast<int>(current_chunk_index_))->length() - 1) {
      iterators_list_[current_chunk_index_] -=
          iterators_list_[current_chunk_index_].index();
      current_chunk_index_++;
      if (!chunked_array_->chunk(static_cast<int>(current_chunk_index_))->length()) {
        current_chunk_index_ = GetChunkLocation(index_).chunk_index;
      }
      iterators_list_[current_chunk_index_] -=
          iterators_list_[current_chunk_index_].index();
    } else {
      iterators_list_[current_chunk_index_]++;
    }
    return *this;
  }
  ChunkedArrayIterator& operator--() {
    --index_;
    if (iterators_list_[current_chunk_index_].index()) {
      iterators_list_[current_chunk_index_]--;
    } else {
      iterators_list_[current_chunk_index_] -=
          iterators_list_[current_chunk_index_].index();
      current_chunk_index_--;
      if (!chunked_array_->chunk(static_cast<int>(current_chunk_index_))->length()) {
        current_chunk_index_ = GetChunkLocation(index_).chunk_index;
      }
      iterators_list_[current_chunk_index_] -=
          iterators_list_[current_chunk_index_].index();
      iterators_list_[current_chunk_index_] +=
          chunked_array_->chunk(static_cast<int>(current_chunk_index_))->length() - 1;
    }
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
    bool in_current_chunk;
    auto& current_iterator = iterators_list_[current_chunk_index_];
    if (n >= 0) {
      in_current_chunk =
          n < chunked_array_->chunk(static_cast<int>(current_chunk_index_))->length() -
                  current_iterator.index();
    } else {
      in_current_chunk = n > current_iterator.index();
    }
    if (in_current_chunk) {
      index_ += n;
      current_iterator += n;
      return *this;
    } else {
      current_iterator -= current_iterator.index();
      index_ += n;
      UpdateComponents(*this, true);
      return *this;
    }
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
    return chunked_array_->GetChunkResolver().Resolve(index);
  }

  void UpdateComponents(ChunkedArrayIterator<ArrayType>& chunked_array_iterator,
                        bool update = false) {
    if (!update) {
      int64_t chunk_index = 0;
      for (const auto& array : chunked_array_iterator.chunked_array_->chunks()) {
        chunked_array_iterator.iterators_list_.emplace_back(
            *arrow::internal::checked_pointer_cast<ArrayType>(array));
        auto chunk_length = array->length();
        if (chunk_index) {
          chunk_length += chunked_array_iterator.chunks_lengths_[chunk_index - 1];
        }
        chunked_array_iterator.chunks_lengths_.push_back(chunk_length);
        chunk_index++;
      }
    }

    chunked_array_iterator.current_chunk_index_ =
        GetChunkLocation(chunked_array_iterator.index_).chunk_index;
    auto& current_iterator =
        chunked_array_iterator
            .iterators_list_[chunked_array_iterator.current_chunk_index_];
    current_iterator -= current_iterator.index() - chunked_array_iterator.index_;
    if (chunked_array_iterator.current_chunk_index_) {
      current_iterator -=
          chunked_array_iterator
              .chunks_lengths_[chunked_array_iterator.current_chunk_index_ - 1];
    }
  }

  const ChunkedArray* chunked_array_;
  int64_t index_;
  int64_t current_chunk_index_;
  std::vector<int64_t> chunks_lengths_;
  std::vector<ArrayIterator<ArrayType>> iterators_list_;
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
