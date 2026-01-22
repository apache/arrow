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
#include <cstddef>
#include <iterator>
#include <optional>
#include <utility>

#include "arrow/chunked_array.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/functional.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace stl {

namespace detail {

template <typename ArrayType>
struct DefaultValueAccessor {
  using ValueType = decltype(std::declval<ArrayType>().GetView(0));

  ValueType operator()(const ArrayType& array, int64_t index) const {
    return array.GetView(index);
  }
};

// Helper to detect if a type has a ValueType member typedef
template <typename T, typename = void>
struct has_value_type : std::false_type {};

template <typename T>
struct has_value_type<T, std::void_t<typename T::ValueType>> : std::true_type {};

// Wrapper for callable objects (like lambdas) that don't have ValueType
template <typename Callable, typename Ret, typename ArrayType>
struct CallableValueAccessor {
  using ValueType = Ret;

  Callable callable;

  explicit CallableValueAccessor(Callable c) : callable(std::move(c)) {}

  ValueType operator()(const ArrayType& array, int64_t index) const {
    return callable(array, index);
  }
};

}  // namespace detail

template <typename ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
class ArrayIterator {
 public:
  using value_type = std::optional<typename ValueAccessor::ValueType>;
  using difference_type = int64_t;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::random_access_iterator_tag;

  // Some algorithms need to default-construct an iterator
  ArrayIterator() : array_(NULLPTR), index_(0), value_accessor_() {}

  explicit ArrayIterator(const ArrayType& array, int64_t index = 0,
                         ValueAccessor value_accessor = {})
      : array_(&array), index_(index), value_accessor_(std::move(value_accessor)) {}

  // Value access
  value_type operator*() const {
    assert(array_);
    return array_->IsNull(index_) ? value_type{} : value_accessor_(*array_, index_);
  }

  value_type operator[](difference_type n) const {
    assert(array_);
    return array_->IsNull(index_ + n) ? value_type{}
                                      : value_accessor_(*array_, index_ + n);
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
    return ArrayIterator(*array_, index_ + n, value_accessor_);
  }
  ArrayIterator operator-(difference_type n) const {
    return ArrayIterator(*array_, index_ - n, value_accessor_);
  }
  friend inline ArrayIterator operator+(difference_type diff,
                                        const ArrayIterator& other) {
    return ArrayIterator(*other.array_, diff + other.index_, other.value_accessor_);
  }
  friend inline ArrayIterator operator-(difference_type diff,
                                        const ArrayIterator& other) {
    return ArrayIterator(*other.array_, diff - other.index_, other.value_accessor_);
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
  ValueAccessor value_accessor_;
};

template <typename ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
class ChunkedArrayIterator {
 public:
  using value_type = std::optional<typename ValueAccessor::ValueType>;
  using difference_type = int64_t;
  using pointer = value_type*;
  using reference = value_type&;
  using iterator_category = std::random_access_iterator_tag;

  // Some algorithms need to default-construct an iterator
  ChunkedArrayIterator() noexcept
      : chunked_array_(NULLPTR), index_(0), value_accessor_() {}

  explicit ChunkedArrayIterator(const ChunkedArray& chunked_array, int64_t index = 0,
                                ValueAccessor value_accessor = {}) noexcept
      : chunked_array_(&chunked_array),
        index_(index),
        value_accessor_(std::move(value_accessor)) {}

  // Value access
  value_type operator*() const {
    auto chunk_location = GetChunkLocation(index_);
    ArrayIterator<ArrayType, ValueAccessor> target_iterator{
        arrow::internal::checked_cast<const ArrayType&>(
            *chunked_array_->chunk(static_cast<int>(chunk_location.chunk_index))),
        0, value_accessor_};
    return target_iterator[chunk_location.index_in_chunk];
  }

  value_type operator[](difference_type n) const { return *(*this + n); }

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
    assert(chunked_array_);
    return ChunkedArrayIterator(*chunked_array_, index_ + n, value_accessor_);
  }
  ChunkedArrayIterator operator-(difference_type n) const {
    assert(chunked_array_);
    return ChunkedArrayIterator(*chunked_array_, index_ - n, value_accessor_);
  }
  friend inline ChunkedArrayIterator operator+(difference_type diff,
                                               const ChunkedArrayIterator& other) {
    assert(other.chunked_array_);
    return ChunkedArrayIterator(*other.chunked_array_, diff + other.index_,
                                other.value_accessor_);
  }
  friend inline ChunkedArrayIterator operator-(difference_type diff,
                                               const ChunkedArrayIterator& other) {
    assert(other.chunked_array_);
    return ChunkedArrayIterator(*other.chunked_array_, diff - other.index_,
                                other.value_accessor_);
  }
  ChunkedArrayIterator& operator+=(difference_type n) {
    index_ += n;
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
  arrow::ChunkLocation GetChunkLocation(int64_t index) const {
    assert(chunked_array_);
    return chunked_array_->chunk_resolver_.Resolve(index);
  }

  const ChunkedArray* chunked_array_;
  int64_t index_;
  ValueAccessor value_accessor_;
};

/// Return an iterator to the beginning of the chunked array
template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
ChunkedArrayIterator<ArrayType, ValueAccessor> Begin(const ChunkedArray& chunked_array,
                                                     ValueAccessor value_accessor = {}) {
  return ChunkedArrayIterator<ArrayType, ValueAccessor>(chunked_array, 0,
                                                        std::move(value_accessor));
}

/// Return an iterator to the end of the chunked array
template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
ChunkedArrayIterator<ArrayType, ValueAccessor> End(const ChunkedArray& chunked_array,
                                                   ValueAccessor value_accessor = {}) {
  return ChunkedArrayIterator<ArrayType, ValueAccessor>(
      chunked_array, chunked_array.length(), std::move(value_accessor));
}

template <typename ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
struct ChunkedArrayRange {
  const ChunkedArray* chunked_array;
  ValueAccessor value_accessor;

  ChunkedArrayIterator<ArrayType, ValueAccessor> begin() {
    return stl::ChunkedArrayIterator<ArrayType, ValueAccessor>(*chunked_array, 0,
                                                               value_accessor);
  }
  ChunkedArrayIterator<ArrayType, ValueAccessor> end() {
    return stl::ChunkedArrayIterator<ArrayType, ValueAccessor>(
        *chunked_array, chunked_array->length(), value_accessor);
  }
};

/// Return an iterable range over the chunked array
template <typename Type, typename ArrayType = typename TypeTraits<Type>::ArrayType,
          typename ValueAccessor = detail::DefaultValueAccessor<ArrayType>>
ChunkedArrayRange<ArrayType, ValueAccessor> Iterate(const ChunkedArray& chunked_array,
                                                    ValueAccessor value_accessor = {}) {
  return stl::ChunkedArrayRange<ArrayType, ValueAccessor>{&chunked_array,
                                                          std::move(value_accessor)};
}

/// Return an iterable range over the chunked array with a custom value accessor
/// This overload deduces ArrayType from the ValueAccessor's first parameter type
/// and requires that ValueAccessor has a ValueType typedef (i.e., it's a struct)
template <typename ValueAccessor,
          typename = ::arrow::internal::call_traits::disable_if_overloaded<ValueAccessor>,
          typename = std::enable_if_t<detail::has_value_type<ValueAccessor>::value>>
auto Iterate(const ChunkedArray& chunked_array, ValueAccessor value_accessor) {
  using ArrayType =
      std::decay_t<::arrow::internal::call_traits::argument_type<0, ValueAccessor>>;
  return stl::ChunkedArrayRange<ArrayType, ValueAccessor>{&chunked_array,
                                                          std::move(value_accessor)};
}

/// Return an iterable range over the chunked array with a callable (e.g., lambda)
/// This overload wraps callables that don't have a ValueType typedef
template <typename Callable,
          typename = ::arrow::internal::call_traits::disable_if_overloaded<Callable>,
          typename = std::enable_if_t<!detail::has_value_type<Callable>::value>,
          typename = void>
auto Iterate(const ChunkedArray& chunked_array, Callable callable) {
  using ArrayType =
      std::decay_t<::arrow::internal::call_traits::argument_type<0, Callable>>;
  using ReturnType = std::decay_t<::arrow::internal::call_traits::return_type<Callable>>;
  using WrappedAccessor = detail::CallableValueAccessor<Callable, ReturnType, ArrayType>;

  return stl::ChunkedArrayRange<ArrayType, WrappedAccessor>{
      &chunked_array, WrappedAccessor{std::move(callable)}};
}

}  // namespace stl
}  // namespace arrow

namespace std {

template <typename ArrayType, typename ValueAccessor>
struct iterator_traits<::arrow::stl::ArrayIterator<ArrayType, ValueAccessor>> {
  using IteratorType = ::arrow::stl::ArrayIterator<ArrayType, ValueAccessor>;
  using difference_type = typename IteratorType::difference_type;
  using value_type = typename IteratorType::value_type;
  using pointer = typename IteratorType::pointer;
  using reference = typename IteratorType::reference;
  using iterator_category = typename IteratorType::iterator_category;
};

template <typename ArrayType, typename ValueAccessor>
struct iterator_traits<::arrow::stl::ChunkedArrayIterator<ArrayType, ValueAccessor>> {
  using IteratorType = ::arrow::stl::ChunkedArrayIterator<ArrayType, ValueAccessor>;
  using difference_type = typename IteratorType::difference_type;
  using value_type = typename IteratorType::value_type;
  using pointer = typename IteratorType::pointer;
  using reference = typename IteratorType::reference;
  using iterator_category = typename IteratorType::iterator_category;
};

}  // namespace std
