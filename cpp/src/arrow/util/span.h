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
#include <cstdint>
#include <cstring>
#include <iterator>
#include <type_traits>

namespace arrow::util {

/// std::span polyfill.
///
/// Does not support static extents.
template <typename T>
class span {
  static_assert(sizeof(T),
                R"(
std::span allows contiguous_iterators instead of just pointers, the enforcement
of which requires T to be a complete type. arrow::util::span does not support
contiguous_iterators, but T is still required to be a complete type to prevent
writing code which would break when it is replaced by std::span.)");

 public:
  using element_type = T;
  using value_type = std::remove_cv_t<T>;
  using iterator = T*;
  using const_iterator = T const*;

  span() = default;
  span(const span&) = default;
  span& operator=(const span&) = default;

  template <typename M, typename = std::enable_if_t<std::is_same_v<T, M const>>>
  // NOLINTNEXTLINE runtime/explicit
  constexpr span(span<M> mut) : span{mut.data(), mut.size()} {}

  constexpr span(T* data, size_t count) : data_{data}, size_{count} {}

  constexpr span(T* begin, T* end)
      : data_{begin}, size_{static_cast<size_t>(end - begin)} {}

  template <
      typename R,
      typename DisableUnlessConstructibleFromDataAndSize =
          decltype(span<T>(std::data(std::declval<R>()), std::size(std::declval<R>()))),
      typename DisableUnlessSimilarTypes = std::enable_if_t<std::is_same_v<
          std::decay_t<std::remove_pointer_t<decltype(std::data(std::declval<R>()))>>,
          std::decay_t<T>>>>
  // NOLINTNEXTLINE runtime/explicit, non-const reference
  constexpr span(R&& range) : span{std::data(range), std::size(range)} {}

  constexpr T* begin() const { return data_; }
  constexpr T* end() const { return data_ + size_; }
  constexpr T* data() const { return data_; }

  constexpr size_t size() const { return size_; }
  constexpr size_t size_bytes() const { return size_ * sizeof(T); }
  constexpr bool empty() const { return size_ == 0; }

  constexpr T& operator[](size_t i) { return data_[i]; }
  constexpr const T& operator[](size_t i) const { return data_[i]; }

  constexpr span subspan(size_t offset) const {
    if (offset > size_) return {data_, data_};
    return {data_ + offset, size_ - offset};
  }

  constexpr span subspan(size_t offset, size_t count) const {
    auto out = subspan(offset);
    if (count < out.size_) {
      out.size_ = count;
    }
    return out;
  }

  constexpr bool operator==(span const& other) const {
    if (size_ != other.size_) return false;

    if constexpr (std::is_integral_v<T>) {
      if (size_ == 0) {
        return true;  // memcmp does not handle null pointers, even if size_ == 0
      }
      return std::memcmp(data_, other.data_, size_bytes()) == 0;
    } else {
      T* ptr = data_;
      for (T const& e : other) {
        if (*ptr++ != e) return false;
      }
      return true;
    }
  }
  constexpr bool operator!=(span const& other) const { return !(*this == other); }

 private:
  T* data_{};
  size_t size_{};
};

template <typename R>
span(R& range) -> span<std::remove_pointer_t<decltype(std::data(range))>>;

template <typename T>
span(T*, size_t) -> span<T>;

template <typename T>
constexpr span<std::byte const> as_bytes(span<T> s) {
  return {reinterpret_cast<std::byte const*>(s.data()), s.size_bytes()};
}

template <typename T>
constexpr span<std::byte> as_writable_bytes(span<T> s) {
  return {reinterpret_cast<std::byte*>(s.data()), s.size_bytes()};
}

}  // namespace arrow::util

#if __has_include(<span>)

template <>
class arrow::util::span<std::byte> : public std::span<std::byte> {
 public:
  explicit arrow::util::span(const arrow::util::span<std::byte>& other)
      : std::span<std::byte>(other.data(), other.size()) {}

  std::byte* data() const { return this->data_; }

  size_t size() const { return this->size_; }

  operator std::span<std::byte>() const {
    return std::span<std::byte>(this->data_, this->size_);
  }

  bool operator==(const arrow::util::span<std::byte>& other) const {
    return this->data_ == other.data_ && this->size_ == other.size_;
  }

  bool operator!=(const arrow::util::span<std::byte>& other) const {
    return !(*this == other);
  }
};

template <>
class arrow::util::span<std::byte>;

template <>
class std::span<arrow::util::span<std::byte>>;

template <>
class std::span<arrow::util::span<std::byte>> {
 public:
  explicit std::span(const std::span<arrow::util::span<std::byte>>& other) {
    data_.resize(other.size());
    size_ = other.size();
    for (size_t i = 0; i < size_; ++i) {
      data_[i] = arrow::util::span<std::byte>(other[i].data(), other[i].size());
    }
  }

  arrow::util::span<std::byte>& operator[](size_t idx) { return data_[idx]; }

  const arrow::util::span<std::byte>& operator[](size_t idx) const { return data_[idx]; }

  arrow::util::span<std::byte>* data() const { return &data_[0]; }

  size_t size() const { return size_; }

  operator arrow::util::span<std::byte>() const {
    return arrow::util::span<std::byte>(data_[0].data(), size_ * data_[0].size());
  }

  bool operator==(const std::span<arrow::util::span<std::byte>>& other) const {
    if (size_ != other.size_) {
      return false;
    }
    for (size_t i = 0; i < size_; ++i) {
      if (data_[i] != other[i]) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const std::span<arrow::util::span<std::byte>>& other) const {
    return !(*this == other);
  }

 private:
  std::vector<arrow::util::span<std::byte>> data_;
  size_t size_;
};

#endif
