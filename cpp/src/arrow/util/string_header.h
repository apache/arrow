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

/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <ostream>
#include <string>
#include <string_view>

namespace arrow {

// Variable length string or binary with 4 byte prefix and inline optimization
// for small values (12 bytes or fewer). This is similar to std::string_view
// except that the referenced is limited in size to UINT32_MAX and up to the
// first four bytes of the string are copied into the struct. The prefix allows
// failing comparisons early and can reduce the CPU cache working set when
// dealing with short strings.
//
// Short string   |----|----|--------|
//                 ^    ^      ^
//                 |    |      |
//                 size prefix remaining in-line portion
//
// Long string    |----|----|--------|
//                 ^    ^      ^
//                 |    |      |
//                 size prefix pointer to out-of-line portion
//
// Adapted from TU Munich's UmbraDB [1], Velox, DuckDB.
//
// [1]: https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf
struct StringHeader {
 public:
  using value_type = char;

  static constexpr size_t kPrefixSize = 4;
  static constexpr size_t kInlineSize = 12;

  StringHeader() = default;

  static StringHeader makeInline(uint32_t size, char** data) {
    assert(size <= kInlineSize);
    StringHeader s;
    s.size_ = size;
    *data = const_cast<char*>(s.data());
    return s;
  }

  StringHeader(const char* data, size_t len) : size_(static_cast<uint32_t>(len)) {
    if (size_ == 0) return;

    // TODO: better option than assert?
    assert(data);
    if (IsInline()) {
      // small string: inlined. Bytes beyond size_ are already 0
      memcpy(prefix_.data(), data, size_);
    } else {
      // large string: store pointer
      memcpy(prefix_.data(), data, kPrefixSize);
      value_.data = data;
    }
  }

  StringHeader(const uint8_t* data, int64_t len)
      : StringHeader(reinterpret_cast<const char*>(data), static_cast<size_t>(len)) {}

  // Making StringHeader implicitly constructible/convertible from char* and
  // string literals, in order to allow for a more flexible API and optional
  // interoperability. E.g:
  //
  //   StringHeader bh = "literal";
  //   std::optional<BytesView> obh = "literal";
  //
  // NOLINTNEXTLINE runtime/explicit
  StringHeader(const char* data) : StringHeader(data, strlen(data)) {}

  explicit StringHeader(const std::string& value)
      : StringHeader(value.data(), value.size()) {}

  explicit StringHeader(std::string_view value)
      : StringHeader(value.data(), value.size()) {}

  bool IsInline() const { return IsInline(size_); }

  static constexpr bool IsInline(uint32_t size) { return size <= kInlineSize; }

  const char* data() const { return IsInline() ? prefix_.data() : value_.data; }

  size_t size() const { return size_; }

  size_t capacity() const { return size_; }

  friend std::ostream& operator<<(std::ostream& os, const StringHeader& header) {
    os.write(header.data(), header.size());
    return os;
  }

  bool operator==(const StringHeader& other) const {
    // Compare lengths and first 4 characters.
    if (SizeAndPrefixAsInt64() != other.SizeAndPrefixAsInt64()) {
      return false;
    }
    if (IsInline()) {
      // The inline part is zeroed at construction, so we can compare
      // a word at a time if data extends past 'prefix_'.
      return size_ <= kPrefixSize || InlinedAsInt64() == other.InlinedAsInt64();
    }
    // Sizes are equal and this is not inline, therefore both are out
    // of line and have kPrefixSize first in common.
    return memcmp(value_.data + kPrefixSize, other.value_.data + kPrefixSize,
                  size_ - kPrefixSize) == 0;
  }

  bool operator!=(const StringHeader& other) const { return !(*this == other); }

  // Returns 0, if this == other
  //       < 0, if this < other
  //       > 0, if this > other
  int32_t Compare(const StringHeader& other) const {
    if (PrefixAsInt() != other.PrefixAsInt()) {
      // The result is decided on prefix. The shorter will be less
      // because the prefix is padded with zeros.
      return memcmp(prefix_.data(), other.prefix_.data(), kPrefixSize);
    }
    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
      // One ends within the prefix.
      return size_ - other.size_;
    }
    if (static_cast<uint32_t>(size) <= kInlineSize && IsInline() && other.IsInline()) {
      int32_t result = memcmp(value_.inlined.data(), other.value_.inlined.data(), size);
      return (result != 0) ? result : size_ - other.size_;
    }
    int32_t result = memcmp(data() + kPrefixSize, other.data() + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
  }

  bool operator<(const StringHeader& other) const { return Compare(other) < 0; }

  bool operator<=(const StringHeader& other) const { return Compare(other) <= 0; }

  bool operator>(const StringHeader& other) const { return Compare(other) > 0; }

  bool operator>=(const StringHeader& other) const { return Compare(other) >= 0; }

  std::string GetString() const { return std::string(data(), size()); }

  explicit operator std::string_view() const { return std::string_view(data(), size()); }

  const char* begin() const { return data(); }

  const char* end() const { return data() + size(); }

  bool empty() const { return size() == 0; }

 private:
  inline int64_t SizeAndPrefixAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[0];
  }

  inline int64_t InlinedAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[1];
  }

  int32_t PrefixAsInt() const { return *reinterpret_cast<const int32_t*>(&prefix_); }

  // We rely on all members being laid out top to bottom . C++
  // guarantees this.
  uint32_t size_ = 0;
  std::array<char, 4> prefix_ = {0};
  union {
    std::array<char, 8> inlined = {0};
    const char* data;
  } value_;
};

static_assert(sizeof(StringHeader) == 16, "struct expected by exactly 16 bytes");

}  // namespace arrow
