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
#include <iosfwd>
#include <string>
#include <string_view>

namespace arrow {

/// Variable length string or binary with 4 byte prefix and inline optimization
/// for small values (12 bytes or fewer). This is similar to std::string_view
/// except that the referenced is limited in size to UINT32_MAX and up to the
/// first four bytes of the string are copied into the struct. The prefix allows
/// failing comparisons early and can reduce the CPU cache working set when
/// dealing with short strings.
///
/// This structure supports three states:
///
/// Short string   |----|----|--------|
///                 ^    ^      ^
///                 |    |      |
///                 size prefix remaining in-line portion, zero padded
///
/// Long string    |----|----|--------|
///                 ^    ^      ^
///                 |    |      |
///                 size prefix raw pointer to out-of-line portion
///
/// IO Long string |----|----|----|----|
///                 ^    ^      ^     ^
///                 |    |      |     `----------.
///                 size prefix buffer index and offset to out-of-line portion
///
/// Adapted from TU Munich's UmbraDB [1], Velox, DuckDB.
///
/// [1]: https://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf
///
/// There is no way to determine from a non-inline StringHeader whether it refers
/// to its out-of-line portion with a raw pointer or with index/offset. This
/// information is stored at the column level; so a buffer of StringHeader will
/// contain only one or the other. In general unless a StringHeader is resident
/// in a StringView array's buffer it will refer to out-of-line data with a raw
/// pointer. This default is assumed by several members of StringHeader such as
/// operator==() and operator string_view() since these and other operations cannot
/// be performed on index/offset StringHeaders without also accessing the buffers
/// storing their out-of-line data. Which states pertain to each accessor and
/// constructor are listed in their comments.
struct alignas(8) StringHeader {
 public:
  using value_type = char;

  static constexpr size_t kTotalSize = 16;
  static constexpr size_t kSizeSize = sizeof(uint32_t);
  static constexpr size_t kIndexOffsetSize = sizeof(uint32_t) * 2;
  static constexpr size_t kPrefixSize = kTotalSize - kSizeSize - kIndexOffsetSize;
  static_assert(kPrefixSize == 4);
  static constexpr size_t kInlineSize = kTotalSize - kPrefixSize;
  static_assert(kInlineSize == 12);

  /// Construct an empty view.
  StringHeader() = default;

  /// Construct a RAW POINTER view.
  StringHeader(const char* data, size_t len) : size_(static_cast<uint32_t>(len)) {
    if (size_ == 0) return;

    // TODO(bkietz) better option than assert?
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

  /// Construct a RAW POINTER view.
  StringHeader(const uint8_t* data, int64_t len)
      : StringHeader(reinterpret_cast<const char*>(data), static_cast<size_t>(len)) {}

  /// Convenience implicit constructor for RAW POINTER views from C string/string literal.
  ///
  /// NOLINTNEXTLINE runtime/explicit
  StringHeader(const char* data) : StringHeader(data, std::strlen(data)) {}

  /// Construct a RAW POINTER view.
  explicit StringHeader(const std::string& value)
      : StringHeader(value.data(), value.size()) {}

  /// Construct a RAW POINTER view.
  explicit StringHeader(std::string_view value)
      : StringHeader(value.data(), value.size()) {}

  /// Construct an INDEX/OFFSET view.
  StringHeader(const char* data, uint32_t len, uint32_t buffer_index,
               const char* buffer_data)
      : size_(len) {
    if (size_ == 0) return;

    // TODO(bkietz) better option than assert?
    assert(data);
    if (IsInline()) {
      // small string: inlined. Bytes beyond size_ are already 0
      memcpy(prefix_.data(), data, size_);
    } else {
      // large string: store index/offset
      memcpy(prefix_.data(), data, kPrefixSize);
      SetIndexOffset(buffer_index, static_cast<uint32_t>(data - buffer_data));
    }
  }

  /// Construct an INDEX/OFFSET view.
  StringHeader(uint32_t len, std::array<char, kPrefixSize> prefix, uint32_t buffer_index,
               uint32_t offset)
      : size_(len), prefix_(prefix) {
    SetIndexOffset(buffer_index, offset);
  }

  /// True if the view's data is entirely stored inline.
  /// This function is safe for use against both RAW POINTER and INDEX/OFFSET views.
  bool IsInline() const { return IsInline(size_); }

  template <typename I>
  static constexpr bool IsInline(I size) {
    return size <= static_cast<I>(kInlineSize);
  }

  /// Return a RAW POINTER view's data.
  const char* data() const& { return IsInline() ? prefix_.data() : value_.data; }
  const char* data() && = delete;

  /// The number of characters viewed by this StringHeader.
  /// This function is safe for use against both RAW POINTER and INDEX/OFFSET views.
  size_t size() const { return size_; }

  /// Print a RAW POINTER view to a std::ostream.
  friend std::ostream& operator<<(std::ostream& os, const StringHeader& header);

  /// Equality comparison between RAW POINTER views.
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

  /// Inequality comparison between RAW POINTER views.
  bool operator!=(const StringHeader& other) const { return !(*this == other); }

  /// Less-than comparison between RAW POINTER views.
  bool operator<(const StringHeader& other) const { return Compare(other) < 0; }

  /// Less-than-or-equal comparison between RAW POINTER views.
  bool operator<=(const StringHeader& other) const { return Compare(other) <= 0; }

  /// Greater-than comparison between RAW POINTER views.
  bool operator>(const StringHeader& other) const { return Compare(other) > 0; }

  /// Greater-than-or-equal comparison between RAW POINTER views.
  bool operator>=(const StringHeader& other) const { return Compare(other) >= 0; }

  /// Conversion to std::string_view for RAW POINTER views.
  explicit operator std::string_view() const& { return {data(), size()}; }
  explicit operator std::string_view() && = delete;

  /// Return the always-inline cached first 4 bytes of this StringHeader.
  /// This function is safe for use against both RAW POINTER and INDEX/OFFSET views.
  std::array<char, kPrefixSize> GetPrefix() const { return prefix_; }

  /// Return an INDEX/OFFSET view's buffer index.
  uint32_t GetBufferIndex() const { return value_.io_data.buffer_index; }

  /// Return an INDEX/OFFSET view's buffer offset.
  uint32_t GetBufferOffset() const { return value_.io_data.offset; }

  /// Return a RAW POINTER view's data pointer.
  ///
  /// NOT VALID FOR INLINE VIEWS.
  const char* GetRawPointer() const { return value_.data; }

  /// Return an INDEX/OFFSET view's data pointer.
  ///
  /// NOT VALID FOR INLINE VIEWS.
  template <typename BufferPtr>
  const char* GetPointerFromBuffers(const BufferPtr* char_buffers) const {
    return char_buffers[GetBufferIndex()]->template data_as<char>() + GetBufferOffset();
  }

  /// Return an INDEX/OFFSET view's data pointer.
  template <typename BufferPtr>
  const char* GetPointerFromBuffersOrInlineData(const BufferPtr* char_buffers) const {
    return IsInline() ? GetInlineData() : GetPointerFromBuffers(char_buffers);
  }

  /// Return a the inline data of a view.
  ///
  /// For inline views, this points to the entire data of the view.
  /// For other views, this points to the 4 byte prefix.
  const char* GetInlineData() const& { return prefix_.data(); }
  const char* GetInlineData() && = delete;

  /// Mutate into a RAW POINTER view.
  ///
  /// This function is only intended for use in converting from an equivalent INDEX/OFFSET
  /// view; in particular it does not check or modify the prefix for consistency with the
  /// new data pointer.
  void SetRawPointer(const char* data) { value_.data = data; }

  /// Mutate into an INDEX/OFFSET view.
  ///
  /// This function is only intended for use in converting from an equivalent RAW POINTER
  /// view; in particular it does not check or modify the prefix for consistency with the
  /// new buffer index/offset.
  void SetIndexOffset(uint32_t buffer_index, uint32_t offset) {
    value_.io_data = {buffer_index, offset};
  }

  /// Equality compare an INDEX/OFFSET view in place.
  ///
  /// Equivalent comparison will be accomplished by (for example) first converting both
  /// views to std::string_view and comparing those, but this would not take advantage
  /// of the cached 4 byte prefix.
  template <typename BufferPtr>
  bool EqualsIndexOffset(const BufferPtr* char_buffers, const StringHeader& other,
                         const BufferPtr* other_char_buffers) const {
    if (SizeAndPrefixAsInt64() != other.SizeAndPrefixAsInt64()) {
      return false;
    }
    if (IsInline()) {
      return InlinedAsInt64() == other.InlinedAsInt64();
    }
    // Sizes are equal and this is not inline, therefore both are out of line and we
    // have already checked that their kPrefixSize first characters are equal.
    return memcmp(GetPointerFromBuffers(char_buffers) + kPrefixSize,
                  other.GetPointerFromBuffers(other_char_buffers) + kPrefixSize,
                  size() - kPrefixSize) == 0;
  }

  /// Less-than compare an INDEX/OFFSET view in place.
  ///
  /// Equivalent comparison will be accomplished by (for example) first converting both
  /// views to std::string_view and comparing those, but this would not take advantage
  /// of the cached 4 byte prefix.
  template <typename BufferPtr>
  bool LessThanIndexOffset(const BufferPtr* char_buffers, const StringHeader& other,
                           const BufferPtr* other_char_buffers) const {
    return CompareIndexOffset(char_buffers, other, other_char_buffers) < 0;
  }

 private:
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
      // One string is just the prefix.
      return size_ - other.size_;
    }
    if (static_cast<uint32_t>(size) <= kInlineSize && IsInline() && other.IsInline()) {
      int32_t result = memcmp(value_.inlined.data(), other.value_.inlined.data(), size);
      return (result != 0) ? result : size_ - other.size_;
    }
    int32_t result = memcmp(data() + kPrefixSize, other.data() + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
  }

  template <typename BufferPtr>
  int CompareIndexOffset(const BufferPtr* char_buffers, const StringHeader& other,
                         const BufferPtr* other_char_buffers) const {
    if (PrefixAsInt() != other.PrefixAsInt()) {
      // The result is decided on prefix. The shorter will be less
      // because the prefix is padded with zeros.
      return memcmp(prefix_.data(), other.prefix_.data(), kPrefixSize);
    }
    int32_t size = std::min(size_, other.size_) - kPrefixSize;
    if (size <= 0) {
      // One string is just the prefix.
      return size_ - other.size_;
    }
    if (static_cast<uint32_t>(size) <= kInlineSize && IsInline() && other.IsInline()) {
      int32_t result = memcmp(value_.inlined.data(), other.value_.inlined.data(), size);
      return (result != 0) ? result : size_ - other.size_;
    }

    int32_t result = memcmp(
        GetPointerFromBuffersOrInlineData(char_buffers) + kPrefixSize,
        other.GetPointerFromBuffersOrInlineData(other_char_buffers) + kPrefixSize, size);
    return (result != 0) ? result : size_ - other.size_;
  }

  int64_t SizeAndPrefixAsInt64() const {
    return reinterpret_cast<const int64_t*>(this)[0];
  }

  int64_t InlinedAsInt64() const { return reinterpret_cast<const int64_t*>(this)[1]; }

  int32_t PrefixAsInt() const { return *reinterpret_cast<const int32_t*>(&prefix_); }

  // FIXME(bkietz) replace this with a std::array<uint8_t, 16> and forgo the union.
  // Type punning (AKA violation of the strict aliasing rule) is undefined behavior.
  // Using memcpy to access the bytes of the object representation of trivially copyable
  // objects is not undefined behavior. Given sufficiently explicit hints on alignment
  // and size, compilers elide memcpy calls in favor of identical assembly to what
  // the type punning implementation produces.
  // We rely on all members being laid out top to bottom . C++
  // guarantees this.
  uint32_t size_ = 0;
  std::array<char, kPrefixSize> prefix_ = {0};
  union {
    std::array<char, 8> inlined = {0};
    const char* data;
    struct {
      uint32_t buffer_index;
      uint32_t offset;
    } io_data;
  } value_;
};

static_assert(sizeof(StringHeader) == 16, "struct size expected to be exactly 16 bytes");
static_assert(alignof(StringHeader) == 8,
              "struct alignment expected to be exactly 8 bytes");

}  // namespace arrow
