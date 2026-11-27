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

#include <concepts>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"

namespace arrow::util {

inline BinaryViewType::c_type ToInlineBinaryView(const void* data, int32_t size) {
  assert(size <= BinaryViewType::kInlineSize);
  // Small string: inlined. Bytes beyond size are zeroed
  BinaryViewType::c_type out;
  out.inlined = {size, {}};
  memcpy(&out.inlined.data, data, size);
  return out;
}

inline BinaryViewType::c_type ToInlineBinaryView(std::string_view v) {
  assert(v.size() <= BinaryViewType::kInlineSize);
  return ToInlineBinaryView(v.data(), static_cast<int32_t>(v.size()));
}

inline BinaryViewType::c_type ToNonInlineBinaryView(const void* data, int32_t size,
                                                    int32_t buffer_index,
                                                    int32_t offset) {
  // Large string: store index/offset.
  BinaryViewType::c_type out;
  out.ref = {size, {}, buffer_index, offset};
  memcpy(&out.ref.prefix, data, sizeof(out.ref.prefix));
  return out;
}

inline BinaryViewType::c_type ToBinaryView(const void* data, int32_t size,
                                           int32_t buffer_index, int32_t offset) {
  if (size <= BinaryViewType::kInlineSize) {
    return ToInlineBinaryView(data, size);
  }
  return ToNonInlineBinaryView(data, size, buffer_index, offset);
}

inline BinaryViewType::c_type ToBinaryView(std::string_view v, int32_t buffer_index,
                                           int32_t offset) {
  return ToBinaryView(v.data(), static_cast<int32_t>(v.size()), buffer_index, offset);
}

template <typename BufferPtr>
std::string_view FromBinaryView(const BinaryViewType::c_type& v,
                                const BufferPtr* data_buffers) {
  auto* data = v.is_inline() ? v.inlined.data.data()
                             : data_buffers[v.ref.buffer_index]->data() + v.ref.offset;
  return {reinterpret_cast<const char*>(data), static_cast<size_t>(v.size())};
}
template <typename BufferPtr>
std::string_view FromBinaryView(BinaryViewType::c_type&&, const BufferPtr*) = delete;

template <typename BufferPtr>
bool EqualBinaryView(BinaryViewType::c_type l, BinaryViewType::c_type r,
                     const BufferPtr* l_buffers, const BufferPtr* r_buffers) {
  int64_t l_size_and_prefix, r_size_and_prefix;
  memcpy(&l_size_and_prefix, &l, sizeof(l_size_and_prefix));
  memcpy(&r_size_and_prefix, &r, sizeof(r_size_and_prefix));

  if (l_size_and_prefix != r_size_and_prefix) return false;

  if (l.is_inline()) {
    // The columnar spec mandates that the inlined part be zero-padded, so we can compare
    // a word at a time regardless of the exact size.
    int64_t l_inlined, r_inlined;
    memcpy(&l_inlined, l.inline_data() + BinaryViewType::kPrefixSize, sizeof(l_inlined));
    memcpy(&r_inlined, r.inline_data() + BinaryViewType::kPrefixSize, sizeof(r_inlined));
    return l_inlined == r_inlined;
  }

  // Sizes are equal and this is not inline, therefore both are out
  // of line and have kPrefixSize first in common.
  const uint8_t* l_data = l_buffers[l.ref.buffer_index]->data() + l.ref.offset;
  const uint8_t* r_data = r_buffers[r.ref.buffer_index]->data() + r.ref.offset;
  return memcmp(l_data + BinaryViewType::kPrefixSize,
                r_data + BinaryViewType::kPrefixSize,
                l.size() - BinaryViewType::kPrefixSize) == 0;
}

/// \brief Compare a BinaryView value with contiguous string data.
///
/// Performs prefix-accelerated early rejection by checking string length and the
/// 4-byte inline prefix before dereferencing variadic buffers.
template <typename BufferPtr>
bool EqualBinaryViewString(BinaryViewType::c_type view, const uint8_t* str_data,
                           int64_t str_len, const BufferPtr* view_buffers) {
  static_assert(BinaryViewType::kPrefixSize == sizeof(uint32_t));
  if (view.size() != str_len) return false;
  if (view.size() == 0) return true;

  if (view.size() < BinaryViewType::kPrefixSize) {
    return memcmp(view.inline_data(), str_data, view.size()) == 0;
  }

  uint32_t view_prefix, str_prefix;
  memcpy(&view_prefix, view.inline_data(), sizeof(uint32_t));
  memcpy(&str_prefix, str_data, sizeof(uint32_t));
  if (view_prefix != str_prefix) return false;

  if (view.is_inline()) {
    if (view.size() == BinaryViewType::kPrefixSize) return true;
    return memcmp(view.inline_data() + BinaryViewType::kPrefixSize,
                  str_data + BinaryViewType::kPrefixSize,
                  view.size() - BinaryViewType::kPrefixSize) == 0;
  }

  const uint8_t* view_payload =
      view_buffers[view.ref.buffer_index]->data() + view.ref.offset;
  return memcmp(view_payload + BinaryViewType::kPrefixSize,
                str_data + BinaryViewType::kPrefixSize,
                view.size() - BinaryViewType::kPrefixSize) == 0;
}

template <typename BufferPtr>
bool EqualBinaryViewString(BinaryViewType::c_type view, std::string_view str,
                           const BufferPtr* view_buffers) {
  return EqualBinaryViewString(view, reinterpret_cast<const uint8_t*>(str.data()),
                               static_cast<int64_t>(str.size()), view_buffers);
}

/// \brief Lazy descriptor view of a BinaryView array element.
///
/// Allows operators that opt into CallView to inspect inline size and 4-byte
/// prefix metadata before materializing a variadic buffer string_view.
struct BinaryViewAccessor {
  BinaryViewType::c_type view;
  const std::shared_ptr<Buffer>* buffers;

  int32_t size() const { return view.size(); }
  bool is_inline() const { return view.is_inline(); }
  std::string_view value() const& { return FromBinaryView(view, buffers); }
  std::string_view value() && = delete;
  operator std::string_view() const& { return value(); }
  operator std::string_view() && = delete;
};

/// \brief Hoisted metadata for comparing a scalar string/binary value against
/// BinaryView descriptors without per-row prefix extraction.
struct PinnedBinaryScalar {
  std::string_view str;
  int64_t len = 0;
  uint32_t prefix = 0;
  int64_t size_and_prefix = 0;
  int64_t inlined = 0;
  bool is_inline = true;
  bool fits_in_binary_view = true;

  static PinnedBinaryScalar FromString(bool is_valid, std::string_view s) {
    static_assert(BinaryViewType::kPrefixSize == sizeof(uint32_t));
    static_assert(sizeof(int32_t) + BinaryViewType::kPrefixSize == sizeof(int64_t));
    static_assert(BinaryViewType::kInlineSize - BinaryViewType::kPrefixSize ==
                  sizeof(int64_t));

    PinnedBinaryScalar out;
    if (!is_valid) {
      return out;
    }

    out.str = s;
    out.len = static_cast<int64_t>(s.size());
    out.fits_in_binary_view = (out.len <= std::numeric_limits<int32_t>::max());
    if (out.len >= BinaryViewType::kPrefixSize) {
      memcpy(&out.prefix, s.data(), sizeof(out.prefix));
    }

    if (out.fits_in_binary_view) {
      BinaryViewType::c_type scalar_view;
      if (out.len == 0) {
        scalar_view.inlined = {0, {}};
      } else {
        scalar_view = ToBinaryView(s.data(), static_cast<int32_t>(out.len), 0, 0);
      }
      memcpy(&out.size_and_prefix, &scalar_view, sizeof(out.size_and_prefix));
      out.is_inline = scalar_view.is_inline();
      if (out.is_inline) {
        memcpy(&out.inlined, scalar_view.inline_data() + BinaryViewType::kPrefixSize,
               sizeof(out.inlined));
      }
    } else {
      out.is_inline = false;
    }
    return out;
  }
};

template <typename T>
concept IsBinaryViewOperand = std::same_as<std::remove_cvref_t<T>, BinaryViewAccessor> ||
                              std::same_as<std::remove_cvref_t<T>, PinnedBinaryScalar>;

inline bool EqualBinaryViewValue(const BinaryViewAccessor& lhs,
                                 const BinaryViewAccessor& rhs) {
  return EqualBinaryView(lhs.view, rhs.view, lhs.buffers, rhs.buffers);
}

inline bool EqualBinaryViewValue(const BinaryViewAccessor& lhs, std::string_view rhs) {
  return EqualBinaryViewString(lhs.view, rhs, lhs.buffers);
}

inline bool EqualBinaryViewValue(std::string_view lhs, const BinaryViewAccessor& rhs) {
  return EqualBinaryViewString(rhs.view, lhs, rhs.buffers);
}

inline bool EqualBinaryViewValue(const BinaryViewAccessor& lhs,
                                 const PinnedBinaryScalar& rhs) {
  static_assert(BinaryViewType::kPrefixSize == sizeof(uint32_t));
  static_assert(sizeof(int32_t) + BinaryViewType::kPrefixSize == sizeof(int64_t));
  static_assert(BinaryViewType::kInlineSize - BinaryViewType::kPrefixSize ==
                sizeof(int64_t));

  if (ARROW_PREDICT_FALSE(!rhs.fits_in_binary_view)) {
    return false;
  }

  int64_t view_size_and_prefix;
  memcpy(&view_size_and_prefix, &lhs.view, sizeof(view_size_and_prefix));
  if (view_size_and_prefix != rhs.size_and_prefix) {
    return false;
  }

  if (rhs.is_inline) {
    int64_t view_inlined;
    memcpy(&view_inlined, lhs.view.inline_data() + BinaryViewType::kPrefixSize,
           sizeof(view_inlined));
    return view_inlined == rhs.inlined;
  }

  const uint8_t* view_payload =
      lhs.buffers[lhs.view.ref.buffer_index]->data() + lhs.view.ref.offset;
  return memcmp(view_payload + BinaryViewType::kPrefixSize,
                rhs.str.data() + BinaryViewType::kPrefixSize,
                lhs.view.size() - BinaryViewType::kPrefixSize) == 0;
}

inline bool EqualBinaryViewValue(const PinnedBinaryScalar& lhs,
                                 const BinaryViewAccessor& rhs) {
  return EqualBinaryViewValue(rhs, lhs);
}

inline bool EqualBinaryViewValue(std::string_view lhs, const PinnedBinaryScalar& rhs) {
  static_assert(BinaryViewType::kPrefixSize == sizeof(uint32_t));
  if (static_cast<int64_t>(lhs.size()) != rhs.len) {
    return false;
  }
  if (rhs.len == 0) {
    return true;
  }

  if (rhs.len < BinaryViewType::kPrefixSize) {
    return memcmp(lhs.data(), rhs.str.data(), static_cast<size_t>(rhs.len)) == 0;
  }

  uint32_t lhs_prefix;
  memcpy(&lhs_prefix, lhs.data(), sizeof(uint32_t));
  if (lhs_prefix != rhs.prefix) {
    return false;
  }

  if (rhs.len == BinaryViewType::kPrefixSize) {
    return true;
  }
  return memcmp(lhs.data() + BinaryViewType::kPrefixSize,
                rhs.str.data() + BinaryViewType::kPrefixSize,
                static_cast<size_t>(rhs.len - BinaryViewType::kPrefixSize)) == 0;
}

inline bool EqualBinaryViewValue(const PinnedBinaryScalar& lhs, std::string_view rhs) {
  return EqualBinaryViewValue(rhs, lhs);
}

/// \brief Compute the total size of a list of binary views including null
/// views.
///
/// This is useful when calculating the necessary memory to store all the string
/// data from the views.
inline int64_t SumOfBinaryViewSizes(const BinaryViewType::c_type* views, int64_t length) {
  int64_t total = 0;
  for (int64_t i = 0; i < length; ++i) {
    total += views[i].size();
  }
  return total;
}

}  // namespace arrow::util
