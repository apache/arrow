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

// This is a private header for number-to-string formatting utilities

#pragma once

#include <cassert>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

/////////////////////////////////////////////////////////////////////////
// Boolean formatting

template <typename ARROW_TYPE, typename Enable = void>
class StringFormatter;

template <typename Appender>
using Return = decltype(std::declval<Appender>()(util::string_view{}));

template <>
class StringFormatter<BooleanType> {
 public:
  explicit StringFormatter(const std::shared_ptr<DataType>& = NULLPTR) {}

  using value_type = bool;

  template <typename Appender>
  Return<Appender> operator()(bool value, Appender&& append) {
    if (value) {
      const char string[] = "true";
      return append(util::string_view(string));
    } else {
      const char string[] = "false";
      return append(util::string_view(string));
    }
  }
};

/////////////////////////////////////////////////////////////////////////
// Integer formatting

namespace detail {

// A 2x100 direct table mapping integers in [0..99] to their decimal representations.
ARROW_EXPORT extern const char digit_pairs[];

template <typename Int>
inline char FormatDigit(Int value) {
  assert(value >= 0 && value < 10);
  return static_cast<char>('0' + value);
}

template <typename Int>
inline const char* FormatTwoDigits(Int value) {
  assert(value >= 0 && value < 100);
  return digit_pairs + (value * 2);
}

}  // namespace detail

template <typename ARROW_TYPE>
class IntToStringFormatterMixin {
 public:
  explicit IntToStringFormatterMixin(const std::shared_ptr<DataType>& = NULLPTR) {}

  using value_type = typename ARROW_TYPE::c_type;
  using unsigned_type = typename std::make_unsigned<value_type>::type;

  static constexpr bool is_signed = std::numeric_limits<value_type>::is_signed;
  static constexpr int buffer_size =
      (is_signed ? 2 : 1) + std::numeric_limits<value_type>::digits10;

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    char buffer[buffer_size];
    char* ptr = buffer + buffer_size;
    int32_t size = 0;
    bool sign = is_signed && (value < 0);
    unsigned_type v;

    if (sign) {
      // Avoid warnings (unsigned negation) and undefined behaviour (signed negation
      // overflow)
      v = ~static_cast<unsigned_type>(value) + 1;
    } else {
      v = static_cast<unsigned_type>(value);
    }

    // Algorithm based on fmtlib's format_int class
    while (v >= 100) {
      unsigned_type next_value = v / 100;
      const char* digit_pair = detail::FormatTwoDigits(v % 100);
      *--ptr = digit_pair[1];
      *--ptr = digit_pair[0];
      size += 2;
      v = next_value;
    }
    if (v < 10) {
      *--ptr = detail::FormatDigit(v);
      ++size;
    } else {
      const char* digit_pair = detail::FormatTwoDigits(v);
      *--ptr = digit_pair[1];
      *--ptr = digit_pair[0];
      size += 2;
    }

    if (sign) {
      *--ptr = '-';
      ++size;
    }

    assert(ptr >= buffer);
    return append(util::string_view(ptr, size));
  }
};

template <>
class StringFormatter<Int8Type> : public IntToStringFormatterMixin<Int8Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<Int16Type> : public IntToStringFormatterMixin<Int16Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<Int32Type> : public IntToStringFormatterMixin<Int32Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<Int64Type> : public IntToStringFormatterMixin<Int64Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<UInt8Type> : public IntToStringFormatterMixin<UInt8Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<UInt16Type> : public IntToStringFormatterMixin<UInt16Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<UInt32Type> : public IntToStringFormatterMixin<UInt32Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <>
class StringFormatter<UInt64Type> : public IntToStringFormatterMixin<UInt64Type> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

/////////////////////////////////////////////////////////////////////////
// Floating-point formatting

class ARROW_EXPORT FloatToStringFormatter {
 public:
  FloatToStringFormatter();
  ~FloatToStringFormatter();

  // Returns the number of characters written
  int FormatFloat(float v, char* out_buffer, int out_size);
  int FormatFloat(double v, char* out_buffer, int out_size);

 protected:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

template <typename ARROW_TYPE>
class FloatToStringFormatterMixin : public FloatToStringFormatter {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  static constexpr int buffer_size = 50;

  explicit FloatToStringFormatterMixin(const std::shared_ptr<DataType>& = NULLPTR) {}

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    char buffer[buffer_size];
    int size = FormatFloat(value, buffer, buffer_size);
    return append(util::string_view(buffer, size));
  }
};

template <>
class StringFormatter<FloatType> : public FloatToStringFormatterMixin<FloatType> {
 public:
  using FloatToStringFormatterMixin::FloatToStringFormatterMixin;
};

template <>
class StringFormatter<DoubleType> : public FloatToStringFormatterMixin<DoubleType> {
 public:
  using FloatToStringFormatterMixin::FloatToStringFormatterMixin;
};

}  // namespace internal
}  // namespace arrow
