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

#include <array>
#include <cassert>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/string_view.h"
#include "arrow/util/time.h"
#include "arrow/util/visibility.h"
#include "arrow/vendored/datetime.h"

namespace arrow {
namespace internal {

/// \brief The entry point for conversion to strings.
///
/// Specializations of FormatValueTraits for `ARROW_TYPE` must define:
/// - A member type `value_type` which will be formatted.
/// - A static member function `MaxSize(const ARROW_TYPE& t)` indicating the
///   maximum number of characters which formatting a `value_type` may yield.
/// - The static member function `Convert`, callable with signature
///   `(const ARROW_TYPE& t, const value_type& v, char* out)`. `Convert` returns
///   the number of characters written into `out`. Parameters required for formatting
///   (for example a timestamp's TimeUnit) are acquired from the type parameter `t`.
template <typename ARROW_TYPE, typename Enable = void>
struct FormatValueTraits;

/// \brief Convenience wrappers around FormatValueTraits.
template <typename T>
size_t FormatValue(const T& type, typename FormatValueTraits<T>::value_type v,
                   char* out) {
  return FormatValueTraits<T>::Convert(type, v, out);
}

template <typename T>
std::string FormatValue(const T& type, typename FormatValueTraits<T>::value_type v) {
  std::string out(FormatValueTraits<T>::MaxSize(type), '\0');
  size_t size = FormatValueTraits<T>::Convert(type, v, &out[0]);
  out.resize(size);
  return out;
}

template <typename T>
enable_if_parameter_free<T, size_t> FormatValue(
    typename FormatValueTraits<T>::value_type v, char* out) {
  static T type;
  return FormatValue(type, v, out);
}

template <typename T>
enable_if_parameter_free<T, std::string> FormatValue(
    typename FormatValueTraits<T>::value_type v) {
  static T type;
  return FormatValue(type, v);
}

/// \brief Traits for detecting formattability
template <typename T>
struct is_formattable {
  template <typename U, typename = typename FormatValueTraits<U>::value_type>
  static std::true_type Test(U*);

  template <typename U>
  static std::false_type Test(...);

  static constexpr bool value = decltype(Test<T>(NULLPTR))::value;
};

template <typename T, typename R = void>
using enable_if_formattable = enable_if_t<is_formattable<T>::value, R>;

/////////////////////////////////////////////////////////////////////////
// Boolean formatting

namespace detail {
inline size_t Copy(util::string_view s, char* out) { return s.copy(out, s.size()); }
}  // namespace detail

template <>
struct FormatValueTraits<BooleanType> {
  using value_type = bool;

  static constexpr size_t MaxSize(const BooleanType&) { return 5; }

  static size_t Convert(const BooleanType&, bool value, char* out) {
    constexpr util::string_view true_string = "true";
    constexpr util::string_view false_string = "false";
    return detail::Copy(value ? true_string : false_string, out);
  }
};

/////////////////////////////////////////////////////////////////////////
// Integer formatting

namespace detail {

// A 2x100 direct table mapping integers in [0..99] to their decimal representations.
ARROW_EXPORT extern const char digit_pairs[];

// Based on fmtlib's format_int class:
// Write digits from right to left into a stack allocated buffer
inline void FormatOneChar(char c, char** cursor) { *--*cursor = c; }

template <typename Int>
void FormatOneDigit(Int value, char** cursor) {
  assert(value >= 0 && value <= 9);
  FormatOneChar(static_cast<char>('0' + value), cursor);
}

template <typename Int>
void FormatTwoDigits(Int value, char** cursor) {
  assert(value >= 0 && value <= 99);
  auto digit_pair = &digit_pairs[value * 2];
  FormatOneChar(digit_pair[1], cursor);
  FormatOneChar(digit_pair[0], cursor);
}

template <typename Int>
void FormatAllDigits(Int value, char** cursor) {
  assert(value >= 0);
  while (value >= 100) {
    FormatTwoDigits(value % 100, cursor);
    value /= 100;
  }

  if (value >= 10) {
    FormatTwoDigits(value, cursor);
  } else {
    FormatOneDigit(value, cursor);
  }
}

template <typename Int>
void FormatAllDigitsLeftPadded(Int value, size_t pad, char pad_char, char** cursor) {
  auto end = *cursor - pad;
  FormatAllDigits(value, cursor);
  while (*cursor > end) {
    FormatOneChar(pad_char, cursor);
  }
}

template <size_t BUFFER_SIZE>
struct DigitBuffer {
  DigitBuffer() : cursor(buffer_.data() + BUFFER_SIZE) {}

  util::string_view view() const {
    auto buffer_end = buffer_.data() + BUFFER_SIZE;
    return {cursor, static_cast<size_t>(buffer_end - cursor)};
  }

  std::array<char, BUFFER_SIZE> buffer_;
  char* cursor;
};

template <typename Int, typename UInt = typename std::make_unsigned<Int>::type>
constexpr UInt Abs(Int value) {
  return value < 0 ? ~static_cast<UInt>(value) + 1 : static_cast<UInt>(value);
}

template <typename Int>
constexpr size_t Digits10(Int value) {
  return value <= 9 ? 1 : Digits10(value / 10) + 1;
}

}  // namespace detail

template <typename ARROW_TYPE>
struct FormatValueTraits<ARROW_TYPE,
                         enable_if_t<is_integer_type<ARROW_TYPE>::value ||
                                     std::is_same<ARROW_TYPE, DurationType>::value>> {
  using value_type = typename ARROW_TYPE::c_type;

  static constexpr size_t max_size =
      detail::Digits10(std::numeric_limits<value_type>::max()) + 1;

  static constexpr size_t MaxSize(const ARROW_TYPE&) { return max_size; }

  static size_t Convert(const ARROW_TYPE&, value_type value, char* out) {
    detail::DigitBuffer<max_size> buffer;
    detail::FormatAllDigits(detail::Abs(value), &buffer.cursor);
    if (std::numeric_limits<value_type>::is_signed && value < 0) {
      detail::FormatOneChar('-', &buffer.cursor);
    }
    return detail::Copy(buffer.view(), out);
  }
};

/////////////////////////////////////////////////////////////////////////
// Floating-point formatting

namespace detail {

ARROW_EXPORT int FormatFloat(float v, size_t buffer_size, char* out);
ARROW_EXPORT int FormatFloat(double v, size_t buffer_size, char* out);

}  // namespace detail

template <typename ARROW_TYPE>
struct FormatValueTraits<ARROW_TYPE,
                         enable_if_t<is_floating_type<ARROW_TYPE>::value &&
                                     !is_half_float_type<ARROW_TYPE>::value>> {
  using value_type = typename ARROW_TYPE::c_type;

  static constexpr size_t max_size =
      1 +                                              // '-' when negative
      1 +                                              // decimal point
      std::numeric_limits<value_type>::max_digits10 +  // significand digits
      2 +                                              // 'e+' or 'e-'
      detail::Digits10(                                // exponent digits
          std::numeric_limits<value_type>::max_exponent10);

  static constexpr size_t MaxSize(const ARROW_TYPE&) { return max_size; }

  static size_t Convert(const ARROW_TYPE&, value_type value, char* out) {
    std::array<char, max_size> buffer;
    int size = detail::FormatFloat(value, max_size, buffer.data());
    return detail::Copy({buffer.data(), static_cast<size_t>(size)}, out);
  }
};

/////////////////////////////////////////////////////////////////////////
// Temporal formatting

namespace detail {

constexpr size_t BufferSizeYYYY_MM_DD() {
  return detail::Digits10(9999) + 1 + detail::Digits10(12) + 1 + detail::Digits10(31);
}

inline void FormatYYYY_MM_DD(arrow_vendored::date::year_month_day ymd, char** cursor) {
  FormatTwoDigits(static_cast<unsigned>(ymd.day()), cursor);
  FormatOneChar('-', cursor);
  FormatTwoDigits(static_cast<unsigned>(ymd.month()), cursor);
  FormatOneChar('-', cursor);
  auto year = static_cast<int>(ymd.year());
  assert(year <= 9999);
  FormatTwoDigits(year % 100, cursor);
  FormatTwoDigits(year / 100, cursor);
}

template <typename Duration>
constexpr size_t BufferSizeHH_MM_SS() {
  return detail::Digits10(23) + 1 + detail::Digits10(59) + 1 + detail::Digits10(59) + 1 +
         detail::Digits10(Duration::period::den) - 1;
}

struct BufferSizeFromTimeUnitImpl {
  template <typename Duration>
  constexpr size_t operator()(Duration) const {
    return detail::BufferSizeHH_MM_SS<Duration>();
  }
};

inline size_t BufferSizeHH_MM_SS(TimeUnit::type unit) {
  return util::VisitDuration(unit, BufferSizeFromTimeUnitImpl{});
}

template <typename Duration>
void FormatHH_MM_SS(arrow_vendored::date::hh_mm_ss<Duration> hms, char** cursor) {
  constexpr size_t subsecond_digits = Digits10(Duration::period::den) - 1;
  if (subsecond_digits != 0) {
    FormatAllDigitsLeftPadded(hms.subseconds().count(), subsecond_digits, '0', cursor);
    FormatOneChar('.', cursor);
  }
  FormatTwoDigits(hms.seconds().count(), cursor);
  FormatOneChar(':', cursor);
  FormatTwoDigits(hms.minutes().count(), cursor);
  FormatOneChar(':', cursor);
  FormatTwoDigits(hms.hours().count(), cursor);
}

}  // namespace detail

template <typename T>
struct FormatValueTraits<T, enable_if_date<T>> {
  using value_type = typename T::c_type;

  static constexpr size_t max_size = detail::BufferSizeYYYY_MM_DD();

  static constexpr size_t MaxSize(const T&) { return max_size; }

  static size_t Convert(const T&, value_type value, char* out) {
    arrow_vendored::date::days since_epoch;
    if (T::type_id == Type::DATE32) {
      since_epoch = arrow_vendored::date::days{value};
    } else {
      since_epoch = std::chrono::duration_cast<arrow_vendored::date::days>(
          std::chrono::milliseconds{value});
    }

    arrow_vendored::date::sys_days timepoint_days{since_epoch};

    detail::DigitBuffer<max_size> buffer;

    detail::FormatYYYY_MM_DD(arrow_vendored::date::year_month_day{timepoint_days},
                             &buffer.cursor);
    return detail::Copy(buffer.view(), out);
  }
};

template <typename T>
struct FormatValueTraits<T, enable_if_time<T>> {
  using value_type = typename T::c_type;

  static size_t MaxSize(const T& type) { return detail::BufferSizeHH_MM_SS(type.unit()); }

  struct ConvertImpl {
    template <typename Duration>
    size_t operator()(Duration, value_type count, char* out) {
      Duration since_midnight{count};

      detail::DigitBuffer<detail::BufferSizeHH_MM_SS<Duration>()> buffer;

      detail::FormatHH_MM_SS(arrow_vendored::date::make_time(since_midnight),
                             &buffer.cursor);
      return detail::Copy(buffer.view(), out);
    }
  };

  static size_t Convert(const T& type, value_type value, char* out) {
    return util::VisitDuration(type.unit(), ConvertImpl{}, value, out);
  }
};

template <>
struct FormatValueTraits<TimestampType> {
  using value_type = int64_t;

  static size_t MaxSize(const TimestampType& type) {
    return detail::BufferSizeYYYY_MM_DD() + 1 + detail::BufferSizeHH_MM_SS(type.unit());
  }

  struct ConvertImpl {
    template <typename Duration>
    size_t operator()(Duration, value_type count, char* out) {
      Duration since_epoch{count};

      arrow_vendored::date::sys_days timepoint_days{
          arrow_vendored::date::floor<arrow_vendored::date::days>(since_epoch)};

      Duration since_midnight = since_epoch - timepoint_days.time_since_epoch();

      detail::DigitBuffer<detail::BufferSizeYYYY_MM_DD() + 1 +
                          detail::BufferSizeHH_MM_SS<Duration>()>
          buffer;

      detail::FormatHH_MM_SS(arrow_vendored::date::make_time(since_midnight),
                             &buffer.cursor);
      detail::FormatOneChar(' ', &buffer.cursor);
      detail::FormatYYYY_MM_DD(arrow_vendored::date::year_month_day{timepoint_days},
                               &buffer.cursor);
      return detail::Copy(buffer.view(), out);
    }
  };

  static size_t Convert(const TimestampType& type, value_type value, char* out) {
    return util::VisitDuration(type.unit(), ConvertImpl{}, value, out);
  }
};

}  // namespace internal
}  // namespace arrow
