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
#include "arrow/util/double_conversion.h"
#include "arrow/util/string_view.h"
#include "arrow/util/time.h"
#include "arrow/util/visibility.h"
#include "arrow/vendored/datetime.h"

namespace arrow {
namespace internal {

/// \brief The entry point for conversion to strings.
template <typename ARROW_TYPE, typename Enable = void>
class StringFormatter;

template <typename T>
struct is_formattable {
  template <typename U, typename = typename StringFormatter<U>::value_type>
  static std::true_type Test(U*);

  template <typename U>
  static std::false_type Test(...);

  static constexpr bool value = decltype(Test<T>(NULLPTR))::value;
};

template <typename T, typename R = void>
using enable_if_formattable = enable_if_t<is_formattable<T>::value, R>;

template <typename Appender>
using Return = decltype(std::declval<Appender>()(util::string_view{}));

/////////////////////////////////////////////////////////////////////////
// Boolean formatting

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
util::string_view ViewDigitBuffer(const std::array<char, BUFFER_SIZE>& buffer,
                                  char* cursor) {
  auto buffer_end = buffer.data() + BUFFER_SIZE;
  return {cursor, static_cast<size_t>(buffer_end - cursor)};
}

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
class IntToStringFormatterMixin {
 public:
  explicit IntToStringFormatterMixin(const std::shared_ptr<DataType>& = NULLPTR) {}

  using value_type = typename ARROW_TYPE::c_type;

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    constexpr size_t buffer_size =
        detail::Digits10(std::numeric_limits<value_type>::max()) + 1;

    std::array<char, buffer_size> buffer;
    char* cursor = buffer.data() + buffer_size;
    detail::FormatAllDigits(detail::Abs(value), &cursor);
    if (value < 0) {
      detail::FormatOneChar('-', &cursor);
    }
    return append(detail::ViewDigitBuffer(buffer, cursor));
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
  FloatToStringFormatter(int flags, const char* inf_symbol, const char* nan_symbol,
                         char exp_character, int decimal_in_shortest_low,
                         int decimal_in_shortest_high,
                         int max_leading_padding_zeroes_in_precision_mode,
                         int max_trailing_padding_zeroes_in_precision_mode);
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

  FloatToStringFormatterMixin(int flags, const char* inf_symbol, const char* nan_symbol,
                              char exp_character, int decimal_in_shortest_low,
                              int decimal_in_shortest_high,
                              int max_leading_padding_zeroes_in_precision_mode,
                              int max_trailing_padding_zeroes_in_precision_mode)
      : FloatToStringFormatter(flags, inf_symbol, nan_symbol, exp_character,
                               decimal_in_shortest_low, decimal_in_shortest_high,
                               max_leading_padding_zeroes_in_precision_mode,
                               max_trailing_padding_zeroes_in_precision_mode) {}

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

/////////////////////////////////////////////////////////////////////////
// Temporal formatting

namespace detail {

template <typename V>
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

template <>
class StringFormatter<DurationType> : public IntToStringFormatterMixin<DurationType> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

template <typename T>
class StringFormatter<T, enable_if_date<T>> {
 public:
  using value_type = typename T::c_type;

  explicit StringFormatter(const std::shared_ptr<DataType>& = NULLPTR) {}

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    arrow_vendored::date::days since_epoch;
    if (T::type_id == Type::DATE32) {
      since_epoch = arrow_vendored::date::days{value};
    } else {
      since_epoch = std::chrono::duration_cast<arrow_vendored::date::days>(
          std::chrono::milliseconds{value});
    }

    arrow_vendored::date::sys_days timepoint_days{since_epoch};

    constexpr size_t buffer_size = detail::BufferSizeYYYY_MM_DD<value_type>();

    std::array<char, buffer_size> buffer;
    char* cursor = buffer.data() + buffer_size;

    detail::FormatYYYY_MM_DD(arrow_vendored::date::year_month_day{timepoint_days},
                             &cursor);
    return append(detail::ViewDigitBuffer(buffer, cursor));
  }
};

template <typename T>
class StringFormatter<T, enable_if_time<T>> {
 public:
  using value_type = typename T::c_type;

  explicit StringFormatter(const std::shared_ptr<DataType>& type)
      : unit_(checked_cast<const T&>(*type).unit()) {}

  template <typename Duration, typename Appender>
  Return<Appender> operator()(Duration, value_type count, Appender&& append) {
    Duration since_midnight{count};

    constexpr size_t buffer_size = detail::BufferSizeHH_MM_SS<Duration>();

    std::array<char, buffer_size> buffer;
    char* cursor = buffer.data() + buffer_size;

    detail::FormatHH_MM_SS(arrow_vendored::date::make_time(since_midnight), &cursor);
    return append(detail::ViewDigitBuffer(buffer, cursor));
  }

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    return util::VisitDuration(unit_, *this, value, std::forward<Appender>(append));
  }

 private:
  TimeUnit::type unit_;
};

template <>
class StringFormatter<TimestampType> {
 public:
  using value_type = int64_t;

  explicit StringFormatter(const std::shared_ptr<DataType>& type)
      : unit_(checked_cast<const TimestampType&>(*type).unit()) {}

  template <typename Duration, typename Appender>
  Return<Appender> operator()(Duration, value_type count, Appender&& append) {
    Duration since_epoch{count};

    arrow_vendored::date::sys_days timepoint_days{
        arrow_vendored::date::floor<arrow_vendored::date::days>(since_epoch)};

    Duration since_midnight = since_epoch - timepoint_days.time_since_epoch();

    constexpr size_t buffer_size = detail::BufferSizeYYYY_MM_DD<value_type>() + 1 +
                                   detail::BufferSizeHH_MM_SS<Duration>();

    std::array<char, buffer_size> buffer;
    char* cursor = buffer.data() + buffer_size;

    detail::FormatHH_MM_SS(arrow_vendored::date::make_time(since_midnight), &cursor);
    detail::FormatOneChar(' ', &cursor);
    detail::FormatYYYY_MM_DD(arrow_vendored::date::year_month_day{timepoint_days},
                             &cursor);
    return append(detail::ViewDigitBuffer(buffer, cursor));
  }

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    return util::VisitDuration(unit_, *this, value, std::forward<Appender>(append));
  }

 private:
  TimeUnit::type unit_;
};

}  // namespace internal
}  // namespace arrow
