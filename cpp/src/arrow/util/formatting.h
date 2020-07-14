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

/////////////////////////////////////////////////////////////////////////
// Boolean formatting

template <typename ARROW_TYPE, typename Enable = void>
class StringFormatter;

template <typename T>
struct is_formattable {
  template <typename U, typename = typename StringFormatter<U>::value_type>
  static std::true_type Test(U*);

  template <typename U>
  static std::false_type Test(...);

  static constexpr bool value = decltype(Test<T>(nullptr))::value;
};

template <typename T, typename R = void>
using enable_if_formattable = enable_if_t<is_formattable<T>::value, R>;

template <typename Appender>
using Return = decltype(std::declval<Appender>()(util::string_view{}));

template <>
class StringFormatter<BooleanType> {
 public:
  explicit StringFormatter(const std::shared_ptr<DataType>& = NULLPTR) {}

  using value_type = bool;

  template <typename Appender>
  Return<Appender> operator()(bool value, Appender&& append) {
    constexpr util::string_view true_string = "true";
    constexpr util::string_view false_string = "false";
    return value ? append(true_string) : append(false_string);
  }
};

/////////////////////////////////////////////////////////////////////////
// Integer formatting

namespace detail {

// A 2x100 direct table mapping integers in [0..99] to their decimal representations.
ARROW_EXPORT extern const char digit_pairs[];

// Based on fmtlib's format_int class:
// Write digits from right to left into a stack allocated buffer
template <size_t BUFFER_SIZE>
struct DigitStringWriter {
  DigitStringWriter() : cursor_(buffer_.data() + BUFFER_SIZE) {}

  void WriteChar(char c) { *--cursor_ = c; }

  template <typename Int>
  void WriteOne(Int value) {
    assert(value >= 0 && value <= 9);
    WriteChar(static_cast<char>('0' + value));
  }

  template <typename Int>
  void WriteTwo(Int value) {
    assert(value >= 0 && value <= 99);
    WriteChar(digit_pairs[value * 2 + 1]);
    WriteChar(digit_pairs[value * 2]);
  }

  template <typename Int>
  void Write(Int value) {
    using unsigned_type = typename std::make_unsigned<Int>::type;

    unsigned_type v = value < 0 ? ~static_cast<unsigned_type>(value) + 1
                                : static_cast<unsigned_type>(value);

    while (v >= 100) {
      WriteTwo(v % 100);
      v /= 100;
    }

    if (v >= 10) {
      WriteTwo(v);
    } else {
      WriteOne(v);
    }
  }

  template <typename Int>
  void WritePadded(Int value, size_t pad, char pad_char) {
    auto minimum_size = size() + pad;
    Write(value);
    for (size_t new_size = size(); new_size < minimum_size; ++new_size) {
      WriteChar(pad_char);
    }
  }

  std::size_t size() const {
    auto buffer_end = buffer_.data() + buffer_.size();
    return static_cast<size_t>(buffer_end - cursor_);
  }

  util::string_view view() const {
    assert(cursor_ >= buffer_.data());
    return {cursor_, size()};
  }

  std::array<char, BUFFER_SIZE> buffer_;
  char* cursor_;
};

template <typename Int>
constexpr size_t Digits10(Int value = std::numeric_limits<Int>::max()) {
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
    detail::DigitStringWriter<detail::Digits10<value_type>() + 1> writer;
    writer.Write(value);
    if (value < 0) {
      writer.WriteChar('-');
    }
    return append(writer.view());
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
 protected:
  void FormatFloat(float v);
  void FormatFloat(double v);

  std::array<char, 50> buffer_;
  size_t size_;
};

template <typename ARROW_TYPE>
class FloatToStringFormatterMixin : public FloatToStringFormatter {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  explicit FloatToStringFormatterMixin(const std::shared_ptr<DataType>& = NULLPTR) {}

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    FormatFloat(value);
    return append(util::string_view(buffer_.data(), size_));
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
  return detail::Digits10<V>() + 1 + detail::Digits10(12) + 1 + detail::Digits10(31);
}

template <size_t BUFFER_SIZE>
void FormatYYYY_MM_DD(arrow_vendored::date::year_month_day ymd,
                      DigitStringWriter<BUFFER_SIZE>* writer) {
  writer->WriteTwo(static_cast<unsigned>(ymd.day()));
  writer->WriteChar('-');
  writer->WriteTwo(static_cast<unsigned>(ymd.month()));
  writer->WriteChar('-');
  writer->Write(static_cast<int>(ymd.year()));
}

template <typename Duration>
constexpr size_t BufferSizeHH_MM_SS() {
  return detail::Digits10(23) + 1 + detail::Digits10(59) + 1 + detail::Digits10(59) + 1 +
         detail::Digits10(Duration::period::den) - 1;
}

template <typename Duration, size_t BUFFER_SIZE>
void FormatHH_MM_SS(arrow_vendored::date::hh_mm_ss<Duration> hms,
                    DigitStringWriter<BUFFER_SIZE>* writer) {
  constexpr size_t subsecond_digits = Digits10(Duration::period::den) - 1;
  if (subsecond_digits != 0) {
    writer->WritePadded(hms.subseconds().count(), subsecond_digits, '0');
    writer->WriteChar('.');
  }
  writer->WriteTwo(hms.seconds().count());
  writer->WriteChar(':');
  writer->WriteTwo(hms.minutes().count());
  writer->WriteChar(':');
  writer->WriteTwo(hms.hours().count());
}

}  // namespace detail

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

    detail::DigitStringWriter<detail::BufferSizeYYYY_MM_DD<value_type>()> writer;

    arrow_vendored::date::year_month_day ymd{timepoint_days};
    detail::FormatYYYY_MM_DD(ymd, &writer);

    return append(writer.view());
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

    detail::DigitStringWriter<detail::BufferSizeHH_MM_SS<Duration>()> writer;

    arrow_vendored::date::hh_mm_ss<Duration> hms =
        arrow_vendored::date::make_time(since_midnight);
    detail::FormatHH_MM_SS(hms, &writer);

    return append(writer.view());
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

    detail::DigitStringWriter<detail::BufferSizeYYYY_MM_DD<value_type>() + 1 +
                              detail::BufferSizeHH_MM_SS<Duration>()>
        writer;

    arrow_vendored::date::hh_mm_ss<Duration> hms =
        arrow_vendored::date::make_time(since_midnight);
    detail::FormatHH_MM_SS(hms, &writer);

    writer.WriteChar(' ');

    arrow_vendored::date::year_month_day ymd{timepoint_days};
    detail::FormatYYYY_MM_DD(ymd, &writer);

    return append(writer.view());
  }

  template <typename Appender>
  Return<Appender> operator()(value_type value, Appender&& append) {
    return util::VisitDuration(unit_, *this, value, std::forward<Appender>(append));
  }

 private:
  TimeUnit::type unit_;
};

template <>
class StringFormatter<DurationType> : public IntToStringFormatterMixin<DurationType> {
  using IntToStringFormatterMixin::IntToStringFormatterMixin;
};

}  // namespace internal
}  // namespace arrow
