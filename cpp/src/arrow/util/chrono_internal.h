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

/// \file chrono_internal.h
/// \brief Abstraction layer for C++20 chrono calendar/timezone APIs
///
/// This header provides a unified interface for chrono calendar and timezone
/// functionality. It uses std::chrono with supported C++20 timezone
/// implementations, otherwise falling back to the vendored Howard Hinnant
/// date library.
/// See chrono_config_internal.h for backend selection.
///
/// On Windows with MSVC, std::chrono uses the system timezone database,
/// eliminating the need for users to install IANA tzdata separately.

#include <chrono>
#include <string>
#include <string_view>

#include "arrow/util/chrono_config_internal.h"

#if ARROW_USE_STD_CHRONO
// Use C++20 standard library chrono
#  include <format>
#  include <locale>
#  include <ostream>
#  include <ratio>
#  include <type_traits>
#else
// Use vendored Howard Hinnant date library
#  include "arrow/vendored/datetime.h"
#endif

namespace arrow::internal::chrono {

#if ARROW_USE_STD_CHRONO

// ============================================================================
// C++20 std::chrono backend
// ============================================================================

// Duration types
using days = std::chrono::days;
using weeks = std::chrono::weeks;
using months = std::chrono::months;
using years = std::chrono::years;

// Time point types
template <typename Duration>
using sys_time = std::chrono::sys_time<Duration>;
using sys_days = std::chrono::sys_days;
using sys_seconds = std::chrono::sys_seconds;

template <typename Duration>
using local_time = std::chrono::local_time<Duration>;
using local_days = std::chrono::local_days;
using local_seconds = std::chrono::local_seconds;

// Calendar types
using year = std::chrono::year;
using month = std::chrono::month;
using day = std::chrono::day;
using weekday = std::chrono::weekday;
using year_month_day = std::chrono::year_month_day;
using year_month_weekday = std::chrono::year_month_weekday;

template <typename Duration>
using hh_mm_ss = std::chrono::hh_mm_ss<Duration>;

// Timezone types
using time_zone = std::chrono::time_zone;
using sys_info = std::chrono::sys_info;
using local_info = std::chrono::local_info;
using choose = std::chrono::choose;

template <typename Duration, typename TimeZonePtr = const time_zone*>
using zoned_time = std::chrono::zoned_time<Duration, TimeZonePtr>;

template <typename TimeZonePtr>
using zoned_traits = std::chrono::zoned_traits<TimeZonePtr>;

// Exceptions
using nonexistent_local_time = std::chrono::nonexistent_local_time;
using ambiguous_local_time = std::chrono::ambiguous_local_time;

// Weekday constants
using std::chrono::Monday;
using std::chrono::Sunday;

// Rounding functions
using std::chrono::ceil;
using std::chrono::floor;
using std::chrono::round;

// trunc (truncation toward zero) is not in std::chrono, only floor/ceil/round
template <typename ToDuration, typename Rep, typename Period>
constexpr ToDuration trunc(const std::chrono::duration<Rep, Period>& d) {
  auto floored = std::chrono::floor<ToDuration>(d);
  // floor rounds toward -infinity; for negative values with remainder, add 1 to get
  // toward zero
  if (d.count() < 0 && (d - floored).count() != 0) {
    return floored + ToDuration{1};
  }
  return floored;
}

// Timezone lookup
inline const time_zone* locate_zone(std::string_view tz_name) {
  return std::chrono::locate_zone(tz_name);
}

inline const time_zone* current_zone() { return std::chrono::current_zone(); }

namespace detail {

// Argument positions passed to std::vformat by to_stream below.
enum class FormatArgument : char {
  ZonedTime = '0',
  TimeOfDay = '1',
  TimeOfDayCount = '2',
};

template <typename CharT>
void AppendEscapedLiteral(std::basic_string<CharT>* out, CharT value) {
  out->push_back(value);
  if (value == CharT{'{'} || value == CharT{'}'}) {
    out->push_back(value);
  }
}

// These are the directives accepted by Arrow's existing strftime syntax. Treat
// all others as literals to preserve compatibility.
template <typename CharT>
bool IsSupportedStrftimeSpecifier(CharT modifier, CharT specifier) {
  const auto contains = [specifier](const char* candidates) {
    for (; *candidates != '\0'; ++candidates) {
      if (specifier == static_cast<CharT>(*candidates)) return true;
    }
    return false;
  };
  if (modifier == CharT{}) {
    return contains("aAbBhcCxdeDFgGHIjmMprRSTuUVWwXyYzZ");
  }
  if (modifier == CharT{'E'}) {
    return contains("cCxXyYz");
  }
  if (modifier == CharT{'O'}) {
    return contains("deHImMSuUVwWyz");
  }
  return false;
}

template <typename CharT>
void AppendChronoField(std::basic_string<CharT>* out, FormatArgument argument,
                       CharT specifier, CharT modifier = CharT{}) {
  *out += {CharT{'{'}, static_cast<CharT>(argument), CharT{':'}, CharT{'L'}, CharT{'%'}};
  if (modifier != CharT{}) out->push_back(modifier);
  *out += {specifier, CharT{'}'}};
}

template <typename CharT>
void AppendLocalizedField(std::basic_string<CharT>* out, FormatArgument argument) {
  *out += {CharT{'{'}, static_cast<CharT>(argument), CharT{':'}, CharT{'L'}, CharT{'}'}};
}

template <typename CharT>
std::basic_string<CharT> ToChronoFormat(const CharT* fmt, bool use_microseconds_suffix) {
  std::basic_string<CharT> out;
  while (*fmt != CharT{}) {
    if (*fmt != CharT{'%'}) {
      AppendEscapedLiteral(&out, *fmt++);
      continue;
    }

    ++fmt;
    if (*fmt == CharT{}) {
      AppendEscapedLiteral(&out, CharT{'%'});
      break;
    }

    CharT modifier{};
    if (*fmt == CharT{'E'} || *fmt == CharT{'O'}) {
      modifier = *fmt++;
      if (*fmt == CharT{}) {
        AppendEscapedLiteral(&out, CharT{'%'});
        AppendEscapedLiteral(&out, modifier);
        break;
      }
    }
    const CharT specifier = *fmt++;

    if (modifier == CharT{}) {
      switch (specifier) {
        case CharT{'%'}:
          AppendEscapedLiteral(&out, CharT{'%'});
          continue;
        case CharT{'n'}:
          AppendEscapedLiteral(&out, CharT{'\n'});
          continue;
        case CharT{'t'}:
          AppendEscapedLiteral(&out, CharT{'\t'});
          continue;
        case CharT{'Q'}:
          // Formatting a duration's %Q does not consistently apply the numeric locale.
          AppendLocalizedField(&out, FormatArgument::TimeOfDayCount);
          continue;
        case CharT{'q'}:
          if (use_microseconds_suffix) {
            // Some standard libraries use "us"; Arrow uses the micro sign.
            if constexpr (std::is_same_v<CharT, char>) {
              AppendEscapedLiteral(&out, CharT{'\xC2'});
              AppendEscapedLiteral(&out, CharT{'\xB5'});
            } else {
              AppendEscapedLiteral(&out, static_cast<CharT>(0xB5));
            }
            AppendEscapedLiteral(&out, CharT{'s'});
          } else {
            AppendChronoField(&out, FormatArgument::TimeOfDay, specifier);
          }
          continue;
        default:
          break;
      }
    }

#  if defined(__GLIBCXX__)
    if (modifier == CharT{'O'} && specifier == CharT{'V'}) {
      // libstdc++ does not yet accept %OV; use its equivalent base representation.
      AppendChronoField(&out, FormatArgument::ZonedTime, specifier);
      continue;
    }
#  endif

    if (IsSupportedStrftimeSpecifier(modifier, specifier)) {
      AppendChronoField(&out, FormatArgument::ZonedTime, specifier, modifier);
    } else {
      AppendEscapedLiteral(&out, CharT{'%'});
      if (modifier != CharT{}) AppendEscapedLiteral(&out, modifier);
      AppendEscapedLiteral(&out, specifier);
    }
  }
  return out;
}

}  // namespace detail

// Convert Arrow's strftime syntax to C++20 replacement fields. Literal braces and
// unsupported directives remain literal, and %Q/%q use local time of day.
template <typename CharT, typename Traits, typename Duration, typename TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os, const CharT* fmt,
    const std::chrono::zoned_time<Duration, TimeZonePtr>& zt) {
  static_assert(std::is_same_v<CharT, char> || std::is_same_v<CharT, wchar_t>);
  using Precision = typename std::chrono::zoned_time<Duration, TimeZonePtr>::duration;
  const auto standard_format = detail::ToChronoFormat(
      fmt, std::ratio_equal_v<typename Precision::period, std::micro>);
  const auto local_time = zt.get_local_time();
  const auto local_day = std::chrono::floor<std::chrono::days>(local_time);
  const auto time_of_day = local_time - local_day;
  const auto time_of_day_count = time_of_day.count();

  std::basic_string<CharT> formatted;
  if constexpr (std::is_same_v<CharT, char>) {
    formatted = std::vformat(os.getloc(), standard_format,
                             std::make_format_args(zt, time_of_day, time_of_day_count));
  } else {
    formatted = std::vformat(os.getloc(), standard_format,
                             std::make_wformat_args(zt, time_of_day, time_of_day_count));
  }
  os.write(formatted.data(), static_cast<std::streamsize>(formatted.size()));
  return os;
}

// Format a duration or time point using strftime-like format specifiers.
// Converts "%H%M" style to C++20's "{:L%H%M}" style and uses std::vformat.
template <typename Temporal>
std::string format(const char* fmt, const Temporal& value) {
  return std::vformat(std::locale{}, std::string("{:L") + fmt + "}",
                      std::make_format_args(value));
}

inline constexpr std::chrono::month jan = std::chrono::January;
inline constexpr std::chrono::month dec = std::chrono::December;

inline constexpr std::chrono::weekday sun = std::chrono::Sunday;
inline constexpr std::chrono::weekday mon = std::chrono::Monday;
inline constexpr std::chrono::weekday wed = std::chrono::Wednesday;
inline constexpr std::chrono::weekday thu = std::chrono::Thursday;

inline constexpr std::chrono::last_spec last = std::chrono::last;

#else  // !ARROW_USE_STD_CHRONO

// ============================================================================
// Vendored Howard Hinnant date library backend
// ============================================================================

namespace vendored = arrow_vendored::date;

// Duration types
using days = vendored::days;
using weeks = vendored::weeks;
using months = vendored::months;
using years = vendored::years;

// Time point types
template <typename Duration>
using sys_time = vendored::sys_time<Duration>;
using sys_days = vendored::sys_days;
using sys_seconds = vendored::sys_seconds;

template <typename Duration>
using local_time = vendored::local_time<Duration>;
using local_days = vendored::local_days;
using local_seconds = vendored::local_seconds;

// Calendar types
using year = vendored::year;
using month = vendored::month;
using day = vendored::day;
using weekday = vendored::weekday;
using year_month_day = vendored::year_month_day;
using year_month_weekday = vendored::year_month_weekday;

template <typename Duration>
using hh_mm_ss = vendored::hh_mm_ss<Duration>;

// Timezone types
using time_zone = vendored::time_zone;
using sys_info = vendored::sys_info;
using local_info = vendored::local_info;
using choose = vendored::choose;

template <typename Duration, typename TimeZonePtr = const time_zone*>
using zoned_time = vendored::zoned_time<Duration, TimeZonePtr>;

template <typename TimeZonePtr>
using zoned_traits = vendored::zoned_traits<TimeZonePtr>;

// Exceptions
using nonexistent_local_time = vendored::nonexistent_local_time;
using ambiguous_local_time = vendored::ambiguous_local_time;

// Weekday constants
inline constexpr vendored::weekday Monday = vendored::Monday;
inline constexpr vendored::weekday Sunday = vendored::Sunday;

// Rounding functions
using vendored::ceil;
using vendored::floor;
using vendored::round;
using vendored::trunc;

// Timezone lookup
inline const time_zone* locate_zone(std::string_view tz_name) {
  return vendored::locate_zone(std::string(tz_name));
}

inline const time_zone* current_zone() { return vendored::current_zone(); }

#  if !ARROW_CHRONO_USE_OS_TZDB
using vendored::reload_tzdb;
using vendored::set_install;
#  endif

// Formatting support
using vendored::format;

template <typename CharT, typename Traits, typename Duration, typename TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os, const CharT* fmt,
    const vendored::zoned_time<Duration, TimeZonePtr>& zt) {
  return vendored::to_stream(os, fmt, zt);
}

inline constexpr vendored::month jan = vendored::jan;
inline constexpr vendored::month dec = vendored::dec;

inline constexpr vendored::weekday sun = vendored::sun;
inline constexpr vendored::weekday mon = vendored::mon;
inline constexpr vendored::weekday wed = vendored::wed;
inline constexpr vendored::weekday thu = vendored::thu;

inline constexpr vendored::last_spec last = vendored::last;

#endif  // ARROW_USE_STD_CHRONO

}  // namespace arrow::internal::chrono
