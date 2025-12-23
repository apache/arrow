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
/// functionality. On compilers with full C++20 chrono support, it uses
/// std::chrono. On other compilers, it falls back to the vendored Howard Hinnant
/// date library.
///
/// The main benefit is on Windows where std::chrono uses the system timezone
/// database, eliminating the need for users to install IANA tzdata separately.

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

// Feature detection for C++20 chrono timezone support
// We only enable for compilers with FULL support (not partial)
// https://en.cppreference.com/w/cpp/compiler_support/20.html#cpp_lib_chrono_201907L
//
// MSVC 19.29+ (VS16.10+): Full C++20 chrono support, uses Windows internal TZ database.
// GCC libstdc++ has a bug where DST state is incorrectly reset when a timezone
// transitions between rule sets in tzdata.zi (e.g., Australia/Broken_Hill around
// 2000-02-29 23:23:24).
// Until this is fixed, we use the vendored date.h library for GCC.

#if defined(_MSC_VER) && defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
#  define ARROW_USE_STD_CHRONO 1
#else
#  define ARROW_USE_STD_CHRONO 0
#endif

#if ARROW_USE_STD_CHRONO
// Use C++20 standard library chrono
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
inline constexpr std::chrono::weekday Monday{1};
inline constexpr std::chrono::weekday Sunday{0};

// Rounding functions
using std::chrono::ceil;
using std::chrono::floor;
using std::chrono::round;

// trunc is not in std::chrono - implement proper truncation toward zero
// floor rounds toward negative infinity, but trunc rounds toward zero
template <typename ToDuration, typename Rep, typename Period>
constexpr ToDuration trunc(const std::chrono::duration<Rep, Period>& d) {
  auto floor_result = std::chrono::floor<ToDuration>(d);
  auto remainder = d - floor_result;
  // If original was negative and there's a non-zero remainder,
  // floor went too far negative, so add one unit back
  if (d.count() < 0 && remainder.count() != 0) {
    return floor_result + ToDuration{1};
  }
  return floor_result;
}

// Timezone lookup
inline const time_zone* locate_zone(std::string_view tz_name) {
  return std::chrono::locate_zone(tz_name);
}

inline const time_zone* current_zone() { return std::chrono::current_zone(); }

// Helper to get subsecond decimal places based on duration period
template <typename Duration>
constexpr int get_subsecond_decimals() {
  using Period = typename Duration::period;
  if constexpr (Period::den == 1000)
    return 3;  // milliseconds
  else if constexpr (Period::den == 1000000)
    return 6;  // microseconds
  else if constexpr (Period::den == 1000000000)
    return 9;  // nanoseconds
  else
    return 0;  // seconds or coarser
}

// Formatting support with subsecond precision and timezone handling
// Mimics the vendored date library's to_stream behavior for compatibility
template <typename CharT, typename Traits, typename Duration, typename TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os, const CharT* fmt,
    const std::chrono::zoned_time<Duration, TimeZonePtr>& zt) {
  // Get local time and timezone info
  auto lt = zt.get_local_time();
  auto info = zt.get_info();

  auto lt_days = std::chrono::floor<days>(lt);
  auto ymd = year_month_day{lt_days};

  // Calculate time of day components
  auto time_since_midnight = lt - local_time<days>{lt_days};
  auto total_secs = std::chrono::duration_cast<std::chrono::seconds>(time_since_midnight);
  auto h = std::chrono::duration_cast<std::chrono::hours>(time_since_midnight);
  auto m = std::chrono::duration_cast<std::chrono::minutes>(time_since_midnight - h);
  auto s = std::chrono::duration_cast<std::chrono::seconds>(time_since_midnight - h - m);

  // Build std::tm for strftime
  std::tm tm{};
  tm.tm_sec = static_cast<int>(s.count());
  tm.tm_min = static_cast<int>(m.count());
  tm.tm_hour = static_cast<int>(h.count());
  tm.tm_mday = static_cast<int>(static_cast<unsigned>(ymd.day()));
  tm.tm_mon = static_cast<int>(static_cast<unsigned>(ymd.month())) - 1;
  tm.tm_year = static_cast<int>(ymd.year()) - 1900;

  auto wd = weekday{lt_days};
  tm.tm_wday = static_cast<int>(wd.c_encoding());

  auto year_start =
      std::chrono::local_days{ymd.year() / std::chrono::January / std::chrono::day{1}};
  tm.tm_yday = static_cast<int>((lt_days - year_start).count());
  tm.tm_isdst = info.save != std::chrono::minutes{0} ? 1 : 0;

  // Timezone offset calculation
  auto offset_mins = std::chrono::duration_cast<std::chrono::minutes>(info.offset);
  bool neg_offset = offset_mins.count() < 0;
  auto abs_offset = neg_offset ? -offset_mins : offset_mins;
  auto off_h = std::chrono::duration_cast<std::chrono::hours>(abs_offset);
  auto off_m = abs_offset - off_h;

  // Calculate subsecond value
  constexpr int decimals = get_subsecond_decimals<Duration>();
  int64_t subsec_value = 0;
  if constexpr (decimals > 0) {
    auto subsec_duration = time_since_midnight - total_secs;
    subsec_value = std::chrono::duration_cast<Duration>(subsec_duration).count();
    if (subsec_value < 0) subsec_value = -subsec_value;
  }

  // Parse format string, handle %S, %z, %Z specially
  std::string result;
  for (const CharT* p = fmt; *p; ++p) {
    if (*p == '%' && *(p + 1)) {
      CharT spec = *(p + 1);
      if (spec == 'S') {
        // %S with subsecond precision
        result += (tm.tm_sec < 10 ? "0" : "") + std::to_string(tm.tm_sec);
        if constexpr (decimals > 0) {
          std::ostringstream ss;
          ss << '.' << std::setfill('0') << std::setw(decimals) << subsec_value;
          result += ss.str();
        }
        ++p;
      } else if (spec == 'z') {
        // %z timezone offset
        std::ostringstream ss;
        ss << (neg_offset ? '-' : '+') << std::setfill('0') << std::setw(2)
           << off_h.count() << std::setfill('0') << std::setw(2) << off_m.count();
        result += ss.str();
        ++p;
      } else if (spec == 'Z') {
        // %Z timezone abbreviation
        result += info.abbrev;
        ++p;
      } else {
        // Use strftime for other specifiers
        char buf[64];
        char small_fmt[3] = {'%', static_cast<char>(spec), '\0'};
        if (std::strftime(buf, sizeof(buf), small_fmt, &tm) > 0) {
          result += buf;
        }
        ++p;
      }
    } else {
      result += static_cast<char>(*p);
    }
  }

  return os << result;
}

template <typename Duration>
std::string format(const char* fmt, const Duration& d) {
  std::ostringstream ss;
  auto total_minutes = std::chrono::duration_cast<std::chrono::minutes>(d).count();
  bool negative = total_minutes < 0;
  if (negative) total_minutes = -total_minutes;
  auto hours = total_minutes / 60;
  auto mins = total_minutes % 60;
  ss << (negative ? "-" : "+");
  ss << std::setfill('0') << std::setw(2) << hours;
  ss << std::setfill('0') << std::setw(2) << mins;
  return ss.str();
}

// Literals namespace
namespace literals {
// Month literals
inline constexpr std::chrono::month jan = std::chrono::January;
inline constexpr std::chrono::month dec = std::chrono::December;

// Weekday literals
inline constexpr std::chrono::weekday sun = std::chrono::Sunday;
inline constexpr std::chrono::weekday mon = std::chrono::Monday;
inline constexpr std::chrono::weekday wed = std::chrono::Wednesday;
inline constexpr std::chrono::weekday thu = std::chrono::Thursday;

// last specifier
inline constexpr std::chrono::last_spec last = std::chrono::last;
}  // namespace literals

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

// Formatting support
using vendored::format;

template <typename CharT, typename Traits, typename Duration, typename TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os, const CharT* fmt,
    const vendored::zoned_time<Duration, TimeZonePtr>& zt) {
  return vendored::to_stream(os, fmt, zt);
}

// Literals namespace
namespace literals {
inline constexpr vendored::month jan = vendored::jan;
inline constexpr vendored::month dec = vendored::dec;

inline constexpr vendored::weekday sun = vendored::sun;
inline constexpr vendored::weekday mon = vendored::mon;
inline constexpr vendored::weekday wed = vendored::wed;
inline constexpr vendored::weekday thu = vendored::thu;

inline constexpr vendored::last_spec last = vendored::last;
}  // namespace literals

#endif  // ARROW_USE_STD_CHRONO

}  // namespace arrow::internal::chrono
