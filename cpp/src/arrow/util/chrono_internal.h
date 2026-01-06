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
#include <string>
#include <string_view>

// Feature detection for C++20 chrono timezone support
// https://en.cppreference.com/w/cpp/compiler_support/20.html#cpp_lib_chrono_201907L
//
// On Windows with MSVC: std::chrono uses Windows' internal timezone database,
// eliminating the need for users to install IANA tzdata separately.
//
// On Windows with MinGW/GCC: libstdc++ reads tzdata files via TZDIR env var.
// The tzdata files must be provided (e.g., via the tzdb R package).
//
// On non-Windows: GCC libstdc++ has a bug where DST state is incorrectly reset when
// a timezone transitions between rule sets (e.g., Australia/Broken_Hill around
// 2000-02-29). Until this is fixed, we use the vendored date.h library.
// See: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=116110

#if defined(_WIN32) && defined(__cpp_lib_chrono) && __cpp_lib_chrono >= 201907L
#  define ARROW_USE_STD_CHRONO 1
#else
#  define ARROW_USE_STD_CHRONO 0
#endif

#if ARROW_USE_STD_CHRONO
// Use C++20 standard library chrono
#  include <format>
#  include <iterator>
#  include <ostream>
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

// Formatting support - streams directly using C++20 std::vformat_to
// Provides: direct streaming, stream state preservation, chaining, rich format specifiers
template <typename CharT, typename Traits, typename Duration, typename TimeZonePtr>
std::basic_ostream<CharT, Traits>& to_stream(
    std::basic_ostream<CharT, Traits>& os, const CharT* fmt,
    const std::chrono::zoned_time<Duration, TimeZonePtr>& zt) {
  std::vformat_to(std::ostreambuf_iterator<CharT>(os), std::string("{:") + fmt + "}",
                  std::make_format_args(zt));
  return os;
}

// Format a duration using strftime-like format specifiers
// Converts "%H%M" style to C++20's "{:%H%M}" style and uses std::vformat
template <typename Duration>
std::string format(const char* fmt, const Duration& d) {
  return std::vformat(std::string("{:") + fmt + "}", std::make_format_args(d));
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
