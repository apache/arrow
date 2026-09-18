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

#include <chrono>
#include <cstdint>
#include <locale>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/util/date_internal.h"
#include "arrow/util/value_parsing.h"

#if ARROW_USE_STD_CHRONO
#  include <format>
#  include <iterator>
#  include <ratio>
#endif

namespace arrow::compute::internal {

namespace chrono = arrow::internal::chrono;

using arrow::internal::checked_cast;
using arrow::internal::OffsetZone;
using chrono::choose;
using chrono::days;
using chrono::floor;
using chrono::local_days;
using chrono::local_time;
using chrono::locate_zone;
using chrono::sys_days;
using chrono::sys_time;
using chrono::time_zone;
using chrono::year_month_day;
using chrono::zoned_time;
using std::chrono::duration_cast;

// https://howardhinnant.github.io/date/tz.html#Examples
using ArrowTimeZone = std::variant<const time_zone*, OffsetZone>;

template <class Duration, class Func>
auto ApplyTimeZone(const ArrowTimeZone& tz, sys_time<Duration> st,
                   Func&& func) -> decltype(func(zoned_time<Duration>{})) {
  return std::visit(
      [&](auto&& zone) {
        if constexpr (std::is_pointer_v<std::decay_t<decltype(zone)> >) {
          return func(zoned_time<Duration>{zone, st});
        } else {
          return func(zoned_time<Duration, const OffsetZone*>{&zone, st});
        }
      },
      tz);
}

template <class Duration, class Func>
auto ApplyTimeZone(const ArrowTimeZone& tz, local_time<Duration> lt,
                   std::optional<choose> c,
                   Func&& func) -> decltype(func(zoned_time<Duration>{})) {
  return std::visit(
      [&](auto&& zone) {
        if constexpr (std::is_pointer_v<std::decay_t<decltype(zone)> >) {
          return c.has_value() ? func(zoned_time<Duration>{zone, lt, c.value()})
                               : func(zoned_time<Duration>{zone, lt});
        } else {
          // Offset zone conversion to/from UTC is always unambiguous
          // therefore `c` can be ignored.
          return func(zoned_time<Duration, const OffsetZone*>{&zone, lt});
        }
      },
      tz);
}

inline int64_t GetQuarter(const year_month_day& ymd) {
  return static_cast<int64_t>((static_cast<uint32_t>(ymd.month()) - 1) / 3);
}

ARROW_EXPORT Result<ArrowTimeZone> LocateZone(const std::string_view timezone);

static inline const std::string& GetInputTimezone(const DataType& type) {
  static const std::string no_timezone = "";
  switch (type.id()) {
    case Type::TIMESTAMP:
      return checked_cast<const TimestampType&>(type).timezone();
    default:
      return no_timezone;
  }
}

static inline Status ValidateDayOfWeekOptions(const DayOfWeekOptions& options) {
  if (options.week_start < 1 || 7 < options.week_start) {
    return Status::Invalid(
        "week_start must follow ISO convention (Monday=1, Sunday=7). Got week_start=",
        options.week_start);
  }
  return Status::OK();
}

static inline Result<std::locale> GetLocale(const std::string& locale) {
  try {
    return std::locale(locale.c_str());
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot find locale '", locale, "': ", ex.what());
  }
}

struct NonZonedLocalizer {
  using days_t = sys_days;

  // No-op conversions: UTC -> UTC
  template <typename Duration>
  sys_time<Duration> ConvertTimePoint(int64_t t) const {
    return sys_time<Duration>(Duration{t});
  }

  template <typename Duration>
  Duration ConvertLocalToSys(Duration t, Status* st) const {
    return t;
  }

  sys_days ConvertDays(sys_days d) const { return d; }
};

struct ZonedLocalizer {
  using days_t = local_days;

  // Timezone-localizing conversions: UTC -> local time
  const ArrowTimeZone tz_;

  template <typename Duration>
  local_time<Duration> ConvertTimePoint(int64_t t) const {
    const auto st = sys_time<Duration>(Duration{t});
    return std::visit(
        [st](const auto& tz) -> local_time<Duration> { return tz->to_local(st); }, tz_);
  }

  template <typename Duration>
  Duration ConvertLocalToSys(Duration t, Status* st) const {
    const auto lt = local_time<Duration>(t);
    auto local_to_sys_time = [&](auto&& t) {
      return t.get_sys_time().time_since_epoch();
    };

    try {
      return ApplyTimeZone(tz_, lt, std::nullopt, local_to_sys_time);
    } catch (const chrono::nonexistent_local_time& e) {
      *st = Status::Invalid("Local time does not exist: ", e.what());
      return Duration{0};
    } catch (const chrono::ambiguous_local_time& e) {
      *st = Status::Invalid("Local time is ambiguous: ", e.what());
      return Duration{0};
    }
  }

  local_days ConvertDays(sys_days d) const { return local_days(year_month_day(d)); }
};

#if ARROW_USE_STD_CHRONO
namespace detail {

// Argument positions passed to std::vformat_to by TimestampFormatter below.
enum class FormatArgument : char {
  ZonedTime = '0',
  TimeOfDay = '1',
  TimeOfDayCount = '2',
};

inline void AppendEscapedLiteral(std::string* out, char value) {
  out->push_back(value);
  if (value == '{' || value == '}') {
    out->push_back(value);
  }
}

// These are the directives accepted by Arrow's existing strftime syntax. Treat
// all others as literals to preserve compatibility.
inline bool IsSupportedStrftimeSpecifier(char modifier, char specifier) {
  const auto contains = [specifier](std::string_view candidates) {
    return candidates.find(specifier) != std::string_view::npos;
  };
  if (modifier == '\0') {
    return contains("aAbBhcCxdeDFgGHIjmMprRSTuUVWwXyYzZ");
  }
  if (modifier == 'E') {
    return contains("cCxXyYz");
  }
  if (modifier == 'O') {
    return contains("deHImMSuUVwWyz");
  }
  return false;
}

inline void AppendChronoField(std::string* out, FormatArgument argument, char specifier,
                              char modifier = '\0') {
  *out += {'{', static_cast<char>(argument), ':', 'L', '%'};
  if (modifier != '\0') out->push_back(modifier);
  *out += {specifier, '}'};
}

inline void AppendLocalizedField(std::string* out, FormatArgument argument) {
  *out += {'{', static_cast<char>(argument), ':', 'L', '}'};
}

inline std::string ToChronoFormat(const char* fmt, bool use_microseconds_suffix) {
  std::string out;
  bool zoned_field_open = false;
  const auto close_zoned_field = [&] {
    if (zoned_field_open) {
      out.push_back('}');
      zoned_field_open = false;
    }
  };
  const auto append_literal = [&](char value) {
    // Braces and literal percent signs must stay outside chrono replacement fields.
    if (value == '{' || value == '}' || value == '%') close_zoned_field();
    AppendEscapedLiteral(&out, value);
  };
  const auto append_zoned_directive = [&](char specifier, char modifier = '\0') {
    // Keep compatible directives and intervening literals in one field to avoid
    // repeating timezone lookup and calendar decomposition for every directive.
    if (!zoned_field_open) {
      out += {'{', static_cast<char>(FormatArgument::ZonedTime), ':', 'L'};
      zoned_field_open = true;
    }
    out.push_back('%');
    if (modifier != '\0') out.push_back(modifier);
    out.push_back(specifier);
  };
  while (*fmt != '\0') {
    if (*fmt != '%') {
      append_literal(*fmt++);
      continue;
    }

    ++fmt;
    if (*fmt == '\0') {
      append_literal('%');
      break;
    }

    char modifier = '\0';
    if (*fmt == 'E' || *fmt == 'O') {
      modifier = *fmt++;
      if (*fmt == '\0') {
        append_literal('%');
        append_literal(modifier);
        break;
      }
    }
    const char specifier = *fmt++;

    if (modifier == '\0') {
      switch (specifier) {
        case '%':
          append_literal('%');
          continue;
        case 'n':
          append_literal('\n');
          continue;
        case 't':
          append_literal('\t');
          continue;
        case 'Q':
          // Formatting a duration's %Q does not consistently apply the numeric locale.
          close_zoned_field();
          AppendLocalizedField(&out, FormatArgument::TimeOfDayCount);
          continue;
        case 'q':
          close_zoned_field();
          if (use_microseconds_suffix) {
            // Some standard libraries use "us"; Arrow uses the micro sign.
            out += "\xC2\xB5s";
          } else {
            AppendChronoField(&out, FormatArgument::TimeOfDay, specifier);
          }
          continue;
        default:
          break;
      }
    }

#  if defined(__GLIBCXX__)
    if (modifier == 'O' && specifier == 'V') {
      // libstdc++ does not yet accept %OV; use its equivalent base representation.
      append_zoned_directive(specifier);
      continue;
    }
#  endif

    if (IsSupportedStrftimeSpecifier(modifier, specifier)) {
      append_zoned_directive(specifier, modifier);
    } else {
      append_literal('%');
      if (modifier != '\0') append_literal(modifier);
      append_literal(specifier);
    }
  }
  close_zoned_field();
  return out;
}

}  // namespace detail
#endif

template <typename Duration>
struct TimestampFormatter {
  static std::string PrepareFormat(const std::string& format) {
#if ARROW_USE_STD_CHRONO
    // Translate strftime syntax once, not for every timestamp.
    using Precision = typename chrono::zoned_time<Duration>::duration;
    return detail::ToChronoFormat(
        format.c_str(), std::ratio_equal_v<typename Precision::period, std::micro>);
#else
    return format;
#endif
  }

  const std::string format;
  const ArrowTimeZone tz;
  std::ostringstream bufstream;

  explicit TimestampFormatter(const std::string& format, const ArrowTimeZone time_zone,
                              const std::locale& locale)
      : format(PrepareFormat(format)), tz(time_zone) {
    bufstream.imbue(locale);
    // Propagate errors as C++ exceptions (to get an actual error message)
    bufstream.exceptions(std::ios::failbit | std::ios::badbit);
  }

  Result<std::string> operator()(int64_t arg) {
    bufstream.str("");
    const auto timepoint = sys_time<Duration>(Duration{arg});
    auto format_zoned_time = [&](auto&& zt) {
      try {
#if ARROW_USE_STD_CHRONO
        // %Q/%q refer to local time of day rather than elapsed time since the epoch.
        const auto local_time = zt.get_local_time();
        const auto local_day = std::chrono::floor<std::chrono::days>(local_time);
        const auto time_of_day = local_time - local_day;
        const auto time_of_day_count = time_of_day.count();
        const std::ostream::sentry sentry(bufstream);
        if (sentry) {
          const auto end = std::vformat_to(
              std::ostreambuf_iterator<char>(bufstream), bufstream.getloc(), format,
              std::make_format_args(zt, time_of_day, time_of_day_count));
          if (end.failed()) bufstream.setstate(std::ios::badbit);
        }
#else
        arrow_vendored::date::to_stream(bufstream, format.c_str(), zt);
#endif
        return Status::OK();
      } catch (const std::runtime_error& ex) {
        bufstream.clear();
        return Status::Invalid("Failed formatting timestamp: ", ex.what());
      }
    };
    RETURN_NOT_OK(ApplyTimeZone(tz, timepoint, format_zoned_time));
    return std::move(bufstream).str();
  }
};

//
// Which types to generate a kernel for
//
struct WithDates {};
struct WithTimes {};
struct WithTimestamps {};
struct WithDurations {};
struct WithStringTypes {};

// This helper allows generating temporal kernels for selected type categories
// without any spurious code generation for other categories (e.g. avoid
// generating code for date kernels for a times-only function).
template <typename Factory>
void AddTemporalKernels(Factory* fac) {}

template <typename Factory, typename... WithOthers>
void AddTemporalKernels(Factory* fac, WithDates, WithOthers... others) {
  fac->template AddKernel<days, Date32Type>(date32());
  fac->template AddKernel<std::chrono::milliseconds, Date64Type>(date64());
  AddTemporalKernels(fac, std::forward<WithOthers>(others)...);
}

template <typename Factory, typename... WithOthers>
void AddTemporalKernels(Factory* fac, WithTimes, WithOthers... others) {
  fac->template AddKernel<std::chrono::seconds, Time32Type>(time32(TimeUnit::SECOND));
  fac->template AddKernel<std::chrono::milliseconds, Time32Type>(time32(TimeUnit::MILLI));
  fac->template AddKernel<std::chrono::microseconds, Time64Type>(time64(TimeUnit::MICRO));
  fac->template AddKernel<std::chrono::nanoseconds, Time64Type>(time64(TimeUnit::NANO));
  AddTemporalKernels(fac, std::forward<WithOthers>(others)...);
}

template <typename Factory, typename... WithOthers>
void AddTemporalKernels(Factory* fac, WithDurations, WithOthers... others) {
  fac->template AddKernel<std::chrono::seconds, DurationType>(duration(TimeUnit::SECOND));
  fac->template AddKernel<std::chrono::milliseconds, DurationType>(
      duration(TimeUnit::MILLI));
  fac->template AddKernel<std::chrono::microseconds, DurationType>(
      duration(TimeUnit::MICRO));
  fac->template AddKernel<std::chrono::nanoseconds, DurationType>(
      duration(TimeUnit::NANO));
  AddTemporalKernels(fac, std::forward<WithOthers>(others)...);
}

template <typename Factory, typename... WithOthers>
void AddTemporalKernels(Factory* fac, WithTimestamps, WithOthers... others) {
  fac->template AddKernel<std::chrono::seconds, TimestampType>(
      match::TimestampTypeUnit(TimeUnit::SECOND));
  fac->template AddKernel<std::chrono::milliseconds, TimestampType>(
      match::TimestampTypeUnit(TimeUnit::MILLI));
  fac->template AddKernel<std::chrono::microseconds, TimestampType>(
      match::TimestampTypeUnit(TimeUnit::MICRO));
  fac->template AddKernel<std::chrono::nanoseconds, TimestampType>(
      match::TimestampTypeUnit(TimeUnit::NANO));
  AddTemporalKernels(fac, std::forward<WithOthers>(others)...);
}

template <typename Factory, typename... WithOthers>
void AddTemporalKernels(Factory* fac, WithStringTypes, WithOthers... others) {
  fac->template AddKernel<TimestampType, StringType>(utf8());
  fac->template AddKernel<TimestampType, LargeStringType>(large_utf8());
  AddTemporalKernels(fac, std::forward<WithOthers>(others)...);
}

//
// Executor class for temporal component extractors, i.e. scalar kernels
// with the signature Timestamp -> <non-temporal scalar type `OutType`>
//
// The `Op` parameter is templated on the Duration (which depends on the timestamp
// unit) and a Localizer class (depending on whether the timestamp has a
// timezone defined).
//
template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType, typename... Args>
struct TemporalComponentExtractBase {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out, Args... args) {
    const auto& timezone = GetInputTimezone(*batch[0].type());
    if (timezone.empty()) {
      using ExecTemplate = Op<Duration, NonZonedLocalizer>;
      auto op = ExecTemplate(options, NonZonedLocalizer(), args...);
      applicator::ScalarUnaryNotNullStateful<OutType, InType, ExecTemplate> kernel{op};
      return kernel.Exec(ctx, batch, out);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
      using ExecTemplate = Op<Duration, ZonedLocalizer>;
      auto op = ExecTemplate(options, ZonedLocalizer{tz}, args...);
      applicator::ScalarUnaryNotNullStateful<OutType, InType, ExecTemplate> kernel{op};
      return kernel.Exec(ctx, batch, out);
    }
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, days, Date32Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<days, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Date32Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, std::chrono::milliseconds, Date64Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<std::chrono::milliseconds, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Date64Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, std::chrono::seconds, Time32Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<std::chrono::seconds, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Time32Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, std::chrono::milliseconds, Time32Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<std::chrono::milliseconds, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Time32Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, std::chrono::microseconds, Time64Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<std::chrono::microseconds, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Date64Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename OutType>
struct TemporalComponentExtractBase<Op, std::chrono::nanoseconds, Time64Type, OutType> {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecSpan& batch, ExecResult* out) {
    using ExecTemplate = Op<std::chrono::nanoseconds, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarUnaryNotNullStateful<OutType, Date64Type, ExecTemplate> kernel{op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType, typename... Args>
struct TemporalComponentExtract
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType, Args...> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType, Args...>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out,
                     Args... args) {
    const FunctionOptions* options = nullptr;
    return Base::ExecWithOptions(ctx, options, batch, out, args...);
  }
};

}  // namespace arrow::compute::internal
