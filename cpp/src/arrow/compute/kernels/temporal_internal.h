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

#include "arrow/compute/api_scalar.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

namespace compute {
namespace internal {

using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::local_days;
using arrow_vendored::date::local_info;
using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::sys_days;
using arrow_vendored::date::sys_info;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::time_zone;
using arrow_vendored::date::year_month_day;
using arrow_vendored::date::zoned_time;
using std::chrono::duration_cast;

inline int64_t GetQuarter(const year_month_day& ymd) {
  return static_cast<int64_t>((static_cast<uint32_t>(ymd.month()) - 1) / 3);
}

static inline Result<const time_zone*> LocateZone(const std::string& timezone) {
  try {
    return locate_zone(timezone);
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot locate timezone '", timezone, "': ", ex.what());
  }
}

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

template <typename Duration, typename Unit>
static inline Unit FloorHelper(const Duration t, const RoundTemporalOptions& options) {
  const Unit d = arrow_vendored::date::floor<Unit>(t);
  if (options.multiple == 1) {
    return d;
  } else {
    const Unit unit = Unit{options.multiple};
    return (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
  }
}

template <typename Duration, typename Unit>
static inline Unit CeilHelper(const Duration t, const RoundTemporalOptions& options) {
  const Unit d = arrow_vendored::date::ceil<Unit>(t);
  const Unit d2 = FloorHelper<Duration, Unit>(t, options);

  if (d2 < d || (options.ceil_is_strictly_greater && d2 == Duration{t})) {
    return d2 + Unit{options.multiple};
  }
  return d2;
}

// This function will return incorrect results for zoned time points when touching
// DST boundaries.
template <typename Duration, typename Unit>
static inline Unit RoundHelper(const Duration t, const RoundTemporalOptions& options) {
  if (options.multiple == 1) {
    return arrow_vendored::date::round<Unit>(t);
  } else {
    const Unit f = FloorHelper<Duration, Unit>(t, options);
    Unit c = f;
    if (options.ceil_is_strictly_greater && f == Duration{t}) {
      c += Unit{options.multiple};
    }
    return (t - f >= c - t) ? c : f;
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

  template <typename Duration>
  Duration OriginHelper(const Duration& d, const sys_time<Duration>& st,
                        const CalendarUnit& unit) const {
    Duration origin;
    switch (unit) {
      case compute::CalendarUnit::DAY: {
        const year_month_day ymd = year_month_day(floor<days>(st));
        origin = duration_cast<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::HOUR: {
        origin = duration_cast<Duration>(floor<days>(st).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::MINUTE: {
        origin =
            duration_cast<Duration>(floor<std::chrono::hours>(st).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::SECOND:
        origin =
            duration_cast<Duration>(floor<std::chrono::minutes>(st).time_since_epoch());
        break;
      case compute::CalendarUnit::MILLISECOND:
        origin = duration_cast<Duration>(floor<std::chrono::seconds>(d));
        break;
      case compute::CalendarUnit::MICROSECOND:
        origin = duration_cast<Duration>(floor<std::chrono::milliseconds>(d));
        break;
      case compute::CalendarUnit::NANOSECOND:
        origin = duration_cast<Duration>(floor<std::chrono::microseconds>(d));
        break;
      default:
        origin = d;
    }
    return origin;
  }

  template <typename Duration, typename Unit>
  Duration FloorTimePoint(int64_t t, const RoundTemporalOptions& options) const {
    const Duration d = Duration{t};
    if (options.calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      const Duration origin =
          OriginHelper(d, ConvertTimePoint<Duration>(t), options.unit);
      return duration_cast<Duration>(CeilHelper<Duration, Unit>((d - origin), options) +
                                     origin);
    } else {
      return duration_cast<Duration>(FloorHelper<Duration, Unit>(d, options));
    }
  }

  template <typename Duration, typename Unit>
  Duration CeilTimePoint(int64_t t, const RoundTemporalOptions& options) const {
    const Duration d = Duration{t};
    if (options.calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      const Duration origin =
          OriginHelper(d, ConvertTimePoint<Duration>(t), options.unit);
      return duration_cast<Duration>(CeilHelper<Duration, Unit>((d - origin), options) +
                                     origin);
    } else {
      return duration_cast<Duration>(CeilHelper<Duration, Unit>(d, options));
    }
  }

  template <typename Duration, typename Unit>
  Duration RoundTimePoint(const int64_t t, const RoundTemporalOptions& options) const {
    return duration_cast<Duration>(RoundHelper<Duration, Unit>(Duration{t}, options));
  }

  sys_days ConvertDays(sys_days d) const { return d; }
};

struct ZonedLocalizer {
  using days_t = local_days;

  // Timezone-localizing conversions: UTC -> local time
  const time_zone* tz;

  template <typename Duration>
  local_time<Duration> ConvertTimePoint(int64_t t) const {
    return tz->to_local(sys_time<Duration>(Duration{t}));
  }

  template <typename Duration>
  Duration OriginHelper(const Duration& d, const local_time<Duration>& lt,
                        const CalendarUnit& unit) const {
    Duration origin;
    switch (unit) {
      case compute::CalendarUnit::DAY: {
        const year_month_day ymd = year_month_day(floor<days>(lt));
        origin = duration_cast<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::HOUR: {
        origin = duration_cast<Duration>(floor<days>(lt).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::MINUTE: {
        origin =
            duration_cast<Duration>(floor<std::chrono::hours>(lt).time_since_epoch());
        break;
      }
      case compute::CalendarUnit::SECOND:
        origin =
            duration_cast<Duration>(floor<std::chrono::minutes>(lt).time_since_epoch());
        break;
      case compute::CalendarUnit::MILLISECOND:
        origin = duration_cast<Duration>(floor<std::chrono::seconds>(d));
        break;
      case compute::CalendarUnit::MICROSECOND:
        origin = duration_cast<Duration>(floor<std::chrono::milliseconds>(d));
        break;
      case compute::CalendarUnit::NANOSECOND:
        origin = duration_cast<Duration>(floor<std::chrono::microseconds>(d));
        break;
      default:
        origin = d;
    }
    return origin;
  }

  template <typename Duration, typename Unit>
  Duration FloorTimePoint(const int64_t t, const RoundTemporalOptions& options) const {
    const Duration d = Duration{t};
    const sys_time<Duration> st = sys_time<Duration>(d);
    const local_time<Duration> lt = tz->to_local(st);
    const sys_info si = tz->get_info(st);
    const local_info li = tz->get_info(lt);

    Duration d2;
    if (options.calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      const Duration origin = OriginHelper(d, lt, options.unit);
      d2 = duration_cast<Duration>(
          FloorHelper<Duration, Unit>((lt - origin).time_since_epoch(), options) +
          origin);
    } else {
      d2 = duration_cast<Duration>(
          FloorHelper<Duration, Unit>(lt.time_since_epoch(), options));
    }
    const local_info li2 = tz->get_info(local_time<Duration>(d2));

    if (li2.result == local_info::ambiguous && li.result == local_info::ambiguous) {
      // In case we floor from an ambiguous period into an ambiguous period we need to
      // decide how to disambiguate the result. We resolve this by adding post-ambiguous
      // period offset to UTC, floor this time and subtract the post-ambiguous period
      // offset to get the locally floored time. Please note pre-ambiguous offset is
      // typically 1 hour greater than post-ambiguous offset. While this produces
      // acceptable result in UTC it can cause discontinuities in local time and destroys
      // local time sortedness.
      return duration_cast<Duration>(
          FloorHelper<Duration, Unit>(d + li2.second.offset, options) -
          li2.second.offset);
    } else if (li2.result == local_info::nonexistent ||
               li2.first.offset < li.first.offset) {
      // In case we hit or cross a nonexistent period we add the pre-DST-jump offset to
      // UTC, floor this time and subtract the pre-DST-jump offset from the floored time.
      return duration_cast<Duration>(
          FloorHelper<Duration, Unit>(d + li2.first.offset, options) - li2.first.offset);
    }
    return duration_cast<Duration>(d2 - si.offset);
  }

  template <typename Duration, typename Unit>
  Duration CeilTimePoint(const int64_t t, const RoundTemporalOptions& options) const {
    const Duration d = Duration{t};
    const sys_time<Duration> st = sys_time<Duration>(d);
    const local_time<Duration> lt = tz->to_local(st);
    const sys_info si = tz->get_info(st);
    const local_info li = tz->get_info(lt);

    Duration d2;
    if (options.calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      const Duration origin = OriginHelper(d, lt, options.unit);
      d2 = duration_cast<Duration>(
          CeilHelper<Duration, Unit>((lt - origin).time_since_epoch(), options) + origin);
    } else {
      d2 = duration_cast<Duration>(
          CeilHelper<Duration, Unit>(lt.time_since_epoch(), options));
    }
    const local_info li2 = tz->get_info(local_time<Duration>(d2));

    if (li2.result == local_info::ambiguous && li.result == local_info::ambiguous) {
      // In case we ceil from an ambiguous period into an ambiguous period we need to
      // decide how to disambiguate the result. We resolve this by adding post-ambiguous
      // period offset to UTC, ceil this time and subtract the post-ambiguous period
      // offset to get the locally ceiled time. Please note pre-ambiguous offset is
      // typically 1 hour greater than post-ambiguous offset. While this produces
      // acceptable result in UTC it can cause discontinuities in local time and destroys
      // local time sortedness.
      return duration_cast<Duration>(
          CeilHelper<Duration, Unit>(d + li2.first.offset, options) - li2.first.offset);
    } else if (li2.result == local_info::nonexistent ||
               li2.first.offset > li.first.offset) {
      // In case we hit or cross a nonexistent period we add the pre-DST-jump offset to
      // UTC, ceil this time and subtract the pre-DST-jump offset from the ceiled time.
      return duration_cast<Duration>(
          CeilHelper<Duration, Unit>(d + li2.second.offset, options) - li2.second.offset);
    }
    return duration_cast<Duration>(d2 - si.offset);
  }

  template <typename Duration, typename Unit>
  Duration RoundTimePoint(const int64_t t, const RoundTemporalOptions& options) const {
    const Duration d = Duration{t};
    const Duration c = CeilTimePoint<Duration, Unit>(t, options);
    const Duration f = FloorTimePoint<Duration, Unit>(t, options);
    return (d - f >= c - d) ? c : f;
  }

  template <typename Duration>
  Duration ConvertLocalToSys(Duration t, Status* st) const {
    return zoned_time<Duration>{tz, local_time<Duration>(t)}
        .get_sys_time()
        .time_since_epoch();
  }

  local_days ConvertDays(sys_days d) const { return local_days(year_month_day(d)); }
};

template <typename Duration>
struct TimestampFormatter {
  const char* format;
  const time_zone* tz;
  std::ostringstream bufstream;

  explicit TimestampFormatter(const std::string& format, const time_zone* tz,
                              const std::locale& locale)
      : format(format.c_str()), tz(tz) {
    bufstream.imbue(locale);
    // Propagate errors as C++ exceptions (to get an actual error message)
    bufstream.exceptions(std::ios::failbit | std::ios::badbit);
  }

  Result<std::string> operator()(int64_t arg) {
    bufstream.str("");
    const auto zt = zoned_time<Duration>{tz, sys_time<Duration>(Duration{arg})};
    try {
      arrow_vendored::date::to_stream(bufstream, format, zt);
    } catch (const std::runtime_error& ex) {
      bufstream.clear();
      return Status::Invalid("Failed formatting timestamp: ", ex.what());
    }
    // XXX could return a view with std::ostringstream::view() (C++20)
    return std::move(bufstream).str();
  }
};

//
// Which types to generate a kernel for
//
struct WithDates {};
struct WithTimes {};
struct WithTimestamps {};
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

}  // namespace internal
}  // namespace compute
}  // namespace arrow
