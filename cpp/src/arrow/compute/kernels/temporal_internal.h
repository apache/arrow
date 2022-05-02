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
using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::sys_days;
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

static inline const std::string& GetInputTimezone(const Datum& datum) {
  return checked_cast<const TimestampType&>(*datum.type()).timezone();
}

static inline const std::string& GetInputTimezone(const Scalar& scalar) {
  return checked_cast<const TimestampType&>(*scalar.type).timezone();
}

static inline const std::string& GetInputTimezone(const ArrayData& array) {
  return checked_cast<const TimestampType&>(*array.type).timezone();
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

  template <typename Duration, typename Unit>
  Duration GetOrigin(int64_t arg, const RoundTemporalOptions* options) const {
    const Duration t = Duration{arg};
    switch (options->unit) {
      case compute::CalendarUnit::DAY:
        return duration_cast<Duration>(floor<arrow_vendored::date::months>(t));
      case compute::CalendarUnit::HOUR:
        return duration_cast<Duration>(floor<days>(t));
      case compute::CalendarUnit::MINUTE:
        return duration_cast<Duration>(floor<std::chrono::hours>(t));
      case compute::CalendarUnit::SECOND:
        return duration_cast<Duration>(floor<std::chrono::minutes>(t));
      case compute::CalendarUnit::MILLISECOND:
        return duration_cast<Duration>(floor<std::chrono::seconds>(t));
      case compute::CalendarUnit::MICROSECOND:
        return duration_cast<Duration>(floor<std::chrono::milliseconds>(t));
      case compute::CalendarUnit::NANOSECOND:
        return duration_cast<Duration>(floor<std::chrono::microseconds>(t));
      default:
        return t;
    }
  }

  template <typename Duration, typename Unit>
  Duration FloorTimePoint(int64_t arg, const RoundTemporalOptions* options) const {
    const sys_time<Duration> t = sys_time<Duration>(Duration{arg});

    if (options->multiple == 1) {
      // Round to a multiple of unit since epoch start (1970-01-01 00:00:00).
      return duration_cast<Duration>(floor<Unit>(t).time_since_epoch());
    } else if (options->calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      const Unit unit = Unit{options->multiple};
      const Duration origin = GetOrigin<Duration, Unit>(arg, options);
      return duration_cast<Duration>((t - origin).time_since_epoch() / unit * unit +
                                     origin);
    } else {
      // Round to a multiple of units * options.multiple since epoch start
      // (1970-01-01 00:00:00).
      const Unit unit = Unit{options->multiple};
      const Unit d = floor<Unit>(t).time_since_epoch();
      const Unit m =
          (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
      return duration_cast<Duration>(m);
    }
  }

  template <typename Duration, typename Unit>
  Duration CeilTimePoint(int64_t arg, const RoundTemporalOptions* options) const {
    const Duration d = FloorTimePoint<Duration, Unit>(arg, options);
    if (options->strict_ceil || d.count() < arg) {
      return d + duration_cast<Duration>(Unit{options->multiple});
    }
    return d;
  }

  template <typename Duration, typename Unit>
  Duration RoundTimePoint(const int64_t arg, const RoundTemporalOptions* options) const {
    const Duration f = FloorTimePoint<Duration, Unit>(arg, options);
    Duration c = f;
    if (f.count() < arg) {
      c += duration_cast<Duration>(Unit{options->multiple});
    }
    return (arg - f.count() >= c.count() - arg) ? c : f;
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

  template <typename Duration, typename Unit>
  inline Duration GetOrigin(local_time<Duration> lt,
                            const RoundTemporalOptions* options) const {
    const Unit unit = Unit{options->multiple};
    switch (options->unit) {
      case compute::CalendarUnit::DAY: {
        const year_month_day ymd{floor<days>(lt)};
        const local_days origin =
            local_days{year_month_day(ymd.year() / ymd.month() / 1)};
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::HOUR: {
        const auto origin = floor<days>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::MINUTE: {
        const auto origin = floor<std::chrono::hours>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::SECOND: {
        const auto origin = floor<std::chrono::minutes>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::MILLISECOND: {
        const auto origin = floor<std::chrono::seconds>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::MICROSECOND: {
        const auto origin = floor<std::chrono::milliseconds>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      case compute::CalendarUnit::NANOSECOND: {
        const auto origin = floor<std::chrono::microseconds>(lt);
        return duration_cast<Duration>(
            ((lt - origin) / unit * unit + origin).time_since_epoch());
      }
      default:
        return lt.time_since_epoch();
    }
  }

  template <typename Duration, typename Unit>
  inline Duration FloorLocalToSys(const int64_t arg, const local_time<Duration> lt,
                           const RoundTemporalOptions* options) const {
    try {
      return zoned_time<Duration>(tz, lt).get_sys_time().time_since_epoch();
    } catch (const arrow_vendored::date::ambiguous_local_time&) {
      // In case we hit an ambiguous period we round to a time multiple just prior,
      // convert to UTC and add the time unit we're rounding to.
      const arrow_vendored::date::local_info li = tz->get_info(lt);
      const Unit unit = Unit{options->multiple};

      const Duration d = zoned_time<Duration>(tz, lt - li.second.offset)
                             .get_sys_time()
                             .time_since_epoch();
      const Unit t3 =
          (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
      const Duration t4 = duration_cast<Duration>(t3 + li.first.offset);
      if (arg < t4.count()) {
        return duration_cast<Duration>(t3 + li.second.offset);
      }
      return t4;
    } catch (const arrow_vendored::date::nonexistent_local_time&) {
      // In case we hit a nonexistent period we calculate the duration between the
      // start of nonexistent period and rounded to moment in UTC (nonexistent_offset).
      // We then floor the beginning of the nonexisting period in local time and add
      // nonexistent_offset to that time point in UTC.
      const arrow_vendored::date::local_info li = tz->get_info(lt);
      const Unit unit = Unit{options->multiple};

      const Duration d =
          zoned_time<Duration>(tz, lt, arrow_vendored::date::choose::earliest)
              .get_sys_time()
              .time_since_epoch();
      const Unit t3 =
          (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
      const Duration nonexistent_offset = d - lt.time_since_epoch();
      return duration_cast<Duration>(t3 + li.second.offset) + nonexistent_offset;
    }
  }

  template <typename Duration, typename Unit>
  Duration FloorTimePoint(const int64_t arg, const RoundTemporalOptions* options) const {
    local_time<Duration> lt = tz->to_local(sys_time<Duration>(Duration{arg}));
    const Unit d = floor<Unit>(lt).time_since_epoch();
    Unit d2;

    if (options->multiple == 1) {
      d2 = d;
    } else if (options->calendar_based_origin) {
      // Round to a multiple of units since the last greater unit.
      // For example: round to multiple of days since the beginning of the month or
      // to hours since the beginning of the day.
      d2 = duration_cast<Unit>(GetOrigin<Duration, Unit>(lt, options));
    } else {
      const Unit unit = Unit{options->multiple};
      d2 = (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
    }
    return FloorLocalToSys<Duration, Unit>(arg, local_time<Duration>(duration_cast<Duration>(d2)), options);
  }

  template <typename Duration, typename Unit>
  Duration CeilTimePoint(const int64_t arg, const RoundTemporalOptions* options) const {
    const Duration d = FloorTimePoint<Duration, Unit>(arg, options);
    if (options->strict_ceil || options->calendar_based_origin) {
      const local_time<Duration> lt = tz->to_local(sys_time<Duration>(d)) +
                                      duration_cast<Duration>(Unit{options->multiple});
      return FloorLocalToSys<Duration, Unit>(arg, lt, options);
    }
    if (d.count() < arg) {
      return FloorTimePoint<Duration, Unit>(
          arg + duration_cast<Duration>(Unit{options->multiple}).count(), options);
    }
    return d;
  }

  template <typename Duration, typename Unit>
  Duration RoundTimePoint(const int64_t arg, const RoundTemporalOptions* options) const {
    const Duration f = FloorTimePoint<Duration, Unit>(arg, options);
    Duration c;
    if (f.count() == arg) {
      c = f;
    } else {
      if (options->strict_ceil || options->calendar_based_origin) {
        const local_time<Duration> lt = tz->to_local(sys_time<Duration>(f)) +
                                        duration_cast<Duration>(Unit{options->multiple});
        c = FloorLocalToSys<Duration, Unit>(arg, lt, options);
      } else {
        c = FloorTimePoint<Duration, Unit>(
            arg + duration_cast<Duration>(Unit{options->multiple}).count(), options);
      }
    }
    return (arg - f.count() >= c.count() - arg) ? c : f;
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
                                const ExecBatch& batch, Datum* out, Args... args) {
    const auto& timezone = GetInputTimezone(batch.values[0]);
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
                                const ExecBatch& batch, Datum* out) {
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
                                const ExecBatch& batch, Datum* out) {
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
                                const ExecBatch& batch, Datum* out) {
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
                                const ExecBatch& batch, Datum* out) {
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
                                const ExecBatch& batch, Datum* out) {
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
                                const ExecBatch& batch, Datum* out) {
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

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out,
                     Args... args) {
    const FunctionOptions* options = nullptr;
    return Base::ExecWithOptions(ctx, options, batch, out, args...);
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
