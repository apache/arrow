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
  const time_zone* tz;

  template <typename Duration>
  local_time<Duration> ConvertTimePoint(int64_t t) const {
    return tz->to_local(sys_time<Duration>(Duration{t}));
  }

  template <typename Duration>
  Duration ConvertLocalToSys(Duration t, Status* st) const {
    try {
      return zoned_time<Duration>{tz, local_time<Duration>(t)}
          .get_sys_time()
          .time_since_epoch();
    } catch (const arrow_vendored::date::nonexistent_local_time& e) {
      *st = Status::Invalid("Local time does not exist: ", e.what());
      return Duration{0};
    } catch (const arrow_vendored::date::ambiguous_local_time& e) {
      *st = Status::Invalid("Local time is ambiguous: ", e.what());
      return Duration{0};
    }
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
