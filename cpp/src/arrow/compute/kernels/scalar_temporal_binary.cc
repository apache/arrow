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

#include <cmath>
#include <initializer_list>
#include <sstream>

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/temporal_internal.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/time.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {
namespace internal {

namespace {

using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::hh_mm_ss;
using arrow_vendored::date::local_days;
using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::sys_days;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::time_zone;
using arrow_vendored::date::trunc;
using arrow_vendored::date::weekday;
using arrow_vendored::date::weeks;
using arrow_vendored::date::year_month_day;
using arrow_vendored::date::year_month_weekday;
using arrow_vendored::date::years;
using arrow_vendored::date::zoned_time;
using arrow_vendored::date::literals::dec;
using arrow_vendored::date::literals::jan;
using arrow_vendored::date::literals::last;
using arrow_vendored::date::literals::mon;
using arrow_vendored::date::literals::sun;
using arrow_vendored::date::literals::thu;
using arrow_vendored::date::literals::wed;
using internal::applicator::ScalarBinaryNotNullStatefulEqualTypes;

using DayOfWeekState = OptionsWrapper<DayOfWeekOptions>;
using WeekState = OptionsWrapper<WeekOptions>;

Status CheckTimezones(const ExecSpan& batch) {
  const auto& timezone = GetInputTimezone(*batch[0].type());
  for (int i = 1; i < batch.num_values(); i++) {
    const auto& other_timezone = GetInputTimezone(*batch[i].type());
    if (other_timezone != timezone) {
      return Status::TypeError("Got differing time zone '", other_timezone,
                               "' for argument ", i + 1, "; expected '", timezone, "'");
    }
  }
  return Status::OK();
}

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalBinary {
  template <typename OptionsType, typename T = InType>
  static enable_if_timestamp<T, Status> ExecWithOptions(KernelContext* ctx,
                                                        const OptionsType* options,
                                                        const ExecSpan& batch,
                                                        ExecResult* out) {
    RETURN_NOT_OK(CheckTimezones(batch));

    const auto& timezone = GetInputTimezone(*batch[0].type());
    if (timezone.empty()) {
      using ExecTemplate = Op<Duration, NonZonedLocalizer>;
      auto op = ExecTemplate(options, NonZonedLocalizer());
      applicator::ScalarBinaryNotNullStatefulEqualTypes<OutType, T, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
      using ExecTemplate = Op<Duration, ZonedLocalizer>;
      auto op = ExecTemplate(options, ZonedLocalizer{tz});
      applicator::ScalarBinaryNotNullStatefulEqualTypes<OutType, T, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    }
  }

  template <typename OptionsType, typename T = InType>
  static enable_if_t<!is_timestamp_type<T>::value, Status> ExecWithOptions(
      KernelContext* ctx, const OptionsType* options, const ExecSpan& batch,
      ExecResult* out) {
    using ExecTemplate = Op<Duration, NonZonedLocalizer>;
    auto op = ExecTemplate(options, NonZonedLocalizer());
    applicator::ScalarBinaryNotNullStatefulEqualTypes<OutType, T, ExecTemplate> kernel{
        op};
    return kernel.Exec(ctx, batch, out);
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const FunctionOptions* options = nullptr;
    return ExecWithOptions(ctx, options, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalDayOfWeekBinary : public TemporalBinary<Op, Duration, InType, OutType> {
  using Base = TemporalBinary<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const DayOfWeekOptions& options = DayOfWeekState::Get(ctx);
    RETURN_NOT_OK(ValidateDayOfWeekOptions(options));
    return Base::ExecWithOptions(ctx, &options, batch, out);
  }
};

// ----------------------------------------------------------------------
// Compute boundary crossings between two timestamps

template <typename Duration, typename Localizer>
struct YearsBetween {
  YearsBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    year_month_day from(
        floor<days>(localizer_.template ConvertTimePoint<Duration>(arg0)));
    year_month_day to(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg1)));
    return static_cast<T>((to.year() - from.year()).count());
  }

  Localizer localizer_;
};

template <typename Duration, typename Localizer>
struct QuartersBetween {
  QuartersBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  static int64_t GetQuarters(const year_month_day& ymd) {
    return static_cast<int64_t>(static_cast<int32_t>(ymd.year())) * 4 + GetQuarter(ymd);
  }

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    year_month_day from_ymd(
        floor<days>(localizer_.template ConvertTimePoint<Duration>(arg0)));
    year_month_day to_ymd(
        floor<days>(localizer_.template ConvertTimePoint<Duration>(arg1)));
    int64_t from_quarters = GetQuarters(from_ymd);
    int64_t to_quarters = GetQuarters(to_ymd);
    return static_cast<T>(to_quarters - from_quarters);
  }

  Localizer localizer_;
};

template <typename Duration, typename Localizer>
struct MonthsBetween {
  MonthsBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    year_month_day from(
        floor<days>(localizer_.template ConvertTimePoint<Duration>(arg0)));
    year_month_day to(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg1)));
    return static_cast<T>((to.year() / to.month() - from.year() / from.month()).count());
  }

  Localizer localizer_;
};

template <typename Duration, typename Localizer>
struct WeeksBetween {
  using days_t = typename Localizer::days_t;

  WeeksBetween(const DayOfWeekOptions* options, Localizer&& localizer)
      : week_start_(options->week_start), localizer_(std::move(localizer)) {}

  /// Adjust the day backwards to land on the start of the week.
  days_t ToWeekStart(days_t point) const {
    const weekday dow(point);
    const weekday start_of_week(week_start_);
    if (dow == start_of_week) return point;
    const days delta = start_of_week - dow;
    // delta is always positive and in [0, 6]
    return point - days(7 - delta.count());
  }

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    auto from =
        ToWeekStart(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg0)));
    auto to =
        ToWeekStart(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg1)));
    return (to - from).count() / 7;
  }

  uint32_t week_start_;
  Localizer localizer_;
};

template <typename Duration, typename Localizer>
struct MonthDayNanoBetween {
  MonthDayNanoBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    static_assert(std::is_same<T, MonthDayNanoIntervalType::MonthDayNanos>::value, "");
    auto from = localizer_.template ConvertTimePoint<Duration>(arg0);
    auto to = localizer_.template ConvertTimePoint<Duration>(arg1);
    year_month_day from_ymd(floor<days>(from));
    year_month_day to_ymd(floor<days>(to));
    const int32_t num_months = static_cast<int32_t>(
        (to_ymd.year() / to_ymd.month() - from_ymd.year() / from_ymd.month()).count());
    const int32_t num_days = static_cast<int32_t>(static_cast<uint32_t>(to_ymd.day())) -
                             static_cast<int32_t>(static_cast<uint32_t>(from_ymd.day()));
    auto from_time = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(from - floor<days>(from))
            .count());
    auto to_time = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(to - floor<days>(to))
            .count());
    const int64_t num_nanos = to_time - from_time;
    return T{num_months, num_days, num_nanos};
  }

  Localizer localizer_;
};

template <typename Duration, typename Localizer>
struct DayTimeBetween {
  DayTimeBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    static_assert(std::is_same<T, DayTimeIntervalType::DayMilliseconds>::value, "");
    auto from = localizer_.template ConvertTimePoint<Duration>(arg0);
    auto to = localizer_.template ConvertTimePoint<Duration>(arg1);
    const int32_t num_days =
        static_cast<int32_t>((floor<days>(to) - floor<days>(from)).count());
    auto from_time = static_cast<int32_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(from - floor<days>(from))
            .count());
    auto to_time = static_cast<int32_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(to - floor<days>(to))
            .count());
    const int32_t num_millis = to_time - from_time;
    return DayTimeIntervalType::DayMilliseconds{num_days, num_millis};
  }

  Localizer localizer_;
};

template <typename Unit, typename Duration, typename Localizer>
struct UnitsBetween {
  UnitsBetween(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0, typename Arg1>
  T Call(KernelContext*, Arg0 arg0, Arg1 arg1, Status*) const {
    auto from = floor<Unit>(localizer_.template ConvertTimePoint<Duration>(arg0));
    auto to = floor<Unit>(localizer_.template ConvertTimePoint<Duration>(arg1));
    return static_cast<T>((to - from).count());
  }

  Localizer localizer_;
};

template <typename Duration, typename Localizer>
using DaysBetween = UnitsBetween<days, Duration, Localizer>;

template <typename Duration, typename Localizer>
using HoursBetween = UnitsBetween<std::chrono::hours, Duration, Localizer>;

template <typename Duration, typename Localizer>
using MinutesBetween = UnitsBetween<std::chrono::minutes, Duration, Localizer>;

template <typename Duration, typename Localizer>
using SecondsBetween = UnitsBetween<std::chrono::seconds, Duration, Localizer>;

template <typename Duration, typename Localizer>
using MillisecondsBetween = UnitsBetween<std::chrono::milliseconds, Duration, Localizer>;

template <typename Duration, typename Localizer>
using MicrosecondsBetween = UnitsBetween<std::chrono::microseconds, Duration, Localizer>;

template <typename Duration, typename Localizer>
using NanosecondsBetween = UnitsBetween<std::chrono::nanoseconds, Duration, Localizer>;

// ----------------------------------------------------------------------
// Registration helpers

template <template <typename...> class Op,
          template <template <typename...> class OpExec, typename Duration,
                    typename InType, typename OutType, typename... Args>
          class ExecTemplate,
          typename OutType>
struct BinaryTemporalFactory {
  OutputType out_type;
  KernelInit init;
  std::shared_ptr<ScalarFunction> func;

  template <typename... WithTypes>
  static std::shared_ptr<ScalarFunction> Make(
      std::string name, OutputType out_type, FunctionDoc doc,
      const FunctionOptions* default_options = NULLPTR, KernelInit init = NULLPTR) {
    DCHECK_NE(sizeof...(WithTypes), 0);
    BinaryTemporalFactory self{
        out_type, init,
        std::make_shared<ScalarFunction>(name, Arity::Binary(), std::move(doc),
                                         default_options)};
    AddTemporalKernels(&self, WithTypes{}...);
    return self.func;
  }

  template <typename Duration, typename InType>
  void AddKernel(InputType in_type) {
    auto exec = ExecTemplate<Op, Duration, InType, OutType>::Exec;
    DCHECK_OK(func->AddKernel({in_type, in_type}, out_type, std::move(exec), init));
  }
};

const FunctionDoc years_between_doc{
    "Compute the number of years between two timestamps",
    ("Returns the number of year boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the year.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc quarters_between_doc{
    "Compute the number of quarters between two timestamps",
    ("Returns the number of quarter start boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the quarter.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc months_between_doc{
    "Compute the number of months between two timestamps",
    ("Returns the number of month boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the month.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc month_day_nano_interval_between_doc{
    "Compute the number of months, days and nanoseconds between two timestamps",
    ("Returns the number of months, days, and nanoseconds from `start` to `end`.\n"
     "That is, first the difference in months is computed as if both timestamps\n"
     "were truncated to the months, then the difference between the days\n"
     "is computed, and finally the difference between the times of the two\n"
     "timestamps is computed as if both times were truncated to the nanosecond.\n"
     "Null values return null."),
    {"start", "end"}};

const FunctionDoc weeks_between_doc{
    "Compute the number of weeks between two timestamps",
    ("Returns the number of week boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the week.\n"
     "Null values emit null."),
    {"start", "end"},
    "DayOfWeekOptions"};

const FunctionDoc day_time_interval_between_doc{
    "Compute the number of days and milliseconds between two timestamps",
    ("Returns the number of days and milliseconds from `start` to `end`.\n"
     "That is, first the difference in days is computed as if both\n"
     "timestamps were truncated to the day, then the difference between time times\n"
     "of the two timestamps is computed as if both times were truncated to the\n"
     "millisecond.\n"
     "Null values return null."),
    {"start", "end"}};

const FunctionDoc days_between_doc{
    "Compute the number of days between two timestamps",
    ("Returns the number of day boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the day.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc hours_between_doc{
    "Compute the number of hours between two timestamps",
    ("Returns the number of hour boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the hour.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc minutes_between_doc{
    "Compute the number of minute boundaries between two timestamps",
    ("Returns the number of minute boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the minute.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc seconds_between_doc{
    "Compute the number of seconds between two timestamps",
    ("Returns the number of second boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the second.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc milliseconds_between_doc{
    "Compute the number of millisecond boundaries between two timestamps",
    ("Returns the number of millisecond boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the millisecond.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc microseconds_between_doc{
    "Compute the number of microseconds between two timestamps",
    ("Returns the number of microsecond boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the microsecond.\n"
     "Null values emit null."),
    {"start", "end"}};

const FunctionDoc nanoseconds_between_doc{
    "Compute the number of nanoseconds between two timestamps",
    ("Returns the number of nanosecond boundaries crossed from `start` to `end`.\n"
     "That is, the difference is calculated as if the timestamps were\n"
     "truncated to the nanosecond.\n"
     "Null values emit null."),
    {"start", "end"}};

}  // namespace

void RegisterScalarTemporalBinary(FunctionRegistry* registry) {
  // Temporal difference functions
  auto years_between =
      BinaryTemporalFactory<YearsBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimestamps>("years_between", int64(), years_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(years_between)));

  auto quarters_between =
      BinaryTemporalFactory<QuartersBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimestamps>("quarters_between", int64(), quarters_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(quarters_between)));

  auto month_interval_between =
      BinaryTemporalFactory<MonthsBetween, TemporalBinary, MonthIntervalType>::Make<
          WithDates, WithTimestamps>("month_interval_between", month_interval(),
                                     months_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(month_interval_between)));

  auto month_day_nano_interval_between =
      BinaryTemporalFactory<MonthDayNanoBetween, TemporalBinary,
                            MonthDayNanoIntervalType>::Make<WithDates, WithTimes,
                                                            WithTimestamps>(
          "month_day_nano_interval_between", month_day_nano_interval(),
          month_day_nano_interval_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(month_day_nano_interval_between)));

  static const auto default_day_of_week_options = DayOfWeekOptions::Defaults();
  auto weeks_between =
      BinaryTemporalFactory<WeeksBetween, TemporalDayOfWeekBinary, Int64Type>::Make<
          WithDates, WithTimestamps>("weeks_between", int64(), weeks_between_doc,
                                     &default_day_of_week_options, DayOfWeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(weeks_between)));

  auto day_time_interval_between =
      BinaryTemporalFactory<DayTimeBetween, TemporalBinary, DayTimeIntervalType>::Make<
          WithDates, WithTimes, WithTimestamps>("day_time_interval_between",
                                                day_time_interval(),
                                                day_time_interval_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(day_time_interval_between)));

  auto days_between =
      BinaryTemporalFactory<DaysBetween, TemporalBinary, Int64Type>::Make<WithDates,
                                                                          WithTimestamps>(
          "days_between", int64(), days_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(days_between)));

  auto hours_between =
      BinaryTemporalFactory<HoursBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("hours_between", int64(),
                                                hours_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(hours_between)));

  auto minutes_between =
      BinaryTemporalFactory<MinutesBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("minutes_between", int64(),
                                                minutes_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(minutes_between)));

  auto seconds_between =
      BinaryTemporalFactory<SecondsBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("seconds_between", int64(),
                                                seconds_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(seconds_between)));

  auto milliseconds_between =
      BinaryTemporalFactory<MillisecondsBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("milliseconds_between", int64(),
                                                milliseconds_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(milliseconds_between)));

  auto microseconds_between =
      BinaryTemporalFactory<MicrosecondsBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("microseconds_between", int64(),
                                                microseconds_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(microseconds_between)));

  auto nanoseconds_between =
      BinaryTemporalFactory<NanosecondsBetween, TemporalBinary, Int64Type>::Make<
          WithDates, WithTimes, WithTimestamps>("nanoseconds_between", int64(),
                                                nanoseconds_between_doc);
  DCHECK_OK(registry->AddFunction(std::move(nanoseconds_between)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
