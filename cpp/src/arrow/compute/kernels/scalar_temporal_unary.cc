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
#include "arrow/util/value_parsing.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;

namespace compute {
namespace internal {

namespace {

using arrow_vendored::date::ceil;
using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::hh_mm_ss;
using arrow_vendored::date::local_days;
using arrow_vendored::date::local_time;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::Monday;
using arrow_vendored::date::months;
using arrow_vendored::date::round;
using arrow_vendored::date::Sunday;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::trunc;
using arrow_vendored::date::weekday;
using arrow_vendored::date::weeks;
using arrow_vendored::date::year;
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
using std::chrono::duration_cast;
using std::chrono::hours;
using std::chrono::minutes;

using DayOfWeekState = OptionsWrapper<DayOfWeekOptions>;
using WeekState = OptionsWrapper<WeekOptions>;
using StrftimeState = OptionsWrapper<StrftimeOptions>;
using StrptimeState = OptionsWrapper<StrptimeOptions>;
using AssumeTimezoneState = OptionsWrapper<AssumeTimezoneOptions>;
using RoundTemporalState = OptionsWrapper<RoundTemporalOptions>;

const std::shared_ptr<DataType>& IsoCalendarType() {
  static auto type = struct_({field("iso_year", int64()), field("iso_week", int64()),
                              field("iso_day_of_week", int64())});
  return type;
}

const std::shared_ptr<DataType>& YearMonthDayType() {
  static auto type =
      struct_({field("year", int64()), field("month", int64()), field("day", int64())});

  return type;
}

Status ValidateDayOfWeekOptions(const DayOfWeekOptions& options) {
  if (options.week_start < 1 || 7 < options.week_start) {
    return Status::Invalid(
        "week_start must follow ISO convention (Monday=1, Sunday=7). Got week_start=",
        options.week_start);
  }
  return Status::OK();
}

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtractDayOfWeek
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const DayOfWeekOptions& options = DayOfWeekState::Get(ctx);
    RETURN_NOT_OK(ValidateDayOfWeekOptions(options));
    return Base::ExecWithOptions(ctx, &options, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct AssumeTimezoneExtractor
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const AssumeTimezoneOptions& options = AssumeTimezoneState::Get(ctx);
    const auto& timezone = GetInputTimezone(*batch[0].type());
    if (!timezone.empty()) {
      return Status::Invalid("Timestamps already have a timezone: '", timezone,
                             "'. Cannot localize to '", options.timezone, "'.");
    }
    ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(options.timezone));
    using ExecTemplate = Op<Duration>;
    auto op = ExecTemplate(&options, tz);
    applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
        op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct DaylightSavingsExtractor
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& timezone = GetInputTimezone(*batch[0].type());
    if (timezone.empty()) {
      return Status::Invalid("Timestamps have no timezone. Cannot determine DST.");
    }
    ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
    using ExecTemplate = Op<Duration>;
    auto op = ExecTemplate(nullptr, tz);
    applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
        op};
    return kernel.Exec(ctx, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtractWeek
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const WeekOptions& options = WeekState::Get(ctx);
    return Base::ExecWithOptions(ctx, &options, batch, out);
  }
};

// Since OutType should always equal InType we set OutType to InType in the
// TemporalComponentExtractBase template parameters.
template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtractRound
    : public TemporalComponentExtractBase<Op, Duration, InType, InType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, InType>;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const RoundTemporalOptions& options = RoundTemporalState::Get(ctx);
    return Base::ExecWithOptions(ctx, &options, batch, out);
  }
};

// ----------------------------------------------------------------------
// Extract year from temporal types
//
// This class and the following (`Month`, etc.) are to be used as the `Op`
// parameter to `TemporalComponentExtract`.

template <typename Duration, typename Localizer>
struct Year {
  Year(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const int32_t>(
        year_month_day(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)))
            .year()));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract is leap year from temporal types

template <typename Duration, typename Localizer>
struct IsLeapYear {
  IsLeapYear(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    int32_t y = static_cast<const int32_t>(
        year_month_day(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)))
            .year());
    return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract month from temporal types

template <typename Duration, typename Localizer>
struct Month {
  explicit Month(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)))
            .month()));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract day from temporal types

template <typename Duration, typename Localizer>
struct Day {
  explicit Day(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)))
            .day()));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract (year, month, day) struct from temporal types

template <typename Duration, typename Localizer>
std::array<int64_t, 3> GetYearMonthDay(int64_t arg, Localizer&& localizer) {
  const auto t = floor<days>(localizer.template ConvertTimePoint<Duration>(arg));
  const auto ymd = year_month_day(t);

  return {static_cast<int64_t>(static_cast<const int32_t>(ymd.year())),
          static_cast<int64_t>(static_cast<const uint32_t>(ymd.month())),
          static_cast<int64_t>(static_cast<const uint32_t>(ymd.day()))};
}

template <typename Duration, typename InType>
struct YearMonthDayWrapper {
  static Result<std::array<int64_t, 3>> Get(const Scalar& in) {
    const auto& in_val = internal::UnboxScalar<const InType>::Unbox(in);
    return GetYearMonthDay<Duration>(in_val, NonZonedLocalizer{});
  }
};

template <typename Duration>
struct YearMonthDayWrapper<Duration, TimestampType> {
  static Result<std::array<int64_t, 3>> Get(const Scalar& in) {
    const auto& in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
    const auto& timezone = GetInputTimezone(*in.type);
    if (timezone.empty()) {
      return GetYearMonthDay<Duration>(in_val, NonZonedLocalizer{});
    } else {
      ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
      return GetYearMonthDay<Duration>(in_val, ZonedLocalizer{tz});
    }
  }
};

template <typename Duration, typename InType, typename BuilderType>
struct YearMonthDayVisitValueFunction {
  static Result<std::function<Status(typename InType::c_type arg)>> Get(
      const std::vector<BuilderType*>& field_builders, const ArraySpan&,
      StructBuilder* struct_builder) {
    return [=](typename InType::c_type arg) {
      const auto ymd = GetYearMonthDay<Duration>(arg, NonZonedLocalizer{});
      field_builders[0]->UnsafeAppend(static_cast<const int32_t>(ymd[0]));
      field_builders[1]->UnsafeAppend(static_cast<const uint32_t>(ymd[1]));
      field_builders[2]->UnsafeAppend(static_cast<const uint32_t>(ymd[2]));
      return struct_builder->Append();
    };
  }
};

template <typename Duration, typename BuilderType>
struct YearMonthDayVisitValueFunction<Duration, TimestampType, BuilderType> {
  static Result<std::function<Status(typename TimestampType::c_type arg)>> Get(
      const std::vector<BuilderType*>& field_builders, const ArraySpan& in,
      StructBuilder* struct_builder) {
    const auto& timezone = GetInputTimezone(*in.type);
    if (timezone.empty()) {
      return [=](TimestampType::c_type arg) {
        const auto ymd = GetYearMonthDay<Duration>(arg, NonZonedLocalizer{});
        field_builders[0]->UnsafeAppend(static_cast<const int32_t>(ymd[0]));
        field_builders[1]->UnsafeAppend(static_cast<const uint32_t>(ymd[1]));
        field_builders[2]->UnsafeAppend(static_cast<const uint32_t>(ymd[2]));
        return struct_builder->Append();
      };
    }
    ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
    return [=](TimestampType::c_type arg) {
      const auto ymd = GetYearMonthDay<Duration>(arg, ZonedLocalizer{tz});
      field_builders[0]->UnsafeAppend(static_cast<const int32_t>(ymd[0]));
      field_builders[1]->UnsafeAppend(static_cast<const uint32_t>(ymd[1]));
      field_builders[2]->UnsafeAppend(static_cast<const uint32_t>(ymd[2]));
      return struct_builder->Append();
    };
  }
};

template <typename Duration, typename InType>
struct YearMonthDay {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& in = batch[0].array;
    using BuilderType = typename TypeTraits<Int64Type>::BuilderType;

    std::unique_ptr<ArrayBuilder> array_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), YearMonthDayType(), &array_builder));
    StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());
    RETURN_NOT_OK(struct_builder->Reserve(in.length));

    std::vector<BuilderType*> field_builders;
    field_builders.reserve(3);
    for (int i = 0; i < 3; i++) {
      field_builders.push_back(
          checked_cast<BuilderType*>(struct_builder->field_builder(i)));
      RETURN_NOT_OK(field_builders[i]->Reserve(1));
    }
    auto visit_null = [&]() { return struct_builder->AppendNull(); };
    std::function<Status(typename InType::c_type arg)> visit_value;
    ARROW_ASSIGN_OR_RAISE(
        visit_value, (YearMonthDayVisitValueFunction<Duration, InType, BuilderType>::Get(
                         field_builders, in, struct_builder)));
    RETURN_NOT_OK(
        VisitArraySpanInline<typename InType::PhysicalType>(in, visit_value, visit_null));
    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    out->value = std::move(out_array->data());
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract day of week from temporal types
//
// By default week starts on Monday represented by 0 and ends on Sunday represented
// by 6. Start day of the week (Monday=1, Sunday=7) and numbering start (0 or 1) can be
// set using DayOfWeekOptions

template <typename Duration, typename Localizer>
struct DayOfWeek {
  explicit DayOfWeek(const DayOfWeekOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {
    for (int i = 0; i < 7; i++) {
      lookup_table[i] = i + 8 - options->week_start;
      lookup_table[i] = (lookup_table[i] > 6) ? lookup_table[i] - 7 : lookup_table[i];
      lookup_table[i] += !options->count_from_zero;
    }
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto wd = year_month_weekday(
                        floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)))
                        .weekday()
                        .iso_encoding();
    return lookup_table[wd - 1];
  }

  std::array<int64_t, 7> lookup_table;
  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract day of year from temporal types

template <typename Duration, typename Localizer>
struct DayOfYear {
  explicit DayOfYear(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(localizer_.template ConvertTimePoint<Duration>(arg));
    return static_cast<T>(
        (t - localizer_.ConvertDays(year_month_day(t).year() / jan / 0)).count());
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract ISO Year values from temporal types
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.

template <typename Duration, typename Localizer>
struct ISOYear {
  explicit ISOYear(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(localizer_.template ConvertTimePoint<Duration>(arg));
    auto y = year_month_day{t + days{3}}.year();
    auto start = localizer_.ConvertDays((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
    }
    return static_cast<T>(static_cast<int32_t>(y));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract US epidemiological year values from temporal types
//
// First week of US epidemiological year has the majority (4 or more) of it's
// days in January. Last week of US epidemiological year has the year's last
// Wednesday in it. US epidemiological week starts on Sunday.

template <typename Duration, typename Localizer>
struct USYear {
  explicit USYear(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(localizer_.template ConvertTimePoint<Duration>(arg));
    auto y = year_month_day{t + days{3}}.year();
    auto start = localizer_.ConvertDays((y - years{1}) / dec / wed[last]) + (mon - thu);
    if (t < start) {
      --y;
    }
    return static_cast<T>(static_cast<int32_t>(y));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract week from temporal types
//
// First week of an ISO year has the majority (4 or more) of its days in January.
// Last week of an ISO year has the year's last Thursday in it.
// Based on
// https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503

template <typename Duration, typename Localizer>
struct Week {
  explicit Week(const WeekOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)),
        count_from_zero_(options->count_from_zero),
        first_week_is_fully_in_year_(options->first_week_is_fully_in_year) {
    if (options->week_starts_monday) {
      if (first_week_is_fully_in_year_) {
        wd_ = mon;
      } else {
        wd_ = thu;
      }
    } else {
      if (first_week_is_fully_in_year_) {
        wd_ = sun;
      } else {
        wd_ = wed;
      }
    }
    if (count_from_zero_) {
      days_offset_ = days{0};
    } else {
      days_offset_ = days{3};
    }
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(localizer_.template ConvertTimePoint<Duration>(arg));
    auto y = year_month_day{t + days_offset_}.year();

    if (first_week_is_fully_in_year_) {
      auto start = localizer_.ConvertDays(y / jan / wd_[1]);
      if (!count_from_zero_) {
        if (t < start) {
          --y;
          start = localizer_.ConvertDays(y / jan / wd_[1]);
        }
      }
      return static_cast<T>(floor<weeks>(t - start).count() + 1);
    }

    auto start = localizer_.ConvertDays((y - years{1}) / dec / wd_[last]) + (mon - thu);
    if (!count_from_zero_) {
      if (t < start) {
        --y;
        start = localizer_.ConvertDays((y - years{1}) / dec / wd_[last]) + (mon - thu);
      }
    }
    return static_cast<T>(floor<weeks>(t - start).count() + 1);
  }

  Localizer localizer_;
  arrow_vendored::date::weekday wd_;
  arrow_vendored::date::days days_offset_;
  const bool count_from_zero_;
  const bool first_week_is_fully_in_year_;
};

// ----------------------------------------------------------------------
// Extract quarter from temporal types

template <typename Duration, typename Localizer>
struct Quarter {
  explicit Quarter(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto ymd =
        year_month_day(floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)));
    return static_cast<T>(GetQuarter(ymd) + 1);
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename Duration, typename Localizer>
struct Hour {
  explicit Hour(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    return static_cast<T>((t - floor<days>(t)) / hours(1));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename Duration, typename Localizer>
struct Minute {
  explicit Minute(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    return static_cast<T>((t - floor<hours>(t)) / minutes(1));
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename Duration, typename Localizer>
struct Second {
  explicit Second(const FunctionOptions* options, Localizer&& localizer) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<minutes>(t)) / std::chrono::seconds(1));
  }
};

// ----------------------------------------------------------------------
// Extract subsecond from timestamp

template <typename Duration, typename Localizer>
struct Subsecond {
  explicit Subsecond(const FunctionOptions* options, Localizer&& localizer) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        (std::chrono::duration<double>(t - floor<std::chrono::seconds>(t)).count()));
  }
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename Duration, typename Localizer>
struct Millisecond {
  explicit Millisecond(const FunctionOptions* options, Localizer&& localizer) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::milliseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename Duration, typename Localizer>
struct Microsecond {
  explicit Microsecond(const FunctionOptions* options, Localizer&& localizer) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::microseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename Duration, typename Localizer>
struct Nanosecond {
  explicit Nanosecond(const FunctionOptions* options, Localizer&& localizer) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::nanoseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract if currently observing daylight savings

template <typename Duration>
struct IsDaylightSavings {
  explicit IsDaylightSavings(const FunctionOptions* options, const time_zone* tz)
      : tz_(tz) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return tz_->get_info(sys_time<Duration>{Duration{arg}}).save.count() != 0;
  }

  const time_zone* tz_;
};

// ----------------------------------------------------------------------
// Extract local timestamp of a given timestamp given its timezone

template <typename Duration, typename Localizer>
struct LocalTimestamp {
  explicit LocalTimestamp(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    return static_cast<T>(t.time_since_epoch().count());
  }

  Localizer localizer_;
};

// ----------------------------------------------------------------------
// Round temporal values to given frequency

template <typename Duration, typename Localizer>
year_month_day GetFlooredYmd(int64_t arg, const int multiple,
                             const RoundTemporalOptions& options, Localizer localizer_) {
  year_month_day ymd{floor<days>(localizer_.template ConvertTimePoint<Duration>(arg))};

  if (multiple == 1) {
    // Round to a multiple of months since epoch start (1970-01-01 00:00:00).
    return year_month_day(ymd.year() / ymd.month() / 1);
  } else if (options.calendar_based_origin) {
    // Round to a multiple of months since the last year.
    //
    // Note: compute::CalendarUnit::YEAR is the greatest unit so there is no logical time
    // point to use as origin. compute::CalendarUnit::DAY is covered by FloorTimePoint.
    // Therefore compute::CalendarUnit::YEAR and compute::CalendarUnit::DAY are not
    // covered here.
    switch (options.unit) {
      case compute::CalendarUnit::MONTH: {
        const auto m = (static_cast<uint32_t>(ymd.month()) - 1) / options.multiple *
                       options.multiple;
        return year_month_day(ymd.year() / jan / 1) + months{m};
      }
      case compute::CalendarUnit::QUARTER: {
        const auto m = (static_cast<uint32_t>(ymd.month()) - 1) / (options.multiple * 3) *
                       (options.multiple * 3);
        return year_month_day(ymd.year() / jan / 1) + months{m};
      }
      default:
        return ymd;
    }
  } else {
    // Round to month * options.multiple since epoch start (1970-01-01 00:00:00).
    int32_t total_months_origin = 1970 * 12;
    int32_t total_months = static_cast<int32_t>(ymd.year()) * 12 +
                           static_cast<int32_t>(static_cast<uint32_t>(ymd.month())) - 1 -
                           total_months_origin;

    if (total_months >= 0) {
      total_months = total_months / multiple * multiple;
    } else {
      total_months = (total_months - multiple + 1) / multiple * multiple;
    }
    return year_month_day(year{1970} / jan / 1) + months{total_months};
  }
}

template <typename Duration, typename Unit, typename Localizer>
const Duration FloorTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                              Localizer localizer_, Status* st) {
  const auto t = localizer_.template ConvertTimePoint<Duration>(arg);

  if (options.multiple == 1) {
    // Round to a multiple of unit since epoch start (1970-01-01 00:00:00).
    const Unit d = floor<Unit>(t).time_since_epoch();
    return localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(d),
                                                           st);
  } else if (options.calendar_based_origin) {
    // Round to a multiple of units since the last greater unit.
    // For example: round to multiple of days since the beginning of the month or
    // to hours since the beginning of the day.
    const Unit unit = Unit{options.multiple};
    Duration origin;

    switch (options.unit) {
      case compute::CalendarUnit::DAY:
        origin = duration_cast<Duration>(
            localizer_
                .ConvertDays(year_month_day(floor<days>(t)).year() /
                             year_month_day(floor<days>(t)).month() / 1)
                .time_since_epoch());
        break;
      case compute::CalendarUnit::HOUR:
        origin = duration_cast<Duration>(
            localizer_.ConvertDays(year_month_day(floor<days>(t))).time_since_epoch());
        break;
      case compute::CalendarUnit::MINUTE:
        origin = duration_cast<Duration>(floor<std::chrono::hours>(t).time_since_epoch());
        break;
      case compute::CalendarUnit::SECOND:
        origin =
            duration_cast<Duration>(floor<std::chrono::minutes>(t).time_since_epoch());
        break;
      case compute::CalendarUnit::MILLISECOND:
        origin =
            duration_cast<Duration>(floor<std::chrono::seconds>(t).time_since_epoch());
        break;
      case compute::CalendarUnit::MICROSECOND:
        origin = duration_cast<Duration>(
            floor<std::chrono::milliseconds>(t).time_since_epoch());
        break;
      case compute::CalendarUnit::NANOSECOND:
        origin = duration_cast<Duration>(
            floor<std::chrono::microseconds>(t).time_since_epoch());
        break;
      default: {
        *st = Status::Invalid("Cannot floor to ", &options.unit);
        return Duration{0};
      }
    }
    const Duration m =
        duration_cast<Duration>(((t - origin).time_since_epoch() / unit * unit + origin));
    return localizer_.template ConvertLocalToSys<Duration>(m, st);
  } else {
    // Round to a multiple of units * options.multiple since epoch start
    // (1970-01-01 00:00:00).
    const Unit d = floor<Unit>(t).time_since_epoch();
    const Unit unit = Unit{options.multiple};
    const Unit m =
        (d.count() >= 0) ? d / unit * unit : (d - unit + Unit{1}) / unit * unit;
    return localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(m),
                                                           st);
  }
}

template <typename Duration, typename Localizer>
const Duration FloorWeekTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                                  Localizer localizer_, const Duration weekday_offset,
                                  Status* st) {
  const auto t = localizer_.template ConvertTimePoint<Duration>(arg) + weekday_offset;
  const weeks d = floor<weeks>(t).time_since_epoch();

  if (options.multiple == 1) {
    // Round to a multiple of weeks since epoch start (1970-01-01 00:00:00).
    return localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(d),
                                                           st) -
           weekday_offset;
  } else if (options.calendar_based_origin) {
    // Round to a multiple of weeks since year prior.
    weekday wd_;
    if (options.week_starts_monday) {
      wd_ = thu;
    } else {
      wd_ = wed;
    }
    const auto y = year_month_day{floor<days>(t)}.year();
    const auto start =
        localizer_.ConvertDays((y - years{1}) / dec / wd_[last]) + (mon - thu);
    const weeks unit = weeks{options.multiple};
    const auto m = (t - start) / unit * unit + start;
    return localizer_.template ConvertLocalToSys<Duration>(m.time_since_epoch(), st);
  } else {
    // Round to a multiple of weeks * options.multiple since epoch start
    // (1970-01-01 00:00:00).
    const weeks unit = weeks{options.multiple};
    const weeks m =
        (d.count() >= 0) ? d / unit * unit : (d - unit + weeks{1}) / unit * unit;
    return localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(m),
                                                           st) -
           weekday_offset;
  }
}

template <typename Duration, typename Unit, typename Localizer>
Duration CeilTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                       Localizer localizer_, Status* st) {
  const Duration f =
      FloorTimePoint<Duration, Unit, Localizer>(arg, options, localizer_, st);
  const auto cl =
      localizer_.template ConvertTimePoint<Duration>(f.count()).time_since_epoch();
  const Duration cs =
      localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(cl), st);

  if (options.ceil_is_strictly_greater || cs < Duration{arg}) {
    return localizer_.template ConvertLocalToSys<Duration>(
        duration_cast<Duration>(cl + duration_cast<Duration>(Unit{options.multiple})),
        st);
  }
  return cs;
}

template <typename Duration, typename Localizer>
Duration CeilWeekTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                           Localizer localizer_, const Duration weekday_offset,
                           Status* st) {
  const Duration f = FloorWeekTimePoint<Duration, Localizer>(arg, options, localizer_,
                                                             weekday_offset, st);
  const auto cl =
      localizer_.template ConvertTimePoint<Duration>(f.count()).time_since_epoch();
  const Duration cs =
      localizer_.template ConvertLocalToSys<Duration>(duration_cast<Duration>(cl), st);
  if (options.ceil_is_strictly_greater || cs < Duration{arg}) {
    return localizer_.template ConvertLocalToSys<Duration>(
        duration_cast<Duration>(cl + duration_cast<Duration>(weeks{options.multiple})),
        st);
  }
  return cs;
}

template <typename Duration, typename Unit, typename Localizer>
Duration RoundTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                        Localizer localizer_, Status* st) {
  const Duration f =
      FloorTimePoint<Duration, Unit, Localizer>(arg, options, localizer_, st);
  const Duration c =
      CeilTimePoint<Duration, Unit, Localizer>(arg, options, localizer_, st);
  return (Duration{arg} - f >= c - Duration{arg}) ? c : f;
}

template <typename Duration, typename Localizer>
Duration RoundWeekTimePoint(const int64_t arg, const RoundTemporalOptions& options,
                            Localizer localizer_, const Duration weekday_offset,
                            Status* st) {
  const Duration f = FloorWeekTimePoint<Duration, Localizer>(arg, options, localizer_,
                                                             weekday_offset, st);
  const Duration c = CeilWeekTimePoint<Duration, Localizer>(arg, options, localizer_,
                                                            weekday_offset, st);
  return (Duration{arg} - f >= c - Duration{arg}) ? c : f;
}

template <typename Duration, typename Localizer>
struct CeilTemporal {
  explicit CeilTemporal(const RoundTemporalOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)), options(*options) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status* st) const {
    Duration t;
    switch (options.unit) {
      case compute::CalendarUnit::NANOSECOND:
        t = CeilTimePoint<Duration, std::chrono::nanoseconds, Localizer>(arg, options,
                                                                         localizer_, st);
        break;
      case compute::CalendarUnit::MICROSECOND:
        t = CeilTimePoint<Duration, std::chrono::microseconds, Localizer>(arg, options,
                                                                          localizer_, st);
        break;
      case compute::CalendarUnit::MILLISECOND:
        t = CeilTimePoint<Duration, std::chrono::milliseconds, Localizer>(arg, options,
                                                                          localizer_, st);
        break;
      case compute::CalendarUnit::SECOND:
        t = CeilTimePoint<Duration, std::chrono::seconds, Localizer>(arg, options,
                                                                     localizer_, st);
        break;
      case compute::CalendarUnit::MINUTE:
        t = CeilTimePoint<Duration, minutes, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::HOUR:
        t = CeilTimePoint<Duration, std::chrono::hours, Localizer>(arg, options,
                                                                   localizer_, st);
        break;
      case compute::CalendarUnit::DAY:
        t = CeilTimePoint<Duration, days, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::WEEK:
        if (options.week_starts_monday) {
          t = CeilWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{3},
                                                     st);
        } else {
          t = CeilWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{4},
                                                     st);
        }
        break;
      case compute::CalendarUnit::MONTH: {
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, options.multiple,
                                                                options, localizer_);
        ymd += months{options.multiple};
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        break;
      }
      case compute::CalendarUnit::QUARTER: {
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, 3 * options.multiple,
                                                                options, localizer_);
        ymd += months{3 * options.multiple};
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        break;
      }
      case compute::CalendarUnit::YEAR: {
        year_month_day ymd(
            floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)));
        year y{(static_cast<int32_t>(ymd.year()) / options.multiple + 1) *
               options.multiple};
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(y / jan / 1).time_since_epoch(), st);
        break;
      }
      default:
        t = Duration{arg};
    }
    return static_cast<T>(t.count());
  }

  Localizer localizer_;
  RoundTemporalOptions options;
};

template <typename Duration, typename Localizer>
struct FloorTemporal {
  explicit FloorTemporal(const RoundTemporalOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)), options(*options) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status* st) const {
    Duration t;
    switch (options.unit) {
      case compute::CalendarUnit::NANOSECOND:
        t = FloorTimePoint<Duration, std::chrono::nanoseconds, Localizer>(arg, options,
                                                                          localizer_, st);
        break;
      case compute::CalendarUnit::MICROSECOND:
        t = FloorTimePoint<Duration, std::chrono::microseconds, Localizer>(
            arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::MILLISECOND:
        t = FloorTimePoint<Duration, std::chrono::milliseconds, Localizer>(
            arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::SECOND:
        t = FloorTimePoint<Duration, std::chrono::seconds, Localizer>(arg, options,
                                                                      localizer_, st);
        break;
      case compute::CalendarUnit::MINUTE:
        t = FloorTimePoint<Duration, minutes, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::HOUR:
        t = FloorTimePoint<Duration, std::chrono::hours, Localizer>(arg, options,
                                                                    localizer_, st);
        break;
      case compute::CalendarUnit::DAY:
        t = FloorTimePoint<Duration, days, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::WEEK:
        if (options.week_starts_monday) {
          t = FloorWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{3},
                                                      st);
        } else {
          t = FloorWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{4},
                                                      st);
        }
        break;
      case compute::CalendarUnit::MONTH: {
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, options.multiple,
                                                                options, localizer_);
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        break;
      }
      case compute::CalendarUnit::QUARTER: {
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, 3 * options.multiple,
                                                                options, localizer_);
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        break;
      }
      case compute::CalendarUnit::YEAR: {
        year_month_day ymd(
            floor<days>(localizer_.template ConvertTimePoint<Duration>(arg)));
        year y{(static_cast<int32_t>(ymd.year()) / options.multiple) * options.multiple};
        t = localizer_.template ConvertLocalToSys<Duration>(
            local_days(y / jan / 1).time_since_epoch(), st);
        break;
      }
      default:
        t = Duration{arg};
    }
    return static_cast<T>(t.count());
  }

  Localizer localizer_;
  RoundTemporalOptions options;
};

template <typename Duration, typename Localizer>
struct RoundTemporal {
  explicit RoundTemporal(const RoundTemporalOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)), options(*options) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status* st) const {
    Duration t;
    switch (options.unit) {
      case compute::CalendarUnit::NANOSECOND:
        t = RoundTimePoint<Duration, std::chrono::nanoseconds, Localizer>(arg, options,
                                                                          localizer_, st);
        break;
      case compute::CalendarUnit::MICROSECOND:
        t = RoundTimePoint<Duration, std::chrono::microseconds, Localizer>(
            arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::MILLISECOND:
        t = RoundTimePoint<Duration, std::chrono::milliseconds, Localizer>(
            arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::SECOND:
        t = RoundTimePoint<Duration, std::chrono::seconds, Localizer>(arg, options,
                                                                      localizer_, st);
        break;
      case compute::CalendarUnit::MINUTE:
        t = RoundTimePoint<Duration, minutes, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::HOUR:
        t = RoundTimePoint<Duration, std::chrono::hours, Localizer>(arg, options,
                                                                    localizer_, st);
        break;
      case compute::CalendarUnit::DAY:
        t = RoundTimePoint<Duration, days, Localizer>(arg, options, localizer_, st);
        break;
      case compute::CalendarUnit::WEEK:
        if (options.week_starts_monday) {
          t = RoundWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{3},
                                                      st);
        } else {
          t = RoundWeekTimePoint<Duration, Localizer>(arg, options, localizer_, days{4},
                                                      st);
        }
        break;
      case compute::CalendarUnit::MONTH: {
        auto t0 = localizer_.template ConvertTimePoint<Duration>(arg);
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, options.multiple,
                                                                options, localizer_);

        auto f = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        ymd += months{options.multiple};
        auto c = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);

        t = (t0.time_since_epoch() - f >= c - t0.time_since_epoch()) ? c : f;
        break;
      }
      case compute::CalendarUnit::QUARTER: {
        auto t0 = localizer_.template ConvertTimePoint<Duration>(arg);
        year_month_day ymd = GetFlooredYmd<Duration, Localizer>(arg, 3 * options.multiple,
                                                                options, localizer_);

        auto f = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);
        ymd += months{3 * options.multiple};
        auto c = localizer_.template ConvertLocalToSys<Duration>(
            local_days(ymd.year() / ymd.month() / 1).time_since_epoch(), st);

        t = (t0.time_since_epoch() - f >= c - t0.time_since_epoch()) ? c : f;
        break;
      }
      case compute::CalendarUnit::YEAR: {
        auto t0 = localizer_.template ConvertTimePoint<Duration>(arg);
        year_month_day ymd(floor<days>(t0));
        year y{(static_cast<int32_t>(ymd.year()) / options.multiple) * options.multiple};
        auto f = localizer_.template ConvertLocalToSys<Duration>(
            local_days(y / jan / 1).time_since_epoch(), st);
        auto c = localizer_.template ConvertLocalToSys<Duration>(
            local_days((y + years{options.multiple}) / jan / 1).time_since_epoch(), st);

        t = (t0.time_since_epoch() - f >= c - t0.time_since_epoch()) ? c : f;
        break;
      }
      default:
        t = Duration{arg};
    }
    return static_cast<T>(t.count());
  }

  Localizer localizer_;
  RoundTemporalOptions options;
};

// ----------------------------------------------------------------------
// Convert timestamps to a string representation with an arbitrary format

Result<std::locale> GetLocale(const std::string& locale) {
  try {
    return std::locale(locale.c_str());
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot find locale '", locale, "': ", ex.what());
  }
}

template <typename Duration, typename InType>
struct Strftime {
  const StrftimeOptions& options;
  const time_zone* tz;
  const std::locale locale;

  static Result<Strftime> Make(KernelContext* ctx, const DataType& type) {
    const StrftimeOptions& options = StrftimeState::Get(ctx);

    // This check is due to surprising %c behavior.
    // See https://github.com/HowardHinnant/date/issues/704
    if ((options.format.find("%c") != std::string::npos) && (options.locale != "C")) {
      return Status::Invalid("%c flag is not supported in non-C locales.");
    }
    const auto& timezone = GetInputTimezone(type);

    if (timezone.empty()) {
      if ((options.format.find("%z") != std::string::npos) ||
          (options.format.find("%Z") != std::string::npos)) {
        return Status::Invalid(
            "Timezone not present, cannot convert to string with timezone: ",
            options.format);
      }
    }

    ARROW_ASSIGN_OR_RAISE(const time_zone* tz,
                          LocateZone(timezone.empty() ? "UTC" : timezone));

    ARROW_ASSIGN_OR_RAISE(std::locale locale, GetLocale(options.locale));

    return Strftime{options, tz, std::move(locale)};
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& in = batch[0].array;
    ARROW_ASSIGN_OR_RAISE(auto self, Make(ctx, *in.type));
    TimestampFormatter<Duration> formatter{self.options.format, self.tz, self.locale};

    StringBuilder string_builder;
    // Presize string data using a heuristic
    {
      ARROW_ASSIGN_OR_RAISE(auto formatted, formatter(42));
      const auto string_size = static_cast<int64_t>(std::ceil(formatted.size() * 1.1));
      RETURN_NOT_OK(string_builder.Reserve(in.length));
      RETURN_NOT_OK(
          string_builder.ReserveData((in.length - in.GetNullCount()) * string_size));
    }

    auto visit_null = [&]() { return string_builder.AppendNull(); };
    auto visit_value = [&](int64_t arg) {
      ARROW_ASSIGN_OR_RAISE(auto formatted, formatter(arg));
      return string_builder.Append(std::move(formatted));
    };
    RETURN_NOT_OK(VisitArraySpanInline<InType>(in, visit_value, visit_null));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(string_builder.Finish(&out_array));
    out->value = std::move(out_array->data());
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Convert string representations of timestamps in arbitrary format to timestamps

const std::string GetZone(const std::string& format) {
  // Check for use of %z or %Z
  size_t cur = 0;
  size_t count = 0;
  std::string zone = "";
  while (cur < format.size() - 1) {
    if (format[cur] == '%') {
      count++;
      if (format[cur + 1] == 'z' && count % 2 == 1) {
        zone = "UTC";
        break;
      }
      cur++;
    } else {
      count = 0;
    }
    cur++;
  }
  return zone;
}

template <typename Duration, typename InType>
struct Strptime {
  const std::shared_ptr<TimestampParser> parser;
  const TimeUnit::type unit;
  const std::string zone;
  const bool error_is_null;

  static Result<Strptime> Make(KernelContext* ctx, const DataType& type) {
    const StrptimeOptions& options = StrptimeState::Get(ctx);

    return Strptime{TimestampParser::MakeStrptime(options.format),
                    std::move(options.unit), GetZone(options.format),
                    options.error_is_null};
  }

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& in = batch[0].array;
    ARROW_ASSIGN_OR_RAISE(auto self, Make(ctx, *in.type));

    ArraySpan* out_span = out->array_span_mutable();
    int64_t* out_data = out_span->GetValues<int64_t>(1);

    if (self.error_is_null) {
      // Set all values to non-null, and only clear bits when there is a
      // parsing error
      bit_util::SetBitmap(out_span->buffers[0].data, out_span->offset, out_span->length);

      int64_t null_count = 0;
      arrow::internal::BitmapWriter out_writer(out_span->buffers[0].data,
                                               out_span->offset, out_span->length);
      auto visit_null = [&]() {
        *out_data++ = 0;
        out_writer.Clear();
        out_writer.Next();
        null_count++;
      };
      auto visit_value = [&](std::string_view s) {
        int64_t result;
        if ((*self.parser)(s.data(), s.size(), self.unit, &result)) {
          *out_data++ = result;
        } else {
          *out_data++ = 0;
          out_writer.Clear();
          null_count++;
        }
        out_writer.Next();
      };
      VisitArraySpanInline<InType>(in, visit_value, visit_null);
      out_writer.Finish();
      out_span->null_count = null_count;
    } else {
      if (in.buffers[0].data != nullptr) {
        ::arrow::internal::CopyBitmap(in.buffers[0].data, in.offset, in.length,
                                      out_span->buffers[0].data, out_span->offset);
      } else {
        // Input is all non-null
        bit_util::SetBitmap(out_span->buffers[0].data, out_span->offset,
                            out_span->length);
      }
      auto visit_null = [&]() {
        *out_data++ = 0;
        return Status::OK();
      };
      auto visit_value = [&](std::string_view s) {
        int64_t result;
        if ((*self.parser)(s.data(), s.size(), self.unit, &result)) {
          *out_data++ = result;
          return Status::OK();
        } else {
          return Status::Invalid("Failed to parse string: '", s, "' as a scalar of type ",
                                 TimestampType(self.unit).ToString());
        }
      };
      RETURN_NOT_OK(VisitArraySpanInline<InType>(in, visit_value, visit_null));
    }
    return Status::OK();
  }
};

Result<TypeHolder> ResolveStrptimeOutput(KernelContext* ctx,
                                         const std::vector<TypeHolder>&) {
  if (!ctx->state()) {
    return Status::Invalid("strptime does not provide default StrptimeOptions");
  }
  const StrptimeOptions& options = StrptimeState::Get(ctx);
  return timestamp(options.unit, GetZone(options.format));
}

// ----------------------------------------------------------------------
// Convert timestamps from local timestamp without a timezone to timestamps with a
// timezone, interpreting the local timestamp as being in the specified timezone

Result<TypeHolder> ResolveAssumeTimezoneOutput(KernelContext* ctx,
                                               const std::vector<TypeHolder>& args) {
  const auto& in_type = checked_cast<const TimestampType&>(*args[0]);
  return timestamp(in_type.unit(), AssumeTimezoneState::Get(ctx).timezone);
}

Result<TypeHolder> ResolveLocalTimestampOutput(KernelContext* ctx,
                                               const std::vector<TypeHolder>& args) {
  const auto& in_type = checked_cast<const TimestampType&>(*args[0]);
  return timestamp(in_type.unit());
}

template <typename Duration>
struct AssumeTimezone {
  explicit AssumeTimezone(const AssumeTimezoneOptions* options, const time_zone* tz)
      : options(*options), tz_(tz) {}

  template <typename T, typename Arg0>
  T get_local_time(Arg0 arg, const time_zone* tz) const {
    return static_cast<T>(zoned_time<Duration>(tz, local_time<Duration>(Duration{arg}))
                              .get_sys_time()
                              .time_since_epoch()
                              .count());
  }

  template <typename T, typename Arg0>
  T get_local_time(Arg0 arg, const arrow_vendored::date::choose choose,
                   const time_zone* tz) const {
    return static_cast<T>(
        zoned_time<Duration>(tz, local_time<Duration>(Duration{arg}), choose)
            .get_sys_time()
            .time_since_epoch()
            .count());
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status* st) const {
    try {
      return get_local_time<T, Arg0>(arg, tz_);
    } catch (const arrow_vendored::date::nonexistent_local_time& e) {
      switch (options.nonexistent) {
        case AssumeTimezoneOptions::Nonexistent::NONEXISTENT_RAISE: {
          *st = Status::Invalid("Timestamp doesn't exist in timezone '", options.timezone,
                                "': ", e.what());
          return arg;
        }
        case AssumeTimezoneOptions::Nonexistent::NONEXISTENT_EARLIEST: {
          return get_local_time<T, Arg0>(arg, arrow_vendored::date::choose::latest, tz_) -
                 1;
        }
        case AssumeTimezoneOptions::Nonexistent::NONEXISTENT_LATEST: {
          return get_local_time<T, Arg0>(arg, arrow_vendored::date::choose::latest, tz_);
        }
      }
    } catch (const arrow_vendored::date::ambiguous_local_time& e) {
      switch (options.ambiguous) {
        case AssumeTimezoneOptions::Ambiguous::AMBIGUOUS_RAISE: {
          *st = Status::Invalid("Timestamp is ambiguous in timezone '", options.timezone,
                                "': ", e.what());
          return arg;
        }
        case AssumeTimezoneOptions::Ambiguous::AMBIGUOUS_EARLIEST: {
          return get_local_time<T, Arg0>(arg, arrow_vendored::date::choose::earliest,
                                         tz_);
        }
        case AssumeTimezoneOptions::Ambiguous::AMBIGUOUS_LATEST: {
          return get_local_time<T, Arg0>(arg, arrow_vendored::date::choose::latest, tz_);
        }
      }
    }
    return 0;
  }
  AssumeTimezoneOptions options;
  const time_zone* tz_;
};

// ----------------------------------------------------------------------
// Extract ISO calendar values from timestamp

template <typename Duration, typename Localizer>
std::array<int64_t, 3> GetIsoCalendar(int64_t arg, Localizer&& localizer) {
  const auto t = floor<days>(localizer.template ConvertTimePoint<Duration>(arg));
  const auto ymd = year_month_day(t);
  auto y = year_month_day{t + days{3}}.year();
  auto start = localizer.ConvertDays((y - years{1}) / dec / thu[last]) + (mon - thu);
  if (t < start) {
    --y;
    start = localizer.ConvertDays((y - years{1}) / dec / thu[last]) + (mon - thu);
  }
  return {static_cast<int64_t>(static_cast<int32_t>(y)),
          static_cast<int64_t>(trunc<weeks>(t - start).count() + 1),
          static_cast<int64_t>(weekday(ymd).iso_encoding())};
}

template <typename Duration, typename InType>
struct ISOCalendarWrapper {
  static Result<std::array<int64_t, 3>> Get(const Scalar& in) {
    const auto& in_val = internal::UnboxScalar<const InType>::Unbox(in);
    return GetIsoCalendar<Duration>(in_val, NonZonedLocalizer{});
  }
};

template <typename Duration>
struct ISOCalendarWrapper<Duration, TimestampType> {
  static Result<std::array<int64_t, 3>> Get(const Scalar& in) {
    const auto& in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
    const auto& timezone = GetInputTimezone(*in.type);
    if (timezone.empty()) {
      return GetIsoCalendar<Duration>(in_val, NonZonedLocalizer{});
    } else {
      ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
      return GetIsoCalendar<Duration>(in_val, ZonedLocalizer{tz});
    }
  }
};

template <typename Duration, typename InType, typename BuilderType>
struct ISOCalendarVisitValueFunction {
  static Result<std::function<Status(typename InType::c_type arg)>> Get(
      const std::vector<BuilderType*>& field_builders, const ArraySpan&,
      StructBuilder* struct_builder) {
    return [=](typename InType::c_type arg) {
      const auto iso_calendar = GetIsoCalendar<Duration>(arg, NonZonedLocalizer{});
      field_builders[0]->UnsafeAppend(iso_calendar[0]);
      field_builders[1]->UnsafeAppend(iso_calendar[1]);
      field_builders[2]->UnsafeAppend(iso_calendar[2]);
      return struct_builder->Append();
    };
  }
};

template <typename Duration, typename BuilderType>
struct ISOCalendarVisitValueFunction<Duration, TimestampType, BuilderType> {
  static Result<std::function<Status(typename TimestampType::c_type arg)>> Get(
      const std::vector<BuilderType*>& field_builders, const ArraySpan& in,
      StructBuilder* struct_builder) {
    const auto& timezone = GetInputTimezone(*in.type);
    if (timezone.empty()) {
      return [=](TimestampType::c_type arg) {
        const auto iso_calendar = GetIsoCalendar<Duration>(arg, NonZonedLocalizer{});
        field_builders[0]->UnsafeAppend(iso_calendar[0]);
        field_builders[1]->UnsafeAppend(iso_calendar[1]);
        field_builders[2]->UnsafeAppend(iso_calendar[2]);
        return struct_builder->Append();
      };
    }
    ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
    return [=](TimestampType::c_type arg) {
      const auto iso_calendar = GetIsoCalendar<Duration>(arg, ZonedLocalizer{tz});
      field_builders[0]->UnsafeAppend(iso_calendar[0]);
      field_builders[1]->UnsafeAppend(iso_calendar[1]);
      field_builders[2]->UnsafeAppend(iso_calendar[2]);
      return struct_builder->Append();
    };
  }
};

template <typename Duration, typename InType>
struct ISOCalendar {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& in = batch[0].array;
    using BuilderType = typename TypeTraits<Int64Type>::BuilderType;

    std::unique_ptr<ArrayBuilder> array_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), IsoCalendarType(), &array_builder));
    StructBuilder* struct_builder = checked_cast<StructBuilder*>(array_builder.get());
    RETURN_NOT_OK(struct_builder->Reserve(in.length));

    std::vector<BuilderType*> field_builders;
    field_builders.reserve(3);
    for (int i = 0; i < 3; i++) {
      field_builders.push_back(
          checked_cast<BuilderType*>(struct_builder->field_builder(i)));
      RETURN_NOT_OK(field_builders[i]->Reserve(1));
    }
    auto visit_null = [&]() { return struct_builder->AppendNull(); };
    std::function<Status(typename InType::c_type arg)> visit_value;
    ARROW_ASSIGN_OR_RAISE(
        visit_value, (ISOCalendarVisitValueFunction<Duration, InType, BuilderType>::Get(
                         field_builders, in, struct_builder)));
    RETURN_NOT_OK(
        VisitArraySpanInline<typename InType::PhysicalType>(in, visit_value, visit_null));
    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    out->value = std::move(out_array->data());
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Registration helpers

template <template <typename...> class Op,
          template <template <typename...> class OpExec, typename Duration,
                    typename InType, typename OutType, typename... Args>
          class ExecTemplate,
          typename OutType>
struct UnaryTemporalFactory {
  OutputType out_type;
  KernelInit init;
  std::shared_ptr<ScalarFunction> func;

  template <typename... WithTypes>
  static std::shared_ptr<ScalarFunction> Make(
      std::string name, OutputType out_type, FunctionDoc doc,
      const FunctionOptions* default_options = NULLPTR, KernelInit init = NULLPTR) {
    DCHECK_NE(sizeof...(WithTypes), 0);
    UnaryTemporalFactory self{out_type, init,
                              std::make_shared<ScalarFunction>(
                                  name, Arity::Unary(), std::move(doc), default_options)};
    AddTemporalKernels(&self, WithTypes{}...);
    return self.func;
  }

  template <typename Duration, typename InType>
  void AddKernel(InputType in_type) {
    auto exec = ExecTemplate<Op, Duration, InType, OutType>::Exec;
    ScalarKernel kernel({std::move(in_type)}, out_type, std::move(exec), init);
    DCHECK_OK(func->AddKernel(kernel));
  }
};

template <template <typename...> class Op>
struct SimpleUnaryTemporalFactory {
  OutputType out_type;
  KernelInit init;
  std::shared_ptr<ScalarFunction> func;
  NullHandling::type null_handling;

  template <typename... WithTypes>
  static std::shared_ptr<ScalarFunction> Make(
      std::string name, OutputType out_type, FunctionDoc doc,
      const FunctionOptions* default_options = NULLPTR, KernelInit init = NULLPTR,
      NullHandling::type null_handling = NullHandling::INTERSECTION) {
    DCHECK_NE(sizeof...(WithTypes), 0);
    SimpleUnaryTemporalFactory self{
        out_type, init,
        std::make_shared<ScalarFunction>(name, Arity::Unary(), std::move(doc),
                                         default_options),
        null_handling};
    AddTemporalKernels(&self, WithTypes{}...);
    return self.func;
  }

  template <typename Duration, typename InType>
  void AddKernel(InputType in_type) {
    ScalarKernel kernel({std::move(in_type)}, out_type, Op<Duration, InType>::Exec, init);
    kernel.null_handling = this->null_handling;
    DCHECK_OK(func->AddKernel(kernel));
  }
};

const FunctionDoc year_doc{
    "Extract year number",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc is_leap_year_doc{
    "Extract if year is a leap year",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc month_doc{
    "Extract month number",
    ("Month is encoded as January=1, December=12.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc day_doc{
    "Extract day number",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc year_month_day_doc{
    "Extract (year, month, day) struct",
    ("Null values emit null.\n"
     "An error is returned in the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc day_of_week_doc{
    "Extract day of the week number",
    ("By default, the week starts on Monday represented by 0 and ends on Sunday\n"
     "represented by 6.\n"
     "`DayOfWeekOptions.week_start` can be used to set another starting day using\n"
     "the ISO numbering convention (1=start week on Monday, 7=start week on Sunday).\n"
     "Day numbers can start at 0 or 1 based on `DayOfWeekOptions.count_from_zero`.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"},
    "DayOfWeekOptions"};

const FunctionDoc day_of_year_doc{
    "Extract day of year number",
    ("January 1st maps to day number 1, February 1st to 32, etc.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc iso_year_doc{
    "Extract ISO year number",
    ("First week of an ISO year has the majority (4 or more) of its days in January.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc us_year_doc{
    "Extract US epidemiological year number",
    ("First week of US epidemiological year has the majority (4 or more) of\n"
     "it's days in January. Last week of US epidemiological year has the\n"
     "year's last Wednesday in it. US epidemiological week starts on Sunday.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc iso_week_doc{
    "Extract ISO week of year number",
    ("First ISO week has the majority (4 or more) of its days in January.\n"
     "ISO week starts on Monday. The week number starts with 1 and can run\n"
     "up to 53.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc us_week_doc{
    "Extract US week of year number",
    ("First US week has the majority (4 or more) of its days in January.\n"
     "US week starts on Monday. The week number starts with 1 and can run\n"
     "up to 53.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc week_doc{
    "Extract week of year number",
    ("First week has the majority (4 or more) of its days in January.\n"
     "Year can have 52 or 53 weeks. Week numbering can start with 0 or 1 using\n"
     "DayOfWeekOptions.count_from_zero.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"},
    "WeekOptions"};

const FunctionDoc iso_calendar_doc{
    "Extract (ISO year, ISO week, ISO day of week) struct",
    ("ISO week starts on Monday denoted by 1 and ends on Sunday denoted by 7.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc quarter_doc{
    "Extract quarter of year number",
    ("First quarter maps to 1 and forth quarter maps to 4.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc hour_doc{
    "Extract hour value",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc minute_doc{
    "Extract minute values",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc second_doc{
    "Extract second values",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc millisecond_doc{
    "Extract millisecond values",
    ("Millisecond returns number of milliseconds since the last full second.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc microsecond_doc{
    "Extract microsecond values",
    ("Millisecond returns number of microseconds since the last full millisecond.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc nanosecond_doc{
    "Extract nanosecond values",
    ("Nanosecond returns number of nanoseconds since the last full microsecond.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc subsecond_doc{
    "Extract subsecond values",
    ("Subsecond returns the fraction of a second since the last full second.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc strftime_doc{
    "Format temporal values according to a format string",
    ("For each input value, emit a formatted string.\n"
     "The time format string and locale can be set using StrftimeOptions.\n"
     "The output precision of the \"%S\" (seconds) format code depends on\n"
     "the input time precision: it is an integer for timestamps with\n"
     "second precision, a real number with the required number of fractional\n"
     "digits for higher precisions.\n"
     "Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database, or if the specified locale\n"
     "does not exist on this system."),
    {"timestamps"},
    "StrftimeOptions"};
const FunctionDoc strptime_doc(
    "Parse timestamps",
    ("For each string in `strings`, parse it as a timestamp.\n"
     "The timestamp unit and the expected string pattern must be given\n"
     "in StrptimeOptions. Null inputs emit null. If a non-null string\n"
     "fails parsing, an error is returned by default."),
    {"strings"}, "StrptimeOptions", /*options_required=*/true);
const FunctionDoc assume_timezone_doc{
    "Convert naive timestamp to timezone-aware timestamp",
    ("Input timestamps are assumed to be relative to the timezone given in the\n"
     "`timezone` option. They are converted to UTC-relative timestamps and\n"
     "the output type has its timezone set to the value of the `timezone`\n"
     "option. Null values emit null.\n"
     "This function is meant to be used when an external system produces\n"
     "\"timezone-naive\" timestamps which need to be converted to\n"
     "\"timezone-aware\" timestamps. An error is returned if the timestamps\n"
     "already have a defined timezone."),
    {"timestamps"},
    "AssumeTimezoneOptions",
    /*options_required=*/true};

const FunctionDoc is_dst_doc{
    "Extracts if currently observing daylight savings",
    ("IsDaylightSavings returns true if a timestamp has a daylight saving\n"
     "offset in the given timezone.\n"
     "Null values emit null.\n"
     "An error is returned if the values do not have a defined timezone."),
    {"values"}};

const FunctionDoc local_timestamp_doc{
    "Convert timestamp to a timezone-naive local time timestamp",
    ("LocalTimestamp converts timezone-aware timestamp to local timestamp\n"
     "of the given timestamp's timezone and removes timezone metadata.\n"
     "Alternative name for this timestamp is also wall clock time.\n"
     "If input is in UTC or without timezone, then unchanged input values\n"
     "without timezone metadata are returned.\n"
     "Null values emit null."),
    {"values"}};
const FunctionDoc floor_temporal_doc{
    "Round temporal values down to nearest multiple of specified time unit",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"timestamps"},
    "RoundTemporalOptions"};
const FunctionDoc ceil_temporal_doc{
    "Round temporal values up to nearest multiple of specified time unit",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"timestamps"},
    "RoundTemporalOptions"};
const FunctionDoc round_temporal_doc{
    "Round temporal values to the nearest multiple of specified time unit",
    ("Null values emit null.\n"
     "An error is returned if the values have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"timestamps"},
    "RoundTemporalOptions"};

}  // namespace

void RegisterScalarTemporalUnary(FunctionRegistry* registry) {
  // Date extractors
  auto year =
      UnaryTemporalFactory<Year, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("year", int64(),
                                                                       year_doc);
  DCHECK_OK(registry->AddFunction(std::move(year)));

  auto is_leap_year =
      UnaryTemporalFactory<IsLeapYear, TemporalComponentExtract, BooleanType>::Make<
          WithDates, WithTimestamps>("is_leap_year", boolean(), is_leap_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(is_leap_year)));

  auto month =
      UnaryTemporalFactory<Month, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("month", int64(),
                                                                       month_doc);
  DCHECK_OK(registry->AddFunction(std::move(month)));

  auto day =
      UnaryTemporalFactory<Day, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("day", int64(),
                                                                       day_doc);
  DCHECK_OK(registry->AddFunction(std::move(day)));

  auto year_month_day =
      SimpleUnaryTemporalFactory<YearMonthDay>::Make<WithDates, WithTimestamps>(
          "year_month_day", YearMonthDayType(), year_month_day_doc);
  DCHECK_OK(registry->AddFunction(std::move(year_month_day)));

  static const auto default_day_of_week_options = DayOfWeekOptions::Defaults();
  auto day_of_week =
      UnaryTemporalFactory<DayOfWeek, TemporalComponentExtractDayOfWeek, Int64Type>::Make<
          WithDates, WithTimestamps>("day_of_week", int64(), day_of_week_doc,
                                     &default_day_of_week_options, DayOfWeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(day_of_week)));

  auto day_of_year =
      UnaryTemporalFactory<DayOfYear, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("day_of_year",
                                                                       int64(),
                                                                       day_of_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(day_of_year)));

  auto iso_year =
      UnaryTemporalFactory<ISOYear, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("iso_year",
                                                                       int64(),
                                                                       iso_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_year)));

  auto us_year =
      UnaryTemporalFactory<USYear, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("us_year", int64(),
                                                                       us_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(us_year)));

  static const auto default_iso_week_options = WeekOptions::ISODefaults();
  auto iso_week =
      UnaryTemporalFactory<Week, TemporalComponentExtractWeek, Int64Type>::Make<
          WithDates, WithTimestamps>("iso_week", int64(), iso_week_doc,
                                     &default_iso_week_options, WeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(iso_week)));

  static const auto default_us_week_options = WeekOptions::USDefaults();
  auto us_week =
      UnaryTemporalFactory<Week, TemporalComponentExtractWeek, Int64Type>::Make<
          WithDates, WithTimestamps>("us_week", int64(), us_week_doc,
                                     &default_us_week_options, WeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(us_week)));

  static const auto default_week_options = WeekOptions();
  auto week = UnaryTemporalFactory<Week, TemporalComponentExtractWeek, Int64Type>::Make<
      WithDates, WithTimestamps>("week", int64(), week_doc, &default_week_options,
                                 WeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(week)));

  auto iso_calendar =
      SimpleUnaryTemporalFactory<ISOCalendar>::Make<WithDates, WithTimestamps>(
          "iso_calendar", IsoCalendarType(), iso_calendar_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_calendar)));

  auto quarter =
      UnaryTemporalFactory<Quarter, TemporalComponentExtract,
                           Int64Type>::Make<WithDates, WithTimestamps>("quarter", int64(),
                                                                       quarter_doc);
  DCHECK_OK(registry->AddFunction(std::move(quarter)));

  // Date / time extractors
  auto hour =
      UnaryTemporalFactory<Hour, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("hour", int64(),
                                                                       hour_doc);
  DCHECK_OK(registry->AddFunction(std::move(hour)));

  auto minute =
      UnaryTemporalFactory<Minute, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("minute", int64(),
                                                                       minute_doc);
  DCHECK_OK(registry->AddFunction(std::move(minute)));

  auto second =
      UnaryTemporalFactory<Second, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("second", int64(),
                                                                       second_doc);
  DCHECK_OK(registry->AddFunction(std::move(second)));

  auto millisecond =
      UnaryTemporalFactory<Millisecond, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("millisecond",
                                                                       int64(),
                                                                       millisecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(millisecond)));

  auto microsecond =
      UnaryTemporalFactory<Microsecond, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("microsecond",
                                                                       int64(),
                                                                       microsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(microsecond)));

  auto nanosecond =
      UnaryTemporalFactory<Nanosecond, TemporalComponentExtract,
                           Int64Type>::Make<WithTimes, WithTimestamps>("nanosecond",
                                                                       int64(),
                                                                       nanosecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(nanosecond)));

  auto subsecond =
      UnaryTemporalFactory<Subsecond, TemporalComponentExtract,
                           DoubleType>::Make<WithTimes, WithTimestamps>("subsecond",
                                                                        float64(),
                                                                        subsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(subsecond)));

  // Timezone-related functions
  static const auto default_strftime_options = StrftimeOptions();
  auto strftime =
      SimpleUnaryTemporalFactory<Strftime>::Make<WithTimes, WithDates, WithTimestamps>(
          "strftime", utf8(), strftime_doc, &default_strftime_options,
          StrftimeState::Init);
  DCHECK_OK(registry->AddFunction(std::move(strftime)));

  auto strptime = SimpleUnaryTemporalFactory<Strptime>::Make<WithStringTypes>(
      "strptime", OutputType::Resolver(ResolveStrptimeOutput), strptime_doc, nullptr,
      StrptimeState::Init, NullHandling::COMPUTED_PREALLOCATE);
  DCHECK_OK(registry->AddFunction(std::move(strptime)));

  auto assume_timezone =
      UnaryTemporalFactory<AssumeTimezone, AssumeTimezoneExtractor, TimestampType>::Make<
          WithTimestamps>("assume_timezone",
                          OutputType::Resolver(ResolveAssumeTimezoneOutput),
                          assume_timezone_doc, nullptr, AssumeTimezoneState::Init);
  DCHECK_OK(registry->AddFunction(std::move(assume_timezone)));

  auto is_dst =
      UnaryTemporalFactory<IsDaylightSavings, DaylightSavingsExtractor,
                           BooleanType>::Make<WithTimestamps>("is_dst", boolean(),
                                                              is_dst_doc);
  DCHECK_OK(registry->AddFunction(std::move(is_dst)));

  auto local_timestamp =
      UnaryTemporalFactory<LocalTimestamp, TemporalComponentExtract, TimestampType>::Make<
          WithTimestamps>("local_timestamp",
                          OutputType::Resolver(ResolveLocalTimestampOutput),
                          local_timestamp_doc);
  DCHECK_OK(registry->AddFunction(std::move(local_timestamp)));

  // Temporal rounding functions
  // Note: UnaryTemporalFactory will not correctly resolve OutputType(FirstType) to
  // output type. See TemporalComponentExtractRound for more.

  static const auto default_round_temporal_options = RoundTemporalOptions::Defaults();
  auto floor_temporal = UnaryTemporalFactory<FloorTemporal, TemporalComponentExtractRound,
                                             TimestampType>::Make<WithDates, WithTimes,
                                                                  WithTimestamps>(
      "floor_temporal", OutputType(FirstType), floor_temporal_doc,
      &default_round_temporal_options, RoundTemporalState::Init);
  DCHECK_OK(registry->AddFunction(std::move(floor_temporal)));
  auto ceil_temporal = UnaryTemporalFactory<CeilTemporal, TemporalComponentExtractRound,
                                            TimestampType>::Make<WithDates, WithTimes,
                                                                 WithTimestamps>(
      "ceil_temporal", OutputType(FirstType), ceil_temporal_doc,
      &default_round_temporal_options, RoundTemporalState::Init);
  DCHECK_OK(registry->AddFunction(std::move(ceil_temporal)));
  auto round_temporal = UnaryTemporalFactory<RoundTemporal, TemporalComponentExtractRound,
                                             TimestampType>::Make<WithDates, WithTimes,
                                                                  WithTimestamps>(
      "round_temporal", OutputType(FirstType), round_temporal_doc,
      &default_round_temporal_options, RoundTemporalState::Init);
  DCHECK_OK(registry->AddFunction(std::move(round_temporal)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
