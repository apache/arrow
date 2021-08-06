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

#include "arrow/builder.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
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
using arrow_vendored::date::years;
using arrow_vendored::date::literals::dec;
using arrow_vendored::date::literals::jan;
using arrow_vendored::date::literals::last;
using arrow_vendored::date::literals::mon;
using arrow_vendored::date::literals::thu;
using internal::applicator::ScalarUnaryNotNull;
using internal::applicator::SimpleUnary;

using DayOfWeekState = OptionsWrapper<DayOfWeekOptions>;
const auto& iso_calendar_type =
    struct_({field("iso_year", int64()), field("iso_week", int64()),
             field("iso_day_of_week", int64())});

const std::string& GetInputTimezone(const Datum& datum) {
  return checked_cast<const TimestampType&>(*datum.type()).timezone();
}

const std::string& GetInputTimezone(const Scalar& scalar) {
  return checked_cast<const TimestampType&>(*scalar.type).timezone();
}

const std::string& GetInputTimezone(const ArrayData& array) {
  return checked_cast<const TimestampType&>(*array.type).timezone();
}

template <
    template <typename Duration, typename TimePointConverter, typename DaysConverter>
    class Op,
    typename Duration, typename OutType>
struct TemporalComponentExtract {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& timezone = GetInputTimezone(batch.values[0]);
    if (timezone.empty()) {
      using ExecTemplate = Op<Duration, std::function<sys_time<Duration>(int64_t)>,
                              std::function<sys_days(sys_days)>>;
      auto op = ExecTemplate([](int64_t t) { return sys_time<Duration>(Duration{t}); },
                             [](sys_days d) { return d; });
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    } else {
      const time_zone* tz;
      try {
        tz = locate_zone(timezone);
      } catch (const std::runtime_error& ex) {
        return Status::Invalid(ex.what());
      }
      using ExecTemplate = Op<Duration, std::function<local_time<Duration>(int64_t)>,
                              std::function<local_days(sys_days)>>;
      auto op = ExecTemplate(
          [tz](int64_t t) { return tz->to_local(sys_time<Duration>(Duration{t})); },
          [](sys_days d) { return local_days(year_month_day(d)); });
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    }
  }
};

template <
    template <typename Duration, typename TimePointConverter, typename DaysConverter>
    class Op,
    typename Duration, typename OutType>
struct TemporalComponentExtractDayOfWeek {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& timezone = GetInputTimezone(batch.values[0]);
    const DayOfWeekOptions& options = DayOfWeekState::Get(ctx);
    if (options.week_start < 1 || 7 < options.week_start) {
      return Status::Invalid(
          "week_start must follow ISO convention (Monday=1, Sunday=7). Got week_start=",
          options.week_start);
    }

    if (timezone.empty()) {
      using ExecTemplate = Op<Duration, std::function<sys_time<Duration>(int64_t)>,
                              std::function<sys_days(sys_days)>>;
      auto op = ExecTemplate(
          options, [](int64_t t) { return sys_time<Duration>(Duration{t}); },
          [](sys_days d) { return d; });
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    } else {
      const time_zone* tz;
      try {
        tz = locate_zone(timezone);
      } catch (const std::runtime_error& ex) {
        return Status::Invalid(ex.what());
      }
      using ExecTemplate = Op<Duration, std::function<local_time<Duration>(int64_t)>,
                              std::function<local_days(sys_days)>>;
      auto op = ExecTemplate(
          options,
          [tz](int64_t t) { return tz->to_local(sys_time<Duration>(Duration{t})); },
          [](sys_days d) { return local_days(year_month_day(d)); });
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    }
  }
};

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Year {
  explicit Year(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const int32_t>(
        year_month_day(floor<days>(convert_timepoint_(arg))).year()));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract month from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Month {
  explicit Month(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(convert_timepoint_(arg))).month()));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Day {
  explicit Day(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(convert_timepoint_(arg))).day()));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract day of week from timestamp
//
// By default week starts on Monday represented by 0 and ends on Sunday represented
// by 6. Start day of the week (Monday=1, Sunday=7) and numbering start (0 or 1) can be
// set using DayOfWeekOptions

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct DayOfWeek {
  explicit DayOfWeek(const DayOfWeekOptions& options,
                     TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {
    for (int i = 0; i < 7; i++) {
      lookup_table[i] = i + 8 - options.week_start;
      lookup_table[i] = (lookup_table[i] > 6) ? lookup_table[i] - 7 : lookup_table[i];
      lookup_table[i] += options.one_based_numbering;
    }
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto wd =
        arrow_vendored::date::year_month_weekday(floor<days>(convert_timepoint_(arg)))
            .weekday()
            .iso_encoding();
    return lookup_table[wd - 1];
  }
  std::array<int64_t, 7> lookup_table;
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract day of year from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct DayOfYear {
  explicit DayOfYear(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(convert_timepoint_(arg));
    return static_cast<T>(
        (t - convert_days_(year_month_day(t).year() / jan / 0)).count());
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract ISO Year values from timestamp
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct ISOYear {
  explicit ISOYear(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(convert_timepoint_(arg));
    auto y = year_month_day{t + days{3}}.year();
    auto start = convert_days_((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
    }
    return static_cast<T>(static_cast<int32_t>(y));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract ISO week from timestamp
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.
// Based on
// https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct ISOWeek {
  explicit ISOWeek(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(convert_timepoint_(arg));
    auto y = year_month_day{t + days{3}}.year();
    auto start = convert_days_((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
      start = convert_days_((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return static_cast<T>(trunc<weeks>(t - start).count() + 1);
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract quarter from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Quarter {
  explicit Quarter(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto ymd = year_month_day(floor<days>(convert_timepoint_(arg)));
    return static_cast<T>((static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1);
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Hour {
  explicit Hour(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = convert_timepoint_(arg);
    return static_cast<T>((t - floor<days>(t)) / std::chrono::hours(1));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Minute {
  explicit Minute(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = convert_timepoint_(arg);
    return static_cast<T>((t - floor<std::chrono::hours>(t)) / std::chrono::minutes(1));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Second {
  explicit Second(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<std::chrono::minutes>(t)) / std::chrono::seconds(1));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract subsecond from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Subsecond {
  explicit Subsecond(TimePointConverter&& convert_timepoint, DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        (std::chrono::duration<double>(t - floor<std::chrono::seconds>(t)).count()));
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Millisecond {
  explicit Millisecond(TimePointConverter&& convert_timepoint,
                       DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::milliseconds(1)) % 1000);
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Microsecond {
  explicit Microsecond(TimePointConverter&& convert_timepoint,
                       DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::microseconds(1)) % 1000);
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
struct Nanosecond {
  explicit Nanosecond(TimePointConverter&& convert_timepoint,
                      DaysConverter&& convert_days)
      : convert_timepoint_(convert_timepoint), convert_days_(convert_days) {}

  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::nanoseconds(1)) % 1000);
  }
  TimePointConverter convert_timepoint_;
  DaysConverter convert_days_;
};

// ----------------------------------------------------------------------
// Extract ISO calendar values from timestamp

template <typename Duration, typename TimePointConverter, typename DaysConverter>
inline std::vector<int64_t> get_iso_calendar(int64_t arg,
                                             TimePointConverter&& convert_timepoint,
                                             DaysConverter&& convert_days) {
  const auto t = floor<days>(convert_timepoint(arg));
  const auto ymd = year_month_day(t);
  auto y = year_month_day{t + days{3}}.year();
  auto start = convert_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  if (t < start) {
    --y;
    start = convert_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  }
  return {static_cast<int64_t>(static_cast<int32_t>(y)),
          static_cast<int64_t>(trunc<weeks>(t - start).count() + 1),
          static_cast<int64_t>(weekday(ymd).iso_encoding())};
}

template <typename Duration>
struct ISOCalendar {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    std::string timezone = GetInputTimezone(in);

    if (in.is_valid) {
      const auto& in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
      std::vector<int64_t> iso_calendar;
      if (timezone.empty()) {
        iso_calendar = get_iso_calendar<Duration>(
            in_val, [](int64_t t) { return sys_time<Duration>(Duration{t}); },
            [](sys_days d) { return d; });
      } else {
        try {
          const time_zone* tz = locate_zone(timezone);
          iso_calendar = get_iso_calendar<Duration>(
              in_val,
              [tz](int64_t t) { return tz->to_local(sys_time<Duration>(Duration{t})); },
              [](sys_days d) { return local_days(year_month_day(d)); });
        } catch (const std::runtime_error& ex) {
          return Status::Invalid(ex.what());
        }
      }
      std::vector<std::shared_ptr<Scalar>> values = {
          std::make_shared<Int64Scalar>(iso_calendar[0]),
          std::make_shared<Int64Scalar>(iso_calendar[1]),
          std::make_shared<Int64Scalar>(iso_calendar[2])};
      *checked_cast<StructScalar*>(out) = StructScalar(values, iso_calendar_type);
    } else {
      out->is_valid = false;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    using BuilderType = typename TypeTraits<Int64Type>::BuilderType;
    std::string timezone = GetInputTimezone(in);

    std::unique_ptr<ArrayBuilder> array_builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), iso_calendar_type, &array_builder));
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
    if (timezone.empty()) {
      auto visit_value = [&](int64_t arg) {
        const auto iso_calendar = get_iso_calendar<Duration>(
            arg, [](int64_t t) { return sys_time<Duration>(Duration{t}); },
            [](sys_days d) { return d; });
        field_builders[0]->UnsafeAppend(iso_calendar[0]);
        field_builders[1]->UnsafeAppend(iso_calendar[1]);
        field_builders[2]->UnsafeAppend(iso_calendar[2]);
        return struct_builder->Append();
      };
      RETURN_NOT_OK(VisitArrayDataInline<Int64Type>(in, visit_value, visit_null));
    } else {
      try {
        const time_zone* tz = locate_zone(timezone);
        auto visit_value = [&](int64_t arg) {
          const auto iso_calendar = get_iso_calendar<Duration>(
              arg,
              [tz](int64_t t) { return tz->to_local(sys_time<Duration>(Duration{t})); },
              [](sys_days d) { return local_days(year_month_day(d)); });
          field_builders[0]->UnsafeAppend(iso_calendar[0]);
          field_builders[1]->UnsafeAppend(iso_calendar[1]);
          field_builders[2]->UnsafeAppend(iso_calendar[2]);
          return struct_builder->Append();
        };
        RETURN_NOT_OK(VisitArrayDataInline<Int64Type>(in, visit_value, visit_null));
      } catch (const std::runtime_error& ex) {
        return Status::Invalid(ex.what());
      }
    }

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    *out = *std::move(out_array->data());
    return Status::OK();
  }
};

template <
    template <typename Duration, typename TimePointConverter, typename DaysConverter>
    class Op,
    template <
        template <typename Duration, typename TimePointConverter, typename DaysConverter>
        class OpExec,
        typename Duration, typename OutType>
    class ExecTemplate,
    typename OutType>
std::shared_ptr<ScalarFunction> MakeTemporal(
    std::string name, const std::shared_ptr<arrow::DataType> out_type,
    const FunctionDoc* doc, const FunctionOptions* default_options = NULLPTR,
    KernelInit init = NULLPTR) {
  auto func =
      std::make_shared<ScalarFunction>(name, Arity::Unary(), doc, default_options);

  for (auto unit : internal::AllTimeUnits()) {
    InputType in_type{match::TimestampTypeUnit(unit)};
    switch (unit) {
      case TimeUnit::SECOND: {
        auto exec = ExecTemplate<Op, std::chrono::seconds, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MILLI: {
        auto exec = ExecTemplate<Op, std::chrono::milliseconds, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MICRO: {
        auto exec = ExecTemplate<Op, std::chrono::microseconds, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::NANO: {
        auto exec = ExecTemplate<Op, std::chrono::nanoseconds, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
    }
  }
  return func;
}

template <template <typename...> class Op>
std::shared_ptr<ScalarFunction> MakeSimpleUnaryTemporal(
    std::string name, const std::shared_ptr<arrow::DataType> out_type,
    const FunctionDoc* doc, const FunctionOptions* default_options = NULLPTR,
    KernelInit init = NULLPTR) {
  auto func =
      std::make_shared<ScalarFunction>(name, Arity::Unary(), doc, default_options);

  for (auto unit : internal::AllTimeUnits()) {
    InputType in_type{match::TimestampTypeUnit(unit)};
    switch (unit) {
      case TimeUnit::SECOND: {
        auto exec = SimpleUnary<Op<std::chrono::seconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MILLI: {
        auto exec = SimpleUnary<Op<std::chrono::milliseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MICRO: {
        auto exec = SimpleUnary<Op<std::chrono::microseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::NANO: {
        auto exec = SimpleUnary<Op<std::chrono::nanoseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
    }
  }
  return func;
}

const FunctionDoc year_doc{
    "Extract year from timestamp",
    "Returns an error if timestamp has a defined timezone. Null values return null.",
    {"values"}};

const FunctionDoc month_doc{
    "Extract month number",
    ("Month is encoded as January=1, December=12.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc day_doc{
    "Extract day number",
    "Returns an error if timestamp has a defined timezone. Null values return null.",
    {"values"}};

const FunctionDoc day_of_week_doc{
    "Extract day of the week number",
    ("By default, the week starts on Monday represented by 0 and ends on Sunday "
     "represented by 6.\n"
     "DayOfWeekOptions.week_start can be used to set another starting day using ISO "
     "convention (Monday=1, Sunday=7). Day numbering can start with 0 or 1 using "
     "DayOfWeekOptions.one_based_numbering parameter.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"},
    "DayOfWeekOptions"};

const FunctionDoc day_of_year_doc{
    "Extract number of day of year",
    ("January 1st maps to day number 1, February 1st to 32, etc.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc iso_year_doc{
    "Extract ISO year number",
    ("First week of an ISO year has the majority (4 or more) of its days in January."
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc iso_week_doc{
    "Extract ISO week of year number",
    ("First ISO week has the majority (4 or more) of its days in January.\n"
     "Week of the year starts with 1 and can run up to 53.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc iso_calendar_doc{
    "Extract (ISO year, ISO week, ISO day of week) struct",
    ("ISO week starts on Monday denoted by 1 and ends on Sunday denoted by 7.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc quarter_doc{
    "Extract quarter of year number",
    ("First quarter maps to 1 and forth quarter maps to 4.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc hour_doc{
    "Extract hour value",
    "Returns an error if timestamp has a defined timezone. Null values return null.",
    {"values"}};

const FunctionDoc minute_doc{
    "Extract minute values",
    "Returns an error if timestamp has a defined timezone. Null values return null.",
    {"values"}};

const FunctionDoc second_doc{
    "Extract second values",
    "Returns an error if timestamp has a defined timezone. Null values return null.",
    {"values"}};

const FunctionDoc millisecond_doc{
    "Extract millisecond values",
    ("Millisecond returns number of milliseconds since the last full second.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc microsecond_doc{
    "Extract microsecond values",
    ("Millisecond returns number of microseconds since the last full millisecond.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc nanosecond_doc{
    "Extract nanosecond values",
    ("Nanosecond returns number of nanoseconds since the last full microsecond.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

const FunctionDoc subsecond_doc{
    "Extract subsecond values",
    ("Subsecond returns the fraction of a second since the last full second.\n"
     "Returns an error if timestamp has a defined timezone. Null values return null."),
    {"values"}};

}  // namespace

void RegisterScalarTemporal(FunctionRegistry* registry) {
  auto year =
      MakeTemporal<Year, TemporalComponentExtract, Int64Type>("year", int64(), &year_doc);
  DCHECK_OK(registry->AddFunction(std::move(year)));

  auto month = MakeTemporal<Month, TemporalComponentExtract, Int64Type>("month", int64(),
                                                                        &month_doc);
  DCHECK_OK(registry->AddFunction(std::move(month)));

  auto day =
      MakeTemporal<Day, TemporalComponentExtract, Int64Type>("day", int64(), &day_doc);
  DCHECK_OK(registry->AddFunction(std::move(day)));

  static auto default_day_of_week_options = DayOfWeekOptions::Defaults();
  auto day_of_week =
      MakeTemporal<DayOfWeek, TemporalComponentExtractDayOfWeek, Int64Type>(
          "day_of_week", int64(), &day_of_week_doc, &default_day_of_week_options,
          DayOfWeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(day_of_week)));

  auto day_of_year = MakeTemporal<DayOfYear, TemporalComponentExtract, Int64Type>(
      "day_of_year", int64(), &day_of_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(day_of_year)));

  auto iso_year = MakeTemporal<ISOYear, TemporalComponentExtract, Int64Type>(
      "iso_year", int64(), &iso_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_year)));

  auto iso_week = MakeTemporal<ISOWeek, TemporalComponentExtract, Int64Type>(
      "iso_week", int64(), &iso_week_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_week)));

  auto iso_calendar = MakeSimpleUnaryTemporal<ISOCalendar>(
      "iso_calendar", iso_calendar_type, &iso_calendar_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_calendar)));

  auto quarter = MakeTemporal<Quarter, TemporalComponentExtract, Int64Type>(
      "quarter", int64(), &quarter_doc);
  DCHECK_OK(registry->AddFunction(std::move(quarter)));

  auto hour =
      MakeTemporal<Hour, TemporalComponentExtract, Int64Type>("hour", int64(), &hour_doc);
  DCHECK_OK(registry->AddFunction(std::move(hour)));

  auto minute = MakeTemporal<Minute, TemporalComponentExtract, Int64Type>(
      "minute", int64(), &minute_doc);
  DCHECK_OK(registry->AddFunction(std::move(minute)));

  auto second = MakeTemporal<Second, TemporalComponentExtract, Int64Type>(
      "second", int64(), &second_doc);
  DCHECK_OK(registry->AddFunction(std::move(second)));

  auto millisecond = MakeTemporal<Millisecond, TemporalComponentExtract, Int64Type>(
      "millisecond", int64(), &millisecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(millisecond)));

  auto microsecond = MakeTemporal<Microsecond, TemporalComponentExtract, Int64Type>(
      "microsecond", int64(), &microsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(microsecond)));

  auto nanosecond = MakeTemporal<Nanosecond, TemporalComponentExtract, Int64Type>(
      "nanosecond", int64(), &nanosecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(nanosecond)));

  auto subsecond = MakeTemporal<Subsecond, TemporalComponentExtract, DoubleType>(
      "subsecond", float64(), &subsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(subsecond)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
