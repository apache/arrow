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
using arrow_vendored::date::sys_time;
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

const std::string& GetInputTimezone(const Datum& datum) {
  return checked_cast<const TimestampType&>(*datum.type()).timezone();
}

const std::string& GetInputTimezone(const Scalar& scalar) {
  return checked_cast<const TimestampType&>(*scalar.type).timezone();
}

const std::string& GetInputTimezone(const ArrayData& array) {
  return checked_cast<const TimestampType&>(*array.type).timezone();
}

template <typename T>
Status TemporalComponentExtractCheckTimezone(const T& input) {
  const auto& timezone = GetInputTimezone(input);
  if (!timezone.empty()) {
    return Status::NotImplemented(
        "Cannot extract components from timestamp with specific timezone: ", timezone);
  }
  return Status::OK();
}

template <typename Op, typename OutType>
struct TemporalComponentExtract {
  using OutValue = typename internal::GetOutputType<OutType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    RETURN_NOT_OK(TemporalComponentExtractCheckTimezone(batch.values[0]));
    return ScalarUnaryNotNull<OutType, TimestampType, Op>::Exec(ctx, batch, out);
  }
};

template <typename Op, typename OutType>
struct DayOfWeekExec {
  using OutValue = typename internal::GetOutputType<OutType>::T;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const DayOfWeekOptions& options = DayOfWeekState::Get(ctx);
    if (options.week_start < 1 || 7 < options.week_start) {
      return Status::Invalid(
          "week_start must follow ISO convention (Monday=1, Sunday=7). Got week_start=",
          options.week_start);
    }

    RETURN_NOT_OK(TemporalComponentExtractCheckTimezone(batch.values[0]));
    applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, Op> kernel{
        Op(options)};
    return kernel.Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename Duration>
struct Year {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    return static_cast<T>(static_cast<const int32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).year()));
  }
};

// ----------------------------------------------------------------------
// Extract month from timestamp

template <typename Duration>
struct Month {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).month()));
  }
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename Duration>
struct Day {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    return static_cast<T>(static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{arg}))).day()));
  }
};

// ----------------------------------------------------------------------
// Extract day of week from timestamp
//
// By default week starts on Monday represented by 0 and ends on Sunday represented
// by 6. Start day of the week (Monday=1, Sunday=7) and numbering start (0 or 1) can be
// set using DayOfWeekOptions

template <typename Duration>
struct DayOfWeek {
  explicit DayOfWeek(const DayOfWeekOptions& options) {
    for (int i = 0; i < 7; i++) {
      lookup_table[i] = i + 8 - options.week_start;
      lookup_table[i] = (lookup_table[i] > 6) ? lookup_table[i] - 7 : lookup_table[i];
      lookup_table[i] += options.one_based_numbering;
    }
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto wd = arrow_vendored::date::year_month_weekday(
                        floor<days>(sys_time<Duration>(Duration{arg})))
                        .weekday()
                        .iso_encoding();
    return lookup_table[wd - 1];
  }
  std::array<int64_t, 7> lookup_table;
};

// ----------------------------------------------------------------------
// Extract day of year from timestamp

template <typename Duration>
struct DayOfYear {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    const auto t = floor<days>(sys_time<Duration>(Duration{arg}));
    return static_cast<T>(
        (t - sys_time<days>(year_month_day(t).year() / jan / 0)).count());
  }
};

// ----------------------------------------------------------------------
// Extract ISO Year values from timestamp
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.

template <typename Duration>
struct ISOYear {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    const auto t = floor<days>(sys_time<Duration>(Duration{arg}));
    auto y = year_month_day{t + days{3}}.year();
    auto start = sys_time<days>((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
    }
    return static_cast<T>(static_cast<int32_t>(y));
  }
};

// ----------------------------------------------------------------------
// Extract ISO week from timestamp
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.
// Based on
// https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503
template <typename Duration>
struct ISOWeek {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    const auto t = floor<days>(sys_time<Duration>(Duration{arg}));
    auto y = year_month_day{t + days{3}}.year();
    auto start = sys_time<days>((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
      start = sys_time<days>((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return static_cast<T>(trunc<weeks>(t - start).count() + 1);
  }
};

// ----------------------------------------------------------------------
// Extract quarter from timestamp

template <typename Duration>
struct Quarter {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    const auto ymd = year_month_day(floor<days>(sys_time<Duration>(Duration{arg})));
    return static_cast<T>((static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1);
  }
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename Duration>
struct Hour {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<days>(t)) / std::chrono::hours(1));
  }
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename Duration>
struct Minute {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<std::chrono::hours>(t)) / std::chrono::minutes(1));
  }
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename Duration>
struct Second {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>((t - floor<std::chrono::minutes>(t)) / std::chrono::seconds(1));
  }
};

// ----------------------------------------------------------------------
// Extract subsecond from timestamp

template <typename Duration>
struct Subsecond {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        (std::chrono::duration<double>(t - floor<std::chrono::seconds>(t)).count()));
  }
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename Duration>
struct Millisecond {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::milliseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename Duration>
struct Microsecond {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::microseconds(1)) % 1000);
  }
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename Duration>
struct Nanosecond {
  template <typename T, typename Arg0>
  static T Call(KernelContext*, Arg0 arg, Status*) {
    Duration t = Duration{arg};
    return static_cast<T>(
        ((t - floor<std::chrono::seconds>(t)) / std::chrono::nanoseconds(1)) % 1000);
  }
};

template <typename Duration>
inline std::vector<int64_t> get_iso_calendar(int64_t arg) {
  const auto t = floor<days>(sys_time<Duration>(Duration{arg}));
  const auto ymd = year_month_day(t);
  auto y = year_month_day{t + days{3}}.year();
  auto start = sys_time<days>((y - years{1}) / dec / thu[last]) + (mon - thu);
  if (t < start) {
    --y;
    start = sys_time<days>((y - years{1}) / dec / thu[last]) + (mon - thu);
  }
  return {static_cast<int64_t>(static_cast<int32_t>(y)),
          static_cast<int64_t>(trunc<weeks>(t - start).count() + 1),
          static_cast<int64_t>(weekday(ymd).iso_encoding())};
}

// ----------------------------------------------------------------------
// Extract ISO calendar values from timestamp

template <typename Duration>
struct ISOCalendar {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    RETURN_NOT_OK(TemporalComponentExtractCheckTimezone(in));
    if (in.is_valid) {
      const std::shared_ptr<DataType> iso_calendar_type =
          struct_({field("iso_year", int64()), field("iso_week", int64()),
                   field("iso_day_of_week", int64())});
      const auto& in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
      const auto iso_calendar = get_iso_calendar<Duration>(in_val);

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

    RETURN_NOT_OK(TemporalComponentExtractCheckTimezone(in));
    const std::shared_ptr<DataType> iso_calendar_type =
        struct_({field("iso_year", int64()), field("iso_week", int64()),
                 field("iso_day_of_week", int64())});

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
    auto visit_value = [&](int64_t arg) {
      const auto iso_calendar = get_iso_calendar<Duration>(arg);
      field_builders[0]->UnsafeAppend(iso_calendar[0]);
      field_builders[1]->UnsafeAppend(iso_calendar[1]);
      field_builders[2]->UnsafeAppend(iso_calendar[2]);
      return struct_builder->Append();
    };
    RETURN_NOT_OK(VisitArrayDataInline<Int64Type>(in, visit_value, visit_null));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    *out = *std::move(out_array->data());

    return Status::OK();
  }
};

template <template <typename...> class Op, typename OutType>
std::shared_ptr<ScalarFunction> MakeTemporal(std::string name, const FunctionDoc* doc) {
  const auto& out_type = TypeTraits<OutType>::type_singleton();
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  for (auto unit : internal::AllTimeUnits()) {
    InputType in_type{match::TimestampTypeUnit(unit)};
    switch (unit) {
      case TimeUnit::SECOND: {
        auto exec = TemporalComponentExtract<Op<std::chrono::seconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::MILLI: {
        auto exec =
            TemporalComponentExtract<Op<std::chrono::milliseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::MICRO: {
        auto exec =
            TemporalComponentExtract<Op<std::chrono::microseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::NANO: {
        auto exec = TemporalComponentExtract<Op<std::chrono::nanoseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
    }
  }
  return func;
}

template <template <typename...> class Op, typename OutType>
std::shared_ptr<ScalarFunction> MakeTemporalWithOptions(
    std::string name, const FunctionDoc* doc, const DayOfWeekOptions& default_options,
    KernelInit init) {
  const auto& out_type = TypeTraits<OutType>::type_singleton();
  auto func =
      std::make_shared<ScalarFunction>(name, Arity::Unary(), doc, &default_options);

  for (auto unit : internal::AllTimeUnits()) {
    InputType in_type{match::TimestampTypeUnit(unit)};
    switch (unit) {
      case TimeUnit::SECOND: {
        auto exec = DayOfWeekExec<Op<std::chrono::seconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MILLI: {
        auto exec = DayOfWeekExec<Op<std::chrono::milliseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::MICRO: {
        auto exec = DayOfWeekExec<Op<std::chrono::microseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
      case TimeUnit::NANO: {
        auto exec = DayOfWeekExec<Op<std::chrono::nanoseconds>, OutType>::Exec;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
        break;
      }
    }
  }
  return func;
}

template <template <typename...> class Op>
std::shared_ptr<ScalarFunction> MakeStructTemporal(std::string name,
                                                   const FunctionDoc* doc) {
  const auto& out_type = struct_({field("iso_year", int64()), field("iso_week", int64()),
                                  field("iso_day_of_week", int64())});
  auto func = std::make_shared<ScalarFunction>(name, Arity::Unary(), doc);

  for (auto unit : internal::AllTimeUnits()) {
    InputType in_type{match::TimestampTypeUnit(unit)};
    switch (unit) {
      case TimeUnit::SECOND: {
        auto exec = SimpleUnary<Op<std::chrono::seconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::MILLI: {
        auto exec = SimpleUnary<Op<std::chrono::milliseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::MICRO: {
        auto exec = SimpleUnary<Op<std::chrono::microseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
        break;
      }
      case TimeUnit::NANO: {
        auto exec = SimpleUnary<Op<std::chrono::nanoseconds>>;
        DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec)));
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
  auto year = MakeTemporal<Year, Int64Type>("year", &year_doc);
  DCHECK_OK(registry->AddFunction(std::move(year)));

  auto month = MakeTemporal<Month, Int64Type>("month", &year_doc);
  DCHECK_OK(registry->AddFunction(std::move(month)));

  auto day = MakeTemporal<Day, Int64Type>("day", &year_doc);
  DCHECK_OK(registry->AddFunction(std::move(day)));

  static auto default_day_of_week_options = DayOfWeekOptions::Defaults();
  auto day_of_week = MakeTemporalWithOptions<DayOfWeek, Int64Type>(
      "day_of_week", &day_of_week_doc, default_day_of_week_options, DayOfWeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(day_of_week)));

  auto day_of_year = MakeTemporal<DayOfYear, Int64Type>("day_of_year", &day_of_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(day_of_year)));

  auto iso_year = MakeTemporal<ISOYear, Int64Type>("iso_year", &iso_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_year)));

  auto iso_week = MakeTemporal<ISOWeek, Int64Type>("iso_week", &iso_week_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_week)));

  auto iso_calendar = MakeStructTemporal<ISOCalendar>("iso_calendar", &iso_calendar_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_calendar)));

  auto quarter = MakeTemporal<Quarter, Int64Type>("quarter", &quarter_doc);
  DCHECK_OK(registry->AddFunction(std::move(quarter)));

  auto hour = MakeTemporal<Hour, Int64Type>("hour", &hour_doc);
  DCHECK_OK(registry->AddFunction(std::move(hour)));

  auto minute = MakeTemporal<Minute, Int64Type>("minute", &minute_doc);
  DCHECK_OK(registry->AddFunction(std::move(minute)));

  auto second = MakeTemporal<Second, Int64Type>("second", &second_doc);
  DCHECK_OK(registry->AddFunction(std::move(second)));

  auto millisecond =
      MakeTemporal<Millisecond, Int64Type>("millisecond", &millisecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(millisecond)));

  auto microsecond =
      MakeTemporal<Microsecond, Int64Type>("microsecond", &microsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(microsecond)));

  auto nanosecond = MakeTemporal<Nanosecond, Int64Type>("nanosecond", &nanosecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(nanosecond)));

  auto subsecond = MakeTemporal<Subsecond, DoubleType>("subsecond", &subsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(subsecond)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
