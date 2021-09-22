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
using arrow_vendored::date::zoned_time;
using arrow_vendored::date::literals::dec;
using arrow_vendored::date::literals::jan;
using arrow_vendored::date::literals::last;
using arrow_vendored::date::literals::mon;
using arrow_vendored::date::literals::thu;
using internal::applicator::ScalarUnaryNotNull;
using internal::applicator::SimpleUnary;

using DayOfWeekState = OptionsWrapper<DayOfWeekOptions>;
using StrftimeState = OptionsWrapper<StrftimeOptions>;
using AssumeTimezoneState = OptionsWrapper<AssumeTimezoneOptions>;

const std::shared_ptr<DataType>& IsoCalendarType() {
  static auto type = struct_({field("iso_year", int64()), field("iso_week", int64()),
                              field("iso_day_of_week", int64())});
  return type;
}

const std::string& GetInputTimezone(const DataType& type) {
  return checked_cast<const TimestampType&>(type).timezone();
}

const std::string& GetInputTimezone(const Datum& datum) {
  return checked_cast<const TimestampType&>(*datum.type()).timezone();
}

const std::string& GetInputTimezone(const Scalar& scalar) {
  return checked_cast<const TimestampType&>(*scalar.type).timezone();
}

const std::string& GetInputTimezone(const ArrayData& array) {
  return checked_cast<const TimestampType&>(*array.type).timezone();
}

Result<const time_zone*> LocateZone(const std::string& timezone) {
  try {
    return locate_zone(timezone);
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot locate timezone '", timezone, "': ", ex.what());
  }
}

Result<std::locale> GetLocale(const std::string& locale) {
  try {
    return std::locale(locale.c_str());
  } catch (const std::runtime_error& ex) {
    return Status::Invalid("Cannot find locale '", locale, "': ", ex.what());
  }
}

struct NonZonedLocalizer {
  // No-op conversions: UTC -> UTC
  template <typename Duration>
  sys_time<Duration> ConvertTimePoint(int64_t t) const {
    return sys_time<Duration>(Duration{t});
  }

  sys_days ConvertDays(sys_days d) const { return d; }
};

struct ZonedLocalizer {
  // Timezone-localizing conversions: UTC -> local time
  const time_zone* tz;

  template <typename Duration>
  local_time<Duration> ConvertTimePoint(int64_t t) const {
    return tz->to_local(sys_time<Duration>(Duration{t}));
  }

  local_days ConvertDays(sys_days d) const { return local_days(year_month_day(d)); }
};

//
// Executor class for temporal component extractors, i.e. scalar kernels
// with the signature temporal type -> <non-temporal scalar type `OutType`>
//
// The `Op` parameter is templated on the Duration (which depends on the timestamp
// unit) and a Localizer class (depending on whether the timestamp has a
// timezone defined).
//
template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtractBase {
  template <typename OptionsType>
  static Status ExecWithOptions(KernelContext* ctx, const OptionsType* options,
                                const ExecBatch& batch, Datum* out) {
    const auto& timezone = GetInputTimezone(batch.values[0]);
    if (timezone.empty()) {
      using ExecTemplate = Op<Duration, NonZonedLocalizer>;
      auto op = ExecTemplate(options, NonZonedLocalizer());
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
      return kernel.Exec(ctx, batch, out);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto tz, LocateZone(timezone));
      using ExecTemplate = Op<Duration, ZonedLocalizer>;
      auto op = ExecTemplate(options, ZonedLocalizer{tz});
      applicator::ScalarUnaryNotNullStateful<OutType, TimestampType, ExecTemplate> kernel{
          op};
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

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtract
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const FunctionOptions* options = nullptr;
    return Base::ExecWithOptions(ctx, options, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct TemporalComponentExtractDayOfWeek
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const DayOfWeekOptions& options = DayOfWeekState::Get(ctx);
    if (options.week_start < 1 || 7 < options.week_start) {
      return Status::Invalid(
          "week_start must follow ISO convention (Monday=1, Sunday=7). Got week_start=",
          options.week_start);
    }
    return Base::ExecWithOptions(ctx, &options, batch, out);
  }
};

template <template <typename...> class Op, typename Duration, typename InType,
          typename OutType>
struct AssumeTimezoneExtractor
    : public TemporalComponentExtractBase<Op, Duration, InType, OutType> {
  using Base = TemporalComponentExtractBase<Op, Duration, InType, OutType>;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const AssumeTimezoneOptions& options = AssumeTimezoneState::Get(ctx);
    const auto& timezone = GetInputTimezone(batch.values[0]);
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
      lookup_table[i] += options->one_based_numbering;
    }
  }

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto wd = arrow_vendored::date::year_month_weekday(
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
// Extract ISO week from temporal types
//
// First week of an ISO year has the majority (4 or more) of it's days in January.
// Last week of an ISO year has the year's last Thursday in it.
// Based on
// https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503

template <typename Duration, typename Localizer>
struct ISOWeek {
  explicit ISOWeek(const FunctionOptions* options, Localizer&& localizer)
      : localizer_(std::move(localizer)) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = floor<days>(localizer_.template ConvertTimePoint<Duration>(arg));
    auto y = year_month_day{t + days{3}}.year();
    auto start = localizer_.ConvertDays((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (t < start) {
      --y;
      start = localizer_.ConvertDays((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return static_cast<T>(trunc<weeks>(t - start).count() + 1);
  }

  Localizer localizer_;
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
    return static_cast<T>((static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1);
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
    return static_cast<T>((t - floor<days>(t)) / std::chrono::hours(1));
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
    return static_cast<T>((t - floor<std::chrono::hours>(t)) / std::chrono::minutes(1));
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
    return static_cast<T>((t - floor<std::chrono::minutes>(t)) / std::chrono::seconds(1));
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
// Convert timestamps to a string representation with an arbitrary format

#ifndef _WIN32
template <typename Duration, typename InType>
struct Strftime {
  const StrftimeOptions& options;
  const time_zone* tz;
  const std::locale locale;

  static Result<Strftime> Make(KernelContext* ctx, const DataType& type) {
    const StrftimeOptions& options = StrftimeState::Get(ctx);

    auto timezone = GetInputTimezone(type);
    // This check is due to surprising %c behavior.
    // See https://github.com/HowardHinnant/date/issues/704
    if ((options.format.find("%c") != std::string::npos) && (options.locale != "C")) {
      return Status::Invalid("%c flag is not supported in non-C locales.");
    }
    if (timezone.empty()) {
      if ((options.format.find("%z") != std::string::npos) ||
          (options.format.find("%Z") != std::string::npos)) {
        return Status::Invalid(
            "Timezone not present, cannot convert to string with timezone: ",
            options.format);
      }
      timezone = "UTC";
    }

    ARROW_ASSIGN_OR_RAISE(const time_zone* tz, LocateZone(timezone));

    ARROW_ASSIGN_OR_RAISE(std::locale locale, GetLocale(options.locale));

    return Strftime{options, tz, std::move(locale)};
  }

  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    ARROW_ASSIGN_OR_RAISE(auto self, Make(ctx, *in.type));
    TimestampFormatter formatter{self.options.format, self.tz, self.locale};

    if (in.is_valid) {
      const int64_t in_val = internal::UnboxScalar<const TimestampType>::Unbox(in);
      ARROW_ASSIGN_OR_RAISE(auto formatted, formatter(in_val));
      checked_cast<StringScalar*>(out)->value = Buffer::FromString(std::move(formatted));
    } else {
      out->is_valid = false;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    ARROW_ASSIGN_OR_RAISE(auto self, Make(ctx, *in.type));
    TimestampFormatter formatter{self.options.format, self.tz, self.locale};

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
    RETURN_NOT_OK(VisitArrayDataInline<Int64Type>(in, visit_value, visit_null));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(string_builder.Finish(&out_array));
    *out = *std::move(out_array->data());

    return Status::OK();
  }

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
};
#else
template <typename Duration, typename InType>
struct Strftime {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    return Status::NotImplemented("Strftime not yet implemented on windows.");
  }
  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    return Status::NotImplemented("Strftime not yet implemented on windows.");
  }
};
#endif

// ----------------------------------------------------------------------
// Convert timestamps from local timestamp without a timezone to timestamps with a
// timezone, interpreting the local timestamp as being in the specified timezone

Result<ValueDescr> ResolveAssumeTimezoneOutput(KernelContext* ctx,
                                               const std::vector<ValueDescr>& args) {
  auto in_type = checked_cast<const TimestampType*>(args[0].type.get());
  auto type = timestamp(in_type->unit(), AssumeTimezoneState::Get(ctx).timezone);
  return ValueDescr(std::move(type));
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
    const auto& timezone = GetInputTimezone(in);
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
      const std::vector<BuilderType*>& field_builders, const ArrayData&,
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
      const std::vector<BuilderType*>& field_builders, const ArrayData& in,
      StructBuilder* struct_builder) {
    const auto& timezone = GetInputTimezone(in);
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
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    if (in.is_valid) {
      ARROW_ASSIGN_OR_RAISE(auto iso_calendar,
                            (ISOCalendarWrapper<Duration, InType>::Get(in)));
      ScalarVector values = {std::make_shared<Int64Scalar>(iso_calendar[0]),
                             std::make_shared<Int64Scalar>(iso_calendar[1]),
                             std::make_shared<Int64Scalar>(iso_calendar[2])};
      *checked_cast<StructScalar*>(out) =
          StructScalar(std::move(values), IsoCalendarType());
    } else {
      out->is_valid = false;
    }
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
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
        VisitArrayDataInline<typename InType::PhysicalType>(in, visit_value, visit_null));
    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(struct_builder->Finish(&out_array));
    *out = *std::move(out_array->data());
    return Status::OK();
  }
};

// Which types to generate a kernel for
enum EnabledTypes : uint8_t { WithDates, WithTimestamps };

template <template <typename...> class Op,
          template <template <typename...> class OpExec, typename Duration,
                    typename InType, typename OutType>
          class ExecTemplate,
          typename OutType>
std::shared_ptr<ScalarFunction> MakeTemporal(
    std::string name, std::initializer_list<EnabledTypes> in_types, OutputType out_type,
    const FunctionDoc* doc, const FunctionOptions* default_options = NULLPTR,
    KernelInit init = NULLPTR) {
  DCHECK_NE(in_types.size(), 0);
  auto func =
      std::make_shared<ScalarFunction>(name, Arity::Unary(), doc, default_options);

  for (const auto in_type : in_types) {
    switch (in_type) {
      case WithDates: {
        auto exec32 = ExecTemplate<Op, days, Date32Type, OutType>::Exec;
        DCHECK_OK(func->AddKernel({date32()}, out_type, std::move(exec32), init));
        auto exec64 =
            ExecTemplate<Op, std::chrono::milliseconds, Date64Type, OutType>::Exec;
        DCHECK_OK(func->AddKernel({date64()}, out_type, std::move(exec64), init));
        break;
      }

      case WithTimestamps: {
        for (auto unit : TimeUnit::values()) {
          InputType in_type{match::TimestampTypeUnit(unit)};
          switch (unit) {
            case TimeUnit::SECOND: {
              auto exec =
                  ExecTemplate<Op, std::chrono::seconds, TimestampType, OutType>::Exec;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::MILLI: {
              auto exec = ExecTemplate<Op, std::chrono::milliseconds, TimestampType,
                                       OutType>::Exec;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::MICRO: {
              auto exec = ExecTemplate<Op, std::chrono::microseconds, TimestampType,
                                       OutType>::Exec;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::NANO: {
              auto exec = ExecTemplate<Op, std::chrono::nanoseconds, TimestampType,
                                       OutType>::Exec;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
          }
        }
        break;
      }
    }
  }

  return func;
}

template <template <typename...> class Op>
std::shared_ptr<ScalarFunction> MakeSimpleUnaryTemporal(
    std::string name, std::initializer_list<EnabledTypes> in_types,
    const std::shared_ptr<arrow::DataType> out_type, const FunctionDoc* doc,
    const FunctionOptions* default_options = NULLPTR, KernelInit init = NULLPTR) {
  DCHECK_NE(in_types.size(), 0);
  auto func =
      std::make_shared<ScalarFunction>(name, Arity::Unary(), doc, default_options);

  for (const auto in_type : in_types) {
    switch (in_type) {
      case WithDates: {
        auto exec32 = SimpleUnary<Op<days, Date32Type>>;
        DCHECK_OK(func->AddKernel({date32()}, out_type, std::move(exec32), init));
        auto exec64 = SimpleUnary<Op<std::chrono::milliseconds, Date64Type>>;
        DCHECK_OK(func->AddKernel({date64()}, out_type, std::move(exec64), init));
        break;
      }
      case WithTimestamps: {
        for (auto unit : TimeUnit::values()) {
          InputType in_type{match::TimestampTypeUnit(unit)};
          switch (unit) {
            case TimeUnit::SECOND: {
              auto exec = SimpleUnary<Op<std::chrono::seconds, TimestampType>>;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::MILLI: {
              auto exec = SimpleUnary<Op<std::chrono::milliseconds, TimestampType>>;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::MICRO: {
              auto exec = SimpleUnary<Op<std::chrono::microseconds, TimestampType>>;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
            case TimeUnit::NANO: {
              auto exec = SimpleUnary<Op<std::chrono::nanoseconds, TimestampType>>;
              DCHECK_OK(func->AddKernel({in_type}, out_type, std::move(exec), init));
              break;
            }
          }
        }
        break;
      }
    }
  }

  return func;
}

const FunctionDoc year_doc{
    "Extract year from temporal types",
    ("Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc month_doc{
    "Extract month number",
    ("Month is encoded as January=1, December=12.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc day_doc{
    "Extract day number",
    ("Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc day_of_week_doc{
    "Extract day of the week number",
    ("By default, the week starts on Monday represented by 0 and ends on Sunday\n"
     "represented by 6.\n"
     "`DayOfWeekOptions.week_start` can be used to set another starting day using\n"
     "the ISO numbering convention (1=start week on Monday, 7=start week on Sunday).\n"
     "Day numbers can start at 0 or 1 based on `DayOfWeekOptions.one_based_numbering`.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"},
    "DayOfWeekOptions"};

const FunctionDoc day_of_year_doc{
    "Extract number of day of year",
    ("January 1st maps to day number 1, February 1st to 32, etc.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc iso_year_doc{
    "Extract ISO year number",
    ("First week of an ISO year has the majority (4 or more) of its days in January."
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc iso_week_doc{
    "Extract ISO week of year number",
    ("First ISO week has the majority (4 or more) of its days in January.\n"
     "Week of the year starts with 1 and can run up to 53.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc iso_calendar_doc{
    "Extract (ISO year, ISO week, ISO day of week) struct",
    ("ISO week starts on Monday denoted by 1 and ends on Sunday denoted by 7.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc quarter_doc{
    "Extract quarter of year number",
    ("First quarter maps to 1 and forth quarter maps to 4.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc hour_doc{
    "Extract hour value",
    ("Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc minute_doc{
    "Extract minute values",
    ("Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc second_doc{
    "Extract second values",
    ("Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc millisecond_doc{
    "Extract millisecond values",
    ("Millisecond returns number of milliseconds since the last full second.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc microsecond_doc{
    "Extract microsecond values",
    ("Millisecond returns number of microseconds since the last full millisecond.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc nanosecond_doc{
    "Extract nanosecond values",
    ("Nanosecond returns number of nanoseconds since the last full microsecond.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc subsecond_doc{
    "Extract subsecond values",
    ("Subsecond returns the fraction of a second since the last full second.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database."),
    {"values"}};

const FunctionDoc strftime_doc{
    "Format timestamps according to a format string",
    ("For each input timestamp, emit a formatted string.\n"
     "The time format string and locale can be set using StrftimeOptions.\n"
     "The output precision of the \"%S\" (seconds) format code depends on\n"
     "the input timestamp precision: it is an integer for timestamps with\n"
     "second precision, a real number with the required number of fractional\n"
     "digits for higher precisions.\n"
     "Null values emit null.\n"
     "An error is returned if the timestamps have a defined timezone but it\n"
     "cannot be found in the timezone database, or if the specified locale\n"
     "does not exist on this system."),
    {"timestamps"},
    "StrftimeOptions"};

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
    "AssumeTimezoneOptions"};

}  // namespace

void RegisterScalarTemporal(FunctionRegistry* registry) {
  // Date extractors

  auto year = MakeTemporal<Year, TemporalComponentExtract, Int64Type>(
      "year", {WithDates, WithTimestamps}, int64(), &year_doc);
  DCHECK_OK(registry->AddFunction(std::move(year)));

  auto month = MakeTemporal<Month, TemporalComponentExtract, Int64Type>(
      "month", {WithDates, WithTimestamps}, int64(), &month_doc);
  DCHECK_OK(registry->AddFunction(std::move(month)));

  auto day = MakeTemporal<Day, TemporalComponentExtract, Int64Type>(
      "day", {WithDates, WithTimestamps}, int64(), &day_doc);
  DCHECK_OK(registry->AddFunction(std::move(day)));

  static const auto default_day_of_week_options = DayOfWeekOptions::Defaults();
  auto day_of_week =
      MakeTemporal<DayOfWeek, TemporalComponentExtractDayOfWeek, Int64Type>(
          "day_of_week", {WithDates, WithTimestamps}, int64(), &day_of_week_doc,
          &default_day_of_week_options, DayOfWeekState::Init);
  DCHECK_OK(registry->AddFunction(std::move(day_of_week)));

  auto day_of_year = MakeTemporal<DayOfYear, TemporalComponentExtract, Int64Type>(
      "day_of_year", {WithDates, WithTimestamps}, int64(), &day_of_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(day_of_year)));

  auto iso_year = MakeTemporal<ISOYear, TemporalComponentExtract, Int64Type>(
      "iso_year", {WithDates, WithTimestamps}, int64(), &iso_year_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_year)));

  auto iso_week = MakeTemporal<ISOWeek, TemporalComponentExtract, Int64Type>(
      "iso_week", {WithDates, WithTimestamps}, int64(), &iso_week_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_week)));

  auto iso_calendar = MakeSimpleUnaryTemporal<ISOCalendar>(
      "iso_calendar", {WithDates, WithTimestamps}, IsoCalendarType(), &iso_calendar_doc);
  DCHECK_OK(registry->AddFunction(std::move(iso_calendar)));

  auto quarter = MakeTemporal<Quarter, TemporalComponentExtract, Int64Type>(
      "quarter", {WithDates, WithTimestamps}, int64(), &quarter_doc);
  DCHECK_OK(registry->AddFunction(std::move(quarter)));

  // Date / time extractors

  auto hour = MakeTemporal<Hour, TemporalComponentExtract, Int64Type>(
      "hour", {WithTimestamps}, int64(), &hour_doc);
  DCHECK_OK(registry->AddFunction(std::move(hour)));

  auto minute = MakeTemporal<Minute, TemporalComponentExtract, Int64Type>(
      "minute", {WithTimestamps}, int64(), &minute_doc);
  DCHECK_OK(registry->AddFunction(std::move(minute)));

  auto second = MakeTemporal<Second, TemporalComponentExtract, Int64Type>(
      "second", {WithTimestamps}, int64(), &second_doc);
  DCHECK_OK(registry->AddFunction(std::move(second)));

  auto millisecond = MakeTemporal<Millisecond, TemporalComponentExtract, Int64Type>(
      "millisecond", {WithTimestamps}, int64(), &millisecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(millisecond)));

  auto microsecond = MakeTemporal<Microsecond, TemporalComponentExtract, Int64Type>(
      "microsecond", {WithTimestamps}, int64(), &microsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(microsecond)));

  auto nanosecond = MakeTemporal<Nanosecond, TemporalComponentExtract, Int64Type>(
      "nanosecond", {WithTimestamps}, int64(), &nanosecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(nanosecond)));

  auto subsecond = MakeTemporal<Subsecond, TemporalComponentExtract, DoubleType>(
      "subsecond", {WithTimestamps}, float64(), &subsecond_doc);
  DCHECK_OK(registry->AddFunction(std::move(subsecond)));

  // Timezone-related functions

  static const auto default_strftime_options = StrftimeOptions();
  auto strftime = MakeSimpleUnaryTemporal<Strftime>(
      "strftime", {WithTimestamps}, utf8(), &strftime_doc, &default_strftime_options,
      StrftimeState::Init);
  DCHECK_OK(registry->AddFunction(std::move(strftime)));

  auto assume_timezone =
      MakeTemporal<AssumeTimezone, AssumeTimezoneExtractor, TimestampType>(
          "assume_timezone", {WithTimestamps},
          OutputType::Resolver(ResolveAssumeTimezoneOutput), &assume_timezone_doc,
          nullptr, AssumeTimezoneState::Init);
  DCHECK_OK(registry->AddFunction(std::move(assume_timezone)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
