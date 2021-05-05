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

#include "arrow/compute/kernels/common.h"
#include "arrow/util/time.h"
#include "arrow/vendored/datetime.h"

namespace arrow {

namespace compute {
namespace internal {

using arrow_vendored::date::days;
using arrow_vendored::date::floor;
using arrow_vendored::date::hh_mm_ss;
using arrow_vendored::date::local_days;
using arrow_vendored::date::locate_zone;
using arrow_vendored::date::sys_days;
using arrow_vendored::date::sys_time;
using arrow_vendored::date::trunc;
using arrow_vendored::date::weeks;
using arrow_vendored::date::year_month_day;
using arrow_vendored::date::years;
using arrow_vendored::date::literals::dec;
using arrow_vendored::date::literals::jan;
using arrow_vendored::date::literals::last;
using arrow_vendored::date::literals::mon;
using arrow_vendored::date::literals::thu;

template <typename Duration>
inline year_month_day ymd_caster_template(const int64_t data) {
  return year_month_day(floor<days>(sys_time<Duration>(Duration{data})));
}

template <typename Duration>
inline std::function<year_month_day(const int64_t)> ymd_caster_zoned_template(
    const std::string timezone) {
  static const arrow_vendored::date::time_zone* tz = locate_zone(timezone);
  return [](const int64_t data) {
    return year_month_day(floor<days>(tz->to_local(sys_time<Duration>(Duration{data}))));
  };
}

inline std::function<year_month_day(const int64_t)> make_ymd_caster(
    const std::shared_ptr<DataType> type) {
  const auto ts_type = std::static_pointer_cast<const TimestampType>(type);
  const TimeUnit::type unit = ts_type->unit();
  const std::string timezone = ts_type->timezone();

  if (timezone.empty()) {
    switch (unit) {
      case TimeUnit::SECOND:
        return ymd_caster_template<std::chrono::seconds>;
      case TimeUnit::MILLI:
        return ymd_caster_template<std::chrono::milliseconds>;
      case TimeUnit::MICRO:
        return ymd_caster_template<std::chrono::microseconds>;
      case TimeUnit::NANO:
        return ymd_caster_template<std::chrono::nanoseconds>;
    }
  } else {
    switch (unit) {
      case TimeUnit::SECOND:
        return ymd_caster_zoned_template<std::chrono::seconds>(timezone);
      case TimeUnit::MILLI:
        return ymd_caster_zoned_template<std::chrono::milliseconds>(timezone);
      case TimeUnit::MICRO:
        return ymd_caster_zoned_template<std::chrono::microseconds>(timezone);
      case TimeUnit::NANO:
        return ymd_caster_zoned_template<std::chrono::nanoseconds>(timezone);
    }
  }
  return ymd_caster_template<std::chrono::seconds>;
}

template <typename DurationIn, typename DurationOut>
inline hh_mm_ss<DurationOut> hhmmss_caster_template(const int64_t data) {
  DurationIn t = DurationIn{data};
  return hh_mm_ss<DurationOut>(
      std::chrono::duration_cast<DurationOut>(t - floor<days>(t)));
}

template <typename DurationIn, typename DurationOut>
inline std::function<hh_mm_ss<DurationOut>(const int64_t)> hhmmss_caster_zoned_template(
    const std::string timezone) {
  static const arrow_vendored::date::time_zone* tz = locate_zone(timezone);
  return [](const int64_t data) {
    const auto z = sys_time<DurationIn>(DurationIn{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return hh_mm_ss<DurationOut>(
        std::chrono::duration_cast<DurationOut>(l - floor<days>(l)));
  };
}

template <typename Duration>
inline std::function<hh_mm_ss<Duration>(const int64_t)> make_hhmmss_caster(
    const std::shared_ptr<DataType> type) {
  const auto ts_type = std::static_pointer_cast<const TimestampType>(type);
  const TimeUnit::type unit = ts_type->unit();
  const std::string timezone = ts_type->timezone();

  if (timezone.empty()) {
    switch (unit) {
      case TimeUnit::SECOND:
        return hhmmss_caster_template<std::chrono::seconds, Duration>;
      case TimeUnit::MILLI:
        return hhmmss_caster_template<std::chrono::milliseconds, Duration>;
      case TimeUnit::MICRO:
        return hhmmss_caster_template<std::chrono::microseconds, Duration>;
      case TimeUnit::NANO:
        return hhmmss_caster_template<std::chrono::nanoseconds, Duration>;
    }
  } else {
    switch (unit) {
      case TimeUnit::SECOND:
        return hhmmss_caster_zoned_template<std::chrono::seconds, Duration>(timezone);
      case TimeUnit::MILLI:
        return hhmmss_caster_zoned_template<std::chrono::milliseconds, Duration>(
            timezone);
      case TimeUnit::MICRO:
        return hhmmss_caster_zoned_template<std::chrono::microseconds, Duration>(
            timezone);
      case TimeUnit::NANO:
        return hhmmss_caster_zoned_template<std::chrono::nanoseconds, Duration>(timezone);
    }
  }
  return hhmmss_caster_template<std::chrono::seconds, Duration>;
}

template <typename Duration>
inline unsigned day_of_year_caster_template(const int64_t data) {
  const auto sd = sys_days{floor<days>(Duration{data})};
  const auto y = year_month_day(sd).year();
  return static_cast<unsigned>((sd - sys_days(y / jan / 0)).count());
}

template <typename Duration>
inline std::function<unsigned(const int64_t)> day_of_year_zoned_caster_template(
    const std::string timezone) {
  static const arrow_vendored::date::time_zone* tz = locate_zone(timezone);
  return [](const int64_t data) {
    auto ld =
        year_month_day(floor<days>(tz->to_local(sys_time<Duration>(Duration{data}))));
    return static_cast<unsigned>(
        (local_days(ld) - local_days(ld.year() / jan / 1) + days{1}).count());
  };
}

inline std::function<unsigned(const int64_t)> get_day_of_year_caster(
    const std::shared_ptr<DataType> type) {
  const auto ts_type = std::static_pointer_cast<const TimestampType>(type);
  const TimeUnit::type unit = ts_type->unit();
  const std::string timezone = ts_type->timezone();

  if (timezone.empty()) {
    switch (unit) {
      case TimeUnit::SECOND:
        return day_of_year_caster_template<std::chrono::seconds>;
      case TimeUnit::MILLI:
        return day_of_year_caster_template<std::chrono::milliseconds>;
      case TimeUnit::MICRO:
        return day_of_year_caster_template<std::chrono::microseconds>;
      case TimeUnit::NANO:
        return day_of_year_caster_template<std::chrono::nanoseconds>;
    }
  } else {
    switch (unit) {
      case TimeUnit::SECOND:
        return day_of_year_zoned_caster_template<std::chrono::seconds>(timezone);
      case TimeUnit::MILLI:
        return day_of_year_zoned_caster_template<std::chrono::milliseconds>(timezone);
      case TimeUnit::MICRO:
        return day_of_year_zoned_caster_template<std::chrono::microseconds>(timezone);
      case TimeUnit::NANO:
        return day_of_year_zoned_caster_template<std::chrono::nanoseconds>(timezone);
    }
  }
  return day_of_year_caster_template<std::chrono::seconds>;
}

template <typename Duration>
inline unsigned week_caster_template(const int64_t data) {
  // Based on
  // https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503
  const auto dp = sys_days{floor<days>(Duration{data})};
  auto y = year_month_day{dp + days{3}}.year();
  auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  if (dp < start) {
    --y;
    start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  }
  return static_cast<unsigned>(trunc<weeks>(dp - start).count() + 1);
}

template <typename Duration>
inline std::function<unsigned(const int64_t)> week_zoned_caster_template(
    const std::string timezone) {
  static const arrow_vendored::date::time_zone* tz = locate_zone(timezone);
  return [](const int64_t data) {
    const auto ld = floor<days>(tz->to_local(sys_time<Duration>(Duration{data})));
    auto y = year_month_day{ld + days{3}}.year();
    auto start = local_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (ld < start) {
      --y;
      start = local_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return static_cast<unsigned>(trunc<weeks>(local_days(ld) - start).count() + 1);
  };
}

inline std::function<unsigned(const int64_t)> make_week_caster(
    const std::shared_ptr<DataType> type) {
  const auto ts_type = std::static_pointer_cast<const TimestampType>(type);
  const TimeUnit::type unit = ts_type->unit();
  const std::string timezone = ts_type->timezone();

  if (timezone.empty()) {
    switch (unit) {
      case TimeUnit::SECOND:
        return week_caster_template<std::chrono::seconds>;
      case TimeUnit::MILLI:
        return week_caster_template<std::chrono::milliseconds>;
      case TimeUnit::MICRO:
        return week_caster_template<std::chrono::microseconds>;
      case TimeUnit::NANO:
        return week_caster_template<std::chrono::nanoseconds>;
    }
  } else {
    switch (unit) {
      case TimeUnit::SECOND:
        return week_zoned_caster_template<std::chrono::seconds>(timezone);
      case TimeUnit::MILLI:
        return week_zoned_caster_template<std::chrono::milliseconds>(timezone);
      case TimeUnit::MICRO:
        return week_zoned_caster_template<std::chrono::microseconds>(timezone);
      case TimeUnit::NANO:
        return week_zoned_caster_template<std::chrono::nanoseconds>(timezone);
    }
  }
  return day_of_year_caster_template<std::chrono::seconds>;
}

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename out_type>
struct Year {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto ymd_caster = make_ymd_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value = static_cast<int>(ymd_caster(in_data).year());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto ymd_caster = make_ymd_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<int>(ymd_caster(in_data[i]).year());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract month from timestamp

template <typename out_type>
struct Month {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto ymd_caster = make_ymd_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(ymd_caster(in_data).month());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto ymd_caster = make_ymd_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(ymd_caster(in_data[i]).month());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename out_type>
struct Day {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const int64_t& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto ymd_caster = make_ymd_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(ymd_caster(in_data).day());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto ymd_caster = make_ymd_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(ymd_caster(in_data[i]).day());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract week from timestamp

template <typename out_type>
struct Week {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto week_caster = make_week_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value = week_caster(in_data);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto week_caster = make_week_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = week_caster(in_data[i]);
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract quarter from timestamp

template <typename out_type>
struct Quarter {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto ymd_caster = make_ymd_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        (static_cast<unsigned>(ymd_caster(in_data).month()) - 1) / 3 + 1;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto ymd_caster = make_ymd_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = (static_cast<unsigned>(ymd_caster(in_data[i]).month()) - 1) / 3 + 1;
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract day of year from timestamp

template <typename out_type>
struct DayOfYear {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto day_of_year_caster = get_day_of_year_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value = day_of_year_caster(in_data);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto day_of_year_caster = get_day_of_year_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = day_of_year_caster(in_data[i]);
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract day of week from timestamp

template <typename out_type>
struct DayOfWeek {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto ymd_caster = make_ymd_caster(in.type);
    checked_cast<Int64Scalar*>(out)->value = static_cast<int>(
        arrow_vendored::date::weekday(ymd_caster(in_data)).iso_encoding());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto ymd_caster = make_ymd_caster(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<int>(
          arrow_vendored::date::weekday(ymd_caster(in_data[i])).iso_encoding());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename out_type>
struct Hour {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::seconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).hours().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::seconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(hhmmss_caster(in_data[i]).hours().count());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename out_type>
struct Minute {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::seconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).minutes().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::seconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(hhmmss_caster(in_data[i]).minutes().count());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename out_type>
struct Second {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::milliseconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).seconds().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::milliseconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(hhmmss_caster(in_data[i]).seconds().count());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename out_type>
struct Millisecond {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::milliseconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).subseconds().count() % 1000);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::milliseconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] =
          static_cast<unsigned>(hhmmss_caster(in_data[i]).subseconds().count() % 1000);
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename out_type>
struct Microsecond {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::microseconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).subseconds().count() % 1000);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::microseconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] =
          static_cast<unsigned>(hhmmss_caster(in_data[i]).subseconds().count() % 1000);
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename out_type>
struct Nanosecond {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::nanoseconds>(in.type);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(hhmmss_caster(in_data).subseconds().count() % 1000);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    auto hhmmss_caster = make_hhmmss_caster<std::chrono::nanoseconds>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] =
          static_cast<unsigned>(hhmmss_caster(in_data[i]).subseconds().count() % 1000);
    }
    return Status::OK();
  }
};

void MakeFunction(std::string name, ArrayKernelExec exec, const FunctionDoc* doc,
                  OutputType out_type, FunctionRegistry* registry,
                  bool can_write_into_slices = true,
                  NullHandling::type null_handling = NullHandling::INTERSECTION) {
  auto func = std::make_shared<ScalarFunction>(name, Arity(1), doc);

  std::vector<InputType> in_types(1, InputType(Type::TIMESTAMP));
  ScalarKernel kernel(std::move(in_types), out_type, exec);
  kernel.null_handling = null_handling;
  kernel.can_write_into_slices = can_write_into_slices;

  DCHECK_OK(func->AddKernel(kernel));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

const FunctionDoc year_doc{"Extract year values", "", {"values"}};
const FunctionDoc month_doc{"Extract month values", "", {"values"}};
const FunctionDoc day_doc{"Extract day values", "", {"values"}};
const FunctionDoc day_of_year_doc{"Extract day of year values", "", {"values"}};
const FunctionDoc week_doc{"Extract week values", "", {"values"}};
const FunctionDoc quarter_doc{"Extract quarter values", "", {"values"}};
const FunctionDoc day_of_week_doc{"Extract day of week values", "", {"values"}};
const FunctionDoc hour_doc{"Extract hour values", "", {"values"}};
const FunctionDoc minute_doc{"Extract minute values", "", {"values"}};
const FunctionDoc second_doc{"Extract second values", "", {"values"}};
const FunctionDoc millisecond_doc{"Extract millisecond values", "", {"values"}};
const FunctionDoc microsecond_doc{"Extract microsecond values", "", {"values"}};
const FunctionDoc nanosecond_doc{"Extract nanosecond values", "", {"values"}};

}  // namespace internal
namespace internal {

void RegisterScalarTemporal(FunctionRegistry* registry) {
  MakeFunction("year", applicator::SimpleUnary<Year<int64_t>>, &year_doc, int64(),
               registry);
  MakeFunction("month", applicator::SimpleUnary<Month<int64_t>>, &month_doc, int64(),
               registry);
  MakeFunction("day", applicator::SimpleUnary<Day<int64_t>>, &day_doc, int64(), registry);
  MakeFunction("week", applicator::SimpleUnary<Week<int64_t>>, &week_doc, int64(),
               registry);
  MakeFunction("quarter", applicator::SimpleUnary<Quarter<int64_t>>, &quarter_doc,
               int64(), registry);
  MakeFunction("day_of_year", applicator::SimpleUnary<DayOfYear<int64_t>>,
               &day_of_year_doc, int64(), registry);
  MakeFunction("day_of_week", applicator::SimpleUnary<DayOfWeek<int64_t>>,
               &day_of_week_doc, int64(), registry);
  MakeFunction("hour", applicator::SimpleUnary<Hour<int64_t>>, &hour_doc, int64(),
               registry);
  MakeFunction("minute", applicator::SimpleUnary<Minute<int64_t>>, &minute_doc, int64(),
               registry);
  MakeFunction("second", applicator::SimpleUnary<Second<int64_t>>, &second_doc, int64(),
               registry);
  MakeFunction("millisecond", applicator::SimpleUnary<Millisecond<int64_t>>,
               &millisecond_doc, int64(), registry);
  MakeFunction("microsecond", applicator::SimpleUnary<Microsecond<int64_t>>,
               &microsecond_doc, int64(), registry);
  MakeFunction("nanosecond", applicator::SimpleUnary<Nanosecond<int64_t>>,
               &nanosecond_doc, int64(), registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
