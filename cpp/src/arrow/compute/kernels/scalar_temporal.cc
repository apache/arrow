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
using arrow_vendored::date::make_zoned;
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

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename Duration>
struct year {
  inline const int32_t operator()(const int64_t data) const {
    return static_cast<const int32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{data}))).year());
  }
  inline const int32_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    return static_cast<const int32_t>(year_month_day(floor<days>(zt)).year());
  }
};

// ----------------------------------------------------------------------
// Extract month from timestamp

template <typename Duration>
struct month {
  inline const uint32_t operator()(const int64_t data) const {
    return static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{data}))).month());
  }
  inline const uint32_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    return static_cast<const uint32_t>(year_month_day(floor<days>(zt)).month());
  }
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename Duration>
struct day {
  inline const uint32_t operator()(const int64_t data) const {
    return static_cast<const uint32_t>(
        year_month_day(floor<days>(sys_time<Duration>(Duration{data}))).day());
  }
  inline const uint32_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    return static_cast<const uint32_t>(year_month_day(floor<days>(zt)).day());
  }
};

// ----------------------------------------------------------------------
// Extract day of week from timestamp

template <typename Duration>
struct day_of_week {
  inline const uint8_t operator()(const int64_t data) const {
    return weekday(year_month_day(floor<days>(sys_time<Duration>(Duration{data}))))
        .iso_encoding();
  }
  inline const uint8_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    return weekday(year_month_day(floor<days>(zt))).iso_encoding();
  }
};

// ----------------------------------------------------------------------
// Extract day of year from timestamp

template <typename Duration>
struct day_of_year {
  inline const uint16_t operator()(const int64_t data) const {
    const auto sd = sys_days{floor<days>(Duration{data})};
    const auto y = year_month_day(sd).year();
    return (sd - sys_days(y / jan / 0)).count();
  }
  inline const uint16_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    auto ld = year_month_day(floor<days>(zt));
    return (local_days(ld) - local_days(ld.year() / jan / 1) + days{1}).count();
  }
};

// ----------------------------------------------------------------------
// Extract week from timestamp

template <typename Duration>
struct week {
  // Based on
  // https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503
  inline const uint8_t operator()(const int64_t data) const {
    const auto dp = sys_days{floor<days>(Duration{data})};
    auto y = year_month_day{dp + days{3}}.year();
    auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (dp < start) {
      --y;
      start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return trunc<weeks>(dp - start).count() + 1;
  }
  inline const uint8_t operator()(const int64_t data, const time_zone* tz) const {
    const auto dp = sys_days{
        floor<days>(make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time())};
    auto y = year_month_day{dp + days{3}}.year();
    auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    if (dp < start) {
      --y;
      start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
    }
    return trunc<weeks>(dp - start).count() + 1;
  }
};

// ----------------------------------------------------------------------
// Extract day of quarter from timestamp

template <typename Duration>
struct quarter {
  inline const uint32_t operator()(const int64_t data) const {
    const auto ymd = year_month_day(floor<days>(sys_time<Duration>(Duration{data})));
    return (static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1;
  }
  inline const uint32_t operator()(const int64_t data, const time_zone* tz) const {
    auto zt = make_zoned(tz, sys_time<Duration>(Duration{data})).get_sys_time();
    const auto ymd = year_month_day(floor<days>(zt));
    return (static_cast<const uint32_t>(ymd.month()) - 1) / 3 + 1;
  }
};

// ----------------------------------------------------------------------
// Extract hour from timestamp

template <typename Duration>
struct hour {
  inline uint8_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return hh_mm_ss<Duration>(t - floor<days>(t)).hours().count();
  }
  inline uint8_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return hh_mm_ss<Duration>(std::chrono::duration_cast<Duration>(l - floor<days>(l)))
        .hours()
        .count();
  }
};

// ----------------------------------------------------------------------
// Extract minute from timestamp

template <typename Duration>
struct minute {
  inline uint8_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return hh_mm_ss<Duration>(t - floor<days>(t)).minutes().count();
  }
  inline uint8_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return hh_mm_ss<Duration>(std::chrono::duration_cast<Duration>(l - floor<days>(l)))
        .minutes()
        .count();
  }
};

// ----------------------------------------------------------------------
// Extract second from timestamp

template <typename Duration>
struct second {
  inline uint8_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return hh_mm_ss<Duration>(t - floor<days>(t)).seconds().count();
  }
  inline uint8_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return hh_mm_ss<Duration>(std::chrono::duration_cast<Duration>(l - floor<days>(l)))
        .seconds()
        .count();
  }
};

// ----------------------------------------------------------------------
// Extract milliseconds from timestamp

template <typename Duration>
struct millisecond {
  inline uint16_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return std::chrono::duration_cast<std::chrono::milliseconds>(t - floor<days>(t))
               .count() %
           1000;
  }
  inline uint16_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return std::chrono::duration_cast<std::chrono::milliseconds>(l - floor<days>(l))
               .count() %
           1000;
  }
};

// ----------------------------------------------------------------------
// Extract microseconds from timestamp

template <typename Duration>
struct microsecond {
  inline uint16_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return std::chrono::duration_cast<std::chrono::microseconds>(t - floor<days>(t))
               .count() %
           1000;
  }
  inline uint16_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return std::chrono::duration_cast<std::chrono::microseconds>(l - floor<days>(l))
               .count() %
           1000;
  }
};

// ----------------------------------------------------------------------
// Extract nanoseconds from timestamp

template <typename Duration>
struct nanosecond {
  inline uint16_t operator()(const int64_t data) const {
    Duration t = Duration{data};
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t - floor<days>(t))
               .count() %
           1000;
  }
  inline uint16_t operator()(const int64_t data, const time_zone* tz) const {
    const auto z = sys_time<Duration>(Duration{data});
    const auto l = make_zoned(tz, z).get_local_time();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(l - floor<days>(l))
               .count() %
           1000;
  }
};

template <template <typename...> class Op>
struct TimeUnitCaster {
  TimeUnitCaster(const std::shared_ptr<const arrow::DataType> type) {
    const auto ts_type = std::static_pointer_cast<const TimestampType>(type);

    unit_ = ts_type->unit();
    const auto timezone = ts_type->timezone();

    if (timezone.empty()) {
      switch (unit_) {
        case TimeUnit::SECOND: {
          auto func = Op<std::chrono::seconds>();
          cast_func_ = [&func](const int64_t data) { return func(data); };
          break;
        }
        case TimeUnit::MILLI: {
          auto func = Op<std::chrono::milliseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data); };
          break;
        }
        case TimeUnit::MICRO: {
          auto func = Op<std::chrono::microseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data); };
          break;
        }
        case TimeUnit::NANO: {
          auto func = Op<std::chrono::nanoseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data); };
          break;
        }
      }
    } else {
      static const time_zone* tz = locate_zone(timezone);
      switch (unit_) {
        case TimeUnit::SECOND: {
          auto func = Op<std::chrono::seconds>();
          cast_func_ = [&func](const int64_t data) { return func(data, tz); };
          break;
        }
        case TimeUnit::MILLI: {
          auto func = Op<std::chrono::milliseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data, tz); };
          break;
        }
        case TimeUnit::MICRO: {
          auto func = Op<std::chrono::microseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data, tz); };
          break;
        }
        case TimeUnit::NANO: {
          auto func = Op<std::chrono::nanoseconds>();
          cast_func_ = [&func](const int64_t data) { return func(data, tz); };
          break;
        }
      }
    }
  }

  unsigned operator()(const int64_t data) const { return cast_func_(data); };

  std::function<int(const int64_t)> cast_func_;
  TimeUnit::type unit_;
};

template <typename OutType, template <typename...> class Op>
struct GenericKernel {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    const auto caster = TimeUnitCaster<Op>(in.type);
    *checked_cast<Scalar*>(out) = *MakeScalar<OutType>(caster(in_data));
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<OutType>(1);
    const auto caster = TimeUnitCaster<Op>(in.type);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = caster(in_data[i]);
    }
    return Status::OK();
  }
};

template <template <typename...> class Op, typename OutType>
void MakeFunction(std::string name, const FunctionDoc* doc, OutputType out_type,
                  FunctionRegistry* registry, bool can_write_into_slices = true,
                  NullHandling::type null_handling = NullHandling::INTERSECTION) {
  auto func = std::make_shared<ScalarFunction>(name, Arity(1), doc);

  std::vector<InputType> in_types(1, InputType(Type::TIMESTAMP));
  ArrayKernelExec exec = applicator::SimpleUnary<GenericKernel<OutType, Op>>;
  ScalarKernel kernel(std::move(in_types), out_type, exec);
  kernel.null_handling = null_handling;
  kernel.can_write_into_slices = can_write_into_slices;

  DCHECK_OK(func->AddKernel(kernel));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

const FunctionDoc year_doc{"Extract year values", "", {"values"}};
const FunctionDoc month_doc{"Extract month values", "", {"values"}};
const FunctionDoc day_doc{"Extract day values", "", {"values"}};

const FunctionDoc day_of_week_doc{"Extract day of week values", "", {"values"}};
const FunctionDoc day_of_year_doc{"Extract day of year values", "", {"values"}};
const FunctionDoc week_doc{"Extract week values", "", {"values"}};
const FunctionDoc quarter_doc{"Extract quarter values", "", {"values"}};

const FunctionDoc hour_doc{"Extract hour values", "", {"values"}};
const FunctionDoc minute_doc{"Extract minute values", "", {"values"}};
const FunctionDoc second_doc{"Extract second values", "", {"values"}};
const FunctionDoc millisecond_doc{"Extract millisecond values", "", {"values"}};
const FunctionDoc microsecond_doc{"Extract microsecond values", "", {"values"}};
const FunctionDoc nanosecond_doc{"Extract nanosecond values", "", {"values"}};

void RegisterScalarTemporal(FunctionRegistry* registry) {
  MakeFunction<year, int32_t>("year", &year_doc, int32(), registry);
  MakeFunction<month, uint32_t>("month", &month_doc, uint32(), registry);
  MakeFunction<day, uint32_t>("day", &day_doc, uint32(), registry);

  MakeFunction<day_of_week, uint8_t>("day_of_week", &day_of_week_doc, uint8(), registry);
  MakeFunction<day_of_year, uint16_t>("day_of_year", &day_of_year_doc, uint16(),
                                      registry);
  MakeFunction<week, uint8_t>("week", &week_doc, uint8(), registry);
  MakeFunction<quarter, uint32_t>("quarter", &quarter_doc, uint32(), registry);

  MakeFunction<hour, uint8_t>("hour", &hour_doc, uint8(), registry);
  MakeFunction<minute, uint8_t>("minute", &minute_doc, uint8(), registry);
  MakeFunction<second, uint8_t>("second", &second_doc, uint8(), registry);
  MakeFunction<millisecond, uint16_t>("millisecond", &millisecond_doc, uint16(),
                                      registry);
  MakeFunction<microsecond, uint16_t>("microsecond", &microsecond_doc, uint16(),
                                      registry);
  MakeFunction<nanosecond, uint16_t>("nanosecond", &nanosecond_doc, uint16(), registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
