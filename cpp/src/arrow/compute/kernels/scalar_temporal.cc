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
namespace {

using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

inline arrow_vendored::date::year_month_day get_year_month_day(const int64_t in_data) {
  std::chrono::seconds since_epoch{in_data};
  return arrow_vendored::date::sys_days{
      arrow_vendored::date::floor<arrow_vendored::date::days>(since_epoch)};
}

template <typename Duration>
inline arrow_vendored::date::hh_mm_ss<Duration> get_time_of_day(const int64_t in_data) {
  std::chrono::seconds since_epoch{in_data};
  arrow_vendored::date::sys_days timepoint_days{
      arrow_vendored::date::floor<arrow_vendored::date::days>(since_epoch)};
  std::chrono::seconds since_midnight = since_epoch - timepoint_days.time_since_epoch();
  return arrow_vendored::date::make_time(since_midnight);
}

inline unsigned day_of_year(const int64_t in_data) {
  // Based on
  // https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1021
  const auto t2 = arrow_vendored::date::sys_days{
      arrow_vendored::date::floor<arrow_vendored::date::days>(
          std::chrono::seconds{in_data})};
  const auto t1 = arrow_vendored::date::year_month_day(t2).year() /
                  arrow_vendored::date::month(1) / arrow_vendored::date::day(1);
  const auto since_new_year = t2 - arrow_vendored::date::sys_days(t1);
  return static_cast<unsigned>(since_new_year.count());
}

inline unsigned week(const int64_t in_data) {
  // Based on
  // https://github.com/HowardHinnant/date/blob/6e921e1b1d21e84a5c82416ba7ecd98e33a436d0/include/date/iso_week.h#L1503
  using namespace arrow_vendored::date;
  const auto dp = sys_days{floor<days>(std::chrono::seconds{in_data})};
  auto y = year_month_day{dp + days{3}}.year();
  auto start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  if (dp < start) {
    --y;
    start = sys_days((y - years{1}) / dec / thu[last]) + (mon - thu);
  }
  return static_cast<unsigned>(trunc<weeks>(dp - start).count() + 1);
}

// ----------------------------------------------------------------------
// Extract year from timestamp

template <typename out_type>
struct Year {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<int>(get_year_month_day(in_data).year());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<int>(get_year_month_day(in_data[i]).year());
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
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(get_year_month_day(in_data).month());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(get_year_month_day(in_data[i]).month());
    }
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Extract day from timestamp

template <typename out_type>
struct Day {
  static Status Call(KernelContext* ctx, const Scalar& in, Scalar* out) {
    const auto& in_data = internal::UnboxScalar<const TimestampType>::Unbox(in);
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(get_year_month_day(in_data).day());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(get_year_month_day(in_data[i]).day());
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
    checked_cast<Int64Scalar*>(out)->value = week(in_data);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = week(in_data[i]);
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
    checked_cast<Int64Scalar*>(out)->value =
        static_cast<unsigned>(get_year_month_day(in_data).month()) / 3 + 1;
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(get_year_month_day(in_data[i]).month()) / 3 + 1;
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
    checked_cast<Int64Scalar*>(out)->value = day_of_year(in_data);
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = day_of_year(in_data[i]);
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
    checked_cast<Int64Scalar*>(out)->value = static_cast<int>(
        arrow_vendored::date::weekday(get_year_month_day(in_data)).iso_encoding());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<int>(
          arrow_vendored::date::weekday(get_year_month_day(in_data[i])).iso_encoding());
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
    checked_cast<Int64Scalar*>(out)->value = static_cast<unsigned>(
        get_time_of_day<std::chrono::seconds>(in_data).hours().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(
          get_time_of_day<std::chrono::seconds>(in_data[i]).hours().count());
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
    checked_cast<Int64Scalar*>(out)->value = static_cast<unsigned>(
        get_time_of_day<std::chrono::seconds>(in_data).minutes().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(
          get_time_of_day<std::chrono::seconds>(in_data[i]).minutes().count());
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
    checked_cast<Int64Scalar*>(out)->value = static_cast<unsigned>(
        get_time_of_day<std::chrono::seconds>(in_data).seconds().count());
    return Status::OK();
  }

  static Status Call(KernelContext* ctx, const ArrayData& in, ArrayData* out) {
    auto in_data = in.GetValues<uint64_t>(1);
    auto out_data = out->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < in.length; i++) {
      out_data[i] = static_cast<unsigned>(
          get_time_of_day<std::chrono::seconds>(in_data[i]).seconds().count());
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

}  // namespace
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

  // TODO
  //  millisecond
  //  microsecond
  //  nanosecond
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
