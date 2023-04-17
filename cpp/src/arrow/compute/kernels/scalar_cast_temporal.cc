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

// Implementation of casting to (or between) temporal types

#include <limits>

#include "arrow/array/builder_time.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/temporal_internal.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/time.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::ParseTimestampISO8601;

namespace compute {
namespace internal {

constexpr int64_t kMillisecondsInDay = 86400000;

// ----------------------------------------------------------------------
// From one timestamp to another

template <typename in_type, typename out_type>
Status ShiftTime(KernelContext* ctx, const util::DivideOrMultiply factor_op,
                 const int64_t factor, const ArraySpan& input, ArraySpan* output) {
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  const in_type* in_data = input.GetValues<in_type>(1);
  out_type* out_data = output->GetValues<out_type>(1);

  if (factor == 1) {
    for (int64_t i = 0; i < input.length; i++) {
      out_data[i] = static_cast<out_type>(in_data[i]);
    }
  } else if (factor_op == util::MULTIPLY) {
    if (options.allow_time_overflow) {
      for (int64_t i = 0; i < input.length; i++) {
        out_data[i] = static_cast<out_type>(in_data[i] * factor);
      }
    } else {
#define RAISE_OVERFLOW_CAST(VAL)                                          \
  return Status::Invalid("Casting from ", input.type->ToString(), " to ", \
                         output->type->ToString(), " would result in ",   \
                         "out of bounds timestamp: ", VAL);

      int64_t max_val = std::numeric_limits<int64_t>::max() / factor;
      int64_t min_val = std::numeric_limits<int64_t>::min() / factor;
      if (input.null_count != 0 && input.buffers[0].data != nullptr) {
        BitmapReader bit_reader(input.buffers[0].data, input.offset, input.length);
        for (int64_t i = 0; i < input.length; i++) {
          if (bit_reader.IsSet() && (in_data[i] < min_val || in_data[i] > max_val)) {
            RAISE_OVERFLOW_CAST(in_data[i]);
          }
          out_data[i] = static_cast<out_type>(in_data[i] * factor);
          bit_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; i++) {
          if (in_data[i] < min_val || in_data[i] > max_val) {
            RAISE_OVERFLOW_CAST(in_data[i]);
          }
          out_data[i] = static_cast<out_type>(in_data[i] * factor);
        }
      }

#undef RAISE_OVERFLOW_CAST
    }
  } else {
    if (options.allow_time_truncate) {
      for (int64_t i = 0; i < input.length; i++) {
        out_data[i] = static_cast<out_type>(in_data[i] / factor);
      }
    } else {
#define RAISE_INVALID_CAST(VAL)                                           \
  return Status::Invalid("Casting from ", input.type->ToString(), " to ", \
                         output->type->ToString(), " would lose data: ", VAL);

      if (input.null_count != 0 && input.buffers[0].data != nullptr) {
        BitmapReader bit_reader(input.buffers[0].data, input.offset, input.length);
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (bit_reader.IsSet() && (out_data[i] * factor != in_data[i])) {
            RAISE_INVALID_CAST(in_data[i]);
          }
          bit_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (out_data[i] * factor != in_data[i]) {
            RAISE_INVALID_CAST(in_data[i]);
          }
        }
      }

#undef RAISE_INVALID_CAST
    }
  }

  return Status::OK();
}

template <template <typename...> class Op, typename OutType, typename... Args>
Status ExtractTemporal(KernelContext* ctx, const ExecSpan& batch, ExecResult* out,
                       Args... args) {
  const auto& ty = checked_cast<const TimestampType&>(*batch[0].type());

  switch (ty.unit()) {
    case TimeUnit::SECOND:
      return TemporalComponentExtract<Op, std::chrono::seconds, TimestampType, OutType,
                                      Args...>::Exec(ctx, batch, out, args...);
    case TimeUnit::MILLI:
      return TemporalComponentExtract<Op, std::chrono::milliseconds, TimestampType,
                                      OutType, Args...>::Exec(ctx, batch, out, args...);
    case TimeUnit::MICRO:
      return TemporalComponentExtract<Op, std::chrono::microseconds, TimestampType,
                                      OutType, Args...>::Exec(ctx, batch, out, args...);
    case TimeUnit::NANO:
      return TemporalComponentExtract<Op, std::chrono::nanoseconds, TimestampType,
                                      OutType, Args...>::Exec(ctx, batch, out, args...);
  }
  return Status::Invalid("Unknown timestamp unit: ", ty);
}

// <TimestampType, TimestampType> and <DurationType, DurationType>
template <typename O, typename I>
struct CastFunctor<
    O, I,
    enable_if_t<(is_timestamp_type<O>::value && is_timestamp_type<I>::value) ||
                (is_duration_type<O>::value && is_duration_type<I>::value)>> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& in_type = checked_cast<const I&>(*batch[0].type());
    const auto& out_type = checked_cast<const O&>(*out->type());

    if (in_type.unit() == out_type.unit()) {
      return ZeroCopyCastExec(ctx, batch, out);
    }

    ArrayData* out_arr = out->array_data().get();
    DCHECK_EQ(0, out_arr->offset);
    int value_size = batch[0].type()->byte_width();
    DCHECK_OK(ctx->Allocate(out_arr->length * value_size).Value(&out_arr->buffers[1]));

    ArraySpan output_span;
    output_span.SetMembers(*out_arr);
    const ArraySpan& input = batch[0].array;
    auto conversion = util::GetTimestampConversion(in_type.unit(), out_type.unit());
    return ShiftTime<int64_t, int64_t>(ctx, conversion.first, conversion.second, input,
                                       &output_span);
  }
};

// ----------------------------------------------------------------------
// From timestamp to date32 or date64

template <>
struct CastFunctor<Date32Type, TimestampType> {
  template <typename Duration, typename Localizer>
  struct Date32 {
    Date32(const FunctionOptions* options, Localizer&& localizer)
        : localizer_(std::move(localizer)) {}

    template <typename T, typename Arg0>
    T Call(KernelContext*, Arg0 arg, Status*) const {
      return static_cast<T>(static_cast<const int32_t>(
          floor<days>(localizer_.template ConvertTimePoint<Duration>(arg))
              .time_since_epoch()
              .count()));
    }

    Localizer localizer_;
  };

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExtractTemporal<Date32, Date32Type>(ctx, batch, out);
  }
};

template <>
struct CastFunctor<Date64Type, TimestampType> {
  template <typename Duration, typename Localizer>
  struct Date64 {
    constexpr static int64_t kMillisPerDay = 86400000;
    Date64(const FunctionOptions* options, Localizer&& localizer)
        : localizer_(std::move(localizer)) {}

    template <typename T, typename Arg0>
    T Call(KernelContext*, Arg0 arg, Status*) const {
      return static_cast<T>(
          kMillisPerDay *
          static_cast<const int32_t>(
              floor<days>(localizer_.template ConvertTimePoint<Duration>(arg))
                  .time_since_epoch()
                  .count()));
    }

    Localizer localizer_;
  };

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ExtractTemporal<Date64, Date64Type>(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// From timestamp to time32 or time64

template <typename Duration, typename Localizer>
struct ExtractTimeDownscaled {
  ExtractTimeDownscaled(const FunctionOptions* options, Localizer&& localizer,
                        const int64_t factor)
      : localizer_(std::move(localizer)), factor_(factor) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status* st) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    const int64_t orig_value = (t - floor<days>(t)).count();
    const T scaled = static_cast<T>(orig_value / factor_);
    const int64_t unscaled = static_cast<int64_t>(scaled) * factor_;
    if (unscaled != orig_value) {
      *st = Status::Invalid("Cast would lose data: ", orig_value);
      return 0;
    }
    return scaled;
  }

  Localizer localizer_;
  const int64_t factor_;
};

template <typename Duration, typename Localizer>
struct ExtractTimeUpscaledUnchecked {
  ExtractTimeUpscaledUnchecked(const FunctionOptions* options, Localizer&& localizer,
                               const int64_t factor)
      : localizer_(std::move(localizer)), factor_(factor) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    const int64_t orig_value = (t - floor<days>(t)).count();
    return static_cast<T>(orig_value * factor_);
  }

  Localizer localizer_;
  const int64_t factor_;
};

template <typename Duration, typename Localizer>
struct ExtractTimeDownscaledUnchecked {
  ExtractTimeDownscaledUnchecked(const FunctionOptions* options, Localizer&& localizer,
                                 const int64_t factor)
      : localizer_(std::move(localizer)), factor_(factor) {}

  template <typename T, typename Arg0>
  T Call(KernelContext*, Arg0 arg, Status*) const {
    const auto t = localizer_.template ConvertTimePoint<Duration>(arg);
    const int64_t orig_value = (t - floor<days>(t)).count();
    return static_cast<T>(orig_value / factor_);
  }

  Localizer localizer_;
  const int64_t factor_;
};

template <>
struct CastFunctor<Time32Type, TimestampType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& in_type = checked_cast<const TimestampType&>(*batch[0].type());
    const auto& out_type = checked_cast<const Time32Type&>(*out->type());
    const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

    // Shifting before extraction won't work since the timestamp may not fit
    // even if the time itself fits
    if (in_type.unit() != out_type.unit()) {
      auto conversion = util::GetTimestampConversion(in_type.unit(), out_type.unit());
      if (conversion.first == util::MULTIPLY) {
        return ExtractTemporal<ExtractTimeUpscaledUnchecked, Time32Type>(
            ctx, batch, out, conversion.second);
      } else {
        if (options.allow_time_truncate) {
          return ExtractTemporal<ExtractTimeDownscaledUnchecked, Time32Type>(
              ctx, batch, out, conversion.second);
        } else {
          return ExtractTemporal<ExtractTimeDownscaled, Time32Type>(ctx, batch, out,
                                                                    conversion.second);
        }
      }
    }
    return ExtractTemporal<ExtractTimeUpscaledUnchecked, Time32Type>(ctx, batch, out, 1);
  }
};

template <>
struct CastFunctor<Time64Type, TimestampType> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& in_type = checked_cast<const TimestampType&>(*batch[0].type());
    const auto& out_type = checked_cast<const Time64Type&>(*out->type());
    const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

    // Shifting before extraction won't work since the timestamp may not fit
    // even if the time itself fits
    if (in_type.unit() != out_type.unit()) {
      auto conversion = util::GetTimestampConversion(in_type.unit(), out_type.unit());
      if (conversion.first == util::MULTIPLY) {
        return ExtractTemporal<ExtractTimeUpscaledUnchecked, Time64Type>(
            ctx, batch, out, conversion.second);
      } else {
        if (options.allow_time_truncate) {
          return ExtractTemporal<ExtractTimeDownscaledUnchecked, Time64Type>(
              ctx, batch, out, conversion.second);
        } else {
          return ExtractTemporal<ExtractTimeDownscaled, Time64Type>(ctx, batch, out,
                                                                    conversion.second);
        }
      }
    }
    return ExtractTemporal<ExtractTimeUpscaledUnchecked, Time64Type>(ctx, batch, out, 1);
  }
};

// ----------------------------------------------------------------------
// From one time32 or time64 to another

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_t<is_time_type<I>::value && is_time_type<O>::value>> {
  using in_t = typename I::c_type;
  using out_t = typename O::c_type;

  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const ArraySpan& input = batch[0].array;
    ArraySpan* output = out->array_span_mutable();

    // If units are the same, zero copy, otherwise convert
    const auto& in_type = checked_cast<const I&>(*input.type);
    const auto& out_type = checked_cast<const O&>(*output->type);
    DCHECK_NE(in_type.unit(), out_type.unit()) << "Do not cast equal types";
    auto conversion = util::GetTimestampConversion(in_type.unit(), out_type.unit());
    return ShiftTime<in_t, out_t>(ctx, conversion.first, conversion.second, input,
                                  output);
  }
};

// ----------------------------------------------------------------------
// Between date32 and date64

template <>
struct CastFunctor<Date64Type, Date32Type> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ShiftTime<int32_t, int64_t>(ctx, util::MULTIPLY, kMillisecondsInDay,
                                       batch[0].array, out->array_span_mutable());
  }
};

template <>
struct CastFunctor<Date32Type, Date64Type> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    return ShiftTime<int64_t, int32_t>(ctx, util::DIVIDE, kMillisecondsInDay,
                                       batch[0].array, out->array_span_mutable());
  }
};

// ----------------------------------------------------------------------
// date32, date64 to timestamp

template <>
struct CastFunctor<TimestampType, Date32Type> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& out_type = checked_cast<const TimestampType&>(*out->type());
    // get conversion SECOND -> unit
    auto conversion = util::GetTimestampConversion(TimeUnit::SECOND, out_type.unit());
    DCHECK_EQ(conversion.first, util::MULTIPLY);

    // multiply to achieve days -> unit
    conversion.second *= kMillisecondsInDay / 1000;
    return ShiftTime<int32_t, int64_t>(ctx, util::MULTIPLY, conversion.second,
                                       batch[0].array, out->array_span_mutable());
  }
};

template <>
struct CastFunctor<TimestampType, Date64Type> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& out_type = checked_cast<const TimestampType&>(*out->type());

    // date64 is ms since epoch
    auto conversion = util::GetTimestampConversion(TimeUnit::MILLI, out_type.unit());
    return ShiftTime<int64_t, int64_t>(ctx, conversion.first, conversion.second,
                                       batch[0].array, out->array_span_mutable());
  }
};

// ----------------------------------------------------------------------
// String to Timestamp

struct ParseTimestamp {
  explicit ParseTimestamp(const TimestampType& type)
      : type(type), expect_timezone(!type.timezone().empty()) {}
  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext*, Arg0Value val, Status* st) const {
    OutValue result = 0;
    bool zone_offset_present = false;
    if (ARROW_PREDICT_FALSE(!ParseTimestampISO8601(val.data(), val.size(), type.unit(),
                                                   &result, &zone_offset_present))) {
      *st = Status::Invalid("Failed to parse string: '", val, "' as a scalar of type ",
                            type.ToString());
    }
    if (zone_offset_present != expect_timezone) {
      if (expect_timezone) {
        *st = Status::Invalid(
            "Failed to parse string: '", val, "' as a scalar of type ", type.ToString(),
            ": expected a zone offset. If these timestamps "
            "are in local time, cast to timestamp without timezone, then "
            "call assume_timezone.");
      } else {
        *st = Status::Invalid("Failed to parse string: '", val, "' as a scalar of type ",
                              type.ToString(), ": expected no zone offset.");
      }
    }
    return result;
  }

  const TimestampType& type;
  bool expect_timezone;
};

template <typename I>
struct CastFunctor<TimestampType, I, enable_if_t<is_base_binary_type<I>::value>> {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    const auto& out_type = checked_cast<const TimestampType&>(*out->type());
    applicator::ScalarUnaryNotNullStateful<TimestampType, I, ParseTimestamp> kernel(
        ParseTimestamp{out_type});
    return kernel.Exec(ctx, batch, out);
  }
};

template <typename Type>
void AddCrossUnitCast(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastFunctor<Type, Type>::Exec;
  kernel.signature = KernelSignature::Make({InputType(Type::type_id)}, kOutputTargetType);
  DCHECK_OK(func->AddKernel(Type::type_id, std::move(kernel)));
}

template <typename Type>
void AddCrossUnitCastNoPreallocate(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastFunctor<Type, Type>::Exec;
  kernel.null_handling = NullHandling::INTERSECTION;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  kernel.signature = KernelSignature::Make({InputType(Type::type_id)}, kOutputTargetType);
  DCHECK_OK(func->AddKernel(Type::type_id, std::move(kernel)));
}

std::shared_ptr<CastFunction> GetDate32Cast() {
  auto func = std::make_shared<CastFunction>("cast_date32", Type::DATE32);
  auto out_ty = date32();
  AddCommonCasts(Type::DATE32, out_ty, func.get());

  // int32 -> date32
  AddZeroCopyCast(Type::INT32, int32(), date32(), func.get());

  // date64 -> date32
  AddSimpleCast<Date64Type, Date32Type>(date64(), date32(), func.get());

  // timestamp -> date32
  AddSimpleCast<TimestampType, Date32Type>(InputType(Type::TIMESTAMP), date32(),
                                           func.get());
  return func;
}

std::shared_ptr<CastFunction> GetDate64Cast() {
  auto func = std::make_shared<CastFunction>("cast_date64", Type::DATE64);
  auto out_ty = date64();
  AddCommonCasts(Type::DATE64, out_ty, func.get());

  // int64 -> date64
  AddZeroCopyCast(Type::INT64, int64(), date64(), func.get());

  // date32 -> date64
  AddSimpleCast<Date32Type, Date64Type>(date32(), date64(), func.get());

  // timestamp -> date64
  AddSimpleCast<TimestampType, Date64Type>(InputType(Type::TIMESTAMP), date64(),
                                           func.get());
  return func;
}

std::shared_ptr<CastFunction> GetDurationCast() {
  auto func = std::make_shared<CastFunction>("cast_duration", Type::DURATION);
  AddCommonCasts(Type::DURATION, kOutputTargetType, func.get());

  auto seconds = duration(TimeUnit::SECOND);
  auto millis = duration(TimeUnit::MILLI);
  auto micros = duration(TimeUnit::MICRO);
  auto nanos = duration(TimeUnit::NANO);

  // Same integer representation
  AddZeroCopyCast(Type::INT64, /*in_type=*/int64(), kOutputTargetType, func.get());

  // Between durations
  AddCrossUnitCastNoPreallocate<DurationType>(func.get());

  return func;
}

std::shared_ptr<CastFunction> GetIntervalCast() {
  auto func = std::make_shared<CastFunction>("cast_month_day_nano_interval",
                                             Type::INTERVAL_MONTH_DAY_NANO);
  AddCommonCasts(Type::INTERVAL_MONTH_DAY_NANO, kOutputTargetType, func.get());
  return func;
}

std::shared_ptr<CastFunction> GetTime32Cast() {
  auto func = std::make_shared<CastFunction>("cast_time32", Type::TIME32);
  AddCommonCasts(Type::TIME32, kOutputTargetType, func.get());

  // Zero copy when the unit is the same or same integer representation
  AddZeroCopyCast(Type::INT32, /*in_type=*/int32(), kOutputTargetType, func.get());

  // time64 -> time32
  AddSimpleCast<Time64Type, Time32Type>(InputType(Type::TIME64), kOutputTargetType,
                                        func.get());

  // time32 -> time32
  AddCrossUnitCast<Time32Type>(func.get());

  // timestamp -> time32
  AddSimpleCast<TimestampType, Time32Type>(InputType(Type::TIMESTAMP), kOutputTargetType,
                                           func.get());

  return func;
}

std::shared_ptr<CastFunction> GetTime64Cast() {
  auto func = std::make_shared<CastFunction>("cast_time64", Type::TIME64);
  AddCommonCasts(Type::TIME64, kOutputTargetType, func.get());

  // Zero copy when the unit is the same or same integer representation
  AddZeroCopyCast(Type::INT64, /*in_type=*/int64(), kOutputTargetType, func.get());

  // time32 -> time64
  AddSimpleCast<Time32Type, Time64Type>(InputType(Type::TIME32), kOutputTargetType,
                                        func.get());

  // Between durations
  AddCrossUnitCast<Time64Type>(func.get());

  // timestamp -> time64
  AddSimpleCast<TimestampType, Time64Type>(InputType(Type::TIMESTAMP), kOutputTargetType,
                                           func.get());

  return func;
}

std::shared_ptr<CastFunction> GetTimestampCast() {
  auto func = std::make_shared<CastFunction>("cast_timestamp", Type::TIMESTAMP);
  AddCommonCasts(Type::TIMESTAMP, kOutputTargetType, func.get());

  // Same integer representation
  AddZeroCopyCast(Type::INT64, /*in_type=*/int64(), kOutputTargetType, func.get());

  // From date types
  // TODO: ARROW-8876, these casts are not directly tested
  AddSimpleCast<Date32Type, TimestampType>(InputType(Type::DATE32), kOutputTargetType,
                                           func.get());
  AddSimpleCast<Date64Type, TimestampType>(InputType(Type::DATE64), kOutputTargetType,
                                           func.get());

  // string -> timestamp
  AddSimpleCast<StringType, TimestampType>(utf8(), kOutputTargetType, func.get());
  // large_string -> timestamp
  AddSimpleCast<LargeStringType, TimestampType>(large_utf8(), kOutputTargetType,
                                                func.get());

  // From one timestamp to another
  AddCrossUnitCastNoPreallocate<TimestampType>(func.get());

  return func;
}

std::vector<std::shared_ptr<CastFunction>> GetTemporalCasts() {
  std::vector<std::shared_ptr<CastFunction>> functions;

  functions.push_back(GetDate32Cast());
  functions.push_back(GetDate64Cast());
  functions.push_back(GetDurationCast());
  functions.push_back(GetIntervalCast());
  functions.push_back(GetTime32Cast());
  functions.push_back(GetTime64Cast());
  functions.push_back(GetTimestampCast());
  return functions;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
