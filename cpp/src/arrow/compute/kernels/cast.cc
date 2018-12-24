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

#include "arrow/compute/kernels/cast.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/parsing.h"  // IWYU pragma: keep
#include "arrow/util/utf8.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util-internal.h"

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define FUNC_RETURN_NOT_OK(s)                                                       \
  do {                                                                              \
    Status _s = (s);                                                                \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                                            \
      std::stringstream ss;                                                         \
      ss << __FILE__ << ":" << __LINE__ << " code: " << #s << "\n" << _s.message(); \
      ctx->SetStatus(Status(_s.code(), ss.str()));                                  \
      return;                                                                       \
    }                                                                               \
  } while (0)

#else

#define FUNC_RETURN_NOT_OK(s)            \
  do {                                   \
    Status _s = (s);                     \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      ctx->SetStatus(_s);                \
      return;                            \
    }                                    \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

namespace compute {

constexpr int64_t kMillisecondsInDay = 86400000;

template <typename O, typename I, typename Enable = void>
struct is_binary_to_string {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_binary_to_string<
    O, I,
    typename std::enable_if<std::is_same<BinaryType, I>::value &&
                            std::is_base_of<StringType, O>::value>::type> {
  static constexpr bool value = true;
};

// ----------------------------------------------------------------------
// Zero copy casts

template <typename O, typename I, typename Enable = void>
struct is_zero_copy_cast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_zero_copy_cast<
    O, I,
    typename std::enable_if<std::is_same<I, O>::value &&
                            // Parametric types contains runtime data which
                            // differentiate them, it cannot be checked statically.
                            !std::is_base_of<ParametricType, O>::value>::type> {
  static constexpr bool value = true;
};

// From integers to date/time types with zero copy
template <typename O, typename I>
struct is_zero_copy_cast<
    O, I,
    typename std::enable_if<
        (std::is_base_of<Integer, I>::value &&
         (std::is_base_of<TimeType, O>::value || std::is_base_of<DateType, O>::value ||
          std::is_base_of<TimestampType, O>::value)) ||
        (std::is_base_of<Integer, O>::value &&
         (std::is_base_of<TimeType, I>::value || std::is_base_of<DateType, I>::value ||
          std::is_base_of<TimestampType, I>::value))>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value = sizeof(O_T) == sizeof(I_T);
};

// Binary to String doesn't require copying, the payload only needs to be
// validated.
template <typename O, typename I>
struct is_zero_copy_cast<
    O, I,
    typename std::enable_if<!std::is_same<I, O>::value &&
                            is_binary_to_string<O, I>::value>::type> {
  static constexpr bool value = true;
};

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// Indicated no computation required
//
// The case BinaryType -> StringType is special cased due to validation
// requirements.
template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_zero_copy_cast<O, I>::value &&
                                           !is_binary_to_string<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ZeroCopyData(input, output);
  }
};

// ----------------------------------------------------------------------
// Null to other things

template <typename T>
struct CastFunctor<
    T, NullType,
    typename std::enable_if<std::is_base_of<FixedWidthType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {}
};

template <>
struct CastFunctor<NullType, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {}
};

// ----------------------------------------------------------------------
// Boolean to other things

// Cast from Boolean to other numbers
template <typename T>
struct CastFunctor<T, BooleanType, enable_if_number<T>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using c_type = typename T::c_type;
    constexpr auto kOne = static_cast<c_type>(1);
    constexpr auto kZero = static_cast<c_type>(0);

    internal::BitmapReader bit_reader(input.buffers[1]->data(), input.offset,
                                      input.length);
    auto out = output->GetMutableValues<c_type>(1);
    for (int64_t i = 0; i < input.length; ++i) {
      *out++ = bit_reader.IsSet() ? kOne : kZero;
      bit_reader.Next();
    }
  }
};

// Number to Boolean
template <typename I>
struct CastFunctor<BooleanType, I,
                   typename std::enable_if<std::is_base_of<Number, I>::value &&
                                           !std::is_same<BooleanType, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    auto in_data = input.GetValues<typename I::c_type>(1);
    const auto generate = [&in_data]() -> bool { return *in_data++ != 0; };
    internal::GenerateBitsUnrolled(output->buffers[1]->mutable_data(), output->offset,
                                   input.length, generate);
  }
};

// ----------------------------------------------------------------------
// Integers and Floating Point

// Conversions pairs (<O, I>) are partitioned in 4 type traits:
// - is_number_downcast
// - is_integral_signed_to_unsigned
// - is_integral_unsigned_to_signed
// - is_float_truncate
//
// Each class has a different way of validation if the conversion is safe
// (either with bounded intervals or with explicit C casts)

template <typename O, typename I, typename Enable = void>
struct is_number_downcast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_number_downcast<
    O, I,
    typename std::enable_if<std::is_base_of<Number, O>::value &&
                            std::is_base_of<Number, I>::value>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       // Both types are of the same sign-ness.
       ((std::is_signed<O_T>::value == std::is_signed<I_T>::value) &&
        // Both types are of the same integral-ness.
        (std::is_floating_point<O_T>::value == std::is_floating_point<I_T>::value)) &&
       // Smaller output size
       (sizeof(O_T) < sizeof(I_T)));
};

template <typename O, typename I, typename Enable = void>
struct is_integral_signed_to_unsigned {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_integral_signed_to_unsigned<
    O, I,
    typename std::enable_if<std::is_base_of<Integer, O>::value &&
                            std::is_base_of<Integer, I>::value>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       ((std::is_unsigned<O_T>::value && std::is_signed<I_T>::value)));
};

template <typename O, typename I, typename Enable = void>
struct is_integral_unsigned_to_signed {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_integral_unsigned_to_signed<
    O, I,
    typename std::enable_if<std::is_base_of<Integer, O>::value &&
                            std::is_base_of<Integer, I>::value>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      ((!std::is_same<O, I>::value) &&
       ((std::is_signed<O_T>::value && std::is_unsigned<I_T>::value)));
};

// This set of functions SafeMinimum/SafeMaximum would be simplified with
// C++17 and `if constexpr`.

// clang-format doesn't handle this construct properly. Thus the macro, but it
// also improves readability.
//
// The effective return type of the function is always `I::c_type`, this is
// just how enable_if works with functions.
#define RET_TYPE(TRAIT) \
  typename std::enable_if<TRAIT<O, I>::value, typename I::c_type>::type

template <typename O, typename I>
constexpr RET_TYPE(is_number_downcast) SafeMinimum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::lowest();
}

template <typename O, typename I>
constexpr RET_TYPE(is_number_downcast) SafeMaximum() {
  using out_type = typename O::c_type;

  return std::numeric_limits<out_type>::max();
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_unsigned_to_signed) SafeMinimum() {
  return 0;
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_unsigned_to_signed) SafeMaximum() {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  // Equality is missing because in_type::max() > out_type::max() when types
  // are of the same width.
  return static_cast<in_type>(sizeof(in_type) < sizeof(out_type)
                                  ? std::numeric_limits<in_type>::max()
                                  : std::numeric_limits<out_type>::max());
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_signed_to_unsigned) SafeMinimum() {
  return 0;
}

template <typename O, typename I>
constexpr RET_TYPE(is_integral_signed_to_unsigned) SafeMaximum() {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  return static_cast<in_type>(sizeof(in_type) <= sizeof(out_type)
                                  ? std::numeric_limits<in_type>::max()
                                  : std::numeric_limits<out_type>::max());
}

#undef RET_TYPE

template <typename O, typename I>
struct CastFunctor<
    O, I,
    typename std::enable_if<is_number_downcast<O, I>::value ||
                            is_integral_signed_to_unsigned<O, I>::value ||
                            is_integral_unsigned_to_signed<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_offset = input.offset;

    const in_type* in_data = input.GetValues<in_type>(1);
    auto out_data = output->GetMutableValues<out_type>(1);

    if (!options.allow_int_overflow) {
      constexpr in_type kMax = SafeMaximum<O, I>();
      constexpr in_type kMin = SafeMinimum<O, I>();

      // Null count may be -1 if the input array had been sliced
      if (input.null_count != 0) {
        internal::BitmapReader is_valid_reader(input.buffers[0]->data(), in_offset,
                                               input.length);
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(is_valid_reader.IsSet() &&
                                  (*in_data > kMax || *in_data < kMin))) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
          is_valid_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; ++i) {
          if (ARROW_PREDICT_FALSE(*in_data > kMax || *in_data < kMin)) {
            ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
          }
          *out_data++ = static_cast<out_type>(*in_data++);
        }
      }
    } else {
      for (int64_t i = 0; i < input.length; ++i) {
        *out_data++ = static_cast<out_type>(*in_data++);
      }
    }
  }
};

// Float to Integer or Integer to Float
template <typename O, typename I, typename Enable = void>
struct is_float_truncate {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_float_truncate<
    O, I,
    typename std::enable_if<(std::is_base_of<Integer, O>::value &&
                             std::is_base_of<FloatingPoint, I>::value) ||
                            (std::is_base_of<Integer, I>::value &&
                             std::is_base_of<FloatingPoint, O>::value)>::type> {
  static constexpr bool value = true;
};

template <typename O, typename I>
struct CastFunctor<O, I, typename std::enable_if<is_float_truncate<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    auto in_offset = input.offset;
    const in_type* in_data = input.GetValues<in_type>(1);
    auto out_data = output->GetMutableValues<out_type>(1);

    if (options.allow_float_truncate) {
      // unsafe cast
      for (int64_t i = 0; i < input.length; ++i) {
        *out_data++ = static_cast<out_type>(*in_data++);
      }
    } else {
      // safe cast
      if (input.null_count != 0) {
        internal::BitmapReader is_valid_reader(input.buffers[0]->data(), in_offset,
                                               input.length);
        for (int64_t i = 0; i < input.length; ++i) {
          auto out_value = static_cast<out_type>(*in_data);
          if (ARROW_PREDICT_FALSE(is_valid_reader.IsSet() &&
                                  static_cast<in_type>(out_value) != *in_data)) {
            ctx->SetStatus(Status::Invalid("Floating point value truncated"));
          }
          *out_data++ = out_value;
          in_data++;
          is_valid_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; ++i) {
          auto out_value = static_cast<out_type>(*in_data);
          if (ARROW_PREDICT_FALSE(static_cast<in_type>(out_value) != *in_data)) {
            ctx->SetStatus(Status::Invalid("Floating point value truncated"));
          }
          *out_data++ = out_value;
          in_data++;
        }
      }
    }
  }
};

// Leftover of Number combinations that are safe to cast.
template <typename O, typename I, typename Enable = void>
struct is_safe_numeric_cast {
  static constexpr bool value = false;
};

template <typename O, typename I>
struct is_safe_numeric_cast<
    O, I,
    typename std::enable_if<std::is_base_of<Number, O>::value &&
                            std::is_base_of<Number, I>::value>::type> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      (std::is_signed<O_T>::value == std::is_signed<I_T>::value) &&
      (std::is_integral<O_T>::value == std::is_integral<I_T>::value) &&
      (sizeof(O_T) >= sizeof(I_T)) && (!std::is_same<O, I>::value);
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<is_safe_numeric_cast<O, I>::value &&
                                           !is_float_truncate<O, I>::value &&
                                           !is_number_downcast<O, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_type = typename I::c_type;
    using out_type = typename O::c_type;

    const in_type* in_data = input.GetValues<in_type>(1);
    auto out_data = output->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < input.length; ++i) {
      // Due to various checks done via type-trait, the cast is safe and bear
      // no truncation.
      *out_data++ = static_cast<out_type>(*in_data++);
    }
  }
};

// ----------------------------------------------------------------------
// From one timestamp to another

template <typename in_type, typename out_type>
void ShiftTime(FunctionContext* ctx, const CastOptions& options, const bool is_multiply,
               const int64_t factor, const ArrayData& input, ArrayData* output) {
  const in_type* in_data = input.GetValues<in_type>(1);
  auto out_data = output->GetMutableValues<out_type>(1);

  if (factor == 1) {
    for (int64_t i = 0; i < input.length; i++) {
      out_data[i] = static_cast<out_type>(in_data[i]);
    }
  } else if (is_multiply) {
    for (int64_t i = 0; i < input.length; i++) {
      out_data[i] = static_cast<out_type>(in_data[i] * factor);
    }
  } else {
    if (options.allow_time_truncate) {
      for (int64_t i = 0; i < input.length; i++) {
        out_data[i] = static_cast<out_type>(in_data[i] / factor);
      }
    } else {
#define RAISE_INVALID_CAST(VAL)                                                   \
  ctx->SetStatus(Status::Invalid("Casting from ", input.type->ToString(), " to ", \
                                 output->type->ToString(), " would lose data: ", VAL));

      if (input.null_count != 0) {
        internal::BitmapReader bit_reader(input.buffers[0]->data(), input.offset,
                                          input.length);
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (bit_reader.IsSet() && (out_data[i] * factor != in_data[i])) {
            RAISE_INVALID_CAST(in_data[i]);
            break;
          }
          bit_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; i++) {
          out_data[i] = static_cast<out_type>(in_data[i] / factor);
          if (out_data[i] * factor != in_data[i]) {
            RAISE_INVALID_CAST(in_data[i]);
            break;
          }
        }
      }

#undef RAISE_INVALID_CAST
    }
  }
}

namespace {

// {is_multiply, factor}
const std::pair<bool, int64_t> kTimeConversionTable[4][4] = {
    {{true, 1}, {true, 1000}, {true, 1000000}, {true, 1000000000L}},     // SECOND
    {{false, 1000}, {true, 1}, {true, 1000}, {true, 1000000}},           // MILLI
    {{false, 1000000}, {false, 1000}, {true, 1}, {true, 1000}},          // MICRO
    {{false, 1000000000L}, {false, 1000000}, {false, 1000}, {true, 1}},  // NANO
};

}  // namespace

template <>
struct CastFunctor<TimestampType, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    // If units are the same, zero copy, otherwise convert
    const auto& in_type = checked_cast<const TimestampType&>(*input.type);
    const auto& out_type = checked_cast<const TimestampType&>(*output->type);

    if (in_type.unit() == out_type.unit()) {
      ZeroCopyData(input, output);
      return;
    }

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(out_type.unit())];

    ShiftTime<int64_t, int64_t>(ctx, options, conversion.first, conversion.second, input,
                                output);
  }
};

template <>
struct CastFunctor<Date32Type, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type = checked_cast<const TimestampType&>(*input.type);

    static const int64_t kTimestampToDateFactors[4] = {
        86400LL,                             // SECOND
        86400LL * 1000LL,                    // MILLI
        86400LL * 1000LL * 1000LL,           // MICRO
        86400LL * 1000LL * 1000LL * 1000LL,  // NANO
    };

    const int64_t factor = kTimestampToDateFactors[static_cast<int>(in_type.unit())];
    ShiftTime<int64_t, int32_t>(ctx, options, false, factor, input, output);
  }
};

template <>
struct CastFunctor<Date64Type, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type = checked_cast<const TimestampType&>(*input.type);

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(TimeUnit::MILLI)];

    ShiftTime<int64_t, int64_t>(ctx, options, conversion.first, conversion.second, input,
                                output);

    // Ensure that intraday milliseconds have been zeroed out
    auto out_data = output->GetMutableValues<int64_t>(1);

    if (input.null_count != 0) {
      internal::BitmapReader bit_reader(input.buffers[0]->data(), input.offset,
                                        input.length);

      for (int64_t i = 0; i < input.length; ++i) {
        const int64_t remainder = out_data[i] % kMillisecondsInDay;
        if (ARROW_PREDICT_FALSE(!options.allow_time_truncate && bit_reader.IsSet() &&
                                remainder > 0)) {
          ctx->SetStatus(
              Status::Invalid("Timestamp value had non-zero intraday milliseconds"));
          break;
        }
        out_data[i] -= remainder;
        bit_reader.Next();
      }
    } else {
      for (int64_t i = 0; i < input.length; ++i) {
        const int64_t remainder = out_data[i] % kMillisecondsInDay;
        if (ARROW_PREDICT_FALSE(!options.allow_time_truncate && remainder > 0)) {
          ctx->SetStatus(
              Status::Invalid("Timestamp value had non-zero intraday milliseconds"));
          break;
        }
        out_data[i] -= remainder;
      }
    }
  }
};

// ----------------------------------------------------------------------
// From one time32 or time64 to another

template <typename O, typename I>
struct CastFunctor<O, I,
                   typename std::enable_if<std::is_base_of<TimeType, I>::value &&
                                           std::is_base_of<TimeType, O>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using in_t = typename I::c_type;
    using out_t = typename O::c_type;

    // If units are the same, zero copy, otherwise convert
    const auto& in_type = checked_cast<const I&>(*input.type);
    const auto& out_type = checked_cast<const O&>(*output->type);

    if (in_type.unit() == out_type.unit()) {
      ZeroCopyData(input, output);
      return;
    }

    std::pair<bool, int64_t> conversion =
        kTimeConversionTable[static_cast<int>(in_type.unit())]
                            [static_cast<int>(out_type.unit())];

    ShiftTime<in_t, out_t>(ctx, options, conversion.first, conversion.second, input,
                           output);
  }
};

// ----------------------------------------------------------------------
// Between date32 and date64

template <>
struct CastFunctor<Date64Type, Date32Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ShiftTime<int32_t, int64_t>(ctx, options, true, kMillisecondsInDay, input, output);
  }
};

template <>
struct CastFunctor<Date32Type, Date64Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ShiftTime<int64_t, int32_t>(ctx, options, false, kMillisecondsInDay, input, output);
  }
};

// ----------------------------------------------------------------------
// List to List

class ListCastKernel : public UnaryKernel {
 public:
  ListCastKernel(std::unique_ptr<UnaryKernel> child_caster,
                 const std::shared_ptr<DataType>& out_type)
      : child_caster_(std::move(child_caster)), out_type_(out_type) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    DCHECK_EQ(Type::LIST, in_data.type->id());
    ArrayData* result;

    if (in_data.offset != 0) {
      return Status::NotImplemented(
          "Casting sliced lists (non-zero offset) not yet implemented");
    }

    if (out->kind() == Datum::NONE) {
      out->value = ArrayData::Make(out_type_, in_data.length);
    }

    result = out->array().get();

    // Copy buffers from parent
    result->buffers = in_data.buffers;

    Datum casted_child;
    RETURN_NOT_OK(child_caster_->Call(ctx, Datum(in_data.child_data[0]), &casted_child));
    result->child_data.push_back(casted_child.array());

    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  std::unique_ptr<UnaryKernel> child_caster_;
  std::shared_ptr<DataType> out_type_;
};

// ----------------------------------------------------------------------
// Dictionary to other things

template <typename IndexType>
void UnpackFixedSizeBinaryDictionary(FunctionContext* ctx, const Array& indices,
                                     const FixedSizeBinaryArray& dictionary,
                                     ArrayData* output) {
  using index_c_type = typename IndexType::c_type;

  const index_c_type* in = indices.data()->GetValues<index_c_type>(1);
  int32_t byte_width =
      checked_cast<const FixedSizeBinaryType&>(*output->type).byte_width();

  uint8_t* out = output->buffers[1]->mutable_data() + byte_width * output->offset;

  if (indices.null_count() != 0) {
    internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                             indices.length());

    for (int64_t i = 0; i < indices.length(); ++i) {
      if (valid_bits_reader.IsSet()) {
        const uint8_t* value = dictionary.Value(in[i]);
        memcpy(out + i * byte_width, value, byte_width);
      }
      valid_bits_reader.Next();
    }
  } else {
    for (int64_t i = 0; i < indices.length(); ++i) {
      const uint8_t* value = dictionary.Value(in[i]);
      memcpy(out + i * byte_width, value, byte_width);
    }
  }
}

template <typename T>
struct CastFunctor<
    T, DictionaryType,
    typename std::enable_if<std::is_base_of<FixedSizeBinaryType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    DictionaryArray dict_array(input.Copy());

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const DataType& values_type = *type.dictionary()->type();
    const FixedSizeBinaryArray& dictionary =
        checked_cast<const FixedSizeBinaryArray&>(*type.dictionary());

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        UnpackFixedSizeBinaryDictionary<Int8Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT16:
        UnpackFixedSizeBinaryDictionary<Int16Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT32:
        UnpackFixedSizeBinaryDictionary<Int32Type>(ctx, indices, dictionary, output);
        break;
      case Type::INT64:
        UnpackFixedSizeBinaryDictionary<Int64Type>(ctx, indices, dictionary, output);
        break;
      default:
        ctx->SetStatus(
            Status::Invalid("Invalid index type: ", indices.type()->ToString()));
        return;
    }
  }
};

template <typename IndexType>
Status UnpackBinaryDictionary(FunctionContext* ctx, const Array& indices,
                              const BinaryArray& dictionary, ArrayData* output) {
  using index_c_type = typename IndexType::c_type;
  std::unique_ptr<ArrayBuilder> builder;
  RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), output->type, &builder));
  BinaryBuilder* binary_builder = checked_cast<BinaryBuilder*>(builder.get());

  const index_c_type* in = indices.data()->GetValues<index_c_type>(1);
  if (indices.null_count() != 0) {
    internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                             indices.length());

    for (int64_t i = 0; i < indices.length(); ++i) {
      if (valid_bits_reader.IsSet()) {
        RETURN_NOT_OK(binary_builder->Append(dictionary.GetView(in[i])));
      } else {
        RETURN_NOT_OK(binary_builder->AppendNull());
      }
      valid_bits_reader.Next();
    }
  } else {
    for (int64_t i = 0; i < indices.length(); ++i) {
      RETURN_NOT_OK(binary_builder->Append(dictionary.GetView(in[i])));
    }
  }

  std::shared_ptr<Array> plain_array;
  RETURN_NOT_OK(binary_builder->Finish(&plain_array));
  // Copy all buffer except the valid bitmap
  for (size_t i = 1; i < plain_array->data()->buffers.size(); i++) {
    output->buffers.push_back(plain_array->data()->buffers[i]);
  }

  return Status::OK();
}

template <typename T>
struct CastFunctor<T, DictionaryType,
                   typename std::enable_if<std::is_base_of<BinaryType, T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    DictionaryArray dict_array(input.Copy());

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const DataType& values_type = *type.dictionary()->type();
    const BinaryArray& dictionary = checked_cast<const BinaryArray&>(*type.dictionary());

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int8Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT16:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int16Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT32:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int32Type>(ctx, indices, dictionary, output)));
        break;
      case Type::INT64:
        FUNC_RETURN_NOT_OK(
            (UnpackBinaryDictionary<Int64Type>(ctx, indices, dictionary, output)));
        break;
      default:
        ctx->SetStatus(
            Status::Invalid("Invalid index type: ", indices.type()->ToString()));
        return;
    }
  }
};

template <typename IndexType, typename c_type>
void UnpackPrimitiveDictionary(const Array& indices, const c_type* dictionary,
                               c_type* out) {
  internal::BitmapReader valid_bits_reader(indices.null_bitmap_data(), indices.offset(),
                                           indices.length());

  auto in = indices.data()->GetValues<typename IndexType::c_type>(1);
  for (int64_t i = 0; i < indices.length(); ++i) {
    if (valid_bits_reader.IsSet()) {
      out[i] = dictionary[in[i]];
    }
    valid_bits_reader.Next();
  }
}

// Cast from dictionary to plain representation
template <typename T>
struct CastFunctor<T, DictionaryType,
                   typename std::enable_if<IsNumeric<T>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using c_type = typename T::c_type;

    DictionaryArray dict_array(input.Copy());

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const DataType& values_type = *type.dictionary()->type();

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    const c_type* dictionary = type.dictionary()->data()->GetValues<c_type>(1);

    auto out = output->GetMutableValues<c_type>(1);
    const Array& indices = *dict_array.indices();
    switch (indices.type()->id()) {
      case Type::INT8:
        UnpackPrimitiveDictionary<Int8Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT16:
        UnpackPrimitiveDictionary<Int16Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT32:
        UnpackPrimitiveDictionary<Int32Type, c_type>(indices, dictionary, out);
        break;
      case Type::INT64:
        UnpackPrimitiveDictionary<Int64Type, c_type>(indices, dictionary, out);
        break;
      default:
        ctx->SetStatus(
            Status::Invalid("Invalid index type: ", indices.type()->ToString()));
        return;
    }
  }
};

// ----------------------------------------------------------------------
// String to Number

template <typename O>
struct CastFunctor<O, StringType, enable_if_number<O>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using out_type = typename O::c_type;

    StringArray input_array(input.Copy());
    auto out_data = output->GetMutableValues<out_type>(1);
    internal::StringConverter<O> converter;

    for (int64_t i = 0; i < input.length; ++i, ++out_data) {
      if (input_array.IsNull(i)) {
        continue;
      }

      auto str = input_array.GetView(i);
      if (!converter(str.data(), str.length(), out_data)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '", str, "' into ",
                                       output->type->ToString()));
        return;
      }
    }
  }
};

// ----------------------------------------------------------------------
// String to Boolean

template <typename O>
struct CastFunctor<O, StringType,
                   typename std::enable_if<std::is_same<BooleanType, O>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    StringArray input_array(input.Copy());
    internal::FirstTimeBitmapWriter writer(output->buffers[1]->mutable_data(),
                                           output->offset, input.length);
    internal::StringConverter<O> converter;

    for (int64_t i = 0; i < input.length; ++i) {
      if (input_array.IsNull(i)) {
        writer.Next();
        continue;
      }

      bool value;
      auto str = input_array.GetView(i);
      if (!converter(str.data(), str.length(), &value)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '",
                                       input_array.GetString(i), "' into ",
                                       output->type->ToString()));
        return;
      }

      if (value) {
        writer.Set();
      } else {
        writer.Clear();
      }
      writer.Next();
    }
    writer.Finish();
  }
};

// ----------------------------------------------------------------------
// String to Timestamp

template <>
struct CastFunctor<TimestampType, StringType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using out_type = TimestampType::c_type;

    StringArray input_array(input.Copy());
    auto out_data = output->GetMutableValues<out_type>(1);
    internal::StringConverter<TimestampType> converter(output->type);

    for (int64_t i = 0; i < input.length; ++i, ++out_data) {
      if (input_array.IsNull(i)) {
        continue;
      }

      const auto str = input_array.GetView(i);
      if (!converter(str.data(), str.length(), out_data)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '", str, "' into ",
                                       output->type->ToString()));
        return;
      }
    }
  }
};

// ----------------------------------------------------------------------
// Binary to String
//

template <typename I>
struct CastFunctor<
    StringType, I,
    typename std::enable_if<is_binary_to_string<StringType, I>::value>::type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    BinaryArray binary(input.Copy());

    if (options.allow_invalid_utf8) {
      ZeroCopyData(input, output);
      return;
    }

    util::InitializeUTF8();

    if (binary.null_count() != 0) {
      for (int64_t i = 0; i < input.length; i++) {
        if (binary.IsNull(i)) {
          continue;
        }

        const auto str = binary.GetView(i);
        if (ARROW_PREDICT_FALSE(!arrow::util::ValidateUTF8(str))) {
          ctx->SetStatus(Status::Invalid("Invalid UTF8 payload"));
          return;
        }
      }

    } else {
      for (int64_t i = 0; i < input.length; i++) {
        const auto str = binary.GetView(i);
        if (ARROW_PREDICT_FALSE(!arrow::util::ValidateUTF8(str))) {
          ctx->SetStatus(Status::Invalid("Invalid UTF8 payload"));
          return;
        }
      }
    }

    ZeroCopyData(input, output);
  }
};

// ----------------------------------------------------------------------

typedef std::function<void(FunctionContext*, const CastOptions& options, const ArrayData&,
                           ArrayData*)>
    CastFunction;

static Status AllocateIfNotPreallocated(FunctionContext* ctx, const ArrayData& input,
                                        bool can_pre_allocate_values, ArrayData* out) {
  const int64_t length = input.length;
  out->null_count = input.null_count;

  // Propagate bitmap unless we are null type
  std::shared_ptr<Buffer> validity_bitmap = input.buffers[0];
  if (input.type->id() == Type::NA) {
    int64_t bitmap_size = BitUtil::BytesForBits(length);
    RETURN_NOT_OK(ctx->Allocate(bitmap_size, &validity_bitmap));
    memset(validity_bitmap->mutable_data(), 0, bitmap_size);
  } else if (input.offset != 0) {
    RETURN_NOT_OK(CopyBitmap(ctx->memory_pool(), validity_bitmap->data(), input.offset,
                             length, &validity_bitmap));
  }

  if (out->buffers.size() == 2) {
    // Assuming preallocated, propagage bitmap and move on
    out->buffers[0] = validity_bitmap;
    return Status::OK();
  } else {
    DCHECK_EQ(0, out->buffers.size());
  }

  out->buffers.push_back(validity_bitmap);

  if (can_pre_allocate_values) {
    std::shared_ptr<Buffer> out_data;

    const Type::type type_id = out->type->id();

    if (!(is_primitive(type_id) || type_id == Type::FIXED_SIZE_BINARY ||
          type_id == Type::DECIMAL)) {
      return Status::NotImplemented("Cannot pre-allocate memory for type: ",
                                    out->type->ToString());
    }

    if (type_id != Type::NA) {
      const auto& fw_type = checked_cast<const FixedWidthType&>(*out->type);

      int bit_width = fw_type.bit_width();
      int64_t buffer_size = 0;

      if (bit_width == 1) {
        buffer_size = BitUtil::BytesForBits(length);
      } else if (bit_width % 8 == 0) {
        buffer_size = length * fw_type.bit_width() / 8;
      } else {
        DCHECK(false);
      }

      RETURN_NOT_OK(ctx->Allocate(buffer_size, &out_data));
      memset(out_data->mutable_data(), 0, buffer_size);

      out->buffers.push_back(out_data);
    }
  }

  return Status::OK();
}

class CastKernel : public UnaryKernel {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func, bool is_zero_copy,
             bool can_pre_allocate_values, const std::shared_ptr<DataType>& out_type)
      : options_(options),
        func_(func),
        is_zero_copy_(is_zero_copy),
        can_pre_allocate_values_(can_pre_allocate_values),
        out_type_(out_type) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    if (input.kind() != Datum::ARRAY)
      return Status::NotImplemented("CastKernel only supports Datum::ARRAY input");

    const ArrayData& in_data = *input.array();

    switch (out->kind()) {
      case Datum::NONE:
        out->value = ArrayData::Make(out_type_, in_data.length);
        break;
      case Datum::ARRAY:
        break;
      default:
        return Status::NotImplemented("CastKernel only supports Datum::ARRAY output");
    }

    ArrayData* result = out->array().get();
    if (!is_zero_copy_) {
      RETURN_NOT_OK(
          AllocateIfNotPreallocated(ctx, in_data, can_pre_allocate_values_, result));
    }
    func_(ctx, options_, in_data, result);

    RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  CastOptions options_;
  CastFunction func_;
  bool is_zero_copy_;
  bool can_pre_allocate_values_;
  std::shared_ptr<DataType> out_type_;
};

#define CAST_CASE(InType, OutType)                                                      \
  case OutType::type_id:                                                                \
    is_zero_copy = is_zero_copy_cast<OutType, InType>::value;                           \
    can_pre_allocate_values =                                                           \
        !(!is_binary_like(InType::type_id) && is_binary_like(OutType::type_id));        \
    func = [](FunctionContext* ctx, const CastOptions& options, const ArrayData& input, \
              ArrayData* out) {                                                         \
      CastFunctor<OutType, InType> func;                                                \
      func(ctx, options, input, out);                                                   \
    };                                                                                  \
    break;

#define NUMERIC_CASES(FN, IN_TYPE) \
  FN(IN_TYPE, BooleanType);        \
  FN(IN_TYPE, UInt8Type);          \
  FN(IN_TYPE, Int8Type);           \
  FN(IN_TYPE, UInt16Type);         \
  FN(IN_TYPE, Int16Type);          \
  FN(IN_TYPE, UInt32Type);         \
  FN(IN_TYPE, Int32Type);          \
  FN(IN_TYPE, UInt64Type);         \
  FN(IN_TYPE, Int64Type);          \
  FN(IN_TYPE, FloatType);          \
  FN(IN_TYPE, DoubleType);

#define NULL_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)    \
  FN(NullType, Time32Type);     \
  FN(NullType, Date32Type);     \
  FN(NullType, TimestampType);  \
  FN(NullType, Time64Type);     \
  FN(NullType, Date64Type);

#define INT32_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)     \
  FN(Int32Type, Time32Type);     \
  FN(Int32Type, Date32Type);

#define INT64_CASES(FN, IN_TYPE) \
  NUMERIC_CASES(FN, IN_TYPE)     \
  FN(Int64Type, TimestampType);  \
  FN(Int64Type, Time64Type);     \
  FN(Int64Type, Date64Type);

#define DATE32_CASES(FN, IN_TYPE) \
  FN(Date32Type, Date32Type);     \
  FN(Date32Type, Date64Type);     \
  FN(Date32Type, Int32Type);

#define DATE64_CASES(FN, IN_TYPE) \
  FN(Date64Type, Date64Type);     \
  FN(Date64Type, Date32Type);     \
  FN(Date64Type, Int64Type);

#define TIME32_CASES(FN, IN_TYPE) \
  FN(Time32Type, Time32Type);     \
  FN(Time32Type, Time64Type);     \
  FN(Time32Type, Int32Type);

#define TIME64_CASES(FN, IN_TYPE) \
  FN(Time64Type, Time32Type);     \
  FN(Time64Type, Time64Type);     \
  FN(Time64Type, Int64Type);

#define TIMESTAMP_CASES(FN, IN_TYPE) \
  FN(TimestampType, TimestampType);  \
  FN(TimestampType, Date32Type);     \
  FN(TimestampType, Date64Type);     \
  FN(TimestampType, Int64Type);

#define BINARY_CASES(FN, IN_TYPE) \
  FN(BinaryType, BinaryType);     \
  FN(BinaryType, StringType);

#define STRING_CASES(FN, IN_TYPE) \
  FN(StringType, StringType);     \
  FN(StringType, BooleanType);    \
  FN(StringType, UInt8Type);      \
  FN(StringType, Int8Type);       \
  FN(StringType, UInt16Type);     \
  FN(StringType, Int16Type);      \
  FN(StringType, UInt32Type);     \
  FN(StringType, Int32Type);      \
  FN(StringType, UInt64Type);     \
  FN(StringType, Int64Type);      \
  FN(StringType, FloatType);      \
  FN(StringType, DoubleType);     \
  FN(StringType, TimestampType);

#define DICTIONARY_CASES(FN, IN_TYPE) \
  FN(IN_TYPE, NullType);              \
  FN(IN_TYPE, Time32Type);            \
  FN(IN_TYPE, Date32Type);            \
  FN(IN_TYPE, TimestampType);         \
  FN(IN_TYPE, Time64Type);            \
  FN(IN_TYPE, Date64Type);            \
  FN(IN_TYPE, UInt8Type);             \
  FN(IN_TYPE, Int8Type);              \
  FN(IN_TYPE, UInt16Type);            \
  FN(IN_TYPE, Int16Type);             \
  FN(IN_TYPE, UInt32Type);            \
  FN(IN_TYPE, Int32Type);             \
  FN(IN_TYPE, UInt64Type);            \
  FN(IN_TYPE, Int64Type);             \
  FN(IN_TYPE, FloatType);             \
  FN(IN_TYPE, DoubleType);            \
  FN(IN_TYPE, FixedSizeBinaryType);   \
  FN(IN_TYPE, Decimal128Type);        \
  FN(IN_TYPE, BinaryType);            \
  FN(IN_TYPE, StringType);

#define GET_CAST_FUNCTION(CASE_GENERATOR, InType)                              \
  static std::unique_ptr<UnaryKernel> Get##InType##CastFunc(                   \
      const std::shared_ptr<DataType>& out_type, const CastOptions& options) { \
    CastFunction func;                                                         \
    bool is_zero_copy = false;                                                 \
    bool can_pre_allocate_values = true;                                       \
    switch (out_type->id()) {                                                  \
      CASE_GENERATOR(CAST_CASE, InType);                                       \
      default:                                                                 \
        break;                                                                 \
    }                                                                          \
    if (func != nullptr) {                                                     \
      return std::unique_ptr<UnaryKernel>(new CastKernel(                      \
          options, func, is_zero_copy, can_pre_allocate_values, out_type));    \
    }                                                                          \
    return nullptr;                                                            \
  }

GET_CAST_FUNCTION(NULL_CASES, NullType);
GET_CAST_FUNCTION(NUMERIC_CASES, BooleanType);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt8Type);
GET_CAST_FUNCTION(NUMERIC_CASES, Int8Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt16Type);
GET_CAST_FUNCTION(NUMERIC_CASES, Int16Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt32Type);
GET_CAST_FUNCTION(INT32_CASES, Int32Type);
GET_CAST_FUNCTION(NUMERIC_CASES, UInt64Type);
GET_CAST_FUNCTION(INT64_CASES, Int64Type);
GET_CAST_FUNCTION(NUMERIC_CASES, FloatType);
GET_CAST_FUNCTION(NUMERIC_CASES, DoubleType);
GET_CAST_FUNCTION(DATE32_CASES, Date32Type);
GET_CAST_FUNCTION(DATE64_CASES, Date64Type);
GET_CAST_FUNCTION(TIME32_CASES, Time32Type);
GET_CAST_FUNCTION(TIME64_CASES, Time64Type);
GET_CAST_FUNCTION(TIMESTAMP_CASES, TimestampType);
GET_CAST_FUNCTION(BINARY_CASES, BinaryType);
GET_CAST_FUNCTION(STRING_CASES, StringType);
GET_CAST_FUNCTION(DICTIONARY_CASES, DictionaryType);

#define CAST_FUNCTION_CASE(InType)                      \
  case InType::type_id:                                 \
    *kernel = Get##InType##CastFunc(out_type, options); \
    break

namespace {

Status GetListCastFunc(const DataType& in_type, const std::shared_ptr<DataType>& out_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel) {
  if (out_type->id() != Type::LIST) {
    // Kernel will be null
    return Status::OK();
  }
  const DataType& in_value_type = *checked_cast<const ListType&>(in_type).value_type();
  std::shared_ptr<DataType> out_value_type =
      checked_cast<const ListType&>(*out_type).value_type();
  std::unique_ptr<UnaryKernel> child_caster;
  RETURN_NOT_OK(GetCastFunction(in_value_type, out_value_type, options, &child_caster));
  *kernel =
      std::unique_ptr<UnaryKernel>(new ListCastKernel(std::move(child_caster), out_type));
  return Status::OK();
}

}  // namespace

Status GetCastFunction(const DataType& in_type, const std::shared_ptr<DataType>& out_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel) {
  switch (in_type.id()) {
    CAST_FUNCTION_CASE(NullType);
    CAST_FUNCTION_CASE(BooleanType);
    CAST_FUNCTION_CASE(UInt8Type);
    CAST_FUNCTION_CASE(Int8Type);
    CAST_FUNCTION_CASE(UInt16Type);
    CAST_FUNCTION_CASE(Int16Type);
    CAST_FUNCTION_CASE(UInt32Type);
    CAST_FUNCTION_CASE(Int32Type);
    CAST_FUNCTION_CASE(UInt64Type);
    CAST_FUNCTION_CASE(Int64Type);
    CAST_FUNCTION_CASE(FloatType);
    CAST_FUNCTION_CASE(DoubleType);
    CAST_FUNCTION_CASE(Date32Type);
    CAST_FUNCTION_CASE(Date64Type);
    CAST_FUNCTION_CASE(Time32Type);
    CAST_FUNCTION_CASE(Time64Type);
    CAST_FUNCTION_CASE(TimestampType);
    CAST_FUNCTION_CASE(BinaryType);
    CAST_FUNCTION_CASE(StringType);
    CAST_FUNCTION_CASE(DictionaryType);
    case Type::LIST:
      RETURN_NOT_OK(GetListCastFunc(in_type, out_type, options, kernel));
      break;
    default:
      break;
  }
  if (*kernel == nullptr) {
    return Status::NotImplemented("No cast implemented from ", in_type.ToString(), " to ",
                                  out_type->ToString());
  }
  return Status::OK();
}

Status Cast(FunctionContext* ctx, const Datum& value,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            Datum* out) {
  // Dynamic dispatch to obtain right cast function
  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetCastFunction(*value.type(), out_type, options, &func));

  std::vector<Datum> result;
  RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func.get(), value, &result));

  *out = detail::WrapDatumsLike(value, result);
  return Status::OK();
}

Status Cast(FunctionContext* ctx, const Array& array,
            const std::shared_ptr<DataType>& out_type, const CastOptions& options,
            std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Cast(ctx, Datum(array.data()), out_type, options, &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
