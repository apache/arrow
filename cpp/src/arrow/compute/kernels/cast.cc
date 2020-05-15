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
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/formatting.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/time.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"  // IWYU pragma: keep
#include "arrow/visitor_inline.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/util_internal.h"

#ifdef ARROW_EXTRA_ERROR_CONTEXT

#define FUNC_RETURN_NOT_OK(expr)                     \
  do {                                               \
    Status _st = (expr);                             \
    if (ARROW_PREDICT_FALSE(!_st.ok())) {            \
      _st.AddContextLine(__FILE__, __LINE__, #expr); \
      ctx->SetStatus(_st);                           \
      return;                                        \
    }                                                \
  } while (0)

#else

#define FUNC_RETURN_NOT_OK(expr)          \
  do {                                    \
    Status _st = (expr);                  \
    if (ARROW_PREDICT_FALSE(!_st.ok())) { \
      ctx->SetStatus(_st);                \
      return;                             \
    }                                     \
  } while (0)

#endif  // ARROW_EXTRA_ERROR_CONTEXT

namespace arrow {

using internal::checked_cast;
using internal::CopyBitmap;

namespace compute {

constexpr int64_t kMillisecondsInDay = 86400000;

Status CastNotImplemented(const DataType& in_type, const DataType& out_type) {
  return Status::NotImplemented("No cast implemented from ", in_type.ToString(), " to ",
                                out_type.ToString());
}

template <typename OutType, typename InType, typename Enable = void>
struct CastFunctor {};

// ----------------------------------------------------------------------
// Dictionary to null

template <>
struct CastFunctor<NullType, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    output->buffers = {nullptr};
    output->null_count = output->length;
  }
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

    if (input.length == 0) return;

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
                   enable_if_t<is_number_type<I>::value && !is_boolean_type<I>::value>> {
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
    O, I, enable_if_t<is_number_type<O>::value && is_number_type<I>::value>> {
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
    O, I, enable_if_t<is_integer_type<O>::value && is_integer_type<I>::value>> {
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
    O, I, enable_if_t<is_integer_type<O>::value && is_integer_type<I>::value>> {
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
#define RET_TYPE(TRAIT) enable_if_t<TRAIT<O, I>::value, typename I::c_type>

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
struct CastFunctor<O, I,
                   enable_if_t<is_number_downcast<O, I>::value ||
                               is_integral_signed_to_unsigned<O, I>::value ||
                               is_integral_unsigned_to_signed<O, I>::value>> {
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
    enable_if_t<(is_integer_type<O>::value && is_floating_type<I>::value) ||
                (is_integer_type<I>::value && is_floating_type<O>::value)>> {
  static constexpr bool value = true;
};

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_t<is_float_truncate<O, I>::value>> {
  ARROW_DISABLE_UBSAN("float-cast-overflow")
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
    O, I, enable_if_t<is_number_type<O>::value && is_number_type<I>::value>> {
  using O_T = typename O::c_type;
  using I_T = typename I::c_type;

  static constexpr bool value =
      (std::is_signed<O_T>::value == std::is_signed<I_T>::value) &&
      (std::is_integral<O_T>::value == std::is_integral<I_T>::value) &&
      (sizeof(O_T) >= sizeof(I_T)) && (!std::is_same<O, I>::value);
};

template <typename O, typename I>
struct CastFunctor<
    O, I,
    enable_if_t<is_safe_numeric_cast<O, I>::value && !is_float_truncate<O, I>::value &&
                !is_number_downcast<O, I>::value>> {
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
// Decimals

// Decimal to Integer

template <typename O>
struct CastFunctor<O, Decimal128Type, enable_if_t<is_integer_type<O>::value>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using out_type = typename O::c_type;
    const auto& in_type_inst = checked_cast<const Decimal128Type&>(*input.type);
    auto in_scale = in_type_inst.scale();

    auto out_data = output->GetMutableValues<out_type>(1);

    constexpr auto min_value = std::numeric_limits<out_type>::min();
    constexpr auto max_value = std::numeric_limits<out_type>::max();
    constexpr auto zero = out_type{};

    if (options.allow_decimal_truncate) {
      if (in_scale < 0) {
        // Unsafe upscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          *out_data = zero;
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            auto converted = dec_value.IncreaseScaleBy(-in_scale);
            if (!options.allow_int_overflow &&
                ARROW_PREDICT_FALSE(converted < min_value || converted > max_value)) {
              ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
            } else {
              *out_data = static_cast<out_type>(converted.low_bits());
            }
          }
          ++out_data;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      } else {
        // Unsafe downscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          *out_data = zero;
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            auto converted = dec_value.ReduceScaleBy(in_scale, false);
            if (!options.allow_int_overflow &&
                ARROW_PREDICT_FALSE(converted < min_value || converted > max_value)) {
              ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
            } else {
              *out_data = static_cast<out_type>(converted.low_bits());
            }
          }
          ++out_data;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      }
    } else {
      // Safe rescale
      auto convert_value = [&](util::optional<util::string_view> v) {
        *out_data = zero;
        if (v.has_value()) {
          auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
          auto result = dec_value.Rescale(in_scale, 0);
          if (ARROW_PREDICT_FALSE(!result.ok())) {
            ctx->SetStatus(result.status());
          } else {
            auto converted = *std::move(result);
            if (!options.allow_int_overflow &&
                ARROW_PREDICT_FALSE(converted < min_value || converted > max_value)) {
              ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
            } else {
              *out_data = static_cast<out_type>(converted.low_bits());
            }
          }
        }
        ++out_data;
      };
      VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
    }
  }
};

// Decimal to Decimal

template <>
struct CastFunctor<Decimal128Type, Decimal128Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type_inst = checked_cast<const Decimal128Type&>(*input.type);
    const auto& out_type_inst = checked_cast<const Decimal128Type&>(*output->type);
    auto in_scale = in_type_inst.scale();
    auto out_scale = out_type_inst.scale();

    auto out_data = output->GetMutableValues<uint8_t>(1);

    const auto write_zero = [](uint8_t* out_data) { memset(out_data, 0, 16); };

    if (options.allow_decimal_truncate) {
      if (in_scale < out_scale) {
        // Unsafe upscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            dec_value.IncreaseScaleBy(out_scale - in_scale).ToBytes(out_data);
          } else {
            write_zero(out_data);
          }
          out_data += 16;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      } else {
        // Unsafe downscale
        auto convert_value = [&](util::optional<util::string_view> v) {
          if (v.has_value()) {
            auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
            dec_value.ReduceScaleBy(in_scale - out_scale, false).ToBytes(out_data);
          } else {
            write_zero(out_data);
          }
          out_data += 16;
        };
        VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
      }
    } else {
      // Safe rescale
      auto convert_value = [&](util::optional<util::string_view> v) {
        if (v.has_value()) {
          auto dec_value = Decimal128(reinterpret_cast<const uint8_t*>(v->data()));
          auto result = dec_value.Rescale(in_scale, out_scale);
          if (ARROW_PREDICT_FALSE(!result.ok())) {
            ctx->SetStatus(result.status());
            write_zero(out_data);
          } else {
            (*std::move(result)).ToBytes(out_data);
          }
        } else {
          write_zero(out_data);
        }
        out_data += 16;
      };
      VisitArrayDataInline<Decimal128Type>(input, std::move(convert_value));
    }
  }
};

// ----------------------------------------------------------------------
// From one timestamp to another

template <typename in_type, typename out_type>
void ShiftTime(FunctionContext* ctx, const CastOptions& options,
               const util::DivideOrMultiply factor_op, const int64_t factor,
               const ArrayData& input, ArrayData* output) {
  const in_type* in_data = input.GetValues<in_type>(1);
  auto out_data = output->GetMutableValues<out_type>(1);

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
#define RAISE_OVERFLOW_CAST(VAL)                                                  \
  ctx->SetStatus(Status::Invalid("Casting from ", input.type->ToString(), " to ", \
                                 output->type->ToString(), " would result in ",   \
                                 "out of bounds timestamp: ", VAL));

      int64_t max_val = std::numeric_limits<int64_t>::max() / factor;
      int64_t min_val = std::numeric_limits<int64_t>::min() / factor;
      if (input.null_count != 0) {
        internal::BitmapReader bit_reader(input.buffers[0]->data(), input.offset,
                                          input.length);
        for (int64_t i = 0; i < input.length; i++) {
          if (bit_reader.IsSet() && (in_data[i] < min_val || in_data[i] > max_val)) {
            RAISE_OVERFLOW_CAST(in_data[i]);
            break;
          }
          out_data[i] = static_cast<out_type>(in_data[i] * factor);
          bit_reader.Next();
        }
      } else {
        for (int64_t i = 0; i < input.length; i++) {
          if (in_data[i] < min_val || in_data[i] > max_val) {
            RAISE_OVERFLOW_CAST(in_data[i]);
            break;
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

// <TimestampType, TimestampType> and <DurationType, DurationType>
template <typename O, typename I>
struct CastFunctor<
    O, I,
    enable_if_t<(is_timestamp_type<O>::value && is_timestamp_type<I>::value) ||
                (is_duration_type<O>::value && is_duration_type<I>::value)>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    // If units are the same, zero copy, otherwise convert
    const auto& in_type = checked_cast<const I&>(*input.type);
    const auto& out_type = checked_cast<const O&>(*output->type);

    if (in_type.unit() == out_type.unit()) {
      ZeroCopyData(input, output);
      return;
    }

    auto conversion = util::kTimestampConversionTable[static_cast<int>(in_type.unit())]
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
    ShiftTime<int64_t, int32_t>(ctx, options, util::DIVIDE, factor, input, output);
  }
};

template <>
struct CastFunctor<Date64Type, TimestampType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    const auto& in_type = checked_cast<const TimestampType&>(*input.type);

    auto conversion = util::kTimestampConversionTable[static_cast<int>(in_type.unit())]
                                                     [static_cast<int>(TimeUnit::MILLI)];
    ShiftTime<int64_t, int64_t>(ctx, options, conversion.first, conversion.second, input,
                                output);
    if (!ctx->status().ok()) {
      return;
    }

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
struct CastFunctor<O, I, enable_if_t<is_time_type<I>::value && is_time_type<O>::value>> {
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

    auto conversion = util::kTimestampConversionTable[static_cast<int>(in_type.unit())]
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
    ShiftTime<int32_t, int64_t>(ctx, options, util::MULTIPLY, kMillisecondsInDay, input,
                                output);
  }
};

template <>
struct CastFunctor<Date32Type, Date64Type> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ShiftTime<int64_t, int32_t>(ctx, options, util::DIVIDE, kMillisecondsInDay, input,
                                output);
  }
};

// ----------------------------------------------------------------------
// List to List

class CastKernelBase : public UnaryKernel {
 public:
  explicit CastKernelBase(std::shared_ptr<DataType> out_type)
      : out_type_(std::move(out_type)) {}

  std::shared_ptr<DataType> out_type() const override { return out_type_; }

  virtual Status Init(const DataType& in_type) { return Status::OK(); }

 protected:
  std::shared_ptr<DataType> out_type_;
};

bool NeedToPreallocate(const DataType& type) { return is_fixed_width(type.id()); }

Status InvokeWithAllocation(FunctionContext* ctx, UnaryKernel* func, const Datum& input,
                            Datum* out) {
  std::vector<Datum> result;
  if (NeedToPreallocate(*func->out_type())) {
    // Create wrapper that allocates output memory for primitive types
    detail::PrimitiveAllocatingUnaryKernel wrapper(func);
    RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, &wrapper, input, &result));
  } else {
    RETURN_NOT_OK(detail::InvokeUnaryArrayKernel(ctx, func, input, &result));
  }
  ARROW_RETURN_IF_ERROR(ctx);
  *out = detail::WrapDatumsLike(input, func->out_type(), result);
  return Status::OK();
}

template <typename TypeClass>
class ListCastKernel : public CastKernelBase {
 public:
  ListCastKernel(std::unique_ptr<UnaryKernel> child_caster,
                 std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), child_caster_(std::move(child_caster)) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    DCHECK_EQ(TypeClass::type_id, in_data.type->id());
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
    RETURN_NOT_OK(InvokeWithAllocation(ctx, child_caster_.get(), in_data.child_data[0],
                                       &casted_child));
    DCHECK_EQ(Datum::ARRAY, casted_child.kind());
    result->child_data.push_back(casted_child.array());
    return Status::OK();
  }

 private:
  std::unique_ptr<UnaryKernel> child_caster_;
};

// ----------------------------------------------------------------------
// Null to other things

class FromNullCastKernel : public CastKernelBase {
 public:
  explicit FromNullCastKernel(std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(Datum::ARRAY, input.kind());

    const ArrayData& in_data = *input.array();
    DCHECK_EQ(Type::NA, in_data.type->id());
    auto length = in_data.length;

    // A ArrayData may be preallocated for the output (see InvokeUnaryArrayKernel),
    // however, it doesn't have any actual data, so throw it away and start anew.
    std::unique_ptr<ArrayBuilder> builder;
    RETURN_NOT_OK(MakeBuilder(ctx->memory_pool(), out_type_, &builder));
    NullBuilderVisitor visitor = {length, builder.get()};
    RETURN_NOT_OK(VisitTypeInline(*out_type_, &visitor));

    std::shared_ptr<Array> out_array;
    RETURN_NOT_OK(visitor.builder_->Finish(&out_array));
    out->value = out_array->data();
    return Status::OK();
  }

  struct NullBuilderVisitor {
    // Generic implementation
    Status Visit(const DataType& type) { return builder_->AppendNulls(length_); }

    Status Visit(const StructType& type) {
      RETURN_NOT_OK(builder_->AppendNulls(length_));
      auto& struct_builder = checked_cast<StructBuilder&>(*builder_);
      // Append nulls to all child builders too
      for (int i = 0; i < struct_builder.num_fields(); ++i) {
        NullBuilderVisitor visitor = {length_, struct_builder.field_builder(i)};
        RETURN_NOT_OK(VisitTypeInline(*type.field(i)->type(), &visitor));
      }
      return Status::OK();
    }

    Status Visit(const DictionaryType& type) {
      // XXX (ARROW-5215): Cannot implement this easily, as DictionaryBuilder
      // disregards the index type given in the dictionary type, and instead
      // chooses the smallest possible index type.
      return CastNotImplemented(*null(), type);
    }

    Status Visit(const UnionType& type) { return CastNotImplemented(*null(), type); }

    int64_t length_;
    ArrayBuilder* builder_;
  };
};

// ----------------------------------------------------------------------
// Dictionary to other things

template <typename T, typename IndexType, typename Enable = void>
struct FromDictVisitor {};

// Visitor for Dict<FixedSizeBinaryType>
template <typename T, typename IndexType>
struct FromDictVisitor<T, IndexType, enable_if_fixed_size_binary<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : dictionary_(dictionary),
        byte_width_(dictionary.byte_width()),
        out_(output->buffers[1]->mutable_data() + byte_width_ * output->offset) {}

  Status Init() { return Status::OK(); }

  Status VisitNull() {
    memset(out_, 0, byte_width_);
    out_ += byte_width_;
    return Status::OK();
  }

  Status VisitValue(typename IndexType::c_type dict_index) {
    const uint8_t* value = dictionary_.Value(dict_index);
    memcpy(out_, value, byte_width_);
    out_ += byte_width_;
    return Status::OK();
  }

  Status Finish() { return Status::OK(); }

  const ArrayType& dictionary_;
  int32_t byte_width_;
  uint8_t* out_;
};

// Visitor for Dict<BinaryType>
template <typename T, typename IndexType>
struct FromDictVisitor<T, IndexType, enable_if_base_binary<T>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : ctx_(ctx), dictionary_(dictionary), output_(output) {}

  Status Init() {
    RETURN_NOT_OK(MakeBuilder(ctx_->memory_pool(), output_->type, &builder_));
    binary_builder_ = checked_cast<BinaryBuilder*>(builder_.get());
    return Status::OK();
  }

  Status VisitNull() { return binary_builder_->AppendNull(); }

  Status VisitValue(typename IndexType::c_type dict_index) {
    return binary_builder_->Append(dictionary_.GetView(dict_index));
  }

  Status Finish() {
    std::shared_ptr<Array> plain_array;
    RETURN_NOT_OK(binary_builder_->Finish(&plain_array));
    // Copy all buffer except the valid bitmap
    DCHECK_EQ(output_->buffers.size(), 1);
    for (size_t i = 1; i < plain_array->data()->buffers.size(); i++) {
      output_->buffers.push_back(plain_array->data()->buffers[i]);
    }
    return Status::OK();
  }

  FunctionContext* ctx_;
  const ArrayType& dictionary_;
  ArrayData* output_;
  std::unique_ptr<ArrayBuilder> builder_;
  BinaryBuilder* binary_builder_;
};

// Visitor for Dict<NumericType | TemporalType>
template <typename T, typename IndexType>
struct FromDictVisitor<
    T, IndexType, enable_if_t<is_number_type<T>::value || is_temporal_type<T>::value>> {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  using value_type = typename T::c_type;

  FromDictVisitor(FunctionContext* ctx, const ArrayType& dictionary, ArrayData* output)
      : dictionary_(dictionary), out_(output->GetMutableValues<value_type>(1)) {}

  Status Init() { return Status::OK(); }

  Status VisitNull() {
    *out_++ = value_type{};  // Zero-initialize
    return Status::OK();
  }

  Status VisitValue(typename IndexType::c_type dict_index) {
    *out_++ = dictionary_.Value(dict_index);
    return Status::OK();
  }

  Status Finish() { return Status::OK(); }

  const ArrayType& dictionary_;
  value_type* out_;
};

template <typename T>
struct FromDictUnpackHelper {
  using ArrayType = typename TypeTraits<T>::ArrayType;

  template <typename IndexType>
  Status Unpack(FunctionContext* ctx, const ArrayData& indices,
                const ArrayType& dictionary, ArrayData* output) {
    FromDictVisitor<T, IndexType> visitor{ctx, dictionary, output};
    RETURN_NOT_OK(visitor.Init());
    RETURN_NOT_OK(ArrayDataVisitor<IndexType>::Visit(indices, &visitor));
    return visitor.Finish();
  }
};

// Dispatch dictionary casts to UnpackHelper
template <typename T>
struct CastFunctor<T, DictionaryType> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using ArrayType = typename TypeTraits<T>::ArrayType;

    const DictionaryType& type = checked_cast<const DictionaryType&>(*input.type);
    const Array& dictionary = *input.dictionary;
    const DataType& values_type = *dictionary.type();

    // Check if values and output type match
    DCHECK(values_type.Equals(*output->type))
        << "Dictionary type: " << values_type << " target type: " << (*output->type);

    FromDictUnpackHelper<T> unpack_helper;
    switch (type.index_type()->id()) {
      case Type::INT8:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int8Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT16:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int16Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT32:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int32Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      case Type::INT64:
        FUNC_RETURN_NOT_OK(unpack_helper.template Unpack<Int64Type>(
            ctx, input, static_cast<const ArrayType&>(dictionary), output));
        break;
      default:
        ctx->SetStatus(
            Status::TypeError("Invalid index type: ", type.index_type()->ToString()));
        return;
    }
  }
};

// ----------------------------------------------------------------------
// String to Number

template <typename I, typename O>
struct CastFunctor<
    O, I, enable_if_t<is_string_like_type<I>::value && is_number_type<O>::value>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using out_type = typename O::c_type;

    typename TypeTraits<I>::ArrayType input_array(input.Copy());
    auto out_data = output->GetMutableValues<out_type>(1);
    for (int64_t i = 0; i < input.length; ++i, ++out_data) {
      if (input_array.IsNull(i)) {
        continue;
      }

      auto str = input_array.GetView(i);
      if (!internal::ParseValue<O>(str.data(), str.length(), out_data)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '", str, "' into ",
                                       output->type->ToString()));
        return;
      }
    }
  }
};

// ----------------------------------------------------------------------
// String to Boolean

template <typename I>
struct CastFunctor<BooleanType, I, enable_if_t<is_string_like_type<I>::value>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    typename TypeTraits<I>::ArrayType input_array(input.Copy());
    internal::FirstTimeBitmapWriter writer(output->buffers[1]->mutable_data(),
                                           output->offset, input.length);

    for (int64_t i = 0; i < input.length; ++i) {
      if (input_array.IsNull(i)) {
        writer.Next();
        continue;
      }

      bool value;
      auto str = input_array.GetView(i);
      if (!internal::ParseValue<BooleanType>(str.data(), str.length(), &value)) {
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

template <typename I>
struct CastFunctor<TimestampType, I, enable_if_t<is_string_like_type<I>::value>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    using out_type = TimestampType::c_type;

    typename TypeTraits<I>::ArrayType input_array(input.Copy());
    auto out_data = output->GetMutableValues<out_type>(1);

    const TimeUnit::type unit = checked_cast<const TimestampType&>(*output->type).unit();

    for (int64_t i = 0; i < input.length; ++i, ++out_data) {
      if (input_array.IsNull(i)) {
        continue;
      }
      const auto str = input_array.GetView(i);
      if (!internal::ParseTimestampISO8601(str.data(), str.length(), unit, out_data)) {
        ctx->SetStatus(Status::Invalid("Failed to cast String '", str, "' into ",
                                       output->type->ToString()));
        return;
      }
    }
  }
};

// ----------------------------------------------------------------------
// Number / Boolean to String

template <typename I, typename O>
struct CastFunctor<O, I,
                   enable_if_t<is_string_like_type<O>::value &&
                               (is_number_type<I>::value || is_boolean_type<I>::value)>> {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    ctx->SetStatus(Convert(ctx, options, input, output));
  }

  Status Convert(FunctionContext* ctx, const CastOptions& options, const ArrayData& input,
                 ArrayData* output) {
    using value_type = typename TypeTraits<I>::CType;
    using BuilderType = typename TypeTraits<O>::BuilderType;
    using FormatterType = typename internal::StringFormatter<I>;

    FormatterType formatter(input.type);
    BuilderType builder(input.type, ctx->memory_pool());

    auto convert_value = [&](util::optional<value_type> v) {
      if (v.has_value()) {
        return formatter(*v, [&](util::string_view v) { return builder.Append(v); });
      } else {
        return builder.AppendNull();
      }
    };
    RETURN_NOT_OK(VisitArrayDataInline<I>(input, std::move(convert_value)));

    std::shared_ptr<Array> output_array;
    RETURN_NOT_OK(builder.Finish(&output_array));
    *output = std::move(*output_array->data());
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Binary to String
//

#if defined(_MSC_VER)
// Silence warning: """'visitor': unreferenced local variable"""
#pragma warning(push)
#pragma warning(disable : 4101)
#endif

template <typename I, typename O>
struct BinaryToStringSameWidthCastFunctor {
  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
    if (!options.allow_invalid_utf8) {
      util::InitializeUTF8();

      ArrayDataVisitor<I> visitor;
      Status st = visitor.Visit(input, this);
      if (!st.ok()) {
        ctx->SetStatus(st);
        return;
      }
    }
    ZeroCopyData(input, output);
  }

  Status VisitNull() { return Status::OK(); }

  Status VisitValue(util::string_view str) {
    if (ARROW_PREDICT_FALSE(!arrow::util::ValidateUTF8(str))) {
      return Status::Invalid("Invalid UTF8 payload");
    }
    return Status::OK();
  }
};

template <>
struct CastFunctor<StringType, BinaryType>
    : public BinaryToStringSameWidthCastFunctor<StringType, BinaryType> {};

template <>
struct CastFunctor<LargeStringType, LargeBinaryType>
    : public BinaryToStringSameWidthCastFunctor<LargeStringType, LargeBinaryType> {};

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

// ----------------------------------------------------------------------

typedef std::function<void(FunctionContext*, const CastOptions& options, const ArrayData&,
                           ArrayData*)>
    CastFunction;

class IdentityCast : public CastKernelBase {
 public:
  using CastKernelBase::CastKernelBase;

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    out->value = input.array()->Copy();
    return Status::OK();
  }
};

class ZeroCopyCast : public CastKernelBase {
 public:
  using CastKernelBase::CastKernelBase;

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    auto result = input.array()->Copy();
    result->type = out_type_;
    out->value = result;
    return Status::OK();
  }
};

class ExtensionCastKernel : public CastKernelBase {
 public:
  static Status Make(const DataType& in_type, std::shared_ptr<DataType> out_type,
                     const CastOptions& options,
                     std::unique_ptr<CastKernelBase>* kernel) {
    const auto storage_type = checked_cast<const ExtensionType&>(in_type).storage_type();

    std::unique_ptr<UnaryKernel> storage_caster;
    RETURN_NOT_OK(GetCastFunction(*storage_type, out_type, options, &storage_caster));
    kernel->reset(
        new ExtensionCastKernel(std::move(storage_caster), std::move(out_type)));

    return Status::OK();
  }

  Status Init(const DataType& in_type) override {
    auto& type = checked_cast<const ExtensionType&>(in_type);
    storage_type_ = type.storage_type();
    extension_name_ = type.extension_name();
    return Status::OK();
  }

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);

    // validate: type is the same as the type the kernel was constructed with
    const auto& input_type = checked_cast<const ExtensionType&>(*input.type());
    if (input_type.extension_name() != extension_name_) {
      return Status::TypeError(
          "The cast kernel was constructed to cast from the extension type named '",
          extension_name_, "' but input has extension type named '",
          input_type.extension_name(), "'");
    }
    if (!input_type.storage_type()->Equals(storage_type_)) {
      return Status::TypeError("The cast kernel was constructed with a storage type: ",
                               storage_type_->ToString(),
                               ", but it is called with a different storage type:",
                               input_type.storage_type()->ToString());
    }

    // construct an ArrayData object with the underlying storage type
    auto new_input = input.array()->Copy();
    new_input->type = storage_type_;
    return InvokeWithAllocation(ctx, storage_caster_.get(), new_input, out);
  }

 protected:
  ExtensionCastKernel(std::unique_ptr<UnaryKernel> storage_caster,
                      std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), storage_caster_(std::move(storage_caster)) {}

  std::string extension_name_;
  std::shared_ptr<DataType> storage_type_;
  std::unique_ptr<UnaryKernel> storage_caster_;
};

class CastKernel : public CastKernelBase {
 public:
  CastKernel(const CastOptions& options, const CastFunction& func,
             std::shared_ptr<DataType> out_type)
      : CastKernelBase(std::move(out_type)), options_(options), func_(func) {}

  Status Call(FunctionContext* ctx, const Datum& input, Datum* out) override {
    DCHECK_EQ(input.kind(), Datum::ARRAY);
    DCHECK_EQ(out->kind(), Datum::ARRAY);

    const ArrayData& in_data = *input.array();
    ArrayData* result = out->array().get();

    RETURN_NOT_OK(detail::PropagateNulls(ctx, in_data, result));

    func_(ctx, options_, in_data, result);
    ARROW_RETURN_IF_ERROR(ctx);
    return Status::OK();
  }

 private:
  CastOptions options_;
  CastFunction func_;
};

class DictionaryCastKernel : public CastKernel {
 public:
  using CastKernel::CastKernel;

  Status Init(const DataType& in_type) override {
    const auto value_type = checked_cast<const DictionaryType&>(in_type).value_type();
    if (!out_type_->Equals(value_type)) {
      return CastNotImplemented(in_type, *out_type_);
    }
    return Status::OK();
  }
};

#define CAST_CASE(InType, OutType)                                                      \
  case OutType::type_id:                                                                \
    func = [](FunctionContext* ctx, const CastOptions& options, const ArrayData& input, \
              ArrayData* out) {                                                         \
      CastFunctor<OutType, InType> func;                                                \
      func(ctx, options, input, out);                                                   \
    };                                                                                  \
    break;

#define GET_CAST_FUNCTION(CASE_GENERATOR, InType, KernelType)           \
  static std::unique_ptr<CastKernelBase> Get##InType##CastFunc(         \
      std::shared_ptr<DataType> out_type, const CastOptions& options) { \
    CastFunction func;                                                  \
    switch (out_type->id()) {                                           \
      CASE_GENERATOR(CAST_CASE);                                        \
      default:                                                          \
        break;                                                          \
    }                                                                   \
    if (func != nullptr) {                                              \
      return std::unique_ptr<CastKernelBase>(                           \
          new KernelType(options, func, std::move(out_type)));          \
    }                                                                   \
    return nullptr;                                                     \
  }

#include "generated/cast_codegen_internal.h"  // NOLINT

GET_CAST_FUNCTION(BOOLEAN_CASES, BooleanType, CastKernel)
GET_CAST_FUNCTION(UINT8_CASES, UInt8Type, CastKernel)
GET_CAST_FUNCTION(INT8_CASES, Int8Type, CastKernel)
GET_CAST_FUNCTION(UINT16_CASES, UInt16Type, CastKernel)
GET_CAST_FUNCTION(INT16_CASES, Int16Type, CastKernel)
GET_CAST_FUNCTION(UINT32_CASES, UInt32Type, CastKernel)
GET_CAST_FUNCTION(INT32_CASES, Int32Type, CastKernel)
GET_CAST_FUNCTION(UINT64_CASES, UInt64Type, CastKernel)
GET_CAST_FUNCTION(INT64_CASES, Int64Type, CastKernel)
GET_CAST_FUNCTION(FLOAT_CASES, FloatType, CastKernel)
GET_CAST_FUNCTION(DOUBLE_CASES, DoubleType, CastKernel)
GET_CAST_FUNCTION(DECIMAL128_CASES, Decimal128Type, CastKernel)
GET_CAST_FUNCTION(DATE32_CASES, Date32Type, CastKernel)
GET_CAST_FUNCTION(DATE64_CASES, Date64Type, CastKernel)
GET_CAST_FUNCTION(TIME32_CASES, Time32Type, CastKernel)
GET_CAST_FUNCTION(TIME64_CASES, Time64Type, CastKernel)
GET_CAST_FUNCTION(TIMESTAMP_CASES, TimestampType, CastKernel)
GET_CAST_FUNCTION(DURATION_CASES, DurationType, CastKernel)
GET_CAST_FUNCTION(BINARY_CASES, BinaryType, CastKernel)
GET_CAST_FUNCTION(STRING_CASES, StringType, CastKernel)
GET_CAST_FUNCTION(LARGEBINARY_CASES, LargeBinaryType, CastKernel)
GET_CAST_FUNCTION(LARGESTRING_CASES, LargeStringType, CastKernel)
GET_CAST_FUNCTION(DICTIONARY_CASES, DictionaryType, DictionaryCastKernel)

#define CAST_FUNCTION_CASE(InType)                          \
  case InType::type_id:                                     \
    cast_kernel = Get##InType##CastFunc(out_type, options); \
    break

namespace {

template <typename TypeClass>
Status GetListCastFunc(const DataType& in_type, std::shared_ptr<DataType> out_type,
                       const CastOptions& options,
                       std::unique_ptr<CastKernelBase>* kernel) {
  if (out_type->id() != TypeClass::type_id) {
    return Status::Invalid("Cannot cast from ", in_type.ToString(), " to ",
                           out_type->ToString());
  }
  const DataType& in_value_type = *checked_cast<const TypeClass&>(in_type).value_type();
  std::shared_ptr<DataType> out_value_type =
      checked_cast<const TypeClass&>(*out_type).value_type();
  std::unique_ptr<UnaryKernel> child_caster;
  RETURN_NOT_OK(GetCastFunction(in_value_type, out_value_type, options, &child_caster));
  *kernel = std::unique_ptr<CastKernelBase>(
      new ListCastKernel<TypeClass>(std::move(child_caster), std::move(out_type)));
  return Status::OK();
}

}  // namespace

inline bool IsZeroCopyCast(Type::type in_type, Type::type out_type) {
  switch (in_type) {
    case Type::INT32:
      return (out_type == Type::DATE32) || (out_type == Type::TIME32);
    case Type::INT64:
      return ((out_type == Type::DATE64) || (out_type == Type::TIME64) ||
              (out_type == Type::TIMESTAMP) || (out_type == Type::DURATION));
    case Type::DATE32:
    case Type::TIME32:
      return out_type == Type::INT32;
    case Type::DATE64:
    case Type::TIME64:
    case Type::TIMESTAMP:
    case Type::DURATION:
      return out_type == Type::INT64;
    default:
      break;
  }
  return false;
}

Status GetCastFunction(const DataType& in_type, std::shared_ptr<DataType> out_type,
                       const CastOptions& options, std::unique_ptr<UnaryKernel>* kernel) {
  if (in_type.Equals(out_type)) {
    kernel->reset(new IdentityCast(std::move(out_type)));
    return Status::OK();
  }

  if (IsZeroCopyCast(in_type.id(), out_type->id())) {
    kernel->reset(new ZeroCopyCast(std::move(out_type)));
    return Status::OK();
  }

  std::unique_ptr<CastKernelBase> cast_kernel;
  switch (in_type.id()) {
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
    CAST_FUNCTION_CASE(Decimal128Type);
    CAST_FUNCTION_CASE(Date32Type);
    CAST_FUNCTION_CASE(Date64Type);
    CAST_FUNCTION_CASE(Time32Type);
    CAST_FUNCTION_CASE(Time64Type);
    CAST_FUNCTION_CASE(TimestampType);
    CAST_FUNCTION_CASE(DurationType);
    CAST_FUNCTION_CASE(BinaryType);
    CAST_FUNCTION_CASE(StringType);
    CAST_FUNCTION_CASE(LargeBinaryType);
    CAST_FUNCTION_CASE(LargeStringType);
    CAST_FUNCTION_CASE(DictionaryType);
    case Type::NA:
      cast_kernel.reset(new FromNullCastKernel(out_type));
      break;
    case Type::LIST:
      RETURN_NOT_OK(GetListCastFunc<ListType>(in_type, out_type, options, &cast_kernel));
      break;
    case Type::LARGE_LIST:
      RETURN_NOT_OK(
          GetListCastFunc<LargeListType>(in_type, out_type, options, &cast_kernel));
      break;
    case Type::EXTENSION:
      RETURN_NOT_OK(
          ExtensionCastKernel::Make(std::move(in_type), out_type, options, &cast_kernel));
      break;
    default:
      break;
  }
  if (cast_kernel == nullptr) {
    return CastNotImplemented(in_type, *out_type);
  }
  Status st = cast_kernel->Init(in_type);
  if (st.ok()) {
    *kernel = std::move(cast_kernel);
  }
  return st;
}

Status Cast(FunctionContext* ctx, const Datum& value, std::shared_ptr<DataType> out_type,
            const CastOptions& options, Datum* out) {
  const DataType& in_type = *value.type();

  // Dynamic dispatch to obtain right cast function
  std::unique_ptr<UnaryKernel> func;
  RETURN_NOT_OK(GetCastFunction(in_type, std::move(out_type), options, &func));
  return InvokeWithAllocation(ctx, func.get(), value, out);
}

Status Cast(FunctionContext* ctx, const Array& array, std::shared_ptr<DataType> out_type,
            const CastOptions& options, std::shared_ptr<Array>* out) {
  Datum datum_out;
  RETURN_NOT_OK(Cast(ctx, Datum(array.data()), std::move(out_type), options, &datum_out));
  DCHECK_EQ(Datum::ARRAY, datum_out.kind());
  *out = MakeArray(datum_out.array());
  return Status::OK();
}

}  // namespace compute
}  // namespace arrow
