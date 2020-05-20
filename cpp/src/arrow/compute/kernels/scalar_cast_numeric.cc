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

// Implementation of casting to integer or floating point types

namespace arrow {
namespace compute {

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

// ----------------------------------------------------------------------
// Possible integer truncation

struct IntegerDowncastNoOverflow {
  template <typename OutT, typename InT>
  OutT Call(KernelContext ctx, InT val) {
    constexpr InT kMax = SafeMaximum<OutT, InT>();
    constexpr InT kMin = SafeMinimum<OutT, InT>();
    if (ARROW_PREDICT_FALSE(val > kMax || val < kMin)) {
      ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
    }
    return static_cast<OutT>(val);
  }
};

struct StaticCast {
  ARROW_DISABLE_UBSAN("float-cast-overflow")
  template <typename OutT, typename InT>
  OutT Call(KernelContext ctx, InT val) {
    return static_cast<out_type>(val);
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_number_downcast<O, I>::value ||
                               is_integral_signed_to_unsigned<O, I>::value ||
                               is_integral_unsigned_to_signed<O, I>::value>> {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;
  out_type Call(KernelContext ctx, in_type val) {
    if (!options.allow_int_overflow) {
      // TODO
    } else {
      return static_cast<out_type>(val);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_t<is_float_truncate<O, I>::value>> {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  ARROW_DISABLE_UBSAN("float-cast-overflow")
  out_type Call(KernelContext ctx, in_type val) {
    if (options.allow_float_truncate) {
      // unsafe cast
      return static_cast<out_type>(*in_data++);
    } else {
      // safe cast
      auto out_value = static_cast<out_type>(*in_data);
      if (ARROW_PREDICT_FALSE(static_cast<in_type>(out_value) != *in_data)) {
        ctx->SetStatus(Status::Invalid("Floating point value truncated"));
      }
      return out_value;
    }
  }
};

template <typename O, typename I>
struct CastFunctor<
    O, I,
    enable_if_t<is_safe_numeric_cast<O, I>::value && !is_float_truncate<O, I>::value &&
                !is_number_downcast<O, I>::value>> {
  using in_type = typename I::c_type;
  using out_type = typename O::c_type;

  out_type Call(KernelContext ctx, in_type val) {
    // Due to various checks done via type-trait, the cast is safe and bear
    // no truncation.
    return static_cast<out_type>(*in_data++);
  }
};

// ----------------------------------------------------------------------
// Decimals

// Decimal to Integer

template <typename O>
struct CastFunctor<O, Decimal128Type, enable_if_t<is_integer_type<O>::value>> {
  using OUT = typename O::c_type;

  static OUT Call(KernelContext* ctx, Decimal128 val) {}

  void operator()(FunctionContext* ctx, const CastOptions& options,
                  const ArrayData& input, ArrayData* output) {
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

// ----------------------------------------------------------------------
// Boolean to other things

struct BooleanToNumber {
  template <typename OUT, typename ARG0>
  static OUT Call(KernelContext*, ARG0 val) {
    constexpr auto kOne = static_cast<OUT>(1);
    constexpr auto kZero = static_cast<OUT>(0);
    return val ? kOne : kZero;
  }
};

// ----------------------------------------------------------------------
// String to Number

template <typename ArrowType>
struct StringToNumber {
  template <typename OUT, typename ARG0>
  static OUT Call(KernelContext*, ARG0 val) {
    OUT result;
    if (ARROW_PREDICT_FALSE(
            !::arrow::internal::ParseValue<ArrowType>(val.data(), val.size(), &result))) {
      ctx->SetStatus(Status::Invalid("Failed to parse string: ", val));
    }
    return result;
  }
};

}  // namespace compute
}  // namespace arrow
