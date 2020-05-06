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

}  // namespace compute
}  // namespace arrow
