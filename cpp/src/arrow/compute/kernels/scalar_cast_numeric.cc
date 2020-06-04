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

// Implementation of casting to integer, floating point, or decimal types

#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::ParseValue;

namespace compute {
namespace internal {

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
// Integer to other number types

template <typename O, typename I>
struct IntegerDowncastNoOverflow {
  using InT = typename I::c_type;
  static constexpr InT kMax = SafeMaximum<O, I>();
  static constexpr InT kMin = SafeMinimum<O, I>();

  template <typename OutT, typename InT>
  OutT Call(KernelContext* ctx, InT val) const {
    if (ARROW_PREDICT_FALSE(val > kMax || val < kMin)) {
      ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
    }
    return static_cast<OutT>(val);
  }
};

struct StaticCast {
  template <typename OutT, typename InT>
  ARROW_DISABLE_UBSAN("float-cast-overflow")
  static OutT Call(KernelContext*, InT val) {
    return static_cast<OutT>(val);
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_number_downcast<O, I>::value ||
                               is_integral_signed_to_unsigned<O, I>::value ||
                               is_integral_unsigned_to_signed<O, I>::value>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;
    if (!options.allow_int_overflow) {
      codegen::ScalarUnaryNotNull<O, I, IntegerDowncastNoOverflow<O, I>>::Exec(ctx, batch,
                                                                               out);
    } else {
      codegen::ScalarUnary<O, I, StaticCast>::Exec(ctx, batch, out);
    }
  }
};

// ----------------------------------------------------------------------
// Float to other number types

struct FloatToIntegerNoTruncate {
  template <typename OutT, typename InT>
  ARROW_DISABLE_UBSAN("float-cast-overflow")
  OutT Call(KernelContext* ctx, InT val) const {
    auto out_value = static_cast<OutT>(val);
    if (ARROW_PREDICT_FALSE(static_cast<InT>(out_value) != val)) {
      ctx->SetStatus(Status::Invalid("Floating point value truncated"));
    }
    return out_value;
  }
};

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_t<is_float_truncate<O, I>::value>> {
  ARROW_DISABLE_UBSAN("float-cast-overflow")
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;
    if (options.allow_float_truncate) {
      codegen::ScalarUnary<O, I, StaticCast>::Exec(ctx, batch, out);
    } else {
      codegen::ScalarUnaryNotNull<O, I, FloatToIntegerNoTruncate>::Exec(ctx, batch, out);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<
    O, I,
    enable_if_t<is_safe_numeric_cast<O, I>::value && !is_float_truncate<O, I>::value &&
                !is_number_downcast<O, I>::value>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    // Due to various checks done via type-trait, the cast is safe and bear
    // no truncation.
    codegen::ScalarUnary<O, I, StaticCast>::Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Boolean to number

struct BooleanToNumber {
  template <typename OUT, typename ARG0>
  static OUT Call(KernelContext*, ARG0 val) {
    constexpr auto kOne = static_cast<OUT>(1);
    constexpr auto kZero = static_cast<OUT>(0);
    return val ? kOne : kZero;
  }
};

template <typename O>
struct CastFunctor<O, BooleanType, enable_if_number<O>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    codegen::ScalarUnary<O, BooleanType, BooleanToNumber>::Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// String to number

template <typename OutType>
struct ParseString {
  template <typename OUT, typename ARG0>
  OUT Call(KernelContext* ctx, ARG0 val) const {
    OUT result = OUT(0);
    if (ARROW_PREDICT_FALSE(!ParseValue<OutType>(val.data(), val.size(), &result))) {
      ctx->SetStatus(Status::Invalid("Failed to parse string: ", val));
    }
    return result;
  }
};

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_base_binary<I>> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    codegen::ScalarUnaryNotNull<O, I, ParseString<O>>::Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Decimal to integer

struct DecimalToIntegerMixin {
  template <typename OUT>
  OUT ToInteger(KernelContext* ctx, const Decimal128& val) const {
    constexpr auto min_value = std::numeric_limits<OUT>::min();
    constexpr auto max_value = std::numeric_limits<OUT>::max();

    if (!allow_int_overflow_ && ARROW_PREDICT_FALSE(val < min_value || val > max_value)) {
      ctx->SetStatus(Status::Invalid("Integer value out of bounds"));
      return OUT{};  // Zero
    } else {
      return static_cast<OUT>(val.low_bits());
    }
  }

  DecimalToIntegerMixin(int32_t in_scale, bool allow_int_overflow)
      : in_scale_(in_scale), allow_int_overflow_(allow_int_overflow) {}

  int32_t in_scale_;
  bool allow_int_overflow_;
};

struct UnsafeUpscaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OUT, typename ARG0>
  OUT Call(KernelContext* ctx, Decimal128 val) const {
    return ToInteger<OUT>(ctx, val.IncreaseScaleBy(-in_scale_));
  }
};

struct UnsafeDownscaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OUT, typename ARG0>
  OUT Call(KernelContext* ctx, Decimal128 val) const {
    return ToInteger<OUT>(ctx, val.ReduceScaleBy(in_scale_, false));
  }
};

struct SafeRescaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OUT, typename ARG0>
  OUT Call(KernelContext* ctx, Decimal128 val) const {
    auto result = val.Rescale(in_scale_, 0);
    if (ARROW_PREDICT_FALSE(!result.ok())) {
      ctx->SetStatus(result.status());
      return OUT{};  // Zero
    } else {
      return ToInteger<OUT>(ctx, *result);
    }
  }
};

template <typename O>
struct CastFunctor<O, Decimal128Type, enable_if_t<is_integer_type<O>::value>> {
  using out_type = typename O::c_type;

  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;

    const ArrayData& input = *batch[0].array();
    const auto& in_type_inst = checked_cast<const Decimal128Type&>(*input.type);
    const auto in_scale = in_type_inst.scale();

    if (options.allow_decimal_truncate) {
      if (in_scale < 0) {
        // Unsafe upscale
        codegen::ScalarUnaryNotNullStateful<O, Decimal128Type,
                                            UnsafeUpscaleDecimalToInteger>
            kernel(UnsafeUpscaleDecimalToInteger{in_scale, options.allow_int_overflow});
        return kernel.Exec(ctx, batch, out);
      } else {
        // Unsafe downscale
        codegen::ScalarUnaryNotNullStateful<O, Decimal128Type,
                                            UnsafeDownscaleDecimalToInteger>
            kernel(UnsafeDownscaleDecimalToInteger{in_scale, options.allow_int_overflow});
        return kernel.Exec(ctx, batch, out);
      }
    } else {
      // Safe rescale
      codegen::ScalarUnaryNotNullStateful<O, Decimal128Type, SafeRescaleDecimalToInteger>
          kernel(SafeRescaleDecimalToInteger{in_scale, options.allow_int_overflow});
      return kernel.Exec(ctx, batch, out);
    }
  }
};

// ----------------------------------------------------------------------
// Decimal to decimal

struct UnsafeUpscaleDecimal {
  template <typename... Unused>
  Decimal128 Call(KernelContext* ctx, Decimal128 val) const {
    return val.IncreaseScaleBy(out_scale_ - in_scale_);
  }

  int32_t out_scale_, in_scale_;
};

struct UnsafeDownscaleDecimal {
  template <typename... Unused>
  Decimal128 Call(KernelContext* ctx, Decimal128 val) const {
    return val.ReduceScaleBy(in_scale_ - out_scale_, false);
  }

  int32_t out_scale_, in_scale_;
};

struct SafeRescaleDecimal {
  template <typename... Unused>
  Decimal128 Call(KernelContext* ctx, Decimal128 val) const {
    auto result = val.Rescale(in_scale_, out_scale_);
    if (ARROW_PREDICT_FALSE(!result.ok())) {
      ctx->SetStatus(result.status());
      return Decimal128();  // Zero
    } else if (ARROW_PREDICT_FALSE(!(*result).FitsInPrecision(out_precision_))) {
      ctx->SetStatus(Status::Invalid("Decimal value does not fit in precision"));
      return Decimal128();  // Zero
    } else {
      return *std::move(result);
    }
  }

  int32_t out_scale_, out_precision_, in_scale_;
};

template <>
struct CastFunctor<Decimal128Type, Decimal128Type> {
  static void Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();

    const auto& in_type_inst = checked_cast<const Decimal128Type&>(*input.type);
    const auto& out_type_inst = checked_cast<const Decimal128Type&>(*output->type);
    const auto in_scale = in_type_inst.scale();
    const auto out_scale = out_type_inst.scale();
    const auto out_precision = out_type_inst.precision();

    if (options.allow_decimal_truncate) {
      if (in_scale < out_scale) {
        // Unsafe upscale
        codegen::ScalarUnaryNotNullStateful<Decimal128Type, Decimal128Type,
                                            UnsafeUpscaleDecimal>
            kernel(UnsafeUpscaleDecimal{out_scale, in_scale});
        return kernel.Exec(ctx, batch, out);
      } else {
        // Unsafe downscale
        codegen::ScalarUnaryNotNullStateful<Decimal128Type, Decimal128Type,
                                            UnsafeDownscaleDecimal>
            kernel(UnsafeDownscaleDecimal{out_scale, in_scale});
        return kernel.Exec(ctx, batch, out);
      }
    } else {
      // Safe rescale
      codegen::ScalarUnaryNotNullStateful<Decimal128Type, Decimal128Type,
                                          SafeRescaleDecimal>
          kernel(SafeRescaleDecimal{out_scale, out_precision, in_scale});
      return kernel.Exec(ctx, batch, out);
    }
  }
};

template <typename OutType>
void AddPrimitiveNumberCasts(const std::shared_ptr<DataType>& out_ty,
                             CastFunction* func) {
  AddCommonCasts<OutType>(out_ty, func);

  // Cast from boolean to number
  DCHECK_OK(func->AddKernel(Type::BOOL, {boolean()}, out_ty,
                            CastFunctor<OutType, BooleanType>::Exec));

  // Cast from other numbers
  for (const std::shared_ptr<DataType>& in_ty : NumericTypes()) {
    auto exec = codegen::Numeric<CastFunctor, OutType>(*in_ty);
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, exec));
  }

  // Cast from other strings
  for (const std::shared_ptr<DataType>& in_ty : BaseBinaryTypes()) {
    auto exec = codegen::BaseBinary<CastFunctor, OutType>(*in_ty);
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, exec));
  }
}

template <typename OutType>
std::shared_ptr<CastFunction> GetCastToInteger(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), OutType::type_id);
  auto out_ty = TypeTraits<OutType>::type_singleton();

  // From other numbers to integer
  AddPrimitiveNumberCasts<OutType>(out_ty, func.get());

  // From decimal to integer
  DCHECK_OK(func->AddKernel(Type::DECIMAL, {InputType::Array(Type::DECIMAL)}, out_ty,
                            CastFunctor<OutType, Decimal128Type>::Exec));
  return func;
}

template <typename OutType>
std::shared_ptr<CastFunction> GetCastToFloating(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), OutType::type_id);
  auto out_ty = TypeTraits<OutType>::type_singleton();

  // From other numbers to integer
  AddPrimitiveNumberCasts<OutType>(out_ty, func.get());
  return func;
}

std::shared_ptr<CastFunction> GetCastToDecimal() {
  OutputType sig_out_ty(ResolveOutputFromOptions);

  // Cast to decimal
  auto func = std::make_shared<CastFunction>("cast_decimal", Type::DECIMAL);
  AddCommonCasts<Decimal128Type>(sig_out_ty, func.get());

  auto exec = CastFunctor<Decimal128Type, Decimal128Type>::Exec;
  // We resolve the output type of this kernel from the CastOptions
  DCHECK_OK(func->AddKernel(Type::DECIMAL, {InputType::Array(Type::DECIMAL)}, sig_out_ty,
                            exec));
  return func;
}

std::vector<std::shared_ptr<CastFunction>> GetNumericCasts() {
  std::vector<std::shared_ptr<CastFunction>> functions;

  // Make a cast to null that does not do much. Not sure why we need to be able
  // to cast from dict<null> -> null but there are unit tests for it
  auto cast_null = std::make_shared<CastFunction>("cast_null", Type::NA);
  DCHECK_OK(cast_null->AddKernel(Type::DICTIONARY, {InputType::Array(Type::DICTIONARY)},
                                 null(), OutputAllNull));
  functions.push_back(cast_null);

  functions.push_back(GetCastToInteger<Int8Type>("cast_int8"));
  functions.push_back(GetCastToInteger<Int16Type>("cast_int16"));

  auto cast_int32 = GetCastToInteger<Int32Type>("cast_int32");
  // Convert DATE32 or TIME32 to INT32 zero copy
  AddZeroCopyCast(Type::DATE32, date32(), int32(), cast_int32.get());
  AddZeroCopyCast(Type::TIME32, InputType(Type::TIME32), int32(), cast_int32.get());
  functions.push_back(cast_int32);

  auto cast_int64 = GetCastToInteger<Int64Type>("cast_int64");
  // Convert DATE64, DURATION, TIMESTAMP, TIME64 to INT64 zero copy
  AddZeroCopyCast(Type::DATE64, InputType(Type::DATE64), int64(), cast_int64.get());
  AddZeroCopyCast(Type::DURATION, InputType(Type::DURATION), int64(), cast_int64.get());
  AddZeroCopyCast(Type::TIMESTAMP, InputType(Type::TIMESTAMP), int64(), cast_int64.get());
  AddZeroCopyCast(Type::TIME64, InputType(Type::TIME64), int64(), cast_int64.get());
  functions.push_back(cast_int64);

  functions.push_back(GetCastToInteger<UInt8Type>("cast_uint8"));
  functions.push_back(GetCastToInteger<UInt16Type>("cast_uint16"));
  functions.push_back(GetCastToInteger<UInt32Type>("cast_uint32"));
  functions.push_back(GetCastToInteger<UInt64Type>("cast_uint64"));

  // HalfFloat is a bit brain-damaged for now
  auto cast_half_float =
      std::make_shared<CastFunction>("cast_half_float", Type::HALF_FLOAT);
  AddCommonCasts<HalfFloatType>(float16(), cast_half_float.get());
  functions.push_back(cast_half_float);

  functions.push_back(GetCastToFloating<FloatType>("cast_float"));
  functions.push_back(GetCastToFloating<DoubleType>("cast_double"));

  functions.push_back(GetCastToDecimal());

  return functions;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
