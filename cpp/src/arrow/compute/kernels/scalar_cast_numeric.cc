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

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/int_util.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::BitBlockCount;
using internal::CheckIntegersInRange;
using internal::IntegersCanFit;
using internal::OptionalBitBlockCounter;
using internal::ParseValue;

namespace compute {
namespace internal {

Status CastIntegerToInteger(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& options = checked_cast<const CastState*>(ctx->state())->options;
  if (!options.allow_int_overflow) {
    RETURN_NOT_OK(IntegersCanFit(batch[0], *out->type()));
  }
  CastNumberToNumberUnsafe(batch[0].type()->id(), out->type()->id(), batch[0], out);
  return Status::OK();
}

Status CastFloatingToFloating(KernelContext*, const ExecBatch& batch, Datum* out) {
  CastNumberToNumberUnsafe(batch[0].type()->id(), out->type()->id(), batch[0], out);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement fast safe floating point to integer cast

// InType is a floating point type we are planning to cast to integer
template <typename InType, typename OutType, typename InT = typename InType::c_type,
          typename OutT = typename OutType::c_type>
ARROW_DISABLE_UBSAN("float-cast-overflow")
Status CheckFloatTruncation(const Datum& input, const Datum& output) {
  auto WasTruncated = [&](OutT out_val, InT in_val) -> bool {
    return static_cast<InT>(out_val) != in_val;
  };
  auto WasTruncatedMaybeNull = [&](OutT out_val, InT in_val, bool is_valid) -> bool {
    return is_valid && static_cast<InT>(out_val) != in_val;
  };
  auto GetErrorMessage = [&](InT val) {
    return Status::Invalid("Float value ", val, " was truncated converting to ",
                           *output.type());
  };

  if (input.kind() == Datum::SCALAR) {
    DCHECK_EQ(output.kind(), Datum::SCALAR);
    const auto& in_scalar = input.scalar_as<typename TypeTraits<InType>::ScalarType>();
    const auto& out_scalar = output.scalar_as<typename TypeTraits<OutType>::ScalarType>();
    if (WasTruncatedMaybeNull(out_scalar.value, in_scalar.value, out_scalar.is_valid)) {
      return GetErrorMessage(in_scalar.value);
    }
    return Status::OK();
  }

  const ArrayData& in_array = *input.array();
  const ArrayData& out_array = *output.array();

  const InT* in_data = in_array.GetValues<InT>(1);
  const OutT* out_data = out_array.GetValues<OutT>(1);

  const uint8_t* bitmap = nullptr;
  if (in_array.buffers[0]) {
    bitmap = in_array.buffers[0]->data();
  }
  OptionalBitBlockCounter bit_counter(bitmap, in_array.offset, in_array.length);
  int64_t position = 0;
  int64_t offset_position = in_array.offset;
  while (position < in_array.length) {
    BitBlockCount block = bit_counter.NextBlock();
    bool block_out_of_bounds = false;
    if (block.popcount == block.length) {
      // Fast path: branchless
      for (int64_t i = 0; i < block.length; ++i) {
        block_out_of_bounds |= WasTruncated(out_data[i], in_data[i]);
      }
    } else if (block.popcount > 0) {
      // Indices have nulls, must only boundscheck non-null values
      for (int64_t i = 0; i < block.length; ++i) {
        block_out_of_bounds |= WasTruncatedMaybeNull(
            out_data[i], in_data[i], BitUtil::GetBit(bitmap, offset_position + i));
      }
    }
    if (ARROW_PREDICT_FALSE(block_out_of_bounds)) {
      if (in_array.GetNullCount() > 0) {
        for (int64_t i = 0; i < block.length; ++i) {
          if (WasTruncatedMaybeNull(out_data[i], in_data[i],
                                    BitUtil::GetBit(bitmap, offset_position + i))) {
            return GetErrorMessage(in_data[i]);
          }
        }
      } else {
        for (int64_t i = 0; i < block.length; ++i) {
          if (WasTruncated(out_data[i], in_data[i])) {
            return GetErrorMessage(in_data[i]);
          }
        }
      }
    }
    in_data += block.length;
    out_data += block.length;
    position += block.length;
    offset_position += block.length;
  }
  return Status::OK();
}

template <typename InType>
Status CheckFloatToIntTruncationImpl(const Datum& input, const Datum& output) {
  switch (output.type()->id()) {
    case Type::INT8:
      return CheckFloatTruncation<InType, Int8Type>(input, output);
    case Type::INT16:
      return CheckFloatTruncation<InType, Int16Type>(input, output);
    case Type::INT32:
      return CheckFloatTruncation<InType, Int32Type>(input, output);
    case Type::INT64:
      return CheckFloatTruncation<InType, Int64Type>(input, output);
    case Type::UINT8:
      return CheckFloatTruncation<InType, UInt8Type>(input, output);
    case Type::UINT16:
      return CheckFloatTruncation<InType, UInt16Type>(input, output);
    case Type::UINT32:
      return CheckFloatTruncation<InType, UInt32Type>(input, output);
    case Type::UINT64:
      return CheckFloatTruncation<InType, UInt64Type>(input, output);
    default:
      break;
  }
  DCHECK(false);
  return Status::OK();
}

Status CheckFloatToIntTruncation(const Datum& input, const Datum& output) {
  switch (input.type()->id()) {
    case Type::FLOAT:
      return CheckFloatToIntTruncationImpl<FloatType>(input, output);
    case Type::DOUBLE:
      return CheckFloatToIntTruncationImpl<DoubleType>(input, output);
    default:
      break;
  }
  DCHECK(false);
  return Status::OK();
}

Status CastFloatingToInteger(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& options = checked_cast<const CastState*>(ctx->state())->options;
  CastNumberToNumberUnsafe(batch[0].type()->id(), out->type()->id(), batch[0], out);
  if (!options.allow_float_truncate) {
    RETURN_NOT_OK(CheckFloatToIntTruncation(batch[0], *out));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement fast integer to floating point cast

// These are the limits for exact representation of whole numbers in floating
// point numbers
template <typename T>
struct FloatingIntegerBound {};

template <>
struct FloatingIntegerBound<float> {
  static const int64_t value = 1LL << 24;
};

template <>
struct FloatingIntegerBound<double> {
  static const int64_t value = 1LL << 53;
};

template <typename InType, typename OutType, typename InT = typename InType::c_type,
          typename OutT = typename OutType::c_type,
          bool IsSigned = is_signed_integer_type<InType>::value>
Status CheckIntegerFloatTruncateImpl(const Datum& input) {
  using InScalarType = typename TypeTraits<InType>::ScalarType;
  const int64_t limit = FloatingIntegerBound<OutT>::value;
  InScalarType bound_lower(IsSigned ? -limit : 0);
  InScalarType bound_upper(limit);
  return CheckIntegersInRange(input, bound_lower, bound_upper);
}

Status CheckForIntegerToFloatingTruncation(const Datum& input, Type::type out_type) {
  switch (input.type()->id()) {
    // Small integers are all exactly representable as whole numbers
    case Type::INT8:
    case Type::INT16:
    case Type::UINT8:
    case Type::UINT16:
      return Status::OK();
    case Type::INT32: {
      if (out_type == Type::DOUBLE) {
        return Status::OK();
      }
      return CheckIntegerFloatTruncateImpl<Int32Type, FloatType>(input);
    }
    case Type::UINT32: {
      if (out_type == Type::DOUBLE) {
        return Status::OK();
      }
      return CheckIntegerFloatTruncateImpl<UInt32Type, FloatType>(input);
    }
    case Type::INT64: {
      if (out_type == Type::FLOAT) {
        return CheckIntegerFloatTruncateImpl<Int64Type, FloatType>(input);
      } else {
        return CheckIntegerFloatTruncateImpl<Int64Type, DoubleType>(input);
      }
    }
    case Type::UINT64: {
      if (out_type == Type::FLOAT) {
        return CheckIntegerFloatTruncateImpl<UInt64Type, FloatType>(input);
      } else {
        return CheckIntegerFloatTruncateImpl<UInt64Type, DoubleType>(input);
      }
    }
    default:
      break;
  }
  DCHECK(false);
  return Status::OK();
}

Status CastIntegerToFloating(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& options = checked_cast<const CastState*>(ctx->state())->options;
  Type::type out_type = out->type()->id();
  if (!options.allow_float_truncate) {
    RETURN_NOT_OK(CheckForIntegerToFloatingTruncation(batch[0], out_type));
  }
  CastNumberToNumberUnsafe(batch[0].type()->id(), out_type, batch[0], out);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Boolean to number

struct BooleanToNumber {
  template <typename OutValue, typename Arg0Value>
  static OutValue Call(KernelContext*, Arg0Value val, Status*) {
    constexpr auto kOne = static_cast<OutValue>(1);
    constexpr auto kZero = static_cast<OutValue>(0);
    return val ? kOne : kZero;
  }
};

template <typename O>
struct CastFunctor<O, BooleanType, enable_if_number<O>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return applicator::ScalarUnary<O, BooleanType, BooleanToNumber>::Exec(ctx, batch,
                                                                          out);
  }
};

// ----------------------------------------------------------------------
// String to number

template <typename OutType>
struct ParseString {
  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext* ctx, Arg0Value val, Status* st) const {
    OutValue result = OutValue(0);
    if (ARROW_PREDICT_FALSE(!ParseValue<OutType>(val.data(), val.size(), &result))) {
      *st = Status::Invalid("Failed to parse string: '", val, "' as a scalar of type ",
                            TypeTraits<OutType>::type_singleton()->ToString());
    }
    return result;
  }
};

template <typename O, typename I>
struct CastFunctor<O, I, enable_if_base_binary<I>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return applicator::ScalarUnaryNotNull<O, I, ParseString<O>>::Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Decimal to integer

struct DecimalToIntegerMixin {
  template <typename OutValue, typename Arg0Value>
  OutValue ToInteger(KernelContext* ctx, const Arg0Value& val, Status* st) const {
    constexpr auto min_value = std::numeric_limits<OutValue>::min();
    constexpr auto max_value = std::numeric_limits<OutValue>::max();

    if (!allow_int_overflow_ && ARROW_PREDICT_FALSE(val < min_value || val > max_value)) {
      *st = Status::Invalid("Integer value out of bounds");
      return OutValue{};  // Zero
    } else {
      return static_cast<OutValue>(val.low_bits());
    }
  }

  DecimalToIntegerMixin(int32_t in_scale, bool allow_int_overflow)
      : in_scale_(in_scale), allow_int_overflow_(allow_int_overflow) {}

  int32_t in_scale_;
  bool allow_int_overflow_;
};

struct UnsafeUpscaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext* ctx, Arg0Value val, Status* st) const {
    return ToInteger<OutValue>(ctx, val.IncreaseScaleBy(-in_scale_), st);
  }
};

struct UnsafeDownscaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext* ctx, Arg0Value val, Status* st) const {
    return ToInteger<OutValue>(ctx, val.ReduceScaleBy(in_scale_, false), st);
  }
};

struct SafeRescaleDecimalToInteger : public DecimalToIntegerMixin {
  using DecimalToIntegerMixin::DecimalToIntegerMixin;

  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext* ctx, Arg0Value val, Status* st) const {
    auto result = val.Rescale(in_scale_, 0);
    if (ARROW_PREDICT_FALSE(!result.ok())) {
      *st = result.status();
      return OutValue{};  // Zero
    } else {
      return ToInteger<OutValue>(ctx, *result, st);
    }
  }
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_integer_type<O>::value && is_decimal_type<I>::value>> {
  using out_type = typename O::c_type;

  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;

    const auto& in_type_inst = checked_cast<const I&>(*batch[0].type());
    const auto in_scale = in_type_inst.scale();

    if (options.allow_decimal_truncate) {
      if (in_scale < 0) {
        // Unsafe upscale
        applicator::ScalarUnaryNotNullStateful<O, I, UnsafeUpscaleDecimalToInteger>
            kernel(UnsafeUpscaleDecimalToInteger{in_scale, options.allow_int_overflow});
        return kernel.Exec(ctx, batch, out);
      } else {
        // Unsafe downscale
        applicator::ScalarUnaryNotNullStateful<O, I, UnsafeDownscaleDecimalToInteger>
            kernel(UnsafeDownscaleDecimalToInteger{in_scale, options.allow_int_overflow});
        return kernel.Exec(ctx, batch, out);
      }
    } else {
      // Safe rescale
      applicator::ScalarUnaryNotNullStateful<O, I, SafeRescaleDecimalToInteger> kernel(
          SafeRescaleDecimalToInteger{in_scale, options.allow_int_overflow});
      return kernel.Exec(ctx, batch, out);
    }
  }
};

// ----------------------------------------------------------------------
// Decimal to decimal

// Helper that converts the input and output decimals
// For instance, Decimal128 -> Decimal256 requires converting, then scaling
// Decimal256 -> Decimal128 requires scaling, then truncating
template <typename OutDecimal, typename InDecimal>
struct DecimalConversions {};

template <typename InDecimal>
struct DecimalConversions<Decimal256, InDecimal> {
  // Convert then scale
  static Decimal256 ConvertInput(InDecimal&& val) { return Decimal256(val); }
  static Decimal256 ConvertOutput(Decimal256&& val) { return val; }
};

template <>
struct DecimalConversions<Decimal128, Decimal256> {
  // Scale then truncate
  static Decimal256 ConvertInput(Decimal256&& val) { return val; }
  static Decimal128 ConvertOutput(Decimal256&& val) {
    return Decimal128(val.little_endian_array()[1], val.little_endian_array()[0]);
  }
};

template <>
struct DecimalConversions<Decimal128, Decimal128> {
  static Decimal128 ConvertInput(Decimal128&& val) { return val; }
  static Decimal128 ConvertOutput(Decimal128&& val) { return val; }
};

struct UnsafeUpscaleDecimal {
  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext*, Arg0Value val, Status*) const {
    using Conv = DecimalConversions<OutValue, Arg0Value>;
    return Conv::ConvertOutput(Conv::ConvertInput(std::move(val)).IncreaseScaleBy(by_));
  }
  int32_t by_;
};

struct UnsafeDownscaleDecimal {
  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext*, Arg0Value val, Status*) const {
    using Conv = DecimalConversions<OutValue, Arg0Value>;
    return Conv::ConvertOutput(
        Conv::ConvertInput(std::move(val)).ReduceScaleBy(by_, false));
  }
  int32_t by_;
};

struct SafeRescaleDecimal {
  template <typename OutValue, typename Arg0Value>
  OutValue Call(KernelContext*, Arg0Value val, Status* st) const {
    using Conv = DecimalConversions<OutValue, Arg0Value>;
    auto maybe_rescaled =
        Conv::ConvertInput(std::move(val)).Rescale(in_scale_, out_scale_);
    if (ARROW_PREDICT_FALSE(!maybe_rescaled.ok())) {
      *st = maybe_rescaled.status();
      return {};  // Zero
    }

    if (ARROW_PREDICT_TRUE(maybe_rescaled->FitsInPrecision(out_precision_))) {
      return Conv::ConvertOutput(maybe_rescaled.MoveValueUnsafe());
    }

    *st = Status::Invalid("Decimal value does not fit in precision ", out_precision_);
    return {};  // Zero
  }

  int32_t out_scale_, out_precision_, in_scale_;
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_decimal_type<O>::value && is_decimal_type<I>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;

    const auto& in_type = checked_cast<const I&>(*batch[0].type());
    const auto& out_type = checked_cast<const O&>(*out->type());
    const auto in_scale = in_type.scale();
    const auto out_scale = out_type.scale();

    if (options.allow_decimal_truncate) {
      if (in_scale < out_scale) {
        // Unsafe upscale
        applicator::ScalarUnaryNotNullStateful<O, I, UnsafeUpscaleDecimal> kernel(
            UnsafeUpscaleDecimal{out_scale - in_scale});
        return kernel.Exec(ctx, batch, out);
      } else {
        // Unsafe downscale
        applicator::ScalarUnaryNotNullStateful<O, I, UnsafeDownscaleDecimal> kernel(
            UnsafeDownscaleDecimal{in_scale - out_scale});
        return kernel.Exec(ctx, batch, out);
      }
    }

    // Safe rescale
    applicator::ScalarUnaryNotNullStateful<O, I, SafeRescaleDecimal> kernel(
        SafeRescaleDecimal{out_scale, out_type.precision(), in_scale});
    return kernel.Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Real to decimal

struct RealToDecimal {
  template <typename OutValue, typename RealType>
  OutValue Call(KernelContext*, RealType val, Status* st) const {
    auto maybe_decimal = OutValue::FromReal(val, out_precision_, out_scale_);

    if (ARROW_PREDICT_TRUE(maybe_decimal.ok())) {
      return maybe_decimal.MoveValueUnsafe();
    }

    if (!allow_truncate_) {
      *st = maybe_decimal.status();
    }
    return {};  // Zero
  }

  int32_t out_scale_, out_precision_;
  bool allow_truncate_;
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_decimal_type<O>::value && is_floating_type<I>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& options = checked_cast<const CastState*>(ctx->state())->options;
    const auto& out_type = checked_cast<const O&>(*out->type());
    const auto out_scale = out_type.scale();
    const auto out_precision = out_type.precision();

    applicator::ScalarUnaryNotNullStateful<O, I, RealToDecimal> kernel(
        RealToDecimal{out_scale, out_precision, options.allow_decimal_truncate});
    return kernel.Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Decimal to real

struct DecimalToReal {
  template <typename RealType, typename Arg0Value>
  RealType Call(KernelContext*, const Arg0Value& val, Status*) const {
    return val.template ToReal<RealType>(in_scale_);
  }

  int32_t in_scale_;
};

template <typename O, typename I>
struct CastFunctor<O, I,
                   enable_if_t<is_floating_type<O>::value && is_decimal_type<I>::value>> {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    const auto& in_type = checked_cast<const I&>(*batch[0].type());
    const auto in_scale = in_type.scale();

    applicator::ScalarUnaryNotNullStateful<O, I, DecimalToReal> kernel(
        DecimalToReal{in_scale});
    return kernel.Exec(ctx, batch, out);
  }
};

// ----------------------------------------------------------------------
// Top-level kernel instantiation

namespace {

template <typename OutType>
void AddCommonNumberCasts(const std::shared_ptr<DataType>& out_ty, CastFunction* func) {
  AddCommonCasts(out_ty->id(), out_ty, func);

  // Cast from boolean to number
  DCHECK_OK(func->AddKernel(Type::BOOL, {boolean()}, out_ty,
                            CastFunctor<OutType, BooleanType>::Exec));

  // Cast from other strings
  for (const std::shared_ptr<DataType>& in_ty : BaseBinaryTypes()) {
    auto exec = GenerateVarBinaryBase<CastFunctor, OutType>(*in_ty);
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, exec));
  }
}

template <typename OutType>
std::shared_ptr<CastFunction> GetCastToInteger(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), OutType::type_id);
  auto out_ty = TypeTraits<OutType>::type_singleton();

  for (const std::shared_ptr<DataType>& in_ty : IntTypes()) {
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, CastIntegerToInteger));
  }

  // Cast from floating point
  for (const std::shared_ptr<DataType>& in_ty : FloatingPointTypes()) {
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, CastFloatingToInteger));
  }

  // From other numbers to integer
  AddCommonNumberCasts<OutType>(out_ty, func.get());

  // From decimal to integer
  DCHECK_OK(func->AddKernel(Type::DECIMAL, {InputType(Type::DECIMAL)}, out_ty,
                            CastFunctor<OutType, Decimal128Type>::Exec));
  DCHECK_OK(func->AddKernel(Type::DECIMAL256, {InputType(Type::DECIMAL256)}, out_ty,
                            CastFunctor<OutType, Decimal256Type>::Exec));
  return func;
}

template <typename OutType>
std::shared_ptr<CastFunction> GetCastToFloating(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), OutType::type_id);
  auto out_ty = TypeTraits<OutType>::type_singleton();

  // Casts from integer to floating point
  for (const std::shared_ptr<DataType>& in_ty : IntTypes()) {
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, CastIntegerToFloating));
  }

  // Cast from floating point
  for (const std::shared_ptr<DataType>& in_ty : FloatingPointTypes()) {
    DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, out_ty, CastFloatingToFloating));
  }

  // From other numbers to floating point
  AddCommonNumberCasts<OutType>(out_ty, func.get());

  // From decimal to floating point
  DCHECK_OK(func->AddKernel(Type::DECIMAL, {InputType(Type::DECIMAL)}, out_ty,
                            CastFunctor<OutType, Decimal128Type>::Exec));
  DCHECK_OK(func->AddKernel(Type::DECIMAL256, {InputType(Type::DECIMAL256)}, out_ty,
                            CastFunctor<OutType, Decimal256Type>::Exec));
  return func;
}

std::shared_ptr<CastFunction> GetCastToDecimal128() {
  OutputType sig_out_ty(ResolveOutputFromOptions);

  auto func = std::make_shared<CastFunction>("cast_decimal", Type::DECIMAL128);
  AddCommonCasts(Type::DECIMAL128, sig_out_ty, func.get());

  // Cast from floating point
  DCHECK_OK(func->AddKernel(Type::FLOAT, {float32()}, sig_out_ty,
                            CastFunctor<Decimal128Type, FloatType>::Exec));
  DCHECK_OK(func->AddKernel(Type::DOUBLE, {float64()}, sig_out_ty,
                            CastFunctor<Decimal128Type, DoubleType>::Exec));

  // Cast from other decimal
  auto exec = CastFunctor<Decimal128Type, Decimal128Type>::Exec;
  // We resolve the output type of this kernel from the CastOptions
  DCHECK_OK(
      func->AddKernel(Type::DECIMAL128, {InputType(Type::DECIMAL128)}, sig_out_ty, exec));
  exec = CastFunctor<Decimal128Type, Decimal256Type>::Exec;
  DCHECK_OK(
      func->AddKernel(Type::DECIMAL256, {InputType(Type::DECIMAL256)}, sig_out_ty, exec));
  return func;
}

std::shared_ptr<CastFunction> GetCastToDecimal256() {
  OutputType sig_out_ty(ResolveOutputFromOptions);

  auto func = std::make_shared<CastFunction>("cast_decimal256", Type::DECIMAL256);
  AddCommonCasts(Type::DECIMAL256, sig_out_ty, func.get());

  // Cast from floating point
  DCHECK_OK(func->AddKernel(Type::FLOAT, {float32()}, sig_out_ty,
                            CastFunctor<Decimal256Type, FloatType>::Exec));
  DCHECK_OK(func->AddKernel(Type::DOUBLE, {float64()}, sig_out_ty,
                            CastFunctor<Decimal256Type, DoubleType>::Exec));

  // Cast from other decimal
  auto exec = CastFunctor<Decimal256Type, Decimal128Type>::Exec;
  DCHECK_OK(
      func->AddKernel(Type::DECIMAL128, {InputType(Type::DECIMAL128)}, sig_out_ty, exec));
  exec = CastFunctor<Decimal256Type, Decimal256Type>::Exec;
  DCHECK_OK(
      func->AddKernel(Type::DECIMAL256, {InputType(Type::DECIMAL256)}, sig_out_ty, exec));
  return func;
}

}  // namespace

std::vector<std::shared_ptr<CastFunction>> GetNumericCasts() {
  std::vector<std::shared_ptr<CastFunction>> functions;

  // Make a cast to null that does not do much. Not sure why we need to be able
  // to cast from dict<null> -> null but there are unit tests for it
  auto cast_null = std::make_shared<CastFunction>("cast_null", Type::NA);
  DCHECK_OK(cast_null->AddKernel(Type::DICTIONARY, {InputType(Type::DICTIONARY)}, null(),
                                 OutputAllNull));
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
  AddCommonCasts(Type::HALF_FLOAT, float16(), cast_half_float.get());
  functions.push_back(cast_half_float);

  functions.push_back(GetCastToFloating<FloatType>("cast_float"));
  functions.push_back(GetCastToFloating<DoubleType>("cast_double"));

  functions.push_back(GetCastToDecimal128());
  functions.push_back(GetCastToDecimal256());

  return functions;
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
