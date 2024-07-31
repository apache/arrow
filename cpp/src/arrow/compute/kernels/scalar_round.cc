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

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/compare.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/base_arithmetic_internal.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/macros.h"
#include "arrow/visit_scalar_inline.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::NegateWithOverflow;
using internal::SubtractWithOverflow;

namespace compute::internal {

using applicator::ScalarBinary;
using applicator::ScalarBinaryEqualTypes;
using applicator::ScalarBinaryNotNull;
using applicator::ScalarBinaryNotNullEqualTypes;
using applicator::ScalarUnary;
using applicator::ScalarUnaryNotNull;
using applicator::ScalarUnaryNotNullStateful;

namespace {

// ----------------------------------------------------------------------
// Begin utility structs for round kernels

// Convenience visitor to detect if a numeric Scalar is positive.
struct IsPositiveVisitor {
  bool result = false;

  template <typename... Ts>
  Status Visit(const NumericScalar<Ts...>& scalar) {
    result = scalar.value > 0;
    return Status::OK();
  }
  template <typename... Ts>
  Status Visit(const DecimalScalar<Ts...>& scalar) {
    result = scalar.value > 0;
    return Status::OK();
  }
  Status Visit(const Scalar& scalar) { return Status::OK(); }
};

bool IsPositive(const Scalar& scalar) {
  IsPositiveVisitor visitor{};
  std::ignore = VisitScalarInline(scalar, &visitor);
  return visitor.result;
}

struct RoundUtil {
  // Calculate powers of ten with arbitrary integer exponent
  template <typename T>
  static enable_if_floating_value<T> Pow10(int64_t power) {
    static constexpr T lut[] = {1e0F, 1e1F, 1e2F,  1e3F,  1e4F,  1e5F,  1e6F,  1e7F,
                                1e8F, 1e9F, 1e10F, 1e11F, 1e12F, 1e13F, 1e14F, 1e15F};
    int64_t lut_size = (sizeof(lut) / sizeof(*lut));
    int64_t abs_power = std::abs(power);
    auto pow10 = lut[std::min(abs_power, lut_size - 1)];
    while (abs_power-- >= lut_size) {
      pow10 *= 1e1F;
    }
    return (power >= 0) ? pow10 : (1 / pow10);
  }

  // Calculate powers of ten with arbitrary integer exponent
  template <typename T>
  static enable_if_integer_value<T> Pow10(int64_t power) {
    DCHECK_GE(power, 0);
    DCHECK_LE(power, std::numeric_limits<T>::digits10);
    static constexpr uint64_t lut[] = {
        // clang-format off
        1ULL,
        10ULL,
        100ULL,
        1000ULL,
        10000ULL,
        100000ULL,
        1000000ULL,
        10000000ULL,
        100000000ULL,
        1000000000ULL,
        10000000000ULL,
        100000000000ULL,
        1000000000000ULL,
        10000000000000ULL,
        100000000000000ULL,
        1000000000000000ULL,
        10000000000000000ULL,
        100000000000000000ULL,
        1000000000000000000ULL
        // clang-format on
    };

    return static_cast<T>(lut[power]);
  }
};

// End utility structs for round kernels
// ----------------------------------------------------------------------
// Begin round implementations for single scalar

// Specializations of rounding implementations for round kernels
template <typename Type, RoundMode>
struct RoundImpl;

template <typename Type>
struct RoundImpl<Type, RoundMode::DOWN> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::floor(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() < 0) {
      (*val) -= pow10;
    }
  }

  template <typename T = Type>
  static enable_if_integer_value<T> Round(const T val, const T floor, const T multiple,
                                          Status* st) {
    if constexpr (is_signed_integer_value<T>::value) {
      if (ARROW_PREDICT_FALSE(val < 0 &&
                              std::numeric_limits<T>::min() + multiple > floor)) {
        *st = Status::Invalid("Rounding ", val, " down to multiple of ", multiple,
                              " would overflow");
        return val;
      }
      return val < 0 ? floor - multiple : floor;
    }
    return floor;
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::UP> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::ceil(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() > 0 && remainder != 0) {
      (*val) += pow10;
    }
  }

  template <typename T = Type>
  static enable_if_integer_value<T> Round(const T val, const T floor, const T multiple,
                                          Status* st) {
    if (ARROW_PREDICT_FALSE(val > 0 &&
                            std::numeric_limits<T>::max() - multiple < floor)) {
      *st = Status::Invalid("Rounding ", val, " up to multiple of ", multiple,
                            " would overflow");
      return val;
    }
    return val > 0 ? floor + multiple : floor;
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::TOWARDS_ZERO> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::trunc(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
  }

  template <typename T = Type>
  static enable_if_integer_value<T> Round(const T val, const T floor, const T pow10,
                                          Status* st) {
    return floor;
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::TOWARDS_INFINITY> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::signbit(val) ? std::floor(val) : std::ceil(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    (*val) -= remainder;
    if (remainder.Sign() < 0) {
      (*val) -= pow10;
    } else if (remainder.Sign() > 0 && remainder != 0) {
      (*val) += pow10;
    }
  }

  template <typename T = Type>
  static enable_if_integer_value<T> Round(const T val, const T floor, const T multiple,
                                          Status* st) {
    if constexpr (is_signed_integer_value<T>::value) {
      if (ARROW_PREDICT_FALSE(val < 0 &&
                              std::numeric_limits<T>::min() + multiple > floor)) {
        *st = Status::Invalid("Rounding ", val, " down to multiple of ", multiple,
                              " would overflow");
        return val;
      }
    }

    if (ARROW_PREDICT_FALSE(val > 0 &&
                            std::numeric_limits<T>::max() - multiple < floor)) {
      *st = Status::Invalid("Rounding ", val, " up to multiple of ", multiple,
                            " would overflow");
      return val;
    }

    if constexpr (is_signed_integer_value<T>::value) {
      return val < 0 ? floor - multiple : floor + multiple;
    }

    return floor + multiple;
  }
};

// NOTE: RoundImpl variants for the HALF_* rounding modes are only
// invoked when the fractional part is equal to 0.5 (std::round is invoked
// otherwise).

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_DOWN> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return RoundImpl<T, RoundMode::DOWN>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::DOWN>::Round(val, remainder, pow10, scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    return RoundImpl<T, RoundMode::DOWN>::Round(val, floor, multiple, st);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_UP> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return RoundImpl<T, RoundMode::UP>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::UP>::Round(val, remainder, pow10, scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    return RoundImpl<T, RoundMode::UP>::Round(val, floor, multiple, st);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TOWARDS_ZERO> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(val, remainder, pow10, scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    return RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(val, floor, multiple, st);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TOWARDS_INFINITY> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val);
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& multiple, const int32_t scale) {
    RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val, remainder, multiple, scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    return RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val, floor, multiple, st);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TO_EVEN> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::round(val * T(0.5)) * 2;
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    auto scaled = val->ReduceScaleBy(scale, /*round=*/false);
    if (scaled.low_bits() % 2 != 0) {
      scaled += remainder.Sign() >= 0 ? 1 : -1;
    }
    *val = scaled.IncreaseScaleBy(scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    if ((floor / multiple) % 2 == 0) {
      return floor;
    }
    return RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val, floor, multiple, st);
  }
};

template <typename Type>
struct RoundImpl<Type, RoundMode::HALF_TO_ODD> {
  template <typename T = Type>
  static constexpr enable_if_floating_value<T> Round(const T val) {
    return std::floor(val * T(0.5)) + std::ceil(val * T(0.5));
  }

  template <typename T = Type>
  static enable_if_decimal_value<T, void> Round(T* val, const T& remainder,
                                                const T& pow10, const int32_t scale) {
    auto scaled = val->ReduceScaleBy(scale, /*round=*/false);
    if (scaled.low_bits() % 2 == 0) {
      scaled += remainder.Sign() ? 1 : -1;
    }
    *val = scaled.IncreaseScaleBy(scale);
  }

  template <typename T = Type>
  static constexpr enable_if_integer_value<T> Round(const T val, const T floor,
                                                    const T multiple, Status* st) {
    if ((floor / multiple) % 2 != 0) {
      return floor;
    }
    return RoundImpl<T, RoundMode::TOWARDS_INFINITY>::Round(val, floor, multiple, st);
  }
};

// End round implementations for single scalar
// ----------------------------------------------------------------------
// Begin round options wrappers

// Specializations of kernel state for round kernels
// CType is the physical type used to store pow10
template <typename OptionsType, typename CType>
struct RoundOptionsWrapper;

template <typename CType>
struct RoundOptionsWrapper<RoundOptions, CType> : public OptionsWrapper<RoundOptions> {
  using OptionsType = RoundOptions;
  CType pow10;

  explicit RoundOptionsWrapper(OptionsType options) : OptionsWrapper(std::move(options)) {
    // Only positive exponents for powers of 10 are used because combining
    // multiply and division operations produced more stable rounding than
    // using multiply-only.  Refer to NumPy's round implementation:
    // https://github.com/numpy/numpy/blob/7b2f20b406d27364c812f7a81a9c901afbd3600c/numpy/core/src/multiarray/calculation.c#L589
    pow10 = RoundUtil::Pow10<CType>(std::abs(options.ndigits));
  }

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      if constexpr (is_integer_value<CType>::value) {
        if (-options->ndigits > std::numeric_limits<CType>::digits10) {
          return Status::Invalid("Rounding to ", options->ndigits,
                                 " digits is out of range for type ",
                                 args.inputs[0].ToString());
        }
      }
      return std::make_unique<RoundOptionsWrapper>(*options);
    }
    return Status::Invalid(
        "Attempted to initialize KernelState from null FunctionOptions");
  }
};

template <typename CType>
struct RoundOptionsWrapper<RoundBinaryOptions, CType>
    : public OptionsWrapper<RoundBinaryOptions> {
  using OptionsType = RoundBinaryOptions;

  explicit RoundOptionsWrapper(OptionsType options)
      : OptionsWrapper(std::move(options)) {}

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    if (auto options = static_cast<const OptionsType*>(args.options)) {
      return std::make_unique<RoundOptionsWrapper>(*options);
    }
    return Status::Invalid(
        "Attempted to initialize KernelState from null FunctionOptions");
  }
};

template <typename CType>
struct RoundOptionsWrapper<RoundToMultipleOptions, CType>
    : public OptionsWrapper<RoundToMultipleOptions> {
  using OptionsType = RoundToMultipleOptions;
  using OptionsWrapper::OptionsWrapper;

  static Result<std::unique_ptr<KernelState>> Init(KernelContext* ctx,
                                                   const KernelInitArgs& args) {
    auto options = static_cast<const OptionsType*>(args.options);
    if (!options) {
      return Status::Invalid(
          "Attempted to initialize KernelState from null FunctionOptions");
    }

    const auto& multiple = options->multiple;
    if (!multiple || !multiple->is_valid) {
      return Status::Invalid("Rounding multiple must be non-null and valid");
    }

    if (!IsPositive(*multiple)) {
      return Status::Invalid("Rounding multiple must be positive");
    }

    // Ensure the rounding multiple option matches the kernel's input type.
    std::shared_ptr<DataType> to_type = args.inputs[0].GetSharedPtr();
    if (!multiple->type->Equals(to_type)) {
      ARROW_ASSIGN_OR_RAISE(
          auto casted_multiple,
          Cast(Datum(multiple), to_type, CastOptions::Safe(), ctx->exec_context()));

      // Create a new option object if the rounding multiple was casted.
      auto new_options = OptionsType(casted_multiple.scalar(), options->round_mode);
      return std::make_unique<RoundOptionsWrapper>(new_options);
    }

    return std::make_unique<RoundOptionsWrapper>(*options);
  }
};

template <typename ArrowType, typename Enable = void>
struct RoundOptionsTrait;

template <typename ArrowType>
struct RoundOptionsTrait<ArrowType, enable_if_floating_point<ArrowType>> {
  using CType = double;
};

template <typename ArrowType>
struct RoundOptionsTrait<ArrowType, enable_if_decimal<ArrowType>> {
  using CType = double;
};

template <typename ArrowType>
struct RoundOptionsTrait<ArrowType, enable_if_integer<ArrowType>> {
  using CType = typename ArrowType::c_type;
};

// End round options wrappers
// ----------------------------------------------------------------------
// Begin round op implementations

template <typename ArrowType, RoundMode kRoundMode, typename Enable = void>
struct RoundToMultiple {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundToMultipleOptions,
                                    typename RoundOptionsTrait<ArrowType>::CType>;

  CType multiple;

  explicit RoundToMultiple(const State& state, const DataType& out_ty)
      : multiple(UnboxScalar<ArrowType>::Unbox(*state.options.multiple)) {
    const auto& options = state.options;
    DCHECK(options.multiple);
    DCHECK(options.multiple->is_valid);
    DCHECK(is_floating(options.multiple->type->id()));
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_floating_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    // Do not process Inf or NaN because they will trigger the overflow error at end of
    // function.
    if (!std::isfinite(arg)) {
      return arg;
    }
    auto round_val = arg / multiple;
    auto frac = round_val - std::floor(round_val);
    if (frac != T(0)) {
      // Use std::round() if in tie-breaking mode and scaled value is not 0.5.
      if ((kRoundMode >= RoundMode::HALF_DOWN) && (frac != T(0.5))) {
        round_val = std::round(round_val);
      } else {
        round_val = RoundImpl<CType, kRoundMode>::Round(round_val);
      }
      round_val *= multiple;
      if (!std::isfinite(round_val)) {
        *st = Status::Invalid("overflow occurred during rounding");
        return arg;
      }
    } else {
      // If scaled value is an integer, then no rounding is needed.
      round_val = arg;
    }
    return round_val;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct RoundToMultiple<ArrowType, kRoundMode, enable_if_decimal<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundToMultipleOptions, double>;
  const ArrowType& ty;
  CType multiple, half_multiple, neg_half_multiple;
  bool has_halfway_point;

  explicit RoundToMultiple(const State& state, const DataType& out_ty)
      : ty(checked_cast<const ArrowType&>(out_ty)),
        multiple(UnboxScalar<ArrowType>::Unbox(*state.options.multiple)),
        half_multiple(multiple / 2),
        neg_half_multiple(-half_multiple),
        has_halfway_point(multiple.low_bits() % 2 == 0) {
    const auto& options = state.options;
    DCHECK(options.multiple);
    DCHECK(options.multiple->is_valid);
    DCHECK(options.multiple->type->Equals(out_ty));
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_decimal_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    std::pair<CType, CType> pair;
    *st = arg.Divide(multiple).Value(&pair);
    if (!st->ok()) return arg;
    const auto& remainder = pair.second;
    if (remainder == 0) return arg;
    if (kRoundMode >= RoundMode::HALF_DOWN) {
      if (has_halfway_point &&
          (remainder == half_multiple || remainder == neg_half_multiple)) {
        // On the halfway point, use tiebreaker
        // Manually implement rounding since we're not actually rounding a
        // decimal value, but rather manipulating the multiple
        switch (kRoundMode) {
          case RoundMode::HALF_DOWN:
            if (remainder.Sign() < 0) pair.first -= 1;
            break;
          case RoundMode::HALF_UP:
            if (remainder.Sign() >= 0) pair.first += 1;
            break;
          case RoundMode::HALF_TOWARDS_ZERO:
            // Do nothing
            break;
          case RoundMode::HALF_TOWARDS_INFINITY:
            pair.first += remainder.Sign() >= 0 ? 1 : -1;
            break;
          case RoundMode::HALF_TO_EVEN:
            if (pair.first.low_bits() % 2 != 0) {
              pair.first += remainder.Sign() >= 0 ? 1 : -1;
            }
            break;
          case RoundMode::HALF_TO_ODD:
            if (pair.first.low_bits() % 2 == 0) {
              pair.first += remainder.Sign() >= 0 ? 1 : -1;
            }
            break;
          default:
            DCHECK(false);
        }
      } else if (remainder.Sign() >= 0) {
        // Positive, round up/down
        if (remainder > half_multiple) {
          pair.first += 1;
        }
      } else {
        // Negative, round up/down
        if (remainder < neg_half_multiple) {
          pair.first -= 1;
        }
      }
    } else {
      // Manually implement rounding since we're not actually rounding a
      // decimal value, but rather manipulating the multiple
      switch (kRoundMode) {
        case RoundMode::DOWN:
          if (remainder.Sign() < 0) pair.first -= 1;
          break;
        case RoundMode::UP:
          if (remainder.Sign() >= 0) pair.first += 1;
          break;
        case RoundMode::TOWARDS_ZERO:
          // Do nothing
          break;
        case RoundMode::TOWARDS_INFINITY:
          pair.first += remainder.Sign() >= 0 ? 1 : -1;
          break;
        default:
          DCHECK(false);
      }
    }
    CType round_val = pair.first * multiple;
    if (!round_val.FitsInPrecision(ty.precision())) {
      *st = Status::Invalid("Rounded value ", round_val.ToString(ty.scale()),
                            " does not fit in precision of ", ty);
      return 0;
    }
    return round_val;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct RoundToMultiple<ArrowType, kRoundMode, enable_if_integer<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundToMultipleOptions, CType>;
  CType multiple;

  explicit RoundToMultiple(const State& state, const DataType& out_ty)
      : multiple(UnboxScalar<ArrowType>::Unbox(*state.options.multiple)) {
    const auto& options = state.options;
    DCHECK(options.multiple);
    DCHECK(options.multiple->is_valid);
    DCHECK(is_integer(options.multiple->type->id()));
  }

  explicit RoundToMultiple(const CType multiple, const DataType& out_ty)
      : multiple(multiple) {}

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_integer_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    CType floor = arg / multiple * multiple;
    CType remainder = arg > floor ? arg - floor : floor - arg;

    if (remainder == 0) {
      return arg;
    }

    if (kRoundMode >= RoundMode::HALF_DOWN && remainder * 2 != multiple) {
      // not half way, round to nearest multiple of multiple like std::round
      if (remainder * 2 > multiple) {
        if (arg >= 0) {
          if (ARROW_PREDICT_FALSE(std::numeric_limits<CType>::max() - multiple < floor)) {
            *st = Status::Invalid("Rounding ", arg, " up to multiples of ", multiple,
                                  " would overflow");
            return arg;
          }
          return floor + multiple;
        } else {
          if (ARROW_PREDICT_FALSE(std::numeric_limits<CType>::min() + multiple > floor)) {
            *st = Status::Invalid("Rounding ", arg, " down to multiples of ", multiple,
                                  " would overflow");
            return arg;
          }
          return floor - multiple;
        }
      } else {
        return floor;
      }
    } else {
      return RoundImpl<CType, kRoundMode>::Round(arg, floor, multiple, st);
    }
  }
};

template <typename ArrowType, RoundMode RndMode, typename Enable = void>
struct Round {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State =
      RoundOptionsWrapper<RoundOptions, typename RoundOptionsTrait<ArrowType>::CType>;
  CType pow10;
  int64_t ndigits;

  explicit Round(const State& state, const DataType& out_ty)
      : pow10(static_cast<CType>(state.pow10)), ndigits(state.options.ndigits) {}

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_floating_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    // Do not process Inf or NaN because they will trigger the overflow error at end of
    // function.
    if (!std::isfinite(arg)) {
      return arg;
    }
    auto round_val = ndigits >= 0 ? (arg * pow10) : (arg / pow10);
    auto frac = round_val - std::floor(round_val);
    if (frac != T(0)) {
      // Use std::round() if in tie-breaking mode and scaled value is not 0.5.
      if ((RndMode >= RoundMode::HALF_DOWN) && (frac != T(0.5))) {
        round_val = std::round(round_val);
      } else {
        round_val = RoundImpl<CType, RndMode>::Round(round_val);
      }
      // Equality check is omitted so that the common case of 10^0 (integer rounding)
      // uses multiply-only
      round_val = ndigits > 0 ? (round_val / pow10) : (round_val * pow10);
      if (!std::isfinite(round_val)) {
        *st = Status::Invalid("overflow occurred during rounding");
        return arg;
      }
    } else {
      // If scaled value is an integer, then no rounding is needed.
      round_val = arg;
    }
    return round_val;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct Round<ArrowType, kRoundMode, enable_if_decimal<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundOptions, double>;
  const ArrowType& ty;
  int64_t ndigits;
  int32_t pow;
  // pow10 is "1" for the given decimal scale. Similarly half_pow10 is "0.5".
  CType pow10, half_pow10, neg_half_pow10;

  explicit Round(const State& state, const DataType& out_ty)
      : Round(state.options.ndigits, out_ty) {}

  explicit Round(int64_t ndigits, const DataType& out_ty)
      : ty(checked_cast<const ArrowType&>(out_ty)),
        ndigits(ndigits),
        pow(static_cast<int32_t>(ty.scale() - ndigits)) {
    if (pow >= ty.precision() || pow < 0) {
      pow10 = half_pow10 = neg_half_pow10 = 0;
    } else {
      pow10 = CType::GetScaleMultiplier(pow);
      half_pow10 = CType::GetHalfScaleMultiplier(pow);
      neg_half_pow10 = -half_pow10;
    }
  }

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_decimal_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    if (pow >= ty.precision()) {
      *st = Status::Invalid("Rounding to ", ndigits,
                            " digits will not fit in precision of ", ty);
      return 0;
    } else if (pow < 0) {
      // no-op, copy output to input
      return arg;
    }

    std::pair<CType, CType> pair;
    *st = arg.Divide(pow10).Value(&pair);
    if (!st->ok()) return arg;
    // The remainder is effectively the scaled fractional part after division.
    const auto& remainder = pair.second;
    if (remainder == 0) return arg;
    if (kRoundMode >= RoundMode::HALF_DOWN) {
      if (remainder == half_pow10 || remainder == neg_half_pow10) {
        // On the halfway point, use tiebreaker
        RoundImpl<CType, kRoundMode>::Round(&arg, remainder, pow10, pow);
      } else if (remainder.Sign() >= 0) {
        // Positive, round up/down
        arg -= remainder;
        if (remainder > half_pow10) {
          arg += pow10;
        }
      } else {
        // Negative, round up/down
        arg -= remainder;
        if (remainder < neg_half_pow10) {
          arg -= pow10;
        }
      }
    } else {
      RoundImpl<CType, kRoundMode>::Round(&arg, remainder, pow10, pow);
    }
    if (!arg.FitsInPrecision(ty.precision())) {
      *st = Status::Invalid("Rounded value ", arg.ToString(ty.scale()),
                            " does not fit in precision of ", ty);
      return 0;
    }
    return arg;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct Round<ArrowType, kRoundMode, enable_if_integer<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundOptions, CType>;
  CType pow10;
  int64_t ndigits;
  const DataType& out_ty;

  explicit Round(const State& state, const DataType& out_ty)
      : pow10(static_cast<CType>(state.pow10)),
        ndigits(state.options.ndigits),
        out_ty(out_ty) {}

  template <typename T = ArrowType, typename CType = typename TypeTraits<T>::CType>
  enable_if_integer_value<CType> Call(KernelContext* ctx, CType arg, Status* st) const {
    // no-op if ndigits is non-negative
    if (ndigits >= 0) {
      return arg;
    }

    // If ndigits is negative, then round to the nearest multiple of 10^ndigits.
    RoundToMultiple<ArrowType, kRoundMode> round_to_multiple(pow10, out_ty);
    return round_to_multiple.Call(ctx, arg, st);
  }
};

template <typename ArrowType, RoundMode RndMode, typename Enable = void>
struct RoundBinary {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundBinaryOptions,
                                    typename RoundOptionsTrait<ArrowType>::CType>;

  explicit RoundBinary(const State& state, const DataType& out_ty) {}

  template <typename T = ArrowType, typename CType0 = typename TypeTraits<T>::CType0,
            typename CType1 = typename TypeTraits<T>::CType1>
  enable_if_floating_value<CType> Call(KernelContext* ctx, CType0 arg0, CType1 arg1,
                                       Status* st) const {
    // Do not process Inf or NaN because they will trigger the overflow error at end of
    // function.
    if (!std::isfinite(arg0)) {
      return arg0;
    }

    // Only positive exponents for powers of 10 are used because combining
    // multiply and division operations produced more stable rounding than
    // using multiply-only.  Refer to NumPy's round implementation:
    // https://github.com/numpy/numpy/blob/7b2f20b406d27364c812f7a81a9c901afbd3600c/numpy/core/src/multiarray/calculation.c#L589
    double pow10 = RoundUtil::Pow10<double>(std::abs(arg1));

    auto round_val = arg1 >= 0 ? (arg0 * pow10) : (arg0 / pow10);
    auto frac = round_val - std::floor(round_val);
    if (frac != T(0)) {
      // Use std::round() if in tie-breaking mode and scaled value is not 0.5.
      if ((RndMode >= RoundMode::HALF_DOWN) && (frac != T(0.5))) {
        round_val = std::round(round_val);
      } else {
        round_val = RoundImpl<CType, RndMode>::Round(round_val);
      }
      // Equality check is omitted so that the common case of 10^0 (integer rounding)
      // uses multiply-only
      round_val = arg1 > 0 ? (round_val / pow10) : (round_val * pow10);
      if (!std::isfinite(round_val)) {
        *st = Status::Invalid("overflow occurred during rounding");
        return arg0;
      }
    } else {
      // If scaled value is an integer, then no rounding is needed.
      round_val = arg0;
    }
    return static_cast<CType0>(round_val);
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct RoundBinary<ArrowType, kRoundMode, enable_if_decimal<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundBinaryOptions, double>;
  const ArrowType& ty;
  int32_t pow;
  // pow10 is "1" for the given decimal scale. Similarly half_pow10 is "0.5".
  CType half_pow10, neg_half_pow10;

  explicit RoundBinary(const State& state, const DataType& out_ty)
      : RoundBinary(out_ty) {}

  explicit RoundBinary(const DataType& out_ty)
      : ty(checked_cast<const ArrowType&>(out_ty)),
        pow(static_cast<int32_t>(ty.scale() - 0)) {
    if (pow >= ty.precision() || pow < 0) {
      half_pow10 = neg_half_pow10 = 0;
    } else {
      half_pow10 = CType::GetHalfScaleMultiplier(pow);
      neg_half_pow10 = -half_pow10;
    }
  }

  template <typename T = ArrowType, typename CType0 = typename TypeTraits<T>::CType0,
            typename CType1 = typename TypeTraits<T>::CType1>
  enable_if_decimal_value<CType> Call(KernelContext* ctx, CType0 arg0, CType1 arg1,
                                      Status* st) const {
    if (pow - arg1 >= ty.precision()) {
      *st = Status::Invalid("Rounding to ", arg1, " digits will not fit in precision of ",
                            ty);
      return 0;
    } else if (pow < 0) {
      // no-op, copy output to input
      return arg0;
    }

    CType0 pow10 = CType0::GetScaleMultiplier(static_cast<int32_t>(ty.scale() - arg1));

    std::pair<CType, CType> pair;
    *st = arg0.Divide(pow10).Value(&pair);
    if (!st->ok()) return arg0;
    // The remainder is effectively the scaled fractional part after division.
    const auto& remainder = pair.second;
    if (remainder == 0) return arg0;
    if (kRoundMode >= RoundMode::HALF_DOWN) {
      if (remainder == half_pow10 || remainder == neg_half_pow10) {
        // On the halfway point, use tiebreaker
        RoundImpl<CType0, kRoundMode>::Round(&arg0, remainder, pow10, pow);
      } else if (remainder.Sign() >= 0) {
        // Positive, round up/down
        arg0 -= remainder;
        if (remainder > half_pow10) {
          arg0 += pow10;
        }
      } else {
        // Negative, round up/down
        arg0 -= remainder;
        if (remainder < neg_half_pow10) {
          arg0 -= pow10;
        }
      }
    } else {
      RoundImpl<CType0, kRoundMode>::Round(&arg0, remainder, pow10, pow);
    }
    if (!arg0.FitsInPrecision(ty.precision())) {
      *st = Status::Invalid("Rounded value ", arg0.ToString(ty.scale()),
                            " does not fit in precision of ", ty);
      return 0;
    }
    return arg0;
  }
};

template <typename ArrowType, RoundMode kRoundMode>
struct RoundBinary<ArrowType, kRoundMode, enable_if_integer<ArrowType>> {
  using CType = typename TypeTraits<ArrowType>::CType;
  using State = RoundOptionsWrapper<RoundBinaryOptions, CType>;

  const DataType& out_ty;
  explicit RoundBinary(const State& state, const DataType& out_ty) : out_ty(out_ty) {}

  template <typename T = ArrowType, typename CType0 = typename TypeTraits<T>::CType0,
            typename CType1 = typename TypeTraits<T>::CType1>
  enable_if_integer_value<CType> Call(KernelContext* ctx, CType0 arg0, CType1 arg1,
                                      Status* st) const {
    // ndigits >= 0 is a no-op
    if (arg1 >= 0) {
      return arg0;
    }

    if (-arg1 > std::numeric_limits<CType>::digits10) {
      // ndigits is larger than the number of digits CType can hold
      *st = Status::Invalid("Rounding to ", arg1, " digits is out of range for type ",
                            out_ty.ToString());
      return arg0;
    }

    // If ndigits is negative, then round to the nearest multiple of 10^ndigits.
    CType pow10 = RoundUtil::Pow10<CType>(std::abs(arg1));
    RoundToMultiple<ArrowType, kRoundMode> round_to_multiple(pow10, out_ty);
    return round_to_multiple.Call(ctx, arg0, st);
  }
};

struct Floor {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value);
    return RoundImpl<T, RoundMode::DOWN>::Round(arg);
  }
};

struct Ceil {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value);
    return RoundImpl<T, RoundMode::UP>::Round(arg);
  }
};

struct Trunc {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_value<Arg, T> Call(KernelContext*, Arg arg,
                                                         Status*) {
    static_assert(std::is_same<T, Arg>::value);
    return RoundImpl<T, RoundMode::TOWARDS_ZERO>::Round(arg);
  }
};

// End round op implementations
// ----------------------------------------------------------------------
// Begin round functions

struct RoundFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    // for binary round functions, the second scalar must be int32
    if (types->size() == 2 && (*types)[1].id() != Type::INT32) {
      (*types)[1] = int32();
    }
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

/// A RoundFunction that promotes only decimal arguments to double.
struct RoundDecimalToFloatingPointFunction : public RoundFunction {
  using RoundFunction::RoundFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    // Size of types is checked above.
    const auto originalType = (*types)[0];
    if (is_decimal((*types)[0].type->id())) {
      (*types)[0] = float64();
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    (*types)[0] = originalType;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

/// A RoundFunction that promotes only the first integer argument to double.
struct RoundIntegerToFloatingPointFunction : public RoundFunction {
  using RoundFunction::RoundFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    // Size of types is checked above.
    const auto originalType = (*types)[0];
    if (is_integer((*types)[0].type->id())) {
      (*types)[0] = float64();
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    (*types)[0] = originalType;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

/// A RoundFunction that promotes integer and decimal arguments to double.
struct RoundFloatingPointFunction : public RoundFunction {
  using RoundFunction::RoundFunction;

  Result<const Kernel*> DispatchBest(std::vector<TypeHolder>* types) const override {
    RETURN_NOT_OK(CheckArity(types->size()));

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    EnsureDictionaryDecoded(types);

    // Size of types is checked above.
    const auto originalType = (*types)[0];
    if (is_integer((*types)[0].type->id()) || is_decimal((*types)[0].type->id())) {
      (*types)[0] = float64();
    }

    if (auto kernel = DispatchExactImpl(this, *types)) return kernel;

    (*types)[0] = originalType;
    return arrow::compute::detail::NoMatchingKernel(this, *types);
  }
};

// End round functions
// ----------------------------------------------------------------------
// Begin round kernels

#define ROUND_CASE(MODE)                                                       \
  case RoundMode::MODE: {                                                      \
    using Op = OpImpl<Type, RoundMode::MODE>;                                  \
    return ScalarUnaryNotNullStateful<Type, Type, Op>(Op(state, *out->type())) \
        .Exec(ctx, batch, out);                                                \
  }

// Exec the round kernel for the given types
template <typename Type, typename OptionsType,
          template <typename, RoundMode, typename...> class OpImpl>
struct RoundKernel {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    using State =
        RoundOptionsWrapper<OptionsType, typename RoundOptionsTrait<Type>::CType>;
    const auto& state = static_cast<const State&>(*ctx->state());
    switch (state.options.round_mode) {
      ROUND_CASE(DOWN)
      ROUND_CASE(UP)
      ROUND_CASE(TOWARDS_ZERO)
      ROUND_CASE(TOWARDS_INFINITY)
      ROUND_CASE(HALF_DOWN)
      ROUND_CASE(HALF_UP)
      ROUND_CASE(HALF_TOWARDS_ZERO)
      ROUND_CASE(HALF_TOWARDS_INFINITY)
      ROUND_CASE(HALF_TO_EVEN)
      ROUND_CASE(HALF_TO_ODD)
    }
    DCHECK(false);
    return Status::NotImplemented(
        "Internal implementation error: round mode not implemented: ",
        state.options.ToString());
  }
};
#undef ROUND_CASE

#define ROUND_BINARY_CASE(MODE)                                                \
  case RoundMode::MODE: {                                                      \
    using Op = OpImpl<Type, RoundMode::MODE>;                                  \
    return applicator::ScalarBinaryNotNullStateful<Type, Type, Int32Type, Op>( \
               Op(state, *out->type()))                                        \
        .Exec(ctx, batch, out);                                                \
  }

// Exec the round (binary) kernel for the given types
template <typename Type, typename OptionsType,
          template <typename, RoundMode, typename...> class OpImpl>
struct RoundBinaryKernel {
  static Status Exec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
    using State =
        RoundOptionsWrapper<OptionsType, typename RoundOptionsTrait<Type>::CType>;
    const auto& state = static_cast<const State&>(*ctx->state());
    switch (state.options.round_mode) {
      ROUND_BINARY_CASE(DOWN)
      ROUND_BINARY_CASE(UP)
      ROUND_BINARY_CASE(TOWARDS_ZERO)
      ROUND_BINARY_CASE(TOWARDS_INFINITY)
      ROUND_BINARY_CASE(HALF_DOWN)
      ROUND_BINARY_CASE(HALF_UP)
      ROUND_BINARY_CASE(HALF_TOWARDS_ZERO)
      ROUND_BINARY_CASE(HALF_TOWARDS_INFINITY)
      ROUND_BINARY_CASE(HALF_TO_EVEN)
      ROUND_BINARY_CASE(HALF_TO_ODD)
    }
    DCHECK(false);
    return Status::NotImplemented(
        "Internal implementation error: round mode not implemented: ",
        state.options.ToString());
  }
};
#undef ROUND_BINARY_CASE

template <typename DecimalType, RoundMode kMode, int32_t kDigits>
Status FixedRoundDecimalExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  using Op = Round<DecimalType, kMode>;
  return ScalarUnaryNotNullStateful<DecimalType, DecimalType, Op>(
             Op(kDigits, *out->type()))
      .Exec(ctx, batch, out);
}

// End round kernels
// ----------------------------------------------------------------------
// Begin round kernel generation and function registration

template <
    template <typename, RoundMode, typename...> class Op,
    template <typename, typename, template <typename, RoundMode, typename...> typename>
    class Kernel,
    typename OptionsType>
struct RoundKernelGenerator {
  template <typename ArrowType>
  Status Visit(const ArrowType& type, ArrayKernelExec* exec, KernelInit* init) {
    if constexpr (is_integer_type<ArrowType>::value ||
                  (is_floating_type<ArrowType>::value &&
                   !is_half_float_type<ArrowType>::value) ||
                  is_decimal_type<ArrowType>::value) {
      *exec = Kernel<ArrowType, OptionsType, Op>::Exec;
      *init = RoundOptionsWrapper<OptionsType,
                                  typename RoundOptionsTrait<ArrowType>::CType>::Init;
    } else {
      DCHECK(false);
      return Status::NotImplemented("Round does not support ", type.ToString());
    }
    return Status::OK();
  }
};

// For unary rounding functions that control kernel dispatch based on RoundMode, only on
// non-null output.
template <template <typename, RoundMode, typename...> class Op, typename OptionsType>
std::shared_ptr<ScalarFunction> MakeUnaryRoundFunction(std::string name,
                                                       FunctionDoc doc) {
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func = std::make_shared<RoundFunction>(name, Arity::Unary(), std::move(doc),
                                              &kDefaultOptions);
  RoundKernelGenerator<Op, RoundKernel, OptionsType> kernel_generator;
  for (const auto& tys : {NumericTypes(), {decimal128(1, 0), decimal256(1, 0)}}) {
    for (const auto& ty : tys) {
      ArrayKernelExec exec = nullptr;
      KernelInit init;
      DCHECK_OK(VisitTypeInline(*ty, &kernel_generator, &exec, &init));
      DCHECK_OK(func->AddKernel(
          {InputType(ty->id())},
          is_decimal(ty->id()) ? OutputType(FirstType) : OutputType(ty), exec, init));
    }
  }
  AddNullExec(func.get());
  return func;
}

template <template <typename, RoundMode, typename...> class Op, typename OptionsType>
std::shared_ptr<ScalarFunction> MakeBinaryRoundFunction(const std::string& name,
                                                        FunctionDoc doc) {
  static const OptionsType kDefaultOptions = OptionsType::Defaults();
  auto func = std::make_shared<RoundFunction>(name, Arity::Binary(), std::move(doc),
                                              &kDefaultOptions);
  RoundKernelGenerator<Op, RoundBinaryKernel, OptionsType> kernel_generator;
  for (const auto& tys : {NumericTypes(), {decimal128(1, 0), decimal256(1, 0)}}) {
    for (const auto& ty : tys) {
      ArrayKernelExec exec = nullptr;
      KernelInit init;
      DCHECK_OK(VisitTypeInline(*ty, &kernel_generator, &exec, &init));
      DCHECK_OK(func->AddKernel(
          {InputType(ty->id()), Type::INT32},
          is_decimal(ty->id()) ? OutputType(FirstType) : OutputType(ty), exec, init));
    }
  }
  AddNullExec(func.get());
  return func;
}

template <typename Op, typename FunctionImpl = RoundFloatingPointFunction>
std::shared_ptr<ScalarFunction> MakeUnaryRoundFunctionFloatingPoint(std::string name,
                                                                    FunctionDoc doc) {
  auto func = std::make_shared<FunctionImpl>(name, Arity::Unary(), std::move(doc));
  for (const auto& ty : FloatingPointTypes()) {
    auto exec = GenerateArithmeticFloatingPoint<ScalarUnary, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  AddNullExec(func.get());
  return func;
}

const FunctionDoc floor_doc{
    "Round down to the nearest integer",
    ("Compute the largest integer value not greater in magnitude than `x`."),
    {"x"}};

const FunctionDoc ceil_doc{
    "Round up to the nearest integer",
    ("Compute the smallest integer value not less in magnitude than `x`."),
    {"x"}};

const FunctionDoc trunc_doc{
    "Compute the integral part",
    ("Compute the nearest integer not greater in magnitude than `x`."),
    {"x"}};

const FunctionDoc round_doc{
    "Round to a given precision",
    ("Options are used to control the number of digits and rounding mode.\n"
     "Default behavior is to round to the nearest integer and\n"
     "use half-to-even rule to break ties."),
    {"x"},
    "RoundOptions"};

const FunctionDoc round_binary_doc{
    "Round to the given precision",
    ("Options are used to control the rounding mode.\n"
     "Default behavior is to use the half-to-even rule to break ties."),
    {"x", "s"},
    "RoundBinaryOptions"};

const FunctionDoc round_to_multiple_doc{
    "Round to a given multiple",
    ("Options are used to control the rounding multiple and rounding mode.\n"
     "Default behavior is to round to the nearest integer and\n"
     "use half-to-even rule to break ties."),
    {"x"},
    "RoundToMultipleOptions"};
}  // namespace

void RegisterScalarRoundArithmetic(FunctionRegistry* registry) {
  auto floor =
      MakeUnaryRoundFunctionFloatingPoint<Floor, RoundIntegerToFloatingPointFunction>(
          "floor", floor_doc);
  DCHECK_OK(floor->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::DOWN, /*ndigits=*/0>));
  DCHECK_OK(floor->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::DOWN, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(floor)));

  auto ceil =
      MakeUnaryRoundFunctionFloatingPoint<Ceil, RoundIntegerToFloatingPointFunction>(
          "ceil", ceil_doc);
  DCHECK_OK(ceil->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::UP, /*ndigits=*/0>));
  DCHECK_OK(ceil->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::UP, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(ceil)));

  auto trunc =
      MakeUnaryRoundFunctionFloatingPoint<Trunc, RoundIntegerToFloatingPointFunction>(
          "trunc", trunc_doc);
  DCHECK_OK(trunc->AddKernel(
      {InputType(Type::DECIMAL128)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal128Type, RoundMode::TOWARDS_ZERO, /*ndigits=*/0>));
  DCHECK_OK(trunc->AddKernel(
      {InputType(Type::DECIMAL256)}, OutputType(FirstType),
      FixedRoundDecimalExec<Decimal256Type, RoundMode::TOWARDS_ZERO, /*ndigits=*/0>));
  DCHECK_OK(registry->AddFunction(std::move(trunc)));

  auto round = MakeUnaryRoundFunction<Round, RoundOptions>("round", round_doc);
  DCHECK_OK(registry->AddFunction(std::move(round)));

  auto round_binary = MakeBinaryRoundFunction<RoundBinary, RoundBinaryOptions>(
      "round_binary", round_binary_doc);
  DCHECK_OK(registry->AddFunction(std::move(round_binary)));

  auto round_to_multiple =
      MakeUnaryRoundFunction<RoundToMultiple, RoundToMultipleOptions>(
          "round_to_multiple", round_to_multiple_doc);
  DCHECK_OK(registry->AddFunction(std::move(round_to_multiple)));
}

// End round kernel generation and function registration
// ----------------------------------------------------------------------
}  // namespace compute::internal
}  // namespace arrow
