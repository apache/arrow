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

#include "arrow/compute/kernels/common.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::NegateWithOverflow;
using internal::SubtractWithOverflow;

namespace compute {
namespace internal {

using applicator::ScalarBinaryEqualTypes;
using applicator::ScalarBinaryNotNullEqualTypes;
using applicator::ScalarUnary;
using applicator::ScalarUnaryNotNull;

namespace {

template <typename T>
using is_unsigned_integer = std::integral_constant<bool, std::is_integral<T>::value &&
                                                             std::is_unsigned<T>::value>;

template <typename T>
using is_signed_integer =
    std::integral_constant<bool, std::is_integral<T>::value && std::is_signed<T>::value>;

template <typename T>
using enable_if_signed_integer = enable_if_t<is_signed_integer<T>::value, T>;

template <typename T>
using enable_if_unsigned_integer = enable_if_t<is_unsigned_integer<T>::value, T>;

template <typename T>
using enable_if_integer =
    enable_if_t<is_signed_integer<T>::value || is_unsigned_integer<T>::value, T>;

template <typename T>
using enable_if_floating_point = enable_if_t<std::is_floating_point<T>::value, T>;

template <typename T>
using enable_if_decimal =
    enable_if_t<std::is_same<Decimal128, T>::value || std::is_same<Decimal256, T>::value,
                T>;

template <typename T, typename Unsigned = typename std::make_unsigned<T>::type>
constexpr Unsigned to_unsigned(T signed_) {
  return static_cast<Unsigned>(signed_);
}

struct AbsoluteValue {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T arg, Status*) {
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T arg, Status*) {
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T arg, Status* st) {
    return (arg < 0) ? arrow::internal::SafeSignedNegate(arg) : arg;
  }

  template <typename T, typename Arg>
  static enable_if_decimal<T> Call(KernelContext*, T arg, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

struct AbsoluteValueChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    if (arg == std::numeric_limits<Arg>::min()) {
      *st = Status::Invalid("overflow");
      return arg;
    }
    return std::abs(arg);
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer<T> Call(KernelContext* ctx, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return std::fabs(arg);
  }

  template <typename T, typename Arg>
  static enable_if_decimal<T> Call(KernelContext*, Arg arg, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

struct Add {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return arrow::internal::SafeSignedAdd(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

struct AddChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + right;
  }
};

struct Subtract {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return arrow::internal::SafeSignedSubtract(left, right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct SubtractChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left + (-right);
  }
};

struct Multiply {
  static_assert(std::is_same<decltype(int8_t() * int8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint8_t() * uint8_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int16_t() * int16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint16_t() * uint16_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(int32_t() * int32_t()), int32_t>::value, "");
  static_assert(std::is_same<decltype(uint32_t() * uint32_t()), uint32_t>::value, "");
  static_assert(std::is_same<decltype(int64_t() * int64_t()), int64_t>::value, "");
  static_assert(std::is_same<decltype(uint64_t() * uint64_t()), uint64_t>::value, "");

  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right,
                                                      Status*) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right,
                                                    Status*) {
    return to_unsigned(left) * to_unsigned(right);
  }

  // Multiplication of 16 bit integer types implicitly promotes to signed 32 bit
  // integer. However, some inputs may nevertheless overflow (which triggers undefined
  // behaviour). Therefore we first cast to 32 bit unsigned integers where overflow is
  // well defined.
  template <typename T = void>
  static constexpr int16_t Call(KernelContext*, int16_t left, int16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
  template <typename T = void>
  static constexpr uint16_t Call(KernelContext*, uint16_t left, uint16_t right, Status*) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left * right;
  }
};

struct MultiplyChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(left, right, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left * right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status*) {
    return left * right;
  }
};

struct Divide {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status*) {
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        result = 0;
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    if (right == Arg1()) {
      *st = Status::Invalid("Divide by zero");
      return T();
    } else {
      return left / right;
    }
  }
};

struct DivideChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        *st = Status::Invalid("divide by zero");
      } else {
        *st = Status::Invalid("overflow");
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right,
                                          Status* st) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    if (ARROW_PREDICT_FALSE(right == 0)) {
      *st = Status::Invalid("divide by zero");
      return 0;
    }
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 left, Arg1 right, Status* st) {
    if (right == Arg1()) {
      *st = Status::Invalid("Divide by zero");
      return T();
    } else {
      return left / right;
    }
  }
};

struct Negate {
  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status*) {
    return -arg;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, Arg arg, Status*) {
    return ~arg + 1;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status*) {
    return arrow::internal::SafeSignedNegate(arg);
  }

  template <typename T, typename Arg>
  static enable_if_decimal<T> Call(KernelContext*, Arg arg, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

struct NegateChecked {
  template <typename T, typename Arg>
  static enable_if_signed_integer<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(NegateWithOverflow(arg, &result))) {
      *st = Status::Invalid("overflow");
    }
    return result;
  }

  template <typename T, typename Arg>
  static enable_if_unsigned_integer<T> Call(KernelContext* ctx, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    DCHECK(false) << "This is included only for the purposes of instantiability from the "
                     "arithmetic kernel generator";
    return 0;
  }

  template <typename T, typename Arg>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, Arg arg, Status* st) {
    static_assert(std::is_same<T, Arg>::value, "");
    return -arg;
  }

  template <typename T, typename Arg>
  static enable_if_decimal<T> Call(KernelContext*, Arg arg, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

struct Power {
  ARROW_NOINLINE
  static uint64_t IntegerPower(uint64_t base, uint64_t exp) {
    // right to left O(logn) power
    uint64_t pow = 1;
    while (exp) {
      pow *= (exp & 1) ? base : 1;
      base *= base;
      exp >>= 1;
    }
    return pow;
  }

  template <typename T>
  static enable_if_integer<T> Call(KernelContext*, T base, T exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    }
    return static_cast<T>(IntegerPower(base, exp));
  }

  template <typename T>
  static enable_if_floating_point<T> Call(KernelContext*, T base, T exp, Status*) {
    return std::pow(base, exp);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

struct PowerChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status* st) {
    if (exp < 0) {
      *st = Status::Invalid("integers to negative integer powers are not allowed");
      return 0;
    } else if (exp == 0) {
      return 1;
    }
    // left to right O(logn) power with overflow checks
    bool overflow = false;
    uint64_t bitmask =
        1ULL << (63 - BitUtil::CountLeadingZeros(static_cast<uint64_t>(exp)));
    T pow = 1;
    while (bitmask) {
      overflow |= MultiplyWithOverflow(pow, pow, &pow);
      if (exp & bitmask) {
        overflow |= MultiplyWithOverflow(pow, base, &pow);
      }
      bitmask >>= 1;
    }
    if (overflow) {
      *st = Status::Invalid("overflow");
    }
    return pow;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status*) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return std::pow(base, exp);
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_decimal<T> Call(KernelContext*, Arg0 base, Arg1 exp, Status* st) {
    *st = Status::NotImplemented("NYI");
    return T();
  }
};

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec ArithmeticExecFromOp(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return KernelGenerator<Int8Type, Int8Type, Op>::Exec;
    case Type::UINT8:
      return KernelGenerator<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
      return KernelGenerator<Int16Type, Int16Type, Op>::Exec;
    case Type::UINT16:
      return KernelGenerator<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
      return KernelGenerator<Int32Type, Int32Type, Op>::Exec;
    case Type::UINT32:
      return KernelGenerator<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
    case Type::TIMESTAMP:
      return KernelGenerator<Int64Type, Int64Type, Op>::Exec;
    case Type::UINT64:
      return KernelGenerator<UInt64Type, UInt64Type, Op>::Exec;
    case Type::FLOAT:
      return KernelGenerator<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return KernelGenerator<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

// calculate output precision/scale and args rescaling per operation type
Result<std::shared_ptr<DataType>> GetDecimalBinaryOutput(
    const std::string& op, const std::vector<ValueDescr>& values,
    std::vector<std::shared_ptr<DataType>>* replaced = nullptr) {
  const auto& left_type = checked_pointer_cast<DecimalType>(values[0].type);
  const auto& right_type = checked_pointer_cast<DecimalType>(values[1].type);

  const int32_t p1 = left_type->precision(), s1 = left_type->scale();
  const int32_t p2 = right_type->precision(), s2 = right_type->scale();
  if (s1 < 0 || s2 < 0) {
    return Status::NotImplemented("Decimals with negative scales not supported");
  }

  int32_t out_prec, out_scale;
  int32_t left_scaleup = 0, right_scaleup = 0;

  // decimal upscaling behaviour references amazon redshift
  // https://docs.aws.amazon.com/redshift/latest/dg/r_numeric_computations201.html
  if (op.find("add") == 0 || op.find("subtract") == 0) {
    out_scale = std::max(s1, s2);
    out_prec = std::max(p1 - s1, p2 - s2) + 1 + out_scale;
    left_scaleup = out_scale - s1;
    right_scaleup = out_scale - s2;
  } else if (op.find("multiply") == 0) {
    out_scale = s1 + s2;
    out_prec = p1 + p2 + 1;
  } else if (op.find("divide") == 0) {
    out_scale = std::max(4, s1 + p2 - s2 + 1);
    out_prec = p1 - s1 + s2 + out_scale;  // >= p1 + p2 + 1
    left_scaleup = out_prec - p1;
  } else {
    return Status::Invalid("Invalid decimal operation: ", op);
  }

  const auto id = left_type->id();
  auto make = [id](int32_t precision, int32_t scale) {
    if (id == Type::DECIMAL128) {
      return Decimal128Type::Make(precision, scale);
    } else {
      return Decimal256Type::Make(precision, scale);
    }
  };

  if (replaced) {
    replaced->resize(2);
    ARROW_ASSIGN_OR_RAISE((*replaced)[0], make(p1 + left_scaleup, s1 + left_scaleup));
    ARROW_ASSIGN_OR_RAISE((*replaced)[1], make(p2 + right_scaleup, s2 + right_scaleup));
  }

  return make(out_prec, out_scale);
}

struct ArithmeticFunction : ScalarFunction {
  using ScalarFunction::ScalarFunction;

  Result<const Kernel*> DispatchBest(std::vector<ValueDescr>* values) const override {
    RETURN_NOT_OK(CheckArity(*values));

    const auto type_id = (*values)[0].type->id();
    if (type_id == Type::DECIMAL128 || type_id == Type::DECIMAL256) {
      return DispatchDecimal(values);
    }

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;

    EnsureDictionaryDecoded(values);

    // Only promote types for binary functions
    if (values->size() == 2) {
      ReplaceNullWithOtherType(values);

      if (auto type = CommonNumeric(*values)) {
        ReplaceTypes(type, values);
      }
    }

    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }

  Result<const Kernel*> DispatchDecimal(std::vector<ValueDescr>* values) const {
    if (values->size() == 2) {
      std::vector<std::shared_ptr<DataType>> replaced;
      RETURN_NOT_OK(GetDecimalBinaryOutput(name(), *values, &replaced));
      (*values)[0].type = std::move(replaced[0]);
      (*values)[1].type = std::move(replaced[1]);
    }

    using arrow::compute::detail::DispatchExactImpl;
    if (auto kernel = DispatchExactImpl(this, *values)) return kernel;
    return arrow::compute::detail::NoMatchingKernel(this, *values);
  }
};

// resolve decimal operation output type
struct DecimalBinaryOutputResolver {
  std::string func_name;

  DecimalBinaryOutputResolver(std::string func_name) : func_name(std::move(func_name)) {}

  Result<ValueDescr> operator()(KernelContext*, const std::vector<ValueDescr>& args) {
    ARROW_ASSIGN_OR_RAISE(auto out_type, GetDecimalBinaryOutput(func_name, args));
    return ValueDescr(std::move(out_type));
  }
};

template <typename Op>
void AddDecimalBinaryKernels(const std::string& name,
                             std::shared_ptr<ArithmeticFunction>* func) {
  auto out_type = OutputType(DecimalBinaryOutputResolver(name));
  auto in_type128 = InputType(Type::DECIMAL128);
  auto in_type256 = InputType(Type::DECIMAL256);
  auto exec128 = ScalarBinaryNotNullEqualTypes<Decimal128Type, Decimal128Type, Op>::Exec;
  auto exec256 = ScalarBinaryNotNullEqualTypes<Decimal256Type, Decimal256Type, Op>::Exec;
  DCHECK_OK((*func)->AddKernel({in_type128, in_type128}, out_type, exec128));
  DCHECK_OK((*func)->AddKernel({in_type256, in_type256}, out_type, exec256));
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunction(std::string name,
                                                       const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddDecimalBinaryKernels<Op>(name, &func);

  return func;
}

// Like MakeArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionNotNull(std::string name,
                                                              const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Binary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  AddDecimalBinaryKernels<Op>(name, &func);

  return func;
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunction(std::string name,
                                                            const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnary, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  return func;
}

// Like MakeUnaryArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnaryArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
    DCHECK_OK(func->AddKernel({ty}, ty, exec));
  }
  return func;
}

// Like MakeUnaryArithmeticFunction, but for signed arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeUnarySignedArithmeticFunctionNotNull(
    std::string name, const FunctionDoc* doc) {
  auto func = std::make_shared<ArithmeticFunction>(name, Arity::Unary(), doc);
  for (const auto& ty : NumericTypes()) {
    if (!arrow::is_unsigned_integer(ty->id())) {
      auto exec = ArithmeticExecFromOp<ScalarUnaryNotNull, Op>(ty);
      DCHECK_OK(func->AddKernel({ty}, ty, exec));
    }
  }
  return func;
}

const FunctionDoc absolute_value_doc{
    "Calculate the absolute value of the argument element-wise",
    ("Results will wrap around on integer overflow.\n"
     "Use function \"abs_checked\" if you want overflow\n"
     "to return an error."),
    {"x"}};

const FunctionDoc absolute_value_checked_doc{
    "Calculate the absolute value of the argument element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"abs\"."),
    {"x"}};

const FunctionDoc add_doc{"Add the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"add_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc add_checked_doc{
    "Add the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"add\"."),
    {"x", "y"}};

const FunctionDoc sub_doc{"Subtract the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"subtract_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc sub_checked_doc{
    "Subtract the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"subtract\"."),
    {"x", "y"}};

const FunctionDoc mul_doc{"Multiply the arguments element-wise",
                          ("Results will wrap around on integer overflow.\n"
                           "Use function \"multiply_checked\" if you want overflow\n"
                           "to return an error."),
                          {"x", "y"}};

const FunctionDoc mul_checked_doc{
    "Multiply the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"multiply\"."),
    {"x", "y"}};

const FunctionDoc div_doc{
    "Divide the arguments element-wise",
    ("Integer division by zero returns an error. However, integer overflow\n"
     "wraps around, and floating-point division by zero returns an infinite.\n"
     "Use function \"divide_checked\" if you want to get an error\n"
     "in all the aforementioned cases."),
    {"dividend", "divisor"}};

const FunctionDoc div_checked_doc{
    "Divide the arguments element-wise",
    ("An error is returned when trying to divide by zero, or when\n"
     "integer overflow is encountered."),
    {"dividend", "divisor"}};

const FunctionDoc negate_doc{"Negate the argument element-wise",
                             ("Results will wrap around on integer overflow.\n"
                              "Use function \"negate_checked\" if you want overflow\n"
                              "to return an error."),
                             {"x"}};

const FunctionDoc negate_checked_doc{
    "Negate the arguments element-wise",
    ("This function returns an error on overflow.  For a variant that\n"
     "doesn't fail on overflow, use function \"negate\"."),
    {"x"}};

const FunctionDoc pow_doc{
    "Raise arguments to power element-wise",
    ("Integer to negative integer power returns an error. However, integer overflow\n"
     "wraps around. If either base or exponent is null the result will be null."),
    {"base", "exponent"}};

const FunctionDoc pow_checked_doc{
    "Raise arguments to power element-wise",
    ("An error is returned when integer to negative integer power is encountered,\n"
     "or integer overflow is encountered."),
    {"base", "exponent"}};
}  // namespace

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  // ----------------------------------------------------------------------
  auto absolute_value =
      MakeUnaryArithmeticFunction<AbsoluteValue>("abs", &absolute_value_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value)));

  // ----------------------------------------------------------------------
  auto absolute_value_checked = MakeUnaryArithmeticFunctionNotNull<AbsoluteValueChecked>(
      "abs_checked", &absolute_value_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(absolute_value_checked)));

  // ----------------------------------------------------------------------
  auto add = MakeArithmeticFunction<Add>("add", &add_doc);
  DCHECK_OK(registry->AddFunction(std::move(add)));

  // ----------------------------------------------------------------------
  auto add_checked =
      MakeArithmeticFunctionNotNull<AddChecked>("add_checked", &add_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(add_checked)));

  // ----------------------------------------------------------------------
  // subtract
  auto subtract = MakeArithmeticFunction<Subtract>("subtract", &sub_doc);

  // Add subtract(timestamp, timestamp) -> duration
  for (auto unit : AllTimeUnits()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec = ArithmeticExecFromOp<ScalarBinaryEqualTypes, Subtract>(Type::TIMESTAMP);
    DCHECK_OK(subtract->AddKernel({in_type, in_type}, duration(unit), std::move(exec)));
  }

  DCHECK_OK(registry->AddFunction(std::move(subtract)));

  // ----------------------------------------------------------------------
  auto subtract_checked = MakeArithmeticFunctionNotNull<SubtractChecked>(
      "subtract_checked", &sub_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(subtract_checked)));

  // ----------------------------------------------------------------------
  auto multiply = MakeArithmeticFunction<Multiply>("multiply", &mul_doc);
  DCHECK_OK(registry->AddFunction(std::move(multiply)));

  // ----------------------------------------------------------------------
  auto multiply_checked = MakeArithmeticFunctionNotNull<MultiplyChecked>(
      "multiply_checked", &mul_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(multiply_checked)));

  // ----------------------------------------------------------------------
  auto divide = MakeArithmeticFunctionNotNull<Divide>("divide", &div_doc);
  DCHECK_OK(registry->AddFunction(std::move(divide)));

  // ----------------------------------------------------------------------
  auto divide_checked =
      MakeArithmeticFunctionNotNull<DivideChecked>("divide_checked", &div_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(divide_checked)));

  // ----------------------------------------------------------------------
  auto negate = MakeUnaryArithmeticFunction<Negate>("negate", &negate_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate)));

  // ----------------------------------------------------------------------
  auto negate_checked = MakeUnarySignedArithmeticFunctionNotNull<NegateChecked>(
      "negate_checked", &negate_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(negate_checked)));

  // ----------------------------------------------------------------------
  auto power = MakeArithmeticFunction<Power>("power", &pow_doc);
  DCHECK_OK(registry->AddFunction(std::move(power)));

  // ----------------------------------------------------------------------
  auto power_checked =
      MakeArithmeticFunctionNotNull<PowerChecked>("power_checked", &pow_checked_doc);
  DCHECK_OK(registry->AddFunction(std::move(power_checked)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
