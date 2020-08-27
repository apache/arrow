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
#include "arrow/util/int_util_internal.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::DivideWithOverflow;
using internal::MultiplyWithOverflow;
using internal::SubtractWithOverflow;

namespace compute {
namespace internal {

using applicator::ScalarBinaryEqualTypes;
using applicator::ScalarBinaryNotNullEqualTypes;

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

template <typename T, typename Unsigned = typename std::make_unsigned<T>::type>
constexpr Unsigned to_unsigned(T signed_) {
  return static_cast<Unsigned>(signed_);
}

struct Add {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right) {
    return left + right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right) {
    return arrow::internal::SafeSignedAdd(left, right);
  }
};

struct AddChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(AddWithOverflow(left, right, &result))) {
      ctx->SetStatus(Status::Invalid("overflow"));
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left + right;
  }
};

struct Subtract {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right) {
    return left - right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right) {
    return arrow::internal::SafeSignedSubtract(left, right);
  }
};

struct SubtractChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(SubtractWithOverflow(left, right, &result))) {
      ctx->SetStatus(Status::Invalid("overflow"));
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left - right;
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
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_unsigned_integer<T> Call(KernelContext*, T left, T right) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right) {
    return to_unsigned(left) * to_unsigned(right);
  }

  // Multiplication of 16 bit integer types implicitly promotes to signed 32 bit
  // integer. However, some inputs may nevertheless overflow (which triggers undefined
  // behaviour). Therefore we first cast to 32 bit unsigned integers where overflow is
  // well defined.
  template <typename T = void>
  static constexpr int16_t Call(KernelContext*, int16_t left, int16_t right) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
  template <typename T = void>
  static constexpr uint16_t Call(KernelContext*, uint16_t left, uint16_t right) {
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }
};

struct MultiplyChecked {
  template <typename T, typename Arg0, typename Arg1>
  enable_if_integer<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result = 0;
    if (ARROW_PREDICT_FALSE(MultiplyWithOverflow(left, right, &result))) {
      ctx->SetStatus(Status::Invalid("overflow"));
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  enable_if_floating_point<T> Call(KernelContext*, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    return left * right;
  }
};

struct Divide {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    return left / right;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        ctx->SetStatus(Status::Invalid("divide by zero"));
      } else {
        result = 0;
      }
    }
    return result;
  }
};

struct DivideChecked {
  template <typename T, typename Arg0, typename Arg1>
  static enable_if_integer<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    T result;
    if (ARROW_PREDICT_FALSE(DivideWithOverflow(left, right, &result))) {
      if (right == 0) {
        ctx->SetStatus(Status::Invalid("divide by zero"));
      } else {
        ctx->SetStatus(Status::Invalid("overflow"));
      }
    }
    return result;
  }

  template <typename T, typename Arg0, typename Arg1>
  static enable_if_floating_point<T> Call(KernelContext* ctx, Arg0 left, Arg1 right) {
    static_assert(std::is_same<T, Arg0>::value && std::is_same<T, Arg1>::value, "");
    if (ARROW_PREDICT_FALSE(right == 0)) {
      ctx->SetStatus(Status::Invalid("divide by zero"));
      return 0;
    }
    return left / right;
  }
};

// Generate a kernel given an arithmetic functor
template <template <typename... Args> class KernelGenerator, typename Op>
ArrayKernelExec NumericEqualTypesBinary(detail::GetTypeId get_id) {
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

template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunction(std::string name) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const auto& ty : NumericTypes()) {
    auto exec = NumericEqualTypesBinary<ScalarBinaryEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  return func;
}

// Like MakeArithmeticFunction, but for arithmetic ops that need to run
// only on non-null output.
template <typename Op>
std::shared_ptr<ScalarFunction> MakeArithmeticFunctionNotNull(std::string name) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const auto& ty : NumericTypes()) {
    auto exec = NumericEqualTypesBinary<ScalarBinaryNotNullEqualTypes, Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  return func;
}

}  // namespace

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  // ----------------------------------------------------------------------
  auto add = MakeArithmeticFunction<Add>("add");
  DCHECK_OK(registry->AddFunction(std::move(add)));

  // ----------------------------------------------------------------------
  auto add_checked = MakeArithmeticFunctionNotNull<AddChecked>("add_checked");
  DCHECK_OK(registry->AddFunction(std::move(add_checked)));

  // ----------------------------------------------------------------------
  // subtract
  auto subtract = MakeArithmeticFunction<Subtract>("subtract");

  // Add subtract(timestamp, timestamp) -> duration
  for (auto unit : AllTimeUnits()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec =
        NumericEqualTypesBinary<ScalarBinaryEqualTypes, Subtract>(Type::TIMESTAMP);
    DCHECK_OK(subtract->AddKernel({in_type, in_type}, duration(unit), std::move(exec)));
  }

  DCHECK_OK(registry->AddFunction(std::move(subtract)));

  // ----------------------------------------------------------------------
  auto subtract_checked =
      MakeArithmeticFunctionNotNull<SubtractChecked>("subtract_checked");
  DCHECK_OK(registry->AddFunction(std::move(subtract_checked)));

  // ----------------------------------------------------------------------
  auto multiply = MakeArithmeticFunction<Multiply>("multiply");
  DCHECK_OK(registry->AddFunction(std::move(multiply)));

  // ----------------------------------------------------------------------
  auto multiply_checked =
      MakeArithmeticFunctionNotNull<MultiplyChecked>("multiply_checked");
  DCHECK_OK(registry->AddFunction(std::move(multiply_checked)));

  // ----------------------------------------------------------------------
  auto divide = MakeArithmeticFunctionNotNull<Divide>("divide");
  DCHECK_OK(registry->AddFunction(std::move(divide)));

  // ----------------------------------------------------------------------
  auto divide_checked = MakeArithmeticFunctionNotNull<DivideChecked>("divide_checked");
  DCHECK_OK(registry->AddFunction(std::move(divide_checked)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
