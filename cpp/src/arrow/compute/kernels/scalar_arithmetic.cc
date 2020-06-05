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
#include "arrow/util/int_util.h"

namespace arrow {
namespace compute {

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
using enable_if_floating_point = enable_if_t<std::is_floating_point<T>::value, T>;

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
    using Unsigned = typename std::make_unsigned<T>::type;
    return static_cast<Unsigned>(left) + static_cast<Unsigned>(right);
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
    using Unsigned = typename std::make_unsigned<T>::type;
    return static_cast<Unsigned>(left) - static_cast<Unsigned>(right);
  }
};

struct Multiply {
  template <typename T>
  static constexpr enable_if_floating_point<T> Call(KernelContext*, T left, T right) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_t<
      is_unsigned_integer<T>::value && !std::is_same<T, uint16_t>::value, T>
  Call(KernelContext*, T left, T right) {
    return left * right;
  }

  template <typename T>
  static constexpr enable_if_t<std::is_same<T, uint16_t>::value, T> Call(KernelContext*,
                                                                         T left,
                                                                         T right) {
    // exception because multiplying to uint16 values involves implicit promotion
    // to signed int32 type which can trigger undefined behaviour by signed overflow
    return static_cast<uint32_t>(left) * static_cast<uint32_t>(right);
  }

  template <typename T>
  static constexpr enable_if_signed_integer<T> Call(KernelContext*, T left, T right) {
    using Unsigned = typename std::make_unsigned<T>::type;
    return static_cast<Unsigned>(left) * static_cast<Unsigned>(right);
  }
};

namespace codegen {

// Generate a kernel given an arithmetic functor
//
// To avoid undefined behaviour of signed integer overflow treat the signed
// input argument values as unsigned then cast them to signed making them wrap
// around.
template <typename Op>
ArrayKernelExec NumericEqualTypesBinary(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return ScalarBinaryEqualTypes<Int8Type, Int8Type, Op>::Exec;
    case Type::UINT8:
      return ScalarBinaryEqualTypes<UInt8Type, UInt8Type, Op>::Exec;
    case Type::INT16:
      return ScalarBinaryEqualTypes<Int16Type, Int16Type, Op>::Exec;
    case Type::UINT16:
      return ScalarBinaryEqualTypes<UInt16Type, UInt16Type, Op>::Exec;
    case Type::INT32:
      return ScalarBinaryEqualTypes<Int32Type, Int32Type, Op>::Exec;
    case Type::UINT32:
      return ScalarBinaryEqualTypes<UInt32Type, UInt32Type, Op>::Exec;
    case Type::INT64:
      return ScalarBinaryEqualTypes<Int64Type, Int64Type, Op>::Exec;
    case Type::UINT64:
      return ScalarBinaryEqualTypes<UInt64Type, UInt64Type, Op>::Exec;
    case Type::FLOAT:
      return ScalarBinaryEqualTypes<FloatType, FloatType, Op>::Exec;
    case Type::DOUBLE:
      return ScalarBinaryEqualTypes<DoubleType, DoubleType, Op>::Exec;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

template <typename Op>
void AddBinaryFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const auto& ty : NumericTypes()) {
    auto exec = codegen::NumericEqualTypesBinary<Op>(ty);
    DCHECK_OK(func->AddKernel({ty, ty}, ty, exec));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace codegen

namespace internal {

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  codegen::AddBinaryFunction<Add>("add", registry);
  codegen::AddBinaryFunction<Subtract>("subtract", registry);
  codegen::AddBinaryFunction<Multiply>("multiply", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
