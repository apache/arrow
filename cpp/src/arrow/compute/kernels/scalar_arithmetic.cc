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

namespace arrow {
namespace compute {

struct Add {
  template <typename OUT = int16_t>
  static constexpr OUT Call(KernelContext*, int8_t l, int8_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT = int32_t>
  static constexpr OUT Call(KernelContext*, int16_t l, int16_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT = int64_t>
  static constexpr OUT Call(KernelContext*, int32_t l, int32_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT = uint16_t>
  static constexpr OUT Call(KernelContext*, uint8_t l, uint8_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT = uint32_t>
  static constexpr OUT Call(KernelContext*, uint16_t l, uint16_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT = uint64_t>
  static constexpr OUT Call(KernelContext*, uint32_t l, uint32_t r) {
    return static_cast<OUT>(l) + static_cast<OUT>(r);
  }

  template <typename OUT>
  static constexpr OUT Call(KernelContext*, OUT l, OUT r) {
    return l + r;
  }
};

namespace codegen {

template <typename Op, typename ArgType, typename OutType>
void AddBinaryKernel(const std::shared_ptr<ScalarFunction>& func) {
  // create an exec function with the requested signature
  ArrayKernelExec exec = ScalarBinaryEqualTypes<OutType, ArgType, Op>::Exec;
  // create type objects based on the template arguments
  auto arg = TypeTraits<ArgType>::type_singleton();
  auto out = TypeTraits<OutType>::type_singleton();
  // add the exec function as a kernel with the appropiate signature
  DCHECK_OK(func->AddKernel({arg, arg}, out, exec));
}

template <typename Op>
void AddBinaryFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());

  // signed integers
  AddBinaryKernel<Op, Int8Type, Int16Type>(func);
  AddBinaryKernel<Op, Int16Type, Int32Type>(func);
  AddBinaryKernel<Op, Int32Type, Int64Type>(func);
  AddBinaryKernel<Op, Int64Type, Int64Type>(func);
  // unsigned integers
  AddBinaryKernel<Op, UInt8Type, UInt16Type>(func);
  AddBinaryKernel<Op, UInt16Type, UInt32Type>(func);
  AddBinaryKernel<Op, UInt32Type, UInt64Type>(func);
  AddBinaryKernel<Op, UInt64Type, UInt64Type>(func);
  // floating-point types, TODO(kszucs): add half-float
  AddBinaryKernel<Op, FloatType, FloatType>(func);
  AddBinaryKernel<Op, DoubleType, DoubleType>(func);

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace codegen

namespace internal {

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  codegen::AddBinaryFunction<Add>("add", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
