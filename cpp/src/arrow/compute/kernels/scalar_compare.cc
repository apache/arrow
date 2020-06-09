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

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace compute {
namespace internal {

struct Equal {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left == right;
  }
};

struct NotEqual {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left != right;
  }
};

struct Greater {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left > right;
  }
};

struct GreaterEqual {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left >= right;
  }
};

struct Less {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left < right;
  }
};

struct LessEqual {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& left, const T& right) {
    return left <= right;
  }
};

template <typename InType, typename Op>
void AddCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  ArrayKernelExec exec = codegen::ScalarBinaryEqualTypes<BooleanType, InType, Op>::Exec;
  DCHECK_OK(func->AddKernel({ty, ty}, boolean(), exec));
}

template <typename Op>
void AddTimestampComparisons(ScalarFunction* func) {
  ArrayKernelExec exec =
      codegen::ScalarBinaryEqualTypes<BooleanType, TimestampType, Op>::Exec;
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI, TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::TimestampUnit(unit));
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), exec));
  }
}

template <typename Op>
void MakeCompareFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());

  DCHECK_OK(func->AddKernel(
      {boolean(), boolean()}, boolean(),
      codegen::ScalarBinary<BooleanType, BooleanType, BooleanType, Op>::Exec));

  for (const std::shared_ptr<DataType>& ty : NumericTypes()) {
    auto exec = codegen::Numeric<codegen::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), exec));
  }
  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        codegen::BaseBinary<codegen::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), exec));
  }

  // Temporal types requires some care because cross-unit comparisons with
  // everything but DATE32 and DATE64 are not implemented yet
  AddCompare<Date32Type, Op>(date32(), func.get());
  AddCompare<Date64Type, Op>(date64(), func.get());
  AddTimestampComparisons<Op>(func.get());

  // TODO: Leave time32, time64, and duration for follow up work

  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterScalarComparison(FunctionRegistry* registry) {
  MakeCompareFunction<Equal>("equal", registry);
  MakeCompareFunction<NotEqual>("not_equal", registry);
  MakeCompareFunction<Less>("less", registry);
  MakeCompareFunction<LessEqual>("less_equal", registry);
  MakeCompareFunction<Greater>("greater", registry);
  MakeCompareFunction<GreaterEqual>("greater_equal", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
