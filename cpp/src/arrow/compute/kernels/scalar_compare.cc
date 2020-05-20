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

template <typename Op, typename FlippedOp = Op>
void MakeCompareFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, /*arity=*/2);

  auto out_ty = boolean();
  DCHECK_OK(func->AddKernel(
      {boolean(), boolean()}, out_ty,
      codegen::ScalarBinary<BooleanType, BooleanType, BooleanType, Op, FlippedOp>::Exec));

  for (const std::shared_ptr<DataType>& ty : NumericTypes()) {
    auto exec =
        codegen::Numeric<codegen::ScalarBinaryEqualTypes, BooleanType, Op, FlippedOp>(
            *ty);
    DCHECK_OK(func->AddKernel({ty, ty}, out_ty, exec));
  }
  for (const std::shared_ptr<DataType>& ty : TemporalTypes()) {
    auto exec =
        codegen::Temporal<codegen::ScalarBinaryEqualTypes, BooleanType, Op, FlippedOp>(
            *ty);
    DCHECK_OK(func->AddKernel({ty, ty}, out_ty, exec));
  }
  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        codegen::BaseBinary<codegen::ScalarBinaryEqualTypes, BooleanType, Op, FlippedOp>(
            *ty);
    DCHECK_OK(func->AddKernel({ty, ty}, out_ty, exec));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterScalarComparison(FunctionRegistry* registry) {
  MakeCompareFunction<Equal>("==", registry);
  MakeCompareFunction<NotEqual>("!=", registry);
  MakeCompareFunction<Less, Greater>("<", registry);
  MakeCompareFunction<LessEqual, GreaterEqual>("<=", registry);
  MakeCompareFunction<Greater, Less>(">", registry);
  MakeCompareFunction<GreaterEqual, LessEqual>(">=", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
