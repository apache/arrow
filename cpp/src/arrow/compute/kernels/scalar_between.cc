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

namespace {

struct Between {
  template <typename T>
  static constexpr bool Call(KernelContext*, const T& value, const T& left,
                             const T& right) {
    return (left < value) && (value < right);
  }
};

template <typename Op>
void AddIntegerBetween(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  auto exec =
      GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(*ty);
  DCHECK_OK(func->AddKernel(
      {InputType::Array(ty), InputType::Scalar(ty), InputType::Scalar(ty)}, boolean(),
      exec));
  DCHECK_OK(
      func->AddKernel({InputType::Array(ty), InputType::Array(ty), InputType::Array(ty)},
                      boolean(), std::move(exec)));
}

template <typename InType, typename Op>
void AddGenericBetween(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  auto exec = applicator::ScalarTernaryEqualTypes<BooleanType, InType, Op>::Exec;
  DCHECK_OK(func->AddKernel(
      {InputType::Array(ty), InputType::Scalar(ty), InputType::Scalar(ty)}, boolean(),
      exec));
  DCHECK_OK(
      func->AddKernel({InputType::Array(ty), InputType::Array(ty), InputType::Array(ty)},
                      boolean(), std::move(exec)));
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeBetweenFunction(std::string name) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Ternary());

  for (const std::shared_ptr<DataType>& ty : IntTypes()) {
    AddIntegerBetween<Op>(ty, func.get());
  }
  AddIntegerBetween<Op>(date32(), func.get());
  AddIntegerBetween<Op>(date64(), func.get());

  AddGenericBetween<FloatType, Op>(float32(), func.get());
  AddGenericBetween<DoubleType, Op>(float64(), func.get());
  
  // Add timestamp kernels
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type, in_type}, boolean(), std::move(exec)));
  }

  // Duration
  for (auto unit : TimeUnit::values()) {
    InputType in_type(match::DurationTypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type, in_type}, boolean(), std::move(exec)));
  }

  // Time32 and Time64
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI}) {
    InputType in_type(match::Time32TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(
            int32());
    DCHECK_OK(func->AddKernel({in_type, in_type, in_type}, boolean(), std::move(exec)));
  }
  for (auto unit : {TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::Time64TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type, in_type}, boolean(), std::move(exec)));
  }

  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel(
        {InputType::Array(ty), InputType::Scalar(ty), InputType::Scalar(ty)}, boolean(),
        exec));
    DCHECK_OK(func->AddKernel(
        {InputType::Array(ty), InputType::Array(ty), InputType::Array(ty)}, boolean(),
        std::move(exec)));
  }

  return func;
}

}  // namespace

void RegisterScalarBetween(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(MakeBetweenFunction<Between>("between")));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
