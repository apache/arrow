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

// Implement Less, LessEqual by flipping arguments to Greater, GreaterEqual

template <typename Op>
void AddIntegerCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  auto exec =
      GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
  DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
}

template <typename InType, typename Op>
void AddGenericCompare(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  DCHECK_OK(
      func->AddKernel({ty, ty}, boolean(),
                      applicator::ScalarBinaryEqualTypes<BooleanType, InType, Op>::Exec));
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeCompareFunction(std::string name) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());

  DCHECK_OK(func->AddKernel(
      {boolean(), boolean()}, boolean(),
      applicator::ScalarBinary<BooleanType, BooleanType, BooleanType, Op>::Exec));

  for (const std::shared_ptr<DataType>& ty : IntTypes()) {
    AddIntegerCompare<Op>(ty, func.get());
  }
  AddIntegerCompare<Op>(date32(), func.get());
  AddIntegerCompare<Op>(date64(), func.get());

  AddGenericCompare<FloatType, Op>(float32(), func.get());
  AddGenericCompare<DoubleType, Op>(float64(), func.get());

  // Add timestamp kernels
  for (auto unit : AllTimeUnits()) {
    InputType in_type(match::TimestampTypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }

  // Duration
  for (auto unit : AllTimeUnits()) {
    InputType in_type(match::DurationTypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }

  // Time32 and Time64
  for (auto unit : {TimeUnit::SECOND, TimeUnit::MILLI}) {
    InputType in_type(match::Time32TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int32());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }
  for (auto unit : {TimeUnit::MICRO, TimeUnit::NANO}) {
    InputType in_type(match::Time64TypeUnit(unit));
    auto exec =
        GeneratePhysicalInteger<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(
            int64());
    DCHECK_OK(func->AddKernel({in_type, in_type}, boolean(), std::move(exec)));
  }

  for (const std::shared_ptr<DataType>& ty : BaseBinaryTypes()) {
    auto exec =
        GenerateVarBinaryBase<applicator::ScalarBinaryEqualTypes, BooleanType, Op>(*ty);
    DCHECK_OK(func->AddKernel({ty, ty}, boolean(), std::move(exec)));
  }

  return func;
}

std::shared_ptr<ScalarFunction> MakeFlippedFunction(std::string name,
                                                    const ScalarFunction& func) {
  auto flipped_func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const ScalarKernel* kernel : func.kernels()) {
    ScalarKernel flipped_kernel = *kernel;
    flipped_kernel.exec = MakeFlippedBinaryExec(kernel->exec);
    DCHECK_OK(flipped_func->AddKernel(std::move(flipped_kernel)));
  }
  return flipped_func;
}

}  // namespace

void RegisterScalarComparison(FunctionRegistry* registry) {
  DCHECK_OK(registry->AddFunction(MakeCompareFunction<Equal>("equal")));
  DCHECK_OK(registry->AddFunction(MakeCompareFunction<NotEqual>("not_equal")));

  auto greater = MakeCompareFunction<Greater>("greater");
  auto greater_equal = MakeCompareFunction<GreaterEqual>("greater_equal");

  auto less = MakeFlippedFunction("less", *greater);
  auto less_equal = MakeFlippedFunction("less_equal", *greater_equal);
  DCHECK_OK(registry->AddFunction(std::move(less)));
  DCHECK_OK(registry->AddFunction(std::move(less_equal)));
  DCHECK_OK(registry->AddFunction(std::move(greater)));
  DCHECK_OK(registry->AddFunction(std::move(greater_equal)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
