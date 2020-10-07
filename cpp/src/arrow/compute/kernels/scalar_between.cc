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

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::checked_cast;
using internal::checked_pointer_cast;
using util::string_view;

namespace compute {
namespace internal {

namespace {

struct Between {
  template <typename T, typename Arg0, typename Arg1, typename Arg2>
  static constexpr T Call(KernelContext*, const Arg0& value, const Arg1& left,
                             const Arg2& right, Status*) {
    static_assert(std::is_same<T, bool>::value && 
		  std::is_same<Arg0, Arg1>::value &&
		  std::is_same<Arg1, Arg2>::value, 
		  "");
    return (left < value) && (value < right);
  }
};

template <typename Op>
void AddIntegerBetween(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
  auto exec =
      GeneratePhysicalInteger<applicator::ScalarTernaryEqualTypes, BooleanType, Op>(*ty);
  DCHECK_OK(func->AddKernel({ty, ty, ty}, boolean(), std::move(exec)));
/*
  DCHECK_OK(func->AddKernel(
      {InputType::Array(ty), InputType::Scalar(ty), InputType::Scalar(ty)}, boolean(),
      std::move(exec)));
  DCHECK_OK(func->AddKernel(
       {InputType::Array(ty), InputType::Array(ty), InputType::Array(ty)}, boolean(), 
       std::move(exec)));
*/
}

template <typename InType, typename Op>
void AddGenericBetween(const std::shared_ptr<DataType>& ty, ScalarFunction* func) {
   DCHECK_OK(
      func->AddKernel({ty, ty, ty}, boolean(),
	       applicator::ScalarTernaryEqualTypes<BooleanType, InType, Op>::Exec));

/*  DCHECK_OK(func->AddKernel(
      {InputType::Array(ty), InputType::Scalar(ty), InputType::Scalar(ty)}, boolean(),
      applicator::ScalarTernaryEqualTypes<BooleanType, InType, Op>::Exec));
  DCHECK_OK(
      func->AddKernel({InputType::Array(ty), InputType::Array(ty), InputType::Array(ty)},
                      boolean(),
		      applicator::ScalarTernaryEqualTypes<BooleanType, InType, Op>::Exec));
*/
}

template <typename Op>
std::shared_ptr<ScalarFunction> MakeBetweenFunction(std::string name, 
		const FunctionDoc* doc) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Ternary(), doc);

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

const FunctionDoc between_doc{"Check if values are in a range x <= y <= z",
                             ("A null on either side emits a null comparison result."),
                             {"x", "y", "z"}};

}  // namespace

void RegisterScalarBetween(FunctionRegistry* registry) {
  auto between =
	  MakeBetweenFunction<Between>("between", &between_doc);
  DCHECK_OK(registry->AddFunction(std::move(between)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
