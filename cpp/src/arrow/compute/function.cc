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

#include "arrow/compute/function.h"

#include <cstddef>
#include <memory>
#include <sstream>

namespace arrow {

struct ValueDescr;

namespace compute {

static Status CheckArity(const std::vector<InputType>& args, const FunctionArity& arity) {
  const int passed_num_args = static_cast<int>(args.size());
  if (arity.is_varargs && passed_num_args < arity.num_args) {
    return Status::Invalid("Varargs function needs at least ", arity.num_args,
                           " arguments but kernel accepts only ", passed_num_args);
  } else if (!arity.is_varargs && passed_num_args != arity.num_args) {
    return Status::Invalid("Function accepts ", arity.num_args,
                           " arguments but kernel accepts ", passed_num_args);
  }
  return Status::OK();
}

template <typename DescrType>
std::string FormatArgTypes(const std::vector<DescrType>& descrs) {
  std::stringstream ss;
  ss << "(";
  for (size_t i = 0; i < descrs.size(); ++i) {
    if (i > 0) {
      ss << ", ";
    }
    ss << descrs[i].ToString();
  }
  ss << ")";
  return ss.str();
}

template <typename KernelType, typename DescrType>
Result<const KernelType*> DispatchExactImpl(const Function& func,
                                            const std::vector<KernelType>& kernels,
                                            const std::vector<DescrType>& values) {
  const int passed_num_args = static_cast<int>(values.size());

  // Validate arity
  const FunctionArity arity = func.arity();
  if (arity.is_varargs && passed_num_args < arity.num_args) {
    return Status::Invalid("Varargs function needs at least ", arity.num_args,
                           " arguments but passed only ", passed_num_args);
  } else if (!arity.is_varargs && passed_num_args != arity.num_args) {
    return Status::Invalid("Function accepts ", arity.num_args, " arguments but passed ",
                           passed_num_args);
  }
  for (const auto& kernel : kernels) {
    if (kernel.signature->MatchesInputs(values)) {
      return &kernel;
    }
  }
  return Status::KeyError("Function ", func.name(),
                          " has no kernel exactly matching input types ",
                          FormatArgTypes(values));
}

Status ScalarFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types, arity_));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("Varargs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status ScalarFunction::AddKernel(ScalarKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types(), arity_));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Result<const ScalarKernel*> ScalarFunction::DispatchExact(
    const std::vector<ValueDescr>& values) const {
  return DispatchExactImpl(*this, kernels_, values);
}

Status VectorFunction::AddKernel(std::vector<InputType> in_types, OutputType out_type,
                                 ArrayKernelExec exec, KernelInit init) {
  RETURN_NOT_OK(CheckArity(in_types, arity_));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("Varargs signatures must have exactly one input type");
  }
  auto sig =
      KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();
}

Status VectorFunction::AddKernel(VectorKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types(), arity_));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Result<const VectorKernel*> VectorFunction::DispatchExact(
    const std::vector<ValueDescr>& values) const {
  return DispatchExactImpl(*this, kernels_, values);
}

Status ScalarAggregateFunction::AddKernel(ScalarAggregateKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types(), arity_));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}

Result<const ScalarAggregateKernel*> ScalarAggregateFunction::DispatchExact(
    const std::vector<ValueDescr>& values) const {
  return DispatchExactImpl(*this, kernels_, values);
}

}  // namespace compute
}  // namespace arrow
