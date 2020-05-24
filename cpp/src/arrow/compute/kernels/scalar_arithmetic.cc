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
  template <typename OUT, typename ARG0, typename ARG1>
  static constexpr OUT Call(KernelContext*, ARG0 left, ARG1 right) {
    return left + right;
  }
};

namespace codegen {

template <typename Op>
void MakeBinaryFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const std::shared_ptr<DataType>& ty : NumericTypes()) {
    DCHECK_OK(func->AddKernel({InputType::Array(ty), InputType::Array(ty)}, ty,
                              NumericEqualTypesBinary<Op>(*ty)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace codegen

namespace internal {

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  codegen::MakeBinaryFunction<Add>("add", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
