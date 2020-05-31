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

#include <algorithm>
#include <cctype>
#include <string>

#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_string_internal.h"

namespace arrow {
namespace compute {
namespace internal {

// TODO: optional ascii validation

struct AsciiLength {
  template <typename OUT, typename ARG0 = util::string_view>
  static OUT Call(KernelContext*, ARG0 val) {
    return static_cast<OUT>(val.size());
  }
};

struct AsciiUpper {
  // XXX: the Scalar codegen path passes template arguments that are unused
  template <typename... Ignored>
  static std::string Call(KernelContext*, const util::string_view& val) {
    std::string result = val.to_string();
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return result;
  }
};

void AddAsciiLength(FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>("ascii_length", Arity::Unary());
  ArrayKernelExec exec_offset_32 =
      codegen::ScalarUnaryNotNull<Int32Type, StringType, AsciiLength>::Exec;
  ArrayKernelExec exec_offset_64 =
      codegen::ScalarUnaryNotNull<Int64Type, LargeStringType, AsciiLength>::Exec;
  DCHECK_OK(func->AddKernel({utf8()}, int32(), exec_offset_32));
  DCHECK_OK(func->AddKernel({large_utf8()}, int64(), exec_offset_64));
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

void RegisterScalarStringAscii(FunctionRegistry* registry) {
  MakeUnaryStringToString<AsciiUpper>("ascii_upper", registry);
  AddAsciiLength(registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
