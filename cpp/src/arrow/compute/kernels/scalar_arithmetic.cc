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
ArrayKernelExec GetBinaryExec(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return ScalarPrimitiveExecBinary<Op, Int16Type, Int8Type, Int8Type>;
    case Type::UINT8:
      return ScalarPrimitiveExecBinary<Op, UInt16Type, UInt8Type, UInt8Type>;
    case Type::INT16:
      return ScalarPrimitiveExecBinary<Op, Int32Type, Int16Type, Int16Type>;
    case Type::UINT16:
      return ScalarPrimitiveExecBinary<Op, UInt32Type, UInt16Type, UInt16Type>;
    case Type::INT32:
      return ScalarPrimitiveExecBinary<Op, Int64Type, Int32Type, Int32Type>;
    case Type::UINT32:
      return ScalarPrimitiveExecBinary<Op, UInt64Type, UInt32Type, UInt32Type>;
    case Type::INT64:
      return ScalarPrimitiveExecBinary<Op, Int64Type, Int64Type, Int64Type>;
    case Type::UINT64:
      return ScalarPrimitiveExecBinary<Op, UInt64Type, UInt64Type, UInt64Type>;
    case Type::FLOAT:
      return ScalarPrimitiveExecBinary<Op, FloatType, FloatType, FloatType>;
    case Type::DOUBLE:
      return ScalarPrimitiveExecBinary<Op, DoubleType, DoubleType, DoubleType>;
    default:
      DCHECK(false);
      return ExecFail;
  }
}

std::shared_ptr<DataType> GetOutputType(detail::GetTypeId get_id) {
  switch (get_id.id) {
    case Type::INT8:
      return int16();
    case Type::INT16:
      return int32();
    case Type::INT32:
    case Type::INT64:
      return int64();
    case Type::UINT8:
      return uint16();
    case Type::UINT16:
      return uint32();
    case Type::UINT32:
    case Type::UINT64:
      return uint64();
    case Type::FLOAT:
      return float32();
    case Type::DOUBLE:
      return float64();
    default:
      DCHECK(false);
      return nullptr;
  }
}

template <typename Op>
void MakeBinaryFunction(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());
  for (const std::shared_ptr<DataType>& input_type : NumericTypes()) {
    DCHECK_OK(
      func->AddKernel(
        {InputType::Array(input_type), InputType::Array(input_type)},
        GetOutputType(input_type),
        GetBinaryExec<Op>(*input_type)
      )
    );
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
