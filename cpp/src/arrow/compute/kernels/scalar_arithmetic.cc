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
  // These execution functions doesn't support operations between multiple shapes,
  // only op(array like, array like) is supported.
  // Note that we don't have plans to add binary kernels with differntly types
  // lhs and rhs arguments, that will be handled with implicit casts during the
  // kernel dispatch procedure.
  //
  // There is a ScalarBinary facility in the codegen_internals.h which already
  // handles those cases. I illustrate my plan MakeBinaryFunction2 by trying to
  // add a single kernel to the function.
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
        func->AddKernel({InputType::Array(input_type), InputType::Array(input_type)},
                        GetOutputType(input_type), GetBinaryExec<Op>(*input_type)));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

// Here is the function which doesn't compile because of type deduction error
template <typename Op>
void MakeBinaryFunction2(std::string name, FunctionRegistry* registry) {
  auto func = std::make_shared<ScalarFunction>(name, Arity::Binary());

  // create an exec function with signature (int8, int8) -> int16
  ArrayKernelExec exec = codegen::ScalarBinaryEqualTypes<Int16Type, Int8Type, Op>::Exec;

  // add this exec function as a kernel with the appropiate signature
  DCHECK_OK(func->AddKernel({int8(), int8()}, int16(), exec));

  // finally register the function, but the
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

}  // namespace codegen

namespace internal {

void RegisterScalarArithmetic(FunctionRegistry* registry) {
  codegen::MakeBinaryFunction<Add>("add", registry);

  // Uncommenting the line below raises the following compiler error:
  //
  // In file included from ../src/arrow/compute/kernels/scalar_arithmetic.cc:18:
  // In file included from ../src/arrow/compute/kernels/common.h:31:
  // ../src/arrow/compute/kernels/codegen_internal.h:537:16: error: no matching function for call to 'Call'
  //         return Op::template Call(ctx, arg0(), arg1());
  //                ^~~~~~~~~~~~~~~~~
  // ../src/arrow/compute/kernels/codegen_internal.h:567:16: note: in instantiation of member function 'arrow::compute::codegen::ScalarBinary<arrow::Int16Type, arrow::Int8Type, arrow::Int8Type, arrow::compute::Add>::ArrayArray' requested here
  //         return ArrayArray(ctx, batch, out);
  //                ^
  // ../src/arrow/compute/kernels/scalar_arithmetic.cc:113:84: note: in instantiation of member function 'arrow::compute::codegen::ScalarBinary<arrow::Int16Type, arrow::Int8Type, arrow::Int8Type, arrow::compute::Add>::Exec' requested here
  //   ArrayKernelExec exec = codegen::ScalarBinaryEqualTypes<Int16Type, Int8Type, Op>::Exec;
  //                                                                                    ^
  // ../src/arrow/compute/kernels/scalar_arithmetic.cc:129:12: note: in instantiation of function template specialization 'arrow::compute::codegen::MakeBinaryFunction2<arrow::compute::Add>' requested here
  //   codegen::MakeBinaryFunction2<Add>("add", registry);
  //            ^
  // ../src/arrow/compute/kernels/scalar_arithmetic.cc:25:24: note: candidate template ignored: couldn't infer template argument 'OUT'
  //   static constexpr OUT Call(KernelContext*, ARG0 left, ARG1 right) {
  //

  // codegen::MakeBinaryFunction2<Add>("add", registry);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
