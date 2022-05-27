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

#include "arrow/python/udf.h"
#include "arrow/compute/function.h"
#include "arrow/python/common.h"

namespace arrow {

namespace py {

namespace {
Status CheckOutputType(const DataType& expected, const DataType& actual) {
  if (!expected.Equals(actual)) {
    return Status::TypeError("Expected output datatype ", expected.ToString(),
                             ", but function returned datatype ", actual.ToString());
  }
  return Status::OK();
}

struct PythonUdf {
  ScalarUdfWrapperCallback cb;
  std::shared_ptr<OwnedRefNoGIL> function;
  compute::OutputType output_type;

  // function needs to be destroyed at process exit
  // and Python may no longer be initialized.
  ~PythonUdf() {
    if (_Py_IsFinalizing()) {
      function->detach();
    }
  }

  Status operator()(compute::KernelContext* ctx, const compute::ExecBatch& batch,
                    Datum* out) {
    return SafeCallIntoPython([&]() -> Status { return Execute(ctx, batch, out); });
  }

  Status Execute(compute::KernelContext* ctx, const compute::ExecBatch& batch,
                 Datum* out) {
    const auto num_args = batch.values.size();
    ScalarUdfContext udf_context{ctx->memory_pool(), static_cast<int64_t>(batch.length)};

    OwnedRef arg_tuple(PyTuple_New(num_args));
    RETURN_NOT_OK(CheckPyError());
    for (size_t arg_id = 0; arg_id < num_args; arg_id++) {
      switch (batch[arg_id].kind()) {
        case Datum::SCALAR: {
          auto c_data = batch[arg_id].scalar();
          PyObject* data = wrap_scalar(c_data);
          PyTuple_SetItem(arg_tuple.obj(), arg_id, data);
          break;
        }
        case Datum::ARRAY: {
          auto c_data = batch[arg_id].make_array();
          PyObject* data = wrap_array(c_data);
          PyTuple_SetItem(arg_tuple.obj(), arg_id, data);
          break;
        }
        default:
          auto datum = batch[arg_id];
          return Status::NotImplemented(
              "User-defined-functions are not supported for the datum kind ",
              ToString(batch[arg_id].kind()));
      }
    }

    OwnedRef result(cb(function->obj(), udf_context, arg_tuple.obj()));
    RETURN_NOT_OK(CheckPyError());
    // unwrapping the output for expected output type
    if (is_scalar(result.obj())) {
      ARROW_ASSIGN_OR_RAISE(auto val, unwrap_scalar(result.obj()));
      RETURN_NOT_OK(CheckOutputType(*output_type.type(), *val->type));
      *out = Datum(val);
      return Status::OK();
    } else if (is_array(result.obj())) {
      ARROW_ASSIGN_OR_RAISE(auto val, unwrap_array(result.obj()));
      RETURN_NOT_OK(CheckOutputType(*output_type.type(), *val->type()));
      *out = Datum(val);
      return Status::OK();
    } else {
      return Status::TypeError("Unexpected output type: ", Py_TYPE(result.obj())->tp_name,
                               " (expected Scalar or Array)");
    }
    return Status::OK();
  }
};

}  // namespace

Status RegisterScalarFunction(PyObject* user_function, ScalarUdfWrapperCallback wrapper,
                              const ScalarUdfOptions& options) {
  if (!PyCallable_Check(user_function)) {
    return Status::TypeError("Expected a callable Python object.");
  }
  auto scalar_func = std::make_shared<compute::ScalarFunction>(
      options.func_name, options.arity, options.func_doc);
  Py_INCREF(user_function);
  std::vector<compute::InputType> input_types;
  for (const auto& in_dtype : options.input_types) {
    input_types.emplace_back(in_dtype);
  }
  compute::OutputType output_type(options.output_type);
  PythonUdf exec{wrapper, std::make_shared<OwnedRefNoGIL>(user_function), output_type};
  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(std::move(input_types), std::move(output_type),
                                     options.arity.is_varargs),
      std::move(exec));
  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
