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

#include <cstddef>
#include <memory>
#include <sstream>

#include "arrow/compute/function.h"
#include "arrow/python/common.h"

namespace arrow {

namespace py {

Status VerifyArrayInput(const compute::ExecBatch& batch) {
  for (auto value : batch.values) {
    if (!value.is_array()) {
      return Status::Invalid("Expected array input, but got ", value.type());
    }
  }
  return Status::OK();
}

Status VerifyScalarInput(const compute::ExecBatch& batch) {
  for (auto value : batch.values) {
    if (!value.is_scalar()) {
      return Status::Invalid("Expected scalar input, but got ", value.type());
    }
  }
  return Status::OK();
}

Status VerifyArityAndInput(compute::Arity arity, const compute::ExecBatch& batch) {
  if (!arity.is_varargs) {
    bool match = static_cast<uint64_t>(arity.num_args) == batch.values.size();
    if (!match) {
      return Status::Invalid(
          "Function Arity and Input data shape doesn't match, expected ", arity.num_args,
          ", got ", batch.values.size());
    }
  } else {
    bool match = static_cast<uint64_t>(arity.num_args) <= batch.values.size();
    if (!match) {
      return Status::Invalid("Required minimum number of arguments", arity.num_args,
                             " in VarArgs function is not met.", ", Received ",
                             batch.values.size());
    }
  }
  return Status::OK();
}

Status ExecFunctionScalar(const compute::ExecBatch& batch, PyObject* function,
                          const compute::Arity& arity, Datum* out) {
  // num_args for arity varargs is arity.num_args, and for other arities,
  // it is equal to the number of values in the batch
  int num_args = arity.is_varargs ? batch.values.size() : arity.num_args;
  PyObject* arg_tuple = PyTuple_New(num_args);
  for (int arg_id = 0; arg_id < num_args; arg_id++) {
    if (!batch[arg_id].is_scalar()) {
      return Status::Invalid("Input type and data type doesn't match");
    }
    auto c_data = batch[arg_id].scalar();
    PyObject* data = wrap_scalar(c_data);
    PyTuple_SetItem(arg_tuple, arg_id, data);
  }
  PyObject* result = PyObject_CallObject(function, arg_tuple);
  if (result == NULL) {
    return Status::ExecutionError("Output is null, but expected a scalar");
  }
  if (!is_scalar(result)) {
    return Status::Invalid("Output from function is not a scalar");
  }
  ARROW_ASSIGN_OR_RAISE(auto unwrapped_result, unwrap_scalar(result));
  *out = unwrapped_result;
  return Status::OK();
}

Status ExecFunctionArray(const compute::ExecBatch& batch, PyObject* function,
                         const compute::Arity& arity, Datum* out) {
  // num_args for arity varargs is arity.num_args, and for other arities,
  // it is equal to the number of values in the batch
  int num_args = arity.is_varargs ? batch.values.size() : arity.num_args;
  PyObject* arg_tuple = PyTuple_New(num_args);
  for (int arg_id = 0; arg_id < num_args; arg_id++) {
    if (!batch[arg_id].is_array()) {
      return Status::Invalid("Input type and data type doesn't match");
    }
    auto c_data = batch[arg_id].make_array();
    PyObject* data = wrap_array(c_data);
    PyTuple_SetItem(arg_tuple, arg_id, data);
  }
  PyObject* result = PyObject_CallObject(function, arg_tuple);
  if (result == NULL) {
    return Status::ExecutionError("Output is null, but expected an array");
  }
  if (!is_array(result)) {
    return Status::Invalid("Output from function is not an array");
  }
  return unwrap_array(result).Value(out);
}

Status ScalarUdfBuilder::MakeFunction(PyObject* function, ScalarUdfOptions* options) {
  if (function == NULL) {
    return Status::Invalid("python function cannot be null");
  }
  Py_INCREF(function);
  function_.reset(function);
  if (!PyCallable_Check(function_.obj())) {
    return Status::TypeError("Expected a callable python object.");
  }
  auto doc = options->doc();
  auto arity = options->arity();
  scalar_func_ = std::make_shared<compute::ScalarFunction>(options->name(), arity, doc);
  auto func = function_.obj();
  auto exec = [func, arity](compute::KernelContext* ctx, const compute::ExecBatch& batch,
                            Datum* out) -> Status {
    PyAcquireGIL lock;
    RETURN_NOT_OK(VerifyArityAndInput(arity, batch));
    if (VerifyArrayInput(batch).ok()) {  // checke 0-th element to select array callable
      RETURN_NOT_OK(ExecFunctionArray(batch, func, arity, out));
    } else if (VerifyScalarInput(batch)
                   .ok()) {  // check 0-th element to select scalar callable
      RETURN_NOT_OK(ExecFunctionScalar(batch, func, arity, out));
    } else {
      return Status::Invalid("Unexpected input type, scalar or array type expected.");
    }
    return Status::OK();
  };

  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(options->input_types(), options->output_type(),
                                     arity.is_varargs),
      exec);
  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func_->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func_)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
