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

Status ExecuteFunction(const compute::ExecBatch& batch, PyObject* function,
                       const compute::OutputType& exp_out_type, Datum* out) {
  int num_args = static_cast<int64_t>(batch.values.size());
  PyObject* arg_tuple = PyTuple_New(num_args);
  // wrap exec_batch objects into Python objects based on the datum type
  for (int arg_id = 0; arg_id < num_args; arg_id++) {
    switch (batch[arg_id].kind()) {
      case Datum::SCALAR: {
        auto c_data = batch[arg_id].scalar();
        PyObject* data = wrap_scalar(c_data);
        PyTuple_SetItem(arg_tuple, arg_id, data);
        break;
      }
      case Datum::ARRAY: {
        auto c_data = batch[arg_id].make_array();
        PyObject* data = wrap_array(c_data);
        PyTuple_SetItem(arg_tuple, arg_id, data);
        break;
      }
      default:
        return Status::NotImplemented(
            "User-defined-functions are not supported to the datum kind ",
            batch[arg_id].kind());
    }
  }
  // call to Python executing the function
  PyObject* result = PyObject_CallObject(function, arg_tuple);
  if (result == nullptr) {
    return Status::ExecutionError("Output is null, but expected an array");
  }
  // wrapping the output for expected output type
  if (is_scalar(result)) {
    ARROW_ASSIGN_OR_RAISE(auto val, unwrap_scalar(result));
    if (!exp_out_type.type()->Equals(val->type)) {
      return Status::Invalid("Expected output type, ", exp_out_type.type()->name(),
                             ", but function returned type ", val->type->name());
    }
    *out = Datum(val);
    return Status::OK();
  } else if (is_array(result)) {
    ARROW_ASSIGN_OR_RAISE(auto val, unwrap_array(result));
    if (!exp_out_type.type()->Equals(val->type())) {
      return Status::Invalid("Expected output type, ", exp_out_type.type()->name(),
                             ", but function returned type ", val->type()->name());
    }
    *out = Datum(val);
    return Status::OK();
  } else {
    return Status::Invalid("Not supported output type");
  }
  return Status::OK();
}

Status ScalarUdfBuilder::MakeFunction(PyObject* function, ScalarUdfOptions* options) {
  if (function == nullptr) {
    return Status::Invalid("Python function cannot be null");
  }
  Py_INCREF(function);
  function_.reset(function);
  if (!PyCallable_Check(function_.obj())) {
    return Status::TypeError("Expected a callable Python object.");
  }
  auto doc = options->doc();
  auto arity = options->arity();
  auto exp_out_type = options->output_type();
  scalar_func_ =
      std::make_shared<compute::ScalarFunction>(options->name(), arity, std::move(doc));
  auto func = function_.obj();
  auto exec = [func, exp_out_type](compute::KernelContext* ctx,
                                   const compute::ExecBatch& batch,
                                   Datum* out) -> Status {
    PyAcquireGIL lock;
    RETURN_NOT_OK(ExecuteFunction(batch, func, exp_out_type, out));
    return CheckPyError();
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
