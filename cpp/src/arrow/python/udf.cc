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

Status ExecuteFunction(compute::KernelContext* ctx, const compute::ExecBatch& batch,
                       PyObject* function, const compute::OutputType& exp_out_type,
                       Datum* out) {
  size_t num_args = batch.values.size();
  // ScalarUdfContext udf_context;
  // udf_context.pool = ctx->memory_pool();
  // udf_context.batch_length = num_args;
  PyObject* arg_tuple = PyTuple_New(num_args);
  // PyObject* py_udf_ctx = wrap_udf_context(udf_context);
  // PyTuple_SetItem(arg_tuple, 0, py_udf_ctx);
  // wrap exec_batch objects into Python objects based on the datum type
  for (size_t arg_id = 0; arg_id < num_args; arg_id++) {
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
            "User-defined-functions are not supported for the datum kind ",
            batch[arg_id].kind());
    }
  }
  // call to Python executing the function
  PyObject* result;
  auto st = SafeCallIntoPython([&]() -> Status {
    result = PyObject_CallObject(function, arg_tuple);
    return CheckPyError();
  });
  RETURN_NOT_OK(st);
  if (result == nullptr) {
    return Status::ExecutionError("Output is null, but expected an array");
  }
  // unwrapping the output for expected output type
  if (is_scalar(result)) {
    ARROW_ASSIGN_OR_RAISE(auto val, unwrap_scalar(result));
    if (!exp_out_type.type()->Equals(val->type)) {
      return Status::TypeError("Expected output type, ", exp_out_type.type()->ToString(),
                               ", but function returned type ", val->type->ToString());
    }
    *out = Datum(val);
    return Status::OK();
  } else if (is_array(result)) {
    ARROW_ASSIGN_OR_RAISE(auto val, unwrap_array(result));
    if (!exp_out_type.type()->Equals(val->type())) {
      return Status::TypeError("Expected output type, ", exp_out_type.type()->ToString(),
                               ", but function returned type ", val->type()->ToString());
    }
    *out = Datum(val);
    return Status::OK();
  } else {
    return Status::TypeError("Unexpected output type: ", Py_TYPE(result)->tp_name,
                             " (expected Scalar or Array)");
  }
  return Status::OK();
}

Status RegisterScalarFunction(PyObject* function, const ScalarUdfOptions& options) {
  if (function == nullptr) {
    return Status::Invalid("Python function cannot be null");
  }
  if (!PyCallable_Check(function)) {
    return Status::TypeError("Expected a callable Python object.");
  }
  auto doc = options.doc();
  auto arity = options.arity();
  auto exp_out_type = options.output_type();
  auto scalar_func =
      std::make_shared<compute::ScalarFunction>(options.name(), arity, std::move(doc));
  auto exec = [function, exp_out_type](compute::KernelContext* ctx,
                                       const compute::ExecBatch& batch,
                                       Datum* out) -> Status {
    PyAcquireGIL lock;
    RETURN_NOT_OK(ExecuteFunction(ctx, batch, function, exp_out_type, out));
    return Status::OK();
  };

  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(options.input_types(), options.output_type(),
                                     arity.is_varargs),
      exec);
  kernel.mem_allocation = compute::MemAllocation::NO_PREALLOCATE;
  kernel.null_handling = compute::NullHandling::COMPUTED_NO_PREALLOCATE;
  RETURN_NOT_OK(scalar_func->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
