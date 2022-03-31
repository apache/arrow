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

Status exec_function_scalar(const compute::ExecBatch& batch, PyObject* function,
                            int num_args, Datum* out) {
  std::shared_ptr<Scalar> c_res_data;
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
    return Status::ExecutionError("Error occured in computation");
  }
  auto res = unwrap_scalar(result);
  if (!res.status().ok()) {
    return res.status();
  }
  c_res_data = res.ValueOrDie();
  auto datum = new Datum(c_res_data);
  *out = *datum;
  return Status::OK();
}

Status exec_function_array(const compute::ExecBatch& batch, PyObject* function,
                           int num_args, Datum* out) {
  std::shared_ptr<Array> c_res_data;
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
    return Status::ExecutionError("Error occured in computation");
  }
  auto res = unwrap_array(result);
  if (!res.status().ok()) {
    return res.status();
  }
  c_res_data = res.ValueOrDie();
  auto datum = new Datum(c_res_data);
  *out = *datum;
  return Status::OK();
}

Status VerifyArityAndInput(compute::Arity arity, const compute::ExecBatch& batch) {
  bool match = static_cast<uint64_t>(arity.num_args) == batch.values.size();
  if (!match) {
    return Status::Invalid(
        "Function Arity and Input data shape doesn't match, expected {}");
  }
  return Status::OK();
}

Status ScalarUdfBuilder::MakeFunction(PyObject* function, UDFOptions* options) {
  // creating a copy of objects for the lambda function
  Py_INCREF(function);
  function_.reset(function);
  if (function_.obj() == NULL) {
    return Status::ExecutionError("python function cannot be null");
  }
  if (!PyCallable_Check(function_.obj())) {
    return Status::TypeError("Expected a callable python object.");
  }
  auto doc = this->doc();
  scalar_func_ = 
      std::make_shared<compute::ScalarFunction>(this->name(), this->arity(), &doc);
  auto arity = this->arity();
  // lambda function
  auto call_back = [&, arity](compute::KernelContext* ctx,
                              const compute::ExecBatch& batch, Datum* out) -> Status {
    PyAcquireGIL lock;
    RETURN_NOT_OK(VerifyArityAndInput(arity, batch));
    if (batch[0].is_array()) {  // checke 0-th element to select array callable
      RETURN_NOT_OK(exec_function_array(batch, function_.obj(), arity.num_args, out));
    } else if (batch[0].is_scalar()) {  // check 0-th element to select scalar callable
      RETURN_NOT_OK(exec_function_scalar(batch, function_.obj(), arity.num_args, out));
    } else {
      return Status::Invalid("Unexpected input type, scalar or array type expected.");
    }
    return Status::OK();
  };  // lambda function

  compute::ScalarKernel kernel(
      compute::KernelSignature::Make(this->input_types(), this->output_type(),
                                     this->arity().is_varargs),
      call_back);
  kernel.mem_allocation = this->mem_allocation();
  kernel.null_handling = this->null_handling();
  RETURN_NOT_OK(scalar_func_->AddKernel(std::move(kernel)));
  auto registry = compute::GetFunctionRegistry();
  RETURN_NOT_OK(registry->AddFunction(std::move(scalar_func_)));
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
