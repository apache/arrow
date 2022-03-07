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

namespace cp = arrow::compute;

namespace arrow {

namespace py {

Status VerifyArityAndInput(cp::Arity arity, const cp::ExecBatch& batch) {
  bool match = (uint64_t)arity.num_args == batch.values.size();
  if (!match) {
    return Status::Invalid(
        "Function Arity and Input data shape doesn't match, expceted {}");
  }
  return Status::OK();
}

Status ScalarUdfBuilder::MakeFunction(PyObject* function) {
  Status st;
  auto func =
      std::make_shared<cp::ScalarFunction>(this->name(), this->arity(), &this->doc());
  // lambda function
  auto call_back_lambda = [function, this](cp::KernelContext* ctx,
                                           const cp::ExecBatch& batch,
                                           Datum* out) -> Status {
    PyAcquireGIL lock;
    PyObject* arg_tuple = NULLPTR;
    PyObject* result = NULLPTR;
    if (function == NULL) {
      return Status::ExecutionError("python function cannot be null");
    }

    if (PyCallable_Check(function)) {
      RETURN_NOT_OK(VerifyArityAndInput(this->arity(), batch));
      // if the batch is an array
      auto num_args = this->arity().num_args;
      if (num_args == 1) {  // unary function
        if (batch[0].is_array()) {
          std::shared_ptr<arrow::Array> c_res_array;
          PyObject* py_array = NULLPTR;
          auto c_array = batch[0].make_array();
          Py_XINCREF(py_array);
          Py_XINCREF(arg_tuple);
          Py_XINCREF(result);
          py_array = wrap_array(c_array);
          arg_tuple = PyTuple_Pack(1, py_array);
          result = PyObject_CallObject(function, arg_tuple);
          Py_XDECREF(function);
          if (result == NULL) {
            return Status::ExecutionError("Error occured in computation");
          }
          auto res = unwrap_array(result);
          if (!res.status().ok()) {
            return res.status();
          }
          c_res_array = res.ValueOrDie();
          Py_XDECREF(py_array);
          Py_XDECREF(arg_tuple);
          Py_XDECREF(result);
          auto datum = new Datum(c_res_array);
          *out->mutable_array() = *datum->array();
        } else if (batch[0].is_scalar()) {
          std::shared_ptr<arrow::Scalar> c_res_scalar;
          PyObject* py_scalar = NULLPTR;
          auto c_scalar = batch[0].scalar();
          Py_XINCREF(py_scalar);
          Py_XINCREF(arg_tuple);
          Py_XINCREF(result);
          py_scalar = wrap_scalar(c_scalar);
          arg_tuple = PyTuple_Pack(1, py_scalar);
          result = PyObject_CallObject(function, arg_tuple);
          Py_XDECREF(function);
          if (result == NULL) {
            return Status::ExecutionError("Error occured in computation");
          }
          auto res = unwrap_scalar(result);
          if (!res.status().ok()) {
            return res.status();
          }
          c_res_scalar = res.ValueOrDie();
          Py_XDECREF(py_scalar);
          Py_XDECREF(arg_tuple);
          Py_XDECREF(result);
          auto datum = new Datum(c_res_scalar);
          *out = *datum;
        } else {
          return Status::Invalid("Invalid type, expected scalar or array input");
        }
      } else if (num_args == 2) {  // binary function
        if (batch[0].is_array() && batch[1].is_array()) {
        } else if (batch[0].is_scalar() && batch[1].is_scalar()) {
          return Status::NotImplemented("TODO:");
        } else {
          return Status::Invalid("Invalid type, expected scalar or array input");
        }
      } else if (num_args == 3) {  // ternary function
        return Status::NotImplemented("TODO:");
      } else if (num_args > 3) {  // varargs function
        return Status::NotImplemented("TODO:");
      }
      // if the batch is a scalar
    } else {
      return Status::ExecutionError("Error occured in computation");
    }
    Py_XDECREF(function);
    return Status::OK();
  };

  // lambda function
  cp::ScalarKernel kernel(this->input_types(), this->output_type(), call_back_lambda);
  kernel.mem_allocation = this->mem_allocation();
  kernel.null_handling = this->null_handling();
  st = func->AddKernel(std::move(kernel));
  if (!st.ok()) {
    return Status::ExecutionError("Kernel couldn't be added to the udf");
  }
  auto registry = cp::GetFunctionRegistry();
  st = registry->AddFunction(std::move(func));
  if (!st.ok()) {
    return Status::ExecutionError("udf registration failed");
  }
  return Status::OK();
}

}  // namespace py

}  // namespace arrow
