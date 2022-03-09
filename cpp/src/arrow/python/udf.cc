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

#define DEFINE_CALL_UDF(TYPE_NAME, FUNCTION_SUFFIX, CONVERT_SUFFIX)                                               \
  Status exec_function_##FUNCTION_SUFFIX(const cp::ExecBatch& batch, PyObject* function, int num_args, Datum *out) {                \
    std::shared_ptr<TYPE_NAME> c_res_data;                                                    \
    PyObject* result = NULLPTR;                                                               \
    PyObject* data = NULLPTR;                                                                 \
    PyObject* arg_tuple = NULLPTR;                                                            \
    auto c_data = batch[0].CONVERT_SUFFIX();                                                  \
    Py_XINCREF(data);                                                                         \
    Py_XINCREF(arg_tuple);                                                                    \
    Py_XINCREF(result);                                                                       \
    data = wrap_##FUNCTION_SUFFIX(c_data);                                                    \
    arg_tuple = PyTuple_New(num_args);                                                        \
    PyTuple_SetItem(arg_tuple, 0, data);                                                      \
    result = PyObject_CallObject(function, arg_tuple);                                        \
    Py_XDECREF(function);                                                                     \
    if (result == NULL) {                                                                     \
      return Status::ExecutionError("Error occured in computation");                          \
    }                                                                                         \
    auto res = unwrap_##FUNCTION_SUFFIX(result);                                              \
    if (!res.status().ok()) {                                                                 \
      return res.status();                                                                    \
    }                                                                                         \
    c_res_data = res.ValueOrDie();                                                            \
    Py_XDECREF(data);                                                                         \
    Py_XDECREF(arg_tuple);                                                                    \
    Py_XDECREF(result);                                                                       \
    auto datum = new Datum(c_res_data);                                                       \
    *out = *datum;                                                                            \
    return Status::OK();                                                                      \
  }

DEFINE_CALL_UDF(Scalar, scalar, scalar)
DEFINE_CALL_UDF(Array, array, make_array)

#undef DEFINE_CALL_UDF

Status VerifyArityAndInput(cp::Arity arity, const cp::ExecBatch& batch) {
  bool match = (uint64_t)arity.num_args == batch.values.size();
  if (!match) {
    return Status::Invalid(
        "Function Arity and Input data shape doesn't match, expceted {}");
  }
  return Status::OK();
}

bool CheckBatchValueTypes(const ExecBatch& batch, int num_args) {
  
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
    if (function == NULL) {
      return Status::ExecutionError("python function cannot be null");
    }

    if (PyCallable_Check(function)) {
      RETURN_NOT_OK(VerifyArityAndInput(this->arity(), batch));
      // if the batch is an array
      auto num_args = this->arity().num_args;
      if (num_args == 1) {  // unary function
        if (batch[0].is_array()) {
          RETURN_NOT_OK(exec_function_array(batch, function, 1, out));
        } else if (batch[0].is_scalar()) {
          RETURN_NOT_OK(exec_function_scalar(batch, function, 1, out));
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
