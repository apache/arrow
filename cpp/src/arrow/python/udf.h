#pragma once

#include "arrow/python/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

#include "arrow/python/common.h"
#include "arrow/python/pyarrow.h"
#include "arrow/python/visibility.h"

#include <iostream>

namespace cp = arrow::compute;

namespace arrow {

namespace py {

class ARROW_PYTHON_EXPORT UDFSynthesizer {
 public:

  UDFSynthesizer(std::string func_name, cp::Arity arity, cp::FunctionDoc func_doc,
                 std::vector<cp::InputType> in_types, cp::OutputType out_type)
      : func_name_(func_name),
        arity_(arity),
        func_doc_(func_doc),
        in_types_(in_types),
        out_type_(out_type) {}

  Status MakeFunction(PyObject* function) {
    Status st;
    auto func = std::make_shared<cp::ScalarFunction>(func_name_, arity_, &func_doc_);
    Py_XINCREF(function);
    auto call_back_lambda = [function](cp::KernelContext* ctx, const cp::ExecBatch& batch,
                               Datum* out) {
      PyGILState_STATE state = PyGILState_Ensure();
      std::shared_ptr<arrow::Array> c_res_array;
      if (function == NULL) {
        PyGILState_Release(state);
        return Status::ExecutionError("python function cannot be null");
      }

      int res = PyCallable_Check(function);
      if (res == 1) {
        auto c_array = batch[0].make_array();
        PyObject* py_array = wrap_array(c_array);
        PyObject* arg_tuple = PyTuple_Pack(1, py_array);
        PyObject *result = PyObject_CallObject(function, arg_tuple);
        Py_DECREF(function);
        if (result == NULL) {
          PyGILState_Release(state);
          return Status::ExecutionError("Error occured in computation");
        }
        auto res = unwrap_array(result);
        if(!res.status().ok()) {
          PyGILState_Release(state);
          return res.status();
        }
        c_res_array = res.ValueOrDie();
      } else {
        PyErr_Print();
        return Status::ExecutionError("Error occured in computation");
      }
      auto datum = new Datum(c_res_array);
      *out->mutable_array() = *datum->array();
      PyGILState_Release(state);
      return Status::OK();
    };
    cp::ScalarKernel kernel(in_types_, out_type_, call_back_lambda);
    kernel.mem_allocation = cp::MemAllocation::NO_PREALLOCATE;
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

 private:
  std::string func_name_;
  cp::Arity arity_;
  cp::FunctionDoc func_doc_;
  std::vector<cp::InputType> in_types_;
  cp::OutputType out_type_;
};

}  // namespace py

}  // namespace arrow