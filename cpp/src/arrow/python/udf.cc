#include "arrow/python/udf.h"

#include <cstddef>
#include <memory>
#include <sstream>

#include "arrow/compute/function.h"
#include "arrow/python/common.h"

namespace cp = arrow::compute;

namespace arrow {

namespace py {

Status ScalarUdfBuilder::MakeFunction(PyObject* function) {
  Status st;
  auto func =
      std::make_shared<cp::ScalarFunction>(this->name(), this->arity(), &this->doc());
  // lambda function
  auto call_back_lambda = [function](cp::KernelContext* ctx, const cp::ExecBatch& batch,
                                     Datum* out) -> Status {
    PyAcquireGIL lock;
    PyObject* py_array = NULLPTR;
    PyObject* arg_tuple = NULLPTR;
    PyObject* result = NULLPTR;
    std::shared_ptr<arrow::Array> c_res_array;
    if (function == NULL) {
      return Status::ExecutionError("python function cannot be null");
    }

    if (PyCallable_Check(function)) {
      // if the batch is an array
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
      Py_XDECREF(function);
      // if the batch is a scalar
    } else {
      return Status::ExecutionError("Error occured in computation");
    }
    auto datum = new Datum(c_res_array);
    *out->mutable_array() = *datum->array();
    return Status::OK();
  };  
  // lambda function

  cp::ScalarKernel kernel(this->input_types(), this->output_type(), call_back_lambda);
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

}  // namespace py

}  // namespace arrow