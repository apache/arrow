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
#include "arrow/python/visibility.h"

#include <iostream>

namespace cp = arrow::compute;

namespace arrow {

namespace py {

// PyObject* CallUnaryTableUDF(PyObject* func, PyObject* arg1, std::shared_ptr<Table>
// input); PyObject* CallUnaryArrayUDF(PyObject* func, PyObject* arg1,
// std::shared_ptr<Array> input);

using UDFArrayKernelExec = std::function<Status(cp::KernelContext*, const cp::ExecBatch&,
                                                Datum*, PyObject* func)>;

struct UDFArrayKernel : public cp::Kernel {
  UDFArrayKernel() = default;

  UDFArrayKernel(std::shared_ptr<cp::KernelSignature> sig, UDFArrayKernelExec exec,
                 cp::KernelInit init = NULLPTR)
      : cp::Kernel(std::move(sig), init), exec(std::move(exec)) {}

  UDFArrayKernel(std::vector<cp::InputType> in_types, cp::OutputType out_type,
                 UDFArrayKernelExec exec, cp::KernelInit init = NULLPTR)
      : cp::Kernel(std::move(in_types), std::move(out_type), std::move(init)),
        exec(std::move(exec)) {}

  /// \brief Perform a single invocation of this kernel. Depending on the
  /// implementation, it may only write into preallocated memory, while in some
  /// cases it will allocate its own memory. Any required state is managed
  /// through the KernelContext.
  UDFArrayKernelExec exec;

  /// \brief Writing execution results into larger contiguous allocations
  /// requires that the kernel be able to write into sliced output ArrayData*,
  /// including sliced output validity bitmaps. Some kernel implementations may
  /// not be able to do this, so setting this to false disables this
  /// functionality.
  bool can_write_into_slices = true;
};

struct UDFScalarKernel : public UDFArrayKernel {
  using UDFArrayKernel::UDFArrayKernel;

  // For scalar functions preallocated data and intersecting arg validity
  // bitmaps is a reasonable default
  cp::NullHandling::type null_handling = cp::NullHandling::INTERSECTION;
  cp::MemAllocation::type mem_allocation = cp::MemAllocation::PREALLOCATE;
};

class ARROW_PYTHON_EXPORT UDFScalarFunction
    : public cp::detail::FunctionImpl<UDFScalarKernel> {
 public:
  using KernelType = UDFScalarKernel;

  UDFScalarFunction(std::string name, const cp::Arity& arity, const cp::FunctionDoc* doc,
                    const cp::FunctionOptions* default_options = NULLPTR)
      : cp::detail::FunctionImpl<UDFScalarKernel>(std::move(name), cp::Function::SCALAR,
                                                  arity, doc, default_options) {}

  /// \brief Add a kernel with given input/output types, no required state
  /// initialization, preallocation for fixed-width types, and default null
  /// handling (intersect validity bitmaps of inputs).
  Status AddKernel(std::vector<cp::InputType> in_types, cp::OutputType out_type,
                   UDFArrayKernelExec exec, cp::KernelInit init = NULLPTR);

  /// \brief Add a kernel (function implementation). Returns error if the
  /// kernel's signature does not match the function's arity.
  Status AddKernel(UDFScalarKernel kernel) {
    ARROW_RETURN_NOT_OK(CheckArity(kernel.signature->in_types()));
    if (arity_.is_varargs && !kernel.signature->is_varargs()) {
      return Status::Invalid("Function accepts varargs but kernel signature does not");
    }
    kernels_.emplace_back(std::move(kernel));
    return Status::OK();
  }

  Status Hello1() { return Status::OK(); }

  Status Hello2();
};

using KernelExec =
    std::function<Status(cp::KernelContext*, const cp::ExecBatch&, Datum*)>;

class ARROW_PYTHON_EXPORT UDFSynthesizer {
 public:
  UDFSynthesizer(std::string func_name, cp::Arity arity, cp::FunctionDoc func_doc,
                 std::vector<cp::InputType> in_types, cp::OutputType out_type,
                 Status (*callback)(cp::KernelContext*, const cp::ExecBatch&, Datum*))
      : func_name_(func_name),
        arity_(arity),
        func_doc_(func_doc),
        in_types_(in_types),
        out_type_(out_type),
        callback_(callback) {}

  UDFSynthesizer(std::string func_name, cp::Arity arity, cp::FunctionDoc func_doc,
                 std::vector<cp::InputType> in_types, cp::OutputType out_type)
      : func_name_(func_name),
        arity_(arity),
        func_doc_(func_doc),
        in_types_(in_types),
        out_type_(out_type) {}

  Status MakeFunction() {
    Status st;
    auto func = std::make_shared<cp::ScalarFunction>(func_name_, arity_, &func_doc_);
    cp::ScalarKernel kernel(in_types_, out_type_, callback_);
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

  Status MakePyFunction(PyObject* function, PyObject* args) {
    Status st;
    auto func = std::make_shared<cp::ScalarFunction>(func_name_, arity_, &func_doc_);
    Py_XINCREF(function);
    Py_XINCREF(args);
    //double result = PyFloat_AsDouble(args);
    //std::cout << "Make Function Args : " << result << std::endl;
    auto call_back_lambda = [function, args](cp::KernelContext* ctx, const cp::ExecBatch& batch,
                               Datum* out) {
      PyGILState_STATE state = PyGILState_Ensure();
      // PyObject* obj = Py_BuildValue("s", "hello");
      //Py_XINCREF(function);
      //Py_XINCREF(args);
      if (function == NULL) {
        PyGILState_Release(state);
        return Status::ExecutionError("python function cannot be null");
      }

      int res = PyCallable_Check(function);

      if (res == 1) {
        std::cout << "This is a PyCallback" << std::endl;
        PyObject *result = PyObject_CallObject(function, args);
        Py_DECREF(function);
        if (result == NULL) {
          PyGILState_Release(state);
          return Status::ExecutionError("Error occured in computation");
        }
      } else {
        std::cout << "This is not a callable" << std::endl;
        PyErr_Print();
      }
      // Python Way Ends
      auto res_func = cp::CallFunction("add", {batch[0].array(), batch[0].array()});
      *out->mutable_array() = *res_func.ValueOrDie().array();
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
  // KernelExec kernel_exec_;
  Status (*callback_)(cp::KernelContext*, const cp::ExecBatch&, Datum*);
  // C++ way
  //PyObject* (*py_call_back_)();
  // Python way
};

}  // namespace py

}  // namespace arrow