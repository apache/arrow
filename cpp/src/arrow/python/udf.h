#pragma once

#include "arrow/python/platform.h"

#include <cstdint>
#include <memory>

#include "arrow/compute/function.h"

#include "arrow/python/common.h"

namespace cp = arrow::compute;

namespace arrow {

namespace py {

// PyObject* CallUnaryTableUDF(PyObject* func, PyObject* arg1, std::shared_ptr<Table> input);
// PyObject* CallUnaryArrayUDF(PyObject* func, PyObject* arg1, std::shared_ptr<Array> input);

using UDFArrayKernelExec = std::function<Status(cp::KernelContext*, const cp::ExecBatch&, Datum*, PyObject* func)>;

struct UDFArrayKernel : public cp::Kernel {
  UDFArrayKernel() = default;

  UDFArrayKernel(std::shared_ptr<cp::KernelSignature> sig, UDFArrayKernelExec exec,
              cp::KernelInit init = NULLPTR)
      : Kernel(std::move(sig), init), exec(std::move(exec)) {}

  UDFArrayKernel(std::vector<cp::InputType> in_types, cp::OutputType out_type, UDFArrayKernelExec exec,
              cp::KernelInit init = NULLPTR)
      : Kernel(std::move(in_types), std::move(out_type), std::move(init)),
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

class ARROW_PYTHON_EXPORT UDFScalarFunction : public cp::detail::FunctionImpl<UDFScalarKernel> {
 public:
  using KernelType = UDFScalarKernel;

  UDFScalarFunction(std::string name, const cp::Arity& arity, const cp::FunctionDoc* doc,
                 const cp::FunctionOptions* default_options = NULLPTR)
      : cp::detail::FunctionImpl<UDFScalarKernel>(std::move(name), cp::Function::SCALAR, arity, doc,
                                           default_options) {}

  /// \brief Add a kernel with given input/output types, no required state
  /// initialization, preallocation for fixed-width types, and default null
  /// handling (intersect validity bitmaps of inputs).
  Status AddKernel(std::vector<cp::InputType> in_types, cp::OutputType out_type,
                   UDFArrayKernelExec exec, cp::KernelInit init = NULLPTR);

  /// \brief Add a kernel (function implementation). Returns error if the
  /// kernel's signature does not match the function's arity.
  Status AddKernel(UDFScalarKernel kernel);
};


struct ARROW_PYTHON_EXPORT UDFSynthesizer {
  UDFSynthesizer();

  
};

}  // namespace py

}  // namespace arrow