#include "arrow/python/udf.h"

#include <cstddef>
#include <memory>
#include <sstream>

namespace arrow {

namespace py {


/// \brief Add a kernel with given input/output types, no required state
/// initialization, preallocation for fixed-width types, and default null
/// handling (intersect validity bitmaps of inputs).
Status UDFScalarFunction::AddKernel(std::vector<cp::InputType> in_types, cp::OutputType out_type,
                  UDFArrayKernelExec exec, cp::KernelInit init) {
    
  RETURN_NOT_OK(CheckArity(in_types));

  if (arity_.is_varargs && in_types.size() != 1) {
    return Status::Invalid("VarArgs signatures must have exactly one input type");
  }
  auto sig =
      cp::KernelSignature::Make(std::move(in_types), std::move(out_type), arity_.is_varargs);
  kernels_.emplace_back(std::move(sig), exec, init);
  return Status::OK();

}

/// \brief Add a kernel (function implementation). Returns error if the
/// kernel's signature does not match the function's arity.
Status UDFScalarFunction::AddKernel(UDFScalarKernel kernel) {
  RETURN_NOT_OK(CheckArity(kernel.signature->in_types()));
  if (arity_.is_varargs && !kernel.signature->is_varargs()) {
    return Status::Invalid("Function accepts varargs but kernel signature does not");
  }
  kernels_.emplace_back(std::move(kernel));
  return Status::OK();
}



}  // namespace py

}  // namespace arrow