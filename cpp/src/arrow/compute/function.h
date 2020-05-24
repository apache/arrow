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

// NOTE: API is EXPERIMENTAL and will change without going through a
// deprecation cycle

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/kernel.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

struct Datum;
struct ValueDescr;

namespace compute {

class ExecContext;

struct ARROW_EXPORT FunctionOptions {};

/// \brief Contains the number of required arguments for the function
struct ARROW_EXPORT Arity {
  static Arity Nullary() { return Arity(0, false); }
  static Arity Unary() { return Arity(1, false); }
  static Arity Binary() { return Arity(2, false); }
  static Arity Ternary() { return Arity(3, false); }
  static Arity VarArgs(int min_args = 1) { return Arity(min_args, true); }

  Arity(int num_args, bool is_varargs = false)  // NOLINT implicit conversion
      : num_args(num_args), is_varargs(is_varargs) {}

  /// The number of required arguments (or the minimum number for varargs
  /// functions)
  int num_args;

  /// If true, then the num_args is the minimum number of required arguments
  bool is_varargs = false;
};

/// \brief Base class for function containers that are capable of dispatch to
/// kernel implementations
class ARROW_EXPORT Function {
 public:
  /// \brief The kind of function, which indicates in what contexts it is
  /// valid for use
  enum Kind {
    /// A function that performs scalar data operations on whole arrays of
    /// data. Can generally process Array or Scalar values. The size of the
    /// output will be the same as the size (or broadcasted size, in the case
    /// of mixing Array and Scalar inputs) of the input.
    SCALAR,

    /// A function with array input and output whose behavior depends on the
    /// values of the entire arrays passed, rather than the value of each scalar
    /// value.
    VECTOR,

    /// A function that computes scalar summary statistics from array input.
    SCALAR_AGGREGATE
  };

  virtual ~Function() = default;

  /// \brief The name of the kernel. The registry enforces uniqueness of names
  const std::string& name() const { return name_; }

  /// \brief The kind of kernel, which indicates in what contexts it is valid
  /// for use
  Function::Kind kind() const { return kind_; }

  /// \brief Contains the number of arguments the function requires
  const Arity& arity() const { return arity_; }

  /// \brief Returns the number of registered kernels for this function
  virtual int num_kernels() const = 0;

  /// \brief Convenience for invoking a function with kernel dispatch and
  /// memory allocation details taken care of
  Result<Datum> Execute(const std::vector<Datum>& args, const FunctionOptions* options,
                        ExecContext* ctx = NULLPTR) const;

 protected:
  Function(std::string name, Function::Kind kind, const Arity& arity)
      : name_(std::move(name)), kind_(kind), arity_(arity) {}
  std::string name_;
  Function::Kind kind_;
  Arity arity_;
};

namespace detail {

template <typename KernelType>
class FunctionImpl : public Function {
 public:
  /// \brief Return pointers to current-available kernels for inspection
  std::vector<const KernelType*> kernels() const {
    std::vector<const KernelType*> result;
    for (const auto& kernel : kernels_) {
      result.push_back(&kernel);
    }
    return result;
  }

  int num_kernels() const override { return static_cast<int>(kernels_.size()); }

 protected:
  FunctionImpl(std::string name, Function::Kind kind, const Arity& arity)
      : Function(std::move(name), kind, arity) {}

  std::vector<KernelType> kernels_;
};

}  // namespace detail

/// \brief A function that executes elementwise operations on arrays or
/// scalars, and therefore whose results generally do not depend on the order
/// of the values in the arguments. Accepts and returns arrays that are all of
/// the same size. These functions roughly correspond to the functions used in
/// SQL expressions.
class ARROW_EXPORT ScalarFunction : public detail::FunctionImpl<ScalarKernel> {
 public:
  using KernelType = ScalarKernel;

  ScalarFunction(std::string name, const Arity& arity)
      : detail::FunctionImpl<ScalarKernel>(std::move(name), Function::SCALAR, arity) {}

  /// \brief Add a simple kernel (function implementation) with given
  /// input/output types, no required state initialization, preallocation for
  /// fixed-width types, and default null handling (intersect validity bitmaps
  /// of inputs)
  Status AddKernel(std::vector<InputType> in_types, OutputType out_type,
                   ArrayKernelExec exec, KernelInit init = NULLPTR);

  /// \brief Add a kernel (function implementation). Returns error if fails
  /// to match the other parameters of the function
  Status AddKernel(ScalarKernel kernel);

  /// \brief Return the first kernel that can execute the function given the
  /// exact argument types (without implicit type casts or scalar->array
  /// promotions)
  ///
  /// This function is overridden in CastFunction
  virtual Result<const ScalarKernel*> DispatchExact(
      const std::vector<ValueDescr>& values) const;
};

/// \brief A function that executes general array operations that may yield
/// outputs of different sizes or have results that depend on the whole array
/// contents. These functions roughly correspond to the functions found in
/// non-SQL array languages like APL and its derivatives
class ARROW_EXPORT VectorFunction : public detail::FunctionImpl<VectorKernel> {
 public:
  using KernelType = VectorKernel;

  VectorFunction(std::string name, const Arity& arity)
      : detail::FunctionImpl<VectorKernel>(std::move(name), Function::VECTOR, arity) {}

  /// \brief Add a simple kernel (function implementation) with given
  /// input/output types, no required state initialization, preallocation for
  /// fixed-width types, and default null handling (intersect validity bitmaps
  /// of inputs)
  Status AddKernel(std::vector<InputType> in_types, OutputType out_type,
                   ArrayKernelExec exec, KernelInit init = NULLPTR);

  /// \brief Add a kernel (function implementation). Returns error if fails
  /// to match the other parameters of the function
  Status AddKernel(VectorKernel kernel);

  /// \brief Return the first kernel that can execute the function given the
  /// exact argument types (without implicit type casts or scalar->array
  /// promotions)
  Result<const VectorKernel*> DispatchExact(const std::vector<ValueDescr>& values) const;
};

class ARROW_EXPORT ScalarAggregateFunction
    : public detail::FunctionImpl<ScalarAggregateKernel> {
 public:
  using KernelType = ScalarAggregateKernel;

  ScalarAggregateFunction(std::string name, const Arity& arity)
      : detail::FunctionImpl<ScalarAggregateKernel>(std::move(name),
                                                    Function::SCALAR_AGGREGATE, arity) {}

  /// \brief Add a kernel (function implementation). Returns error if fails
  /// to match the other parameters of the function
  Status AddKernel(ScalarAggregateKernel kernel);

  Result<const ScalarAggregateKernel*> DispatchExact(
      const std::vector<ValueDescr>& values) const;
};

}  // namespace compute
}  // namespace arrow
