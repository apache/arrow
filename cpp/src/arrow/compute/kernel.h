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

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;

namespace compute {

struct FunctionOptions;

/// \brief Base class for opaque kernel-specific state. For example, if there
/// is some kind of initialization required
struct KernelState {
  virtual ~KernelState() = default;
};

/// \brief Context/state for the execution of a particular kernel
class ARROW_EXPORT KernelContext {
 public:
  explicit KernelContext(ExecContext* exec_ctx) : exec_ctx_(exec_ctx) {}

  /// \brief Allocate buffer from the context's memory pool
  Result<std::shared_ptr<Buffer>> Allocate(int64_t nbytes);

  /// \brief Allocate buffer for bitmap from the context's memory pool
  Result<std::shared_ptr<Buffer>> AllocateBitmap(int64_t num_bits);

  /// \brief Indicate that an error has occurred, to be checked by a exec caller
  /// \param[in] status a Status instance
  ///
  /// \note Will not overwrite a prior set Status, so we will have the first
  /// error that occurred until ExecContext::ResetStatus is called
  void SetStatus(const Status& status);

  /// \brief Clear any error status
  void ResetStatus();

  /// \brief Return true if an error has occurred
  bool HasError() const { return !status_.ok(); }

  /// \brief Return the current status of the context
  const Status& status() const { return status_; }

  // For passing kernel state to
  void SetState(KernelState* state) { state_ = state; }

  KernelState* state() { return state_; }

  /// \brief Common state related to function execution
  ExecContext* exec_context() { return exec_ctx_; }

  MemoryPool* memory_pool() { return exec_ctx_->memory_pool(); }

 private:
  ExecContext* exec_ctx_;
  Status status_;
  KernelState* state_;
};

#define ARROW_CTX_RETURN_IF_ERROR(CTX)            \
  do {                                            \
    if (ARROW_PREDICT_FALSE((CTX)->HasError())) { \
      Status s = (CTX)->status();                 \
      (CTX)->ResetStatus();                       \
      return s;                                   \
    }                                             \
  } while (0)

/// A standard function taking zero or more Array/Scalar values and returning
/// Array/Scalar output. May be used for SCALAR and VECTOR kernel kinds. Should
/// write into pre-allocated memory except in cases when a builder
/// (e.g. StringBuilder) must be employed
using ArrayKernelExec = std::function<void(KernelContext*, const ExecBatch&, Datum*)>;

/// \brief An abstract type-checking interface to permit customizable
/// validation rules. This is for scenarios where the acceptance is not an
/// exact type instance along with its unit.
struct TypeMatcher {
  virtual ~TypeMatcher() = default;

  /// \brief Return true if this matcher accepts the data type
  virtual bool Matches(const DataType& type) const = 0;

  /// \brief A human-interpretable string representation of what the type
  /// matcher checks for, usable when printing KernelSignature or formatting
  /// error messages.
  virtual std::string ToString() const = 0;

  virtual bool Equals(const TypeMatcher& other) const = 0;
};

namespace match {

/// \brief Match any DataType instance having the same DataType::id
ARROW_EXPORT std::shared_ptr<TypeMatcher> SameTypeId(Type::type type_id);

/// \brief Match any TimestampType instance having the same unit, but the time
/// zones can be different
ARROW_EXPORT std::shared_ptr<TypeMatcher> TimestampUnit(TimeUnit::type unit);

}  // namespace match

/// \brief A container to express what kernel argument input types are accepted
class ARROW_EXPORT InputType {
 public:
  enum Kind {
    /// Accept any value type
    ANY_TYPE,

    /// A fixed arrow::DataType and will only exact match having this exact
    /// type (e.g. same TimestampType unit, same decimal scale and precision,
    /// or same nested child types
    EXACT_TYPE,

    /// Uses an TypeMatcher implementation to check the type
    USE_TYPE_MATCHER
  };

  InputType(ValueDescr::Shape shape = ValueDescr::ANY)  // NOLINT implicit construction
      : kind_(ANY_TYPE), shape_(shape) {}

  InputType(std::shared_ptr<DataType> type,
            ValueDescr::Shape shape = ValueDescr::ANY)  // NOLINT implicit construction
      : kind_(EXACT_TYPE), shape_(shape), type_(std::move(type)) {}

  InputType(const ValueDescr& descr)  // NOLINT implicit construction
      : InputType(descr.type, descr.shape) {}

  InputType(std::shared_ptr<TypeMatcher> type_matcher,
            ValueDescr::Shape shape = ValueDescr::ANY)
      : kind_(USE_TYPE_MATCHER), shape_(shape), type_matcher_(std::move(type_matcher)) {}

  explicit InputType(Type::type type_id, ValueDescr::Shape shape = ValueDescr::ANY)
      : InputType(match::SameTypeId(type_id), shape) {}

  InputType(const InputType& other) { CopyInto(other); }

  // Convenience ctors
  static InputType Array(std::shared_ptr<DataType> type) {
    return InputType(std::move(type), ValueDescr::ARRAY);
  }

  static InputType Scalar(std::shared_ptr<DataType> type) {
    return InputType(std::move(type), ValueDescr::SCALAR);
  }

  static InputType Array(Type::type id) { return InputType(id, ValueDescr::ARRAY); }

  static InputType Scalar(Type::type id) { return InputType(id, ValueDescr::SCALAR); }

  void operator=(const InputType& other) { CopyInto(other); }

  InputType(InputType&& other) { MoveInto(std::forward<InputType>(other)); }

  void operator=(InputType&& other) { MoveInto(std::forward<InputType>(other)); }

  /// \brief Return true if this type exactly matches another
  bool Equals(const InputType& other) const;

  bool operator==(const InputType& other) const { return this->Equals(other); }

  bool operator!=(const InputType& other) const { return !(*this == other); }

  /// \brief Return hash code
  size_t Hash() const;

  /// \brief Render a human-readable string representation
  std::string ToString() const;

  /// \brief Return true if the value matches this argument kind in type
  /// and shape
  bool Matches(const Datum& value) const;

  /// \brief Return true if the value descriptor matches this argument kind in
  /// type and shape
  bool Matches(const ValueDescr& value) const;

  /// \brief The type matching rule that this InputType uses
  Kind kind() const { return kind_; }

  ValueDescr::Shape shape() const { return shape_; }

  /// \brief For InputType::EXACT_TYPE, the exact type that this InputType must
  /// match. Otherwise this function should not be used
  const std::shared_ptr<DataType>& type() const;

  /// \brief For InputType::, the Type::type that this InputType must
  /// match, Otherwise this function should not be used
  const TypeMatcher& type_matcher() const;

 private:
  void CopyInto(const InputType& other) {
    this->kind_ = other.kind_;
    this->shape_ = other.shape_;
    this->type_ = other.type_;
    this->type_matcher_ = other.type_matcher_;
  }

  void MoveInto(InputType&& other) {
    this->kind_ = other.kind_;
    this->shape_ = other.shape_;
    this->type_ = std::move(other.type_);
    this->type_matcher_ = std::move(other.type_matcher_);
  }

  Kind kind_;

  ValueDescr::Shape shape_ = ValueDescr::ANY;

  // For EXACT_TYPE Kind
  std::shared_ptr<DataType> type_;

  // For USE_TYPE_MATCHER Kind
  std::shared_ptr<TypeMatcher> type_matcher_;
};

/// \brief Container to capture both exact and input-dependent output types
///
/// The value shape returned by Resolve will be determined by broadcasting the
/// shapes of the input arguments, otherwise this is handled by the
/// user-defined resolver function
///
/// * Any ARRAY shape -> output shape is ARRAY
/// * All SCALAR shapes -> output shape is SCALAR
class ARROW_EXPORT OutputType {
 public:
  /// \brief An enum indicating whether the value type is an invariant fixed
  /// value or one that's computed by a kernel-defined resolver function
  enum ResolveKind { FIXED, COMPUTED };

  /// Type resolution function. Given input types and shapes, return output
  /// type and shape. This function SHOULD _not_ be used to check for arity,
  /// that SHOULD be performed one or more layers above. May make use of kernel
  /// state to know what type to output
  using Resolver =
      std::function<Result<ValueDescr>(KernelContext*, const std::vector<ValueDescr>&)>;

  OutputType(std::shared_ptr<DataType> type)  // NOLINT implicit construction
      : kind_(FIXED), type_(std::move(type)) {}

  /// For outputting a particular type and shape
  OutputType(ValueDescr descr);  // NOLINT implicit construction

  explicit OutputType(Resolver resolver) : kind_(COMPUTED), resolver_(resolver) {}

  OutputType(const OutputType& other) {
    this->kind_ = other.kind_;
    this->shape_ = other.shape_;
    this->type_ = other.type_;
    this->resolver_ = other.resolver_;
  }

  OutputType(OutputType&& other) {
    this->kind_ = other.kind_;
    this->type_ = std::move(other.type_);
    this->shape_ = other.shape_;
    this->resolver_ = other.resolver_;
  }

  /// \brief Return the shape and type of the expected output value of the
  /// kernel given the value descriptors (shapes and types). The resolver may
  /// make use of state information kept in the KernelContext
  Result<ValueDescr> Resolve(KernelContext* ctx,
                             const std::vector<ValueDescr>& args) const;

  /// \brief The value type for the FIXED kind rule
  const std::shared_ptr<DataType>& type() const;

  /// \brief For use with COMPUTED resolution strategy, the output type depends
  /// on the input type. It may be more convenient to invoke this with
  /// OutputType::Resolve returned from this method
  const Resolver& resolver() const;

  /// \brief Render a human-readable string representation
  std::string ToString() const;

  /// \brief Return the kind of type resolution of this output type, whether
  /// fixed/invariant or computed by a "user"-defined resolver
  ResolveKind kind() const { return kind_; }

  /// \brief If the shape is ANY, then Resolve will compute the shape based on
  /// the input arguments
  ValueDescr::Shape shape() const { return shape_; }

 private:
  ResolveKind kind_;

  // For FIXED resolution
  std::shared_ptr<DataType> type_;

  ValueDescr::Shape shape_ = ValueDescr::ANY;

  // For COMPUTED resolution
  Resolver resolver_;
};

/// \brief Holds the input types and output type of the kernel
///
/// Varargs functions should pass a single input type to be used to validate
/// the the input types of a function invocation
class ARROW_EXPORT KernelSignature {
 public:
  KernelSignature(std::vector<InputType> in_types, OutputType out_type,
                  bool is_varargs = false);

  /// \brief Convenience ctor since make_shared can be awkward
  static std::shared_ptr<KernelSignature> Make(std::vector<InputType> in_types,
                                               OutputType out_type,
                                               bool is_varargs = false);

  /// \brief Return true if the signature if compatible with the list of input
  /// value descriptors
  bool MatchesInputs(const std::vector<ValueDescr>& descriptors) const;

  /// \brief Returns true if the input types of each signature are
  /// equal. Well-formed functions should have a deterministic output type
  /// given input types, but currently it is the responsibility of the
  /// developer to ensure this
  bool Equals(const KernelSignature& other) const;

  bool operator==(const KernelSignature& other) const { return this->Equals(other); }

  bool operator!=(const KernelSignature& other) const { return !(*this == other); }

  /// \brief Compute a hash code for the signature
  size_t Hash() const;

  const std::vector<InputType>& in_types() const { return in_types_; }

  const OutputType& out_type() const { return out_type_; }

  /// \brief Render a human-readable string representation
  std::string ToString() const;

  bool is_varargs() const { return is_varargs_; }

 private:
  std::vector<InputType> in_types_;
  OutputType out_type_;
  bool is_varargs_;

  // For caching the hash code after it's computed the first time
  mutable uint64_t hash_code_;
};

/// \brief A function may contain multiple variants of a kernel for a given
/// type combination for different SIMD levels. Based on the active system's
/// CPU info or the user's preferences, we can elect to use one over the other.
struct SimdLevel {
  enum type { NONE, SSE4_2, AVX, AVX2, AVX512, NEON };
};

struct NullHandling {
  enum type {
    /// Compute the output validity bitmap by intersecting the validity bitmaps
    /// of the arguments. Kernel does not do anything with the bitmap
    INTERSECTION,

    /// Kernel expects a pre-allocated buffer to write the result bitmap into
    COMPUTED_PREALLOCATE,

    /// Kernel allocates and populates the validity bitmap of the output
    COMPUTED_NO_PREALLOCATE,

    /// Output is never null
    OUTPUT_NOT_NULL
  };
};

struct MemAllocation {
  enum type {
    // For data types that support pre-allocation (fixed-type), the kernel
    // expects to be provided pre-allocated memory to write
    // into. Non-fixed-width must always allocate their own memory but perhaps
    // not their validity bitmaps. The allocation made for the same length as
    // the execution batch, so vector kernels yielding differently sized output
    // should not use this
    PREALLOCATE,

    // The kernel does its own memory allocation
    NO_PREALLOCATE
  };
};

struct Kernel;

struct KernelInitArgs {
  const Kernel* kernel;
  const std::vector<ValueDescr>& inputs;
  const FunctionOptions* options;
};

// Kernel initializer (context, argument descriptors, options)
using KernelInit =
    std::function<std::unique_ptr<KernelState>(KernelContext*, const KernelInitArgs&)>;

/// \brief Base type for kernels. Contains the function signature and
/// optionally the state initialization function, along with some common
/// attributes
struct Kernel {
  Kernel() {}

  Kernel(std::shared_ptr<KernelSignature> sig, KernelInit init)
      : signature(std::move(sig)), init(init) {}

  Kernel(std::vector<InputType> in_types, OutputType out_type, KernelInit init)
      : Kernel(KernelSignature::Make(std::move(in_types), out_type), init) {}

  std::shared_ptr<KernelSignature> signature;

  /// \brief Create a new KernelState for invocations of this kernel, e.g. to
  /// set up any options or state relevant for execution. May be nullptr
  KernelInit init;

  // Does execution benefit from parallelization (splitting large chunks into
  // smaller chunks and using multiple threads). Some vector kernels may
  // require single-threaded execution.
  bool parallelizable = true;

  /// \brief What level of SIMD instruction support in the host CPU is required
  /// to use the function
  SimdLevel::type simd_level = SimdLevel::NONE;
};

/// \brief Descriptor to hold signature and execution function implementations
/// for a particular kernel
struct ArrayKernel : public Kernel {
  ArrayKernel() {}

  ArrayKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec,
              KernelInit init = NULLPTR)
      : Kernel(std::move(sig), init), exec(exec) {}

  ArrayKernel(std::vector<InputType> in_types, OutputType out_type, ArrayKernelExec exec,
              KernelInit init = NULLPTR)
      : Kernel(std::move(in_types), std::move(out_type), init), exec(exec) {}

  /// \brief Perform a single invocation of this kernel. Depending on the
  /// implementation, it may only write into preallocated memory, while in some
  /// cases it will allocate its own memory.
  ArrayKernelExec exec;

  /// \brief Writing execution results into larger contiguous allocations
  /// requires that the kernel be able to write into sliced output
  /// ArrayData*. Some kernel implementations may not be able to do this, so
  /// setting this to false disables this functionality
  bool can_write_into_slices = true;
};

struct ScalarKernel : public ArrayKernel {
  using ArrayKernel::ArrayKernel;

  // For scalar functions preallocated data and intersecting arg validity
  // bitmaps is a reasonable default
  NullHandling::type null_handling = NullHandling::INTERSECTION;
  MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE;
};

// Convert intermediate results into finalized results. Mutates input argument
using VectorFinalize = std::function<void(KernelContext*, std::vector<Datum>*)>;

struct VectorKernel : public ArrayKernel {
  VectorKernel() {}

  VectorKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec)
      : ArrayKernel(std::move(sig), exec) {}

  VectorKernel(std::vector<InputType> in_types, OutputType out_type, ArrayKernelExec exec,
               KernelInit init = NULLPTR, VectorFinalize finalize = NULLPTR)
      : ArrayKernel(std::move(in_types), out_type, exec, init), finalize(finalize) {}

  VectorKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec,
               KernelInit init = NULLPTR, VectorFinalize finalize = NULLPTR)
      : ArrayKernel(std::move(sig), exec, init), finalize(finalize) {}

  VectorFinalize finalize;

  /// Since vector kernels generally are implemented rather differently from
  /// scalar/elementwise kernels (and they may not even yield arrays of the same
  /// size), so we make the developer opt-in to any memory preallocation rather
  /// than having to turn it off.
  NullHandling::type null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  MemAllocation::type mem_allocation = MemAllocation::NO_PREALLOCATE;

  /// Some vector kernels can do chunkwise execution using ExecBatchIterator,
  /// in some cases accumulating some state. Other kernels (like Take) need to
  /// be passed whole arrays and don't work on ChunkedArray inputs
  bool can_execute_chunkwise = true;

  /// Some kernels (like unique and value_counts) yield non-chunked output from
  /// chunked-array inputs. This option controls how the results are boxed when
  /// returned from ExecVectorFunction
  ///
  /// true -> ChunkedArray
  /// false -> Array
  ///
  /// TODO: Where is a better place to deal with this issue?
  bool output_chunked = true;
};

using ScalarAggregateConsume = std::function<void(KernelContext*, const ExecBatch&)>;

using ScalarAggregateMerge =
    std::function<void(KernelContext*, const KernelState&, KernelState*)>;

// Finalize returns Datum to permit multiple return values
using ScalarAggregateFinalize = std::function<void(KernelContext*, Datum*)>;

struct ScalarAggregateKernel : public Kernel {
  ScalarAggregateKernel() {}

  ScalarAggregateKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                        ScalarAggregateConsume consume, ScalarAggregateMerge merge,
                        ScalarAggregateFinalize finalize)
      : Kernel(std::move(sig), init),
        consume(consume),
        merge(merge),
        finalize(finalize) {}

  ScalarAggregateKernel(std::vector<InputType> in_types, OutputType out_type,
                        KernelInit init, ScalarAggregateConsume consume,
                        ScalarAggregateMerge merge, ScalarAggregateFinalize finalize)
      : ScalarAggregateKernel(KernelSignature::Make(std::move(in_types), out_type), init,
                              consume, merge, finalize) {}

  ScalarAggregateConsume consume;
  ScalarAggregateMerge merge;
  ScalarAggregateFinalize finalize;
};

}  // namespace compute
}  // namespace arrow
