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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class FunctionOptions;

/// \brief Base class for opaque kernel-specific state. For example, if there
/// is some kind of initialization required.
struct ARROW_EXPORT KernelState {
  virtual ~KernelState() = default;
};

/// \brief Context/state for the execution of a particular kernel.
class ARROW_EXPORT KernelContext {
 public:
  explicit KernelContext(ExecContext* exec_ctx) : exec_ctx_(exec_ctx) {}

  /// \brief Allocate buffer from the context's memory pool. The contents are
  /// not initialized.
  Result<std::shared_ptr<ResizableBuffer>> Allocate(int64_t nbytes);

  /// \brief Allocate buffer for bitmap from the context's memory pool. Like
  /// Allocate, the contents of the buffer are not initialized but the last
  /// byte is preemptively zeroed to help avoid ASAN or valgrind issues.
  Result<std::shared_ptr<ResizableBuffer>> AllocateBitmap(int64_t num_bits);

  /// \brief Assign the active KernelState to be utilized for each stage of
  /// kernel execution. Ownership and memory lifetime of the KernelState must
  /// be minded separately.
  void SetState(KernelState* state) { state_ = state; }

  KernelState* state() { return state_; }

  /// \brief Configuration related to function execution that is to be shared
  /// across multiple kernels.
  ExecContext* exec_context() { return exec_ctx_; }

  /// \brief The memory pool to use for allocations. For now, it uses the
  /// MemoryPool contained in the ExecContext used to create the KernelContext.
  MemoryPool* memory_pool() { return exec_ctx_->memory_pool(); }

 private:
  ExecContext* exec_ctx_;
  KernelState* state_ = NULLPTR;
};

/// \brief The standard kernel execution API that must be implemented for
/// SCALAR and VECTOR kernel types. This includes both stateless and stateful
/// kernels. Kernels depending on some execution state access that state via
/// subclasses of KernelState set on the KernelContext object. May be used for
/// SCALAR and VECTOR kernel kinds. Implementations should endeavor to write
/// into pre-allocated memory if they are able, though for some kernels
/// (e.g. in cases when a builder like StringBuilder) must be employed this may
/// not be possible.
using ArrayKernelExec = std::function<Status(KernelContext*, const ExecBatch&, Datum*)>;

/// \brief An type-checking interface to permit customizable validation rules
/// for use with InputType and KernelSignature. This is for scenarios where the
/// acceptance is not an exact type instance, such as a TIMESTAMP type for a
/// specific TimeUnit, but permitting any time zone.
struct ARROW_EXPORT TypeMatcher {
  virtual ~TypeMatcher() = default;

  /// \brief Return true if this matcher accepts the data type.
  virtual bool Matches(const DataType& type) const = 0;

  /// \brief A human-interpretable string representation of what the type
  /// matcher checks for, usable when printing KernelSignature or formatting
  /// error messages.
  virtual std::string ToString() const = 0;

  /// \brief Return true if this TypeMatcher contains the same matching rule as
  /// the other. Currently depends on RTTI.
  virtual bool Equals(const TypeMatcher& other) const = 0;
};

namespace match {

/// \brief Match any DataType instance having the same DataType::id.
ARROW_EXPORT std::shared_ptr<TypeMatcher> SameTypeId(Type::type type_id);

/// \brief Match any TimestampType instance having the same unit, but the time
/// zones can be different.
ARROW_EXPORT std::shared_ptr<TypeMatcher> TimestampTypeUnit(TimeUnit::type unit);
ARROW_EXPORT std::shared_ptr<TypeMatcher> Time32TypeUnit(TimeUnit::type unit);
ARROW_EXPORT std::shared_ptr<TypeMatcher> Time64TypeUnit(TimeUnit::type unit);
ARROW_EXPORT std::shared_ptr<TypeMatcher> DurationTypeUnit(TimeUnit::type unit);

// \brief Match any integer type
ARROW_EXPORT std::shared_ptr<TypeMatcher> Integer();

// Match types using 32-bit varbinary representation
ARROW_EXPORT std::shared_ptr<TypeMatcher> BinaryLike();

// Match types using 64-bit varbinary representation
ARROW_EXPORT std::shared_ptr<TypeMatcher> LargeBinaryLike();

// \brief Match any primitive type (boolean or any type representable as a C
// Type)
ARROW_EXPORT std::shared_ptr<TypeMatcher> Primitive();

}  // namespace match

/// \brief An object used for type- and shape-checking arguments to be passed
/// to a kernel and stored in a KernelSignature. Distinguishes between ARRAY
/// and SCALAR arguments using ValueDescr::Shape. The type-checking rule can be
/// supplied either with an exact DataType instance or a custom TypeMatcher.
class ARROW_EXPORT InputType {
 public:
  /// \brief The kind of type-checking rule that the InputType contains.
  enum Kind {
    /// \brief Accept any value type.
    ANY_TYPE,

    /// \brief A fixed arrow::DataType and will only exact match having this
    /// exact type (e.g. same TimestampType unit, same decimal scale and
    /// precision, or same nested child types).
    EXACT_TYPE,

    /// \brief Uses a TypeMatcher implementation to check the type.
    USE_TYPE_MATCHER
  };

  /// \brief Accept any value type but with a specific shape (e.g. any Array or
  /// any Scalar).
  InputType(ValueDescr::Shape shape = ValueDescr::ANY)  // NOLINT implicit construction
      : kind_(ANY_TYPE), shape_(shape) {}

  /// \brief Accept an exact value type.
  InputType(std::shared_ptr<DataType> type,  // NOLINT implicit construction
            ValueDescr::Shape shape = ValueDescr::ANY)
      : kind_(EXACT_TYPE), shape_(shape), type_(std::move(type)) {}

  /// \brief Accept an exact value type and shape provided by a ValueDescr.
  InputType(const ValueDescr& descr)  // NOLINT implicit construction
      : InputType(descr.type, descr.shape) {}

  /// \brief Use the passed TypeMatcher to type check.
  InputType(std::shared_ptr<TypeMatcher> type_matcher,  // NOLINT implicit construction
            ValueDescr::Shape shape = ValueDescr::ANY)
      : kind_(USE_TYPE_MATCHER), shape_(shape), type_matcher_(std::move(type_matcher)) {}

  /// \brief Match any type with the given Type::type. Uses a TypeMatcher for
  /// its implementation.
  explicit InputType(Type::type type_id, ValueDescr::Shape shape = ValueDescr::ANY)
      : InputType(match::SameTypeId(type_id), shape) {}

  InputType(const InputType& other) { CopyInto(other); }

  void operator=(const InputType& other) { CopyInto(other); }

  InputType(InputType&& other) { MoveInto(std::forward<InputType>(other)); }

  void operator=(InputType&& other) { MoveInto(std::forward<InputType>(other)); }

  // \brief Match an array with the given exact type. Convenience constructor.
  static InputType Array(std::shared_ptr<DataType> type) {
    return InputType(std::move(type), ValueDescr::ARRAY);
  }

  // \brief Match a scalar with the given exact type. Convenience constructor.
  static InputType Scalar(std::shared_ptr<DataType> type) {
    return InputType(std::move(type), ValueDescr::SCALAR);
  }

  // \brief Match an array with the given Type::type id. Convenience
  // constructor.
  static InputType Array(Type::type id) { return InputType(id, ValueDescr::ARRAY); }

  // \brief Match a scalar with the given Type::type id. Convenience
  // constructor.
  static InputType Scalar(Type::type id) { return InputType(id, ValueDescr::SCALAR); }

  /// \brief Return true if this input type matches the same type cases as the
  /// other.
  bool Equals(const InputType& other) const;

  bool operator==(const InputType& other) const { return this->Equals(other); }

  bool operator!=(const InputType& other) const { return !(*this == other); }

  /// \brief Return hash code.
  size_t Hash() const;

  /// \brief Render a human-readable string representation.
  std::string ToString() const;

  /// \brief Return true if the value matches this argument kind in type
  /// and shape.
  bool Matches(const Datum& value) const;

  /// \brief Return true if the value descriptor matches this argument kind in
  /// type and shape.
  bool Matches(const ValueDescr& value) const;

  /// \brief The type matching rule that this InputType uses.
  Kind kind() const { return kind_; }

  /// \brief Indicates whether this InputType matches Array (ValueDescr::ARRAY),
  /// Scalar (ValueDescr::SCALAR) values, or both (ValueDescr::ANY).
  ValueDescr::Shape shape() const { return shape_; }

  /// \brief For InputType::EXACT_TYPE kind, the exact type that this InputType
  /// must match. Otherwise this function should not be used and will assert in
  /// debug builds.
  const std::shared_ptr<DataType>& type() const;

  /// \brief For InputType::USE_TYPE_MATCHER, the TypeMatcher to be used for
  /// checking the type of a value. Otherwise this function should not be used
  /// and will assert in debug builds.
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

/// \brief Container to capture both exact and input-dependent output types.
///
/// The value shape returned by Resolve will be determined by broadcasting the
/// shapes of the input arguments, otherwise this is handled by the
/// user-defined resolver function:
///
/// * Any ARRAY shape -> output shape is ARRAY
/// * All SCALAR shapes -> output shape is SCALAR
class ARROW_EXPORT OutputType {
 public:
  /// \brief An enum indicating whether the value type is an invariant fixed
  /// value or one that's computed by a kernel-defined resolver function.
  enum ResolveKind { FIXED, COMPUTED };

  /// Type resolution function. Given input types and shapes, return output
  /// type and shape. This function SHOULD _not_ be used to check for arity,
  /// that is to be performed one or more layers above. May make use of kernel
  /// state to know what type to output in some cases.
  using Resolver =
      std::function<Result<ValueDescr>(KernelContext*, const std::vector<ValueDescr>&)>;

  /// \brief Output an exact type, but with shape determined by promoting the
  /// shapes of the inputs (any ARRAY argument yields ARRAY).
  OutputType(std::shared_ptr<DataType> type)  // NOLINT implicit construction
      : kind_(FIXED), type_(std::move(type)) {}

  /// \brief Output the exact type and shape provided by a ValueDescr
  OutputType(ValueDescr descr);  // NOLINT implicit construction

  explicit OutputType(Resolver resolver)
      : kind_(COMPUTED), resolver_(std::move(resolver)) {}

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

  OutputType& operator=(const OutputType&) = default;
  OutputType& operator=(OutputType&&) = default;

  /// \brief Return the shape and type of the expected output value of the
  /// kernel given the value descriptors (shapes and types) of the input
  /// arguments. The resolver may make use of state information kept in the
  /// KernelContext.
  Result<ValueDescr> Resolve(KernelContext* ctx,
                             const std::vector<ValueDescr>& args) const;

  /// \brief The exact output value type for the FIXED kind.
  const std::shared_ptr<DataType>& type() const;

  /// \brief For use with COMPUTED resolution strategy. It may be more
  /// convenient to invoke this with OutputType::Resolve returned from this
  /// method.
  const Resolver& resolver() const;

  /// \brief Render a human-readable string representation.
  std::string ToString() const;

  /// \brief Return the kind of type resolution of this output type, whether
  /// fixed/invariant or computed by a resolver.
  ResolveKind kind() const { return kind_; }

  /// \brief If the shape is ANY, then Resolve will compute the shape based on
  /// the input arguments.
  ValueDescr::Shape shape() const { return shape_; }

 private:
  ResolveKind kind_;

  // For FIXED resolution
  std::shared_ptr<DataType> type_;

  /// \brief The shape of the output type to return when using Resolve. If ANY
  /// will promote the input shapes.
  ValueDescr::Shape shape_ = ValueDescr::ANY;

  // For COMPUTED resolution
  Resolver resolver_;
};

/// \brief Holds the input types and output type of the kernel.
///
/// VarArgs functions with minimum N arguments should pass up to N input types to be
/// used to validate the input types of a function invocation. The first N-1 types
/// will be matched against the first N-1 arguments, and the last type will be
/// matched against the remaining arguments.
class ARROW_EXPORT KernelSignature {
 public:
  KernelSignature(std::vector<InputType> in_types, OutputType out_type,
                  bool is_varargs = false);

  /// \brief Convenience ctor since make_shared can be awkward
  static std::shared_ptr<KernelSignature> Make(std::vector<InputType> in_types,
                                               OutputType out_type,
                                               bool is_varargs = false);

  /// \brief Return true if the signature if compatible with the list of input
  /// value descriptors.
  bool MatchesInputs(const std::vector<ValueDescr>& descriptors) const;

  /// \brief Returns true if the input types of each signature are
  /// equal. Well-formed functions should have a deterministic output type
  /// given input types, but currently it is the responsibility of the
  /// developer to ensure this.
  bool Equals(const KernelSignature& other) const;

  bool operator==(const KernelSignature& other) const { return this->Equals(other); }

  bool operator!=(const KernelSignature& other) const { return !(*this == other); }

  /// \brief Compute a hash code for the signature
  size_t Hash() const;

  /// \brief The input types for the kernel. For VarArgs functions, this should
  /// generally contain a single validator to use for validating all of the
  /// function arguments.
  const std::vector<InputType>& in_types() const { return in_types_; }

  /// \brief The output type for the kernel. Use Resolve to return the exact
  /// output given input argument ValueDescrs, since many kernels' output types
  /// depend on their input types (or their type metadata).
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
  enum type { NONE = 0, SSE4_2, AVX, AVX2, AVX512, NEON, MAX };
};

/// \brief The strategy to use for propagating or otherwise populating the
/// validity bitmap of a kernel output.
struct NullHandling {
  enum type {
    /// Compute the output validity bitmap by intersecting the validity bitmaps
    /// of the arguments using bitwise-and operations. This means that values
    /// in the output are valid/non-null only if the corresponding values in
    /// all input arguments were valid/non-null. Kernel generally need not
    /// touch the bitmap thereafter, but a kernel's exec function is permitted
    /// to alter the bitmap after the null intersection is computed if it needs
    /// to.
    INTERSECTION,

    /// Kernel expects a pre-allocated buffer to write the result bitmap
    /// into. The preallocated memory is not zeroed (except for the last byte),
    /// so the kernel should ensure to completely populate the bitmap.
    COMPUTED_PREALLOCATE,

    /// Kernel allocates and sets the validity bitmap of the output.
    COMPUTED_NO_PREALLOCATE,

    /// Kernel output is never null and a validity bitmap does not need to be
    /// allocated.
    OUTPUT_NOT_NULL
  };
};

/// \brief The preference for memory preallocation of fixed-width type outputs
/// in kernel execution.
struct MemAllocation {
  enum type {
    // For data types that support pre-allocation (i.e. fixed-width), the
    // kernel expects to be provided a pre-allocated data buffer to write
    // into. Non-fixed-width types must always allocate their own data
    // buffers. The allocation made for the same length as the execution batch,
    // so vector kernels yielding differently sized output should not use this.
    //
    // It is valid for the data to not be preallocated but the validity bitmap
    // is (or is computed using the intersection/bitwise-and method).
    //
    // For variable-size output types like BinaryType or StringType, or for
    // nested types, this option has no effect.
    PREALLOCATE,

    // The kernel is responsible for allocating its own data buffer for
    // fixed-width type outputs.
    NO_PREALLOCATE
  };
};

struct Kernel;

/// \brief Arguments to pass to a KernelInit function. A struct is used to help
/// avoid API breakage should the arguments passed need to be expanded.
struct KernelInitArgs {
  /// \brief A pointer to the kernel being initialized. The init function may
  /// depend on the kernel's KernelSignature or other data contained there.
  const Kernel* kernel;

  /// \brief The types and shapes of the input arguments that the kernel is
  /// about to be executed against.
  ///
  /// TODO: should this be const std::vector<ValueDescr>*? const-ref is being
  /// used to avoid the cost of copying the struct into the args struct.
  const std::vector<ValueDescr>& inputs;

  /// \brief Opaque options specific to this kernel. May be nullptr for functions
  /// that do not require options.
  const FunctionOptions* options;
};

/// \brief Common initializer function for all kernel types.
using KernelInit = std::function<Result<std::unique_ptr<KernelState>>(
    KernelContext*, const KernelInitArgs&)>;

/// \brief Base type for kernels. Contains the function signature and
/// optionally the state initialization function, along with some common
/// attributes
struct Kernel {
  Kernel() = default;

  Kernel(std::shared_ptr<KernelSignature> sig, KernelInit init)
      : signature(std::move(sig)), init(std::move(init)) {}

  Kernel(std::vector<InputType> in_types, OutputType out_type, KernelInit init)
      : Kernel(KernelSignature::Make(std::move(in_types), std::move(out_type)),
               std::move(init)) {}

  /// \brief The "signature" of the kernel containing the InputType input
  /// argument validators and OutputType output type and shape resolver.
  std::shared_ptr<KernelSignature> signature;

  /// \brief Create a new KernelState for invocations of this kernel, e.g. to
  /// set up any options or state relevant for execution.
  KernelInit init;

  /// \brief Create a vector of new KernelState for invocations of this kernel.
  static Status InitAll(KernelContext*, const KernelInitArgs&,
                        std::vector<std::unique_ptr<KernelState>>*);

  /// \brief Indicates whether execution can benefit from parallelization
  /// (splitting large chunks into smaller chunks and using multiple
  /// threads). Some kernels may not support parallel execution at
  /// all. Synchronization and concurrency-related issues are currently the
  /// responsibility of the Kernel's implementation.
  bool parallelizable = true;

  /// \brief Indicates the level of SIMD instruction support in the host CPU is
  /// required to use the function. The intention is for functions to be able to
  /// contain multiple kernels with the same signature but different levels of SIMD,
  /// so that the most optimized kernel supported on a host's processor can be chosen.
  SimdLevel::type simd_level = SimdLevel::NONE;
};

/// \brief Common kernel base data structure for ScalarKernel and
/// VectorKernel. It is called "ArrayKernel" in that the functions generally
/// output array values (as opposed to scalar values in the case of aggregate
/// functions).
struct ArrayKernel : public Kernel {
  ArrayKernel() = default;

  ArrayKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec,
              KernelInit init = NULLPTR)
      : Kernel(std::move(sig), init), exec(std::move(exec)) {}

  ArrayKernel(std::vector<InputType> in_types, OutputType out_type, ArrayKernelExec exec,
              KernelInit init = NULLPTR)
      : Kernel(std::move(in_types), std::move(out_type), std::move(init)),
        exec(std::move(exec)) {}

  /// \brief Perform a single invocation of this kernel. Depending on the
  /// implementation, it may only write into preallocated memory, while in some
  /// cases it will allocate its own memory. Any required state is managed
  /// through the KernelContext.
  ArrayKernelExec exec;

  /// \brief Writing execution results into larger contiguous allocations
  /// requires that the kernel be able to write into sliced output ArrayData*,
  /// including sliced output validity bitmaps. Some kernel implementations may
  /// not be able to do this, so setting this to false disables this
  /// functionality.
  bool can_write_into_slices = true;
};

/// \brief Kernel data structure for implementations of ScalarFunction. In
/// addition to the members found in ArrayKernel, contains the null handling
/// and memory pre-allocation preferences.
struct ScalarKernel : public ArrayKernel {
  using ArrayKernel::ArrayKernel;

  // For scalar functions preallocated data and intersecting arg validity
  // bitmaps is a reasonable default
  NullHandling::type null_handling = NullHandling::INTERSECTION;
  MemAllocation::type mem_allocation = MemAllocation::PREALLOCATE;
};

// ----------------------------------------------------------------------
// VectorKernel (for VectorFunction)

/// \brief See VectorKernel::finalize member for usage
using VectorFinalize = std::function<Status(KernelContext*, std::vector<Datum>*)>;

/// \brief Kernel data structure for implementations of VectorFunction. In
/// addition to the members found in ArrayKernel, contains an optional
/// finalizer function, the null handling and memory pre-allocation preferences
/// (which have different defaults from ScalarKernel), and some other
/// execution-related options.
struct VectorKernel : public ArrayKernel {
  VectorKernel() = default;

  VectorKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec)
      : ArrayKernel(std::move(sig), std::move(exec)) {}

  VectorKernel(std::vector<InputType> in_types, OutputType out_type, ArrayKernelExec exec,
               KernelInit init = NULLPTR, VectorFinalize finalize = NULLPTR)
      : ArrayKernel(std::move(in_types), std::move(out_type), std::move(exec),
                    std::move(init)),
        finalize(std::move(finalize)) {}

  VectorKernel(std::shared_ptr<KernelSignature> sig, ArrayKernelExec exec,
               KernelInit init = NULLPTR, VectorFinalize finalize = NULLPTR)
      : ArrayKernel(std::move(sig), std::move(exec), std::move(init)),
        finalize(std::move(finalize)) {}

  /// \brief For VectorKernel, convert intermediate results into finalized
  /// results. Mutates input argument. Some kernels may accumulate state
  /// (example: hashing-related functions) through processing chunked inputs, and
  /// then need to attach some accumulated state to each of the outputs of
  /// processing each chunk of data.
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
  bool output_chunked = true;
};

// ----------------------------------------------------------------------
// ScalarAggregateKernel (for ScalarAggregateFunction)

using ScalarAggregateConsume = std::function<Status(KernelContext*, const ExecBatch&)>;

using ScalarAggregateMerge =
    std::function<Status(KernelContext*, KernelState&&, KernelState*)>;

// Finalize returns Datum to permit multiple return values
using ScalarAggregateFinalize = std::function<Status(KernelContext*, Datum*)>;

/// \brief Kernel data structure for implementations of
/// ScalarAggregateFunction. The four necessary components of an aggregation
/// kernel are the init, consume, merge, and finalize functions.
///
/// * init: creates a new KernelState for a kernel.
/// * consume: processes an ExecBatch and updates the KernelState found in the
///   KernelContext.
/// * merge: combines one KernelState with another.
/// * finalize: produces the end result of the aggregation using the
///   KernelState in the KernelContext.
struct ScalarAggregateKernel : public Kernel {
  ScalarAggregateKernel() = default;

  ScalarAggregateKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                        ScalarAggregateConsume consume, ScalarAggregateMerge merge,
                        ScalarAggregateFinalize finalize)
      : Kernel(std::move(sig), std::move(init)),
        consume(std::move(consume)),
        merge(std::move(merge)),
        finalize(std::move(finalize)) {}

  ScalarAggregateKernel(std::vector<InputType> in_types, OutputType out_type,
                        KernelInit init, ScalarAggregateConsume consume,
                        ScalarAggregateMerge merge, ScalarAggregateFinalize finalize)
      : ScalarAggregateKernel(
            KernelSignature::Make(std::move(in_types), std::move(out_type)),
            std::move(init), std::move(consume), std::move(merge), std::move(finalize)) {}

  /// \brief Merge a vector of KernelStates into a single KernelState.
  /// The merged state will be returned and will be set on the KernelContext.
  static Result<std::unique_ptr<KernelState>> MergeAll(
      const ScalarAggregateKernel* kernel, KernelContext* ctx,
      std::vector<std::unique_ptr<KernelState>> states);

  ScalarAggregateConsume consume;
  ScalarAggregateMerge merge;
  ScalarAggregateFinalize finalize;
};

// ----------------------------------------------------------------------
// HashAggregateKernel (for HashAggregateFunction)

using HashAggregateResize = std::function<Status(KernelContext*, int64_t)>;

using HashAggregateConsume = std::function<Status(KernelContext*, const ExecBatch&)>;

using HashAggregateMerge =
    std::function<Status(KernelContext*, KernelState&&, const ArrayData&)>;

// Finalize returns Datum to permit multiple return values
using HashAggregateFinalize = std::function<Status(KernelContext*, Datum*)>;

/// \brief Kernel data structure for implementations of
/// HashAggregateFunction. The four necessary components of an aggregation
/// kernel are the init, consume, merge, and finalize functions.
///
/// * init: creates a new KernelState for a kernel.
/// * resize: ensure that the KernelState can accommodate the specified number of groups.
/// * consume: processes an ExecBatch (which includes the argument as well
///   as an array of group identifiers) and updates the KernelState found in the
///   KernelContext.
/// * merge: combines one KernelState with another.
/// * finalize: produces the end result of the aggregation using the
///   KernelState in the KernelContext.
struct HashAggregateKernel : public Kernel {
  HashAggregateKernel() = default;

  HashAggregateKernel(std::shared_ptr<KernelSignature> sig, KernelInit init,
                      HashAggregateResize resize, HashAggregateConsume consume,
                      HashAggregateMerge merge, HashAggregateFinalize finalize)
      : Kernel(std::move(sig), std::move(init)),
        resize(std::move(resize)),
        consume(std::move(consume)),
        merge(std::move(merge)),
        finalize(std::move(finalize)) {}

  HashAggregateKernel(std::vector<InputType> in_types, OutputType out_type,
                      KernelInit init, HashAggregateConsume consume,
                      HashAggregateResize resize, HashAggregateMerge merge,
                      HashAggregateFinalize finalize)
      : HashAggregateKernel(
            KernelSignature::Make(std::move(in_types), std::move(out_type)),
            std::move(init), std::move(resize), std::move(consume), std::move(merge),
            std::move(finalize)) {}

  HashAggregateResize resize;
  HashAggregateConsume consume;
  HashAggregateMerge merge;
  HashAggregateFinalize finalize;
};

}  // namespace compute
}  // namespace arrow
