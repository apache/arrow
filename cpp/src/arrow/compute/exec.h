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
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/compute/exec/expression.h"
#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/type_fwd.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

class CpuInfo;

}  // namespace internal

namespace compute {

class FunctionOptions;
class FunctionRegistry;

// It seems like 64K might be a good default chunksize to use for execution
// based on the experience of other query processing systems. The current
// default is not to chunk contiguous arrays, though, but this may change in
// the future once parallel execution is implemented
static constexpr int64_t kDefaultExecChunksize = UINT16_MAX;

/// \brief Context for expression-global variables and options used by
/// function evaluation
class ARROW_EXPORT ExecContext {
 public:
  // If no function registry passed, the default is used.
  explicit ExecContext(MemoryPool* pool = default_memory_pool(),
                       ::arrow::internal::Executor* executor = NULLPTR,
                       FunctionRegistry* func_registry = NULLPTR);

  /// \brief The MemoryPool used for allocations, default is
  /// default_memory_pool().
  MemoryPool* memory_pool() const { return pool_; }

  ::arrow::internal::CpuInfo* cpu_info() const;

  /// \brief An Executor which may be used to parallelize execution.
  ::arrow::internal::Executor* executor() const { return executor_; }

  /// \brief The FunctionRegistry for looking up functions by name and
  /// selecting kernels for execution. Defaults to the library-global function
  /// registry provided by GetFunctionRegistry.
  FunctionRegistry* func_registry() const { return func_registry_; }

  // \brief Set maximum length unit of work for kernel execution. Larger
  // contiguous array inputs will be split into smaller chunks, and, if
  // possible and enabled, processed in parallel. The default chunksize is
  // INT64_MAX, so contiguous arrays are not split.
  void set_exec_chunksize(int64_t chunksize) { exec_chunksize_ = chunksize; }

  // \brief Maximum length for ExecBatch data chunks processed by
  // kernels. Contiguous array inputs with longer length will be split into
  // smaller chunks.
  int64_t exec_chunksize() const { return exec_chunksize_; }

  /// \brief Set whether to use multiple threads for function execution. This
  /// is not yet used.
  void set_use_threads(bool use_threads = true) { use_threads_ = use_threads; }

  /// \brief If true, then utilize multiple threads where relevant for function
  /// execution. This is not yet used.
  bool use_threads() const { return use_threads_; }

  // Set the preallocation strategy for kernel execution as it relates to
  // chunked execution. For chunked execution, whether via ChunkedArray inputs
  // or splitting larger Array arguments into smaller pieces, contiguous
  // allocation (if permitted by the kernel) will allocate one large array to
  // write output into yielding it to the caller at the end. If this option is
  // set to off, then preallocations will be performed independently for each
  // chunk of execution
  //
  // TODO: At some point we might want the limit the size of contiguous
  // preallocations. For example, even if the exec_chunksize is 64K or less, we
  // might limit contiguous allocations to 1M records, say.
  void set_preallocate_contiguous(bool preallocate) {
    preallocate_contiguous_ = preallocate;
  }

  /// \brief If contiguous preallocations should be used when doing chunked
  /// execution as specified by exec_chunksize(). See
  /// set_preallocate_contiguous() for more information.
  bool preallocate_contiguous() const { return preallocate_contiguous_; }

 private:
  MemoryPool* pool_;
  ::arrow::internal::Executor* executor_;
  FunctionRegistry* func_registry_;
  int64_t exec_chunksize_ = std::numeric_limits<int64_t>::max();
  bool preallocate_contiguous_ = true;
  bool use_threads_ = true;
};

ARROW_EXPORT ExecContext* default_exec_context();

// TODO: Consider standardizing on uint16 selection vectors and only use them
// when we can ensure that each value is 64K length or smaller

/// \brief Container for an array of value selection indices that were
/// materialized from a filter.
///
/// Columnar query engines (see e.g. [1]) have found that rather than
/// materializing filtered data, the filter can instead be converted to an
/// array of the "on" indices and then "fusing" these indices in operator
/// implementations. This is especially relevant for aggregations but also
/// applies to scalar operations.
///
/// We are not yet using this so this is mostly a placeholder for now.
///
/// [1]: http://cidrdb.org/cidr2005/papers/P19.pdf
class ARROW_EXPORT SelectionVector {
 public:
  explicit SelectionVector(std::shared_ptr<ArrayData> data);

  explicit SelectionVector(const Array& arr);

  /// \brief Create SelectionVector from boolean mask
  static Result<std::shared_ptr<SelectionVector>> FromMask(const BooleanArray& arr);

  const int32_t* indices() const { return indices_; }
  int32_t length() const;

 private:
  std::shared_ptr<ArrayData> data_;
  const int32_t* indices_;
};

/// \brief A unit of work for kernel execution. It contains a collection of
/// Array and Scalar values and an optional SelectionVector indicating that
/// there is an unmaterialized filter that either must be materialized, or (if
/// the kernel supports it) pushed down into the kernel implementation.
///
/// ExecBatch is semantically similar to RecordBatch in that in a SQL context
/// it represents a collection of records, but constant "columns" are
/// represented by Scalar values rather than having to be converted into arrays
/// with repeated values.
///
/// TODO: Datum uses arrow/util/variant.h which may be a bit heavier-weight
/// than is desirable for this class. Microbenchmarks would help determine for
/// sure. See ARROW-8928.
struct ARROW_EXPORT ExecBatch {
  ExecBatch() = default;
  ExecBatch(std::vector<Datum> values, int64_t length)
      : values(std::move(values)), length(length) {}

  explicit ExecBatch(const RecordBatch& batch);

  static Result<ExecBatch> Make(std::vector<Datum> values);

  Result<std::shared_ptr<RecordBatch>> ToRecordBatch(
      std::shared_ptr<Schema> schema, MemoryPool* pool = default_memory_pool()) const;

  /// The values representing positional arguments to be passed to a kernel's
  /// exec function for processing.
  std::vector<Datum> values;

  /// A deferred filter represented as an array of indices into the values.
  ///
  /// For example, the filter [true, true, false, true] would be represented as
  /// the selection vector [0, 1, 3]. When the selection vector is set,
  /// ExecBatch::length is equal to the length of this array.
  std::shared_ptr<SelectionVector> selection_vector;

  /// A predicate Expression guaranteed to evaluate to true for all rows in this batch.
  Expression guarantee = literal(true);

  /// The semantic length of the ExecBatch. When the values are all scalars,
  /// the length should be set to 1 for non-aggregate kernels, otherwise the
  /// length is taken from the array values, except when there is a selection
  /// vector. When there is a selection vector set, the length of the batch is
  /// the length of the selection. Aggregate kernels can have an ExecBatch
  /// formed by projecting just the partition columns from a batch in which
  /// case, it would have scalar rows with length greater than 1.
  ///
  /// If the array values are of length 0 then the length is 0 regardless of
  /// whether any values are Scalar. In general ExecBatch objects are produced
  /// by ExecBatchIterator which by design does not yield length-0 batches.
  int64_t length;

  /// \brief Return the value at the i-th index
  template <typename index_type>
  inline const Datum& operator[](index_type i) const {
    return values[i];
  }

  bool Equals(const ExecBatch& other) const;

  /// \brief A convenience for the number of values / arguments.
  int num_values() const { return static_cast<int>(values.size()); }

  ExecBatch Slice(int64_t offset, int64_t length) const;

  /// \brief A convenience for returning the ValueDescr objects (types and
  /// shapes) from the batch.
  std::vector<ValueDescr> GetDescriptors() const {
    std::vector<ValueDescr> result;
    for (const auto& value : this->values) {
      result.emplace_back(value.descr());
    }
    return result;
  }

  std::string ToString() const;

  ARROW_EXPORT friend void PrintTo(const ExecBatch&, std::ostream*);
};

inline bool operator==(const ExecBatch& l, const ExecBatch& r) { return l.Equals(r); }
inline bool operator!=(const ExecBatch& l, const ExecBatch& r) { return !l.Equals(r); }

/// \defgroup compute-call-function One-shot calls to compute functions
///
/// @{

/// \brief One-shot invoker for all types of functions.
///
/// Does kernel dispatch, argument checking, iteration of ChunkedArray inputs,
/// and wrapping of outputs.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           const FunctionOptions* options, ExecContext* ctx = NULLPTR);

/// \brief Variant of CallFunction which uses a function's default options.
///
/// NB: Some functions require FunctionOptions be provided.
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx = NULLPTR);

/// @}

}  // namespace compute
}  // namespace arrow
