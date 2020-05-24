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

#include "arrow/datum.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
struct ArrayData;
class MemoryPool;

namespace internal {

class CpuInfo;

}  // namespace internal

namespace compute {

struct FunctionOptions;
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
  // If no function registry passed, the default is used
  explicit ExecContext(MemoryPool* pool = default_memory_pool(),
                       FunctionRegistry* func_registry = NULLPTR);

  MemoryPool* memory_pool() const { return pool_; }

  ::arrow::internal::CpuInfo* cpu_info() const;

  FunctionRegistry* func_registry() const { return func_registry_; }

  // \brief Set maximum length unit of work for kernel execution. Larger inputs
  // will be split into smaller chunks, and, if desired, processed in
  // parallel. Set to -1 for no limit
  void set_exec_chunksize(int64_t chunksize) { exec_chunksize_ = chunksize; }

  // \brief Maximum length unit of work for kernel execution.
  int64_t exec_chunksize() const { return exec_chunksize_; }

  /// \brief Set whether to use multiple threads for function execution
  void set_use_threads(bool use_threads = true) { use_threads_ = use_threads; }

  /// \brief If true, then utilize multiple threads where relevant for function
  /// execution
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
  // preallocations (for example, merging small ChunkedArray chunks until
  // reaching some desired size)
  void set_preallocate_contiguous(bool preallocate) {
    preallocate_contiguous_ = preallocate;
  }

  bool preallocate_contiguous() const { return preallocate_contiguous_; }

 private:
  MemoryPool* pool_;
  FunctionRegistry* func_registry_;
  int64_t exec_chunksize_ = std::numeric_limits<int64_t>::max();
  bool preallocate_contiguous_ = true;
  bool use_threads_ = true;
};

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
struct ExecBatch {
  ExecBatch() {}
  ExecBatch(std::vector<Datum> values, int64_t length)
      : values(std::move(values)), length(length) {}

  std::vector<Datum> values;
  std::shared_ptr<SelectionVector> selection_vector;
  int64_t length;

  template <typename index_type>
  inline const Datum& operator[](index_type i) const {
    return values[i];
  }

  int num_values() const { return static_cast<int>(values.size()); }

  std::vector<ValueDescr> GetDescriptors() const {
    std::vector<ValueDescr> result;
    for (const auto& value : this->values) {
      result.emplace_back(value.descr());
    }
    return result;
  }
};

/// \brief One-shot invoker for all types of functions. Does kernel dispatch,
/// argument checking, iteration of ChunkedArray inputs, and wrapping of
/// outputs
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           const FunctionOptions* options, ExecContext* ctx = NULLPTR);

/// \brief Variant of CallFunction for functions not requiring options
ARROW_EXPORT
Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx = NULLPTR);

}  // namespace compute
}  // namespace arrow
