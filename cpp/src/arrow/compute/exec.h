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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/datum.h"
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

namespace internal {

class CpuInfo;

}  // namespace internal

namespace compute {

struct FunctionOptions;
class FunctionRegistry;

// It seems like 64K might be a good default chunksize to use for execution
// based on the experience of other query processing systems, so using this for
// now.
static constexpr int64_t kDefaultExecChunksize = UINT16_MAX;

/// \brief Context for expression-global variables and options used by
/// function evaluation
class ARROW_EXPORT ExecContext {
 public:
  // If no function registry passed, the default is used
  explicit ExecContext(MemoryPool* pool = default_memory_pool(),
                       FunctionRegistry* func_registry = NULLPTR);

  MemoryPool* memory_pool() const { return pool_; }

  internal::CpuInfo* cpu_info() const;

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
  void set_preallocate_contiguous(bool preallocate = true) {
    preallocate_contiguous_ = preallocate;
  }

  bool preallocate_contiguous() const { return preallocate_contiguous_; }

 private:
  MemoryPool* pool_;
  FunctionRegistry* func_registry_;
  int64_t exec_chunksize_ = -1;
  bool preallocate_contiguous_ = true;
  bool use_threads_ = true;
};

// TODO: Consider standardizing on uint16 selection vectors and only use them
// when we can ensure that each value is 64K length or smaller

/// \brief Container for a int32 selection
class ARROW_EXPORT SelectionVector {
 public:
  explicit SelectionVector(std::shared_ptr<ArrayData> data);

  explicit SelectionVector(const Array& arr) : SelectionVector(arr.data()) {}

  /// \brief Create SelectionVector from boolean mask
  static Result<std::shared_ptr<SelectionVector>> FromMask(const Array& arr);

  int32_t index(int i) const { return indices_[i]; }
  const int32_t* indices() const { return indices_; }
  int32_t length() const { return static_cast<int32_t>(data_->length); }

 private:
  std::shared_ptr<ArrayData> data_;
  const int32_t* indices_;
};

struct ExecBatch {
  ExecBatch() {}
  ExecBatch(std::vector<Datum> values, int64_t length)
      : values(std::move(values)), length(length) {}

  std::vector<Datum> values;
  std::shared_ptr<SelectionVector> selection_vector;
  int64_t length;
  const Datum& operator[](int i) const { return values[i]; }

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
Result<Datum> CallFunction(ExecContext* ctx, const std::string& func_name,
                           const std::vector<Datum>& args,
                           const FunctionOptions* options = NULLPTR);

}  // namespace compute
}  // namespace arrow
