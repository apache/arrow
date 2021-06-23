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

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace compute {

class Function;

static constexpr int64_t kDefaultMaxChunksize = std::numeric_limits<int64_t>::max();

namespace detail {

/// \brief Break std::vector<Datum> into a sequence of ExecBatch for kernel
/// execution
class ARROW_EXPORT ExecBatchIterator {
 public:
  /// \brief Construct iterator and do basic argument validation
  ///
  /// \param[in] args the Datum argument, must be all array-like or scalar
  /// \param[in] max_chunksize the maximum length of each ExecBatch. Depending
  /// on the chunk layout of ChunkedArray.
  static Result<std::unique_ptr<ExecBatchIterator>> Make(
      std::vector<Datum> args, int64_t max_chunksize = kDefaultMaxChunksize);

  /// \brief Compute the next batch. Always returns at least one batch. Return
  /// false if the iterator is exhausted
  bool Next(ExecBatch* batch);

  int64_t length() const { return length_; }

  int64_t position() const { return position_; }

  int64_t max_chunksize() const { return max_chunksize_; }

 private:
  ExecBatchIterator(std::vector<Datum> args, int64_t length, int64_t max_chunksize);

  std::vector<Datum> args_;
  std::vector<int> chunk_indexes_;
  std::vector<int64_t> chunk_positions_;
  int64_t position_;
  int64_t length_;
  int64_t max_chunksize_;
};

// "Push" / listener API like IPC reader so that consumers can receive
// processed chunks as soon as they're available.

class ARROW_EXPORT ExecListener {
 public:
  virtual ~ExecListener() = default;

  virtual Status OnResult(Datum) { return Status::NotImplemented("OnResult"); }
};

class DatumAccumulator : public ExecListener {
 public:
  DatumAccumulator() = default;

  Status OnResult(Datum value) override {
    values_.emplace_back(value);
    return Status::OK();
  }

  std::vector<Datum> values() { return std::move(values_); }

 private:
  std::vector<Datum> values_;
};

/// \brief Check that each Datum is of a "value" type, which means either
/// SCALAR, ARRAY, or CHUNKED_ARRAY. If there are chunked inputs, then these
/// inputs will be split into non-chunked ExecBatch values for execution
Status CheckAllValues(const std::vector<Datum>& values);

class ARROW_EXPORT KernelExecutor {
 public:
  virtual ~KernelExecutor() = default;

  /// The Kernel's `init` method must be called and any KernelState set in the
  /// KernelContext *before* KernelExecutor::Init is called. This is to facilitate
  /// the case where init may be expensive and does not need to be called again for
  /// each execution of the kernel, for example the same lookup table can be re-used
  /// for all scanned batches in a dataset filter.
  virtual Status Init(KernelContext*, KernelInitArgs) = 0;

  /// XXX: Better configurability for listener
  /// Not thread-safe
  virtual Status Execute(const std::vector<Datum>& args, ExecListener* listener) = 0;

  virtual Datum WrapResults(const std::vector<Datum>& args,
                            const std::vector<Datum>& outputs) = 0;

  static std::unique_ptr<KernelExecutor> MakeScalar();
  static std::unique_ptr<KernelExecutor> MakeVector();
  static std::unique_ptr<KernelExecutor> MakeScalarAggregate();
};

/// \brief Populate validity bitmap with the intersection of the nullity of the
/// arguments. If a preallocated bitmap is not provided, then one will be
/// allocated if needed (in some cases a bitmap can be zero-copied from the
/// arguments). If any Scalar value is null, then the entire validity bitmap
/// will be set to null.
///
/// \param[in] ctx kernel execution context, for memory allocation etc.
/// \param[in] batch the data batch
/// \param[in] out the output ArrayData, must not be null
ARROW_EXPORT
Status PropagateNulls(KernelContext* ctx, const ExecBatch& batch, ArrayData* out);

}  // namespace detail
}  // namespace compute
}  // namespace arrow
