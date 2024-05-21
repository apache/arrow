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

/// \brief Break std::vector<Datum> into a sequence of non-owning
/// ExecSpan for kernel execution. The lifetime of the Datum vector
/// must be longer than the lifetime of this object
class ARROW_EXPORT ExecSpanIterator {
 public:
  ExecSpanIterator() = default;

  /// \brief Initialize iterator and do basic argument validation
  ///
  /// \param[in] batch the input ExecBatch
  /// \param[in] max_chunksize the maximum length of each ExecSpan. Depending
  /// on the chunk layout of ChunkedArray.
  /// \param[in] promote_if_all_scalars if all of the values are scalars,
  /// return them in each ExecSpan as ArraySpan of length 1. This must be set
  /// to true for Scalar and Vector executors but false for Aggregators
  Status Init(const ExecBatch& batch, int64_t max_chunksize = kDefaultMaxChunksize,
              bool promote_if_all_scalars = true);

  /// \brief Compute the next span by updating the state of the
  /// previous span object. You must keep passing in the previous
  /// value for the results to be consistent. If you need to process
  /// in parallel, make a copy of the in-use ExecSpan while it's being
  /// used by another thread and pass it into Next. This function
  /// always populates at least one span. If you call this function
  /// with a blank ExecSpan after the first iteration, it will not
  /// work correctly (maybe we will change this later). Return false
  /// if the iteration is exhausted
  bool Next(ExecSpan* span);

  int64_t length() const { return length_; }
  int64_t position() const { return position_; }

  bool have_all_scalars() const { return have_all_scalars_; }

 private:
  ExecSpanIterator(const std::vector<Datum>& args, int64_t length, int64_t max_chunksize);

  int64_t GetNextChunkSpan(int64_t iteration_size, ExecSpan* span);

  bool initialized_ = false;
  bool have_chunked_arrays_ = false;
  bool have_all_scalars_ = false;
  bool promote_if_all_scalars_ = true;
  const std::vector<Datum>* args_;
  std::vector<int> chunk_indexes_;
  std::vector<int64_t> value_positions_;

  // Keep track of the array offset in the "active" array (e.g. the
  // array or the particular chunk of an array) in each slot, separate
  // from the relative position within each chunk (which is in
  // value_positions_)
  std::vector<int64_t> value_offsets_;
  int64_t position_ = 0;
  int64_t length_ = 0;
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

class ARROW_EXPORT KernelExecutor {
 public:
  virtual ~KernelExecutor() = default;

  /// The Kernel's `init` method must be called and any KernelState set in the
  /// KernelContext *before* KernelExecutor::Init is called. This is to facilitate
  /// the case where init may be expensive and does not need to be called again for
  /// each execution of the kernel, for example the same lookup table can be re-used
  /// for all scanned batches in a dataset filter.
  virtual Status Init(KernelContext*, KernelInitArgs) = 0;

  // TODO(wesm): per ARROW-16819, adding ExecBatch variant so that a batch
  // length can be passed in for scalar functions; will have to return and
  // clean a bunch of things up
  virtual Status Execute(const ExecBatch& batch, ExecListener* listener) = 0;

  virtual Datum WrapResults(const std::vector<Datum>& args,
                            const std::vector<Datum>& outputs) = 0;

  /// \brief Check the actual result type against the resolved output type
  virtual Status CheckResultType(const Datum& out, const char* function_name) = 0;

  static std::unique_ptr<KernelExecutor> MakeScalar();
  static std::unique_ptr<KernelExecutor> MakeVector();
  static std::unique_ptr<KernelExecutor> MakeScalarAggregate();
};

int64_t InferBatchLength(const std::vector<Datum>& values, bool* all_same);

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
Status PropagateNulls(KernelContext* ctx, const ExecSpan& batch, ArrayData* out);

ARROW_EXPORT
void PropagateNullsSpans(const ExecSpan& batch, ArraySpan* out);

}  // namespace detail
}  // namespace compute
}  // namespace arrow
