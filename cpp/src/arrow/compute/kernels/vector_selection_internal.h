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

#include <cassert>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/chunk_resolver.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"

namespace arrow::compute::internal {

using FilterState = OptionsWrapper<FilterOptions>;
using TakeState = OptionsWrapper<TakeOptions>;

/// \brief A class used to represent the values argument in take kernels.
///
/// It can represent either a chunked array or a single array. When the values
/// are chunked, the class provides a ChunkResolver to resolve the target array
/// and index in the chunked array.
class ValuesSpan {
 private:
  const std::shared_ptr<ChunkedArray> chunked_ = nullptr;
  const ArraySpan chunk0_;  // first chunk or the whole array
  mutable std::optional<ChunkResolver> chunk_resolver_;

 public:
  explicit ValuesSpan(const std::shared_ptr<ChunkedArray> values)
      : chunked_(std::move(values)), chunk0_{*chunked_->chunk(0)->data()} {
    assert(chunked_);
    assert(chunked_->num_chunks() > 0);
  }

  explicit ValuesSpan(const ArraySpan& values)  // NOLINT(modernize-pass-by-value)
      : chunk0_(values) {}

  explicit ValuesSpan(const ArrayData& values) : chunk0_{ArraySpan{values}} {}

  bool is_chunked() const { return chunked_ != nullptr; }

  const ChunkedArray& chunked_array() const {
    assert(is_chunked());
    return *chunked_;
  }

  /// \brief Lazily builds a ChunkResolver from the underlying chunked array.
  ///
  /// \note This method is not thread-safe.
  /// \pre is_chunked()
  const ChunkResolver& chunk_resolver() const {
    assert(is_chunked());
    if (!chunk_resolver_.has_value()) {
      chunk_resolver_.emplace(chunked_->chunks());
    }
    return *chunk_resolver_;
  }

  const ArraySpan& chunk0() const { return chunk0_; }

  const ArraySpan& array() const {
    assert(!is_chunked());
    return chunk0_;
  }

  const DataType* type() const { return chunk0_.type; }

  int64_t length() const { return is_chunked() ? chunked_->length() : array().length; }

  bool MayHaveNulls() const {
    return is_chunked() ? chunked_->null_count() != 0 : array().MayHaveNulls();
  }
};

/// \brief Type for a single "array_take" kernel function.
///
/// Instead of implementing both `ArrayKernelExec` and `ChunkedExec` typed
/// functions for each configurations of `array_take` parameters, we use
/// templates wrapping `TakeKernelExec` functions to expose exec functions
/// that can be registered in the kernel registry.
///
/// A `TakeKernelExec` always returns a single array, which is the result of
/// taking values from a single array (AA->A) or multiple arrays (CA->A). The
/// wrappers take care of converting the output of a CA call to C or calling
/// the kernel multiple times to process a CC call.
using TakeKernelExec = Status (*)(KernelContext*, const ValuesSpan&, const ArraySpan&,
                                  std::shared_ptr<ArrayData>*);

struct SelectionKernelData {
  SelectionKernelData(InputType value_type, InputType selection_type,
                      ArrayKernelExec exec,
                      VectorKernel::ChunkedExec chunked_exec = NULLPTR)
      : value_type(std::move(value_type)),
        selection_type(std::move(selection_type)),
        exec(exec),
        chunked_exec(chunked_exec) {}

  InputType value_type;
  InputType selection_type;
  ArrayKernelExec exec;
  VectorKernel::ChunkedExec chunked_exec;
};

void RegisterSelectionFunction(const std::string& name, FunctionDoc doc,
                               VectorKernel base_kernel,
                               std::vector<SelectionKernelData>&& kernels,
                               const FunctionOptions* default_options,
                               FunctionRegistry* registry);

/// \brief Callback type for VisitPlainxREEFilterOutputSegments.
///
/// position is the logical position in the values array relative to its offset.
///
/// segment_length is the number of elements that should be emitted.
///
/// filter_valid is true if the filter run value is non-NULL. This value can
/// only be false if null_selection is NullSelectionBehavior::EMIT_NULL. For
/// NullSelectionBehavior::DROP, NULL values from the filter are simply skipped.
///
/// Return true if iteration should continue, false if iteration should stop.
using EmitREEFilterSegment =
    std::function<bool(int64_t position, int64_t segment_length, bool filter_valid)>;

void VisitPlainxREEFilterOutputSegments(
    const ArraySpan& filter, bool filter_may_have_nulls,
    FilterOptions::NullSelectionBehavior null_selection,
    const EmitREEFilterSegment& emit_segment);

Status PrimitiveFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status ListFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status LargeListFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status ListViewFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status LargeListViewFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status FSLFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status DenseUnionFilterExec(KernelContext*, const ExecSpan&, ExecResult*);
Status MapFilterExec(KernelContext*, const ExecSpan&, ExecResult*);

// Take kernels compatible with the TakeKernelExec signature
Status VarBinaryTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                         std::shared_ptr<ArrayData>*);
Status LargeVarBinaryTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                              std::shared_ptr<ArrayData>*);
Status FixedWidthTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                          std::shared_ptr<ArrayData>*);
Status FixedWidthTakeChunkedExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                                 std::shared_ptr<ArrayData>*);
Status ListTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                    std::shared_ptr<ArrayData>*);
Status LargeListTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                         std::shared_ptr<ArrayData>*);
Status ListViewTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                        std::shared_ptr<ArrayData>*);
Status LargeListViewTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                             std::shared_ptr<ArrayData>*);
Status FSLTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                   std::shared_ptr<ArrayData>*);
Status DenseUnionTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                          std::shared_ptr<ArrayData>*);
Status SparseUnionTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                           std::shared_ptr<ArrayData>*);
Status StructTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                      std::shared_ptr<ArrayData>*);
Status MapTakeExec(KernelContext*, const ValuesSpan&, const ArraySpan&,
                   std::shared_ptr<ArrayData>*);

}  // namespace arrow::compute::internal
