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

#include "arrow/compute/exec.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/data.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/function.h"
#include "arrow/compute/function_internal.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/registry.h"
#include "arrow/datum.h"
#include "arrow/pretty_print.h"
#include "arrow/record_batch.h"
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;
using internal::CopyBitmap;
using internal::CpuInfo;
using internal::GetCpuThreadPool;

namespace compute {

ExecContext* default_exec_context() {
  static ExecContext default_ctx;
  return &default_ctx;
}

ExecContext* threaded_exec_context() {
  static ExecContext threaded_ctx(default_memory_pool(), GetCpuThreadPool());
  return &threaded_ctx;
}

ExecBatch::ExecBatch(const RecordBatch& batch)
    : values(batch.num_columns()), length(batch.num_rows()) {
  auto columns = batch.column_data();
  std::move(columns.begin(), columns.end(), values.begin());
}

bool ExecBatch::Equals(const ExecBatch& other) const {
  return guarantee == other.guarantee && values == other.values;
}

void PrintTo(const ExecBatch& batch, std::ostream* os) {
  *os << "ExecBatch\n";

  static const std::string indent = "    ";

  *os << indent << "# Rows: " << batch.length << "\n";
  if (batch.guarantee != literal(true)) {
    *os << indent << "Guarantee: " << batch.guarantee.ToString() << "\n";
  }

  int i = 0;
  for (const Datum& value : batch.values) {
    *os << indent << "" << i++ << ": ";

    if (value.is_scalar()) {
      *os << "Scalar[" << value.scalar()->ToString() << "]\n";
    } else if (value.is_array() || value.is_chunked_array()) {
      PrettyPrintOptions options;
      options.skip_new_lines = true;
      if (value.is_array()) {
        auto array = value.make_array();
        *os << "Array";
        ARROW_CHECK_OK(PrettyPrint(*array, options, os));
      } else {
        auto array = value.chunked_array();
        *os << "Chunked Array";
        ARROW_CHECK_OK(PrettyPrint(*array, options, os));
      }
      *os << "\n";
    } else {
      ARROW_DCHECK(false);
    }
  }
}

int64_t ExecBatch::TotalBufferSize() const {
  int64_t sum = 0;
  for (const auto& value : values) {
    sum += value.TotalBufferSize();
  }
  return sum;
}

std::string ExecBatch::ToString() const {
  std::stringstream ss;
  PrintTo(*this, &ss);
  return ss.str();
}

ExecBatch ExecBatch::Slice(int64_t offset, int64_t length) const {
  ExecBatch out = *this;
  for (auto& value : out.values) {
    if (value.is_scalar()) {
      // keep value as is
    } else if (value.is_array()) {
      value = value.array()->Slice(offset, length);
    } else if (value.is_chunked_array()) {
      value = value.chunked_array()->Slice(offset, length);
    } else {
      ARROW_DCHECK(false);
    }
  }
  out.length = std::min(length, this->length - offset);
  return out;
}

Result<ExecBatch> ExecBatch::SelectValues(const std::vector<int>& ids) const {
  std::vector<Datum> selected_values;
  selected_values.reserve(ids.size());
  for (int id : ids) {
    if (id < 0 || static_cast<size_t>(id) >= values.size()) {
      return Status::Invalid("ExecBatch invalid value selection: ", id);
    }
    selected_values.push_back(values[id]);
  }
  return ExecBatch(std::move(selected_values), length);
}

namespace {

enum LengthInferenceError {
  kEmptyInput = -1,
  kInvalidValues = -2,
};

/// \brief Infer the ExecBatch length from values.
///
/// \return the inferred length of the batch. If there are no values in the
/// batch then kEmptyInput (-1) is returned. If the values in the batch have
/// different lengths then kInvalidValues (-2) is returned.
int64_t DoInferLength(const std::vector<Datum>& values) {
  if (values.empty()) {
    return kEmptyInput;
  }

  int64_t length = -1;
  for (const auto& value : values) {
    if (value.is_scalar()) {
      continue;
    }

    if (length == -1) {
      length = value.length();
      continue;
    }

    if (length != value.length()) {
      // all the arrays should have the same length
      return kInvalidValues;
    }
  }

  return length == -1 ? 1 : length;
}

}  // namespace

Result<int64_t> ExecBatch::InferLength(const std::vector<Datum>& values) {
  const int64_t length = DoInferLength(values);
  switch (length) {
    case kInvalidValues:
      return Status::Invalid(
          "Arrays used to construct an ExecBatch must have equal length");
    case kEmptyInput:
      return Status::Invalid("Cannot infer ExecBatch length without at least one value");
    default:
      break;
  }
  return {length};
}

Result<ExecBatch> ExecBatch::Make(std::vector<Datum> values, int64_t length) {
  // Infer the length again and/or validate the given length.
  const int64_t inferred_length = DoInferLength(values);
  switch (inferred_length) {
    case kEmptyInput:
      if (length < 0) {
        return Status::Invalid(
            "Cannot infer ExecBatch length without at least one value");
      }
      break;

    case kInvalidValues:
      return Status::Invalid(
          "Arrays used to construct an ExecBatch must have equal length");

    default:
      if (length < 0) {
        length = inferred_length;
      } else if (length != inferred_length) {
        return Status::Invalid("Length used to construct an ExecBatch is invalid");
      }
      break;
  }

  return ExecBatch(std::move(values), length);
}

Result<std::shared_ptr<RecordBatch>> ExecBatch::ToRecordBatch(
    std::shared_ptr<Schema> schema, MemoryPool* pool) const {
  if (static_cast<size_t>(schema->num_fields()) > values.size()) {
    return Status::Invalid("ExecBatch::ToRecordBatch mismatching schema size");
  }
  ArrayVector columns(schema->num_fields());

  for (size_t i = 0; i < columns.size(); ++i) {
    const Datum& value = values[i];
    if (value.is_array()) {
      columns[i] = value.make_array();
      continue;
    } else if (value.is_scalar()) {
      ARROW_ASSIGN_OR_RAISE(columns[i],
                            MakeArrayFromScalar(*value.scalar(), length, pool));
    } else {
      return Status::TypeError("ExecBatch::ToRecordBatch value ", i, " with unsupported ",
                               "value kind ", ::arrow::ToString(value.kind()));
    }
  }

  return RecordBatch::Make(std::move(schema), length, std::move(columns));
}

namespace {

Result<std::shared_ptr<Buffer>> AllocateDataBuffer(KernelContext* ctx, int64_t length,
                                                   int bit_width) {
  if (bit_width == 1) {
    return ctx->AllocateBitmap(length);
  } else {
    int64_t buffer_size = bit_util::BytesForBits(length * bit_width);
    return ctx->Allocate(buffer_size);
  }
}

struct BufferPreallocation {
  explicit BufferPreallocation(int bit_width = -1, int added_length = 0)
      : bit_width(bit_width), added_length(added_length) {}

  int bit_width;
  int added_length;
};

void ComputeDataPreallocate(const DataType& type,
                            std::vector<BufferPreallocation>* widths) {
  if (is_fixed_width(type.id()) && type.id() != Type::NA) {
    widths->emplace_back(checked_cast<const FixedWidthType&>(type).bit_width());
    return;
  }
  // Preallocate binary and list offsets
  switch (type.id()) {
    case Type::BINARY:
    case Type::STRING:
    case Type::LIST:
    case Type::MAP:
      widths->emplace_back(32, /*added_length=*/1);
      return;
    case Type::LARGE_BINARY:
    case Type::LARGE_STRING:
    case Type::LARGE_LIST:
      widths->emplace_back(64, /*added_length=*/1);
      return;
    default:
      break;
  }
}

}  // namespace

namespace detail {

// ----------------------------------------------------------------------
// ExecSpanIterator

namespace {

void PromoteExecSpanScalars(ExecSpan* span) {
  // In the "all scalar" case, we "promote" the scalars to ArraySpans of
  // length 1, since the kernel implementations do not handle the all
  // scalar case
  for (int i = 0; i < span->num_values(); ++i) {
    ExecValue* value = &span->values[i];
    if (value->is_scalar()) {
      value->array.FillFromScalar(*value->scalar);
      value->scalar = nullptr;
    }
  }
}

bool CheckIfAllScalar(const ExecBatch& batch) {
  for (const Datum& value : batch.values) {
    if (!value.is_scalar()) {
      DCHECK(value.is_arraylike());
      return false;
    }
  }
  return batch.num_values() > 0;
}

}  // namespace

Status ExecSpanIterator::Init(const ExecBatch& batch, int64_t max_chunksize,
                              bool promote_if_all_scalars) {
  if (batch.num_values() > 0) {
    // Validate arguments
    bool all_args_same_length = false;
    int64_t inferred_length = InferBatchLength(batch.values, &all_args_same_length);
    if (inferred_length != batch.length) {
      return Status::Invalid("Value lengths differed from ExecBatch length");
    }
    if (!all_args_same_length) {
      return Status::Invalid("Array arguments must all be the same length");
    }
  }
  args_ = &batch.values;
  initialized_ = have_chunked_arrays_ = false;
  have_all_scalars_ = CheckIfAllScalar(batch);
  promote_if_all_scalars_ = promote_if_all_scalars;
  position_ = 0;
  length_ = batch.length;
  chunk_indexes_.clear();
  chunk_indexes_.resize(args_->size(), 0);
  value_positions_.clear();
  value_positions_.resize(args_->size(), 0);
  value_offsets_.clear();
  value_offsets_.resize(args_->size(), 0);
  max_chunksize_ = std::min(length_, max_chunksize);
  return Status::OK();
}

int64_t ExecSpanIterator::GetNextChunkSpan(int64_t iteration_size, ExecSpan* span) {
  for (size_t i = 0; i < args_->size() && iteration_size > 0; ++i) {
    // If the argument is not a chunked array, it's either a Scalar or Array,
    // in which case it doesn't influence the size of this span
    if (!args_->at(i).is_chunked_array()) {
      continue;
    }
    const ChunkedArray* arg = args_->at(i).chunked_array().get();
    if (arg->num_chunks() == 0) {
      iteration_size = 0;
      continue;
    }
    const Array* current_chunk;
    while (true) {
      current_chunk = arg->chunk(chunk_indexes_[i]).get();
      if (value_positions_[i] == current_chunk->length()) {
        // Chunk is zero-length, or was exhausted in the previous
        // iteration. Move to the next chunk
        ++chunk_indexes_[i];
        current_chunk = arg->chunk(chunk_indexes_[i]).get();
        span->values[i].SetArray(*current_chunk->data());
        value_positions_[i] = 0;
        value_offsets_[i] = current_chunk->offset();
        continue;
      }
      break;
    }
    iteration_size =
        std::min(current_chunk->length() - value_positions_[i], iteration_size);
  }
  return iteration_size;
}

bool ExecSpanIterator::Next(ExecSpan* span) {
  if (!initialized_) {
    span->length = 0;

    // The first time this is called, we populate the output span with any
    // Scalar or Array arguments in the ExecValue struct, and then just
    // increment array offsets below. If any arguments are ChunkedArray, then
    // the internal ArraySpans will see their members updated during hte
    // iteration
    span->values.resize(args_->size());
    for (size_t i = 0; i < args_->size(); ++i) {
      const Datum& arg = (*args_)[i];
      if (arg.is_scalar()) {
        span->values[i].SetScalar(arg.scalar().get());
      } else if (arg.is_array()) {
        const ArrayData& arr = *arg.array();
        span->values[i].SetArray(arr);
        value_offsets_[i] = arr.offset;
      } else {
        // Populate members from the first chunk
        const ChunkedArray& carr = *arg.chunked_array();
        if (carr.num_chunks() > 0) {
          const ArrayData& arr = *carr.chunk(0)->data();
          span->values[i].SetArray(arr);
          value_offsets_[i] = arr.offset;
        } else {
          // Fill as zero-length array
          ::arrow::internal::FillZeroLengthArray(carr.type().get(),
                                                 &span->values[i].array);
          span->values[i].scalar = nullptr;
        }
        have_chunked_arrays_ = true;
      }
    }

    if (have_all_scalars_ && promote_if_all_scalars_) {
      PromoteExecSpanScalars(span);
    }

    initialized_ = true;
  } else if (position_ == length_) {
    // We've emitted at least one span and we're at the end so we are done
    return false;
  }

  // Determine how large the common contiguous "slice" of all the arguments is
  int64_t iteration_size = std::min(length_ - position_, max_chunksize_);
  if (have_chunked_arrays_) {
    iteration_size = GetNextChunkSpan(iteration_size, span);
  }

  // Now, adjust the span
  span->length = iteration_size;
  for (size_t i = 0; i < args_->size(); ++i) {
    const Datum& arg = args_->at(i);
    if (!arg.is_scalar()) {
      ArraySpan* arr = &span->values[i].array;
      arr->SetSlice(value_positions_[i] + value_offsets_[i], iteration_size);
      value_positions_[i] += iteration_size;
    }
  }

  position_ += iteration_size;
  DCHECK_LE(position_, length_);
  return true;
}

namespace {

struct NullGeneralization {
  enum type { PERHAPS_NULL, ALL_VALID, ALL_NULL };

  static type Get(const ExecValue& value) {
    const auto dtype_id = value.type()->id();
    if (dtype_id == Type::NA) {
      return ALL_NULL;
    }
    if (!arrow::internal::HasValidityBitmap(dtype_id)) {
      return ALL_VALID;
    }
    if (value.is_scalar()) {
      return value.scalar->is_valid ? ALL_VALID : ALL_NULL;
    } else {
      const ArraySpan& arr = value.array;
      // Do not count the bits if they haven't been counted already
      if ((arr.null_count == 0) || (arr.buffers[0].data == nullptr)) {
        return ALL_VALID;
      }
      if (arr.null_count == arr.length) {
        return ALL_NULL;
      }
    }
    return PERHAPS_NULL;
  }

  static type Get(const Datum& datum) {
    // Temporary workaround to help with ARROW-16756
    ExecValue value;
    if (datum.is_array()) {
      value.SetArray(*datum.array());
    } else if (datum.is_scalar()) {
      value.SetScalar(datum.scalar().get());
    } else {
      // TODO(wesm): ChunkedArray, I think
      return PERHAPS_NULL;
    }
    return Get(value);
  }
};

// Null propagation implementation that deals both with preallocated bitmaps
// and maybe-to-be allocated bitmaps
//
// If the bitmap is preallocated, it MUST be populated (since it might be a
// view of a much larger bitmap). If it isn't preallocated, then we have
// more flexibility.
//
// * If the batch has no nulls, then we do nothing
// * If only a single array has nulls, and its offset is a multiple of 8,
//   then we can zero-copy the bitmap into the output
// * Otherwise, we allocate the bitmap and populate it
class NullPropagator {
 public:
  NullPropagator(KernelContext* ctx, const ExecSpan& batch, ArrayData* output)
      : ctx_(ctx), batch_(batch), output_(output) {
    for (const ExecValue& value : batch_.values) {
      auto null_generalization = NullGeneralization::Get(value);
      if (null_generalization == NullGeneralization::ALL_NULL) {
        is_all_null_ = true;
      }
      if (null_generalization != NullGeneralization::ALL_VALID && value.is_array()) {
        arrays_with_nulls_.push_back(&value.array);
      }
    }
    if (output->buffers[0] != nullptr) {
      bitmap_preallocated_ = true;
      bitmap_ = output_->buffers[0]->mutable_data();
    }
  }

  Status EnsureAllocated() {
    if (bitmap_preallocated_) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(output_->buffers[0], ctx_->AllocateBitmap(output_->length));
    bitmap_ = output_->buffers[0]->mutable_data();
    return Status::OK();
  }

  Status AllNullShortCircuit() {
    // OK, the output should be all null
    output_->null_count = output_->length;

    if (bitmap_preallocated_) {
      bit_util::SetBitsTo(bitmap_, output_->offset, output_->length, false);
      return Status::OK();
    }

    // Walk all the values with nulls instead of breaking on the first in case
    // we find a bitmap that can be reused in the non-preallocated case
    for (const ArraySpan* arr : arrays_with_nulls_) {
      if (arr->null_count == arr->length && arr->buffers[0].owner != nullptr) {
        // Reuse this all null bitmap
        output_->buffers[0] = arr->GetBuffer(0);
        return Status::OK();
      }
    }

    RETURN_NOT_OK(EnsureAllocated());
    bit_util::SetBitsTo(bitmap_, output_->offset, output_->length, false);
    return Status::OK();
  }

  Status PropagateSingle() {
    // One array
    const ArraySpan& arr = *arrays_with_nulls_[0];
    const uint8_t* arr_bitmap = arr.buffers[0].data;

    // Reuse the null count if it's known
    output_->null_count = arr.null_count;

    if (bitmap_preallocated_) {
      CopyBitmap(arr_bitmap, arr.offset, arr.length, bitmap_, output_->offset);
      return Status::OK();
    }

    // Two cases when memory was not pre-allocated:
    //
    // * Offset is zero: we reuse the bitmap as is
    // * Offset is nonzero but a multiple of 8: we can slice the bitmap
    // * Offset is not a multiple of 8: we must allocate and use CopyBitmap
    //
    // Keep in mind that output_->offset is not permitted to be nonzero when
    // the bitmap is not preallocated, and that precondition is asserted
    // higher in the call stack.
    if (arr.offset == 0) {
      output_->buffers[0] = arr.GetBuffer(0);
    } else if (arr.offset % 8 == 0) {
      output_->buffers[0] = SliceBuffer(arr.GetBuffer(0), arr.offset / 8,
                                        bit_util::BytesForBits(arr.length));
    } else {
      RETURN_NOT_OK(EnsureAllocated());
      CopyBitmap(arr_bitmap, arr.offset, arr.length, bitmap_, /*dst_offset=*/0);
    }
    return Status::OK();
  }

  Status PropagateMultiple() {
    // More than one array. We use BitmapAnd to intersect their bitmaps

    // Do not compute the intersection null count until it's needed
    RETURN_NOT_OK(EnsureAllocated());

    auto Accumulate = [&](const uint8_t* left_data, int64_t left_offset,
                          const uint8_t* right_data, int64_t right_offset) {
      BitmapAnd(left_data, left_offset, right_data, right_offset, output_->length,
                output_->offset, bitmap_);
    };

    DCHECK_GT(arrays_with_nulls_.size(), 1);

    // Seed the output bitmap with the & of the first two bitmaps
    Accumulate(arrays_with_nulls_[0]->buffers[0].data, arrays_with_nulls_[0]->offset,
               arrays_with_nulls_[1]->buffers[0].data, arrays_with_nulls_[1]->offset);

    // Accumulate the rest
    for (size_t i = 2; i < arrays_with_nulls_.size(); ++i) {
      Accumulate(bitmap_, output_->offset, arrays_with_nulls_[i]->buffers[0].data,
                 arrays_with_nulls_[i]->offset);
    }
    return Status::OK();
  }

  Status Execute() {
    if (is_all_null_) {
      // An all-null value (scalar null or all-null array) gives us a short
      // circuit opportunity
      return AllNullShortCircuit();
    }

    // At this point, by construction we know that all of the values in
    // arrays_with_nulls_ are arrays that are not all null. So there are a
    // few cases:
    //
    // * No arrays. This is a no-op w/o preallocation but when the bitmap is
    //   pre-allocated we have to fill it with 1's
    // * One array, whose bitmap can be zero-copied (w/o preallocation, and
    //   when no byte is split) or copied (split byte or w/ preallocation)
    // * More than one array, we must compute the intersection of all the
    //   bitmaps
    //
    // BUT, if the output offset is nonzero for some reason, we copy into the
    // output unconditionally

    output_->null_count = kUnknownNullCount;

    if (arrays_with_nulls_.empty()) {
      // No arrays with nulls case
      output_->null_count = 0;
      if (bitmap_preallocated_) {
        bit_util::SetBitsTo(bitmap_, output_->offset, output_->length, true);
      }
      return Status::OK();
    }

    if (arrays_with_nulls_.size() == 1) {
      return PropagateSingle();
    }

    return PropagateMultiple();
  }

 private:
  KernelContext* ctx_;
  const ExecSpan& batch_;
  std::vector<const ArraySpan*> arrays_with_nulls_;
  bool is_all_null_ = false;
  ArrayData* output_;
  uint8_t* bitmap_;
  bool bitmap_preallocated_ = false;
};

std::shared_ptr<ChunkedArray> ToChunkedArray(const std::vector<Datum>& values,
                                             const TypeHolder& type) {
  std::vector<std::shared_ptr<Array>> arrays;
  arrays.reserve(values.size());
  for (const Datum& val : values) {
    if (val.length() == 0) {
      // Skip empty chunks
      continue;
    }
    arrays.emplace_back(val.make_array());
  }
  return std::make_shared<ChunkedArray>(std::move(arrays), type.GetSharedPtr());
}

bool HaveChunkedArray(const std::vector<Datum>& values) {
  for (const auto& value : values) {
    if (value.kind() == Datum::CHUNKED_ARRAY) {
      return true;
    }
  }
  return false;
}

template <typename KernelType>
class KernelExecutorImpl : public KernelExecutor {
 public:
  Status Init(KernelContext* kernel_ctx, KernelInitArgs args) override {
    kernel_ctx_ = kernel_ctx;
    kernel_ = static_cast<const KernelType*>(args.kernel);

    // Resolve the output type for this kernel
    ARROW_ASSIGN_OR_RAISE(
        output_type_, kernel_->signature->out_type().Resolve(kernel_ctx_, args.inputs));

    return Status::OK();
  }

 protected:
  // Prepare an output ArrayData to be written to. If
  // Kernel::mem_allocation is not MemAllocation::PREALLOCATE, then no
  // data buffers will be set
  Result<std::shared_ptr<ArrayData>> PrepareOutput(int64_t length) {
    auto out = std::make_shared<ArrayData>(output_type_.GetSharedPtr(), length);
    out->buffers.resize(output_num_buffers_);

    if (validity_preallocated_) {
      ARROW_ASSIGN_OR_RAISE(out->buffers[0], kernel_ctx_->AllocateBitmap(length));
    }
    if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
      out->null_count = 0;
    }
    for (size_t i = 0; i < data_preallocated_.size(); ++i) {
      const auto& prealloc = data_preallocated_[i];
      if (prealloc.bit_width >= 0) {
        ARROW_ASSIGN_OR_RAISE(
            out->buffers[i + 1],
            AllocateDataBuffer(kernel_ctx_, length + prealloc.added_length,
                               prealloc.bit_width));
      }
    }
    return out;
  }

  Status CheckResultType(const Datum& out, const char* function_name) override {
    const auto& type = out.type();
    if (type != nullptr && !type->Equals(*output_type_.type)) {
      return Status::TypeError(
          "kernel type result mismatch for function '", function_name, "': declared as ",
          output_type_.type->ToString(), ", actual is ", type->ToString());
    }
    return Status::OK();
  }

  ExecContext* exec_context() { return kernel_ctx_->exec_context(); }
  KernelState* state() { return kernel_ctx_->state(); }

  // Not all of these members are used for every executor type

  KernelContext* kernel_ctx_;
  const KernelType* kernel_;
  TypeHolder output_type_;

  int output_num_buffers_;

  // If true, then memory is preallocated for the validity bitmap with the same
  // strategy as the data buffer(s).
  bool validity_preallocated_ = false;

  // The kernel writes into data buffers preallocated for these bit widths
  // (0 indicates no preallocation);
  std::vector<BufferPreallocation> data_preallocated_;
};

class ScalarExecutor : public KernelExecutorImpl<ScalarKernel> {
 public:
  Status Execute(const ExecBatch& batch, ExecListener* listener) override {
    RETURN_NOT_OK(span_iterator_.Init(batch, exec_context()->exec_chunksize()));

    if (batch.length == 0) {
      // For zero-length batches, we do nothing except return a zero-length
      // array of the correct output type
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> result,
                            MakeArrayOfNull(output_type_.GetSharedPtr(), /*length=*/0,
                                            exec_context()->memory_pool()));
      return EmitResult(result->data(), listener);
    }

    // If the executor is configured to produce a single large Array output for
    // kernels supporting preallocation, then we do so up front and then
    // iterate over slices of that large array. Otherwise, we preallocate prior
    // to processing each span emitted from the ExecSpanIterator
    RETURN_NOT_OK(SetupPreallocation(span_iterator_.length(), batch.values));

    // ARROW-16756: Here we have to accommodate the distinct cases
    //
    // * Fully-preallocated contiguous output
    // * Fully-preallocated, non-contiguous kernel output
    // * Not-fully-preallocated kernel output: we pass an empty or
    //   partially-filled ArrayData to the kernel
    if (preallocating_all_buffers_) {
      return ExecuteSpans(listener);
    } else {
      return ExecuteNonSpans(listener);
    }
  }

  Datum WrapResults(const std::vector<Datum>& inputs,
                    const std::vector<Datum>& outputs) override {
    // If execution yielded multiple chunks (because large arrays were split
    // based on the ExecContext parameters, then the result is a ChunkedArray
    if (HaveChunkedArray(inputs) || outputs.size() > 1) {
      return ToChunkedArray(outputs, output_type_);
    } else {
      // Outputs have just one element
      return outputs[0];
    }
  }

 protected:
  Status EmitResult(std::shared_ptr<ArrayData> out, ExecListener* listener) {
    if (span_iterator_.have_all_scalars()) {
      // ARROW-16757 We boxed scalar inputs as ArraySpan, so now we have to
      // unbox the output as a scalar
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Scalar> scalar, MakeArray(out)->GetScalar(0));
      return listener->OnResult(std::move(scalar));
    } else {
      return listener->OnResult(std::move(out));
    }
  }

  Status ExecuteSpans(ExecListener* listener) {
    // We put the preallocation in an ArraySpan to be passed to the
    // kernel which is expecting to receive that. More
    // performance-critical code (e.g. expression evaluation) should
    // eventually skip the creation of ArrayData altogether
    std::shared_ptr<ArrayData> preallocation;
    ExecSpan input;
    ExecResult output;
    ArraySpan* output_span = output.array_span_mutable();

    if (preallocate_contiguous_) {
      // Make one big output allocation
      ARROW_ASSIGN_OR_RAISE(preallocation, PrepareOutput(span_iterator_.length()));

      // Populate and then reuse the ArraySpan inside
      output_span->SetMembers(*preallocation);
      output_span->offset = 0;
      int64_t result_offset = 0;
      while (span_iterator_.Next(&input)) {
        // Set absolute output span position and length
        output_span->SetSlice(result_offset, input.length);
        RETURN_NOT_OK(ExecuteSingleSpan(input, &output));
        result_offset = span_iterator_.position();
      }

      // Kernel execution is complete; emit result
      return EmitResult(std::move(preallocation), listener);
    } else {
      // Fully preallocating, but not contiguously
      // We preallocate (maybe) only for the output of processing the current
      // chunk
      while (span_iterator_.Next(&input)) {
        ARROW_ASSIGN_OR_RAISE(preallocation, PrepareOutput(input.length));
        output_span->SetMembers(*preallocation);
        RETURN_NOT_OK(ExecuteSingleSpan(input, &output));
        // Emit the result for this chunk
        RETURN_NOT_OK(EmitResult(std::move(preallocation), listener));
      }
      return Status::OK();
    }
  }

  Status ExecuteSingleSpan(const ExecSpan& input, ExecResult* out) {
    ArraySpan* result_span = out->array_span_mutable();
    if (output_type_.type->id() == Type::NA) {
      result_span->null_count = result_span->length;
    } else if (kernel_->null_handling == NullHandling::INTERSECTION) {
      if (!elide_validity_bitmap_) {
        PropagateNullsSpans(input, result_span);
      }
    } else if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
      result_span->null_count = 0;
    }
    RETURN_NOT_OK(kernel_->exec(kernel_ctx_, input, out));
    // Output type didn't change
    DCHECK(out->is_array_span());
    return Status::OK();
  }

  Status ExecuteNonSpans(ExecListener* listener) {
    // ARROW-16756: Kernel is going to allocate some memory and so
    // for the time being we pass in an empty or partially-filled
    // shared_ptr<ArrayData> or shared_ptr<Scalar> to be populated
    // by the kernel.
    //
    // We will eventually delete the Scalar output path per
    // ARROW-16757.
    ExecSpan input;
    ExecResult output;
    while (span_iterator_.Next(&input)) {
      ARROW_ASSIGN_OR_RAISE(output.value, PrepareOutput(input.length));
      DCHECK(output.is_array_data());

      ArrayData* out_arr = output.array_data().get();
      if (output_type_.type->id() == Type::NA) {
        out_arr->null_count = out_arr->length;
      } else if (kernel_->null_handling == NullHandling::INTERSECTION) {
        RETURN_NOT_OK(PropagateNulls(kernel_ctx_, input, out_arr));
      } else if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
        out_arr->null_count = 0;
      }

      RETURN_NOT_OK(kernel_->exec(kernel_ctx_, input, &output));

      // Output type didn't change
      DCHECK(output.is_array_data());

      // Emit a result for each chunk
      RETURN_NOT_OK(EmitResult(std::move(output.array_data()), listener));
    }
    return Status::OK();
  }

  Status SetupPreallocation(int64_t total_length, const std::vector<Datum>& args) {
    output_num_buffers_ = static_cast<int>(output_type_.type->layout().buffers.size());
    auto out_type_id = output_type_.type->id();
    // Default to no validity pre-allocation for following cases:
    // - Output Array is NullArray
    // - kernel_->null_handling is COMPUTED_NO_PREALLOCATE or OUTPUT_NOT_NULL
    validity_preallocated_ = false;

    if (out_type_id != Type::NA) {
      if (kernel_->null_handling == NullHandling::COMPUTED_PREALLOCATE) {
        // Override the flag if kernel asks for pre-allocation
        validity_preallocated_ = true;
      } else if (kernel_->null_handling == NullHandling::INTERSECTION) {
        elide_validity_bitmap_ = true;
        for (const auto& arg : args) {
          auto null_gen = NullGeneralization::Get(arg) == NullGeneralization::ALL_VALID;

          // If not all valid, this becomes false
          elide_validity_bitmap_ = elide_validity_bitmap_ && null_gen;
        }
        validity_preallocated_ = !elide_validity_bitmap_;
      } else if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
        elide_validity_bitmap_ = true;
      }
    }
    if (kernel_->mem_allocation == MemAllocation::PREALLOCATE) {
      data_preallocated_.clear();
      ComputeDataPreallocate(*output_type_.type, &data_preallocated_);
    }

    // Validity bitmap either preallocated or elided, and all data
    // buffers allocated. This is basically only true for primitive
    // types that are not dictionary-encoded
    preallocating_all_buffers_ =
        ((validity_preallocated_ || elide_validity_bitmap_) &&
         data_preallocated_.size() == static_cast<size_t>(output_num_buffers_ - 1) &&
         !is_nested(out_type_id) && !is_dictionary(out_type_id));

    // TODO(wesm): why was this check ever here? Fixed width binary
    // can be 0-width but anything else?
    DCHECK(std::all_of(
        data_preallocated_.begin(), data_preallocated_.end(),
        [](const BufferPreallocation& prealloc) { return prealloc.bit_width >= 0; }));

    // Contiguous preallocation only possible on non-nested types if all
    // buffers are preallocated.  Otherwise, we must go chunk-by-chunk.
    //
    // Some kernels are also unable to write into sliced outputs, so we respect the
    // kernel's attributes.
    preallocate_contiguous_ =
        (exec_context()->preallocate_contiguous() && kernel_->can_write_into_slices &&
         preallocating_all_buffers_);
    return Status::OK();
  }

  // Used to account for the case where we do not preallocate a
  // validity bitmap because the inputs are all non-null and we're
  // using NullHandling::INTERSECTION to compute the validity bitmap
  bool elide_validity_bitmap_ = false;

  // All memory is preallocated for output, contiguous and
  // non-contiguous
  bool preallocating_all_buffers_ = false;

  // If true, and the kernel and output type supports preallocation (for both
  // the validity and data buffers), then we allocate one big array and then
  // iterate through it while executing the kernel in chunks
  bool preallocate_contiguous_ = false;

  ExecSpanIterator span_iterator_;
};

namespace {

Status CheckCanExecuteChunked(const VectorKernel* kernel) {
  if (kernel->exec_chunked == nullptr) {
    return Status::Invalid(
        "Vector kernel cannot execute chunkwise and no "
        "chunked exec function was defined");
  }

  if (kernel->null_handling == NullHandling::INTERSECTION) {
    return Status::Invalid(
        "Null pre-propagation is unsupported for ChunkedArray "
        "execution in vector kernels");
  }
  return Status::OK();
}

}  // namespace

class VectorExecutor : public KernelExecutorImpl<VectorKernel> {
 public:
  Status Execute(const ExecBatch& batch, ExecListener* listener) override {
    // Some vector kernels have a separate code path for handling
    // chunked arrays (VectorKernel::exec_chunked) so we check if we
    // have any chunked arrays. If we do and an exec_chunked function
    // is defined then we call that.
    bool have_chunked_arrays = false;
    for (const Datum& arg : batch.values) {
      if (arg.is_chunked_array()) have_chunked_arrays = true;
    }

    output_num_buffers_ = static_cast<int>(output_type_.type->layout().buffers.size());

    // Decide if we need to preallocate memory for this kernel
    validity_preallocated_ =
        (kernel_->null_handling != NullHandling::COMPUTED_NO_PREALLOCATE &&
         kernel_->null_handling != NullHandling::OUTPUT_NOT_NULL);
    if (kernel_->mem_allocation == MemAllocation::PREALLOCATE) {
      data_preallocated_.clear();
      ComputeDataPreallocate(*output_type_.type, &data_preallocated_);
    }

    if (kernel_->can_execute_chunkwise) {
      RETURN_NOT_OK(span_iterator_.Init(batch, exec_context()->exec_chunksize()));
      ExecSpan span;
      while (span_iterator_.Next(&span)) {
        RETURN_NOT_OK(Exec(span, listener));
      }
    } else {
      // Kernel cannot execute chunkwise. If we have any chunked
      // arrays, then VectorKernel::exec_chunked must be defined
      // otherwise we raise an error
      if (have_chunked_arrays) {
        RETURN_NOT_OK(ExecChunked(batch, listener));
      } else {
        // No chunked arrays. We pack the args into an ExecSpan and
        // call the regular exec code path
        ExecSpan span(batch);
        if (CheckIfAllScalar(batch)) {
          PromoteExecSpanScalars(&span);
        }
        RETURN_NOT_OK(Exec(span, listener));
      }
    }

    if (kernel_->finalize) {
      // Intermediate results require post-processing after the execution is
      // completed (possibly involving some accumulated state)
      RETURN_NOT_OK(kernel_->finalize(kernel_ctx_, &results_));
      for (const auto& result : results_) {
        RETURN_NOT_OK(listener->OnResult(result));
      }
    }
    return Status::OK();
  }

  Datum WrapResults(const std::vector<Datum>& inputs,
                    const std::vector<Datum>& outputs) override {
    // If execution yielded multiple chunks (because large arrays were split
    // based on the ExecContext parameters, then the result is a ChunkedArray
    if (kernel_->output_chunked && (HaveChunkedArray(inputs) || outputs.size() > 1)) {
      return ToChunkedArray(outputs, output_type_.GetSharedPtr());
    } else {
      // Outputs have just one element
      return outputs[0];
    }
  }

 protected:
  Status EmitResult(Datum result, ExecListener* listener) {
    if (!kernel_->finalize) {
      // If there is no result finalizer (e.g. for hash-based functions, we can
      // emit the processed batch right away rather than waiting
      RETURN_NOT_OK(listener->OnResult(std::move(result)));
    } else {
      results_.emplace_back(std::move(result));
    }
    return Status::OK();
  }

  Status Exec(const ExecSpan& span, ExecListener* listener) {
    ExecResult out;
    ARROW_ASSIGN_OR_RAISE(out.value, PrepareOutput(span.length));
    if (kernel_->null_handling == NullHandling::INTERSECTION) {
      RETURN_NOT_OK(PropagateNulls(kernel_ctx_, span, out.array_data().get()));
    }
    RETURN_NOT_OK(kernel_->exec(kernel_ctx_, span, &out));
    return EmitResult(std::move(out.array_data()), listener);
  }

  Status ExecChunked(const ExecBatch& batch, ExecListener* listener) {
    RETURN_NOT_OK(CheckCanExecuteChunked(kernel_));
    Datum out;
    ARROW_ASSIGN_OR_RAISE(out.value, PrepareOutput(batch.length));
    RETURN_NOT_OK(kernel_->exec_chunked(kernel_ctx_, batch, &out));
    if (out.is_array()) {
      return EmitResult(std::move(out.array()), listener);
    } else {
      DCHECK(out.is_chunked_array());
      return EmitResult(std::move(out.chunked_array()), listener);
    }
  }

  ExecSpanIterator span_iterator_;
  std::vector<Datum> results_;
};

class ScalarAggExecutor : public KernelExecutorImpl<ScalarAggregateKernel> {
 public:
  Status Init(KernelContext* ctx, KernelInitArgs args) override {
    input_types_ = &args.inputs;
    options_ = args.options;
    return KernelExecutorImpl<ScalarAggregateKernel>::Init(ctx, args);
  }

  Status Execute(const ExecBatch& batch, ExecListener* listener) override {
    RETURN_NOT_OK(span_iterator_.Init(batch, exec_context()->exec_chunksize(),
                                      /*promote_if_all_scalars=*/false));

    ExecSpan span;
    while (span_iterator_.Next(&span)) {
      // TODO: implement parallelism
      if (span.length > 0) {
        RETURN_NOT_OK(Consume(span));
      }
    }

    Datum out;
    RETURN_NOT_OK(kernel_->finalize(kernel_ctx_, &out));
    RETURN_NOT_OK(listener->OnResult(std::move(out)));
    return Status::OK();
  }

  Datum WrapResults(const std::vector<Datum>&,
                    const std::vector<Datum>& outputs) override {
    DCHECK_EQ(1, outputs.size());
    return outputs[0];
  }

 private:
  Status Consume(const ExecSpan& span) {
    // TODO(wesm): this is odd and should be examined soon -- only one state
    // "should" be needed per thread of execution

    // FIXME(ARROW-11840) don't merge *any* aggegates for every batch
    ARROW_ASSIGN_OR_RAISE(auto batch_state,
                          kernel_->init(kernel_ctx_, {kernel_, *input_types_, options_}));

    if (batch_state == nullptr) {
      return Status::Invalid("ScalarAggregation requires non-null kernel state");
    }

    KernelContext batch_ctx(exec_context());
    batch_ctx.SetState(batch_state.get());

    RETURN_NOT_OK(kernel_->consume(&batch_ctx, span));
    RETURN_NOT_OK(kernel_->merge(kernel_ctx_, std::move(*batch_state), state()));
    return Status::OK();
  }

  ExecSpanIterator span_iterator_;
  const std::vector<TypeHolder>* input_types_;
  const FunctionOptions* options_;
};

template <typename ExecutorType,
          typename FunctionType = typename ExecutorType::FunctionType>
Result<std::unique_ptr<KernelExecutor>> MakeExecutor(ExecContext* ctx,
                                                     const Function* func,
                                                     const FunctionOptions* options) {
  DCHECK_EQ(ExecutorType::function_kind, func->kind());
  auto typed_func = checked_cast<const FunctionType*>(func);
  return std::make_unique<ExecutorType>(ctx, typed_func, options);
}

}  // namespace

Status PropagateNulls(KernelContext* ctx, const ExecSpan& batch, ArrayData* output) {
  DCHECK_NE(nullptr, output);
  DCHECK_GT(output->buffers.size(), 0);

  if (output->type->id() == Type::NA) {
    // Null output type is a no-op (rare when this would happen but we at least
    // will test for it)
    return Status::OK();
  }

  // This function is ONLY able to write into output with non-zero offset
  // when the bitmap is preallocated. This could be a DCHECK but returning
  // error Status for now for emphasis
  if (output->offset != 0 && output->buffers[0] == nullptr) {
    return Status::Invalid(
        "Can only propagate nulls into pre-allocated memory "
        "when the output offset is non-zero");
  }
  NullPropagator propagator(ctx, batch, output);
  return propagator.Execute();
}

void PropagateNullsSpans(const ExecSpan& batch, ArraySpan* out) {
  if (out->type->id() == Type::NA) {
    // Null output type is a no-op (rare when this would happen but we at least
    // will test for it)
    return;
  }

  std::vector<const ArraySpan*> arrays_with_nulls;
  bool is_all_null = false;
  for (const ExecValue& value : batch.values) {
    auto null_generalization = NullGeneralization::Get(value);
    if (null_generalization == NullGeneralization::ALL_NULL) {
      is_all_null = true;
    }
    if (null_generalization != NullGeneralization::ALL_VALID && value.is_array()) {
      arrays_with_nulls.push_back(&value.array);
    }
  }
  uint8_t* out_bitmap = out->buffers[0].data;
  if (is_all_null) {
    // An all-null value (scalar null or all-null array) gives us a short
    // circuit opportunity
    // OK, the output should be all null
    out->null_count = out->length;
    bit_util::SetBitsTo(out_bitmap, out->offset, out->length, false);
    return;
  }

  out->null_count = kUnknownNullCount;
  if (arrays_with_nulls.empty()) {
    // No arrays with nulls case
    out->null_count = 0;
    if (out_bitmap != nullptr) {
      // An output buffer was allocated, so we fill it with all valid
      bit_util::SetBitsTo(out_bitmap, out->offset, out->length, true);
    }
  } else if (arrays_with_nulls.size() == 1) {
    // One array
    const ArraySpan& arr = *arrays_with_nulls[0];

    // Reuse the null count if it's known
    out->null_count = arr.null_count;
    CopyBitmap(arr.buffers[0].data, arr.offset, arr.length, out_bitmap, out->offset);
  } else {
    // More than one array. We use BitmapAnd to intersect their bitmaps
    auto Accumulate = [&](const ArraySpan& left, const ArraySpan& right) {
      DCHECK(left.buffers[0].data != nullptr);
      DCHECK(right.buffers[0].data != nullptr);
      BitmapAnd(left.buffers[0].data, left.offset, right.buffers[0].data, right.offset,
                out->length, out->offset, out_bitmap);
    };
    // Seed the output bitmap with the & of the first two bitmaps
    Accumulate(*arrays_with_nulls[0], *arrays_with_nulls[1]);

    // Accumulate the rest
    for (size_t i = 2; i < arrays_with_nulls.size(); ++i) {
      Accumulate(*out, *arrays_with_nulls[i]);
    }
  }
}

std::unique_ptr<KernelExecutor> KernelExecutor::MakeScalar() {
  return std::make_unique<detail::ScalarExecutor>();
}

std::unique_ptr<KernelExecutor> KernelExecutor::MakeVector() {
  return std::make_unique<detail::VectorExecutor>();
}

std::unique_ptr<KernelExecutor> KernelExecutor::MakeScalarAggregate() {
  return std::make_unique<detail::ScalarAggExecutor>();
}

int64_t InferBatchLength(const std::vector<Datum>& values, bool* all_same) {
  int64_t length = -1;
  bool are_all_scalar = true;
  for (const Datum& arg : values) {
    if (arg.is_array()) {
      int64_t arg_length = arg.array()->length;
      if (length < 0) {
        length = arg_length;
      } else {
        if (length != arg_length) {
          *all_same = false;
          return length;
        }
      }
      are_all_scalar = false;
    } else if (arg.is_chunked_array()) {
      int64_t arg_length = arg.chunked_array()->length();
      if (length < 0) {
        length = arg_length;
      } else {
        if (length != arg_length) {
          *all_same = false;
          return length;
        }
      }
      are_all_scalar = false;
    }
  }

  if (are_all_scalar && values.size() > 0) {
    length = 1;
  } else if (length < 0) {
    length = 0;
  }
  *all_same = true;
  return length;
}

}  // namespace detail

ExecContext::ExecContext(MemoryPool* pool, ::arrow::internal::Executor* executor,
                         FunctionRegistry* func_registry)
    : pool_(pool), executor_(executor) {
  this->func_registry_ = func_registry == nullptr ? GetFunctionRegistry() : func_registry;
}

const CpuInfo* ExecContext::cpu_info() const { return CpuInfo::GetInstance(); }

// ----------------------------------------------------------------------
// SelectionVector

SelectionVector::SelectionVector(std::shared_ptr<ArrayData> data)
    : data_(std::move(data)) {
  DCHECK_EQ(Type::INT32, data_->type->id());
  DCHECK_EQ(0, data_->GetNullCount());
  indices_ = data_->GetValues<int32_t>(1);
}

SelectionVector::SelectionVector(const Array& arr) : SelectionVector(arr.data()) {}

int32_t SelectionVector::length() const { return static_cast<int32_t>(data_->length); }

Result<std::shared_ptr<SelectionVector>> SelectionVector::FromMask(
    const BooleanArray& arr) {
  return Status::NotImplemented("FromMask");
}

Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           const FunctionOptions* options, ExecContext* ctx) {
  if (ctx == nullptr) {
    ctx = default_exec_context();
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const Function> func,
                        ctx->func_registry()->GetFunction(func_name));
  return func->Execute(args, options, ctx);
}

Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx) {
  return CallFunction(func_name, args, /*options=*/nullptr, ctx);
}

Result<Datum> CallFunction(const std::string& func_name, const ExecBatch& batch,
                           const FunctionOptions* options, ExecContext* ctx) {
  if (ctx == nullptr) {
    ctx = default_exec_context();
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const Function> func,
                        ctx->func_registry()->GetFunction(func_name));
  return func->Execute(batch, options, ctx);
}

Result<Datum> CallFunction(const std::string& func_name, const ExecBatch& batch,
                           ExecContext* ctx) {
  return CallFunction(func_name, batch, /*options=*/nullptr, ctx);
}

Result<std::shared_ptr<FunctionExecutor>> GetFunctionExecutor(
    const std::string& func_name, std::vector<TypeHolder> in_types,
    const FunctionOptions* options, FunctionRegistry* func_registry) {
  if (func_registry == NULLPTR) {
    func_registry = GetFunctionRegistry();
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const Function> func,
                        func_registry->GetFunction(func_name));
  ARROW_ASSIGN_OR_RAISE(auto func_exec, func->GetBestExecutor(std::move(in_types)));
  ARROW_RETURN_NOT_OK(func_exec->Init(options));
  return func_exec;
}

Result<std::shared_ptr<FunctionExecutor>> GetFunctionExecutor(
    const std::string& func_name, const std::vector<Datum>& args,
    const FunctionOptions* options, FunctionRegistry* func_registry) {
  ARROW_ASSIGN_OR_RAISE(auto in_types, internal::GetFunctionArgumentTypes(args));
  return GetFunctionExecutor(func_name, std::move(in_types), options, func_registry);
}

}  // namespace compute
}  // namespace arrow
