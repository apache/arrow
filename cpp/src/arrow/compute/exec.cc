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
#include "arrow/compute/kernel.h"
#include "arrow/compute/registry.h"
#include "arrow/compute/util_internal.h"
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
#include "arrow/util/make_unique.h"
#include "arrow/util/vector.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;
using internal::CopyBitmap;
using internal::CpuInfo;

namespace compute {

ExecContext* default_exec_context() {
  static ExecContext default_ctx;
  return &default_ctx;
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
      continue;
    }

    auto array = value.make_array();
    PrettyPrintOptions options;
    options.skip_new_lines = true;
    *os << "Array";
    ARROW_CHECK_OK(PrettyPrint(*array, options, os));
    *os << "\n";
  }
}

std::string ExecBatch::ToString() const {
  std::stringstream ss;
  PrintTo(*this, &ss);
  return ss.str();
}

ExecBatch ExecBatch::Slice(int64_t offset, int64_t length) const {
  ExecBatch out = *this;
  for (auto& value : out.values) {
    if (value.is_scalar()) continue;
    value = value.array()->Slice(offset, length);
  }
  out.length = length;
  return out;
}

Result<ExecBatch> ExecBatch::Make(std::vector<Datum> values) {
  if (values.empty()) {
    return Status::Invalid("Cannot infer ExecBatch length without at least one value");
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
      return Status::Invalid(
          "Arrays used to construct an ExecBatch must have equal length");
    }
  }

  if (length == -1) {
    length = 1;
  }

  return ExecBatch(std::move(values), length);
}

Result<std::shared_ptr<RecordBatch>> ExecBatch::ToRecordBatch(
    std::shared_ptr<Schema> schema, MemoryPool* pool) const {
  ArrayVector columns(schema->num_fields());

  for (size_t i = 0; i < columns.size(); ++i) {
    const Datum& value = values[i];
    if (value.is_array()) {
      columns[i] = value.make_array();
      continue;
    }
    ARROW_ASSIGN_OR_RAISE(columns[i], MakeArrayFromScalar(*value.scalar(), length, pool));
  }

  return RecordBatch::Make(std::move(schema), length, std::move(columns));
}

namespace {

Result<std::shared_ptr<Buffer>> AllocateDataBuffer(KernelContext* ctx, int64_t length,
                                                   int bit_width) {
  if (bit_width == 1) {
    return ctx->AllocateBitmap(length);
  } else {
    int64_t buffer_size = BitUtil::BytesForBits(length * bit_width);
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

Status CheckAllValues(const std::vector<Datum>& values) {
  for (const auto& value : values) {
    if (!value.is_value()) {
      return Status::Invalid("Tried executing function with non-value type: ",
                             value.ToString());
    }
  }
  return Status::OK();
}

ExecBatchIterator::ExecBatchIterator(std::vector<Datum> args, int64_t length,
                                     int64_t max_chunksize)
    : args_(std::move(args)),
      position_(0),
      length_(length),
      max_chunksize_(max_chunksize) {
  chunk_indexes_.resize(args_.size(), 0);
  chunk_positions_.resize(args_.size(), 0);
}

Result<std::unique_ptr<ExecBatchIterator>> ExecBatchIterator::Make(
    std::vector<Datum> args, int64_t max_chunksize) {
  for (const auto& arg : args) {
    if (!(arg.is_arraylike() || arg.is_scalar())) {
      return Status::Invalid(
          "ExecBatchIterator only works with Scalar, Array, and "
          "ChunkedArray arguments");
    }
  }

  // If the arguments are all scalars, then the length is 1
  int64_t length = 1;

  bool length_set = false;
  for (auto& arg : args) {
    if (arg.is_scalar()) {
      continue;
    }
    if (!length_set) {
      length = arg.length();
      length_set = true;
    } else {
      if (arg.length() != length) {
        return Status::Invalid("Array arguments must all be the same length");
      }
    }
  }

  max_chunksize = std::min(length, max_chunksize);

  return std::unique_ptr<ExecBatchIterator>(
      new ExecBatchIterator(std::move(args), length, max_chunksize));
}

bool ExecBatchIterator::Next(ExecBatch* batch) {
  if (position_ == length_) {
    return false;
  }

  // Determine how large the common contiguous "slice" of all the arguments is
  int64_t iteration_size = std::min(length_ - position_, max_chunksize_);

  // If length_ is 0, then this loop will never execute
  for (size_t i = 0; i < args_.size() && iteration_size > 0; ++i) {
    // If the argument is not a chunked array, it's either a Scalar or Array,
    // in which case it doesn't influence the size of this batch. Note that if
    // the args are all scalars the batch length is 1
    if (args_[i].kind() != Datum::CHUNKED_ARRAY) {
      continue;
    }
    const ChunkedArray& arg = *args_[i].chunked_array();
    std::shared_ptr<Array> current_chunk;
    while (true) {
      current_chunk = arg.chunk(chunk_indexes_[i]);
      if (chunk_positions_[i] == current_chunk->length()) {
        // Chunk is zero-length, or was exhausted in the previous iteration
        chunk_positions_[i] = 0;
        ++chunk_indexes_[i];
        continue;
      }
      break;
    }
    iteration_size =
        std::min(current_chunk->length() - chunk_positions_[i], iteration_size);
  }

  // Now, fill the batch
  batch->values.resize(args_.size());
  batch->length = iteration_size;
  for (size_t i = 0; i < args_.size(); ++i) {
    if (args_[i].is_scalar()) {
      batch->values[i] = args_[i].scalar();
    } else if (args_[i].is_array()) {
      batch->values[i] = args_[i].array()->Slice(position_, iteration_size);
    } else {
      const ChunkedArray& carr = *args_[i].chunked_array();
      const auto& chunk = carr.chunk(chunk_indexes_[i]);
      batch->values[i] = chunk->data()->Slice(chunk_positions_[i], iteration_size);
      chunk_positions_[i] += iteration_size;
    }
  }
  position_ += iteration_size;
  DCHECK_LE(position_, length_);
  return true;
}

namespace {

struct NullGeneralization {
  enum type { PERHAPS_NULL, ALL_VALID, ALL_NULL };

  static type Get(const Datum& datum) {
    if (datum.type()->id() == Type::NA) {
      return ALL_NULL;
    }

    if (datum.is_scalar()) {
      return datum.scalar()->is_valid ? ALL_VALID : ALL_NULL;
    }

    const auto& arr = *datum.array();

    // Do not count the bits if they haven't been counted already
    const int64_t known_null_count = arr.null_count.load();
    if ((known_null_count == 0) || (arr.buffers[0] == NULLPTR)) {
      return ALL_VALID;
    }

    if (known_null_count == arr.length) {
      return ALL_NULL;
    }

    return PERHAPS_NULL;
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
  NullPropagator(KernelContext* ctx, const ExecBatch& batch, ArrayData* output)
      : ctx_(ctx), batch_(batch), output_(output) {
    for (const Datum& datum : batch_.values) {
      auto null_generalization = NullGeneralization::Get(datum);

      if (null_generalization == NullGeneralization::ALL_NULL) {
        is_all_null_ = true;
      }

      if (null_generalization != NullGeneralization::ALL_VALID &&
          datum.kind() == Datum::ARRAY) {
        arrays_with_nulls_.push_back(datum.array().get());
      }
    }

    if (output->buffers[0] != nullptr) {
      bitmap_preallocated_ = true;
      SetBitmap(output_->buffers[0].get());
    }
  }

  void SetBitmap(Buffer* bitmap) { bitmap_ = bitmap->mutable_data(); }

  Status EnsureAllocated() {
    if (bitmap_preallocated_) {
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(output_->buffers[0], ctx_->AllocateBitmap(output_->length));
    SetBitmap(output_->buffers[0].get());
    return Status::OK();
  }

  Status AllNullShortCircuit() {
    // OK, the output should be all null
    output_->null_count = output_->length;

    if (bitmap_preallocated_) {
      BitUtil::SetBitsTo(bitmap_, output_->offset, output_->length, false);
      return Status::OK();
    }

    // Walk all the values with nulls instead of breaking on the first in case
    // we find a bitmap that can be reused in the non-preallocated case
    for (const ArrayData* arr : arrays_with_nulls_) {
      if (arr->null_count.load() == arr->length && arr->buffers[0] != nullptr) {
        // Reuse this all null bitmap
        output_->buffers[0] = arr->buffers[0];
        return Status::OK();
      }
    }

    RETURN_NOT_OK(EnsureAllocated());
    BitUtil::SetBitsTo(bitmap_, output_->offset, output_->length, false);
    return Status::OK();
  }

  Status PropagateSingle() {
    // One array
    const ArrayData& arr = *arrays_with_nulls_[0];
    const std::shared_ptr<Buffer>& arr_bitmap = arr.buffers[0];

    // Reuse the null count if it's known
    output_->null_count = arr.null_count.load();

    if (bitmap_preallocated_) {
      CopyBitmap(arr_bitmap->data(), arr.offset, arr.length, bitmap_, output_->offset);
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
      output_->buffers[0] = arr_bitmap;
    } else if (arr.offset % 8 == 0) {
      output_->buffers[0] =
          SliceBuffer(arr_bitmap, arr.offset / 8, BitUtil::BytesForBits(arr.length));
    } else {
      RETURN_NOT_OK(EnsureAllocated());
      CopyBitmap(arr_bitmap->data(), arr.offset, arr.length, bitmap_,
                 /*dst_offset=*/0);
    }
    return Status::OK();
  }

  Status PropagateMultiple() {
    // More than one array. We use BitmapAnd to intersect their bitmaps

    // Do not compute the intersection null count until it's needed
    RETURN_NOT_OK(EnsureAllocated());

    auto Accumulate = [&](const ArrayData& left, const ArrayData& right) {
      DCHECK(left.buffers[0]);
      DCHECK(right.buffers[0]);
      BitmapAnd(left.buffers[0]->data(), left.offset, right.buffers[0]->data(),
                right.offset, output_->length, output_->offset,
                output_->buffers[0]->mutable_data());
    };

    DCHECK_GT(arrays_with_nulls_.size(), 1);

    // Seed the output bitmap with the & of the first two bitmaps
    Accumulate(*arrays_with_nulls_[0], *arrays_with_nulls_[1]);

    // Accumulate the rest
    for (size_t i = 2; i < arrays_with_nulls_.size(); ++i) {
      Accumulate(*output_, *arrays_with_nulls_[i]);
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
        BitUtil::SetBitsTo(bitmap_, output_->offset, output_->length, true);
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
  const ExecBatch& batch_;
  std::vector<const ArrayData*> arrays_with_nulls_;
  bool is_all_null_ = false;
  ArrayData* output_;
  uint8_t* bitmap_;
  bool bitmap_preallocated_ = false;
};

std::shared_ptr<ChunkedArray> ToChunkedArray(const std::vector<Datum>& values,
                                             const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Array>> arrays;
  arrays.reserve(values.size());
  for (const Datum& val : values) {
    if (val.length() == 0) {
      // Skip empty chunks
      continue;
    }
    arrays.emplace_back(val.make_array());
  }
  return std::make_shared<ChunkedArray>(std::move(arrays), type);
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

    // Resolve the output descriptor for this kernel
    ARROW_ASSIGN_OR_RAISE(
        output_descr_, kernel_->signature->out_type().Resolve(kernel_ctx_, args.inputs));

    return Status::OK();
  }

 protected:
  // This is overridden by the VectorExecutor
  virtual Status SetupArgIteration(const std::vector<Datum>& args) {
    ARROW_ASSIGN_OR_RAISE(
        batch_iterator_, ExecBatchIterator::Make(args, exec_context()->exec_chunksize()));
    return Status::OK();
  }

  Result<std::shared_ptr<ArrayData>> PrepareOutput(int64_t length) {
    auto out = std::make_shared<ArrayData>(output_descr_.type, length);
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

  ExecContext* exec_context() { return kernel_ctx_->exec_context(); }
  KernelState* state() { return kernel_ctx_->state(); }

  // Not all of these members are used for every executor type

  KernelContext* kernel_ctx_;
  const KernelType* kernel_;
  std::unique_ptr<ExecBatchIterator> batch_iterator_;
  ValueDescr output_descr_;

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
  Status Execute(const std::vector<Datum>& args, ExecListener* listener) override {
    RETURN_NOT_OK(PrepareExecute(args));
    ExecBatch batch;
    while (batch_iterator_->Next(&batch)) {
      RETURN_NOT_OK(ExecuteBatch(batch, listener));
    }
    if (preallocate_contiguous_) {
      // If we preallocated one big chunk, since the kernel execution is
      // completed, we can now emit it
      RETURN_NOT_OK(listener->OnResult(std::move(preallocated_)));
    }
    return Status::OK();
  }

  Datum WrapResults(const std::vector<Datum>& inputs,
                    const std::vector<Datum>& outputs) override {
    if (output_descr_.shape == ValueDescr::SCALAR) {
      DCHECK_GT(outputs.size(), 0);
      if (outputs.size() == 1) {
        // Return as SCALAR
        return outputs[0];
      } else {
        // Return as COLLECTION
        return outputs;
      }
    } else {
      // If execution yielded multiple chunks (because large arrays were split
      // based on the ExecContext parameters, then the result is a ChunkedArray
      if (HaveChunkedArray(inputs) || outputs.size() > 1) {
        return ToChunkedArray(outputs, output_descr_.type);
      } else if (outputs.size() == 1) {
        // Outputs have just one element
        return outputs[0];
      } else {
        // XXX: In the case where no outputs are omitted, is returning a 0-length
        // array always the correct move?
        return MakeArrayOfNull(output_descr_.type, /*length=*/0,
                               exec_context()->memory_pool())
            .ValueOrDie();
      }
    }
  }

 protected:
  Status ExecuteBatch(const ExecBatch& batch, ExecListener* listener) {
    Datum out;
    RETURN_NOT_OK(PrepareNextOutput(batch, &out));

    if (output_descr_.shape == ValueDescr::ARRAY) {
      ArrayData* out_arr = out.mutable_array();
      if (kernel_->null_handling == NullHandling::INTERSECTION) {
        RETURN_NOT_OK(PropagateNulls(kernel_ctx_, batch, out_arr));
      } else if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
        out_arr->null_count = 0;
      }
    } else {
      if (kernel_->null_handling == NullHandling::INTERSECTION) {
        // set scalar validity
        out.scalar()->is_valid =
            std::all_of(batch.values.begin(), batch.values.end(),
                        [](const Datum& input) { return input.scalar()->is_valid; });
      } else if (kernel_->null_handling == NullHandling::OUTPUT_NOT_NULL) {
        out.scalar()->is_valid = true;
      }
    }

    RETURN_NOT_OK(kernel_->exec(kernel_ctx_, batch, &out));
    if (!preallocate_contiguous_) {
      // If we are producing chunked output rather than one big array, then
      // emit each chunk as soon as it's available
      RETURN_NOT_OK(listener->OnResult(std::move(out)));
    }
    return Status::OK();
  }

  Status PrepareExecute(const std::vector<Datum>& args) {
    RETURN_NOT_OK(this->SetupArgIteration(args));

    if (output_descr_.shape == ValueDescr::ARRAY) {
      // If the executor is configured to produce a single large Array output for
      // kernels supporting preallocation, then we do so up front and then
      // iterate over slices of that large array. Otherwise, we preallocate prior
      // to processing each batch emitted from the ExecBatchIterator
      RETURN_NOT_OK(SetupPreallocation(batch_iterator_->length()));
    }
    return Status::OK();
  }

  // We must accommodate two different modes of execution for preallocated
  // execution
  //
  // * A single large ("contiguous") allocation that we populate with results
  //   on a chunkwise basis according to the ExecBatchIterator. This permits
  //   parallelization even if the objective is to obtain a single Array or
  //   ChunkedArray at the end
  // * A standalone buffer preallocation for each chunk emitted from the
  //   ExecBatchIterator
  //
  // When data buffer preallocation is not possible (e.g. with BINARY / STRING
  // outputs), then contiguous results are only possible if the input is
  // contiguous.

  Status PrepareNextOutput(const ExecBatch& batch, Datum* out) {
    if (output_descr_.shape == ValueDescr::ARRAY) {
      if (preallocate_contiguous_) {
        // The output is already fully preallocated
        const int64_t batch_start_position = batch_iterator_->position() - batch.length;

        if (batch.length < batch_iterator_->length()) {
          // If this is a partial execution, then we write into a slice of
          // preallocated_
          out->value = preallocated_->Slice(batch_start_position, batch.length);
        } else {
          // Otherwise write directly into preallocated_. The main difference
          // computationally (versus the Slice approach) is that the null_count
          // may not need to be recomputed in the result
          out->value = preallocated_;
        }
      } else {
        // We preallocate (maybe) only for the output of processing the current
        // batch
        ARROW_ASSIGN_OR_RAISE(out->value, PrepareOutput(batch.length));
      }
    } else {
      // For scalar outputs, we set a null scalar of the correct type to
      // communicate the output type to the kernel if needed
      //
      // XXX: Is there some way to avoid this step?
      out->value = MakeNullScalar(output_descr_.type);
    }
    return Status::OK();
  }

  Status SetupPreallocation(int64_t total_length) {
    output_num_buffers_ = static_cast<int>(output_descr_.type->layout().buffers.size());

    // Decide if we need to preallocate memory for this kernel
    validity_preallocated_ =
        (kernel_->null_handling != NullHandling::COMPUTED_NO_PREALLOCATE &&
         kernel_->null_handling != NullHandling::OUTPUT_NOT_NULL &&
         output_descr_.type->id() != Type::NA);
    if (kernel_->mem_allocation == MemAllocation::PREALLOCATE) {
      ComputeDataPreallocate(*output_descr_.type, &data_preallocated_);
    }

    // Contiguous preallocation only possible on non-nested types if all
    // buffers are preallocated.  Otherwise, we must go chunk-by-chunk.
    //
    // Some kernels are also unable to write into sliced outputs, so we respect the
    // kernel's attributes.
    preallocate_contiguous_ =
        (exec_context()->preallocate_contiguous() && kernel_->can_write_into_slices &&
         validity_preallocated_ && !is_nested(output_descr_.type->id()) &&
         !is_dictionary(output_descr_.type->id()) &&
         data_preallocated_.size() == static_cast<size_t>(output_num_buffers_ - 1) &&
         std::all_of(data_preallocated_.begin(), data_preallocated_.end(),
                     [](const BufferPreallocation& prealloc) {
                       return prealloc.bit_width >= 0;
                     }));
    if (preallocate_contiguous_) {
      ARROW_ASSIGN_OR_RAISE(preallocated_, PrepareOutput(total_length));
    }
    return Status::OK();
  }

  // If true, and the kernel and output type supports preallocation (for both
  // the validity and data buffers), then we allocate one big array and then
  // iterate through it while executing the kernel in chunks
  bool preallocate_contiguous_ = false;

  // For storing a contiguous preallocation per above. Unused otherwise
  std::shared_ptr<ArrayData> preallocated_;
};

Status PackBatchNoChunks(const std::vector<Datum>& args, ExecBatch* out) {
  int64_t length = 0;
  for (const auto& arg : args) {
    switch (arg.kind()) {
      case Datum::SCALAR:
      case Datum::ARRAY:
      case Datum::CHUNKED_ARRAY:
        length = std::max(arg.length(), length);
        break;
      default:
        DCHECK(false);
        break;
    }
  }
  out->length = length;
  out->values = args;
  return Status::OK();
}

class VectorExecutor : public KernelExecutorImpl<VectorKernel> {
 public:
  Status Execute(const std::vector<Datum>& args, ExecListener* listener) override {
    RETURN_NOT_OK(PrepareExecute(args));
    ExecBatch batch;
    if (kernel_->can_execute_chunkwise) {
      while (batch_iterator_->Next(&batch)) {
        RETURN_NOT_OK(ExecuteBatch(batch, listener));
      }
    } else {
      RETURN_NOT_OK(PackBatchNoChunks(args, &batch));
      RETURN_NOT_OK(ExecuteBatch(batch, listener));
    }
    return Finalize(listener);
  }

  Datum WrapResults(const std::vector<Datum>& inputs,
                    const std::vector<Datum>& outputs) override {
    // If execution yielded multiple chunks (because large arrays were split
    // based on the ExecContext parameters, then the result is a ChunkedArray
    if (kernel_->output_chunked && (HaveChunkedArray(inputs) || outputs.size() > 1)) {
      return ToChunkedArray(outputs, output_descr_.type);
    } else if (outputs.size() == 1) {
      // Outputs have just one element
      return outputs[0];
    } else {
      // XXX: In the case where no outputs are omitted, is returning a 0-length
      // array always the correct move?
      return MakeArrayOfNull(output_descr_.type, /*length=*/0).ValueOrDie();
    }
  }

 protected:
  Status ExecuteBatch(const ExecBatch& batch, ExecListener* listener) {
    if (batch.length == 0) {
      // Skip empty batches. This may only happen when not using
      // ExecBatchIterator
      return Status::OK();
    }
    Datum out;
    if (output_descr_.shape == ValueDescr::ARRAY) {
      // We preallocate (maybe) only for the output of processing the current
      // batch
      ARROW_ASSIGN_OR_RAISE(out.value, PrepareOutput(batch.length));
    }

    if (kernel_->null_handling == NullHandling::INTERSECTION &&
        output_descr_.shape == ValueDescr::ARRAY) {
      RETURN_NOT_OK(PropagateNulls(kernel_ctx_, batch, out.mutable_array()));
    }
    RETURN_NOT_OK(kernel_->exec(kernel_ctx_, batch, &out));
    if (!kernel_->finalize) {
      // If there is no result finalizer (e.g. for hash-based functions, we can
      // emit the processed batch right away rather than waiting
      RETURN_NOT_OK(listener->OnResult(std::move(out)));
    } else {
      results_.emplace_back(std::move(out));
    }
    return Status::OK();
  }

  Status Finalize(ExecListener* listener) {
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

  Status SetupArgIteration(const std::vector<Datum>& args) override {
    if (kernel_->can_execute_chunkwise) {
      ARROW_ASSIGN_OR_RAISE(batch_iterator_, ExecBatchIterator::Make(
                                                 args, exec_context()->exec_chunksize()));
    }
    return Status::OK();
  }

  Status PrepareExecute(const std::vector<Datum>& args) {
    RETURN_NOT_OK(this->SetupArgIteration(args));
    output_num_buffers_ = static_cast<int>(output_descr_.type->layout().buffers.size());

    // Decide if we need to preallocate memory for this kernel
    validity_preallocated_ =
        (kernel_->null_handling != NullHandling::COMPUTED_NO_PREALLOCATE &&
         kernel_->null_handling != NullHandling::OUTPUT_NOT_NULL);
    if (kernel_->mem_allocation == MemAllocation::PREALLOCATE) {
      ComputeDataPreallocate(*output_descr_.type, &data_preallocated_);
    }
    return Status::OK();
  }

  std::vector<Datum> results_;
};

class ScalarAggExecutor : public KernelExecutorImpl<ScalarAggregateKernel> {
 public:
  Status Init(KernelContext* ctx, KernelInitArgs args) override {
    input_descrs_ = &args.inputs;
    options_ = args.options;
    return KernelExecutorImpl<ScalarAggregateKernel>::Init(ctx, args);
  }

  Status Execute(const std::vector<Datum>& args, ExecListener* listener) override {
    RETURN_NOT_OK(this->SetupArgIteration(args));

    ExecBatch batch;
    while (batch_iterator_->Next(&batch)) {
      // TODO: implement parallelism
      if (batch.length > 0) {
        RETURN_NOT_OK(Consume(batch));
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
  Status Consume(const ExecBatch& batch) {
    // FIXME(ARROW-11840) don't merge *any* aggegates for every batch
    ARROW_ASSIGN_OR_RAISE(
        auto batch_state,
        kernel_->init(kernel_ctx_, {kernel_, *input_descrs_, options_}));

    if (batch_state == nullptr) {
      return Status::Invalid("ScalarAggregation requires non-null kernel state");
    }

    KernelContext batch_ctx(exec_context());
    batch_ctx.SetState(batch_state.get());

    RETURN_NOT_OK(kernel_->consume(&batch_ctx, batch));
    RETURN_NOT_OK(kernel_->merge(kernel_ctx_, std::move(*batch_state), state()));
    return Status::OK();
  }

  const std::vector<ValueDescr>* input_descrs_;
  const FunctionOptions* options_;
};

template <typename ExecutorType,
          typename FunctionType = typename ExecutorType::FunctionType>
Result<std::unique_ptr<KernelExecutor>> MakeExecutor(ExecContext* ctx,
                                                     const Function* func,
                                                     const FunctionOptions* options) {
  DCHECK_EQ(ExecutorType::function_kind, func->kind());
  auto typed_func = checked_cast<const FunctionType*>(func);
  return std::unique_ptr<KernelExecutor>(new ExecutorType(ctx, typed_func, options));
}

}  // namespace

Status PropagateNulls(KernelContext* ctx, const ExecBatch& batch, ArrayData* output) {
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

std::unique_ptr<KernelExecutor> KernelExecutor::MakeScalar() {
  return ::arrow::internal::make_unique<detail::ScalarExecutor>();
}

std::unique_ptr<KernelExecutor> KernelExecutor::MakeVector() {
  return ::arrow::internal::make_unique<detail::VectorExecutor>();
}

std::unique_ptr<KernelExecutor> KernelExecutor::MakeScalarAggregate() {
  return ::arrow::internal::make_unique<detail::ScalarAggExecutor>();
}

}  // namespace detail

ExecContext::ExecContext(MemoryPool* pool, ::arrow::internal::Executor* executor,
                         FunctionRegistry* func_registry)
    : pool_(pool), executor_(executor) {
  this->func_registry_ = func_registry == nullptr ? GetFunctionRegistry() : func_registry;
}

CpuInfo* ExecContext::cpu_info() const { return CpuInfo::GetInstance(); }

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
    ExecContext default_ctx;
    return CallFunction(func_name, args, options, &default_ctx);
  }
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<const Function> func,
                        ctx->func_registry()->GetFunction(func_name));
  return func->Execute(args, options, ctx);
}

Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx) {
  return CallFunction(func_name, args, /*options=*/nullptr, ctx);
}

}  // namespace compute
}  // namespace arrow
