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
#include "arrow/scalar.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;
using internal::CopyBitmap;
using internal::CpuInfo;

namespace compute {

namespace {

Result<std::shared_ptr<Buffer>> AllocateDataBuffer(KernelContext* ctx, int64_t length,
                                                   int bit_width) {
  if (bit_width == 1) {
    return ctx->AllocateBitmap(length);
  } else {
    ARROW_CHECK_EQ(bit_width % 8, 0)
        << "Only bit widths with multiple of 8 are currently supported";
    int64_t buffer_size = length * bit_width / 8;
    return ctx->Allocate(buffer_size);
  }
  return Status::OK();
}

bool CanPreallocate(const DataType& type) {
  // There are currently cases where NullType is the output type, so we disable
  // any preallocation logic when this occurs
  return is_fixed_width(type.id()) && type.id() != Type::NA;
}

Status GetValueDescriptors(const std::vector<Datum>& args,
                           std::vector<ValueDescr>* descrs) {
  for (const auto& arg : args) {
    descrs->emplace_back(arg.descr());
  }
  return Status::OK();
}

}  // namespace

namespace detail {

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

bool ArrayHasNulls(const ArrayData& data) {
  // As discovered in ARROW-8863 (and not only for that reason)
  // ArrayData::null_count can -1 even when buffers[0] is nullptr. So we check
  // for both cases (nullptr means no nulls, or null_count already computed)
  if (data.type->id() == Type::NA) {
    return true;
  } else if (data.buffers[0] == nullptr) {
    return false;
  } else {
    // Do not count the bits if they haven't been counted already
    const int64_t known_null_count = data.null_count.load();
    return known_null_count == kUnknownNullCount || known_null_count > 0;
  }
}

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
    // At this point, the values in batch_.values must have been validated to
    // all be value-like
    for (const Datum& val : batch_.values) {
      if (val.kind() == Datum::ARRAY) {
        if (ArrayHasNulls(*val.array())) {
          values_with_nulls_.push_back(&val);
        }
      } else if (!val.scalar()->is_valid) {
        values_with_nulls_.push_back(&val);
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

  Result<bool> ShortCircuitIfAllNull() {
    // An all-null value (scalar null or all-null array) gives us a short
    // circuit opportunity
    bool is_all_null = false;
    std::shared_ptr<Buffer> all_null_bitmap;

    // Walk all the values with nulls instead of breaking on the first in case
    // we find a bitmap that can be reused in the non-preallocated case
    for (const Datum* value : values_with_nulls_) {
      if (value->type()->id() == Type::NA) {
        // No bitmap
        is_all_null = true;
      } else if (value->kind() == Datum::ARRAY) {
        const ArrayData& arr = *value->array();
        if (arr.null_count.load() == arr.length) {
          // Pluck the all null bitmap so we can set it in the output if it was
          // not pre-allocated
          all_null_bitmap = arr.buffers[0];
          is_all_null = true;
        }
      } else {
        // Scalar
        is_all_null = !value->scalar()->is_valid;
      }
    }
    if (!is_all_null) {
      return false;
    }

    // OK, the output should be all null
    output_->null_count = output_->length;

    if (!bitmap_preallocated_ && all_null_bitmap) {
      // If we did not pre-allocate memory, and we observed an all-null bitmap,
      // then we can zero-copy it into the output
      output_->buffers[0] = std::move(all_null_bitmap);
    } else {
      RETURN_NOT_OK(EnsureAllocated());
      BitUtil::SetBitsTo(bitmap_, output_->offset, output_->length, false);
    }
    return true;
  }

  Status PropagateSingle() {
    // One array
    const ArrayData& arr = *values_with_nulls_[0]->array();
    const std::shared_ptr<Buffer>& arr_bitmap = arr.buffers[0];

    // Reuse the null count if it's known
    output_->null_count = arr.null_count.load();

    if (bitmap_preallocated_) {
      CopyBitmap(arr_bitmap->data(), arr.offset, arr.length, bitmap_, output_->offset);
    } else {
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
    }
    return Status::OK();
  }

  Status PropagateMultiple() {
    // More than one array. We use BitmapAnd to intersect their bitmaps

    // Do not compute the intersection null count until it's needed
    RETURN_NOT_OK(EnsureAllocated());

    auto Accumulate = [&](const ArrayData& left, const ArrayData& right) {
      // This is a precondition of reaching this code path
      DCHECK(left.buffers[0]);
      DCHECK(right.buffers[0]);
      BitmapAnd(left.buffers[0]->data(), left.offset, right.buffers[0]->data(),
                right.offset, output_->length, output_->offset,
                output_->buffers[0]->mutable_data());
    };

    DCHECK_GT(values_with_nulls_.size(), 1);

    // Seed the output bitmap with the & of the first two bitmaps
    Accumulate(*values_with_nulls_[0]->array(), *values_with_nulls_[1]->array());

    // Accumulate the rest
    for (size_t i = 2; i < values_with_nulls_.size(); ++i) {
      Accumulate(*output_, *values_with_nulls_[i]->array());
    }
    return Status::OK();
  }

  Status Execute() {
    bool finished = false;
    ARROW_ASSIGN_OR_RAISE(finished, ShortCircuitIfAllNull());
    if (finished) {
      return Status::OK();
    }

    // At this point, by construction we know that all of the values in
    // values_with_nulls_ are arrays that are not all null. So there are a
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

    if (values_with_nulls_.size() == 0) {
      // No arrays with nulls case
      output_->null_count = 0;
      if (bitmap_preallocated_) {
        BitUtil::SetBitsTo(bitmap_, output_->offset, output_->length, true);
      }
      return Status::OK();
    } else if (values_with_nulls_.size() == 1) {
      return PropagateSingle();
    } else {
      return PropagateMultiple();
    }
  }

 private:
  KernelContext* ctx_;
  const ExecBatch& batch_;
  std::vector<const Datum*> values_with_nulls_;
  ArrayData* output_;
  uint8_t* bitmap_;
  bool bitmap_preallocated_ = false;
};

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

std::shared_ptr<ChunkedArray> ToChunkedArray(const std::vector<Datum>& values,
                                             const std::shared_ptr<DataType>& type) {
  std::vector<std::shared_ptr<Array>> arrays;
  for (const auto& val : values) {
    auto boxed = val.make_array();
    if (boxed->length() == 0) {
      // Skip empty chunks
      continue;
    }
    arrays.emplace_back(std::move(boxed));
  }
  return std::make_shared<ChunkedArray>(arrays, type);
}

bool HaveChunkedArray(const std::vector<Datum>& values) {
  for (const auto& value : values) {
    if (value.kind() == Datum::CHUNKED_ARRAY) {
      return true;
    }
  }
  return false;
}

Status CheckAllValues(const std::vector<Datum>& values) {
  for (const auto& value : values) {
    if (!value.is_value()) {
      return Status::Invalid("Tried executing function with non-value type: ",
                             value.ToString());
    }
  }
  return Status::OK();
}

template <typename FunctionType>
class FunctionExecutorImpl : public FunctionExecutor {
 public:
  FunctionExecutorImpl(ExecContext* exec_ctx, const FunctionType* func,
                       const FunctionOptions* options)
      : exec_ctx_(exec_ctx), kernel_ctx_(exec_ctx), func_(func), options_(options) {}

 protected:
  using KernelType = typename FunctionType::KernelType;

  void Reset() {}

  Status InitState() {
    // Some kernels require initialization of an opaque state object
    if (kernel_->init) {
      KernelInitArgs init_args{kernel_, input_descrs_, options_};
      state_ = kernel_->init(&kernel_ctx_, init_args);
      ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
      kernel_ctx_.SetState(state_.get());
    }
    return Status::OK();
  }

  // This is overridden by the VectorExecutor
  virtual Status SetupArgIteration(const std::vector<Datum>& args) {
    ARROW_ASSIGN_OR_RAISE(batch_iterator_,
                          ExecBatchIterator::Make(args, exec_ctx_->exec_chunksize()));
    return Status::OK();
  }

  Status BindArgs(const std::vector<Datum>& args) {
    RETURN_NOT_OK(GetValueDescriptors(args, &input_descrs_));
    ARROW_ASSIGN_OR_RAISE(kernel_, func_->DispatchExact(input_descrs_));

    // Initialize kernel state, since type resolution may depend on this state
    RETURN_NOT_OK(this->InitState());

    // Resolve the output descriptor for this kernel
    ARROW_ASSIGN_OR_RAISE(output_descr_, kernel_->signature->out_type().Resolve(
                                             &kernel_ctx_, input_descrs_));

    return SetupArgIteration(args);
  }

  Result<std::shared_ptr<ArrayData>> PrepareOutput(int64_t length) {
    auto out = std::make_shared<ArrayData>(output_descr_.type, length);
    out->buffers.resize(output_num_buffers_);

    if (validity_preallocated_) {
      ARROW_ASSIGN_OR_RAISE(out->buffers[0], kernel_ctx_.AllocateBitmap(length));
    }
    if (data_preallocated_) {
      const auto& fw_type = checked_cast<const FixedWidthType&>(*out->type);
      ARROW_ASSIGN_OR_RAISE(
          out->buffers[1], AllocateDataBuffer(&kernel_ctx_, length, fw_type.bit_width()));
    }
    return out;
  }

  ValueDescr output_descr() const override { return output_descr_; }

  // Not all of these members are used for every executor type

  ExecContext* exec_ctx_;
  KernelContext kernel_ctx_;
  const FunctionType* func_;
  const KernelType* kernel_;
  std::unique_ptr<ExecBatchIterator> batch_iterator_;
  std::unique_ptr<KernelState> state_;
  std::vector<ValueDescr> input_descrs_;
  ValueDescr output_descr_;
  const FunctionOptions* options_;

  int output_num_buffers_;

  // If true, then the kernel writes into a preallocated data buffer
  bool data_preallocated_ = false;

  // If true, then memory is preallocated for the validity bitmap with the same
  // strategy as the data buffer(s).
  bool validity_preallocated_ = false;
};

class ScalarExecutor : public FunctionExecutorImpl<ScalarFunction> {
 public:
  using FunctionType = ScalarFunction;
  static constexpr Function::Kind function_kind = Function::SCALAR;
  using BASE = FunctionExecutorImpl<ScalarFunction>;
  using BASE::BASE;

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
        return MakeArrayOfNull(output_descr_.type, /*length=*/0).ValueOrDie();
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
        RETURN_NOT_OK(PropagateNulls(&kernel_ctx_, batch, out_arr));
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

    kernel_->exec(&kernel_ctx_, batch, &out);
    ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
    if (!preallocate_contiguous_) {
      // If we are producing chunked output rather than one big array, then
      // emit each chunk as soon as it's available
      RETURN_NOT_OK(listener->OnResult(std::move(out)));
    }
    return Status::OK();
  }

  Status PrepareExecute(const std::vector<Datum>& args) {
    this->Reset();
    RETURN_NOT_OK(this->BindArgs(args));

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
    data_preallocated_ = ((kernel_->mem_allocation == MemAllocation::PREALLOCATE) &&
                          CanPreallocate(*output_descr_.type));
    validity_preallocated_ =
        (kernel_->null_handling != NullHandling::COMPUTED_NO_PREALLOCATE &&
         kernel_->null_handling != NullHandling::OUTPUT_NOT_NULL);

    // Contiguous preallocation only possible if both the VALIDITY and DATA can
    // be preallocated. Otherwise, we must go chunk-by-chunk. Note that when
    // the DATA cannot be preallocated, the VALIDITY may still be preallocated
    // depending on the NullHandling of the kernel
    //
    // Some kernels are unable to write into sliced outputs, so we respect the
    // kernel's attributes
    preallocate_contiguous_ =
        (exec_ctx_->preallocate_contiguous() && kernel_->can_write_into_slices &&
         data_preallocated_ && validity_preallocated_);
    if (preallocate_contiguous_) {
      DCHECK_EQ(2, output_num_buffers_);
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
        length = std::max(arg.length(), length);
        break;
      case Datum::CHUNKED_ARRAY:
        return Status::Invalid("Kernel does not support chunked array arguments");
      default:
        DCHECK(false);
        break;
    }
  }
  out->length = length;
  out->values = args;
  return Status::OK();
}

class VectorExecutor : public FunctionExecutorImpl<VectorFunction> {
 public:
  using FunctionType = VectorFunction;
  static constexpr Function::Kind function_kind = Function::VECTOR;
  using BASE = FunctionExecutorImpl<VectorFunction>;
  using BASE::BASE;

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
    if (kernel_->output_chunked) {
      if (HaveChunkedArray(inputs) || outputs.size() > 1) {
        return ToChunkedArray(outputs, output_descr_.type);
      } else if (outputs.size() == 1) {
        // Outputs have just one element
        return outputs[0];
      } else {
        // XXX: In the case where no outputs are omitted, is returning a 0-length
        // array always the correct move?
        return MakeArrayOfNull(output_descr_.type, /*length=*/0).ValueOrDie();
      }
    } else {
      return outputs[0];
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
      RETURN_NOT_OK(PropagateNulls(&kernel_ctx_, batch, out.mutable_array()));
    }
    kernel_->exec(&kernel_ctx_, batch, &out);
    ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
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
      kernel_->finalize(&kernel_ctx_, &results_);
      ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
      for (const auto& result : results_) {
        RETURN_NOT_OK(listener->OnResult(result));
      }
    }
    return Status::OK();
  }

  Status SetupArgIteration(const std::vector<Datum>& args) override {
    if (kernel_->can_execute_chunkwise) {
      ARROW_ASSIGN_OR_RAISE(batch_iterator_,
                            ExecBatchIterator::Make(args, exec_ctx_->exec_chunksize()));
    }
    return Status::OK();
  }

  Status PrepareExecute(const std::vector<Datum>& args) {
    this->Reset();
    RETURN_NOT_OK(this->BindArgs(args));
    output_num_buffers_ = static_cast<int>(output_descr_.type->layout().buffers.size());

    // Decide if we need to preallocate memory for this kernel
    data_preallocated_ = ((kernel_->mem_allocation == MemAllocation::PREALLOCATE) &&
                          CanPreallocate(*output_descr_.type));
    validity_preallocated_ =
        (kernel_->null_handling != NullHandling::COMPUTED_NO_PREALLOCATE &&
         kernel_->null_handling != NullHandling::OUTPUT_NOT_NULL);
    return Status::OK();
  }

  std::vector<Datum> results_;
};

class ScalarAggExecutor : public FunctionExecutorImpl<ScalarAggregateFunction> {
 public:
  using FunctionType = ScalarAggregateFunction;
  static constexpr Function::Kind function_kind = Function::SCALAR_AGGREGATE;
  using BASE = FunctionExecutorImpl<ScalarAggregateFunction>;
  using BASE::BASE;

  Status Execute(const std::vector<Datum>& args, ExecListener* listener) override {
    RETURN_NOT_OK(BindArgs(args));

    ExecBatch batch;
    while (batch_iterator_->Next(&batch)) {
      // TODO: implement parallelism
      if (batch.length > 0) {
        RETURN_NOT_OK(Consume(batch));
      }
    }

    Datum out;
    kernel_->finalize(&kernel_ctx_, &out);
    ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
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
    KernelInitArgs init_args{kernel_, input_descrs_, options_};
    auto batch_state = kernel_->init(&kernel_ctx_, init_args);
    ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);

    if (batch_state == nullptr) {
      kernel_ctx_.SetStatus(
          Status::Invalid("ScalarAggregation requires non-null kernel state"));
      return kernel_ctx_.status();
    }

    KernelContext batch_ctx(exec_ctx_);
    batch_ctx.SetState(batch_state.get());

    kernel_->consume(&batch_ctx, batch);
    ARROW_CTX_RETURN_IF_ERROR(&batch_ctx);

    kernel_->merge(&kernel_ctx_, *batch_state, state_.get());
    ARROW_CTX_RETURN_IF_ERROR(&kernel_ctx_);
    return Status::OK();
  }
};

template <typename ExecutorType,
          typename FunctionType = typename ExecutorType::FunctionType>
Result<std::unique_ptr<FunctionExecutor>> MakeExecutor(ExecContext* ctx,
                                                       const Function* func,
                                                       const FunctionOptions* options) {
  DCHECK_EQ(ExecutorType::function_kind, func->kind());
  auto typed_func = checked_cast<const FunctionType*>(func);
  return std::unique_ptr<FunctionExecutor>(new ExecutorType(ctx, typed_func, options));
}

Result<std::unique_ptr<FunctionExecutor>> FunctionExecutor::Make(
    ExecContext* ctx, const Function* func, const FunctionOptions* options) {
  switch (func->kind()) {
    case Function::SCALAR:
      return MakeExecutor<detail::ScalarExecutor>(ctx, func, options);
    case Function::VECTOR:
      return MakeExecutor<detail::VectorExecutor>(ctx, func, options);
    case Function::SCALAR_AGGREGATE:
      return MakeExecutor<detail::ScalarAggExecutor>(ctx, func, options);
    default:
      DCHECK(false);
      return nullptr;
  }
}

}  // namespace detail

ExecContext::ExecContext(MemoryPool* pool, FunctionRegistry* func_registry)
    : pool_(pool) {
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
  if (options == nullptr) {
    options = func->default_options();
  }
  return func->Execute(args, options, ctx);
}

Result<Datum> CallFunction(const std::string& func_name, const std::vector<Datum>& args,
                           ExecContext* ctx) {
  return CallFunction(func_name, args, /*options=*/nullptr, ctx);
}

}  // namespace compute
}  // namespace arrow
