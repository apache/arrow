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

#include "arrow/compute/kernels/util-internal.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/table.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

namespace arrow {

using internal::checked_cast;

namespace compute {
namespace detail {

Status InvokeUnaryArrayKernel(FunctionContext* ctx, UnaryKernel* kernel,
                              const Datum& value, std::vector<Datum>* outputs) {
  if (value.kind() == Datum::ARRAY) {
    Datum out;
    out.value = ArrayData::Make(kernel->out_type(), value.array()->length);
    RETURN_NOT_OK(kernel->Call(ctx, value, &out));
    outputs->push_back(out);
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    const ChunkedArray& array = *value.chunked_array();
    for (int i = 0; i < array.num_chunks(); i++) {
      Datum out;
      out.value = ArrayData::Make(kernel->out_type(), array.chunk(i)->length());
      RETURN_NOT_OK(kernel->Call(ctx, array.chunk(i), &out));
      outputs->push_back(out);
    }
  } else {
    return Status::Invalid("Input Datum was not array-like");
  }
  return Status::OK();
}

Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right,
                               std::vector<Datum>* outputs) {
  int64_t left_length;
  std::vector<std::shared_ptr<Array>> left_arrays;
  if (left.kind() == Datum::ARRAY) {
    left_length = left.array()->length;
    left_arrays.push_back(left.make_array());
  } else if (left.kind() == Datum::CHUNKED_ARRAY) {
    left_length = left.chunked_array()->length();
    left_arrays = left.chunked_array()->chunks();
  } else {
    return Status::Invalid("Left input Datum was not array-like");
  }

  int64_t right_length;
  std::vector<std::shared_ptr<Array>> right_arrays;
  if (right.kind() == Datum::ARRAY) {
    right_length = right.array()->length;
    right_arrays.push_back(right.make_array());
  } else if (right.kind() == Datum::CHUNKED_ARRAY) {
    right_length = right.chunked_array()->length();
    right_arrays = right.chunked_array()->chunks();
  } else {
    return Status::Invalid("Right input Datum was not array-like");
  }

  if (right_length != left_length) {
    return Status::Invalid("Right and left have different lengths");
  }

  // TODO: Remove duplication with ChunkedArray::Equals
  int left_chunk_idx = 0;
  int64_t left_start_idx = 0;
  int right_chunk_idx = 0;
  int64_t right_start_idx = 0;

  int64_t elements_compared = 0;
  while (elements_compared < left_length) {
    const std::shared_ptr<Array> left_array = left_arrays[left_chunk_idx];
    const std::shared_ptr<Array> right_array = right_arrays[right_chunk_idx];
    int64_t common_length = std::min(left_array->length() - left_start_idx,
                                     right_array->length() - right_start_idx);

    std::shared_ptr<Array> left_op = left_array->Slice(left_start_idx, common_length);
    std::shared_ptr<Array> right_op = right_array->Slice(right_start_idx, common_length);
    Datum output;
    RETURN_NOT_OK(kernel->Call(ctx, Datum(left_op), Datum(right_op), &output));
    outputs->push_back(output);

    elements_compared += common_length;

    // If we have exhausted the current chunk, proceed to the next one individually.
    if (left_start_idx + common_length == left_array->length()) {
      left_chunk_idx++;
      left_start_idx = 0;
    } else {
      left_start_idx += common_length;
    }

    if (right_start_idx + common_length == right_array->length()) {
      right_chunk_idx++;
      right_start_idx = 0;
    } else {
      right_start_idx += common_length;
    }
  }

  return Status::OK();
}

Status InvokeBinaryArrayKernel(FunctionContext* ctx, BinaryKernel* kernel,
                               const Datum& left, const Datum& right, Datum* output) {
  std::vector<Datum> result;
  RETURN_NOT_OK(InvokeBinaryArrayKernel(ctx, kernel, left, right, &result));
  *output = detail::WrapDatumsLike(left, result);
  return Status::OK();
}

Datum WrapArraysLike(const Datum& value,
                     const std::vector<std::shared_ptr<Array>>& arrays) {
  // Create right kind of datum
  if (value.kind() == Datum::ARRAY) {
    return Datum(arrays[0]->data());
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    return Datum(std::make_shared<ChunkedArray>(arrays));
  } else {
    DCHECK(false) << "unhandled datum kind";
    return Datum();
  }
}

Datum WrapDatumsLike(const Datum& value, const std::vector<Datum>& datums) {
  // Create right kind of datum
  if (value.kind() == Datum::ARRAY) {
    DCHECK_EQ(1, datums.size());
    return Datum(datums[0].array());
  } else if (value.kind() == Datum::CHUNKED_ARRAY) {
    std::vector<std::shared_ptr<Array>> arrays;
    for (const Datum& datum : datums) {
      DCHECK_EQ(Datum::ARRAY, datum.kind());
      arrays.emplace_back(MakeArray(datum.array()));
    }
    return Datum(std::make_shared<ChunkedArray>(arrays));
  } else {
    DCHECK(false) << "unhandled datum kind";
    return Datum();
  }
}

PrimitiveAllocatingUnaryKernel::PrimitiveAllocatingUnaryKernel(
    UnaryKernel* delegate, const std::shared_ptr<DataType>& out_type)
    : delegate_(delegate), out_type_(out_type) {}

PrimitiveAllocatingUnaryKernel::PrimitiveAllocatingUnaryKernel(
    std::unique_ptr<UnaryKernel> delegate, const std::shared_ptr<DataType>& out_type)
    : PrimitiveAllocatingUnaryKernel(delegate.get(), out_type) {
  owned_delegate_ = std::move(delegate);
}

inline void ZeroLastByte(Buffer* buffer) {
  *(buffer->mutable_data() + (buffer->size() - 1)) = 0;
}

Status PropagateNulls(FunctionContext* ctx, const ArrayData& input, ArrayData* output) {
  const int64_t length = input.length;

  if (output->buffers.size() == 0) {
    // Ensure we can assign a buffer
    output->buffers.resize(1);
  }

  // Handle validity bitmap
  output->null_count = input.GetNullCount();
  if (input.offset != 0 && output->null_count > 0) {
    DCHECK(input.buffers[0]);
    const Buffer& validity_bitmap = *input.buffers[0];
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(ctx->Allocate(BitUtil::BytesForBits(length), &buffer));
    // Per spec all trailing bits should indicate nullness, since
    // the last byte might only be partially set, we ensure the
    // remaining bit is set.
    ZeroLastByte(buffer.get());
    buffer->ZeroPadding();
    internal::CopyBitmap(validity_bitmap.data(), input.offset, length,
                         buffer->mutable_data(), 0 /* destination offset */);
    output->buffers[0] = std::move(buffer);
  } else {
    output->buffers[0] = input.buffers[0];
  }
  return Status::OK();
}

Status PrimitiveAllocatingUnaryKernel::Call(FunctionContext* ctx, const Datum& input,
                                            Datum* out) {
  std::vector<std::shared_ptr<Buffer>> data_buffers;
  const ArrayData& in_data = *input.array();

  DCHECK_EQ(out->kind(), Datum::ARRAY);

  ArrayData* result = out->array().get();

  result->buffers.resize(2);

  const int64_t length = in_data.length;

  // Allocate the value buffer
  std::shared_ptr<Buffer> buffer;
  if (out_type_->id() != Type::NA) {
    const auto& fw_type = checked_cast<const FixedWidthType&>(*out_type_);

    int bit_width = fw_type.bit_width();
    int64_t buffer_size = 0;

    if (bit_width == 1) {
      buffer_size = BitUtil::BytesForBits(length);
    } else {
      DCHECK_EQ(bit_width % 8, 0)
          << "Only bit widths with multiple of 8 are currently supported";
      buffer_size = length * fw_type.bit_width() / 8;
    }
    RETURN_NOT_OK(ctx->Allocate(buffer_size, &buffer));
    buffer->ZeroPadding();

    if (bit_width == 1 && buffer_size > 0) {
      // Some utility methods access the last byte before it might be
      // initialized this makes valgrind/asan unhappy, so we proactively
      // zero it.
      ZeroLastByte(buffer.get());
    }

    memset(buffer->mutable_data(), 0, buffer_size);
    result->buffers[1] = std::move(buffer);
  }
  return delegate_->Call(ctx, input, out);
}

std::shared_ptr<DataType> PrimitiveAllocatingUnaryKernel::out_type() const {
  return delegate_->out_type();
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow
