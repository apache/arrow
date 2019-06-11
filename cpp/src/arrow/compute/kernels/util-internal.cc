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
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

#include "arrow/compute/context.h"
#include "arrow/compute/kernel.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;

namespace compute {
namespace detail {

namespace {

inline void ZeroLastByte(Buffer* buffer) {
  *(buffer->mutable_data() + (buffer->size() - 1)) = 0;
}

Status AllocateValueBuffer(FunctionContext* ctx, const DataType& type, int64_t length,
                           std::shared_ptr<Buffer>* buffer) {
  if (type.id() != Type::NA) {
    const auto& fw_type = checked_cast<const FixedWidthType&>(type);

    int bit_width = fw_type.bit_width();
    int64_t buffer_size = 0;

    if (bit_width == 1) {
      buffer_size = BitUtil::BytesForBits(length);
    } else {
      DCHECK_EQ(bit_width % 8, 0)
          << "Only bit widths with multiple of 8 are currently supported";
      buffer_size = length * fw_type.bit_width() / 8;
    }
    RETURN_NOT_OK(ctx->Allocate(buffer_size, buffer));

    if (bit_width == 1 && buffer_size > 0) {
      // Some utility methods access the last byte before it might be
      // initialized this makes valgrind/asan unhappy, so we proactively
      // zero it.
      ZeroLastByte(buffer->get());
    }
  }
  return Status::OK();
}

}  // namespace

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
  do {
    const std::shared_ptr<Array> left_array = left_arrays[left_chunk_idx];
    const std::shared_ptr<Array> right_array = right_arrays[right_chunk_idx];
    int64_t common_length = std::min(left_array->length() - left_start_idx,
                                     right_array->length() - right_start_idx);
    std::shared_ptr<Array> left_op = left_array->Slice(left_start_idx, common_length);
    std::shared_ptr<Array> right_op = right_array->Slice(right_start_idx, common_length);

    Datum output;
    output.value = ArrayData::Make(kernel->out_type(), common_length);
    RETURN_NOT_OK(kernel->Call(ctx, left_op, right_op, &output));
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
  } while (elements_compared < left_length);
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

PrimitiveAllocatingUnaryKernel::PrimitiveAllocatingUnaryKernel(UnaryKernel* delegate)
    : delegate_(delegate) {}

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
  } else if (output->null_count > 0) {
    output->buffers[0] = input.buffers[0];
  }
  return Status::OK();
}

Status PropagateNulls(FunctionContext* ctx, const ArrayData& lhs, const ArrayData& rhs,
                      ArrayData* output) {
  return AssignNullIntersection(ctx, lhs, rhs, output);
}

Status SetAllNulls(FunctionContext* ctx, const ArrayData& input, ArrayData* output) {
  const int64_t length = input.length;
  if (output->buffers.size() == 0) {
    // Ensure we can assign a buffer
    output->buffers.resize(1);
  }

  // Handle validity bitmap
  if (output->buffers[0] == nullptr) {
    std::shared_ptr<Buffer> buffer;
    RETURN_NOT_OK(ctx->Allocate(BitUtil::BytesForBits(length), &buffer));
    output->buffers[0] = std::move(buffer);
  }

  output->null_count = length;
  BitUtil::SetBitsTo(output->buffers[0]->mutable_data(), 0, length, false);

  return Status::OK();
}

Status AssignNullIntersection(FunctionContext* ctx, const ArrayData& left,
                              const ArrayData& right, ArrayData* output) {
  if (output->buffers.size() == 0) {
    // Ensure we can assign a buffer
    output->buffers.resize(1);
  }

  if (left.GetNullCount() > 0 && right.GetNullCount() > 0) {
    RETURN_NOT_OK(BitmapAnd(ctx->memory_pool(), left.buffers[0]->data(), left.offset,
                            right.buffers[0]->data(), right.offset, right.length, 0,
                            &(output->buffers[0])));
    // Force computation of null count.
    output->null_count = kUnknownNullCount;
    output->GetNullCount();
    return Status::OK();
  } else if (left.null_count != 0) {
    return PropagateNulls(ctx, left, output);
  } else {
    // right has a positive null_count or both are zero.
    return PropagateNulls(ctx, right, output);
  }
  return Status::OK();
}

Status PrimitiveAllocatingUnaryKernel::Call(FunctionContext* ctx, const Datum& input,
                                            Datum* out) {
  DCHECK_EQ(out->kind(), Datum::ARRAY);
  ArrayData* result = out->array().get();
  result->buffers.resize(2);

  const int64_t length = input.length();
  // Allocate the value buffer
  RETURN_NOT_OK(AllocateValueBuffer(ctx, *out_type(), length, &(result->buffers[1])));
  return delegate_->Call(ctx, input, out);
}

std::shared_ptr<DataType> PrimitiveAllocatingUnaryKernel::out_type() const {
  return delegate_->out_type();
}

PrimitiveAllocatingBinaryKernel::PrimitiveAllocatingBinaryKernel(BinaryKernel* delegate)
    : delegate_(delegate) {}

Status PrimitiveAllocatingBinaryKernel::Call(FunctionContext* ctx, const Datum& left,
                                             const Datum& right, Datum* out) {
  DCHECK_EQ(out->kind(), Datum::ARRAY);
  ArrayData* result = out->array().get();
  result->buffers.resize(2);

  const int64_t length = result->length;
  RETURN_NOT_OK(AllocateValueBuffer(ctx, *out_type(), length, &(result->buffers[1])));

  // Allocate the value buffer
  return delegate_->Call(ctx, left, right, out);
}

std::shared_ptr<DataType> PrimitiveAllocatingBinaryKernel::out_type() const {
  return delegate_->out_type();
}

}  // namespace detail
}  // namespace compute
}  // namespace arrow
