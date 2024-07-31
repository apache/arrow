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

#include "arrow/array/builder_binary.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/visit_data_inline.h"

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Binary/StringView
BinaryViewBuilder::BinaryViewBuilder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool)
    : BinaryViewBuilder(pool) {}

Status BinaryViewBuilder::AppendArraySlice(const ArraySpan& array, int64_t offset,
                                           int64_t length) {
  auto bitmap = array.GetValues<uint8_t>(0, 0);
  auto values = array.GetValues<BinaryViewType::c_type>(1) + offset;

  int64_t out_of_line_total = 0, i = 0;
  VisitNullBitmapInline(
      array.buffers[0].data, array.offset + offset, length, array.null_count,
      [&] {
        if (!values[i].is_inline()) {
          out_of_line_total += static_cast<int64_t>(values[i].size());
        }
        ++i;
      },
      [&] { ++i; });

  RETURN_NOT_OK(Reserve(length));
  RETURN_NOT_OK(ReserveData(out_of_line_total));

  for (int64_t i = 0; i < length; i++) {
    if (bitmap && !bit_util::GetBit(bitmap, array.offset + offset + i)) {
      UnsafeAppendNull();
      continue;
    }

    UnsafeAppend(util::FromBinaryView(values[i], array.GetVariadicBuffers().data()));
  }
  return Status::OK();
}

Status BinaryViewBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  ARROW_ASSIGN_OR_RAISE(auto null_bitmap, null_bitmap_builder_.FinishWithLength(length_));
  ARROW_ASSIGN_OR_RAISE(auto data, data_builder_.FinishWithLength(length_));
  ARROW_ASSIGN_OR_RAISE(auto byte_buffers, data_heap_builder_.Finish());
  BufferVector buffers(byte_buffers.size() + 2);
  buffers[0] = std::move(null_bitmap);
  buffers[1] = std::move(data);
  std::move(byte_buffers.begin(), byte_buffers.end(), buffers.begin() + 2);
  *out = ArrayData::Make(type(), length_, std::move(buffers), null_count_);
  Reset();
  return Status::OK();
}

Status BinaryViewBuilder::ReserveData(int64_t length) {
  return data_heap_builder_.Reserve(length);
}

void BinaryViewBuilder::Reset() {
  ArrayBuilder::Reset();
  data_builder_.Reset();
  data_heap_builder_.Reset();
}

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryBuilder::FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                                               MemoryPool* pool, int64_t alignment)
    : ArrayBuilder(pool, alignment),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()),
      byte_builder_(pool, alignment) {}

void FixedSizeBinaryBuilder::CheckValueSize(int64_t size) {
  DCHECK_EQ(size, byte_width_) << "Appending wrong size to FixedSizeBinaryBuilder";
}

Status FixedSizeBinaryBuilder::AppendValues(const uint8_t* data, int64_t length,
                                            const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
  return byte_builder_.Append(data, length * byte_width_);
}

Status FixedSizeBinaryBuilder::AppendValues(const uint8_t* data, int64_t length,
                                            const uint8_t* validity,
                                            int64_t bitmap_offset) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(validity, bitmap_offset, length);
  return byte_builder_.Append(data, length * byte_width_);
}

Status FixedSizeBinaryBuilder::AppendNull() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendNull();
  return Status::OK();
}

Status FixedSizeBinaryBuilder::AppendNulls(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(length, false);
  byte_builder_.UnsafeAppend(/*num_copies=*/length * byte_width_, 0);
  return Status::OK();
}

Status FixedSizeBinaryBuilder::AppendEmptyValue() {
  RETURN_NOT_OK(Reserve(1));
  UnsafeAppendToBitmap(true);
  byte_builder_.UnsafeAppend(/*num_copies=*/byte_width_, 0);
  return Status::OK();
}

Status FixedSizeBinaryBuilder::AppendEmptyValues(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(length, true);
  byte_builder_.UnsafeAppend(/*num_copies=*/length * byte_width_, 0);
  return Status::OK();
}

void FixedSizeBinaryBuilder::Reset() {
  ArrayBuilder::Reset();
  byte_builder_.Reset();
}

Status FixedSizeBinaryBuilder::Resize(int64_t capacity) {
  RETURN_NOT_OK(CheckCapacity(capacity));
  RETURN_NOT_OK(byte_builder_.Resize(capacity * byte_width_));
  return ArrayBuilder::Resize(capacity);
}

Status FixedSizeBinaryBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));

  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);

  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

const uint8_t* FixedSizeBinaryBuilder::GetValue(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return data_ptr + i * byte_width_;
}

std::string_view FixedSizeBinaryBuilder::GetView(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return {reinterpret_cast<const char*>(data_ptr + i * byte_width_),
          static_cast<size_t>(byte_width_)};
}

// ----------------------------------------------------------------------
// ChunkedArray builders

namespace internal {

ChunkedBinaryBuilder::ChunkedBinaryBuilder(int32_t max_chunk_value_length,
                                           MemoryPool* pool)
    : max_chunk_value_length_(max_chunk_value_length), builder_(new BinaryBuilder(pool)) {
  DCHECK_LE(max_chunk_value_length, kBinaryMemoryLimit);
}

ChunkedBinaryBuilder::ChunkedBinaryBuilder(int32_t max_chunk_value_length,
                                           int32_t max_chunk_length, MemoryPool* pool)
    : ChunkedBinaryBuilder(max_chunk_value_length, pool) {
  max_chunk_length_ = max_chunk_length;
}

Status ChunkedBinaryBuilder::Finish(ArrayVector* out) {
  if (builder_->length() > 0 || chunks_.size() == 0) {
    std::shared_ptr<Array> chunk;
    RETURN_NOT_OK(builder_->Finish(&chunk));
    chunks_.emplace_back(std::move(chunk));
  }
  *out = std::move(chunks_);
  return Status::OK();
}

Status ChunkedBinaryBuilder::NextChunk() {
  std::shared_ptr<Array> chunk;
  RETURN_NOT_OK(builder_->Finish(&chunk));
  chunks_.emplace_back(std::move(chunk));

  if (auto capacity = extra_capacity_) {
    extra_capacity_ = 0;
    return Reserve(capacity);
  }

  return Status::OK();
}

Status ChunkedStringBuilder::Finish(ArrayVector* out) {
  RETURN_NOT_OK(ChunkedBinaryBuilder::Finish(out));

  // Change data type to string/utf8
  for (auto& chunk : *out) {
    std::shared_ptr<ArrayData> data = chunk->data()->Copy();
    data->type = ::arrow::utf8();
    chunk = std::make_shared<StringArray>(std::move(data));
  }
  return Status::OK();
}

Status ChunkedBinaryBuilder::Reserve(int64_t values) {
  if (ARROW_PREDICT_FALSE(extra_capacity_ != 0)) {
    extra_capacity_ += values;
    return Status::OK();
  }

  auto current_capacity = builder_->capacity();
  auto min_capacity = builder_->length() + values;
  if (current_capacity >= min_capacity) {
    return Status::OK();
  }

  auto new_capacity = BufferBuilder::GrowByFactor(current_capacity, min_capacity);
  if (ARROW_PREDICT_TRUE(new_capacity <= max_chunk_length_)) {
    return builder_->Resize(new_capacity);
  }

  extra_capacity_ = new_capacity - max_chunk_length_;
  return builder_->Resize(max_chunk_length_);
}

}  // namespace internal

}  // namespace arrow
