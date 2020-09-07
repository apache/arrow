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

namespace arrow {

using internal::checked_cast;

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryBuilder::FixedSizeBinaryBuilder(const std::shared_ptr<DataType>& type,
                                               MemoryPool* pool)
    : ArrayBuilder(pool),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()),
      byte_builder_(pool) {}

void FixedSizeBinaryBuilder::CheckValueSize(int64_t size) {
  DCHECK_EQ(size, byte_width_) << "Appending wrong size to FixedSizeBinaryBuilder";
}

Status FixedSizeBinaryBuilder::AppendValues(const uint8_t* data, int64_t length,
                                            const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeAppendToBitmap(valid_bytes, length);
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

util::string_view FixedSizeBinaryBuilder::GetView(int64_t i) const {
  const uint8_t* data_ptr = byte_builder_.data();
  return util::string_view(reinterpret_cast<const char*>(data_ptr + i * byte_width_),
                           byte_width_);
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
  for (size_t i = 0; i < out->size(); ++i) {
    std::shared_ptr<ArrayData> data = (*out)[i]->data();
    data->type = ::arrow::utf8();
    (*out)[i] = std::make_shared<StringArray>(data);
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
