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

#include "arrow/array/builder_decimal.h"

#include <cstdint>
#include <memory>

#include "arrow/array/data.h"
#include "arrow/buffer.h"
#include "arrow/buffer_builder.h"
#include "arrow/status.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"

namespace arrow {

class Buffer;
class MemoryPool;

// ----------------------------------------------------------------------
// Decimal32Builder

Decimal32Builder::Decimal32Builder(const std::shared_ptr<DataType>& type,
                                   MemoryPool* pool, int64_t alignment)
    : FixedSizeBinaryBuilder(type, pool, alignment),
      decimal_type_(internal::checked_pointer_cast<Decimal32Type>(type)) {}

Status Decimal32Builder::Append(Decimal32 value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  UnsafeAppend(value);
  return Status::OK();
}

void Decimal32Builder::UnsafeAppend(Decimal32 value) {
  value.ToBytes(GetMutableValue(length()));
  byte_builder_.UnsafeAdvance(4);
  UnsafeAppendToBitmap(true);
}

void Decimal32Builder::UnsafeAppend(std::string_view value) {
  FixedSizeBinaryBuilder::UnsafeAppend(value);
}

Status Decimal32Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Decimal64Builder

Decimal64Builder::Decimal64Builder(const std::shared_ptr<DataType>& type,
                                   MemoryPool* pool, int64_t alignment)
    : FixedSizeBinaryBuilder(type, pool, alignment),
      decimal_type_(internal::checked_pointer_cast<Decimal64Type>(type)) {}

Status Decimal64Builder::Append(Decimal64 value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  UnsafeAppend(value);
  return Status::OK();
}

void Decimal64Builder::UnsafeAppend(Decimal64 value) {
  value.ToBytes(GetMutableValue(length()));
  byte_builder_.UnsafeAdvance(8);
  UnsafeAppendToBitmap(true);
}

void Decimal64Builder::UnsafeAppend(std::string_view value) {
  FixedSizeBinaryBuilder::UnsafeAppend(value);
}

Status Decimal64Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Decimal128Builder

Decimal128Builder::Decimal128Builder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool, int64_t alignment)
    : FixedSizeBinaryBuilder(type, pool, alignment),
      decimal_type_(internal::checked_pointer_cast<Decimal128Type>(type)) {}

Status Decimal128Builder::Append(Decimal128 value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  UnsafeAppend(value);
  return Status::OK();
}

void Decimal128Builder::UnsafeAppend(Decimal128 value) {
  value.ToBytes(GetMutableValue(length()));
  byte_builder_.UnsafeAdvance(16);
  UnsafeAppendToBitmap(true);
}

void Decimal128Builder::UnsafeAppend(std::string_view value) {
  FixedSizeBinaryBuilder::UnsafeAppend(value);
}

Status Decimal128Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Decimal256Builder

Decimal256Builder::Decimal256Builder(const std::shared_ptr<DataType>& type,
                                     MemoryPool* pool, int64_t alignment)
    : FixedSizeBinaryBuilder(type, pool, alignment),
      decimal_type_(internal::checked_pointer_cast<Decimal256Type>(type)) {}

Status Decimal256Builder::Append(const Decimal256& value) {
  RETURN_NOT_OK(FixedSizeBinaryBuilder::Reserve(1));
  UnsafeAppend(value);
  return Status::OK();
}

void Decimal256Builder::UnsafeAppend(const Decimal256& value) {
  value.ToBytes(GetMutableValue(length()));
  byte_builder_.UnsafeAdvance(32);
  UnsafeAppendToBitmap(true);
}

void Decimal256Builder::UnsafeAppend(std::string_view value) {
  FixedSizeBinaryBuilder::UnsafeAppend(value);
}

Status Decimal256Builder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  std::shared_ptr<Buffer> data;
  RETURN_NOT_OK(byte_builder_.Finish(&data));
  std::shared_ptr<Buffer> null_bitmap;
  RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));

  *out = ArrayData::Make(type(), length_, {null_bitmap, data}, null_count_);
  capacity_ = length_ = null_count_ = 0;
  return Status::OK();
}

}  // namespace arrow
