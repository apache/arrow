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

#include "arrow/array/array_binary.h"

#include <cstdint>
#include <memory>

#include "arrow/array/array_base.h"
#include "arrow/array/validate.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

void BinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_binary_like(data->type->id()));
  BaseBinaryArray<BinaryType>::SetData(data);
}

void LargeBinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_large_binary_like(data->type->id()));
  BaseBinaryArray<LargeBinaryType>::SetData(data);
}

void StringArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING);
  BinaryArray::SetData(data);
}

StringArray::StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(utf8(), length, {null_bitmap, value_offsets, data}, null_count,
                          offset));
}

Status StringArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

void LargeStringArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::LARGE_STRING);
  LargeBinaryArray::SetData(data);
}

LargeStringArray::LargeStringArray(int64_t length,
                                   const std::shared_ptr<Buffer>& value_offsets,
                                   const std::shared_ptr<Buffer>& data,
                                   const std::shared_ptr<Buffer>& null_bitmap,
                                   int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(large_utf8(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

Status LargeStringArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

void BinaryViewArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), expected_type_id());
  FlatArray::SetData(data);
  raw_values_ = data_->GetValuesSafe<c_type>(1);
}

namespace {
void InitViewArrayBuffers(BufferVector& buffers, std::shared_ptr<Buffer> views,
                          std::shared_ptr<Buffer> null_bitmap) {
  buffers.insert(buffers.begin(), std::move(views));
  buffers.insert(buffers.begin(), std::move(null_bitmap));
}
};  // namespace

BinaryViewArray::BinaryViewArray(std::shared_ptr<DataType> type, int64_t length,
                                 std::shared_ptr<Buffer> views, BufferVector buffers,
                                 std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                                 int64_t offset) {
  InitViewArrayBuffers(buffers, std::move(views), std::move(null_bitmap));
  Init(ArrayData::Make(std::move(type), length, std::move(buffers), null_count, offset),
       nullptr);
}

std::string_view BinaryViewArray::GetView(int64_t i) const {
  const std::shared_ptr<Buffer>* data_buffers = data_->buffers.data() + 2;
  return util::FromBinaryView(raw_values_[i], data_buffers);
}

StringViewArray::StringViewArray(std::shared_ptr<DataType> type, int64_t length,
                                 std::shared_ptr<Buffer> views, BufferVector buffers,
                                 std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                                 int64_t offset) {
  InitViewArrayBuffers(buffers, std::move(views), std::move(null_bitmap));
  Init(ArrayData::Make(std::move(type), length, std::move(buffers), null_count, offset),
       nullptr);
}

Status StringViewArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

void FixedSizeBinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  PrimitiveArray::SetData(data);
  byte_width_ = internal::checked_cast<const FixedSizeBinaryType&>(*type()).byte_width();
}

const uint8_t* FixedSizeBinaryArray::GetValue(int64_t i) const {
  return raw_values_ + (i + data_->offset) * byte_width_;
}

}  // namespace arrow
