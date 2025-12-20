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
#include "arrow/array/data.h"
#include "arrow/array/validate.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/binary_view_util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::checked_cast;

template <typename TYPE>
std::shared_ptr<Buffer> BaseBinaryArray<TYPE>::value_offsets() const {
  return data_->buffers[1];
}

template <typename TYPE>
std::shared_ptr<Buffer> BaseBinaryArray<TYPE>::value_data() const {
  return data_->buffers[2];
}

template <typename TYPE>
typename BaseBinaryArray<TYPE>::offset_type BaseBinaryArray<TYPE>::total_values_length()
    const {
  if (data_->length > 0) {
    return raw_value_offsets_[data_->length] - raw_value_offsets_[0];
  } else {
    return 0;
  }
}

template <typename TYPE>
void BaseBinaryArray<TYPE>::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  raw_value_offsets_ = data->GetValuesSafe<offset_type>(1);
  raw_data_ = data->GetValuesSafe<uint8_t>(2, /*offset=*/0);
}

BinaryArray::BinaryArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_binary_like(data->type->id()));
  SetData(data);
}

BinaryArray::BinaryArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(binary(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

LargeBinaryArray::LargeBinaryArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK(is_large_binary_like(data->type->id()));
  SetData(data);
}

LargeBinaryArray::LargeBinaryArray(int64_t length,
                                   const std::shared_ptr<Buffer>& value_offsets,
                                   const std::shared_ptr<Buffer>& data,
                                   const std::shared_ptr<Buffer>& null_bitmap,
                                   int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(large_binary(), length, {null_bitmap, value_offsets, data},
                          null_count, offset));
}

StringArray::StringArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING);
  SetData(data);
}

StringArray::StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(utf8(), length, {null_bitmap, value_offsets, data}, null_count,
                          offset));
}

Status StringArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

LargeStringArray::LargeStringArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::LARGE_STRING);
  SetData(data);
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

BinaryViewArray::BinaryViewArray(std::shared_ptr<ArrayData> data) {
  ARROW_CHECK_EQ(data->type->id(), Type::BINARY_VIEW);
  SetData(std::move(data));
}

BinaryViewArray::BinaryViewArray(std::shared_ptr<DataType> type, int64_t length,
                                 std::shared_ptr<Buffer> views, BufferVector buffers,
                                 std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                                 int64_t offset) {
  buffers.insert(buffers.begin(), std::move(views));
  buffers.insert(buffers.begin(), std::move(null_bitmap));
  SetData(
      ArrayData::Make(std::move(type), length, std::move(buffers), null_count, offset));
}

std::string_view BinaryViewArray::GetView(int64_t i) const {
  const std::shared_ptr<Buffer>* data_buffers = data_->buffers.data() + 2;
  return util::FromBinaryView(raw_values_[i], data_buffers);
}

const std::shared_ptr<Buffer>& BinaryViewArray::values() const {
  return data_->buffers[1];
}

StringViewArray::StringViewArray(std::shared_ptr<ArrayData> data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING_VIEW);
  SetData(std::move(data));
}

void BinaryViewArray::SetData(std::shared_ptr<ArrayData> data) {
  FlatArray::SetData(std::move(data));
  raw_values_ = data_->GetValuesSafe<c_type>(1);
}

Status StringViewArray::ValidateUTF8() const { return internal::ValidateUTF8(*data_); }

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, data}, null_count, offset));
}

void FixedSizeBinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->PrimitiveArray::SetData(data);
  byte_width_ = internal::checked_cast<const FixedSizeBinaryType&>(*type()).byte_width();
  values_ = raw_values_ + data_->offset * byte_width_;
}

template class BaseBinaryArray<BinaryType>;
template class BaseBinaryArray<LargeBinaryType>;

}  // namespace arrow
