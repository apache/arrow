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

#include "arrow/array.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <sstream>
#include <utility>

#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/extension_type.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visitor.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::BitmapAnd;
using internal::checked_cast;
using internal::CopyBitmap;
using internal::CountSetBits;

std::shared_ptr<ArrayData> ArrayData::Make(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           std::vector<std::shared_ptr<Buffer>>&& buffers,
                                           int64_t null_count, int64_t offset) {
  return std::make_shared<ArrayData>(type, length, std::move(buffers), null_count,
                                     offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(
    const std::shared_ptr<DataType>& type, int64_t length,
    const std::vector<std::shared_ptr<Buffer>>& buffers, int64_t null_count,
    int64_t offset) {
  return std::make_shared<ArrayData>(type, length, buffers, null_count, offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(
    const std::shared_ptr<DataType>& type, int64_t length,
    const std::vector<std::shared_ptr<Buffer>>& buffers,
    const std::vector<std::shared_ptr<ArrayData>>& child_data, int64_t null_count,
    int64_t offset) {
  return std::make_shared<ArrayData>(type, length, buffers, child_data, null_count,
                                     offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(const std::shared_ptr<DataType>& type,
                                           int64_t length, int64_t null_count,
                                           int64_t offset) {
  return std::make_shared<ArrayData>(type, length, null_count, offset);
}

ArrayData ArrayData::Slice(int64_t off, int64_t len) const {
  ARROW_CHECK_LE(off, length) << "Slice offset greater than array length";
  len = std::min(length - off, len);
  off += offset;

  auto copy = *this;
  copy.length = len;
  copy.offset = off;
  copy.null_count = null_count != 0 ? kUnknownNullCount : 0;
  return copy;
}

int64_t ArrayData::GetNullCount() const {
  if (ARROW_PREDICT_FALSE(this->null_count == kUnknownNullCount)) {
    if (this->buffers[0]) {
      this->null_count = this->length - CountSetBits(this->buffers[0]->data(),
                                                     this->offset, this->length);
    } else {
      this->null_count = 0;
    }
  }
  return this->null_count;
}

// ----------------------------------------------------------------------
// Base array class

int64_t Array::null_count() const { return data_->GetNullCount(); }

bool Array::Equals(const Array& arr, const EqualOptions& opts) const {
  return ArrayEquals(*this, arr, opts);
}

bool Array::Equals(const std::shared_ptr<Array>& arr, const EqualOptions& opts) const {
  if (!arr) {
    return false;
  }
  return Equals(*arr, opts);
}

bool Array::ApproxEquals(const Array& arr, const EqualOptions& opts) const {
  return ArrayApproxEquals(*this, arr, opts);
}

bool Array::ApproxEquals(const std::shared_ptr<Array>& arr,
                         const EqualOptions& opts) const {
  if (!arr) {
    return false;
  }
  return ApproxEquals(*arr, opts);
}

bool Array::RangeEquals(const Array& other, int64_t start_idx, int64_t end_idx,
                        int64_t other_start_idx) const {
  return ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx);
}

bool Array::RangeEquals(const std::shared_ptr<Array>& other, int64_t start_idx,
                        int64_t end_idx, int64_t other_start_idx) const {
  if (!other) {
    return false;
  }
  return ArrayRangeEquals(*this, *other, start_idx, end_idx, other_start_idx);
}

bool Array::RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                        const Array& other) const {
  return ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx);
}

bool Array::RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                        const std::shared_ptr<Array>& other) const {
  if (!other) {
    return false;
  }
  return ArrayRangeEquals(*this, *other, start_idx, end_idx, other_start_idx);
}

std::shared_ptr<Array> Array::Slice(int64_t offset, int64_t length) const {
  return MakeArray(std::make_shared<ArrayData>(data_->Slice(offset, length)));
}

std::shared_ptr<Array> Array::Slice(int64_t offset) const {
  int64_t slice_length = data_->length - offset;
  return Slice(offset, slice_length);
}

std::string Array::ToString() const {
  std::stringstream ss;
  ARROW_CHECK_OK(PrettyPrint(*this, 0, &ss));
  return ss.str();
}

NullArray::NullArray(int64_t length) {
  SetData(ArrayData::Make(null(), length, {nullptr}, length));
}

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
                               const std::shared_ptr<Buffer>& data,
                               const std::shared_ptr<Buffer>& null_bitmap,
                               int64_t null_count, int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, data}, null_count, offset));
}

// ----------------------------------------------------------------------
// BooleanArray

BooleanArray::BooleanArray(const std::shared_ptr<ArrayData>& data)
    : PrimitiveArray(data) {
  ARROW_CHECK_EQ(data->type->id(), Type::BOOL);
}

BooleanArray::BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
                           const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                           int64_t offset)
    : PrimitiveArray(boolean(), length, data, null_bitmap, null_count, offset) {}

// ----------------------------------------------------------------------
// ListArray

ListArray::ListArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

ListArray::ListArray(const std::shared_ptr<DataType>& type, int64_t length,
                     const std::shared_ptr<Buffer>& value_offsets,
                     const std::shared_ptr<Array>& values,
                     const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                     int64_t offset) {
  auto internal_data =
      ArrayData::Make(type, length, {null_bitmap, value_offsets}, null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  SetData(internal_data);
}

Status ListArray::FromArrays(const Array& offsets, const Array& values, MemoryPool* pool,
                             std::shared_ptr<Array>* out) {
  if (offsets.length() == 0) {
    return Status::Invalid("List offsets must have non-zero length");
  }

  if (offsets.type_id() != Type::INT32) {
    return Status::TypeError("List offsets must be signed int32");
  }

  BufferVector buffers = {};

  const auto& typed_offsets = checked_cast<const Int32Array&>(offsets);

  const int64_t num_offsets = offsets.length();

  if (offsets.null_count() > 0) {
    if (!offsets.IsValid(num_offsets - 1)) {
      return Status::Invalid("Last list offset should be non-null");
    }

    std::shared_ptr<Buffer> clean_offsets, clean_valid_bits;
    RETURN_NOT_OK(AllocateBuffer(pool, num_offsets * sizeof(int32_t), &clean_offsets));

    // Copy valid bits, zero out the bit for the final offset
    // XXX why?
    RETURN_NOT_OK(offsets.null_bitmap()->Copy(0, BitUtil::BytesForBits(num_offsets - 1),
                                              &clean_valid_bits));
    BitUtil::ClearBit(clean_valid_bits->mutable_data(), num_offsets);
    buffers.emplace_back(std::move(clean_valid_bits));

    const int32_t* raw_offsets = typed_offsets.raw_values();
    auto clean_raw_offsets = reinterpret_cast<int32_t*>(clean_offsets->mutable_data());

    // Must work backwards so we can tell how many values were in the last non-null value
    int32_t current_offset = raw_offsets[num_offsets - 1];
    for (int64_t i = num_offsets - 1; i >= 0; --i) {
      if (offsets.IsValid(i)) {
        current_offset = raw_offsets[i];
      }
      clean_raw_offsets[i] = current_offset;
    }

    buffers.emplace_back(std::move(clean_offsets));
  } else {
    buffers.emplace_back(offsets.null_bitmap());
    buffers.emplace_back(typed_offsets.values());
  }

  auto list_type = list(values.type());
  auto internal_data = ArrayData::Make(list_type, num_offsets - 1, std::move(buffers),
                                       offsets.null_count(), offsets.offset());
  internal_data->child_data.push_back(values.data());

  *out = std::make_shared<ListArray>(internal_data);
  return Status::OK();
}

void ListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  ARROW_CHECK_EQ(data->buffers.size(), 2);
  ARROW_CHECK(data->type->id() == Type::LIST);
  list_type_ = checked_cast<const ListType*>(data->type.get());

  auto value_offsets = data->buffers[1];
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());

  ARROW_CHECK_EQ(data_->child_data.size(), 1);
  ARROW_CHECK_EQ(list_type_->value_type()->id(), data->child_data[0]->type->id());
  DCHECK(list_type_->value_type()->Equals(data->child_data[0]->type));
  values_ = MakeArray(data_->child_data[0]);
}

std::shared_ptr<DataType> ListArray::value_type() const {
  return list_type()->value_type();
}

std::shared_ptr<Array> ListArray::values() const { return values_; }

// ----------------------------------------------------------------------
// MapArray

MapArray::MapArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

MapArray::MapArray(const std::shared_ptr<DataType>& type, int64_t length,
                   const std::shared_ptr<Buffer>& offsets,
                   const std::shared_ptr<Array>& values,
                   const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                   int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, offsets}, {values->data()},
                          null_count, offset));
}

MapArray::MapArray(const std::shared_ptr<DataType>& type, int64_t length,
                   const std::shared_ptr<Buffer>& offsets,
                   const std::shared_ptr<Array>& keys,
                   const std::shared_ptr<Array>& items,
                   const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                   int64_t offset) {
  auto pair_data = ArrayData::Make(type->children()[0]->type(), keys->data()->length,
                                   {nullptr}, {keys->data(), items->data()}, 0, offset);
  auto map_data = ArrayData::Make(type, length, {null_bitmap, offsets}, {pair_data},
                                  null_count, offset);
  SetData(map_data);
}

void MapArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::MAP);
  auto pair_data = data->child_data[0];
  ARROW_CHECK_EQ(pair_data->type->id(), Type::STRUCT);
  ARROW_CHECK_EQ(pair_data->null_count, 0);
  ARROW_CHECK_EQ(pair_data->child_data.size(), 2);
  ARROW_CHECK_EQ(pair_data->child_data[0]->null_count, 0);

  auto pair_list_data = data->Copy();
  pair_list_data->type = list(pair_data->type);
  this->ListArray::SetData(pair_list_data);
  data_->type = data->type;

  keys_ = MakeArray(pair_data->child_data[0]);
  items_ = MakeArray(pair_data->child_data[1]);
}

// ----------------------------------------------------------------------
// FixedSizeListArray

FixedSizeListArray::FixedSizeListArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

FixedSizeListArray::FixedSizeListArray(const std::shared_ptr<DataType>& type,
                                       int64_t length,
                                       const std::shared_ptr<Array>& values,
                                       const std::shared_ptr<Buffer>& null_bitmap,
                                       int64_t null_count, int64_t offset) {
  auto internal_data = ArrayData::Make(type, length, {null_bitmap}, null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  SetData(internal_data);
}

void FixedSizeListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::FIXED_SIZE_LIST);
  this->Array::SetData(data);

  ARROW_CHECK_EQ(list_type()->value_type()->id(), data->child_data[0]->type->id());
  DCHECK(list_type()->value_type()->Equals(data->child_data[0]->type));
  list_size_ = list_type()->list_size();

  ARROW_CHECK_EQ(data_->child_data.size(), 1);
  values_ = MakeArray(data_->child_data[0]);
}

const FixedSizeListType* FixedSizeListArray::list_type() const {
  return checked_cast<const FixedSizeListType*>(data_->type.get());
}

std::shared_ptr<DataType> FixedSizeListArray::value_type() const {
  return list_type()->value_type();
}

std::shared_ptr<Array> FixedSizeListArray::values() const { return values_; }

// ----------------------------------------------------------------------
// String and binary

BinaryArray::BinaryArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::BINARY);
  SetData(data);
}

void BinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->buffers.size(), 3);
  auto value_offsets = data->buffers[1];
  auto value_data = data->buffers[2];
  this->Array::SetData(data);
  raw_data_ = value_data == nullptr ? nullptr : value_data->data();
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());
}

BinaryArray::BinaryArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset)
    : BinaryArray(binary(), length, value_offsets, data, null_bitmap, null_count,
                  offset) {}

BinaryArray::BinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap, value_offsets, data}, null_count,
                          offset));
}

StringArray::StringArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRING);
  SetData(data);
}

StringArray::StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset)
    : BinaryArray(utf8(), length, value_offsets, data, null_bitmap, null_count, offset) {}

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset)
    : PrimitiveArray(type, length, data, null_bitmap, null_count, offset),
      byte_width_(checked_cast<const FixedSizeBinaryType&>(*type).byte_width()) {}

const uint8_t* FixedSizeBinaryArray::GetValue(int64_t i) const {
  return raw_values_ + (i + data_->offset) * byte_width_;
}

// ----------------------------------------------------------------------
// Day time interval

DayTimeIntervalArray::DayTimeIntervalArray(const std::shared_ptr<ArrayData>& data) {
  SetData(data);
}

DayTimeIntervalArray::DayTimeIntervalArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset)
    : PrimitiveArray(type, length, data, null_bitmap, null_count, offset) {}

DayTimeIntervalType::DayMilliseconds DayTimeIntervalArray::GetValue(int64_t i) const {
  DCHECK(i < length());
  return *reinterpret_cast<const DayTimeIntervalType::DayMilliseconds*>(
      raw_values_ + (i + data_->offset) * byte_width());
}

// ----------------------------------------------------------------------
// Decimal

Decimal128Array::Decimal128Array(const std::shared_ptr<ArrayData>& data)
    : FixedSizeBinaryArray(data) {
  ARROW_CHECK_EQ(data->type->id(), Type::DECIMAL);
}

std::string Decimal128Array::FormatValue(int64_t i) const {
  const auto& type_ = checked_cast<const Decimal128Type&>(*type());
  const Decimal128 value(GetValue(i));
  return value.ToString(type_.scale());
}

// ----------------------------------------------------------------------
// Struct

StructArray::StructArray(const std::shared_ptr<ArrayData>& data) {
  ARROW_CHECK_EQ(data->type->id(), Type::STRUCT);
  SetData(data);
  boxed_fields_.resize(data->child_data.size());
}

StructArray::StructArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::vector<std::shared_ptr<Array>>& children,
                         std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                         int64_t offset) {
  ARROW_CHECK_EQ(type->id(), Type::STRUCT);
  SetData(ArrayData::Make(type, length, {null_bitmap}, null_count, offset));
  for (const auto& child : children) {
    data_->child_data.push_back(child->data());
  }
  boxed_fields_.resize(children.size());
}

Result<std::shared_ptr<Array>> StructArray::Make(
    const std::vector<std::shared_ptr<Array>>& children,
    const std::vector<std::string>& field_names, std::shared_ptr<Buffer> null_bitmap,
    int64_t null_count, int64_t offset) {
  if (children.size() != field_names.size()) {
    return Status::Invalid("Mismatching number of field names and child arrays");
  }
  int64_t length = 0;
  if (children.size() == 0) {
    return Status::Invalid("Can't infer struct array length with 0 child arrays");
  }
  length = children.front()->length();
  for (const auto& child : children) {
    if (length != child->length()) {
      return Status::Invalid("Mismatching child array lengths");
    }
  }
  if (offset > length) {
    return Status::IndexError("Offset greater than length of child arrays");
  }
  std::vector<std::shared_ptr<Field>> fields(children.size());
  for (size_t i = 0; i < children.size(); ++i) {
    fields[i] = ::arrow::field(field_names[i], children[i]->type());
  }
  return std::make_shared<StructArray>(struct_(fields), length - offset, children,
                                       null_bitmap, null_count, offset);
}

const StructType* StructArray::struct_type() const {
  return checked_cast<const StructType*>(data_->type.get());
}

std::shared_ptr<Array> StructArray::field(int i) const {
  if (!boxed_fields_[i]) {
    std::shared_ptr<ArrayData> field_data;
    if (data_->offset != 0 || data_->child_data[i]->length != data_->length) {
      field_data = std::make_shared<ArrayData>(
          data_->child_data[i]->Slice(data_->offset, data_->length));
    } else {
      field_data = data_->child_data[i];
    }
    boxed_fields_[i] = MakeArray(field_data);
  }
  return boxed_fields_[i];
}

std::shared_ptr<Array> StructArray::GetFieldByName(const std::string& name) const {
  int i = struct_type()->GetFieldIndex(name);
  return i == -1 ? nullptr : field(i);
}

Status StructArray::Flatten(MemoryPool* pool, ArrayVector* out) const {
  ArrayVector flattened;
  std::shared_ptr<Buffer> null_bitmap = data_->buffers[0];

  for (auto& child_data : data_->child_data) {
    std::shared_ptr<Buffer> flattened_null_bitmap;
    int64_t flattened_null_count = kUnknownNullCount;

    // Need to adjust for parent offset
    if (data_->offset != 0 || data_->length != child_data->length) {
      *child_data = child_data->Slice(data_->offset, data_->length);
    }
    std::shared_ptr<Buffer> child_null_bitmap = child_data->buffers[0];
    const int64_t child_offset = child_data->offset;

    // The validity of a flattened datum is the logical AND of the struct
    // element's validity and the individual field element's validity.
    if (null_bitmap && child_null_bitmap) {
      RETURN_NOT_OK(BitmapAnd(pool, child_null_bitmap->data(), child_offset,
                              null_bitmap_data_, data_->offset, data_->length,
                              child_offset, &flattened_null_bitmap));
    } else if (child_null_bitmap) {
      flattened_null_bitmap = child_null_bitmap;
      flattened_null_count = child_data->null_count;
    } else if (null_bitmap) {
      if (child_offset == data_->offset) {
        flattened_null_bitmap = null_bitmap;
      } else {
        RETURN_NOT_OK(CopyBitmap(pool, null_bitmap_data_, data_->offset, data_->length,
                                 &flattened_null_bitmap));
      }
      flattened_null_count = data_->null_count;
    } else {
      flattened_null_count = 0;
    }

    auto flattened_data = child_data->Copy();
    flattened_data->buffers[0] = flattened_null_bitmap;
    flattened_data->null_count = flattened_null_count;

    flattened.push_back(MakeArray(flattened_data));
  }

  *out = flattened;
  return Status::OK();
}

// ----------------------------------------------------------------------
// UnionArray

void UnionArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);

  ARROW_CHECK_EQ(data->type->id(), Type::UNION);
  ARROW_CHECK_EQ(data->buffers.size(), 3);

  auto type_ids = data_->buffers[1];
  auto value_offsets = data_->buffers[2];
  raw_type_ids_ =
      type_ids == nullptr ? nullptr : reinterpret_cast<const uint8_t*>(type_ids->data());
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());
  boxed_fields_.resize(data->child_data.size());
}

UnionArray::UnionArray(const std::shared_ptr<ArrayData>& data) { SetData(data); }

UnionArray::UnionArray(const std::shared_ptr<DataType>& type, int64_t length,
                       const std::vector<std::shared_ptr<Array>>& children,
                       const std::shared_ptr<Buffer>& type_ids,
                       const std::shared_ptr<Buffer>& value_offsets,
                       const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                       int64_t offset) {
  auto internal_data = ArrayData::Make(
      type, length, {null_bitmap, type_ids, value_offsets}, null_count, offset);
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  SetData(internal_data);
}

Status UnionArray::MakeDense(const Array& type_ids, const Array& value_offsets,
                             const std::vector<std::shared_ptr<Array>>& children,
                             const std::vector<std::string>& field_names,
                             const std::vector<uint8_t>& type_codes,
                             std::shared_ptr<Array>* out) {
  if (value_offsets.length() == 0) {
    return Status::Invalid("UnionArray offsets must have non-zero length");
  }

  if (value_offsets.type_id() != Type::INT32) {
    return Status::TypeError("UnionArray offsets must be signed int32");
  }

  if (type_ids.type_id() != Type::INT8) {
    return Status::TypeError("UnionArray type_ids must be signed int8");
  }

  if (value_offsets.null_count() != 0) {
    return Status::Invalid("MakeDense does not allow NAs in value_offsets");
  }

  if (field_names.size() > 0 && field_names.size() != children.size()) {
    return Status::Invalid("field_names must have the same length as children");
  }

  if (type_codes.size() > 0 && type_codes.size() != children.size()) {
    return Status::Invalid("type_codes must have the same length as children");
  }

  BufferVector buffers = {type_ids.null_bitmap(),
                          checked_cast<const Int8Array&>(type_ids).values(),
                          checked_cast<const Int32Array&>(value_offsets).values()};

  std::shared_ptr<DataType> union_type =
      union_(children, field_names, type_codes, UnionMode::DENSE);
  auto internal_data = ArrayData::Make(union_type, type_ids.length(), std::move(buffers),
                                       type_ids.null_count(), type_ids.offset());
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  *out = std::make_shared<UnionArray>(internal_data);
  return Status::OK();
}

Status UnionArray::MakeSparse(const Array& type_ids,
                              const std::vector<std::shared_ptr<Array>>& children,
                              const std::vector<std::string>& field_names,
                              const std::vector<uint8_t>& type_codes,
                              std::shared_ptr<Array>* out) {
  if (type_ids.type_id() != Type::INT8) {
    return Status::TypeError("UnionArray type_ids must be signed int8");
  }

  if (field_names.size() > 0 && field_names.size() != children.size()) {
    return Status::Invalid("field_names must have the same length as children");
  }

  if (type_codes.size() > 0 && type_codes.size() != children.size()) {
    return Status::Invalid("type_codes must have the same length as children");
  }

  BufferVector buffers = {type_ids.null_bitmap(),
                          checked_cast<const Int8Array&>(type_ids).values(), nullptr};
  std::shared_ptr<DataType> union_type =
      union_(children, field_names, type_codes, UnionMode::SPARSE);
  auto internal_data = ArrayData::Make(union_type, type_ids.length(), std::move(buffers),
                                       type_ids.null_count(), type_ids.offset());
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
    if (child->length() != type_ids.length()) {
      return Status::Invalid(
          "Sparse UnionArray must have len(child) == len(type_ids) for all children");
    }
  }
  *out = std::make_shared<UnionArray>(internal_data);
  return Status::OK();
}

std::shared_ptr<Array> UnionArray::child(int i) const {
  if (!boxed_fields_[i]) {
    std::shared_ptr<ArrayData> child_data = data_->child_data[i]->Copy();
    if (mode() == UnionMode::SPARSE) {
      // Sparse union: need to adjust child if union is sliced
      // (for dense unions, the need to lookup through the offsets
      //  makes this unnecessary)
      if (data_->offset != 0 || child_data->length > data_->length) {
        *child_data = child_data->Slice(data_->offset, data_->length);
      }
    }
    boxed_fields_[i] = MakeArray(child_data);
  }
  return boxed_fields_[i];
}

const Array* UnionArray::UnsafeChild(int i) const {
  if (!boxed_fields_[i]) {
    boxed_fields_[i] = MakeArray(data_->child_data[i]);
  }
  return boxed_fields_[i].get();
}

// ----------------------------------------------------------------------
// DictionaryArray

/// \brief Perform validation check to determine if all dictionary indices
/// are within valid range (0 <= index < upper_bound)
///
/// \param[in] indices array of dictionary indices
/// \param[in] upper_bound upper bound of valid range for indices
/// \return Status
template <typename ArrowType>
Status ValidateDictionaryIndices(const std::shared_ptr<Array>& indices,
                                 const int64_t upper_bound) {
  using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
  const auto& array = checked_cast<const ArrayType&>(*indices);
  const typename ArrowType::c_type* data = array.raw_values();
  const int64_t size = array.length();

  if (array.null_count() == 0) {
    for (int64_t idx = 0; idx < size; ++idx) {
      if (data[idx] < 0 || data[idx] >= upper_bound) {
        return Status::Invalid("Dictionary has out-of-bound index [0, dict.length)");
      }
    }
  } else {
    for (int64_t idx = 0; idx < size; ++idx) {
      if (!array.IsNull(idx)) {
        if (data[idx] < 0 || data[idx] >= upper_bound) {
          return Status::Invalid("Dictionary has out-of-bound index [0, dict.length)");
        }
      }
    }
  }

  return Status::OK();
}

std::shared_ptr<Array> DictionaryArray::indices() const { return indices_; }

DictionaryArray::DictionaryArray(const std::shared_ptr<ArrayData>& data)
    : dict_type_(checked_cast<const DictionaryType*>(data->type.get())) {
  ARROW_CHECK_EQ(data->type->id(), Type::DICTIONARY);
  ARROW_CHECK_NE(data->dictionary, nullptr);
  SetData(data);
}

void DictionaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto indices_data = data_->Copy();
  indices_data->type = dict_type_->index_type();
  indices_ = MakeArray(indices_data);
}

DictionaryArray::DictionaryArray(const std::shared_ptr<DataType>& type,
                                 const std::shared_ptr<Array>& indices,
                                 const std::shared_ptr<Array>& dictionary)
    : dict_type_(checked_cast<const DictionaryType*>(type.get())) {
  ARROW_CHECK_EQ(type->id(), Type::DICTIONARY);
  ARROW_CHECK_EQ(indices->type_id(), dict_type_->index_type()->id());
  ARROW_CHECK_EQ(dict_type_->value_type()->id(), dictionary->type()->id());
  DCHECK(dict_type_->value_type()->Equals(*dictionary->type()));
  auto data = indices->data()->Copy();
  data->type = type;
  data->dictionary = dictionary;
  SetData(data);
}

std::shared_ptr<Array> DictionaryArray::dictionary() const { return data_->dictionary; }

Status DictionaryArray::FromArrays(const std::shared_ptr<DataType>& type,
                                   const std::shared_ptr<Array>& indices,
                                   const std::shared_ptr<Array>& dictionary,
                                   std::shared_ptr<Array>* out) {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected a dictionary type");
  }
  const auto& dict = checked_cast<const DictionaryType&>(*type);
  ARROW_CHECK_EQ(indices->type_id(), dict.index_type()->id());

  int64_t upper_bound = dictionary->length();
  Status is_valid;

  switch (indices->type_id()) {
    case Type::INT8:
      is_valid = ValidateDictionaryIndices<Int8Type>(indices, upper_bound);
      break;
    case Type::INT16:
      is_valid = ValidateDictionaryIndices<Int16Type>(indices, upper_bound);
      break;
    case Type::INT32:
      is_valid = ValidateDictionaryIndices<Int32Type>(indices, upper_bound);
      break;
    case Type::INT64:
      is_valid = ValidateDictionaryIndices<Int64Type>(indices, upper_bound);
      break;
    default:
      return Status::NotImplemented("Dictionary index type not supported: ",
                                    indices->type()->ToString());
  }
  RETURN_NOT_OK(is_valid);
  *out = std::make_shared<DictionaryArray>(type, indices, dictionary);
  return Status::OK();
}

template <typename InType, typename OutType>
static Status TransposeDictIndices(MemoryPool* pool, const ArrayData& in_data,
                                   const std::vector<int32_t>& transpose_map,
                                   const std::shared_ptr<ArrayData>& out_data,
                                   std::shared_ptr<Array>* out) {
  using in_c_type = typename InType::c_type;
  using out_c_type = typename OutType::c_type;
  internal::TransposeInts(in_data.GetValues<in_c_type>(1),
                          out_data->GetMutableValues<out_c_type>(1), in_data.length,
                          transpose_map.data());
  *out = MakeArray(out_data);
  return Status::OK();
}

Status DictionaryArray::Transpose(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                                  const std::shared_ptr<Array>& dictionary,
                                  const std::vector<int32_t>& transpose_map,
                                  std::shared_ptr<Array>* out) const {
  if (type->id() != Type::DICTIONARY) {
    return Status::TypeError("Expected dictionary type");
  }
  const auto& out_dict_type = checked_cast<const DictionaryType&>(*type);

  const auto& out_index_type =
      static_cast<const FixedWidthType&>(*out_dict_type.index_type());

  auto in_type_id = dict_type_->index_type()->id();
  auto out_type_id = out_index_type.id();

  std::shared_ptr<Buffer> out_buffer;
  RETURN_NOT_OK(AllocateBuffer(
      pool, data_->length * out_index_type.bit_width() * CHAR_BIT, &out_buffer));
  // Null bitmap is unchanged
  auto out_data = ArrayData::Make(type, data_->length, {data_->buffers[0], out_buffer},
                                  data_->null_count);
  out_data->dictionary = dictionary;

#define TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, OUT_INDEX_TYPE)    \
  case OUT_INDEX_TYPE::type_id:                                 \
    return TransposeDictIndices<IN_INDEX_TYPE, OUT_INDEX_TYPE>( \
        pool, *data_, transpose_map, out_data, out);

#define TRANSPOSE_IN_CASE(IN_INDEX_TYPE)                        \
  case IN_INDEX_TYPE::type_id:                                  \
    switch (out_type_id) {                                      \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int8Type)            \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int16Type)           \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int32Type)           \
      TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, Int64Type)           \
      default:                                                  \
        return Status::NotImplemented("unexpected index type"); \
    }

  switch (in_type_id) {
    TRANSPOSE_IN_CASE(Int8Type)
    TRANSPOSE_IN_CASE(Int16Type)
    TRANSPOSE_IN_CASE(Int32Type)
    TRANSPOSE_IN_CASE(Int64Type)
    default:
      return Status::NotImplemented("unexpected index type");
  }
#undef TRANSPOSE_IN_CASE
#undef TRANSPOSE_IN_OUT_CASE
}

// ----------------------------------------------------------------------
// Implement Array::View

namespace {

void AccumulateLayouts(const std::shared_ptr<DataType>& type,
                       std::vector<DataTypeLayout>* layouts) {
  layouts->push_back(type->layout());
  for (const auto& child : type->children()) {
    AccumulateLayouts(child->type(), layouts);
  }
}

void AccumulateArrayData(const std::shared_ptr<ArrayData>& data,
                         std::vector<std::shared_ptr<ArrayData>>* out) {
  out->push_back(data);
  for (const auto& child : data->child_data) {
    AccumulateArrayData(child, out);
  }
}

struct ViewDataImpl {
  std::shared_ptr<DataType> root_in_type;
  std::shared_ptr<DataType> root_out_type;
  std::vector<DataTypeLayout> in_layouts;
  std::vector<std::shared_ptr<ArrayData>> in_data;
  int64_t in_data_length;
  size_t in_layout_idx = 0;
  size_t in_buffer_idx = 0;
  bool input_exhausted = false;

  Status InvalidView(const std::string& msg) {
    return Status::Invalid("Can't view array of type ", root_in_type->ToString(), " as ",
                           root_out_type->ToString(), ": ", msg);
  }

  void AdjustInputPointer() {
    if (input_exhausted) {
      return;
    }
    while (true) {
      // Skip exhausted layout (might be empty layout)
      while (in_buffer_idx >= in_layouts[in_layout_idx].bit_widths.size()) {
        in_buffer_idx = 0;
        ++in_layout_idx;
        if (in_layout_idx >= in_layouts.size()) {
          input_exhausted = true;
          return;
        }
      }
      if (in_layouts[in_layout_idx].bit_widths[in_buffer_idx] > 0) {
        return;
      }
      // Skip always-null input buffers
      // (e.g. buffer 0 of a null type or buffer 2 of a sparse union)
      ++in_buffer_idx;
    }
  }

  Status CheckInputAvailable() {
    if (input_exhausted) {
      return InvalidView("not enough buffers for view type");
    }
    return Status::OK();
  }

  Status CheckInputExhausted() {
    if (!input_exhausted) {
      return InvalidView("too many buffers for view type");
    }
    return Status::OK();
  }

  Status CheckInputHasNoDictionaries() {
    for (const auto& layout : in_layouts) {
      if (layout.has_dictionary) {
        return InvalidView("input has dictionary");
      }
    }
    return Status::OK();
  }

  Status CheckInputAtZeroOffset() {
    for (const auto& data : in_data) {
      if (data->offset != 0) {
        return InvalidView("input has non-zero offset");
      }
    }
    return Status::OK();
  }

  Status MakeDataView(const std::shared_ptr<Field>& out_field,
                      std::shared_ptr<ArrayData>* out) {
    const auto out_type = out_field->type();
    const auto out_layout = out_type->layout();
    if (out_layout.has_dictionary) {
      return InvalidView("view type requires dictionary");
    }

    AdjustInputPointer();
    int64_t out_length = in_data_length;
    int64_t out_null_count;

    // No type has a purely empty layout
    DCHECK_GT(out_layout.bit_widths.size(), 0);

    if (out_layout.bit_widths[0] == 0) {
      // Assuming null type or equivalent.
      DCHECK_EQ(out_layout.bit_widths.size(), 1);
      *out = ArrayData::Make(out_type, out_length, {nullptr}, out_length);
      return Status::OK();
    }

    std::vector<std::shared_ptr<Buffer>> out_buffers;

    // Process null bitmap
    DCHECK_EQ(out_layout.bit_widths[0], 1);
    if (in_buffer_idx == 0) {
      // Copy input null bitmap
      RETURN_NOT_OK(CheckInputAvailable());
      const auto& in_data_item = in_data[in_layout_idx];
      if (!out_field->nullable() && in_data_item->GetNullCount() != 0) {
        return InvalidView("nulls in input cannot be viewed as non-nullable");
      }
      DCHECK_GT(in_data_item->buffers.size(), in_buffer_idx);
      out_buffers.push_back(in_data_item->buffers[in_buffer_idx]);
      out_length = in_data_item->length;
      out_null_count = in_data_item->null_count;
      ++in_buffer_idx;
      AdjustInputPointer();
    } else {
      // No null bitmap in input, append no-nulls bitmap
      out_buffers.push_back(nullptr);
      out_null_count = 0;
    }

    // Process other buffers in output layout
    for (size_t out_buffer_idx = 1; out_buffer_idx < out_layout.bit_widths.size();
         ++out_buffer_idx) {
      auto out_bit_width = out_layout.bit_widths[out_buffer_idx];
      // If always-null buffer is expected, just construct it
      if (out_bit_width == DataTypeLayout::kAlwaysNullBuffer) {
        out_buffers.push_back(nullptr);
        continue;
      }

      // If input buffer is null bitmap, try to ignore it
      while (in_buffer_idx == 0) {
        RETURN_NOT_OK(CheckInputAvailable());
        if (in_data[in_layout_idx]->GetNullCount() != 0) {
          return InvalidView("cannot represent nested nulls");
        }
        ++in_buffer_idx;
        AdjustInputPointer();
      }

      RETURN_NOT_OK(CheckInputAvailable());
      if (out_bit_width == DataTypeLayout::kVariableSizeBuffer ||
          out_bit_width != in_layouts[in_layout_idx].bit_widths[in_buffer_idx]) {
        return InvalidView("incompatible layouts");
      }
      // Copy input buffer
      const auto& in_data_item = in_data[in_layout_idx];
      out_length = in_data_item->length;
      DCHECK_GT(in_data_item->buffers.size(), in_buffer_idx);
      out_buffers.push_back(in_data_item->buffers[in_buffer_idx]);
      ++in_buffer_idx;
      AdjustInputPointer();
    }

    std::shared_ptr<ArrayData> out_data =
        ArrayData::Make(out_type, out_length, std::move(out_buffers), out_null_count);
    // Process children recursively, depth-first
    for (const auto& child_field : out_type->children()) {
      std::shared_ptr<ArrayData> child_data;
      RETURN_NOT_OK(MakeDataView(child_field, &child_data));
      out_data->child_data.push_back(std::move(child_data));
    }
    *out = std::move(out_data);
    return Status::OK();
  }
};

}  // namespace

Status Array::View(const std::shared_ptr<DataType>& out_type,
                   std::shared_ptr<Array>* out) {
  ViewDataImpl impl;
  impl.root_in_type = data_->type;
  impl.root_out_type = out_type;
  AccumulateLayouts(impl.root_in_type, &impl.in_layouts);
  AccumulateArrayData(data_, &impl.in_data);
  impl.in_data_length = data_->length;

  std::shared_ptr<ArrayData> out_data;
  RETURN_NOT_OK(impl.CheckInputHasNoDictionaries());
  RETURN_NOT_OK(impl.CheckInputAtZeroOffset());
  // Dummy field for output type
  auto out_field = field("", out_type);
  RETURN_NOT_OK(impl.MakeDataView(out_field, &out_data));
  RETURN_NOT_OK(impl.CheckInputExhausted());
  *out = MakeArray(out_data);
  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement Array::Accept as inline visitor

Status Array::Accept(ArrayVisitor* visitor) const {
  return VisitArrayInline(*this, visitor);
}

// ----------------------------------------------------------------------
// Implement Array::Validate as inline visitor

namespace internal {

struct ValidateVisitor {
  Status Visit(const NullArray& array) {
    ARROW_RETURN_IF(array.null_count() != array.length(),
                    Status::Invalid("null_count was invalid"));
    return Status::OK();
  }

  Status Visit(const PrimitiveArray& array) {
    ARROW_RETURN_IF(array.data()->buffers.size() != 2,
                    Status::Invalid("number of buffers was != 2"));

    ARROW_RETURN_IF(array.values() == nullptr, Status::Invalid("values was null"));

    return Status::OK();
  }

  Status Visit(const Decimal128Array& array) {
    if (array.data()->buffers.size() != 2) {
      return Status::Invalid("number of buffers was != 2");
    }
    if (array.values() == nullptr) {
      return Status::Invalid("values was null");
    }
    return Status::OK();
  }

  Status Visit(const BinaryArray& array) {
    if (array.data()->buffers.size() != 3) {
      return Status::Invalid("number of buffers was != 3");
    }
    return ValidateOffsets(array);
  }

  Status Visit(const ListArray& array) {
    if (array.length() < 0) {
      return Status::Invalid("Length was negative");
    }

    auto value_offsets = array.value_offsets();
    if (array.length() && !value_offsets) {
      return Status::Invalid("value_offsets_ was null");
    }
    if (value_offsets->size() / static_cast<int>(sizeof(int32_t)) < array.length()) {
      return Status::Invalid("offset buffer size (bytes): ", value_offsets->size(),
                             " isn't large enough for length: ", array.length());
    }

    if (!array.values()) {
      return Status::Invalid("values was null");
    }

    const int32_t last_offset = array.value_offset(array.length());
    if (array.values()->length() != last_offset) {
      return Status::Invalid("Final offset invariant not equal to values length: ",
                             last_offset, "!=", array.values()->length());
    }

    const Status child_valid = ValidateArray(*array.values());
    if (!child_valid.ok()) {
      return Status::Invalid("Child array invalid: ", child_valid.ToString());
    }

    return ValidateOffsets(array);
  }

  Status Visit(const MapArray& array) {
    if (array.length() < 0) {
      return Status::Invalid("Length was negative");
    }

    auto value_offsets = array.value_offsets();
    if (array.length() && !value_offsets) {
      return Status::Invalid("value_offsets_ was null");
    }
    if (value_offsets->size() / static_cast<int>(sizeof(int32_t)) < array.length()) {
      return Status::Invalid("offset buffer size (bytes): ", value_offsets->size(),
                             " isn't large enough for length: ", array.length());
    }

    if (!array.keys()) {
      return Status::Invalid("keys was null");
    }
    const Status key_valid = ValidateArray(*array.values());
    if (!key_valid.ok()) {
      return Status::Invalid("key array invalid: ", key_valid.ToString());
    }

    if (!array.values()) {
      return Status::Invalid("values was null");
    }
    const Status values_valid = ValidateArray(*array.values());
    if (!values_valid.ok()) {
      return Status::Invalid("values array invalid: ", values_valid.ToString());
    }

    const int32_t last_offset = array.value_offset(array.length());
    if (array.values()->length() != last_offset) {
      return Status::Invalid("Final offset invariant not equal to values length: ",
                             last_offset, "!=", array.values()->length());
    }
    if (array.keys()->length() != last_offset) {
      return Status::Invalid("Final offset invariant not equal to keys length: ",
                             last_offset, "!=", array.keys()->length());
    }

    return ValidateOffsets(array);
  }

  Status Visit(const FixedSizeListArray& array) {
    if (array.length() < 0) {
      return Status::Invalid("Length was negative");
    }
    if (!array.values()) {
      return Status::Invalid("values was null");
    }
    if (array.values()->length() != array.length() * array.value_length()) {
      return Status::Invalid(
          "Values Length (", array.values()->length(), ") was not equal to the length (",
          array.length(), ") multiplied by the list size (", array.value_length(), ")");
    }

    return Status::OK();
  }

  Status Visit(const StructArray& array) {
    if (array.length() < 0) {
      return Status::Invalid("Length was negative");
    }

    if (array.null_count() > array.length()) {
      return Status::Invalid("Null count exceeds the length of this struct");
    }

    if (array.num_fields() > 0) {
      // Validate fields
      int64_t array_length = array.field(0)->length();
      size_t idx = 0;
      for (int i = 0; i < array.num_fields(); ++i) {
        auto it = array.field(i);
        if (it->length() != array_length) {
          return Status::Invalid("Length is not equal from field ",
                                 it->type()->ToString(), " at position [", idx, "]");
        }

        const Status child_valid = ValidateArray(*it);
        if (!child_valid.ok()) {
          return Status::Invalid("Child array invalid: ", child_valid.ToString(),
                                 " at position [", idx, "}");
        }
        ++idx;
      }

      if (array_length > 0 && array_length != array.length()) {
        return Status::Invalid("Struct's length is not equal to its child arrays");
      }
    }
    return Status::OK();
  }

  Status Visit(const UnionArray& array) {
    if (array.length() < 0) {
      return Status::Invalid("Length was negative");
    }

    if (array.null_count() > array.length()) {
      return Status::Invalid("Null count exceeds the length of this struct");
    }
    return Status::OK();
  }

  Status Visit(const DictionaryArray& array) {
    Type::type index_type_id = array.indices()->type()->id();
    if (!is_integer(index_type_id)) {
      return Status::Invalid("Dictionary indices must be integer type");
    }
    if (!array.data()->dictionary) {
      return Status::Invalid("Dictionary values must be non-null");
    }
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) {
    const auto& ext_type = checked_cast<const ExtensionType&>(*array.type());

    if (!array.storage()->type()->Equals(*ext_type.storage_type())) {
      return Status::Invalid("Extension array of type '", array.type()->ToString(),
                             "' has storage array of incompatible type '",
                             array.storage()->type()->ToString(), "'");
    }
    return ValidateArray(*array.storage());
  }

 protected:
  template <typename ArrayType>
  Status ValidateOffsets(ArrayType& array) {
    int32_t prev_offset = array.value_offset(0);
    if (array.offset() == 0 && prev_offset != 0) {
      return Status::Invalid("The first offset wasn't zero");
    }
    for (int64_t i = 1; i <= array.length(); ++i) {
      int32_t current_offset = array.value_offset(i);
      if (array.IsNull(i - 1) && current_offset != prev_offset) {
        return Status::Invalid("Offset invariant failure at: ", i,
                               " inconsistent value_offsets for null slot",
                               current_offset, "!=", prev_offset);
      }
      if (current_offset < prev_offset) {
        return Status::Invalid("Offset invariant failure: ", i,
                               " inconsistent offset for non-null slot: ", current_offset,
                               "<", prev_offset);
      }
      prev_offset = current_offset;
    }
    return Status::OK();
  }
};

}  // namespace internal

Status ValidateArray(const Array& array) {
  // First check the array layout conforms to the spec
  const DataType& type = *array.type();
  const auto layout = type.layout();
  const ArrayData& data = *array.data();

  if (data.buffers.size() != layout.bit_widths.size()) {
    return Status::Invalid("Expected ", layout.bit_widths.size(),
                           " buffers in array "
                           "of type ",
                           type.ToString(), ", got ", data.buffers.size());
  }
  if (data.child_data.size() != static_cast<size_t>(type.num_children())) {
    return Status::Invalid("Expected ", type.num_children(),
                           " child arrays in array "
                           "of type ",
                           type.ToString(), ", got ", data.child_data.size());
  }
  if (layout.has_dictionary && !data.dictionary) {
    return Status::Invalid("Array of type ", type.ToString(),
                           " must have dictionary values");
  }
  if (!layout.has_dictionary && data.dictionary) {
    return Status::Invalid("Unexpected dictionary values in array of type ",
                           type.ToString());
  }

  internal::ValidateVisitor validate_visitor;
  return VisitArrayInline(array, &validate_visitor);
}

// ----------------------------------------------------------------------
// Loading from ArrayData

namespace internal {

class ArrayDataWrapper {
 public:
  ArrayDataWrapper(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out)
      : data_(data), out_(out) {}

  template <typename T>
  Status Visit(const T&) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    *out_ = std::make_shared<ArrayType>(data_);
    return Status::OK();
  }

  Status Visit(const ExtensionType& type) {
    *out_ = type.MakeArray(data_);
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<Array>* out_;
};

}  // namespace internal

std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data) {
  std::shared_ptr<Array> out;
  internal::ArrayDataWrapper wrapper_visitor(data, &out);
  DCHECK_OK(VisitTypeInline(*data->type, &wrapper_visitor));
  DCHECK(out);
  return out;
}

// ----------------------------------------------------------------------
// Misc APIs

namespace internal {

// get the maximum buffer length required, then allocate a single zeroed buffer
// to use anywhere a buffer is required
class NullArrayFactory {
 public:
  struct GetBufferLength {
    GetBufferLength(const std::shared_ptr<DataType>& type, int64_t length)
        : type_(*type), length_(length), buffer_length_(BitUtil::BytesForBits(length)) {}

    operator int64_t() && {
      // TODO this should implement proper error propagation
      ARROW_CHECK_OK(VisitTypeInline(type_, this));
      return buffer_length_;
    }

    template <typename T, typename = decltype(TypeTraits<T>::bytes_required(0))>
    Status Visit(const T&) {
      return MaxOf(TypeTraits<T>::bytes_required(length_));
    }

    Status Visit(const ListType& type) {
      // list's values array may be empty, but there must be at least one offset of 0
      return MaxOf(sizeof(int32_t));
    }

    Status Visit(const FixedSizeListType& type) {
      return MaxOf(GetBufferLength(type.value_type(), type.list_size() * length_));
    }

    Status Visit(const StructType& type) {
      for (const auto& child : type.children()) {
        DCHECK_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const UnionType& type) {
      // type codes
      DCHECK_OK(MaxOf(length_));
      if (type.mode() == UnionMode::DENSE) {
        // offsets
        DCHECK_OK(MaxOf(sizeof(int32_t) * length_));
      }
      for (const auto& child : type.children()) {
        DCHECK_OK(MaxOf(GetBufferLength(child->type(), length_)));
      }
      return Status::OK();
    }

    Status Visit(const DictionaryType& type) {
      DCHECK_OK(MaxOf(GetBufferLength(type.value_type(), length_)));
      return MaxOf(GetBufferLength(type.index_type(), length_));
    }

    Status Visit(const ExtensionType& type) {
      // XXX is an extension array's length always == storage length
      return MaxOf(GetBufferLength(type.storage_type(), length_));
    }

    Status Visit(const DataType& type) {
      return Status::NotImplemented("construction of all-null ", type);
    }

   private:
    Status MaxOf(int64_t buffer_length) {
      if (buffer_length > buffer_length_) {
        buffer_length_ = buffer_length;
      }
      return Status::OK();
    }

    const DataType& type_;
    int64_t length_, buffer_length_;
  };

  NullArrayFactory(const std::shared_ptr<DataType>& type, int64_t length,
                   std::shared_ptr<ArrayData>* out)
      : type_(type), length_(length), out_(out) {}

  Status CreateBuffer() {
    int64_t buffer_length = GetBufferLength(type_, length_);
    RETURN_NOT_OK(AllocateBuffer(buffer_length, &buffer_));
    std::memset(buffer_->mutable_data(), 0, buffer_->size());
    return Status::OK();
  }

  Status Create() {
    if (buffer_ == nullptr) {
      RETURN_NOT_OK(CreateBuffer());
    }
    std::vector<std::shared_ptr<ArrayData>> child_data(type_->num_children());
    *out_ = ArrayData::Make(type_, length_, {buffer_}, child_data, length_, 0);
    return VisitTypeInline(*type_, this);
  }

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const FixedWidthType&) {
    (*out_)->buffers.resize(2, buffer_);
    return Status::OK();
  }

  Status Visit(const BinaryType&) {
    (*out_)->buffers.resize(3, buffer_);
    return Status::OK();
  }

  Status Visit(const ListType& type) {
    (*out_)->buffers.resize(2, buffer_);
    return CreateChild(0, length_, &(*out_)->child_data[0]);
  }

  Status Visit(const FixedSizeListType& type) {
    return CreateChild(0, length_ * type.list_size(), &(*out_)->child_data[0]);
  }

  Status Visit(const StructType& type) {
    for (int i = 0; i < type_->num_children(); ++i) {
      RETURN_NOT_OK(CreateChild(i, length_, &(*out_)->child_data[i]));
    }
    return Status::OK();
  }

  Status Visit(const UnionType& type) {
    if (type.mode() == UnionMode::DENSE) {
      (*out_)->buffers.resize(3, buffer_);
    } else {
      (*out_)->buffers.resize(2, buffer_);
    }

    for (int i = 0; i < type_->num_children(); ++i) {
      RETURN_NOT_OK(CreateChild(i, length_, &(*out_)->child_data[i]));
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& type) {
    (*out_)->buffers.resize(2, buffer_);
    std::shared_ptr<ArrayData> dictionary_data;
    return MakeArrayOfNull(type.value_type(), 0, &(*out_)->dictionary);
  }

  Status Visit(const DataType& type) {
    return Status::NotImplemented("construction of all-null ", type);
  }

  Status CreateChild(int i, int64_t length, std::shared_ptr<ArrayData>* out) {
    NullArrayFactory child_factory(type_->child(i)->type(), length,
                                   &(*out_)->child_data[i]);
    child_factory.buffer_ = buffer_;
    return child_factory.Create();
  }

  std::shared_ptr<DataType> type_;
  int64_t length_;
  std::shared_ptr<ArrayData>* out_;
  std::shared_ptr<Buffer> buffer_;
};

}  // namespace internal

Status MakeArrayOfNull(const std::shared_ptr<DataType>& type, int64_t length,
                       std::shared_ptr<Array>* out) {
  std::shared_ptr<ArrayData> out_data;
  RETURN_NOT_OK(internal::NullArrayFactory(type, length, &out_data).Create());
  *out = MakeArray(out_data);
  return Status::OK();
}

namespace internal {

std::vector<ArrayVector> RechunkArraysConsistently(
    const std::vector<ArrayVector>& groups) {
  if (groups.size() <= 1) {
    return groups;
  }
  int64_t total_length = 0;
  for (const auto& array : groups.front()) {
    total_length += array->length();
  }
#ifndef NDEBUG
  for (const auto& group : groups) {
    int64_t group_length = 0;
    for (const auto& array : group) {
      group_length += array->length();
    }
    DCHECK_EQ(group_length, total_length)
        << "Array groups should have the same total number of elements";
  }
#endif
  if (total_length == 0) {
    return groups;
  }

  // Set up result vectors
  std::vector<ArrayVector> rechunked_groups(groups.size());

  // Set up progress counters
  std::vector<ArrayVector::const_iterator> current_arrays;
  std::vector<int64_t> array_offsets;
  for (const auto& group : groups) {
    current_arrays.emplace_back(group.cbegin());
    array_offsets.emplace_back(0);
  }

  // Scan all array vectors at once, rechunking along the way
  int64_t start = 0;
  while (start < total_length) {
    // First compute max possible length for next chunk
    int64_t chunk_length = std::numeric_limits<int64_t>::max();
    for (size_t i = 0; i < groups.size(); i++) {
      auto& arr_it = current_arrays[i];
      auto& offset = array_offsets[i];
      // Skip any done arrays (including 0-length arrays)
      while (offset == (*arr_it)->length()) {
        ++arr_it;
        offset = 0;
      }
      const auto& array = *arr_it;
      DCHECK_GT(array->length(), offset);
      chunk_length = std::min(chunk_length, array->length() - offset);
    }
    DCHECK_GT(chunk_length, 0);

    // Then slice all arrays along this chunk size
    for (size_t i = 0; i < groups.size(); i++) {
      const auto& array = *current_arrays[i];
      auto& offset = array_offsets[i];
      if (offset == 0 && array->length() == chunk_length) {
        // Slice spans entire array
        rechunked_groups[i].emplace_back(array);
      } else {
        DCHECK_LT(chunk_length - offset, array->length());
        rechunked_groups[i].emplace_back(array->Slice(offset, chunk_length));
      }
      offset += chunk_length;
    }
    start += chunk_length;
  }

  return rechunked_groups;
}

}  // namespace internal
}  // namespace arrow
