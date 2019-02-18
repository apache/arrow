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

bool Array::Equals(const Array& arr) const { return ArrayEquals(*this, arr); }

bool Array::Equals(const std::shared_ptr<Array>& arr) const {
  if (!arr) {
    return false;
  }
  return Equals(*arr);
}

bool Array::ApproxEquals(const Array& arr) const { return ArrayApproxEquals(*this, arr); }

bool Array::ApproxEquals(const std::shared_ptr<Array>& arr) const {
  if (!arr) {
    return false;
  }
  return ApproxEquals(*arr);
}

bool Array::RangeEquals(int64_t start_idx, int64_t end_idx, int64_t other_start_idx,
                        const std::shared_ptr<Array>& other) const {
  if (!other) {
    return false;
  }
  return RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

bool Array::RangeEquals(const Array& other, int64_t start_idx, int64_t end_idx,
                        int64_t other_start_idx) const {
  return ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx);
}

static inline std::shared_ptr<ArrayData> SliceData(const ArrayData& data, int64_t offset,
                                                   int64_t length) {
  DCHECK_LE(offset, data.length);
  length = std::min(data.length - offset, length);
  offset += data.offset;

  auto new_data = data.Copy();
  new_data->length = length;
  new_data->offset = offset;
  new_data->null_count = data.null_count != 0 ? kUnknownNullCount : 0;
  return new_data;
}

std::shared_ptr<Array> Array::Slice(int64_t offset, int64_t length) const {
  return MakeArray(SliceData(*data_, offset, length));
}

std::shared_ptr<Array> Array::Slice(int64_t offset) const {
  int64_t slice_length = data_->length - offset;
  return Slice(offset, slice_length);
}

std::string Array::ToString() const {
  std::stringstream ss;
  Status s = PrettyPrint(*this, 0, &ss);
  DCHECK_OK(s);
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
  DCHECK_EQ(data->type->id(), Type::BOOL);
}

BooleanArray::BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
                           const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                           int64_t offset)
    : PrimitiveArray(boolean(), length, data, null_bitmap, null_count, offset) {}

// ----------------------------------------------------------------------
// ListArray

ListArray::ListArray(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::LIST);
  SetData(data);
}

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
    return Status::Invalid("List offsets must be signed int32");
  }

  BufferVector buffers = {};

  const auto& typed_offsets = checked_cast<const Int32Array&>(offsets);

  const int64_t num_offsets = offsets.length();

  if (offsets.null_count() > 0) {
    std::shared_ptr<Buffer> clean_offsets, clean_valid_bits;

    RETURN_NOT_OK(AllocateBuffer(pool, num_offsets * sizeof(int32_t), &clean_offsets));

    // Copy valid bits, zero out the bit for the final offset
    RETURN_NOT_OK(offsets.null_bitmap()->Copy(0, BitUtil::BytesForBits(num_offsets - 1),
                                              &clean_valid_bits));
    BitUtil::ClearBit(clean_valid_bits->mutable_data(), num_offsets);
    buffers.emplace_back(std::move(clean_valid_bits));

    const int32_t* raw_offsets = typed_offsets.raw_values();
    auto clean_raw_offsets = reinterpret_cast<int32_t*>(clean_offsets->mutable_data());

    // Must work backwards so we can tell how many values were in the last non-null value
    DCHECK(offsets.IsValid(num_offsets - 1));
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
  DCHECK_EQ(data->buffers.size(), 2);

  auto value_offsets = data->buffers[1];
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());

  DCHECK_EQ(data_->child_data.size(), 1);
  values_ = MakeArray(data_->child_data[0]);
}

const ListType* ListArray::list_type() const {
  return checked_cast<const ListType*>(data_->type.get());
}

std::shared_ptr<DataType> ListArray::value_type() const {
  return list_type()->value_type();
}

std::shared_ptr<Array> ListArray::values() const { return values_; }

// ----------------------------------------------------------------------
// String and binary

BinaryArray::BinaryArray(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::BINARY);
  SetData(data);
}

void BinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->buffers.size(), 3);
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
  DCHECK_EQ(data->type->id(), Type::STRING);
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
// Decimal

Decimal128Array::Decimal128Array(const std::shared_ptr<ArrayData>& data)
    : FixedSizeBinaryArray(data) {
  DCHECK_EQ(data->type->id(), Type::DECIMAL);
}

std::string Decimal128Array::FormatValue(int64_t i) const {
  const auto& type_ = checked_cast<const Decimal128Type&>(*type());
  const Decimal128 value(GetValue(i));
  return value.ToString(type_.scale());
}

// ----------------------------------------------------------------------
// Struct

StructArray::StructArray(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::STRUCT);
  SetData(data);
  boxed_fields_.resize(data->child_data.size());
}

StructArray::StructArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::vector<std::shared_ptr<Array>>& children,
                         std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                         int64_t offset) {
  SetData(ArrayData::Make(type, length, {null_bitmap}, null_count, offset));
  for (const auto& child : children) {
    data_->child_data.push_back(child->data());
  }
  boxed_fields_.resize(children.size());
}

const StructType* StructArray::struct_type() const {
  return checked_cast<const StructType*>(data_->type.get());
}

std::shared_ptr<Array> StructArray::field(int i) const {
  if (!boxed_fields_[i]) {
    std::shared_ptr<ArrayData> field_data;
    if (data_->offset != 0 || data_->child_data[i]->length != data_->length) {
      field_data = SliceData(*data_->child_data[i].get(), data_->offset, data_->length);
    } else {
      field_data = data_->child_data[i];
    }
    boxed_fields_[i] = MakeArray(field_data);
  }
  DCHECK(boxed_fields_[i]);
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
      child_data = SliceData(*child_data, data_->offset, data_->length);
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

  DCHECK_EQ(data->buffers.size(), 3);

  auto type_ids = data_->buffers[1];
  auto value_offsets = data_->buffers[2];
  raw_type_ids_ =
      type_ids == nullptr ? nullptr : reinterpret_cast<const uint8_t*>(type_ids->data());
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());
  boxed_fields_.resize(data->child_data.size());
}

UnionArray::UnionArray(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::UNION);
  SetData(data);
}

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
                             std::shared_ptr<Array>* out) {
  if (value_offsets.length() == 0) {
    return Status::Invalid("UnionArray offsets must have non-zero length");
  }

  if (value_offsets.type_id() != Type::INT32) {
    return Status::Invalid("UnionArray offsets must be signed int32");
  }

  if (type_ids.type_id() != Type::INT8) {
    return Status::Invalid("UnionArray type_ids must be signed int8");
  }

  if (value_offsets.null_count() != 0) {
    return Status::Invalid("MakeDense does not allow NAs in value_offsets");
  }

  BufferVector buffers = {type_ids.null_bitmap(),
                          checked_cast<const Int8Array&>(type_ids).values(),
                          checked_cast<const Int32Array&>(value_offsets).values()};
  auto union_type = union_(children, UnionMode::DENSE);
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
                              std::shared_ptr<Array>* out) {
  if (type_ids.type_id() != Type::INT8) {
    return Status::Invalid("UnionArray type_ids must be signed int8");
  }
  BufferVector buffers = {type_ids.null_bitmap(),
                          checked_cast<const Int8Array&>(type_ids).values(), nullptr};
  auto union_type = union_(children, UnionMode::SPARSE);
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
    std::shared_ptr<ArrayData> child_data = data_->child_data[i];
    if (mode() == UnionMode::SPARSE) {
      // Sparse union: need to adjust child if union is sliced
      // (for dense unions, the need to lookup through the offsets
      //  makes this unnecessary)
      if (data_->offset != 0 || child_data->length > data_->length) {
        child_data = SliceData(*child_data.get(), data_->offset, data_->length);
      }
    }
    boxed_fields_[i] = MakeArray(child_data);
  }
  DCHECK(boxed_fields_[i]);
  return boxed_fields_[i];
}

const Array* UnionArray::UnsafeChild(int i) const {
  if (!boxed_fields_[i]) {
    boxed_fields_[i] = MakeArray(data_->child_data[i]);
  }
  DCHECK(boxed_fields_[i]);
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

DictionaryArray::DictionaryArray(const std::shared_ptr<ArrayData>& data)
    : dict_type_(checked_cast<const DictionaryType*>(data->type.get())) {
  DCHECK_EQ(data->type->id(), Type::DICTIONARY);
  SetData(data);
}

DictionaryArray::DictionaryArray(const std::shared_ptr<DataType>& type,
                                 const std::shared_ptr<Array>& indices)
    : dict_type_(checked_cast<const DictionaryType*>(type.get())) {
  DCHECK_EQ(type->id(), Type::DICTIONARY);
  DCHECK_EQ(indices->type_id(), dict_type_->index_type()->id());
  auto data = indices->data()->Copy();
  data->type = type;
  SetData(data);
}

Status DictionaryArray::FromArrays(const std::shared_ptr<DataType>& type,
                                   const std::shared_ptr<Array>& indices,
                                   std::shared_ptr<Array>* out) {
  DCHECK_EQ(type->id(), Type::DICTIONARY);
  const auto& dict = checked_cast<const DictionaryType&>(*type);
  DCHECK_EQ(indices->type_id(), dict.index_type()->id());

  int64_t upper_bound = dict.dictionary()->length();
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
      return Status::NotImplemented("Categorical index type not supported: ",
                                    indices->type()->ToString());
  }

  if (!is_valid.ok()) {
    return is_valid;
  }

  *out = std::make_shared<DictionaryArray>(type, indices);
  return is_valid;
}

void DictionaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto indices_data = data_->Copy();
  indices_data->type = dict_type_->index_type();
  indices_ = MakeArray(indices_data);
}

std::shared_ptr<Array> DictionaryArray::indices() const { return indices_; }

std::shared_ptr<Array> DictionaryArray::dictionary() const {
  return dict_type_->dictionary();
}

template <typename InType, typename OutType>
static Status TransposeDictIndices(MemoryPool* pool, const ArrayData& in_data,
                                   const std::shared_ptr<DataType>& type,
                                   const std::vector<int32_t>& transpose_map,
                                   std::shared_ptr<Array>* out) {
  using in_c_type = typename InType::c_type;
  using out_c_type = typename OutType::c_type;

  std::shared_ptr<Buffer> out_buffer;
  RETURN_NOT_OK(AllocateBuffer(pool, in_data.length * sizeof(out_c_type), &out_buffer));
  // Null bitmap is unchanged
  auto out_data = ArrayData::Make(type, in_data.length, {in_data.buffers[0], out_buffer},
                                  in_data.null_count);
  internal::TransposeInts(in_data.GetValues<in_c_type>(1),
                          out_data->GetMutableValues<out_c_type>(1), in_data.length,
                          transpose_map.data());
  *out = MakeArray(out_data);
  return Status::OK();
}

Status DictionaryArray::Transpose(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                                  const std::vector<int32_t>& transpose_map,
                                  std::shared_ptr<Array>* out) const {
  DCHECK_EQ(type->id(), Type::DICTIONARY);
  const auto& out_dict_type = checked_cast<const DictionaryType&>(*type);

  // XXX We'll probably want to make this operation a kernel when we
  // implement dictionary-to-dictionary casting.
  auto in_type_id = dict_type_->index_type()->id();
  auto out_type_id = out_dict_type.index_type()->id();

#define TRANSPOSE_IN_OUT_CASE(IN_INDEX_TYPE, OUT_INDEX_TYPE)                        \
  case OUT_INDEX_TYPE::type_id:                                                     \
    return TransposeDictIndices<IN_INDEX_TYPE, OUT_INDEX_TYPE>(pool, *data(), type, \
                                                               transpose_map, out);

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

#undef TRANSPOSE_IN_OUT_CASE
#undef TRANSPOSE_IN_CASE
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
  Status Visit(const NullArray&) { return Status::OK(); }

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
    return Status::OK();
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

    int32_t prev_offset = array.value_offset(0);
    if (prev_offset != 0) {
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
    return Status::OK();
  }

  Status Visit(const ExtensionArray& array) { return ValidateArray(*array.storage()); }
};

}  // namespace internal

Status ValidateArray(const Array& array) {
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
  Status s = VisitTypeInline(*data->type, &wrapper_visitor);
  DCHECK(s.ok());
  DCHECK(out);
  return out;
}

// ----------------------------------------------------------------------
// Misc APIs

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

struct ConcatenateImpl {
  ConcatenateImpl(const std::vector<std::shared_ptr<ArrayData>>& in, MemoryPool* pool)
      : in_(in),
        in_size_(static_cast<int>(in.size())),
        lengths_(in.size()),
        offsets_(in.size()),
        pool_(pool),
        out_(new ArrayData) {
    out_->type = in[0]->type;
    for (int i = 0; i != in_size_; ++i) {
      lengths_[i] = in[i]->length;
      offsets_[i] = in[i]->offset;
      out_->length += lengths_[i];
      if (out_->null_count == kUnknownNullCount ||
          in[i]->null_count == kUnknownNullCount) {
        out_->null_count = kUnknownNullCount;
        continue;
      }
      out_->null_count += in[i]->null_count;
    }
  }

  struct range {
    range() : offset(-1), length(0) {}
    range(int32_t o, int32_t l) : offset(o), length(l) {}
    void widen_to_include(int32_t index) {
      if (length == 0) {
        offset = index;
        length = 1;
        return;
      }
      auto end = std::max(offset + length, index + 1);
      length = end - offset;
      offset = std::min(index, offset);
    }
    int32_t offset, length;
  };

  Status Visit(const NullType&) { return Status::OK(); }

  Status Visit(const BooleanType&) {
    std::shared_ptr<Buffer> values_buffer;
    RETURN_NOT_OK(ConcatenateBitmaps(1, &values_buffer));
    out_->buffers.push_back(values_buffer);
    return Status::OK();
  }

  // handle numbers, decimal128, fixed_size_binary
  Status Visit(const FixedWidthType& fixed) {
    DCHECK_EQ(fixed.bit_width() % 8, 0);
    const int byte_width = fixed.bit_width() / 8;
    std::shared_ptr<Buffer> values_buffer;
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      auto byte_length = byte_width * lengths_[i];
      auto byte_offset = byte_width * offsets_[i];
      values_slices[i] = SliceBuffer(in_[i]->buffers[1], byte_offset, byte_length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &values_buffer));
    out_->buffers.push_back(values_buffer);
    return Status::OK();
  }

  Status Visit(const BinaryType&) {
    std::shared_ptr<Buffer> values_buffer, offset_buffer;
    std::vector<range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &offset_buffer, &value_ranges));
    out_->buffers.push_back(offset_buffer);
    std::vector<std::shared_ptr<Buffer>> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      values_slices[i] =
          SliceBuffer(in_[i]->buffers[2], value_ranges[i].offset, value_ranges[i].length);
    }
    RETURN_NOT_OK(arrow::Concatenate(values_slices, pool_, &values_buffer));
    out_->buffers.push_back(values_buffer);
    return Status::OK();
  }

  Status Visit(const ListType&) {
    std::shared_ptr<Buffer> offset_buffer;
    std::vector<range> value_ranges;
    RETURN_NOT_OK(ConcatenateOffsets(1, &offset_buffer, &value_ranges));
    out_->buffers.push_back(offset_buffer);
    std::vector<std::shared_ptr<ArrayData>> values_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      values_slices[i] = SliceData(*in_[i]->child_data[0], value_ranges[i].offset,
                                   value_ranges[i].length);
    }
    std::shared_ptr<ArrayData> values;
    RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(&values));
    out_->child_data = {values};
    return Status::OK();
  }

  Status Visit(const StructType& s) {
    std::vector<std::shared_ptr<ArrayData>> values_slices(in_size_);
    for (int field_index = 0; field_index != s.num_children(); ++field_index) {
      for (int i = 0; i != in_size_; ++i) {
        values_slices[i] =
            SliceData(*in_[i]->child_data[field_index], offsets_[i], lengths_[i]);
      }
      std::shared_ptr<ArrayData> values;
      RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(&values));
      out_->child_data.push_back(values);
    }
    return Status::OK();
  }

  Status Visit(const DictionaryType& d) {
    std::vector<std::shared_ptr<ArrayData>> indices_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      indices_slices[i] = SliceData(*in_[i], offsets_[i], lengths_[i]);
      indices_slices[i]->type = d.index_type();
    }
    std::shared_ptr<ArrayData> indices;
    RETURN_NOT_OK(ConcatenateImpl(indices_slices, pool_).Concatenate(&indices));
    auto type = out_->type;
    auto null_bitmap = out_->buffers[0];
    out_ = indices;
    out_->type = type;
    out_->buffers[0] = null_bitmap;
    return Status::OK();
  }

  Status Visit(const UnionType& u) {
    // type_codes are an index into child_data
    std::vector<std::shared_ptr<Buffer>> codes_slices(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      codes_slices[i] = SliceBuffer(in_[i]->buffers[1], offsets_[i], lengths_[i]);
    }
    std::shared_ptr<Buffer> codes_buffer;
    RETURN_NOT_OK(arrow::Concatenate(codes_slices, pool_, &codes_buffer));
    out_->buffers.push_back(codes_buffer);
    if (u.mode() == UnionMode::SPARSE) {
      std::vector<std::shared_ptr<ArrayData>> values_slices(in_size_);
      for (int field_index = 0; field_index != u.num_children(); ++field_index) {
        for (int i = 0; i != in_size_; ++i) {
          values_slices[i] =
              SliceData(*in_[i]->child_data[field_index], offsets_[i], lengths_[i]);
        }
        std::shared_ptr<ArrayData> values;
        RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(&values));
        out_->child_data.push_back(values);
      }
      out_->buffers.push_back(nullptr);
      return Status::OK();
    }
    DCHECK_EQ(u.mode(), UnionMode::DENSE);

    // build mapping from (input, type_code)->the range of values actually referenced
    auto max_code = *std::max_element(u.type_codes().begin(), u.type_codes().end());
    std::vector<range> values_ranges((static_cast<int>(max_code) + 1) * in_size_);
    auto get_values_range = [&](int in_i, UnionArray::type_id_t code) -> range& {
      return values_ranges[in_i * in_size_ + code];
    };
    for (int i = 0; i != in_size_; ++i) {
      auto codes = in_[i]->buffers[1]->data();
      auto offsets = reinterpret_cast<const int32_t*>(in_[i]->buffers[2]->data());
      for (auto index = offsets_[i]; index != offsets_[i] + lengths_[i]; ++index) {
        get_values_range(i, codes[index]).widen_to_include(offsets[index]);
      }
    }

    // for each type_code, use the min/max offset as a slice range
    // and concatenate sliced data for that type_code
    out_->child_data.resize(static_cast<int>(max_code) + 1);
    for (auto code : u.type_codes()) {
      std::vector<std::shared_ptr<ArrayData>> values_slices(in_size_);
      for (int i = 0; i != in_size_; ++i) {
        auto values_range = get_values_range(i, code);
        values_slices[i] = SliceData(*in_[i]->child_data[code], values_range.offset,
                                     values_range.length);
      }
      std::shared_ptr<ArrayData> values;
      RETURN_NOT_OK(ConcatenateImpl(values_slices, pool_).Concatenate(&values));
      out_->child_data[code] = values;
    }

    // for each input array, adjust the offsets by the length of the slice
    // range for a given type code and the minimum offset in that input array,
    // so that offsets point into the concatenated values
    std::vector<int32_t> total_lengths(static_cast<int>(max_code) + 1, 0);
    std::shared_ptr<Buffer> offsets_buffer;
    RETURN_NOT_OK(AllocateBuffer(pool_, out_->length * sizeof(int32_t), &offsets_buffer));
    auto raw_offsets = reinterpret_cast<int32_t*>(offsets_buffer->mutable_data());
    for (int i = 0; i != in_size_; ++i) {
      auto codes = in_[i]->buffers[1]->data();
      auto offsets = reinterpret_cast<const int32_t*>(in_[i]->buffers[2]->data());
      for (auto index = offsets_[i]; index != offsets_[i] + lengths_[i]; ++index) {
        auto min_offset = get_values_range(i, codes[index]).offset;
        *raw_offsets++ = offsets[index] - min_offset + total_lengths[codes[index]];
      }
      for (auto code : u.type_codes()) {
        total_lengths[code] += get_values_range(i, code).length;
      }
    }
    out_->buffers.push_back(offsets_buffer);
    return Status::OK();
  }

  Status Concatenate(std::shared_ptr<ArrayData>* out) && {
    std::shared_ptr<Buffer> null_bitmap;
    if (out_->null_count != 0) {
      RETURN_NOT_OK(ConcatenateBitmaps(0, &null_bitmap));
    }
    out_->buffers = {null_bitmap};
    RETURN_NOT_OK(VisitTypeInline(*out_->type, this));
    *out = std::move(out_);
    return Status::OK();
  }

  Status ConcatenateBitmaps(int index, std::shared_ptr<Buffer>* bitmap_buffer) {
    RETURN_NOT_OK(AllocateBitmap(pool_, out_->length, bitmap_buffer));
    uint8_t* bitmap_data = (*bitmap_buffer)->mutable_data();
    int64_t bitmap_offset = 0;
    for (int i = 0; i != in_size_; ++i) {
      if (auto bitmap = in_[i]->buffers[0]) {
        internal::CopyBitmap(bitmap->data(), offsets_[i], lengths_[i], bitmap_data,
                             bitmap_offset);
      } else {
        BitUtil::SetBitsTo(bitmap_data, bitmap_offset, lengths_[i], true);
      }
      bitmap_offset += lengths_[i];
    }
    if (auto preceding_bits = BitUtil::kPrecedingBitmask[out_->length % 8]) {
      bitmap_data[out_->length / 8] &= preceding_bits;
    }
    return Status::OK();
  }

  // FIXME the below assumes that the first offset in the inputs will always be 0, which
  // isn't necessarily correct. Accumulating first and last offsets will be necessary
  // because we need to slice the referenced data (child_data for lists, values_buffer for
  // strings)
  Status ConcatenateOffsets(int index, std::shared_ptr<Buffer>* offset_buffer,
                            std::vector<range>* ranges) {
    RETURN_NOT_OK(
        AllocateBuffer(pool_, (out_->length + 1) * sizeof(int32_t), offset_buffer));
    auto dst_offsets_begin = reinterpret_cast<int32_t*>((*offset_buffer)->mutable_data());
    int32_t values_length = 0;
    ranges->resize(in_size_);
    for (int i = 0; i != in_size_; ++i) {
      auto src_offsets_begin = in_[i]->GetValues<int32_t>(index) + offsets_[i];
      auto src_offsets_end = src_offsets_begin + lengths_[i];
      auto first_offset = src_offsets_begin[0];
      ranges->at(i).offset = first_offset;
      std::transform(src_offsets_begin, src_offsets_end, dst_offsets_begin,
                     [values_length, first_offset](int32_t offset) {
                       return offset - first_offset + values_length;
                     });
      auto last_offset = *src_offsets_end;
      ranges->at(i).length = last_offset - first_offset;
      values_length += last_offset - first_offset;
      dst_offsets_begin += lengths_[i];
    }
    *dst_offsets_begin = values_length;
    return Status::OK();
  }

  const std::vector<std::shared_ptr<ArrayData>>& in_;
  int in_size_;
  std::vector<int64_t> lengths_, offsets_;
  MemoryPool* pool_;
  std::shared_ptr<ArrayData> out_;
};

Status Concatenate(const std::vector<std::shared_ptr<Array>>& arrays, MemoryPool* pool,
                   std::shared_ptr<Array>* out) {
  DCHECK_GT(arrays.size(), 0);
  std::vector<std::shared_ptr<ArrayData>> data(arrays.size());
  for (std::size_t i = 0; i != arrays.size(); ++i) {
    DCHECK(arrays[i]->type()->Equals(*arrays[0]->type()));
    data[i] = arrays[i]->data();
  }
  std::shared_ptr<ArrayData> out_data;
  RETURN_NOT_OK(ConcatenateImpl(data, pool).Concatenate(&out_data));
  *out = MakeArray(std::move(out_data));
  return Status::OK();
}

}  // namespace arrow
