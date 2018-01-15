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
#include <cstdint>
#include <cstring>
#include <sstream>

#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/visitor.h"
#include "arrow/visitor_inline.h"

namespace arrow {

std::shared_ptr<ArrayData> ArrayData::Make(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           std::vector<std::shared_ptr<Buffer>>&& buffers,
                                           int64_t null_count, int64_t offset) {
  return std::make_shared<ArrayData>(type, length, std::move(buffers), null_count,
                                     offset);
}

std::shared_ptr<ArrayData> ArrayData::Make(const std::shared_ptr<DataType>& type,
                                           int64_t length, int64_t null_count,
                                           int64_t offset) {
  return std::make_shared<ArrayData>(type, length, null_count, offset);
}

// ----------------------------------------------------------------------
// Base array class

int64_t Array::null_count() const {
  if (ARROW_PREDICT_FALSE(data_->null_count < 0)) {
    if (data_->buffers[0]) {
      data_->null_count =
          data_->length - CountSetBits(null_bitmap_data_, data_->offset, data_->length);

    } else {
      data_->null_count = 0;
    }
  }
  return data_->null_count;
}

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
  DCHECK(PrettyPrint(*this, 0, &ss).ok());
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

#ifndef ARROW_NO_DEPRECATED_API

const uint8_t* PrimitiveArray::raw_values() const {
  return raw_values_ +
         offset() * static_cast<const FixedWidthType&>(*type()).bit_width() / CHAR_BIT;
}

#endif

template <typename T>
NumericArray<T>::NumericArray(const std::shared_ptr<ArrayData>& data)
    : PrimitiveArray(data) {
  DCHECK_EQ(data->type->id(), T::type_id);
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

  const auto& typed_offsets = static_cast<const Int32Array&>(offsets);

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
  values_ = MakeArray(data_->child_data[0]);
}

std::shared_ptr<DataType> ListArray::value_type() const {
  return static_cast<const ListType&>(*type()).value_type();
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
      byte_width_(static_cast<const FixedSizeBinaryType&>(*type).byte_width()) {}

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
  const auto& type_ = static_cast<const Decimal128Type&>(*type());
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

std::shared_ptr<Array> StructArray::field(int i) const {
  if (!boxed_fields_[i]) {
    boxed_fields_[i] = MakeArray(data_->child_data[i]);
  }
  DCHECK(boxed_fields_[i]);
  return boxed_fields_[i];
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
                          static_cast<const UInt8Array&>(type_ids).values(),
                          static_cast<const Int32Array&>(value_offsets).values()};
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
                          static_cast<const UInt8Array&>(type_ids).values(), nullptr};
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
    boxed_fields_[i] = MakeArray(data_->child_data[i]);
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

DictionaryArray::DictionaryArray(const std::shared_ptr<ArrayData>& data)
    : dict_type_(static_cast<const DictionaryType*>(data->type.get())) {
  DCHECK_EQ(data->type->id(), Type::DICTIONARY);
  SetData(data);
}

DictionaryArray::DictionaryArray(const std::shared_ptr<DataType>& type,
                                 const std::shared_ptr<Array>& indices)
    : dict_type_(static_cast<const DictionaryType*>(type.get())) {
  DCHECK_EQ(type->id(), Type::DICTIONARY);
  DCHECK_EQ(indices->type_id(), dict_type_->index_type()->id());
  auto data = indices->data()->Copy();
  data->type = type;
  SetData(data);
}

void DictionaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto indices_data = data_->Copy();
  indices_data->type = dict_type_->index_type();
  std::shared_ptr<Array> result;
  indices_ = MakeArray(indices_data);
}

std::shared_ptr<Array> DictionaryArray::indices() const { return indices_; }

std::shared_ptr<Array> DictionaryArray::dictionary() const {
  return dict_type_->dictionary();
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

  Status Visit(const PrimitiveArray&) { return Status::OK(); }

  Status Visit(const Decimal128Array&) { return Status::OK(); }

  Status Visit(const BinaryArray&) {
    // TODO(wesm): what to do here?
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
      std::stringstream ss;
      ss << "offset buffer size (bytes): " << value_offsets->size()
         << " isn't large enough for length: " << array.length();
      return Status::Invalid(ss.str());
    }
    const int32_t last_offset = array.value_offset(array.length());
    if (last_offset > 0) {
      if (!array.values()) {
        return Status::Invalid("last offset was non-zero and values was null");
      }
      if (array.values()->length() != last_offset) {
        std::stringstream ss;
        ss << "Final offset invariant not equal to values length: " << last_offset
           << "!=" << array.values()->length();
        return Status::Invalid(ss.str());
      }

      const Status child_valid = ValidateArray(*array.values());
      if (!child_valid.ok()) {
        std::stringstream ss;
        ss << "Child array invalid: " << child_valid.ToString();
        return Status::Invalid(ss.str());
      }
    }

    int32_t prev_offset = array.value_offset(0);
    if (prev_offset != 0) {
      return Status::Invalid("The first offset wasn't zero");
    }
    for (int64_t i = 1; i <= array.length(); ++i) {
      int32_t current_offset = array.value_offset(i);
      if (array.IsNull(i - 1) && current_offset != prev_offset) {
        std::stringstream ss;
        ss << "Offset invariant failure at: " << i
           << " inconsistent value_offsets for null slot" << current_offset
           << "!=" << prev_offset;
        return Status::Invalid(ss.str());
      }
      if (current_offset < prev_offset) {
        std::stringstream ss;
        ss << "Offset invariant failure: " << i
           << " inconsistent offset for non-null slot: " << current_offset << "<"
           << prev_offset;
        return Status::Invalid(ss.str());
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
          std::stringstream ss;
          ss << "Length is not equal from field " << it->type()->ToString()
             << " at position {" << idx << "}";
          return Status::Invalid(ss.str());
        }

        const Status child_valid = ValidateArray(*it);
        if (!child_valid.ok()) {
          std::stringstream ss;
          ss << "Child array invalid: " << child_valid.ToString() << " at position {"
             << idx << "}";
          return Status::Invalid(ss.str());
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

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<Array>* out_;
};

}  // namespace internal

#ifndef ARROW_NO_DEPRECATED_API

Status MakeArray(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out) {
  internal::ArrayDataWrapper wrapper_visitor(data, out);
  RETURN_NOT_OK(VisitTypeInline(*data->type, &wrapper_visitor));
  DCHECK(out);
  return Status::OK();
}

#endif

std::shared_ptr<Array> MakeArray(const std::shared_ptr<ArrayData>& data) {
  std::shared_ptr<Array> out;
  internal::ArrayDataWrapper wrapper_visitor(data, &out);
  Status s = VisitTypeInline(*data->type, &wrapper_visitor);
  DCHECK(s.ok());
  DCHECK(out);
  return out;
}

// ----------------------------------------------------------------------
// Instantiate templates

template class ARROW_TEMPLATE_EXPORT NumericArray<UInt8Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<UInt16Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<UInt32Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<UInt64Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Int8Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Int16Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Int32Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Int64Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<TimestampType>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Date32Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Date64Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Time32Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<Time64Type>;
template class ARROW_TEMPLATE_EXPORT NumericArray<HalfFloatType>;
template class ARROW_TEMPLATE_EXPORT NumericArray<FloatType>;
template class ARROW_TEMPLATE_EXPORT NumericArray<DoubleType>;

}  // namespace arrow
