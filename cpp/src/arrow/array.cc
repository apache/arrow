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

using internal::ArrayData;

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

bool Array::Equals(const Array& arr) const {
  bool are_equal = false;
  Status error = ArrayEquals(*this, arr, &are_equal);
  if (!error.ok()) {
    DCHECK(false) << "Arrays not comparable: " << error.ToString();
  }
  return are_equal;
}

bool Array::Equals(const std::shared_ptr<Array>& arr) const {
  if (!arr) {
    return false;
  }
  return Equals(*arr);
}

bool Array::ApproxEquals(const Array& arr) const {
  bool are_equal = false;
  Status error = ArrayApproxEquals(*this, arr, &are_equal);
  if (!error.ok()) {
    DCHECK(false) << "Arrays not comparable: " << error.ToString();
  }
  return are_equal;
}

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
  bool are_equal = false;
  Status error =
      ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx, &are_equal);
  if (!error.ok()) {
    DCHECK(false) << "Arrays not comparable: " << error.ToString();
  }
  return are_equal;
}

// Last two parameters are in-out parameters
static inline void ConformSliceParams(int64_t array_offset, int64_t array_length,
                                      int64_t* offset, int64_t* length) {
  DCHECK_LE(*offset, array_length);
  DCHECK_NE(offset, nullptr);
  *length = std::min(array_length - *offset, *length);
  *offset = array_offset + *offset;
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

static inline std::shared_ptr<ArrayData> SliceData(const ArrayData& data, int64_t offset,
                                                   int64_t length) {
  ConformSliceParams(data.offset, data.length, &offset, &length);

  auto new_data = data.ShallowCopy();
  new_data->length = length;
  new_data->offset = offset;
  new_data->null_count = kUnknownNullCount;
  return new_data;
}

NullArray::NullArray(int64_t length) {
  BufferVector buffers = {nullptr};
  SetData(std::make_shared<ArrayData>(null(), length, std::move(buffers), length));
}

std::shared_ptr<Array> NullArray::Slice(int64_t offset, int64_t length) const {
  DCHECK_LE(offset, data_->length);
  length = std::min(data_->length - offset, length);
  return std::make_shared<NullArray>(length);
}

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
                               const std::shared_ptr<Buffer>& data,
                               const std::shared_ptr<Buffer>& null_bitmap,
                               int64_t null_count, int64_t offset) {
  BufferVector buffers = {null_bitmap, data};
  SetData(
      std::make_shared<ArrayData>(type, length, std::move(buffers), null_count, offset));
}

const uint8_t* PrimitiveArray::raw_values() const {
  return raw_values_ +
         offset() * static_cast<const FixedWidthType&>(*type()).bit_width() / CHAR_BIT;
}

template <typename T>
NumericArray<T>::NumericArray(const std::shared_ptr<internal::ArrayData>& data)
    : PrimitiveArray(data) {
  DCHECK_EQ(data->type->id(), T::type_id);
}

template <typename T>
std::shared_ptr<Array> NumericArray<T>::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<NumericArray<T>>(SliceData(*data_, offset, length));
}

// ----------------------------------------------------------------------
// BooleanArray

BooleanArray::BooleanArray(const std::shared_ptr<internal::ArrayData>& data)
    : PrimitiveArray(data) {
  DCHECK_EQ(data->type->id(), Type::BOOL);
}

BooleanArray::BooleanArray(int64_t length, const std::shared_ptr<Buffer>& data,
                           const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                           int64_t offset)
    : PrimitiveArray(boolean(), length, data, null_bitmap, null_count, offset) {}

std::shared_ptr<Array> BooleanArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<BooleanArray>(SliceData(*data_, offset, length));
}

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
  BufferVector buffers = {null_bitmap, value_offsets};
  auto internal_data =
      std::make_shared<ArrayData>(type, length, std::move(buffers), null_count, offset);
  internal_data->child_data.emplace_back(values->data());
  SetData(internal_data);
}

Status ListArray::FromArrays(const Array& offsets, const Array& values, MemoryPool* pool,
                             std::shared_ptr<Array>* out) {
  if (ARROW_PREDICT_FALSE(offsets.length() == 0)) {
    return Status::Invalid("List offsets must have non-zero length");
  }

  if (ARROW_PREDICT_FALSE(offsets.null_count() > 0)) {
    return Status::Invalid("Null offsets in ListArray::FromArrays not yet implemented");
  }

  if (ARROW_PREDICT_FALSE(offsets.type_id() != Type::INT32)) {
    return Status::Invalid("List offsets must be signed int32");
  }

  BufferVector buffers = {offsets.null_bitmap(),
                          static_cast<const Int32Array&>(offsets).values()};

  auto list_type = list(values.type());
  auto internal_data = std::make_shared<internal::ArrayData>(
      list_type, offsets.length() - 1, std::move(buffers), offsets.null_count(),
      offsets.offset());
  internal_data->child_data.push_back(values.data());

  *out = std::make_shared<ListArray>(internal_data);
  return Status::OK();
}

void ListArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto value_offsets = data->buffers[1];
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());
  DCHECK(internal::MakeArray(data_->child_data[0], &values_).ok());
}

std::shared_ptr<DataType> ListArray::value_type() const {
  return static_cast<const ListType&>(*type()).value_type();
}

std::shared_ptr<Array> ListArray::values() const { return values_; }

std::shared_ptr<Array> ListArray::Slice(int64_t offset, int64_t length) const {
  ConformSliceParams(data_->offset, data_->length, &offset, &length);
  return std::make_shared<ListArray>(type(), length, value_offsets(), values(),
                                     null_bitmap(), kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// String and binary

static std::shared_ptr<DataType> kBinary = std::make_shared<BinaryType>();
static std::shared_ptr<DataType> kString = std::make_shared<StringType>();

BinaryArray::BinaryArray(const std::shared_ptr<internal::ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::BINARY);
  SetData(data);
}

void BinaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
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
    : BinaryArray(kBinary, length, value_offsets, data, null_bitmap, null_count, offset) {
}

BinaryArray::BinaryArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset) {
  BufferVector buffers = {null_bitmap, value_offsets, data};
  SetData(
      std::make_shared<ArrayData>(type, length, std::move(buffers), null_count, offset));
}

std::shared_ptr<Array> BinaryArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<BinaryArray>(SliceData(*data_, offset, length));
}

StringArray::StringArray(const std::shared_ptr<internal::ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::STRING);
  SetData(data);
}

StringArray::StringArray(int64_t length, const std::shared_ptr<Buffer>& value_offsets,
                         const std::shared_ptr<Buffer>& data,
                         const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                         int64_t offset)
    : BinaryArray(kString, length, value_offsets, data, null_bitmap, null_count, offset) {
}

std::shared_ptr<Array> StringArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<StringArray>(SliceData(*data_, offset, length));
}

// ----------------------------------------------------------------------
// Fixed width binary

FixedSizeBinaryArray::FixedSizeBinaryArray(
    const std::shared_ptr<internal::ArrayData>& data) {
  SetData(data);
}

FixedSizeBinaryArray::FixedSizeBinaryArray(const std::shared_ptr<DataType>& type,
                                           int64_t length,
                                           const std::shared_ptr<Buffer>& data,
                                           const std::shared_ptr<Buffer>& null_bitmap,
                                           int64_t null_count, int64_t offset)
    : PrimitiveArray(type, length, data, null_bitmap, null_count, offset),
      byte_width_(static_cast<const FixedSizeBinaryType&>(*type).byte_width()) {}

std::shared_ptr<Array> FixedSizeBinaryArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<FixedSizeBinaryArray>(SliceData(*data_, offset, length));
}

const uint8_t* FixedSizeBinaryArray::GetValue(int64_t i) const {
  return raw_values_ + (i + data_->offset) * byte_width_;
}

// ----------------------------------------------------------------------
// Decimal

DecimalArray::DecimalArray(const std::shared_ptr<internal::ArrayData>& data)
    : FixedSizeBinaryArray(data) {
  DCHECK_EQ(data->type->id(), Type::DECIMAL);
}

#define DECIMAL_TO_STRING_CASE(bits, bytes, precision, scale) \
  case bits: {                                                \
    decimal::Decimal##bits value;                             \
    decimal::FromBytes((bytes), &value);                      \
    return decimal::ToString(value, (precision), (scale));    \
  }

std::string DecimalArray::FormatValue(int64_t i) const {
  const auto& type_ = static_cast<const DecimalType&>(*type());
  const int precision = type_.precision();
  const int scale = type_.scale();
  const int bit_width = type_.bit_width();
  const uint8_t* bytes = GetValue(i);
  switch (bit_width) {
    DECIMAL_TO_STRING_CASE(32, bytes, precision, scale)
    DECIMAL_TO_STRING_CASE(64, bytes, precision, scale)
    DECIMAL_TO_STRING_CASE(128, bytes, precision, scale)
    default: {
      DCHECK(false) << "Invalid bit width: " << bit_width;
      return "";
    }
  }
}

std::shared_ptr<Array> DecimalArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<DecimalArray>(SliceData(*data_, offset, length));
}

// ----------------------------------------------------------------------
// Struct

StructArray::StructArray(const std::shared_ptr<ArrayData>& data) {
  DCHECK_EQ(data->type->id(), Type::STRUCT);
  SetData(data);
}

StructArray::StructArray(const std::shared_ptr<DataType>& type, int64_t length,
                         const std::vector<std::shared_ptr<Array>>& children,
                         std::shared_ptr<Buffer> null_bitmap, int64_t null_count,
                         int64_t offset) {
  BufferVector buffers = {null_bitmap};
  SetData(
      std::make_shared<ArrayData>(type, length, std::move(buffers), null_count, offset));
  for (const auto& child : children) {
    data_->child_data.push_back(child->data());
  }
}

std::shared_ptr<Array> StructArray::field(int pos) const {
  std::shared_ptr<Array> result;
  DCHECK(internal::MakeArray(data_->child_data[pos], &result).ok());
  return result;
}

std::shared_ptr<Array> StructArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<StructArray>(SliceData(*data_, offset, length));
}

// ----------------------------------------------------------------------
// UnionArray

void UnionArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);

  auto type_ids = data_->buffers[1];
  auto value_offsets = data_->buffers[2];
  raw_type_ids_ =
      type_ids == nullptr ? nullptr : reinterpret_cast<const uint8_t*>(type_ids->data());
  raw_value_offsets_ = value_offsets == nullptr
                           ? nullptr
                           : reinterpret_cast<const int32_t*>(value_offsets->data());
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
  BufferVector buffers = {null_bitmap, type_ids, value_offsets};
  auto internal_data =
      std::make_shared<ArrayData>(type, length, std::move(buffers), null_count, offset);
  for (const auto& child : children) {
    internal_data->child_data.push_back(child->data());
  }
  SetData(internal_data);
}

std::shared_ptr<Array> UnionArray::child(int pos) const {
  std::shared_ptr<Array> result;
  DCHECK(internal::MakeArray(data_->child_data[pos], &result).ok());
  return result;
}

std::shared_ptr<Array> UnionArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<UnionArray>(SliceData(*data_, offset, length));
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
  auto data = indices->data()->ShallowCopy();
  data->type = type;
  SetData(data);
}

void DictionaryArray::SetData(const std::shared_ptr<ArrayData>& data) {
  this->Array::SetData(data);
  auto indices_data = data_->ShallowCopy();
  indices_data->type = dict_type_->index_type();
  std::shared_ptr<Array> result;
  DCHECK(internal::MakeArray(indices_data, &indices_).ok());
}

std::shared_ptr<Array> DictionaryArray::indices() const { return indices_; }

std::shared_ptr<Array> DictionaryArray::dictionary() const {
  return dict_type_->dictionary();
}

std::shared_ptr<Array> DictionaryArray::Slice(int64_t offset, int64_t length) const {
  return std::make_shared<DictionaryArray>(SliceData(*data_, offset, length));
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
  Status Visit(const NullArray& array) { return Status::OK(); }

  Status Visit(const PrimitiveArray& array) { return Status::OK(); }

  Status Visit(const DecimalArray& array) { return Status::OK(); }

  Status Visit(const BinaryArray& array) {
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
  Status Visit(const T& type) {
    using ArrayType = typename TypeTraits<T>::ArrayType;
    *out_ = std::make_shared<ArrayType>(data_);
    return Status::OK();
  }

  const std::shared_ptr<ArrayData>& data_;
  std::shared_ptr<Array>* out_;
};

Status MakeArray(const std::shared_ptr<ArrayData>& data, std::shared_ptr<Array>* out) {
  ArrayDataWrapper wrapper_visitor(data, out);
  return VisitTypeInline(*data->type, &wrapper_visitor);
}

}  // namespace internal

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type, int64_t length,
                          const std::shared_ptr<Buffer>& data,
                          const std::shared_ptr<Buffer>& null_bitmap, int64_t null_count,
                          int64_t offset, std::shared_ptr<Array>* out) {
  BufferVector buffers = {null_bitmap, data};
  auto internal_data = std::make_shared<internal::ArrayData>(
      type, length, std::move(buffers), null_count, offset);
  return internal::MakeArray(internal_data, out);
}

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type,
                          const std::vector<std::shared_ptr<Buffer>>& buffers,
                          int64_t length, int64_t null_count, int64_t offset,
                          std::shared_ptr<Array>* out) {
  auto internal_data =
      std::make_shared<internal::ArrayData>(type, length, buffers, null_count, offset);
  return internal::MakeArray(internal_data, out);
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
