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
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/logging.h"

namespace arrow {

// When slicing, we do not know the null count of the sliced range without
// doing some computation. To avoid doing this eagerly, we set the null count
// to -1 (any negative number will do). When Array::null_count is called the
// first time, the null count will be computed. See ARROW-33
constexpr int32_t kUnknownNullCount = -1;

// ----------------------------------------------------------------------
// Base array class

Array::Array(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& null_bitmap, int32_t null_count, int32_t offset)
    : type_(type),
      length_(length),
      offset_(offset),
      null_count_(null_count),
      null_bitmap_(null_bitmap),
      null_bitmap_data_(nullptr) {
  if (null_count_ == 0) { null_bitmap_ = nullptr; }
  if (null_bitmap_) { null_bitmap_data_ = null_bitmap_->data(); }
}

int32_t Array::null_count() const {
  if (null_count_ < 0) {
    if (null_bitmap_) {
      null_count_ = length_ - CountSetBits(null_bitmap_data_, offset_, length_);
    } else {
      null_count_ = 0;
    }
  }
  return null_count_;
}

bool Array::Equals(const Array& arr) const {
  bool are_equal = false;
  Status error = ArrayEquals(*this, arr, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Arrays not comparable: " << error.ToString(); }
  return are_equal;
}

bool Array::Equals(const std::shared_ptr<Array>& arr) const {
  if (!arr) { return false; }
  return Equals(*arr);
}

bool Array::ApproxEquals(const Array& arr) const {
  bool are_equal = false;
  Status error = ArrayApproxEquals(*this, arr, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Arrays not comparable: " << error.ToString(); }
  return are_equal;
}

bool Array::ApproxEquals(const std::shared_ptr<Array>& arr) const {
  if (!arr) { return false; }
  return ApproxEquals(*arr);
}

bool Array::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& other) const {
  if (!other) { return false; }
  return RangeEquals(*other, start_idx, end_idx, other_start_idx);
}

bool Array::RangeEquals(const Array& other, int32_t start_idx, int32_t end_idx,
    int32_t other_start_idx) const {
  bool are_equal = false;
  Status error =
      ArrayRangeEquals(*this, other, start_idx, end_idx, other_start_idx, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Arrays not comparable: " << error.ToString(); }
  return are_equal;
}

// Last two parameters are in-out parameters
static inline void ConformSliceParams(
    int32_t array_offset, int32_t array_length, int32_t* offset, int32_t* length) {
  DCHECK_LE(*offset, array_length);
  DCHECK_GE(offset, 0);
  *length = std::min(array_length - *offset, *length);
  *offset = array_offset + *offset;
}

std::shared_ptr<Array> Array::Slice(int32_t offset) const {
  int32_t slice_length = length_ - offset;
  return Slice(offset, slice_length);
}

Status Array::Validate() const {
  return Status::OK();
}

NullArray::NullArray(int32_t length) : Array(null(), length, nullptr, length) {}

std::shared_ptr<Array> NullArray::Slice(int32_t offset, int32_t length) const {
  DCHECK_LE(offset, length_);
  length = std::min(length_ - offset, length);
  return std::make_shared<NullArray>(length);
}

Status NullArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& data, const std::shared_ptr<Buffer>& null_bitmap,
    int32_t null_count, int32_t offset)
    : Array(type, length, null_bitmap, null_count, offset) {
  data_ = data;
  raw_data_ = data == nullptr ? nullptr : data_->data();
}

template <typename T>
Status NumericArray<T>::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

template <typename T>
std::shared_ptr<Array> NumericArray<T>::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<NumericArray<T>>(
      type_, length, data_, null_bitmap_, kUnknownNullCount, offset);
}

template class NumericArray<UInt8Type>;
template class NumericArray<UInt16Type>;
template class NumericArray<UInt32Type>;
template class NumericArray<UInt64Type>;
template class NumericArray<Int8Type>;
template class NumericArray<Int16Type>;
template class NumericArray<Int32Type>;
template class NumericArray<Int64Type>;
template class NumericArray<TimestampType>;
template class NumericArray<DateType>;
template class NumericArray<TimeType>;
template class NumericArray<HalfFloatType>;
template class NumericArray<FloatType>;
template class NumericArray<DoubleType>;

// ----------------------------------------------------------------------
// BooleanArray

BooleanArray::BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
    const std::shared_ptr<Buffer>& null_bitmap, int32_t null_count, int32_t offset)
    : PrimitiveArray(std::make_shared<BooleanType>(), length, data, null_bitmap,
          null_count, offset) {}

Status BooleanArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> BooleanArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<BooleanArray>(
      length, data_, null_bitmap_, kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// ListArray

Status ListArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }
  if (!value_offsets_) { return Status::Invalid("value_offsets_ was null"); }
  if (value_offsets_->size() / static_cast<int>(sizeof(int32_t)) < length_) {
    std::stringstream ss;
    ss << "offset buffer size (bytes): " << value_offsets_->size()
       << " isn't large enough for length: " << length_;
    return Status::Invalid(ss.str());
  }
  const int32_t last_offset = this->value_offset(length_);
  if (last_offset > 0) {
    if (!values_) {
      return Status::Invalid("last offset was non-zero and values was null");
    }
    if (values_->length() != last_offset) {
      std::stringstream ss;
      ss << "Final offset invariant not equal to values length: " << last_offset
         << "!=" << values_->length();
      return Status::Invalid(ss.str());
    }

    const Status child_valid = values_->Validate();
    if (!child_valid.ok()) {
      std::stringstream ss;
      ss << "Child array invalid: " << child_valid.ToString();
      return Status::Invalid(ss.str());
    }
  }

  int32_t prev_offset = this->value_offset(0);
  if (prev_offset != 0) { return Status::Invalid("The first offset wasn't zero"); }
  for (int32_t i = 1; i <= length_; ++i) {
    int32_t current_offset = this->value_offset(i);
    if (IsNull(i - 1) && current_offset != prev_offset) {
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

Status ListArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> ListArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<ListArray>(
      type_, length, value_offsets_, values_, null_bitmap_, kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// String and binary

static std::shared_ptr<DataType> kBinary = std::make_shared<BinaryType>();
static std::shared_ptr<DataType> kString = std::make_shared<StringType>();

BinaryArray::BinaryArray(int32_t length, const std::shared_ptr<Buffer>& value_offsets,
    const std::shared_ptr<Buffer>& data, const std::shared_ptr<Buffer>& null_bitmap,
    int32_t null_count, int32_t offset)
    : BinaryArray(kBinary, length, value_offsets, data, null_bitmap, null_count, offset) {
}

BinaryArray::BinaryArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& value_offsets, const std::shared_ptr<Buffer>& data,
    const std::shared_ptr<Buffer>& null_bitmap, int32_t null_count, int32_t offset)
    : Array(type, length, null_bitmap, null_count, offset),
      value_offsets_(value_offsets),
      raw_value_offsets_(nullptr),
      data_(data),
      raw_data_(nullptr) {
  if (value_offsets_ != nullptr) {
    raw_value_offsets_ = reinterpret_cast<const int32_t*>(value_offsets_->data());
  }
  if (data_ != nullptr) { raw_data_ = data_->data(); }
}

Status BinaryArray::Validate() const {
  // TODO(wesm): what to do here?
  return Status::OK();
}

Status BinaryArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> BinaryArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<BinaryArray>(
      length, value_offsets_, data_, null_bitmap_, kUnknownNullCount, offset);
}

StringArray::StringArray(int32_t length, const std::shared_ptr<Buffer>& value_offsets,
    const std::shared_ptr<Buffer>& data, const std::shared_ptr<Buffer>& null_bitmap,
    int32_t null_count, int32_t offset)
    : BinaryArray(kString, length, value_offsets, data, null_bitmap, null_count, offset) {
}

Status StringArray::Validate() const {
  // TODO(emkornfield) Validate proper UTF8 code points?
  return BinaryArray::Validate();
}

Status StringArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> StringArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<StringArray>(
      length, value_offsets_, data_, null_bitmap_, kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// Struct

StructArray::StructArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::vector<std::shared_ptr<Array>>& children,
    std::shared_ptr<Buffer> null_bitmap, int32_t null_count, int32_t offset)
    : Array(type, length, null_bitmap, null_count, offset) {
  type_ = type;
  children_ = children;
}

std::shared_ptr<Array> StructArray::field(int32_t pos) const {
  DCHECK_GT(children_.size(), 0);
  return children_[pos];
}

Status StructArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  if (children_.size() > 0) {
    // Validate fields
    int32_t array_length = children_[0]->length();
    size_t idx = 0;
    for (auto it : children_) {
      if (it->length() != array_length) {
        std::stringstream ss;
        ss << "Length is not equal from field " << it->type()->ToString()
           << " at position {" << idx << "}";
        return Status::Invalid(ss.str());
      }

      const Status child_valid = it->Validate();
      if (!child_valid.ok()) {
        std::stringstream ss;
        ss << "Child array invalid: " << child_valid.ToString() << " at position {" << idx
           << "}";
        return Status::Invalid(ss.str());
      }
      ++idx;
    }

    if (array_length > 0 && array_length != length_) {
      return Status::Invalid("Struct's length is not equal to its child arrays");
    }
  }
  return Status::OK();
}

Status StructArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> StructArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<StructArray>(
      type_, length, children_, null_bitmap_, kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// UnionArray

UnionArray::UnionArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::vector<std::shared_ptr<Array>>& children,
    const std::shared_ptr<Buffer>& type_ids, const std::shared_ptr<Buffer>& value_offsets,
    const std::shared_ptr<Buffer>& null_bitmap, int32_t null_count, int32_t offset)
    : Array(type, length, null_bitmap, null_count, offset),
      children_(children),
      type_ids_(type_ids),
      raw_type_ids_(nullptr),
      value_offsets_(value_offsets),
      raw_value_offsets_(nullptr) {
  if (type_ids) { raw_type_ids_ = reinterpret_cast<const uint8_t*>(type_ids->data()); }
  if (value_offsets) {
    raw_value_offsets_ = reinterpret_cast<const int32_t*>(value_offsets->data());
  }
}

std::shared_ptr<Array> UnionArray::child(int32_t pos) const {
  DCHECK_GT(children_.size(), 0);
  return children_[pos];
}

Status UnionArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  DCHECK(false) << "Validate not yet implemented";
  return Status::OK();
}

Status UnionArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> UnionArray::Slice(int32_t offset, int32_t length) const {
  ConformSliceParams(offset_, length_, &offset, &length);
  return std::make_shared<UnionArray>(type_, length, children_, type_ids_, value_offsets_,
      null_bitmap_, kUnknownNullCount, offset);
}

// ----------------------------------------------------------------------
// DictionaryArray

Status DictionaryArray::FromBuffer(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& indices, const std::shared_ptr<Buffer>& null_bitmap,
    int32_t null_count, int32_t offset, std::shared_ptr<DictionaryArray>* out) {
  DCHECK_EQ(type->type, Type::DICTIONARY);
  const auto& dict_type = static_cast<const DictionaryType*>(type.get());

  std::shared_ptr<Array> boxed_indices;
  RETURN_NOT_OK(MakePrimitiveArray(dict_type->index_type(), length, indices, null_bitmap,
      null_count, offset, &boxed_indices));

  *out = std::make_shared<DictionaryArray>(type, boxed_indices);
  return Status::OK();
}

DictionaryArray::DictionaryArray(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& indices)
    : Array(type, indices->length(), indices->null_bitmap(), indices->null_count(),
          indices->offset()),
      dict_type_(static_cast<const DictionaryType*>(type.get())),
      indices_(indices) {
  DCHECK_EQ(type->type, Type::DICTIONARY);
}

Status DictionaryArray::Validate() const {
  Type::type index_type_id = indices_->type()->type;
  if (!is_integer(index_type_id)) {
    return Status::Invalid("Dictionary indices must be integer type");
  }
  return Status::OK();
}

std::shared_ptr<Array> DictionaryArray::dictionary() const {
  return dict_type_->dictionary();
}

Status DictionaryArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

std::shared_ptr<Array> DictionaryArray::Slice(int32_t offset, int32_t length) const {
  std::shared_ptr<Array> sliced_indices = indices_->Slice(offset, length);
  return std::make_shared<DictionaryArray>(type_, sliced_indices);
}

// ----------------------------------------------------------------------

#define MAKE_PRIMITIVE_ARRAY_CASE(ENUM, ArrayType)                                  \
  case Type::ENUM:                                                                  \
    out->reset(new ArrayType(type, length, data, null_bitmap, null_count, offset)); \
    break;

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& data, const std::shared_ptr<Buffer>& null_bitmap,
    int32_t null_count, int32_t offset, std::shared_ptr<Array>* out) {
  switch (type->type) {
    MAKE_PRIMITIVE_ARRAY_CASE(BOOL, BooleanArray);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT8, UInt8Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT8, Int8Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT16, UInt16Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT16, Int16Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT32, UInt32Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT32, Int32Array);
    MAKE_PRIMITIVE_ARRAY_CASE(UINT64, UInt64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(INT64, Int64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(FLOAT, FloatArray);
    MAKE_PRIMITIVE_ARRAY_CASE(DOUBLE, DoubleArray);
    MAKE_PRIMITIVE_ARRAY_CASE(TIME, Int64Array);
    MAKE_PRIMITIVE_ARRAY_CASE(TIMESTAMP, TimestampArray);
    default:
      return Status::NotImplemented(type->ToString());
  }
#ifdef NDEBUG
  return Status::OK();
#else
  return (*out)->Validate();
#endif
}

// ----------------------------------------------------------------------
// Default implementations of ArrayVisitor methods

#define ARRAY_VISITOR_DEFAULT(ARRAY_CLASS)                   \
  Status ArrayVisitor::Visit(const ARRAY_CLASS& array) {     \
    return Status::NotImplemented(array.type()->ToString()); \
  }

ARRAY_VISITOR_DEFAULT(NullArray);
ARRAY_VISITOR_DEFAULT(BooleanArray);
ARRAY_VISITOR_DEFAULT(Int8Array);
ARRAY_VISITOR_DEFAULT(Int16Array);
ARRAY_VISITOR_DEFAULT(Int32Array);
ARRAY_VISITOR_DEFAULT(Int64Array);
ARRAY_VISITOR_DEFAULT(UInt8Array);
ARRAY_VISITOR_DEFAULT(UInt16Array);
ARRAY_VISITOR_DEFAULT(UInt32Array);
ARRAY_VISITOR_DEFAULT(UInt64Array);
ARRAY_VISITOR_DEFAULT(HalfFloatArray);
ARRAY_VISITOR_DEFAULT(FloatArray);
ARRAY_VISITOR_DEFAULT(DoubleArray);
ARRAY_VISITOR_DEFAULT(StringArray);
ARRAY_VISITOR_DEFAULT(BinaryArray);
ARRAY_VISITOR_DEFAULT(DateArray);
ARRAY_VISITOR_DEFAULT(TimeArray);
ARRAY_VISITOR_DEFAULT(TimestampArray);
ARRAY_VISITOR_DEFAULT(IntervalArray);
ARRAY_VISITOR_DEFAULT(ListArray);
ARRAY_VISITOR_DEFAULT(StructArray);
ARRAY_VISITOR_DEFAULT(UnionArray);
ARRAY_VISITOR_DEFAULT(DictionaryArray);

Status ArrayVisitor::Visit(const DecimalArray& array) {
  return Status::NotImplemented("decimal");
}

}  // namespace arrow
