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

Status GetEmptyBitmap(
    MemoryPool* pool, int32_t length, std::shared_ptr<MutableBuffer>* result) {
  auto buffer = std::make_shared<PoolBuffer>(pool);
  RETURN_NOT_OK(buffer->Resize(BitUtil::BytesForBits(length)));
  memset(buffer->mutable_data(), 0, buffer->size());

  *result = buffer;
  return Status::OK();
}

// ----------------------------------------------------------------------
// Base array class

Array::Array(const std::shared_ptr<DataType>& type, int32_t length, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap) {
  type_ = type;
  length_ = length;
  null_count_ = null_count;
  null_bitmap_ = null_bitmap;
  if (null_bitmap_) { null_bitmap_data_ = null_bitmap_->data(); }
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
    const std::shared_ptr<Array>& arr) const {
  if (!arr) { return false; }
  bool are_equal = false;
  Status error =
      ArrayRangeEquals(*this, *arr, start_idx, end_idx, other_start_idx, &are_equal);
  if (!error.ok()) { DCHECK(false) << "Arrays not comparable: " << error.ToString(); }
  return are_equal;
}

Status Array::Validate() const {
  return Status::OK();
}

Status NullArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : Array(type, length, null_count, null_bitmap) {
  data_ = data;
  raw_data_ = data == nullptr ? nullptr : data_->data();
}

template <typename T>
Status NumericArray<T>::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
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
    int32_t null_count, const std::shared_ptr<Buffer>& null_bitmap)
    : PrimitiveArray(
          std::make_shared<BooleanType>(), length, data, null_count, null_bitmap) {}

BooleanArray::BooleanArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : PrimitiveArray(type, length, data, null_count, null_bitmap) {}

Status BooleanArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// ----------------------------------------------------------------------
// ListArray

Status ListArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }
  if (!offsets_buffer_) { return Status::Invalid("offsets_buffer_ was null"); }
  if (offsets_buffer_->size() / static_cast<int>(sizeof(int32_t)) < length_) {
    std::stringstream ss;
    ss << "offset buffer size (bytes): " << offsets_buffer_->size()
       << " isn't large enough for length: " << length_;
    return Status::Invalid(ss.str());
  }
  const int32_t last_offset = offset(length_);
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

  int32_t prev_offset = offset(0);
  if (prev_offset != 0) { return Status::Invalid("The first offset wasn't zero"); }
  for (int32_t i = 1; i <= length_; ++i) {
    int32_t current_offset = offset(i);
    if (IsNull(i - 1) && current_offset != prev_offset) {
      std::stringstream ss;
      ss << "Offset invariant failure at: " << i << " inconsistent offsets for null slot"
         << current_offset << "!=" << prev_offset;
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

// ----------------------------------------------------------------------
// String and binary

static std::shared_ptr<DataType> kBinary = std::make_shared<BinaryType>();
static std::shared_ptr<DataType> kString = std::make_shared<StringType>();

BinaryArray::BinaryArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : BinaryArray(kBinary, length, offsets, data, null_count, null_bitmap) {}

BinaryArray::BinaryArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& offsets, const std::shared_ptr<Buffer>& data,
    int32_t null_count, const std::shared_ptr<Buffer>& null_bitmap)
    : Array(type, length, null_count, null_bitmap),
      offsets_buffer_(offsets),
      offsets_(reinterpret_cast<const int32_t*>(offsets_buffer_->data())),
      data_buffer_(data),
      data_(nullptr) {
  if (data_buffer_ != nullptr) { data_ = data_buffer_->data(); }
}

Status BinaryArray::Validate() const {
  // TODO(wesm): what to do here?
  return Status::OK();
}

Status BinaryArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

StringArray::StringArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : BinaryArray(kString, length, offsets, data, null_count, null_bitmap) {}

Status StringArray::Validate() const {
  // TODO(emkornfield) Validate proper UTF8 code points?
  return BinaryArray::Validate();
}

Status StringArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// ----------------------------------------------------------------------
// Struct

std::shared_ptr<Array> StructArray::field(int32_t pos) const {
  DCHECK_GT(field_arrays_.size(), 0);
  return field_arrays_[pos];
}

Status StructArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }

  if (null_count() > length_) {
    return Status::Invalid("Null count exceeds the length of this struct");
  }

  if (field_arrays_.size() > 0) {
    // Validate fields
    int32_t array_length = field_arrays_[0]->length();
    size_t idx = 0;
    for (auto it : field_arrays_) {
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

// ----------------------------------------------------------------------
// UnionArray

UnionArray::UnionArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::vector<std::shared_ptr<Array>>& children,
    const std::shared_ptr<Buffer>& type_ids, const std::shared_ptr<Buffer>& offsets,
    int32_t null_count, const std::shared_ptr<Buffer>& null_bitmap)
    : Array(type, length, null_count, null_bitmap),
      children_(children),
      type_ids_buffer_(type_ids),
      offsets_buffer_(offsets) {
  type_ids_ = reinterpret_cast<const uint8_t*>(type_ids->data());
  if (offsets) { offsets_ = reinterpret_cast<const int32_t*>(offsets->data()); }
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

// ----------------------------------------------------------------------
// DictionaryArray

Status DictionaryArray::FromBuffer(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& indices, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap, std::shared_ptr<DictionaryArray>* out) {
  DCHECK_EQ(type->type, Type::DICTIONARY);
  const auto& dict_type = static_cast<const DictionaryType*>(type.get());

  std::shared_ptr<Array> boxed_indices;
  RETURN_NOT_OK(MakePrimitiveArray(
      dict_type->index_type(), length, indices, null_count, null_bitmap, &boxed_indices));

  *out = std::make_shared<DictionaryArray>(type, boxed_indices);
  return Status::OK();
}

DictionaryArray::DictionaryArray(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& indices)
    : Array(type, indices->length(), indices->null_count(), indices->null_bitmap()),
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

// ----------------------------------------------------------------------

#define MAKE_PRIMITIVE_ARRAY_CASE(ENUM, ArrayType)                          \
  case Type::ENUM:                                                          \
    out->reset(new ArrayType(type, length, data, null_count, null_bitmap)); \
    break;

Status MakePrimitiveArray(const std::shared_ptr<DataType>& type, int32_t length,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap, std::shared_ptr<Array>* out) {
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

}  // namespace arrow
