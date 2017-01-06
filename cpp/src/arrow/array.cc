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

bool Array::BaseEquals(const std::shared_ptr<Array>& other) const {
  if (this == other.get()) { return true; }
  if (!other) { return false; }
  return EqualsExact(*other.get());
}

bool Array::EqualsExact(const Array& other) const {
  if (this == &other) { return true; }
  if (length_ != other.length_ || null_count_ != other.null_count_ ||
      type_enum() != other.type_enum()) {
    return false;
  }
  if (null_count_ > 0) {
    return null_bitmap_->Equals(*other.null_bitmap_, BitUtil::BytesForBits(length_));
  }
  return true;
}

bool Array::ApproxEquals(const std::shared_ptr<Array>& arr) const {
  return Equals(arr);
}

Status Array::Validate() const {
  return Status::OK();
}

bool NullArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (Type::NA != arr->type_enum()) { return false; }
  return arr->length() == length_;
}

bool NullArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_index,
    const std::shared_ptr<Array>& arr) const {
  if (!arr) { return false; }
  if (Type::NA != arr->type_enum()) { return false; }
  return true;
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

bool PrimitiveArray::EqualsExact(const PrimitiveArray& other) const {
  if (!Array::EqualsExact(other)) { return false; }

  if (null_count_ > 0) {
    const uint8_t* this_data = raw_data_;
    const uint8_t* other_data = other.raw_data_;

    auto size_meta = dynamic_cast<const FixedWidthType*>(type_.get());
    int value_byte_size = size_meta->bit_width() / 8;
    DCHECK_GT(value_byte_size, 0);

    for (int i = 0; i < length_; ++i) {
      if (!IsNull(i) && memcmp(this_data, other_data, value_byte_size)) { return false; }
      this_data += value_byte_size;
      other_data += value_byte_size;
    }
    return true;
  } else {
    if (length_ == 0 && other.length_ == 0) { return true; }
    return data_->Equals(*other.data_, length_);
  }
}

bool PrimitiveArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(static_cast<const PrimitiveArray&>(*arr.get()));
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

bool BooleanArray::EqualsExact(const BooleanArray& other) const {
  if (this == &other) return true;
  if (null_count_ != other.null_count_) { return false; }

  if (null_count_ > 0) {
    bool equal_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, BitUtil::BytesForBits(length_));
    if (!equal_bitmap) { return false; }

    const uint8_t* this_data = raw_data_;
    const uint8_t* other_data = other.raw_data_;

    for (int i = 0; i < length_; ++i) {
      if (!IsNull(i) && BitUtil::GetBit(this_data, i) != BitUtil::GetBit(other_data, i)) {
        return false;
      }
    }
    return true;
  } else {
    return data_->Equals(*other.data_, BitUtil::BytesForBits(length_));
  }
}

bool BooleanArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) return true;
  if (Type::BOOL != arr->type_enum()) { return false; }
  return EqualsExact(static_cast<const BooleanArray&>(*arr.get()));
}

bool BooleanArray::RangeEquals(int32_t start_idx, int32_t end_idx,
    int32_t other_start_idx, const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  const auto other = static_cast<BooleanArray*>(arr.get());
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    const bool is_null = IsNull(i);
    if (is_null != arr->IsNull(o_i) || (!is_null && Value(i) != other->Value(o_i))) {
      return false;
    }
  }
  return true;
}

Status BooleanArray::Accept(ArrayVisitor* visitor) const {
  return visitor->Visit(*this);
}

// ----------------------------------------------------------------------
// ListArray

bool ListArray::EqualsExact(const ListArray& other) const {
  if (this == &other) { return true; }
  if (null_count_ != other.null_count_) { return false; }

  bool equal_offsets =
      offsets_buffer_->Equals(*other.offsets_buffer_, (length_ + 1) * sizeof(int32_t));
  if (!equal_offsets) { return false; }
  bool equal_null_bitmap = true;
  if (null_count_ > 0) {
    equal_null_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, BitUtil::BytesForBits(length_));
  }

  if (!equal_null_bitmap) { return false; }

  return values()->Equals(other.values());
}

bool ListArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(static_cast<const ListArray&>(*arr.get()));
}

bool ListArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  const auto other = static_cast<ListArray*>(arr.get());
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    const bool is_null = IsNull(i);
    if (is_null != arr->IsNull(o_i)) { return false; }
    if (is_null) continue;
    const int32_t begin_offset = offset(i);
    const int32_t end_offset = offset(i + 1);
    const int32_t other_begin_offset = other->offset(o_i);
    const int32_t other_end_offset = other->offset(o_i + 1);
    // Underlying can't be equal if the size isn't equal
    if (end_offset - begin_offset != other_end_offset - other_begin_offset) {
      return false;
    }
    if (!values_->RangeEquals(
            begin_offset, end_offset, other_begin_offset, other->values())) {
      return false;
    }
  }
  return true;
}

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

bool BinaryArray::EqualsExact(const BinaryArray& other) const {
  if (!Array::EqualsExact(other)) { return false; }

  bool equal_offsets =
      offsets_buffer_->Equals(*other.offsets_buffer_, (length_ + 1) * sizeof(int32_t));
  if (!equal_offsets) { return false; }

  if (!data_buffer_ && !(other.data_buffer_)) { return true; }

  return data_buffer_->Equals(*other.data_buffer_, data_buffer_->size());
}

bool BinaryArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(static_cast<const BinaryArray&>(*arr.get()));
}

bool BinaryArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  const auto other = static_cast<const BinaryArray*>(arr.get());
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    const bool is_null = IsNull(i);
    if (is_null != arr->IsNull(o_i)) { return false; }
    if (is_null) continue;
    const int32_t begin_offset = offset(i);
    const int32_t end_offset = offset(i + 1);
    const int32_t other_begin_offset = other->offset(o_i);
    const int32_t other_end_offset = other->offset(o_i + 1);
    // Underlying can't be equal if the size isn't equal
    if (end_offset - begin_offset != other_end_offset - other_begin_offset) {
      return false;
    }

    if (std::memcmp(data_ + begin_offset, other->data_ + other_begin_offset,
            end_offset - begin_offset)) {
      return false;
    }
  }
  return true;
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

bool StructArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  if (null_count_ != arr->null_count()) { return false; }
  return RangeEquals(0, length_, 0, arr);
}

bool StructArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (Type::STRUCT != arr->type_enum()) { return false; }
  const auto& other = static_cast<const StructArray&>(*arr.get());

  bool equal_fields = true;
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    if (IsNull(i) != arr->IsNull(o_i)) { return false; }
    if (IsNull(i)) continue;
    for (size_t j = 0; j < field_arrays_.size(); ++j) {
      // TODO: really we should be comparing stretches of non-null data rather
      // than looking at one value at a time.
      equal_fields = field(j)->RangeEquals(i, i + 1, o_i, other.field(j));
      if (!equal_fields) { return false; }
    }
  }

  return true;
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

bool UnionArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (!this->type_->Equals(arr->type())) { return false; }
  if (null_count_ != arr->null_count()) { return false; }
  return RangeEquals(0, length_, 0, arr);
}

bool UnionArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (Type::UNION != arr->type_enum()) { return false; }
  const auto& other = static_cast<const UnionArray&>(*arr.get());

  const UnionMode union_mode = mode();
  if (union_mode != other.mode()) { return false; }

  // Define a mapping from the type id to child number
  const auto& type_codes = static_cast<const UnionType&>(*arr->type().get()).type_ids;
  uint8_t max_code = 0;
  for (uint8_t code : type_codes) {
    if (code > max_code) { max_code = code; }
  }

  // Store mapping in a vector for constant time lookups
  std::vector<uint8_t> type_id_to_child_num(max_code + 1);
  for (uint8_t i = 0; i < static_cast<uint8_t>(type_codes.size()); ++i) {
    type_id_to_child_num[type_codes[i]] = i;
  }

  const uint8_t* this_ids = raw_type_ids();
  const uint8_t* other_ids = other.raw_type_ids();

  uint8_t id, child_num;
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    if (IsNull(i) != other.IsNull(o_i)) { return false; }
    if (IsNull(i)) continue;
    if (this_ids[i] != other_ids[o_i]) { return false; }

    id = this_ids[i];
    child_num = type_id_to_child_num[id];

    // TODO(wesm): really we should be comparing stretches of non-null data
    // rather than looking at one value at a time.
    if (union_mode == UnionMode::SPARSE) {
      if (!child(child_num)->RangeEquals(i, i + 1, o_i, other.child(child_num))) {
        return false;
      }
    } else {
      const int32_t offset = offsets_[i];
      const int32_t o_offset = other.offsets_[i];
      if (!child(child_num)->RangeEquals(
              offset, offset + 1, o_offset, other.child(child_num))) {
        return false;
      }
    }
  }
  return true;
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

bool DictionaryArray::EqualsExact(const DictionaryArray& other) const {
  if (!dictionary()->Equals(other.dictionary())) { return false; }
  return indices_->Equals(other.indices());
}

bool DictionaryArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (Type::DICTIONARY != arr->type_enum()) { return false; }
  return EqualsExact(static_cast<const DictionaryArray&>(*arr.get()));
}

bool DictionaryArray::RangeEquals(int32_t start_idx, int32_t end_idx,
    int32_t other_start_idx, const std::shared_ptr<Array>& arr) const {
  if (Type::DICTIONARY != arr->type_enum()) { return false; }
  const auto& dict_other = static_cast<const DictionaryArray&>(*arr.get());
  if (!dictionary()->Equals(dict_other.dictionary())) { return false; }
  return indices_->RangeEquals(start_idx, end_idx, other_start_idx, dict_other.indices());
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
