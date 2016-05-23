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

#include "arrow/types/primitive.h"

#include <memory>

#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const TypePtr& type, int32_t length,
    const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap)
    : Array(type, length, null_count, null_bitmap) {
  data_ = data;
  raw_data_ = data == nullptr ? nullptr : data_->data();
}

bool PrimitiveArray::EqualsExact(const PrimitiveArray& other) const {
  if (this == &other) { return true; }
  if (null_count_ != other.null_count_) { return false; }

  if (null_count_ > 0) {
    bool equal_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, util::ceil_byte(length_) / 8);
    if (!equal_bitmap) { return false; }

    const uint8_t* this_data = raw_data_;
    const uint8_t* other_data = other.raw_data_;

    int value_size = type_->value_size();
    DCHECK_GT(value_size, 0);

    for (int i = 0; i < length_; ++i) {
      if (!IsNull(i) && memcmp(this_data, other_data, value_size)) { return false; }
      this_data += value_size;
      other_data += value_size;
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
  return EqualsExact(*static_cast<const PrimitiveArray*>(arr.get()));
}

template <typename T>
Status PrimitiveBuilder<T>::Init(int32_t capacity) {
  RETURN_NOT_OK(ArrayBuilder::Init(capacity));
  data_ = std::make_shared<PoolBuffer>(pool_);

  int64_t nbytes = type_traits<T>::bytes_required(capacity);
  RETURN_NOT_OK(data_->Resize(nbytes));
  // TODO(emkornfield) valgrind complains without this
  memset(data_->mutable_data(), 0, nbytes);

  raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Resize(int32_t capacity) {
  // XXX: Set floor size for now
  if (capacity < MIN_BUILDER_CAPACITY) { capacity = MIN_BUILDER_CAPACITY; }

  if (capacity_ == 0) {
    RETURN_NOT_OK(Init(capacity));
  } else {
    RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
    const int64_t old_bytes = data_->size();
    const int64_t new_bytes = type_traits<T>::bytes_required(capacity);
    RETURN_NOT_OK(data_->Resize(new_bytes));
    raw_data_ = reinterpret_cast<value_type*>(data_->mutable_data());

    memset(data_->mutable_data() + old_bytes, 0, new_bytes - old_bytes);
  }
  return Status::OK();
}

template <typename T>
Status PrimitiveBuilder<T>::Append(
    const value_type* values, int32_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  if (length > 0) {
    memcpy(raw_data_ + length_, values, type_traits<T>::bytes_required(length));
  }

  // length_ is update by these
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);

  return Status::OK();
}

template <typename T>
std::shared_ptr<Array> PrimitiveBuilder<T>::Finish() {
  std::shared_ptr<Array> result = std::make_shared<typename type_traits<T>::ArrayType>(
      type_, length_, data_, null_count_, null_bitmap_);

  data_ = null_bitmap_ = nullptr;
  capacity_ = length_ = null_count_ = 0;
  return result;
}

template <>
Status PrimitiveBuilder<BooleanType>::Append(
    const uint8_t* values, int32_t length, const uint8_t* valid_bytes) {
  RETURN_NOT_OK(Reserve(length));

  for (int i = 0; i < length; ++i) {
    if (values[i] > 0) {
      util::set_bit(raw_data_, length_ + i);
    } else {
      util::clear_bit(raw_data_, length_ + i);
    }
  }

  // this updates length_
  ArrayBuilder::UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

template class PrimitiveBuilder<UInt8Type>;
template class PrimitiveBuilder<UInt16Type>;
template class PrimitiveBuilder<UInt32Type>;
template class PrimitiveBuilder<UInt64Type>;
template class PrimitiveBuilder<Int8Type>;
template class PrimitiveBuilder<Int16Type>;
template class PrimitiveBuilder<Int32Type>;
template class PrimitiveBuilder<Int64Type>;
template class PrimitiveBuilder<FloatType>;
template class PrimitiveBuilder<DoubleType>;
template class PrimitiveBuilder<BooleanType>;

BooleanArray::BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
    int32_t null_count, const std::shared_ptr<Buffer>& null_bitmap)
    : PrimitiveArray(
          std::make_shared<BooleanType>(), length, data, null_count, null_bitmap) {}

bool BooleanArray::EqualsExact(const BooleanArray& other) const {
  if (this == &other) return true;
  if (null_count_ != other.null_count_) { return false; }

  if (null_count_ > 0) {
    bool equal_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, util::bytes_for_bits(length_));
    if (!equal_bitmap) { return false; }

    const uint8_t* this_data = raw_data_;
    const uint8_t* other_data = other.raw_data_;

    for (int i = 0; i < length_; ++i) {
      if (!IsNull(i) && util::get_bit(this_data, i) != util::get_bit(other_data, i)) {
        return false;
      }
    }
    return true;
  } else {
    return data_->Equals(*other.data_, util::bytes_for_bits(length_));
  }
}

bool BooleanArray::Equals(const ArrayPtr& arr) const {
  if (this == arr.get()) return true;
  if (Type::BOOL != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const BooleanArray*>(arr.get()));
}

bool BooleanArray::RangeEquals(int32_t start_idx, int32_t end_idx,
    int32_t other_start_idx, const ArrayPtr& arr) const {
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

}  // namespace arrow
