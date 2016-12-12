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

#ifndef ARROW_ARRAY_H
#define ARROW_ARRAY_H

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;
class MutableBuffer;
class Status;

// Immutable data array with some logical type and some length. Any memory is
// owned by the respective Buffer instance (or its parents).
//
// The base class is only required to have a null bitmap buffer if the null
// count is greater than 0
class ARROW_EXPORT Array {
 public:
  Array(const std::shared_ptr<DataType>& type, int32_t length, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  virtual ~Array() = default;

  // Determine if a slot is null. For inner loops. Does *not* boundscheck
  bool IsNull(int i) const {
    return null_count_ > 0 && BitUtil::BitNotSet(null_bitmap_data_, i);
  }

  int32_t length() const { return length_; }
  int32_t null_count() const { return null_count_; }

  std::shared_ptr<DataType> type() const { return type_; }
  Type::type type_enum() const { return type_->type; }

  std::shared_ptr<Buffer> null_bitmap() const { return null_bitmap_; }

  const uint8_t* null_bitmap_data() const { return null_bitmap_data_; }

  bool EqualsExact(const Array& arr) const;
  virtual bool Equals(const std::shared_ptr<Array>& arr) const = 0;
  virtual bool ApproxEquals(const std::shared_ptr<Array>& arr) const;

  // Compare if the range of slots specified are equal for the given array and
  // this array.  end_idx exclusive.  This methods does not bounds check.
  virtual bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& arr) const = 0;

  // Determines if the array is internally consistent.  Defaults to always
  // returning Status::OK.  This can be an expensive check.
  virtual Status Validate() const;

  virtual Status Accept(ArrayVisitor* visitor) const = 0;

 protected:
  std::shared_ptr<DataType> type_;
  int32_t null_count_;
  int32_t length_;

  std::shared_ptr<Buffer> null_bitmap_;
  const uint8_t* null_bitmap_data_;

 private:
  Array() {}
  DISALLOW_COPY_AND_ASSIGN(Array);
};

// Degenerate null type Array
class ARROW_EXPORT NullArray : public Array {
 public:
  using TypeClass = NullType;

  NullArray(const std::shared_ptr<DataType>& type, int32_t length)
      : Array(type, length, length, nullptr) {}

  explicit NullArray(int32_t length) : NullArray(std::make_shared<NullType>(), length) {}

  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_index,
      const std::shared_ptr<Array>& arr) const override;

  Status Accept(ArrayVisitor* visitor) const override;
};

typedef std::shared_ptr<Array> ArrayPtr;

Status ARROW_EXPORT GetEmptyBitmap(
    MemoryPool* pool, int32_t length, std::shared_ptr<MutableBuffer>* result);

// Base class for fixed-size logical types
class ARROW_EXPORT PrimitiveArray : public Array {
 public:
  virtual ~PrimitiveArray() {}

  std::shared_ptr<Buffer> data() const { return data_; }

  bool EqualsExact(const PrimitiveArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

 protected:
  PrimitiveArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

template <typename TYPE>
class ARROW_EXPORT NumericArray : public PrimitiveArray {
 public:
  using TypeClass = TYPE;
  using value_type = typename TypeClass::c_type;
  NumericArray(int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr)
      : PrimitiveArray(
            std::make_shared<TypeClass>(), length, data, null_count, null_bitmap) {}
  NumericArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr)
      : PrimitiveArray(type, length, data, null_count, null_bitmap) {}

  bool EqualsExact(const NumericArray<TypeClass>& other) const {
    return PrimitiveArray::EqualsExact(static_cast<const PrimitiveArray&>(other));
  }

  bool ApproxEquals(const std::shared_ptr<Array>& arr) const { return Equals(arr); }

  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override {
    if (this == arr.get()) { return true; }
    if (!arr) { return false; }
    if (this->type_enum() != arr->type_enum()) { return false; }
    const auto other = static_cast<NumericArray<TypeClass>*>(arr.get());
    for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
      const bool is_null = IsNull(i);
      if (is_null != arr->IsNull(o_i) || (!is_null && Value(i) != other->Value(o_i))) {
        return false;
      }
    }
    return true;
  }
  const value_type* raw_data() const {
    return reinterpret_cast<const value_type*>(raw_data_);
  }

  Status Accept(ArrayVisitor* visitor) const override;

  value_type Value(int i) const { return raw_data()[i]; }
};

template <>
inline bool NumericArray<FloatType>::ApproxEquals(
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }

  const auto& other = *static_cast<NumericArray<FloatType>*>(arr.get());

  if (this == &other) { return true; }
  if (null_count_ != other.null_count_) { return false; }

  auto this_data = reinterpret_cast<const float*>(raw_data_);
  auto other_data = reinterpret_cast<const float*>(other.raw_data_);

  static constexpr float EPSILON = 1E-5;

  if (length_ == 0 && other.length_ == 0) { return true; }

  if (null_count_ > 0) {
    bool equal_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, BitUtil::CeilByte(length_) / 8);
    if (!equal_bitmap) { return false; }

    for (int i = 0; i < length_; ++i) {
      if (IsNull(i)) continue;
      if (fabs(this_data[i] - other_data[i]) > EPSILON) { return false; }
    }
  } else {
    for (int i = 0; i < length_; ++i) {
      if (fabs(this_data[i] - other_data[i]) > EPSILON) { return false; }
    }
  }
  return true;
}

template <>
inline bool NumericArray<DoubleType>::ApproxEquals(
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }

  const auto& other = *static_cast<NumericArray<DoubleType>*>(arr.get());

  if (this == &other) { return true; }
  if (null_count_ != other.null_count_) { return false; }

  auto this_data = reinterpret_cast<const double*>(raw_data_);
  auto other_data = reinterpret_cast<const double*>(other.raw_data_);

  if (length_ == 0 && other.length_ == 0) { return true; }

  static constexpr double EPSILON = 1E-5;

  if (null_count_ > 0) {
    bool equal_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, BitUtil::CeilByte(length_) / 8);
    if (!equal_bitmap) { return false; }

    for (int i = 0; i < length_; ++i) {
      if (IsNull(i)) continue;
      if (fabs(this_data[i] - other_data[i]) > EPSILON) { return false; }
    }
  } else {
    for (int i = 0; i < length_; ++i) {
      if (fabs(this_data[i] - other_data[i]) > EPSILON) { return false; }
    }
  }
  return true;
}

class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  using TypeClass = BooleanType;

  BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);
  BooleanArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  bool EqualsExact(const BooleanArray& other) const;
  bool Equals(const ArrayPtr& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

  Status Accept(ArrayVisitor* visitor) const override;

  const uint8_t* raw_data() const { return reinterpret_cast<const uint8_t*>(raw_data_); }

  bool Value(int i) const { return BitUtil::GetBit(raw_data(), i); }
};

// ----------------------------------------------------------------------
// ListArray

class ARROW_EXPORT ListArray : public Array {
 public:
  using TypeClass = ListType;

  ListArray(const TypePtr& type, int32_t length, std::shared_ptr<Buffer> offsets,
      const ArrayPtr& values, int32_t null_count = 0,
      std::shared_ptr<Buffer> null_bitmap = nullptr)
      : Array(type, length, null_count, null_bitmap) {
    offset_buffer_ = offsets;
    offsets_ = offsets == nullptr ? nullptr : reinterpret_cast<const int32_t*>(
                                                  offset_buffer_->data());
    values_ = values;
  }

  Status Validate() const override;

  virtual ~ListArray() = default;

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> values() const { return values_; }
  std::shared_ptr<Buffer> offsets() const {
    return std::static_pointer_cast<Buffer>(offset_buffer_);
  }

  std::shared_ptr<DataType> value_type() const { return values_->type(); }

  const int32_t* raw_offsets() const { return offsets_; }

  int32_t offset(int i) const { return offsets_[i]; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return offsets_[i]; }
  int32_t value_length(int i) const { return offsets_[i + 1] - offsets_[i]; }

  bool EqualsExact(const ListArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

  Status Accept(ArrayVisitor* visitor) const override;

 protected:
  std::shared_ptr<Buffer> offset_buffer_;
  const int32_t* offsets_;
  ArrayPtr values_;
};

// ----------------------------------------------------------------------
// Binary and String

class ARROW_EXPORT BinaryArray : public Array {
 public:
  using TypeClass = BinaryType;

  BinaryArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Constructor that allows sub-classes/builders to propagate there logical type up the
  // class hierarchy.
  BinaryArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Return the pointer to the given elements bytes
  // TODO(emkornfield) introduce a StringPiece or something similar to capture zero-copy
  // pointer + offset
  const uint8_t* GetValue(int i, int32_t* out_length) const {
    const int32_t pos = offsets_[i];
    *out_length = offsets_[i + 1] - pos;
    return data_ + pos;
  }

  std::shared_ptr<Buffer> data() const { return data_buffer_; }
  std::shared_ptr<Buffer> offsets() const { return offset_buffer_; }

  const int32_t* raw_offsets() const { return offsets_; }

  int32_t offset(int i) const { return offsets_[i]; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return offsets_[i]; }
  int32_t value_length(int i) const { return offsets_[i + 1] - offsets_[i]; }

  bool EqualsExact(const BinaryArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

  Status Validate() const override;

  Status Accept(ArrayVisitor* visitor) const override;

 private:
  std::shared_ptr<Buffer> offset_buffer_;
  const int32_t* offsets_;

  std::shared_ptr<Buffer> data_buffer_;
  const uint8_t* data_;
};

class ARROW_EXPORT StringArray : public BinaryArray {
 public:
  using TypeClass = StringType;

  StringArray(int32_t length, const std::shared_ptr<Buffer>& offsets,
      const std::shared_ptr<Buffer>& data, int32_t null_count = 0,
      const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  // Construct a std::string
  // TODO: std::bad_alloc possibility
  std::string GetString(int i) const {
    int32_t nchars;
    const uint8_t* str = GetValue(i, &nchars);
    return std::string(reinterpret_cast<const char*>(str), nchars);
  }

  Status Validate() const override;

  Status Accept(ArrayVisitor* visitor) const override;
};

// ----------------------------------------------------------------------
// Struct

class ARROW_EXPORT StructArray : public Array {
 public:
  using TypeClass = StructType;

  StructArray(const TypePtr& type, int32_t length, std::vector<ArrayPtr>& field_arrays,
      int32_t null_count = 0, std::shared_ptr<Buffer> null_bitmap = nullptr)
      : Array(type, length, null_count, null_bitmap) {
    type_ = type;
    field_arrays_ = field_arrays;
  }

  Status Validate() const override;

  virtual ~StructArray() {}

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  std::shared_ptr<Array> field(int32_t pos) const;

  const std::vector<ArrayPtr>& fields() const { return field_arrays_; }

  bool EqualsExact(const StructArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const std::shared_ptr<Array>& arr) const override;

  Status Accept(ArrayVisitor* visitor) const override;

 protected:
  // The child arrays corresponding to each field of the struct data type.
  std::vector<ArrayPtr> field_arrays_;
};

// ----------------------------------------------------------------------
// Union

class UnionArray : public Array {
 protected:
  // The data are types encoded as int16
  Buffer* types_;
  std::vector<std::shared_ptr<Array>> children_;
};

class DenseUnionArray : public UnionArray {
 protected:
  Buffer* offset_buf_;
};

class SparseUnionArray : public UnionArray {};

// ----------------------------------------------------------------------
// extern templates and other details

// gcc and clang disagree about how to handle template visibility when you have
// explicit specializations https://llvm.org/bugs/show_bug.cgi?id=24815
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif

// Only instantiate these templates once
extern template class ARROW_EXPORT NumericArray<Int8Type>;
extern template class ARROW_EXPORT NumericArray<UInt8Type>;
extern template class ARROW_EXPORT NumericArray<Int16Type>;
extern template class ARROW_EXPORT NumericArray<UInt16Type>;
extern template class ARROW_EXPORT NumericArray<Int32Type>;
extern template class ARROW_EXPORT NumericArray<UInt32Type>;
extern template class ARROW_EXPORT NumericArray<Int64Type>;
extern template class ARROW_EXPORT NumericArray<UInt64Type>;
extern template class ARROW_EXPORT NumericArray<HalfFloatType>;
extern template class ARROW_EXPORT NumericArray<FloatType>;
extern template class ARROW_EXPORT NumericArray<DoubleType>;
extern template class ARROW_EXPORT NumericArray<TimestampType>;

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

// ----------------------------------------------------------------------
// Helper functions

// Create new arrays for logical types that are backed by primitive arrays.
Status ARROW_EXPORT MakePrimitiveArray(const std::shared_ptr<DataType>& type,
    int32_t length, const std::shared_ptr<Buffer>& data, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap, std::shared_ptr<Array>* out);

}  // namespace arrow

#endif
