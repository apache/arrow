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

#ifndef ARROW_TYPES_PRIMITIVE_H
#define ARROW_TYPES_PRIMITIVE_H

#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/types/datetime.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

// Base class for fixed-size logical types.  See MakePrimitiveArray
// (types/construct.h) for constructing a specific subclass.
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

template <typename Type>
class ARROW_EXPORT PrimitiveBuilder : public ArrayBuilder {
 public:
  using value_type = typename Type::c_type;

  explicit PrimitiveBuilder(MemoryPool* pool, const TypePtr& type)
      : ArrayBuilder(pool, type), data_(nullptr) {}

  virtual ~PrimitiveBuilder() {}

  using ArrayBuilder::Advance;

  // Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int32_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  std::shared_ptr<Buffer> data() const { return data_; }

  // Vector append
  //
  // If passed, valid_bytes is of equal length to values, and any zero byte
  // will be considered as a null for that slot
  Status Append(
      const value_type* values, int32_t length, const uint8_t* valid_bytes = nullptr);

  Status Finish(std::shared_ptr<Array>* out) override;
  Status Init(int32_t capacity) override;

  // Increase the capacity of the builder to accommodate at least the indicated
  // number of elements
  Status Resize(int32_t capacity) override;

 protected:
  std::shared_ptr<PoolBuffer> data_;
  value_type* raw_data_;
};

template <typename T>
class ARROW_EXPORT NumericBuilder : public PrimitiveBuilder<T> {
 public:
  using typename PrimitiveBuilder<T>::value_type;
  using PrimitiveBuilder<T>::PrimitiveBuilder;

  using PrimitiveBuilder<T>::Append;
  using PrimitiveBuilder<T>::Init;
  using PrimitiveBuilder<T>::Resize;
  using PrimitiveBuilder<T>::Reserve;

  // Scalar append.
  Status Append(value_type val) {
    RETURN_NOT_OK(ArrayBuilder::Reserve(1));
    UnsafeAppend(val);
    return Status::OK();
  }

  // Does not capacity-check; make sure to call Reserve beforehand
  void UnsafeAppend(value_type val) {
    BitUtil::SetBit(null_bitmap_data_, length_);
    raw_data_[length_++] = val;
  }

 protected:
  using PrimitiveBuilder<T>::length_;
  using PrimitiveBuilder<T>::null_bitmap_data_;
  using PrimitiveBuilder<T>::raw_data_;
};

// Builders

using UInt8Builder = NumericBuilder<UInt8Type>;
using UInt16Builder = NumericBuilder<UInt16Type>;
using UInt32Builder = NumericBuilder<UInt32Type>;
using UInt64Builder = NumericBuilder<UInt64Type>;

using Int8Builder = NumericBuilder<Int8Type>;
using Int16Builder = NumericBuilder<Int16Type>;
using Int32Builder = NumericBuilder<Int32Type>;
using Int64Builder = NumericBuilder<Int64Type>;
using TimestampBuilder = NumericBuilder<TimestampType>;

using HalfFloatBuilder = NumericBuilder<HalfFloatType>;
using FloatBuilder = NumericBuilder<FloatType>;
using DoubleBuilder = NumericBuilder<DoubleType>;

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

class ARROW_EXPORT BooleanBuilder : public ArrayBuilder {
 public:
  explicit BooleanBuilder(MemoryPool* pool, const TypePtr& type)
      : ArrayBuilder(pool, type), data_(nullptr) {}

  virtual ~BooleanBuilder() {}

  using ArrayBuilder::Advance;

  // Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  Status AppendNulls(const uint8_t* valid_bytes, int32_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    return Status::OK();
  }

  Status AppendNull() {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(false);
    return Status::OK();
  }

  std::shared_ptr<Buffer> data() const { return data_; }

  // Scalar append
  Status Append(bool val) {
    Reserve(1);
    BitUtil::SetBit(null_bitmap_data_, length_);
    if (val) {
      BitUtil::SetBit(raw_data_, length_);
    } else {
      BitUtil::ClearBit(raw_data_, length_);
    }
    ++length_;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, valid_bytes is of equal length to values, and any zero byte
  // will be considered as a null for that slot
  Status Append(
      const uint8_t* values, int32_t length, const uint8_t* valid_bytes = nullptr);

  Status Finish(std::shared_ptr<Array>* out) override;
  Status Init(int32_t capacity) override;

  // Increase the capacity of the builder to accommodate at least the indicated
  // number of elements
  Status Resize(int32_t capacity) override;

 protected:
  std::shared_ptr<PoolBuffer> data_;
  uint8_t* raw_data_;
};

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

}  // namespace arrow

#endif  // ARROW_TYPES_PRIMITIVE_H
