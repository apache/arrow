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

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
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

  const std::shared_ptr<Buffer>& data() const { return data_; }

  bool EqualsExact(const PrimitiveArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

 protected:
  PrimitiveArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

template <class TypeClass>
class ARROW_EXPORT NumericArray : public PrimitiveArray {
 public:
  using value_type = typename TypeClass::c_type;
  NumericArray(int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr)
      : PrimitiveArray(
            std::make_shared<TypeClass>(), length, data, null_count, null_bitmap) {}
  NumericArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr)
      : PrimitiveArray(type, length, data, null_count, null_bitmap) {}

  bool EqualsExact(const NumericArray<TypeClass>& other) const {
    return PrimitiveArray::EqualsExact(*static_cast<const PrimitiveArray*>(&other));
  }

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

  value_type Value(int i) const { return raw_data()[i]; }
};

#define NUMERIC_ARRAY_DECL(NAME, TypeClass) \
  using NAME = NumericArray<TypeClass>;     \
  extern template class ARROW_EXPORT NumericArray<TypeClass>;

NUMERIC_ARRAY_DECL(UInt8Array, UInt8Type);
NUMERIC_ARRAY_DECL(Int8Array, Int8Type);
NUMERIC_ARRAY_DECL(UInt16Array, UInt16Type);
NUMERIC_ARRAY_DECL(Int16Array, Int16Type);
NUMERIC_ARRAY_DECL(UInt32Array, UInt32Type);
NUMERIC_ARRAY_DECL(Int32Array, Int32Type);
NUMERIC_ARRAY_DECL(UInt64Array, UInt64Type);
NUMERIC_ARRAY_DECL(Int64Array, Int64Type);
NUMERIC_ARRAY_DECL(TimestampArray, TimestampType);
NUMERIC_ARRAY_DECL(FloatArray, FloatType);
NUMERIC_ARRAY_DECL(DoubleArray, DoubleType);

template <typename Type>
class ARROW_EXPORT PrimitiveBuilder : public ArrayBuilder {
 public:
  typedef typename Type::c_type value_type;

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
    util::set_bit(null_bitmap_data_, length_);
    raw_data_[length_++] = val;
  }

 protected:
  using PrimitiveBuilder<T>::length_;
  using PrimitiveBuilder<T>::null_bitmap_data_;
  using PrimitiveBuilder<T>::raw_data_;
};

template <>
struct TypeTraits<UInt8Type> {
  typedef UInt8Array ArrayType;

  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<Int8Type> {
  typedef Int8Array ArrayType;

  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct TypeTraits<UInt16Type> {
  typedef UInt16Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint16_t); }
};

template <>
struct TypeTraits<Int16Type> {
  typedef Int16Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int16_t); }
};

template <>
struct TypeTraits<UInt32Type> {
  typedef UInt32Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint32_t); }
};

template <>
struct TypeTraits<Int32Type> {
  typedef Int32Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int32_t); }
};

template <>
struct TypeTraits<UInt64Type> {
  typedef UInt64Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint64_t); }
};

template <>
struct TypeTraits<Int64Type> {
  typedef Int64Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};

template <>
struct TypeTraits<TimestampType> {
  typedef TimestampArray ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};
template <>

struct TypeTraits<FloatType> {
  typedef FloatArray ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(float); }
};

template <>
struct TypeTraits<DoubleType> {
  typedef DoubleArray ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(double); }
};

// Builders

typedef NumericBuilder<UInt8Type> UInt8Builder;
typedef NumericBuilder<UInt16Type> UInt16Builder;
typedef NumericBuilder<UInt32Type> UInt32Builder;
typedef NumericBuilder<UInt64Type> UInt64Builder;

typedef NumericBuilder<Int8Type> Int8Builder;
typedef NumericBuilder<Int16Type> Int16Builder;
typedef NumericBuilder<Int32Type> Int32Builder;
typedef NumericBuilder<Int64Type> Int64Builder;
typedef NumericBuilder<TimestampType> TimestampBuilder;

typedef NumericBuilder<FloatType> FloatBuilder;
typedef NumericBuilder<DoubleType> DoubleBuilder;

class ARROW_EXPORT BooleanArray : public PrimitiveArray {
 public:
  BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);
  BooleanArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  bool EqualsExact(const BooleanArray& other) const;
  bool Equals(const ArrayPtr& arr) const override;
  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

  const uint8_t* raw_data() const { return reinterpret_cast<const uint8_t*>(raw_data_); }

  bool Value(int i) const { return util::get_bit(raw_data(), i); }
};

template <>
struct TypeTraits<BooleanType> {
  typedef BooleanArray ArrayType;

  static inline int bytes_required(int elements) {
    return util::bytes_for_bits(elements);
  }
};

class ARROW_EXPORT BooleanBuilder : public PrimitiveBuilder<BooleanType> {
 public:
  explicit BooleanBuilder(MemoryPool* pool, const TypePtr& type)
      : PrimitiveBuilder<BooleanType>(pool, type) {}

  virtual ~BooleanBuilder() {}

  using PrimitiveBuilder<BooleanType>::Append;

  // Scalar append
  Status Append(bool val) {
    Reserve(1);
    util::set_bit(null_bitmap_data_, length_);
    if (val) {
      util::set_bit(raw_data_, length_);
    } else {
      util::clear_bit(raw_data_, length_);
    }
    ++length_;
    return Status::OK();
  }

  Status Append(uint8_t val) { return Append(static_cast<bool>(val)); }
};

}  // namespace arrow

#endif  // ARROW_TYPES_PRIMITIVE_H
