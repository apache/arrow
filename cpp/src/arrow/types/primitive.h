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
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

class MemoryPool;

// Base class for fixed-size logical types
class PrimitiveArray : public Array {
 public:
  PrimitiveArray(const TypePtr& type, int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);
  virtual ~PrimitiveArray() {}

  const std::shared_ptr<Buffer>& data() const { return data_; }

  bool EqualsExact(const PrimitiveArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

 protected:
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

#define NUMERIC_ARRAY_DECL(NAME, TypeClass, T)                                         \
  class NAME : public PrimitiveArray {                                                 \
   public:                                                                             \
    using value_type = T;                                                              \
    using PrimitiveArray::PrimitiveArray;                                              \
                                                                                       \
    NAME(int32_t length, const std::shared_ptr<Buffer>& data, int32_t null_count = 0,  \
        const std::shared_ptr<Buffer>& null_bitmap = nullptr)                          \
        : PrimitiveArray(                                                              \
              std::make_shared<TypeClass>(), length, data, null_count, null_bitmap) {} \
                                                                                       \
    bool EqualsExact(const NAME& other) const {                                        \
      return PrimitiveArray::EqualsExact(*static_cast<const PrimitiveArray*>(&other)); \
    }                                                                                  \
                                                                                       \
    const T* raw_data() const { return reinterpret_cast<const T*>(raw_data_); }        \
                                                                                       \
    T Value(int i) const { return raw_data()[i]; }                                     \
  };

NUMERIC_ARRAY_DECL(UInt8Array, UInt8Type, uint8_t);
NUMERIC_ARRAY_DECL(Int8Array, Int8Type, int8_t);
NUMERIC_ARRAY_DECL(UInt16Array, UInt16Type, uint16_t);
NUMERIC_ARRAY_DECL(Int16Array, Int16Type, int16_t);
NUMERIC_ARRAY_DECL(UInt32Array, UInt32Type, uint32_t);
NUMERIC_ARRAY_DECL(Int32Array, Int32Type, int32_t);
NUMERIC_ARRAY_DECL(UInt64Array, UInt64Type, uint64_t);
NUMERIC_ARRAY_DECL(Int64Array, Int64Type, int64_t);
NUMERIC_ARRAY_DECL(FloatArray, FloatType, float);
NUMERIC_ARRAY_DECL(DoubleArray, DoubleType, double);

template <typename Type>
class PrimitiveBuilder : public ArrayBuilder {
 public:
  typedef typename Type::c_type value_type;

  explicit PrimitiveBuilder(MemoryPool* pool, const TypePtr& type)
      : ArrayBuilder(pool, type), data_(nullptr) {}

  virtual ~PrimitiveBuilder() {}

  using ArrayBuilder::Advance;

  // Write nulls as uint8_t* (0 value indicates null) into pre-allocated memory
  void AppendNulls(const uint8_t* valid_bytes, int32_t length) {
    UnsafeAppendToBitmap(valid_bytes, length);
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

  std::shared_ptr<Array> Finish() override;

  Status Init(int32_t capacity) override;

  // Increase the capacity of the builder to accommodate at least the indicated
  // number of elements
  Status Resize(int32_t capacity) override;

 protected:
  std::shared_ptr<PoolBuffer> data_;
  value_type* raw_data_;
};

template <typename T>
class NumericBuilder : public PrimitiveBuilder<T> {
 public:
  using typename PrimitiveBuilder<T>::value_type;
  using PrimitiveBuilder<T>::PrimitiveBuilder;

  using PrimitiveBuilder<T>::Append;
  using PrimitiveBuilder<T>::Init;
  using PrimitiveBuilder<T>::Resize;
  using PrimitiveBuilder<T>::Reserve;

  // Scalar append.
  void Append(value_type val) {
    ArrayBuilder::Reserve(1);
    UnsafeAppend(val);
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
struct type_traits<UInt8Type> {
  typedef UInt8Array ArrayType;

  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct type_traits<Int8Type> {
  typedef Int8Array ArrayType;

  static inline int bytes_required(int elements) { return elements; }
};

template <>
struct type_traits<UInt16Type> {
  typedef UInt16Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint16_t); }
};

template <>
struct type_traits<Int16Type> {
  typedef Int16Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int16_t); }
};

template <>
struct type_traits<UInt32Type> {
  typedef UInt32Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint32_t); }
};

template <>
struct type_traits<Int32Type> {
  typedef Int32Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int32_t); }
};

template <>
struct type_traits<UInt64Type> {
  typedef UInt64Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(uint64_t); }
};

template <>
struct type_traits<Int64Type> {
  typedef Int64Array ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(int64_t); }
};
template <>
struct type_traits<FloatType> {
  typedef FloatArray ArrayType;

  static inline int bytes_required(int elements) { return elements * sizeof(float); }
};

template <>
struct type_traits<DoubleType> {
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

typedef NumericBuilder<FloatType> FloatBuilder;
typedef NumericBuilder<DoubleType> DoubleBuilder;

class BooleanArray : public PrimitiveArray {
 public:
  using PrimitiveArray::PrimitiveArray;

  BooleanArray(int32_t length, const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0, const std::shared_ptr<Buffer>& null_bitmap = nullptr);

  bool EqualsExact(const BooleanArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

  const uint8_t* raw_data() const { return reinterpret_cast<const uint8_t*>(raw_data_); }

  bool Value(int i) const { return util::get_bit(raw_data(), i); }
};

template <>
struct type_traits<BooleanType> {
  typedef BooleanArray ArrayType;

  static inline int bytes_required(int elements) {
    return util::bytes_for_bits(elements);
  }
};

class BooleanBuilder : public PrimitiveBuilder<BooleanType> {
 public:
  explicit BooleanBuilder(MemoryPool* pool, const TypePtr& type)
      : PrimitiveBuilder<BooleanType>(pool, type) {}

  virtual ~BooleanBuilder() {}

  using PrimitiveBuilder<BooleanType>::Append;

  // Scalar append
  void Append(bool val) {
    util::set_bit(null_bitmap_data_, length_);
    if (val) {
      util::set_bit(raw_data_, length_);
    } else {
      util::clear_bit(raw_data_, length_);
    }
    ++length_;
  }

  void Append(uint8_t val) { Append(static_cast<bool>(val)); }
};

}  // namespace arrow

#endif  // ARROW_TYPES_PRIMITIVE_H
