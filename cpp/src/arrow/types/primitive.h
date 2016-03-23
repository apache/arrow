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
  PrimitiveArray(const TypePtr& type, int32_t length,
      const std::shared_ptr<Buffer>& data,
      int32_t null_count = 0,
      const std::shared_ptr<Buffer>& nulls = nullptr);
  virtual ~PrimitiveArray() {}

  const std::shared_ptr<Buffer>& data() const { return data_;}

  bool EqualsExact(const PrimitiveArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

 protected:
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};

#define NUMERIC_ARRAY_DECL(NAME, TypeClass, T)                      \
class NAME : public PrimitiveArray {                                \
 public:                                                            \
  using value_type = T;                                             \
  using PrimitiveArray::PrimitiveArray;                             \
  NAME(int32_t length, const std::shared_ptr<Buffer>& data,         \
      int32_t null_count = 0,                                       \
      const std::shared_ptr<Buffer>& nulls = nullptr) :             \
      PrimitiveArray(std::make_shared<TypeClass>(), length, data,   \
          null_count, nulls) {}                                     \
                                                                    \
  bool EqualsExact(const NAME& other) const {                       \
    return PrimitiveArray::EqualsExact(                             \
        *static_cast<const PrimitiveArray*>(&other));               \
  }                                                                 \
                                                                    \
  const T* raw_data() const {                                       \
    return reinterpret_cast<const T*>(raw_data_);                   \
  }                                                                 \
                                                                    \
  T Value(int i) const {                                            \
    return raw_data()[i];                                           \
  }                                                                 \
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

template <typename Type, typename ArrayType>
class PrimitiveBuilder : public ArrayBuilder {
 public:
  typedef typename Type::c_type value_type;

  explicit PrimitiveBuilder(MemoryPool* pool, const TypePtr& type) :
      ArrayBuilder(pool, type),
      values_(nullptr) {
    elsize_ = sizeof(value_type);
  }

  virtual ~PrimitiveBuilder() {}

  Status Resize(int32_t capacity) {
    // XXX: Set floor size for now
    if (capacity < MIN_BUILDER_CAPACITY) {
      capacity = MIN_BUILDER_CAPACITY;
    }

    if (capacity_ == 0) {
      RETURN_NOT_OK(Init(capacity));
    } else {
      RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
      RETURN_NOT_OK(values_->Resize(capacity * elsize_));
    }
    capacity_ = capacity;
    return Status::OK();
  }

  Status Init(int32_t capacity) {
    RETURN_NOT_OK(ArrayBuilder::Init(capacity));
    values_ = std::make_shared<PoolBuffer>(pool_);
    return values_->Resize(capacity * elsize_);
  }

  Status Reserve(int32_t elements) {
    if (length_ + elements > capacity_) {
      int32_t new_capacity = util::next_power2(length_ + elements);
      return Resize(new_capacity);
    }
    return Status::OK();
  }

  Status Advance(int32_t elements) {
    return ArrayBuilder::Advance(elements);
  }

  // Scalar append
  Status Append(value_type val, bool is_null = false) {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    if (is_null) {
      ++null_count_;
      util::set_bit(null_bits_, length_);
    }
    raw_buffer()[length_++] = val;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, null_bytes is of equal length to values, and any nonzero byte
  // will be considered as a null for that slot
  Status Append(const value_type* values, int32_t length,
      const uint8_t* null_bytes = nullptr) {
    if (length_ + length > capacity_) {
      int32_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    if (length > 0) {
      memcpy(raw_buffer() + length_, values, length * elsize_);
    }

    if (null_bytes != nullptr) {
      AppendNulls(null_bytes, length);
    }

    length_ += length;
    return Status::OK();
  }

  // Write nulls as uint8_t* into pre-allocated memory
  void AppendNulls(const uint8_t* null_bytes, int32_t length) {
    // If null_bytes is all not null, then none of the values are null
    for (int i = 0; i < length; ++i) {
      if (static_cast<bool>(null_bytes[i])) {
        ++null_count_;
        util::set_bit(null_bits_, length_ + i);
      }
    }
  }

  Status AppendNull() {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    ++null_count_;
    util::set_bit(null_bits_, length_++);
    return Status::OK();
  }

  std::shared_ptr<Array> Finish() override {
    std::shared_ptr<ArrayType> result = std::make_shared<ArrayType>(
        type_, length_, values_, null_count_, nulls_);

    values_ = nulls_ = nullptr;
    capacity_ = length_ = null_count_ = 0;
    return result;
  }

  value_type* raw_buffer() {
    return reinterpret_cast<value_type*>(values_->mutable_data());
  }

  std::shared_ptr<Buffer> buffer() const {
    return values_;
  }

 protected:
  std::shared_ptr<PoolBuffer> values_;
  int elsize_;
};

// Builders

typedef PrimitiveBuilder<UInt8Type, UInt8Array> UInt8Builder;
typedef PrimitiveBuilder<UInt16Type, UInt16Array> UInt16Builder;
typedef PrimitiveBuilder<UInt32Type, UInt32Array> UInt32Builder;
typedef PrimitiveBuilder<UInt64Type, UInt64Array> UInt64Builder;

typedef PrimitiveBuilder<Int8Type, Int8Array> Int8Builder;
typedef PrimitiveBuilder<Int16Type, Int16Array> Int16Builder;
typedef PrimitiveBuilder<Int32Type, Int32Array> Int32Builder;
typedef PrimitiveBuilder<Int64Type, Int64Array> Int64Builder;

typedef PrimitiveBuilder<FloatType, FloatArray> FloatBuilder;
typedef PrimitiveBuilder<DoubleType, DoubleArray> DoubleBuilder;

} // namespace arrow

#endif  // ARROW_TYPES_PRIMITIVE_H
