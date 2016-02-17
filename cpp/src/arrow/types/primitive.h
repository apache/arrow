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
#include <string>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

template <typename Derived>
struct PrimitiveType : public DataType {
  explicit PrimitiveType(bool nullable = true)
      : DataType(Derived::type_enum, nullable) {}

  virtual std::string ToString() const {
    return std::string(static_cast<const Derived*>(this)->name());
  }
};

#define PRIMITIVE_DECL(TYPENAME, C_TYPE, ENUM, SIZE, NAME)          \
  typedef C_TYPE c_type;                                            \
  static constexpr TypeEnum type_enum = TypeEnum::ENUM;             \
  static constexpr int size = SIZE;                              \
                                                                    \
  explicit TYPENAME(bool nullable = true)                           \
      : PrimitiveType<TYPENAME>(nullable) {}                        \
                                                                    \
  static const char* name() {                                       \
    return NAME;                                                    \
  }


// Base class for fixed-size logical types
class PrimitiveArray : public Array {
 public:
  PrimitiveArray() : Array(), data_(nullptr), raw_data_(nullptr) {}

  virtual ~PrimitiveArray() {}

  void Init(const TypePtr& type, int64_t length, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& nulls = nullptr);

  const std::shared_ptr<Buffer>& data() const { return data_;}

  bool Equals(const PrimitiveArray& other) const;

 protected:
  std::shared_ptr<Buffer> data_;
  const uint8_t* raw_data_;
};


template <typename TypeClass>
class PrimitiveArrayImpl : public PrimitiveArray {
 public:
  typedef typename TypeClass::c_type T;

  PrimitiveArrayImpl() : PrimitiveArray() {}

  PrimitiveArrayImpl(int64_t length, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& nulls = nullptr) {
    Init(length, data, nulls);
  }

  void Init(int64_t length, const std::shared_ptr<Buffer>& data,
      const std::shared_ptr<Buffer>& nulls = nullptr) {
    TypePtr type(new TypeClass(nulls != nullptr));
    PrimitiveArray::Init(type, length, data, nulls);
  }

  bool Equals(const PrimitiveArrayImpl& other) const {
    return PrimitiveArray::Equals(*static_cast<const PrimitiveArray*>(&other));
  }

  const T* raw_data() const { return reinterpret_cast<const T*>(raw_data_);}

  T Value(int64_t i) const {
    return raw_data()[i];
  }

  TypeClass* exact_type() const {
    return static_cast<TypeClass*>(type_);
  }
};


template <typename Type, typename ArrayType>
class PrimitiveBuilder : public ArrayBuilder {
 public:
  typedef typename Type::c_type T;

  explicit PrimitiveBuilder(const TypePtr& type)
      : ArrayBuilder(type), values_(nullptr) {
    elsize_ = sizeof(T);
  }

  virtual ~PrimitiveBuilder() {}

  Status Resize(int64_t capacity) {
    // XXX: Set floor size for now
    if (capacity < MIN_BUILDER_CAPACITY) {
      capacity = MIN_BUILDER_CAPACITY;
    }

    if (capacity_ == 0) {
      RETURN_NOT_OK(Init(capacity));
    } else {
      RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
      RETURN_NOT_OK(values_->Resize(capacity * elsize_));
      capacity_ = capacity;
    }
    return Status::OK();
  }

  Status Init(int64_t capacity) {
    RETURN_NOT_OK(ArrayBuilder::Init(capacity));

    values_ = std::make_shared<OwnedMutableBuffer>();
    return values_->Resize(capacity * elsize_);
  }

  Status Reserve(int64_t elements) {
    if (length_ + elements > capacity_) {
      int64_t new_capacity = util::next_power2(length_ + elements);
      return Resize(new_capacity);
    }
    return Status::OK();
  }

  Status Advance(int64_t elements) {
    return ArrayBuilder::Advance(elements);
  }

  // Scalar append
  Status Append(T val, bool is_null = false) {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    if (nullable_) {
      util::set_bit(null_bits_, length_, is_null);
    }
    raw_buffer()[length_++] = val;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, null_bytes is of equal length to values, and any nonzero byte
  // will be considered as a null for that slot
  Status Append(const T* values, int64_t length, uint8_t* null_bytes = nullptr) {
    if (length_ + length > capacity_) {
      int64_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memcpy(raw_buffer() + length_, values, length * elsize_);

    if (nullable_ && null_bytes != nullptr) {
      // If null_bytes is all not null, then none of the values are null
      for (int64_t i = 0; i < length; ++i) {
        util::set_bit(null_bits_, length_ + i, static_cast<bool>(null_bytes[i]));
      }
    }

    length_ += length;
    return Status::OK();
  }

  Status AppendNull() {
    if (!nullable_) {
      return Status::Invalid("not nullable");
    }
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    util::set_bit(null_bits_, length_++, true);
    return Status::OK();
  }

  // Initialize an array type instance with the results of this builder
  // Transfers ownership of all buffers
  Status Transfer(PrimitiveArray* out) {
    out->Init(type_, length_, values_, nulls_);
    values_ = nulls_ = nullptr;
    capacity_ = length_ = 0;
    return Status::OK();
  }

  Status Transfer(ArrayType* out) {
    return Transfer(static_cast<PrimitiveArray*>(out));
  }

  virtual Status ToArray(Array** out) {
    ArrayType* result = new ArrayType();
    RETURN_NOT_OK(Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

  T* raw_buffer() {
    return reinterpret_cast<T*>(values_->mutable_data());
  }

  std::shared_ptr<Buffer> buffer() const {
    return values_;
  }

 protected:
  std::shared_ptr<OwnedMutableBuffer> values_;
  int64_t elsize_;
};

} // namespace arrow

#endif  // ARROW_TYPES_PRIMITIVE_H
