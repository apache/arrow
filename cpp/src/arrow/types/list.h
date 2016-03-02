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

#ifndef ARROW_TYPES_LIST_H
#define ARROW_TYPES_LIST_H

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/types/integer.h"
#include "arrow/types/primitive.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

class MemoryPool;

struct ListType : public DataType {
  // List can contain any other logical value type
  TypePtr value_type;

  explicit ListType(const TypePtr& value_type, bool nullable = true)
      : DataType(TypeEnum::LIST, nullable),
        value_type(value_type) {}

  static char const *name() {
    return "list";
  }

  virtual std::string ToString() const;
};


class ListArray : public Array {
 public:
  ListArray() : Array(), offset_buf_(nullptr), offsets_(nullptr) {}

  ListArray(const TypePtr& type, int64_t length, std::shared_ptr<Buffer> offsets,
      const ArrayPtr& values, std::shared_ptr<Buffer> nulls = nullptr) {
    Init(type, length, offsets, values, nulls);
  }

  virtual ~ListArray() {}

  void Init(const TypePtr& type, int64_t length, std::shared_ptr<Buffer> offsets,
      const ArrayPtr& values, std::shared_ptr<Buffer> nulls = nullptr) {
    offset_buf_ = offsets;
    offsets_ = offsets == nullptr? nullptr :
      reinterpret_cast<const int32_t*>(offset_buf_->data());

    values_ = values;
    Array::Init(type, length, nulls);
  }

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  const ArrayPtr& values() const {return values_;}

  const int32_t* offsets() const { return offsets_;}

  int32_t offset(int i) const { return offsets_[i];}

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) { return offsets_[i];}
  int32_t value_length(int i) { return offsets_[i + 1] - offsets_[i];}

 protected:
  std::shared_ptr<Buffer> offset_buf_;
  const int32_t* offsets_;
  ArrayPtr values_;
};

// ----------------------------------------------------------------------
// Array builder


// Builder class for variable-length list array value types
//
// To use this class, you must append values to the child array builder and use
// the Append function to delimit each distinct list value (once the values
// have been appended to the child array)
class ListBuilder : public Int32Builder {
 public:
  ListBuilder(MemoryPool* pool, const TypePtr& type,
      ArrayBuilder* value_builder)
      : Int32Builder(pool, type) {
    value_builder_.reset(value_builder);
  }

  Status Init(int64_t elements) {
    // One more than requested.
    //
    // XXX: This is slightly imprecise, because we might trigger null mask
    // resizes that are unnecessary when creating arrays with power-of-two size
    return Int32Builder::Init(elements + 1);
  }

  Status Resize(int64_t capacity) {
    // Need space for the end offset
    RETURN_NOT_OK(Int32Builder::Resize(capacity + 1));

    // Slight hack, as the "real" capacity is one less
    --capacity_;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, null_bytes is of equal length to values, and any nonzero byte
  // will be considered as a null for that slot
  Status Append(T* values, int64_t length, uint8_t* null_bytes = nullptr) {
    if (length_ + length > capacity_) {
      int64_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memcpy(raw_buffer() + length_, values, length * elsize_);

    if (nullable_ && null_bytes != nullptr) {
      // If null_bytes is all not null, then none of the values are null
      for (int i = 0; i < length; ++i) {
        util::set_bit(null_bits_, length_ + i, static_cast<bool>(null_bytes[i]));
      }
    }

    length_ += length;
    return Status::OK();
  }

  // Initialize an array type instance with the results of this builder
  // Transfers ownership of all buffers
  template <typename Container>
  Status Transfer(Container* out) {
    Array* child_values;
    RETURN_NOT_OK(value_builder_->ToArray(&child_values));

    // Add final offset if the length is non-zero
    if (length_) {
      raw_buffer()[length_] = child_values->length();
    }

    out->Init(type_, length_, values_, ArrayPtr(child_values), nulls_);
    values_ = nulls_ = nullptr;
    capacity_ = length_ = 0;
    return Status::OK();
  }

  virtual Status ToArray(Array** out) {
    ListArray* result = new ListArray();
    RETURN_NOT_OK(Transfer(result));
    *out = static_cast<Array*>(result);
    return Status::OK();
  }

  // Start a new variable-length list slot
  //
  // This function should be called before beginning to append elements to the
  // value builder
  Status Append(bool is_null = false) {
    if (length_ == capacity_) {
      // If the capacity was not already a multiple of 2, do so here
      RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
    }
    if (nullable_) {
      util::set_bit(null_bits_, length_, is_null);
    }

    raw_buffer()[length_++] = value_builder_->length();
    return Status::OK();
  }

  // Status Append(int32_t* offsets, int length, uint8_t* null_bytes) {
  //   return Int32Builder::Append(offsets, length, null_bytes);
  // }

  Status AppendNull() {
    return Append(true);
  }

  ArrayBuilder* value_builder() const { return value_builder_.get();}

 protected:
  std::unique_ptr<ArrayBuilder> value_builder_;
};


} // namespace arrow

#endif // ARROW_TYPES_LIST_H
