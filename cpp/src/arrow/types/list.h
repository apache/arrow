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

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/types/primitive.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

class MemoryPool;

class ListArray : public Array {
 public:
  ListArray(const TypePtr& type, int32_t length, std::shared_ptr<Buffer> offsets,
      const ArrayPtr& values,
      int32_t null_count = 0,
      std::shared_ptr<Buffer> null_bitmap = nullptr) :
      Array(type, length, null_count, null_bitmap) {
    offset_buf_ = offsets;
    offsets_ = offsets == nullptr? nullptr :
      reinterpret_cast<const int32_t*>(offset_buf_->data());
    values_ = values;
  }

  virtual ~ListArray() {}

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  const std::shared_ptr<Array>& values() const {return values_;}

  const std::shared_ptr<DataType>& value_type() const {
    return values_->type();
  }

  const int32_t* offsets() const { return offsets_;}

  int32_t offset(int i) const { return offsets_[i];}

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) { return offsets_[i];}
  int32_t value_length(int i) { return offsets_[i + 1] - offsets_[i];}

  bool EqualsExact(const ListArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

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
      std::shared_ptr<ArrayBuilder> value_builder)
      : Int32Builder(pool, type),
        value_builder_(value_builder) {}

  Status Init(int32_t elements) {
    // One more than requested.
    //
    // XXX: This is slightly imprecise, because we might trigger null mask
    // resizes that are unnecessary when creating arrays with power-of-two size
    return Int32Builder::Init(elements + 1);
  }

  Status Resize(int32_t capacity) {
    // Need space for the end offset
    RETURN_NOT_OK(Int32Builder::Resize(capacity + 1));

    // Slight hack, as the "real" capacity is one less
    --capacity_;
    return Status::OK();
  }

  // Vector append
  //
  // If passed, valid_bytes is of equal length to values, and any zero byte
  // will be considered as a null for that slot
  Status Append(value_type* values, int32_t length, uint8_t* valid_bytes = nullptr) {
    if (length_ + length > capacity_) {
      int32_t new_capacity = util::next_power2(length_ + length);
      RETURN_NOT_OK(Resize(new_capacity));
    }
    memcpy(raw_data_ + length_, values,
        type_traits<Int32Type>::bytes_required(length));

    if (valid_bytes != nullptr) {
      AppendNulls(valid_bytes, length);
    }

    length_ += length;
    return Status::OK();
  }

  template <typename Container>
  std::shared_ptr<Array> Transfer() {
    std::shared_ptr<Array> items = value_builder_->Finish();

    // Add final offset if the length is non-zero
    if (length_) {
      raw_data_[length_] = items->length();
    }

    auto result = std::make_shared<Container>(type_, length_, data_, items,
        null_count_, null_bitmap_);

    data_ = null_bitmap_ = nullptr;
    capacity_ = length_ = null_count_ = 0;

    return result;
  }

  std::shared_ptr<Array> Finish() override {
    return Transfer<ListArray>();
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
    if (is_null) {
      ++null_count_;
    } else {
      util::set_bit(null_bitmap_data_, length_);
    }
    raw_data_[length_++] = value_builder_->length();
    return Status::OK();
  }

  Status AppendNull() {
    return Append(true);
  }

  const std::shared_ptr<ArrayBuilder>& value_builder() const {
    return value_builder_;
  }

 protected:
  std::shared_ptr<ArrayBuilder> value_builder_;
};


} // namespace arrow

#endif // ARROW_TYPES_LIST_H
