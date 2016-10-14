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
#include <limits>
#include <memory>

#include "arrow/array.h"
#include "arrow/builder.h"
#include "arrow/type.h"
#include "arrow/types/primitive.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"
#include "arrow/util/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class MemoryPool;

class ARROW_EXPORT ListArray : public Array {
 public:
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
  const std::shared_ptr<Array>& values() const { return values_; }
  const std::shared_ptr<Buffer> offset_buffer() const {
    return std::static_pointer_cast<Buffer>(offset_buffer_);
  }

  const std::shared_ptr<DataType>& value_type() const { return values_->type(); }

  const int32_t* offsets() const { return offsets_; }

  int32_t offset(int i) const { return offsets_[i]; }

  // Neither of these functions will perform boundschecking
  int32_t value_offset(int i) const { return offsets_[i]; }
  int32_t value_length(int i) const { return offsets_[i + 1] - offsets_[i]; }

  bool EqualsExact(const ListArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

  bool RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
      const ArrayPtr& arr) const override;

 protected:
  std::shared_ptr<Buffer> offset_buffer_;
  const int32_t* offsets_;
  ArrayPtr values_;
};

// ----------------------------------------------------------------------
// Array builder

// Builder class for variable-length list array value types
//
// To use this class, you must append values to the child array builder and use
// the Append function to delimit each distinct list value (once the values
// have been appended to the child array) or use the bulk API to append
// a sequence of offests and null values.
//
// A note on types.  Per arrow/type.h all types in the c++ implementation are
// logical so even though this class always builds list array, this can
// represent multiple different logical types.  If no logical type is provided
// at construction time, the class defaults to List<T> where t is taken from the
// value_builder/values that the object is constructed with.
class ARROW_EXPORT ListBuilder : public ArrayBuilder {
 public:
  // Use this constructor to incrementally build the value array along with offsets and
  // null bitmap.
  ListBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> value_builder,
      const TypePtr& type = nullptr)
      : ArrayBuilder(
            pool, type ? type : std::static_pointer_cast<DataType>(
                                    std::make_shared<ListType>(value_builder->type()))),
        offset_builder_(pool),
        value_builder_(value_builder) {}

  // Use this constructor to build the list with a pre-existing values array
  ListBuilder(
      MemoryPool* pool, std::shared_ptr<Array> values, const TypePtr& type = nullptr)
      : ArrayBuilder(pool, type ? type : std::static_pointer_cast<DataType>(
                                             std::make_shared<ListType>(values->type()))),
        offset_builder_(pool),
        values_(values) {}

  virtual ~ListBuilder() {}

  Status Init(int32_t elements) override;
  Status Resize(int32_t capacity) override;
  Status Finish(std::shared_ptr<Array>* out) override;

  // Vector append
  //
  // If passed, valid_bytes is of equal length to values, and any zero byte
  // will be considered as a null for that slot
  Status Append(
      const int32_t* offsets, int32_t length, const uint8_t* valid_bytes = nullptr) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(valid_bytes, length);
    offset_builder_.UnsafeAppend<int32_t>(offsets, length);
    return Status::OK();
  }

  // Start a new variable-length list slot
  //
  // This function should be called before beginning to append elements to the
  // value builder
  Status Append(bool is_valid = true) {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    RETURN_NOT_OK(offset_builder_.Append<int32_t>(value_builder_->length()));
    return Status::OK();
  }

  Status AppendNull() { return Append(false); }

  const std::shared_ptr<ArrayBuilder>& value_builder() const {
    DCHECK(!values_) << "Using value builder is pointless when values_ is set";
    return value_builder_;
  }

 protected:
  BufferBuilder offset_builder_;
  std::shared_ptr<ArrayBuilder> value_builder_;
  std::shared_ptr<Array> values_;

  void Reset();
};

}  // namespace arrow

#endif  // ARROW_TYPES_LIST_H
