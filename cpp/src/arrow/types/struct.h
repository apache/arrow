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

#ifndef ARROW_TYPES_STRUCT_H
#define ARROW_TYPES_STRUCT_H

#include <memory>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "arrow/types/primitive.h"
#include "arrow/types/list.h"

namespace arrow {

class StructArray : public Array {
 public:
  StructArray(const TypePtr& type, int32_t length,
      std::vector<ArrayPtr>& values,
      int32_t null_count = 0,
      std::shared_ptr<Buffer> null_bitmap = nullptr) :
      Array(type, length, null_count, null_bitmap) {
    type_ = type;
    values_ = values;
  }

  virtual ~StructArray() {}

  // Return a shared pointer in case the requestor desires to share ownership
  // with this array.
  const std::shared_ptr<Array>& values(int32_t pos) const {return values_.at(pos);}
  const std::vector<ArrayPtr>& values() const {return values_;}

  const std::shared_ptr<DataType>& value_type(int32_t pos) const {
    return values_.at(pos)->type();
  }

  bool EqualsExact(const ListArray& other) const {
    return true;
  }
  bool Equals(const std::shared_ptr<Array>& arr) const override {
    return true;
  }

 protected:
  // Contains kinds of Arrays.
  std::vector<ArrayPtr> values_;
};

// ............................................................................
// StrcutArray builder
class StructBuilder : public ArrayBuilder {
 public:
  StructBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
    const std::vector<FieldPtr>& fields,
    std::vector<std::shared_ptr<ArrayBuilder>>& value_builder)
    : ArrayBuilder(pool, type) {
    fields_ = fields;
    value_builder_ = value_builder;
  }

  Status Init(int32_t elements) {
    return ArrayBuilder::Init(elements);
  }

  Status Resize(int32_t capacity) {
    // Need space for the end offset
    if (capacity < MIN_BUILDER_CAPACITY) {
      capacity = MIN_BUILDER_CAPACITY;
    }

    if (capacity_ == 0) {
      RETURN_NOT_OK(ArrayBuilder::Init(capacity));
    } else {
      RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
    }
    capacity_ = capacity;

    for (auto it = value_builder_.begin(); it != value_builder_.end(); ++it) {
      it->get()->Resize(capacity);
    }
    return Status::OK();
  }

  // Vector append
  //
  // If passed, valid_bytes is of equal length to values, and any zero byte
  // will be considered as a null for that slot
  Status Append(const uint8_t* null_bitmap, int32_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(null_bitmap, length);
    return Status::OK();
  }

  template <typename Container>
  std::shared_ptr<Array> Transfer() {
    DCHECK(value_builder_.size());

    std::vector<std::shared_ptr<Array>> items;
    for (auto it = value_builder_.cbegin(); it != value_builder_.cend(); it++) {
      items.push_back(it->get()->Finish());
    }
    // Here, for ListArray, offsets_ is needed, but StructArray dont need it.
    auto result = std::make_shared<StructArray>(type_, length_, items,
        null_count_, null_bitmap_);

    null_bitmap_ = nullptr;
    capacity_ = length_ = null_count_ = 0;

    return result;
  }

  std::shared_ptr<Array> Finish() override {
    return Transfer<StructArray>();
  }

  // Start a new variable-length list slot
  //
  // This function should be called before beginning to append elements to the
  // value builder
  // TODO: This method should be a virtual method or not, to append null to
  // it's all children?
  Status Append(bool is_valid = true) {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() {
    return Append(false);
  }

  const std::vector<std::shared_ptr<ArrayBuilder>>& value_builder() const {
    return value_builder_;
  }

 protected:
  std::vector<std::shared_ptr<ArrayBuilder>> value_builder_;
  std::vector<FieldPtr> fields_;
};

} // namespace arrow

#endif // ARROW_TYPES_STRUCT_H
