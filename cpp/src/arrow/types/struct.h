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
#include "arrow/types/list.h"
#include "arrow/types/primitive.h"

namespace arrow {

class StructArray : public Array {
 public:
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
  const std::shared_ptr<Array>& field(int32_t pos) const { return field_arrays_.at(pos); }
  const std::vector<ArrayPtr>& fields() const { return field_arrays_; }

  const std::shared_ptr<DataType>& field_type(int32_t pos) const {
    return field_arrays_.at(pos)->type();
  }

  bool EqualsExact(const StructArray& other) const;
  bool Equals(const std::shared_ptr<Array>& arr) const override;

 protected:
  // The child arrays corresponding to each field of the struct data type.
  std::vector<ArrayPtr> field_arrays_;
};

// ---------------------------------------------------------------------------------
// StructArray builder
class StructBuilder : public ArrayBuilder {
 public:
  StructBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
      std::vector<std::shared_ptr<ArrayBuilder>>& field_builders)
      : ArrayBuilder(pool, type) {
    field_builders_ = field_builders;
  }

  Status Init(int32_t elements) override {
    RETURN_NOT_OK(ArrayBuilder::Init(elements));
    return Status::OK();
  }

  Status Resize(int32_t capacity) override {
    // Need space for the end offset
    if (capacity < MIN_BUILDER_CAPACITY) { capacity = MIN_BUILDER_CAPACITY; }

    if (capacity_ == 0) {
      RETURN_NOT_OK(ArrayBuilder::Init(capacity));
    } else {
      RETURN_NOT_OK(ArrayBuilder::Resize(capacity));
    }
    capacity_ = capacity;

    for (auto it : field_builders_) {
      RETURN_NOT_OK(it->Resize(capacity));
    }
    return Status::OK();
  }

  // null_bitmap is of equal length to every child field, and any zero byte
  // will be considered as a null for that field, but users must using app-
  // end methods or advance methods of the child builders independently to
  // insert data.
  Status Append(const uint8_t* null_bitmap, int32_t length) {
    RETURN_NOT_OK(Reserve(length));
    UnsafeAppendToBitmap(null_bitmap, length);
    return Status::OK();
  }

  std::shared_ptr<Array> Finish() override {
    DCHECK(field_builders_.size());

    std::vector<ArrayPtr> fields;
    for (auto it : field_builders_) {
      fields.push_back(it->Finish());
    }

    auto result =
        std::make_shared<StructArray>(type_, length_, fields, null_count_, null_bitmap_);

    null_bitmap_ = nullptr;
    capacity_ = length_ = null_count_ = 0;

    return result;
  }

  // This function just appends elements to StructBuilder, if you want to
  // inserting data into the child fields, please make sure that the chi-
  // ld builders' append methods must be called independently before that.
  Status Append(bool is_valid = true) {
    RETURN_NOT_OK(Reserve(1));
    UnsafeAppendToBitmap(is_valid);
    return Status::OK();
  }

  Status AppendNull() { return Append(false); }

  const std::shared_ptr<ArrayBuilder> field_builder(int pos) const {
    return field_builders_.at(pos);
  }
  const std::vector<std::shared_ptr<ArrayBuilder>>& field_builders() const {
    return field_builders_;
  }

 protected:
  std::vector<std::shared_ptr<ArrayBuilder>> field_builders_;
};

}  // namespace arrow

#endif  // ARROW_TYPES_STRUCT_H
