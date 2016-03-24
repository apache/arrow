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

#include "arrow/types/primitive.h"

#include <memory>

#include "arrow/util/buffer.h"

namespace arrow {

// ----------------------------------------------------------------------
// Primitive array base

PrimitiveArray::PrimitiveArray(const TypePtr& type, int32_t length, int value_size,
    const std::shared_ptr<Buffer>& data,
    int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap) :
    Array(type, length, null_count, null_bitmap) {
  data_ = data;
  raw_data_ = data == nullptr? nullptr : data_->data();
  value_size_ = value_size;
}

bool PrimitiveArray::EqualsExact(const PrimitiveArray& other) const {
  if (this == &other) return true;
  if (null_count_ != other.null_count_) {
    return false;
  }

  if (null_count_ > 0) {
    bool equal_bitmap = null_bitmap_->Equals(*other.null_bitmap_,
        util::ceil_byte(length_) / 8);
    if (!equal_bitmap) {
      return false;
    }

    const uint8_t* this_data = raw_data_;
    const uint8_t* other_data = other.raw_data_;

    for (int i = 0; i < length_; ++i) {
      if (!IsNull(i) && memcmp(this_data, other_data, value_size_)) {
        return false;
      }
      this_data += value_size_;
      other_data += value_size_;
    }
    return true;
  } else {
    return data_->Equals(*other.data_, length_);
  }
}

bool PrimitiveArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) return true;
  if (this->type_enum() != arr->type_enum()) {
    return false;
  }
  return EqualsExact(*static_cast<const PrimitiveArray*>(arr.get()));
}

} // namespace arrow
