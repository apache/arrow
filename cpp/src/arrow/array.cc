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

#include "arrow/array.h"

#include <cstdint>

#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

// ----------------------------------------------------------------------
// Base array class

Array::Array(const TypePtr& type, int32_t length, int32_t null_count,
    const std::shared_ptr<Buffer>& null_bitmap) {
  type_ = type;
  length_ = length;
  null_count_ = null_count;
  null_bitmap_ = null_bitmap;
  if (null_bitmap_) { null_bitmap_data_ = null_bitmap_->data(); }
}

bool Array::EqualsExact(const Array& other) const {
  if (this == &other) { return true; }
  if (length_ != other.length_ || null_count_ != other.null_count_ ||
      type_enum() != other.type_enum()) {
    return false;
  }
  if (null_count_ > 0) {
    return null_bitmap_->Equals(*other.null_bitmap_, BitUtil::BytesForBits(length_));
  }
  return true;
}

Status Array::Validate() const {
  return Status::OK();
}

bool NullArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (Type::NA != arr->type_enum()) { return false; }
  return arr->length() == length_;
}

bool NullArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_index,
    const std::shared_ptr<Array>& arr) const {
  if (!arr) { return false; }
  if (Type::NA != arr->type_enum()) { return false; }
  return true;
}

}  // namespace arrow
