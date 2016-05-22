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

#include "arrow/util/buffer.h"
#include "arrow/util/logging.h"

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
    return null_bitmap_->Equals(*other.null_bitmap_, util::bytes_for_bits(length_));
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

Status Array::SliceNullBitmap(std::shared_ptr<Buffer>* out_buf, int32_t& null_count,
    int32_t start, int32_t length) const {
  DCHECK_GE(start, 0);
  DCHECK_GT(length, 0);
  DCHECK_GT(null_count_, 0);

  auto null_buffer = std::make_shared<PoolBuffer>();
  auto bit_length = util::bytes_for_bits(length);
  DCHECK_GE(bit_length, 0);

  RETURN_NOT_OK(null_buffer->Resize(bit_length));
  memset(null_buffer->mutable_data(), 0, bit_length);

  auto bit_null_count = util::bytes_to_bits(
      null_bitmap_->data(), start, length, null_buffer->mutable_data());

  DCHECK_GT(null_buffer->size(), 0);

  *out_buf = null_buffer;
  null_count = bit_null_count;

  return Status::OK();
}

}  // namespace arrow
