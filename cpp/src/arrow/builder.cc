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

#include "arrow/builder.h"

#include <cstring>

#include "arrow/util/bit-util.h"
#include "arrow/util/buffer.h"
#include "arrow/util/status.h"

namespace arrow {

Status ArrayBuilder::AppendToBitmap(bool is_valid) {
  if (length_ == capacity_) {
    // If the capacity was not already a multiple of 2, do so here
    // TODO(emkornfield) doubling isn't great default allocation practice
    // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
    // fo discussion
    RETURN_NOT_OK(Resize(util::next_power2(capacity_ + 1)));
  }
  UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(const uint8_t* valid_bytes, int32_t length) {
  RETURN_NOT_OK(Reserve(length));

  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status ArrayBuilder::Init(int32_t capacity) {
  int32_t to_alloc = util::ceil_byte(capacity) / 8;
  null_bitmap_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(null_bitmap_->Resize(to_alloc));
  // Buffers might allocate more then necessary to satisfy padding requirements
  const int byte_capacity = null_bitmap_->capacity();
  capacity_ = capacity;
  null_bitmap_data_ = null_bitmap_->mutable_data();
  memset(null_bitmap_data_, 0, byte_capacity);
  return Status::OK();
}

Status ArrayBuilder::Resize(int32_t new_bits) {
  if (!null_bitmap_) { return Init(new_bits); }
  int32_t new_bytes = util::ceil_byte(new_bits) / 8;
  int32_t old_bytes = null_bitmap_->size();
  RETURN_NOT_OK(null_bitmap_->Resize(new_bytes));
  null_bitmap_data_ = null_bitmap_->mutable_data();
  // The buffer might be overpadded to deal with padding according to the spec
  const int32_t byte_capacity = null_bitmap_->capacity();
  capacity_ = new_bits;
  if (old_bytes < new_bytes) {
    memset(null_bitmap_data_ + old_bytes, 0, byte_capacity - old_bytes);
  }
  return Status::OK();
}

Status ArrayBuilder::Advance(int32_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}

Status ArrayBuilder::Reserve(int32_t elements) {
  if (length_ + elements > capacity_) {
    // TODO(emkornfield) power of 2 growth is potentially suboptimal
    int32_t new_capacity = util::next_power2(length_ + elements);
    return Resize(new_capacity);
  }
  return Status::OK();
}

Status ArrayBuilder::SetNotNull(int32_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeSetNotNull(length);
  return Status::OK();
}

void ArrayBuilder::UnsafeAppendToBitmap(bool is_valid) {
  if (is_valid) {
    util::set_bit(null_bitmap_data_, length_);
  } else {
    ++null_count_;
  }
  ++length_;
}

void ArrayBuilder::UnsafeAppendToBitmap(const uint8_t* valid_bytes, int32_t length) {
  if (valid_bytes == nullptr) {
    UnsafeSetNotNull(length);
    return;
  }
  for (int32_t i = 0; i < length; ++i) {
    // TODO(emkornfield) Optimize for large values of length?
    UnsafeAppendToBitmap(valid_bytes[i] > 0);
  }
}

void ArrayBuilder::UnsafeSetNotNull(int32_t length) {
  const int32_t new_length = length + length_;
  // TODO(emkornfield) Optimize for large values of length?
  for (int32_t i = length_; i < new_length; ++i) {
    util::set_bit(null_bitmap_data_, i);
  }
  length_ = new_length;
}

}  // namespace arrow
