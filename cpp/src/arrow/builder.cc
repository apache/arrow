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

Status ArrayBuilder::Init(int32_t capacity) {
  capacity_ = capacity;
  int32_t to_alloc = util::ceil_byte(capacity) / 8;
  null_bitmap_ = std::make_shared<PoolBuffer>(pool_);
  RETURN_NOT_OK(null_bitmap_->Resize(to_alloc));
  null_bitmap_data_ = null_bitmap_->mutable_data();
  memset(null_bitmap_data_, 0, to_alloc);
  return Status::OK();
}

Status ArrayBuilder::Resize(int32_t new_bits) {
  int32_t new_bytes = util::ceil_byte(new_bits) / 8;
  int32_t old_bytes = null_bitmap_->size();
  RETURN_NOT_OK(null_bitmap_->Resize(new_bytes));
  null_bitmap_data_ = null_bitmap_->mutable_data();
  if (old_bytes < new_bytes) {
    memset(null_bitmap_data_ + old_bytes, 0, new_bytes - old_bytes);
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
    int32_t new_capacity = util::next_power2(length_ + elements);
    return Resize(new_capacity);
  }
  return Status::OK();
}

} // namespace arrow
