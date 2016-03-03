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

Status ArrayBuilder::Init(int64_t capacity) {
  capacity_ = capacity;

  if (nullable_) {
    int64_t to_alloc = util::ceil_byte(capacity) / 8;
    nulls_ = std::make_shared<PoolBuffer>(pool_);
    RETURN_NOT_OK(nulls_->Resize(to_alloc));
    null_bits_ = nulls_->mutable_data();
    memset(null_bits_, 0, to_alloc);
  }
  return Status::OK();
}

Status ArrayBuilder::Resize(int64_t new_bits) {
  if (nullable_) {
    int64_t new_bytes = util::ceil_byte(new_bits) / 8;
    int64_t old_bytes = nulls_->size();
    RETURN_NOT_OK(nulls_->Resize(new_bytes));
    null_bits_ = nulls_->mutable_data();
    if (old_bytes < new_bytes) {
      memset(null_bits_ + old_bytes, 0, new_bytes - old_bytes);
    }
  }
  return Status::OK();
}

Status ArrayBuilder::Advance(int64_t elements) {
  if (nullable_ && length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}


} // namespace arrow
