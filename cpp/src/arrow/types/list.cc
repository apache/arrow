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
#include "arrow/types/list.h"

#include <sstream>

namespace arrow {

bool ListArray::EqualsExact(const ListArray& other) const {
  if (this == &other) { return true; }
  if (null_count_ != other.null_count_) { return false; }

  bool equal_offsets =
      offset_buf_->Equals(*other.offset_buf_, (length_ + 1) * sizeof(int32_t));
  if (!equal_offsets) { return false; }
  bool equal_null_bitmap = true;
  if (null_count_ > 0) {
    equal_null_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, util::bytes_for_bits(length_));
  }

  if (!equal_null_bitmap) { return false; }

  return values()->Equals(other.values());
}

bool ListArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const ListArray*>(arr.get()));
}

Status ListArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }
  if (!offset_buf_) { return Status::Invalid("offset_buf_ was null"); }
  if (offset_buf_->size() / sizeof(int32_t) < length_) {
    std::stringstream ss;
    ss << "offset buffer size (bytes): " << offset_buf_->size()
       << " isn't large enough for length: " << length_;
    return Status::Invalid(ss.str());
  }
  const int32_t last_offset = offset(length_);
  if (last_offset > 0) {
    if (!values_) {
      return Status::Invalid("last offset was non-zero and values was null");
    }
    if (values_->length() != last_offset) {
      std::stringstream ss;
      ss << "Final offset invariant not equal to values length: " << last_offset
         << "!=" << values_->length();
      return Status::Invalid(ss.str());
    }

    const Status child_valid = values_->Validate();
    if (!child_valid.ok()) {
      std::stringstream ss;
      ss << "Child array invalid: " << child_valid.ToString();
      return Status::Invalid(ss.str());
    }
  }

  int32_t prev_offset = offset(0);
  if (prev_offset != 0) { return Status::Invalid("The first offset wasn't zero"); }
  for (int32_t i = 1; i <= length_; ++i) {
    int32_t current_offset = offset(i);
    if (IsNull(i - 1) && current_offset != prev_offset) {
      std::stringstream ss;
      ss << "Offset invariant failure at: " << i << " inconsistent offsets for null slot"
         << current_offset << "!=" << prev_offset;
      return Status::Invalid(ss.str());
    }
    if (current_offset < prev_offset) {
      std::stringstream ss;
      ss << "Offset invariant failure: " << i
         << " inconsistent offset for non-null slot: " << current_offset << "<"
         << prev_offset;
      return Status::Invalid(ss.str());
    }
    prev_offset = current_offset;
  }
  return Status::OK();
}

}  // namespace arrow
