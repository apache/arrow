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
      offset_buffer_->Equals(*other.offset_buffer_, (length_ + 1) * sizeof(int32_t));
  if (!equal_offsets) { return false; }
  bool equal_null_bitmap = true;
  if (null_count_ > 0) {
    equal_null_bitmap =
        null_bitmap_->Equals(*other.null_bitmap_, BitUtil::BytesForBits(length_));
  }

  if (!equal_null_bitmap) { return false; }

  return values()->Equals(other.values());
}

bool ListArray::Equals(const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  return EqualsExact(*static_cast<const ListArray*>(arr.get()));
}

bool ListArray::RangeEquals(int32_t start_idx, int32_t end_idx, int32_t other_start_idx,
    const std::shared_ptr<Array>& arr) const {
  if (this == arr.get()) { return true; }
  if (!arr) { return false; }
  if (this->type_enum() != arr->type_enum()) { return false; }
  const auto other = static_cast<ListArray*>(arr.get());
  for (int32_t i = start_idx, o_i = other_start_idx; i < end_idx; ++i, ++o_i) {
    const bool is_null = IsNull(i);
    if (is_null != arr->IsNull(o_i)) { return false; }
    if (is_null) continue;
    const int32_t begin_offset = offset(i);
    const int32_t end_offset = offset(i + 1);
    const int32_t other_begin_offset = other->offset(o_i);
    const int32_t other_end_offset = other->offset(o_i + 1);
    // Underlying can't be equal if the size isn't equal
    if (end_offset - begin_offset != other_end_offset - other_begin_offset) {
      return false;
    }
    if (!values_->RangeEquals(
            begin_offset, end_offset, other_begin_offset, other->values())) {
      return false;
    }
  }
  return true;
}

Status ListArray::Validate() const {
  if (length_ < 0) { return Status::Invalid("Length was negative"); }
  if (!offset_buffer_) { return Status::Invalid("offset_buffer_ was null"); }
  if (offset_buffer_->size() / static_cast<int>(sizeof(int32_t)) < length_) {
    std::stringstream ss;
    ss << "offset buffer size (bytes): " << offset_buffer_->size()
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

Status ListBuilder::Init(int32_t elements) {
  DCHECK_LT(elements, std::numeric_limits<int32_t>::max());
  RETURN_NOT_OK(ArrayBuilder::Init(elements));
  // one more then requested for offsets
  return offset_builder_.Resize((elements + 1) * sizeof(int32_t));
}

Status ListBuilder::Resize(int32_t capacity) {
  DCHECK_LT(capacity, std::numeric_limits<int32_t>::max());
  // one more then requested for offsets
  RETURN_NOT_OK(offset_builder_.Resize((capacity + 1) * sizeof(int32_t)));
  return ArrayBuilder::Resize(capacity);
}

Status ListBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<Array> items = values_;
  if (!items) { RETURN_NOT_OK(value_builder_->Finish(&items)); }

  RETURN_NOT_OK(offset_builder_.Append<int32_t>(items->length()));
  std::shared_ptr<Buffer> offsets = offset_builder_.Finish();

  *out = std::make_shared<ListArray>(
      type_, length_, offsets, items, null_count_, null_bitmap_);

  Reset();

  return Status::OK();
}

void ListBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  null_bitmap_ = nullptr;
}

}  // namespace arrow
