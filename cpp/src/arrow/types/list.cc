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

std::shared_ptr<Array> ListArray::Slice(int32_t start) const {
  return Slice(start, length_);
}

// Slicing a ListArray
std::shared_ptr<Array> ListArray::Slice(int32_t start, int32_t length) const {
  if (start < 0 || start > length_) return nullptr;
  if (length <= 0 || length > length_) return nullptr;

  DCHECK_GE(start, 0);
  DCHECK_LE(start, length_);
  DCHECK_GT(length, 0);

  // asign corret start and length
  auto sliced_start = start;
  auto sliced_length = length;
  sliced_length =
      sliced_length <= length_ - sliced_start ? sliced_length : length_ - sliced_start;

  DCHECK_LE(sliced_length, length_ - sliced_start);

  // calculate the start and length for recursively slicing offsets_ and null_bitmap.
  int32_t real_length = 0;
  int32_t real_start = 0;
  real_start = offsets_[start];
  real_length += offsets_[sliced_length + start] - offsets_[start];

  if (real_length == 0) return nullptr;
  DCHECK_GT(real_length, 0);

  // start and length for slicing values_
  auto value_start = real_start;
  auto value_length = real_length;

  int32_t sliced_null_count = 0;
  std::shared_ptr<Buffer> sliced_null_bitmap;

  // slice null_bitmap_
  Array::SliceNullBitmap(
      &sliced_null_bitmap, sliced_null_count, sliced_start, sliced_length);
  // slice offset_buf
  std::shared_ptr<Buffer> sliced_offset_buf;
  SliceOffset(&sliced_offset_buf, sliced_start, sliced_length);

  // slice values_, recursively
  std::shared_ptr<Array> sliced_values = values_->Slice(value_start, value_length);

  return std::make_shared<ListArray>(type_, sliced_length, sliced_offset_buf,
      sliced_values, sliced_null_count, sliced_null_bitmap);
}

// offsets_ is type of 'const int32_t*', so start and length will not be transfered.
// Currently, a new offset buffer is allocated with value copy.
Status ListArray::SliceOffset(
    std::shared_ptr<Buffer>* out, int32_t start, int32_t length) const {
  auto sliced_length = length;

  DCHECK_LE(sliced_length, length_);

  // create a new offset buffer
  std::shared_ptr<PoolBuffer> sliced_offset_buf = std::make_shared<PoolBuffer>();
  RETURN_NOT_OK(sliced_offset_buf->Resize(
      static_cast<int64_t>((sliced_length + 1) * sizeof(int32_t))));

  // transfer source and target offset buffer from 'int8_t*' to 'int32_t*'
  auto sliced_offsets = reinterpret_cast<int32_t*>(sliced_offset_buf->mutable_data());
  auto offsets = reinterpret_cast<const int32_t*>(offset_buf_->data());
  memset(sliced_offsets, 0, sliced_length + 1);

  // DCHECK_EQ(3, sliced_length + 1);
  // If there are sliced conditions:
  // --offset_buf_ :  [0, 3, 3, 7]
  // --sliced_start:  1
  // --sliced_length: 2
  // So the expected sliced offset elements from offset_buf_ is:
  // --sliced_offset_buf: [3, 7]
  // therefor, sliced_offset_buf should be transfered to:
  // --sliced_offset_buf: [0, 0, 4]
  for (int i = 1; i <= sliced_length; i++) {
    sliced_offsets[i] =
        sliced_offsets[i - 1] + offsets[start + i] - offsets[start + i - 1];
  }

  *out = sliced_offset_buf;
  return Status::OK();
}

}  // namespace arrow
