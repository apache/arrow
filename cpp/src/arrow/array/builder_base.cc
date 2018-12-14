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

#include "arrow/array/builder_base.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/array.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/int-util.h"
#include "arrow/util/logging.h"

namespace arrow {

Status ArrayBuilder::TrimBuffer(const int64_t bytes_filled, ResizableBuffer* buffer) {
  if (buffer) {
    if (bytes_filled < buffer->size()) {
      // Trim buffer
      RETURN_NOT_OK(buffer->Resize(bytes_filled));
    }
    // zero the padding
    buffer->ZeroPadding();
  } else {
    // Null buffers are allowed in place of 0-byte buffers
    DCHECK_EQ(bytes_filled, 0);
  }
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(bool is_valid) {
  if (length_ == capacity_) {
    // If the capacity was not already a multiple of 2, do so here
    // TODO(emkornfield) doubling isn't great default allocation practice
    // see https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
    // fo discussion
    RETURN_NOT_OK(Resize(BitUtil::NextPower2(capacity_ + 1)));
  }
  UnsafeAppendToBitmap(is_valid);
  return Status::OK();
}

Status ArrayBuilder::AppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  RETURN_NOT_OK(Reserve(length));

  UnsafeAppendToBitmap(valid_bytes, length);
  return Status::OK();
}

Status ArrayBuilder::Resize(int64_t capacity) {
  // Target size of validity (null) bitmap data
  const int64_t new_bitmap_size = BitUtil::BytesForBits(capacity);
  RETURN_NOT_OK(CheckCapacity(capacity, capacity_));

  if (capacity_ == 0) {
    RETURN_NOT_OK(AllocateResizableBuffer(pool_, new_bitmap_size, &null_bitmap_));
    null_bitmap_data_ = null_bitmap_->mutable_data();

    // Padding is zeroed by AllocateResizableBuffer
    memset(null_bitmap_data_, 0, static_cast<size_t>(new_bitmap_size));
  } else {
    const int64_t old_bitmap_capacity = null_bitmap_->capacity();
    RETURN_NOT_OK(null_bitmap_->Resize(new_bitmap_size));

    const int64_t new_bitmap_capacity = null_bitmap_->capacity();
    null_bitmap_data_ = null_bitmap_->mutable_data();

    // Zero the region between the original capacity and the new capacity,
    // including padding, which has not been zeroed, unlike
    // AllocateResizableBuffer
    if (old_bitmap_capacity < new_bitmap_capacity) {
      memset(null_bitmap_data_ + old_bitmap_capacity, 0,
             static_cast<size_t>(new_bitmap_capacity - old_bitmap_capacity));
    }
  }
  capacity_ = capacity;
  return Status::OK();
}

Status ArrayBuilder::Advance(int64_t elements) {
  if (length_ + elements > capacity_) {
    return Status::Invalid("Builder must be expanded");
  }
  length_ += elements;
  return Status::OK();
}

Status ArrayBuilder::Finish(std::shared_ptr<Array>* out) {
  std::shared_ptr<ArrayData> internal_data;
  RETURN_NOT_OK(FinishInternal(&internal_data));
  *out = MakeArray(internal_data);
  return Status::OK();
}

Status ArrayBuilder::Reserve(int64_t additional_elements) {
  if (length_ + additional_elements > capacity_) {
    // TODO(emkornfield) power of 2 growth is potentially suboptimal
    int64_t new_size = BitUtil::NextPower2(length_ + additional_elements);
    return Resize(new_size);
  }
  return Status::OK();
}

void ArrayBuilder::Reset() {
  capacity_ = length_ = null_count_ = 0;
  null_bitmap_ = nullptr;
}

Status ArrayBuilder::SetNotNull(int64_t length) {
  RETURN_NOT_OK(Reserve(length));
  UnsafeSetNotNull(length);
  return Status::OK();
}

void ArrayBuilder::UnsafeAppendToBitmap(const uint8_t* valid_bytes, int64_t length) {
  if (valid_bytes == nullptr) {
    UnsafeSetNotNull(length);
    return;
  }
  UnsafeAppendToBitmap(valid_bytes, valid_bytes + length);
}

void ArrayBuilder::UnsafeAppendToBitmap(const std::vector<bool>& is_valid) {
  UnsafeAppendToBitmap(is_valid.begin(), is_valid.end());
}

void ArrayBuilder::UnsafeSetNotNull(int64_t length) {
  const int64_t new_length = length + length_;

  // Fill up the bytes until we have a byte alignment
  int64_t pad_to_byte = std::min<int64_t>(8 - (length_ % 8), length);

  if (pad_to_byte == 8) {
    pad_to_byte = 0;
  }
  for (int64_t i = length_; i < length_ + pad_to_byte; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  // Fast bitsetting
  int64_t fast_length = (length - pad_to_byte) / 8;
  memset(null_bitmap_data_ + ((length_ + pad_to_byte) / 8), 0xFF,
         static_cast<size_t>(fast_length));

  // Trailing bits
  for (int64_t i = length_ + pad_to_byte + (fast_length * 8); i < new_length; ++i) {
    BitUtil::SetBit(null_bitmap_data_, i);
  }

  length_ = new_length;
}

}  // namespace arrow
