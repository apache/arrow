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

#include "arrow/util/bitmap.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/array/array_primitive.h"
#include "arrow/buffer.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

std::string Bitmap::ToString() const {
  std::string out(length_ + ((length_ - 1) / 8), ' ');
  for (int64_t i = 0; i < length_; ++i) {
    out[i + (i / 8)] = GetBit(i) ? '1' : '0';
  }
  return out;
}

std::string Bitmap::Diff(const Bitmap& other) const {
  auto this_buf = std::make_shared<Buffer>(data_, length_);
  auto other_buf = std::make_shared<Buffer>(other.data_, other.length_);

  auto this_arr = std::make_shared<BooleanArray>(length_, this_buf, nullptr, 0, offset_);
  auto other_arr =
      std::make_shared<BooleanArray>(other.length_, other_buf, nullptr, 0, other.offset_);

  return this_arr->Diff(*other_arr);
}

void Bitmap::CopyFrom(const Bitmap& other) {
  ::arrow::internal::CopyBitmap(other.data_, other.offset_, other.length_, mutable_data_,
                                offset_);
}

void Bitmap::CopyFromInverted(const Bitmap& other) {
  ::arrow::internal::InvertBitmap(other.data_, other.offset_, other.length_,
                                  mutable_data_, offset_);
}

bool Bitmap::Equals(const Bitmap& other) const {
  if (length_ != other.length_) {
    return false;
  }
  return BitmapEquals(data_, offset_, other.data_, other.offset(), length_);
}

int64_t Bitmap::BitLength(const Bitmap* bitmaps, size_t N) {
  for (size_t i = 1; i < N; ++i) {
    DCHECK_EQ(bitmaps[i].length(), bitmaps[0].length());
  }
  return bitmaps[0].length();
}

}  // namespace internal
}  // namespace arrow
