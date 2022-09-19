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

#include "arrow/util/bit_block_counter.h"

#include <algorithm>
#include <cstdint>
#include <type_traits>

#include "arrow/buffer.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {
namespace internal {

BitBlockCount BitBlockCounter::GetBlockSlow(int64_t block_size) noexcept {
  const int16_t run_length = static_cast<int16_t>(std::min(bits_remaining_, block_size));
  int16_t popcount = static_cast<int16_t>(CountSetBits(bitmap_, offset_, run_length));
  bits_remaining_ -= run_length;
  // This code path should trigger _at most_ 2 times. In the "two times"
  // case, the first time the run length will be a multiple of 8 by construction
  bitmap_ += run_length / 8;
  return {run_length, popcount};
}

OptionalBitBlockCounter::OptionalBitBlockCounter(const uint8_t* validity_bitmap,
                                                 int64_t offset, int64_t length)
    : has_bitmap_(validity_bitmap != nullptr),
      position_(0),
      length_(length),
      counter_(util::MakeNonNull(validity_bitmap), offset, length) {}

OptionalBitBlockCounter::OptionalBitBlockCounter(
    const std::shared_ptr<Buffer>& validity_bitmap, int64_t offset, int64_t length)
    : OptionalBitBlockCounter(validity_bitmap ? validity_bitmap->data() : nullptr, offset,
                              length) {}

OptionalBinaryBitBlockCounter::OptionalBinaryBitBlockCounter(const uint8_t* left_bitmap,
                                                             int64_t left_offset,
                                                             const uint8_t* right_bitmap,
                                                             int64_t right_offset,
                                                             int64_t length)
    : has_bitmap_(HasBitmapFromBitmaps(left_bitmap != nullptr, right_bitmap != nullptr)),
      position_(0),
      length_(length),
      unary_counter_(
          util::MakeNonNull(left_bitmap != nullptr ? left_bitmap : right_bitmap),
          left_bitmap != nullptr ? left_offset : right_offset, length),
      binary_counter_(util::MakeNonNull(left_bitmap), left_offset,
                      util::MakeNonNull(right_bitmap), right_offset, length) {}

OptionalBinaryBitBlockCounter::OptionalBinaryBitBlockCounter(
    const std::shared_ptr<Buffer>& left_bitmap, int64_t left_offset,
    const std::shared_ptr<Buffer>& right_bitmap, int64_t right_offset, int64_t length)
    : OptionalBinaryBitBlockCounter(
          left_bitmap ? left_bitmap->data() : nullptr, left_offset,
          right_bitmap ? right_bitmap->data() : nullptr, right_offset, length) {}

}  // namespace internal
}  // namespace arrow
