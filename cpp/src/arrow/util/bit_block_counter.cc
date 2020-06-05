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

#include <cstdint>
#include <type_traits>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace internal {

BitBlockCounter::Block BitBlockCounter::NextBlock() {
  auto load_word = [](const uint8_t* bytes) -> uint64_t {
    return BitUtil::ToLittleEndian(util::SafeLoadAs<uint64_t>(bytes));
  };
  auto shift_word = [](uint64_t current, uint64_t next, int64_t shift) -> uint64_t {
    return (current >> shift) | (next << (64 - shift));
  };

  // When the offset is > 0, we need there to be a word beyond the last aligned
  // word in the bitmap for the bit shifting logic.
  const int64_t bits_required_to_scan_words = offset_ == 0 ? 256 : 256 + (64 - offset_);
  if (bits_remaining_ < bits_required_to_scan_words) {
    // End of the bitmap, leave it to the caller to decide how to best check
    // these bits, no need to do redundant computation here.
    const int16_t run_length = static_cast<int16_t>(bits_remaining_);
    bits_remaining_ -= run_length;
    return {run_length, static_cast<int16_t>(CountSetBits(bitmap_, offset_, run_length))};
  }

  int64_t total_popcount = 0;
  if (offset_ == 0) {
    total_popcount += BitUtil::PopCount(load_word(bitmap_));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 8));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 16));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 24));
  } else {
    auto current = load_word(bitmap_);
    auto next = load_word(bitmap_ + 8);
    total_popcount += BitUtil::PopCount(shift_word(current, next, offset_));
    current = next;
    next = load_word(bitmap_ + 16);
    total_popcount += BitUtil::PopCount(shift_word(current, next, offset_));
    current = next;
    next = load_word(bitmap_ + 24);
    total_popcount += BitUtil::PopCount(shift_word(current, next, offset_));
    current = next;
    next = load_word(bitmap_ + 32);
    total_popcount += BitUtil::PopCount(shift_word(current, next, offset_));
  }
  bitmap_ += BitUtil::BytesForBits(kTargetBlockLength);
  bits_remaining_ -= 256;
  return {256, static_cast<int16_t>(total_popcount)};
}

}  // namespace internal
}  // namespace arrow
