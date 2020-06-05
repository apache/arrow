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

BitBlockCount BitBlockCounter::NextWord() { return NextWordInline(); }

BitBlockCount BitBlockCounter::NextFourWords() {
  static constexpr int16_t kTargetBlockLength = 256;
  auto load_word = [](const uint8_t* bytes) -> uint64_t {
    return BitUtil::ToLittleEndian(util::SafeLoadAs<uint64_t>(bytes));
  };
  auto shift_word = [](uint64_t current, uint64_t next, int64_t shift) -> uint64_t {
    return (current >> shift) | (next << (64 - shift));
  };
  int64_t total_popcount = 0;
  if (offset_ == 0) {
    if (bits_remaining_ < 256) {
      return GetLastBlock();
    }
    total_popcount += BitUtil::PopCount(load_word(bitmap_));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 8));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 16));
    total_popcount += BitUtil::PopCount(load_word(bitmap_ + 24));
  } else {
    // When the offset is > 0, we need there to be a word beyond the last
    // aligned word in the bitmap for the bit shifting logic.
    if (bits_remaining_ < 320 - offset_) {
      return GetLastBlock();
    }
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

BitBlockCount BinaryBitBlockCounter::NextAndWord() {
  auto load_word = [](const uint8_t* bytes) -> uint64_t {
    return BitUtil::ToLittleEndian(util::SafeLoadAs<uint64_t>(bytes));
  };
  auto shift_word = [](uint64_t current, uint64_t next, int64_t shift) -> uint64_t {
    if (shift == 0) return current;
    return (current >> shift) | (next << (64 - shift));
  };

  // When the offset is > 0, we need there to be a word beyond the last aligned
  // word in the bitmap for the bit shifting logic.
  const int64_t bits_required_to_use_words =
      std::max(left_offset_ == 0 ? 64 : 64 + (64 - left_offset_),
               right_offset_ == 0 ? 64 : 64 + (64 - right_offset_));
  if (bits_remaining_ < bits_required_to_use_words) {
    const int16_t run_length = static_cast<int16_t>(bits_remaining_);
    int16_t popcount = 0;
    for (int64_t i = 0; i < run_length; ++i) {
      if (BitUtil::GetBit(left_bitmap_, left_offset_ + i) &&
          BitUtil::GetBit(right_bitmap_, right_offset_ + i)) {
        ++popcount;
      }
    }
    bits_remaining_ -= run_length;
    return {run_length, popcount};
  }

  int64_t popcount = 0;
  if (left_offset_ == 0 && right_offset_ == 0) {
    popcount = BitUtil::PopCount(load_word(left_bitmap_) & load_word(right_bitmap_));
  } else {
    auto left_word =
        shift_word(load_word(left_bitmap_), load_word(left_bitmap_ + 8), left_offset_);
    auto right_word =
        shift_word(load_word(right_bitmap_), load_word(right_bitmap_ + 8), right_offset_);
    popcount = BitUtil::PopCount(left_word & right_word);
  }
  left_bitmap_ += 8;
  right_bitmap_ += 8;
  bits_remaining_ -= 64;
  return {64, static_cast<int16_t>(popcount)};
}

}  // namespace internal
}  // namespace arrow
