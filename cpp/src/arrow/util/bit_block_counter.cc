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
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/ubsan.h"

namespace arrow {
namespace internal {

static inline uint64_t LoadWord(const uint8_t* bytes) {
  return BitUtil::ToLittleEndian(util::SafeLoadAs<uint64_t>(bytes));
}

static inline uint64_t ShiftWord(uint64_t current, uint64_t next, int64_t shift) {
  if (shift == 0) {
    return current;
  }
  return (current >> shift) | (next << (64 - shift));
}

BitBlockCount BitBlockCounter::GetBlockSlow(int64_t block_size) {
  const int16_t run_length = static_cast<int16_t>(std::min(bits_remaining_, block_size));
  int16_t popcount = static_cast<int16_t>(CountSetBits(bitmap_, offset_, run_length));
  bits_remaining_ -= run_length;
  // This code path should trigger _at most_ 2 times. In the "two times"
  // case, the first time the run length will be a multiple of 8 by construction
  bitmap_ += run_length / 8;
  return {run_length, popcount};
}

BitBlockCount BitBlockCounter::NextWord() {
  if (!bits_remaining_) {
    return {0, 0};
  }
  int64_t popcount = 0;
  if (offset_ == 0) {
    if (bits_remaining_ < kWordBits) {
      return GetBlockSlow(kWordBits);
    }
    popcount = BitUtil::PopCount(LoadWord(bitmap_));
  } else {
    // When the offset is > 0, we need there to be a word beyond the last
    // aligned word in the bitmap for the bit shifting logic.
    if (bits_remaining_ < 2 * kWordBits - offset_) {
      return GetBlockSlow(kWordBits);
    }
    popcount =
        BitUtil::PopCount(ShiftWord(LoadWord(bitmap_), LoadWord(bitmap_ + 8), offset_));
  }
  bitmap_ += kWordBits / 8;
  bits_remaining_ -= kWordBits;
  return {64, static_cast<int16_t>(popcount)};
}

BitBlockCount BitBlockCounter::NextFourWords() {
  if (!bits_remaining_) {
    return {0, 0};
  }
  int64_t total_popcount = 0;
  if (offset_ == 0) {
    if (bits_remaining_ < kFourWordsBits) {
      return GetBlockSlow(kFourWordsBits);
    }
    total_popcount += BitUtil::PopCount(LoadWord(bitmap_));
    total_popcount += BitUtil::PopCount(LoadWord(bitmap_ + 8));
    total_popcount += BitUtil::PopCount(LoadWord(bitmap_ + 16));
    total_popcount += BitUtil::PopCount(LoadWord(bitmap_ + 24));
  } else {
    // When the offset is > 0, we need there to be a word beyond the last
    // aligned word in the bitmap for the bit shifting logic.
    if (bits_remaining_ < 5 * kFourWordsBits - offset_) {
      return GetBlockSlow(kFourWordsBits);
    }
    auto current = LoadWord(bitmap_);
    auto next = LoadWord(bitmap_ + 8);
    total_popcount += BitUtil::PopCount(ShiftWord(current, next, offset_));
    current = next;
    next = LoadWord(bitmap_ + 16);
    total_popcount += BitUtil::PopCount(ShiftWord(current, next, offset_));
    current = next;
    next = LoadWord(bitmap_ + 24);
    total_popcount += BitUtil::PopCount(ShiftWord(current, next, offset_));
    current = next;
    next = LoadWord(bitmap_ + 32);
    total_popcount += BitUtil::PopCount(ShiftWord(current, next, offset_));
  }
  bitmap_ += BitUtil::BytesForBits(kFourWordsBits);
  bits_remaining_ -= kFourWordsBits;
  return {256, static_cast<int16_t>(total_popcount)};
}

OptionalBitBlockCounter::OptionalBitBlockCounter(const uint8_t* validity_bitmap,
                                                 int64_t offset, int64_t length)
    : counter_(validity_bitmap, offset, length),
      position_(0),
      length_(length),
      has_bitmap_(validity_bitmap != nullptr) {}

OptionalBitBlockCounter::OptionalBitBlockCounter(
    const std::shared_ptr<Buffer>& validity_bitmap, int64_t offset, int64_t length)
    : OptionalBitBlockCounter(validity_bitmap ? validity_bitmap->data() : nullptr, offset,
                              length) {}

template <template <typename T> class Op>
BitBlockCount BinaryBitBlockCounter::NextWord() {
  if (!bits_remaining_) {
    return {0, 0};
  }
  // When the offset is > 0, we need there to be a word beyond the last aligned
  // word in the bitmap for the bit shifting logic.
  constexpr int64_t kWordBits = BitBlockCounter::kWordBits;
  const int64_t bits_required_to_use_words =
      std::max(left_offset_ == 0 ? 64 : 64 + (64 - left_offset_),
               right_offset_ == 0 ? 64 : 64 + (64 - right_offset_));
  if (bits_remaining_ < bits_required_to_use_words) {
    const int16_t run_length = static_cast<int16_t>(std::min(bits_remaining_, kWordBits));
    int16_t popcount = 0;
    for (int64_t i = 0; i < run_length; ++i) {
      if (Op<bool>::Call(BitUtil::GetBit(left_bitmap_, left_offset_ + i),
                         BitUtil::GetBit(right_bitmap_, right_offset_ + i))) {
        ++popcount;
      }
    }
    // This code path should trigger _at most_ 2 times. In the "two times"
    // case, the first time the run length will be a multiple of 8.
    left_bitmap_ += run_length / 8;
    right_bitmap_ += run_length / 8;
    bits_remaining_ -= run_length;
    return {run_length, popcount};
  }

  int64_t popcount = 0;
  if (left_offset_ == 0 && right_offset_ == 0) {
    popcount = BitUtil::PopCount(
        Op<uint64_t>::Call(LoadWord(left_bitmap_), LoadWord(right_bitmap_)));
  } else {
    auto left_word =
        ShiftWord(LoadWord(left_bitmap_), LoadWord(left_bitmap_ + 8), left_offset_);
    auto right_word =
        ShiftWord(LoadWord(right_bitmap_), LoadWord(right_bitmap_ + 8), right_offset_);
    popcount = BitUtil::PopCount(Op<uint64_t>::Call(left_word, right_word));
  }
  left_bitmap_ += kWordBits / 8;
  right_bitmap_ += kWordBits / 8;
  bits_remaining_ -= kWordBits;
  return {64, static_cast<int16_t>(popcount)};
}

BitBlockCount BinaryBitBlockCounter::NextAndWord() {
  return NextWord<detail::BitBlockAnd>();
}

BitBlockCount BinaryBitBlockCounter::NextOrWord() {
  return NextWord<detail::BitBlockOr>();
}

BitBlockCount BinaryBitBlockCounter::NextOrNotWord() {
  return NextWord<detail::BitBlockOrNot>();
}

}  // namespace internal
}  // namespace arrow
