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

#pragma once

#include <cstdint>
#include <cstring>
#include <string>

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

struct BitRun {
  int64_t length;
  // Whether bits are set at this point.
  bool set;

  std::string ToString() const {
    return std::string("{Length: ") + std::to_string(length) +
           ", set=" + std::to_string(set) + "}";
  }
};

static inline bool operator==(const BitRun& lhs, const BitRun& rhs) {
  return lhs.length == rhs.length && lhs.set == rhs.set;
}

class BitRunReaderLinear {
 public:
  BitRunReaderLinear(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : reader_(bitmap, start_offset, length) {}

  BitRun NextRun() {
    BitRun rl = {/*length=*/0, reader_.IsSet()};
    // Advance while the values are equal and not at the end of list.
    while (reader_.position() < reader_.length() && reader_.IsSet() == rl.set) {
      rl.length++;
      reader_.Next();
    }
    return rl;
  }

 private:
  BitmapReader reader_;
};

#if ARROW_LITTLE_ENDIAN
/// A convenience class for counting the number of continguous set/unset bits
/// in a bitmap.
class ARROW_EXPORT BitRunReader {
 public:
  /// \brief Constructs new BitRunReader.
  ///
  /// \param[in] bitmap source data
  /// \param[in] start_offset bit offset into the source data
  /// \param[in] length number of bits to copy
  BitRunReader(const uint8_t* bitmap, int64_t start_offset, int64_t length);

  /// Returns a new BitRun containing the number of contiguous
  /// bits with the same value.  length == 0 indicates the
  /// end of the bitmap.
  BitRun NextRun() {
    if (ARROW_PREDICT_FALSE(position_ >= length_)) {
      return {/*length=*/0, false};
    }
    // This implementation relies on a efficient implementations of
    // CountTrailingZeros and assumes that runs are more often then
    // not.  The logic is to incrementally find the next bit change
    // from the current position.  This is done by zeroing all
    // bits in word_ up to position_ and using the TrailingZeroCount
    // to find the index of the next set bit.

    // The runs alternate on each call, so flip the bit.
    current_run_bit_set_ = !current_run_bit_set_;

    int64_t start_position = position_;
    int64_t start_bit_offset = start_position & 63;
    // Invert the word for proper use of CountTrailingZeros and
    // clear bits so CountTrailingZeros can do it magic.
    word_ = ~word_ & ~BitUtil::LeastSignficantBitMask(start_bit_offset);

    // Go  forward until the next change from unset to set.
    int64_t new_bits = BitUtil::CountTrailingZeros(word_) - start_bit_offset;
    position_ += new_bits;

    if (ARROW_PREDICT_FALSE(BitUtil::IsMultipleOf64(position_)) &&
        ARROW_PREDICT_TRUE(position_ < length_)) {
      // Continue extending position while we can advance an entire word.
      // (updates position_ accordingly).
      AdvanceUntilChange();
    }

    return {/*length=*/position_ - start_position, current_run_bit_set_};
  }

 private:
  void AdvanceUntilChange() {
    int64_t new_bits = 0;
    do {
      // Advance the position of the bitmap for loading.
      bitmap_ += sizeof(uint64_t);
      LoadNextWord();
      new_bits = BitUtil::CountTrailingZeros(word_);
      // Continue calculating run length.
      position_ += new_bits;
    } while (ARROW_PREDICT_FALSE(BitUtil::IsMultipleOf64(position_)) &&
             ARROW_PREDICT_TRUE(position_ < length_) && new_bits > 0);
  }

  void LoadNextWord() { return LoadWord(length_ - position_); }

  // Helper method for Loading the next word.
  void LoadWord(int64_t bits_remaining) {
    word_ = 0;
    // we need at least an extra byte in this case.
    if (ARROW_PREDICT_TRUE(bits_remaining >= 64)) {
      std::memcpy(&word_, bitmap_, 8);
    } else {
      int64_t bytes_to_load = BitUtil::BytesForBits(bits_remaining);
      auto word_ptr = reinterpret_cast<uint8_t*>(&word_);
      std::memcpy(word_ptr, bitmap_, bytes_to_load);
      // Ensure stoppage at last bit in bitmap by reversing the next higher
      // order bit.
      BitUtil::SetBitTo(word_ptr, bits_remaining,
                        !BitUtil::GetBit(word_ptr, bits_remaining - 1));
    }

    // Two cases:
    //   1. For unset, CountTrailingZeros works natually so we don't
    //   invert the word.
    //   2. Otherwise invert so we can use CountTrailingZeros.
    if (current_run_bit_set_) {
      word_ = ~word_;
    }
  }
  const uint8_t* bitmap_;
  int64_t position_;
  int64_t length_;
  uint64_t word_;
  bool current_run_bit_set_;
};
#else
using BitRunReader = BitRunReaderLinear;
#endif

}  // namespace internal
}  // namespace arrow
