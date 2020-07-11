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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>

#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;

namespace internal {

namespace detail {

// These templates are here to help with unit tests

template <typename T>
struct BitBlockAnd {
  static T Call(T left, T right) { return left & right; }
};

template <>
struct BitBlockAnd<bool> {
  static bool Call(bool left, bool right) { return left && right; }
};

template <typename T>
struct BitBlockOr {
  static T Call(T left, T right) { return left | right; }
};

template <>
struct BitBlockOr<bool> {
  static bool Call(bool left, bool right) { return left || right; }
};

template <typename T>
struct BitBlockOrNot {
  static T Call(T left, T right) { return left | ~right; }
};

template <>
struct BitBlockOrNot<bool> {
  static bool Call(bool left, bool right) { return left || !right; }
};

}  // namespace detail

/// \brief Return value from bit block counters: the total number of bits and
/// the number of set bits.
struct BitBlockCount {
  int16_t length;
  int16_t popcount;

  bool NoneSet() const { return this->popcount == 0; }
  bool AllSet() const { return this->length == this->popcount; }
};

/// \brief A class that scans through a true/false bitmap to compute popcounts
/// 64 or 256 bits at a time. This is used to accelerate processing of
/// mostly-not-null array data.
class ARROW_EXPORT BitBlockCounter {
 public:
  BitBlockCounter(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap + start_offset / 8),
        bits_remaining_(length),
        offset_(start_offset % 8) {}

  /// \brief The bit size of each word run
  static constexpr int64_t kWordBits = 64;

  /// \brief The bit size of four words run
  static constexpr int64_t kFourWordsBits = kWordBits * 4;

  /// \brief Return the next run of available bits, usually 256. The returned
  /// pair contains the size of run and the number of true values. The last
  /// block will have a length less than 256 if the bitmap length is not a
  /// multiple of 256, and will return 0-length blocks in subsequent
  /// invocations.
  BitBlockCount NextFourWords();

  /// \brief Return the next run of available bits, usually 64. The returned
  /// pair contains the size of run and the number of true values. The last
  /// block will have a length less than 64 if the bitmap length is not a
  /// multiple of 64, and will return 0-length blocks in subsequent
  /// invocations.
  BitBlockCount NextWord();

 private:
  /// \brief Return block with the requested size when doing word-wise
  /// computation is not possible due to inadequate bits remaining.
  BitBlockCount GetBlockSlow(int64_t block_size);

  const uint8_t* bitmap_;
  int64_t bits_remaining_;
  int64_t offset_;
};

/// \brief A tool to iterate through a possibly non-existent validity bitmap,
/// to allow us to write one code path for both the with-nulls and no-nulls
/// cases without giving up a lot of performance.
class ARROW_EXPORT OptionalBitBlockCounter {
 public:
  // validity_bitmap may be nullptr
  OptionalBitBlockCounter(const uint8_t* validity_bitmap, int64_t offset, int64_t length);

  // validity_bitmap may be null
  OptionalBitBlockCounter(const std::shared_ptr<Buffer>& validity_bitmap, int64_t offset,
                          int64_t length);

  /// Return block count for next word when the bitmap is available otherwise
  /// return a block with length up to INT16_MAX when there is no validity
  /// bitmap (so all the referenced values are not null).
  BitBlockCount NextBlock() {
    static constexpr int64_t kMaxBlockSize = std::numeric_limits<int16_t>::max();
    if (has_bitmap_) {
      BitBlockCount block = counter_.NextWord();
      position_ += block.length;
      return block;
    } else {
      int16_t block_size =
          static_cast<int16_t>(std::min(kMaxBlockSize, length_ - position_));
      position_ += block_size;
      // All values are non-null
      return {block_size, block_size};
    }
  }

  // Like NextBlock, but returns a word-sized block even when there is no
  // validity bitmap
  BitBlockCount NextWord() {
    static constexpr int64_t kWordSize = 64;
    if (has_bitmap_) {
      BitBlockCount block = counter_.NextWord();
      position_ += block.length;
      return block;
    } else {
      int16_t block_size = static_cast<int16_t>(std::min(kWordSize, length_ - position_));
      position_ += block_size;
      // All values are non-null
      return {block_size, block_size};
    }
  }

 private:
  BitBlockCounter counter_;
  int64_t position_;
  int64_t length_;
  bool has_bitmap_;
};

/// \brief A class that computes popcounts on the result of bitwise operations
/// between two bitmaps, 64 bits at a time. A 64-bit word is loaded from each
/// bitmap, then the popcount is computed on e.g. the bitwise-and of the two
/// words.
class ARROW_EXPORT BinaryBitBlockCounter {
 public:
  BinaryBitBlockCounter(const uint8_t* left_bitmap, int64_t left_offset,
                        const uint8_t* right_bitmap, int64_t right_offset, int64_t length)
      : left_bitmap_(left_bitmap + left_offset / 8),
        left_offset_(left_offset % 8),
        right_bitmap_(right_bitmap + right_offset / 8),
        right_offset_(right_offset % 8),
        bits_remaining_(length) {}

  /// \brief Return the popcount of the bitwise-and of the next run of
  /// available bits, up to 64. The returned pair contains the size of run and
  /// the number of true values. The last block will have a length less than 64
  /// if the bitmap length is not a multiple of 64, and will return 0-length
  /// blocks in subsequent invocations.
  BitBlockCount NextAndWord();

  /// \brief Computes "x | y" block for each available run of bits.
  BitBlockCount NextOrWord();

  /// \brief Computes "x | ~y" block for each available run of bits.
  BitBlockCount NextOrNotWord();

 private:
  template <template <typename T> class Op>
  BitBlockCount NextWord();

  const uint8_t* left_bitmap_;
  int64_t left_offset_;
  const uint8_t* right_bitmap_;
  int64_t right_offset_;
  int64_t bits_remaining_;
};

}  // namespace internal
}  // namespace arrow
