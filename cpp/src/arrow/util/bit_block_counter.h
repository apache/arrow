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

#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

/// \brief Return value from bit block counters: the total number of bits and
/// the number of set bits.
struct BitBlockCount {
  int16_t length;
  int16_t popcount;
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

 private:
  const uint8_t* left_bitmap_;
  int64_t left_offset_;
  const uint8_t* right_bitmap_;
  int64_t right_offset_;
  int64_t bits_remaining_;
};

}  // namespace internal
}  // namespace arrow
