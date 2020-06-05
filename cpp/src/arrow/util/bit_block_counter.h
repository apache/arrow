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

#include "arrow/util/visibility.h"

namespace arrow {
namespace internal {

struct BitBlockCount {
  int16_t length;
  int16_t popcount;
};

/// \brief A class that scans through a true/false bitmap to yield blocks of up
/// to 256 bits at a time along with their popcount. This is used to accelerate
/// processing of mostly-not-null array data.
class ARROW_EXPORT BitBlockCounter {
 public:

  static constexpr int16_t kTargetBlockLength = 256;

  BitBlockCounter(const uint8_t* bitmap, int64_t start_offset, int64_t length)
      : bitmap_(bitmap + start_offset / 8),
        bits_remaining_(length),
        offset_(start_offset % 8) {}

  /// \brief Return the next run of available bits, up to 256. The returned
  /// pair contains the size of run and the number of true values
  BitBlockCount NextBlock();

 private:
  const uint8_t* bitmap_;
  int64_t bits_remaining_;
  int64_t offset_;
};


/// \brief A class that computes "co-popcounts" of two bitmaps, 64 bits at a
/// time. A 64-bit word is loaded from each bitmap, then the popcount is
/// computed on the bitwise-and of the two words. So the returned
class ARROW_EXPORT BinaryBitBlockCounter {
 public:
  BinaryBitBlockCounter(const uint8_t* left_bitmap, int64_t left_offset,
                        const uint8_t* right_bitmap, int64_t right_offset,
                        int64_t length)
      : left_bitmap_(left_bitmap + left_offset / 8),
        left_offset_(left_offset % 8),
        right_bitmap_(right_bitmap + right_offset / 8),
        right_offset_(right_offset % 8),
        bits_remaining_(length) {}

  /// \brief Return the next run of available bits, up to 256. The returned
  /// pair contains the size of run and the number of true values
  BitBlockCount NextBlock();

 private:
  const uint8_t* left_bitmap_;
  int64_t left_offset_;
  const uint8_t* right_bitmap_;
  int64_t right_offset_;
  int64_t bits_remaining_;
};

}  // namespace internal
}  // namespace arrow
