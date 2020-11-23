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

#include "arrow/util/bit_run_reader.h"

#include <cstdint>

#include "arrow/util/bit_util.h"

namespace arrow {
namespace internal {

#if ARROW_LITTLE_ENDIAN

BitRunReader::BitRunReader(const uint8_t* bitmap, int64_t start_offset, int64_t length)
    : bitmap_(bitmap + (start_offset / 8)),
      position_(start_offset % 8),
      length_(position_ + length) {
  if (ARROW_PREDICT_FALSE(length == 0)) {
    word_ = 0;
    return;
  }

  // On the initial load if there is an offset we need to account for this when
  // loading bytes.  Every other call to LoadWord() should only occur when
  // position_ is a multiple of 64.
  current_run_bit_set_ = !BitUtil::GetBit(bitmap, start_offset);
  int64_t bits_remaining = length + position_;

  LoadWord(bits_remaining);

  // Prepare for inversion in NextRun.
  // Clear out any preceding bits.
  word_ = word_ & ~BitUtil::LeastSignificantBitMask(position_);
}

#endif

}  // namespace internal
}  // namespace arrow
