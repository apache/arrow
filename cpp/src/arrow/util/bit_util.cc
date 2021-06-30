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

#include "arrow/util/bit_util.h"

#include <cstdint>
#include <cstring>

#include "arrow/util/logging.h"

namespace arrow {
namespace BitUtil {

void SetBitsTo(uint8_t* bits, int64_t start_offset, int64_t length, bool bits_are_set) {
  if (length == 0) {
    return;
  }

  const int64_t i_begin = start_offset;
  const int64_t i_end = start_offset + length;
  const uint8_t fill_byte = static_cast<uint8_t>(-static_cast<uint8_t>(bits_are_set));

  const int64_t bytes_begin = i_begin / 8;
  const int64_t bytes_end = i_end / 8 + 1;

  const uint8_t first_byte_mask = kPrecedingBitmask[i_begin % 8];
  const uint8_t last_byte_mask = kTrailingBitmask[i_end % 8];

  if (bytes_end == bytes_begin + 1) {
    // set bits within a single byte
    const uint8_t only_byte_mask =
        i_end % 8 == 0 ? first_byte_mask
                       : static_cast<uint8_t>(first_byte_mask | last_byte_mask);
    bits[bytes_begin] &= only_byte_mask;
    bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~only_byte_mask);
    return;
  }

  // set/clear trailing bits of first byte
  bits[bytes_begin] &= first_byte_mask;
  bits[bytes_begin] |= static_cast<uint8_t>(fill_byte & ~first_byte_mask);

  if (bytes_end - bytes_begin > 2) {
    // set/clear whole bytes
    std::memset(bits + bytes_begin + 1, fill_byte,
                static_cast<size_t>(bytes_end - bytes_begin - 2));
  }

  if (i_end % 8 == 0) {
    return;
  }

  // set/clear leading bits of last byte
  bits[bytes_end - 1] &= last_byte_mask;
  bits[bytes_end - 1] |= static_cast<uint8_t>(fill_byte & ~last_byte_mask);
}

template <bool value>
void SetBitmapImpl(uint8_t* data, int64_t offset, int64_t length) {
  //                 offset  length
  // data              |<------------->|
  //   |--------|...|--------|...|--------|
  //                   |<--->|   |<--->|
  //                     pro       epi
  if (ARROW_PREDICT_FALSE(length == 0)) {
    return;
  }

  constexpr uint8_t set_byte = value ? UINT8_MAX : 0;

  auto prologue = static_cast<int32_t>(BitUtil::RoundUp(offset, 8) - offset);
  DCHECK_LT(prologue, 8);

  if (length < prologue) {  // special case where a mask is required
    //             offset length
    // data             |<->|
    //   |--------|...|--------|...
    //         mask --> |111|
    //                  |<---->|
    //                     pro
    uint8_t mask = BitUtil::kPrecedingBitmask[8 - prologue] ^
                   BitUtil::kPrecedingBitmask[8 - prologue + length];
    data[offset / 8] = value ? data[offset / 8] | mask : data[offset / 8] & ~mask;
    return;
  }

  // align to a byte boundary
  data[offset / 8] = BitUtil::SpliceWord(8 - prologue, data[offset / 8], set_byte);
  offset += prologue;
  length -= prologue;

  // set values per byte
  DCHECK_EQ(offset % 8, 0);
  std::memset(data + offset / 8, set_byte, length / 8);
  offset += BitUtil::RoundDown(length, 8);
  length -= BitUtil::RoundDown(length, 8);

  // clean up
  DCHECK_LT(length, 8);
  data[offset / 8] =
      BitUtil::SpliceWord(static_cast<int32_t>(length), set_byte, data[offset / 8]);
}

void SetBitmap(uint8_t* data, int64_t offset, int64_t length) {
  SetBitmapImpl<true>(data, offset, length);
}

void ClearBitmap(uint8_t* data, int64_t offset, int64_t length) {
  SetBitmapImpl<false>(data, offset, length);
}

}  // namespace BitUtil
}  // namespace arrow
