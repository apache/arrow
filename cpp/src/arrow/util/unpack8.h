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

#include "arrow/util/endian.h"
#include "arrow/util/visibility.h"

#include <stdint.h>

namespace arrow {
namespace internal {


static constexpr const uint64_t kPdepMask8[] = {
  0x0000000000000000,
  0x0101010101010101,
  0x0303030303030303,
  0x0707070707070707,
  0x0f0f0f0f0f0f0f0f,
  0x1f1f1f1f1f1f1f1f,
  0x3f3f3f3f3f3f3f3f,
  0x7f7f7f7f7f7f7f7f,
  0xffffffffffffffff};

template <typename T>
static inline uint32_t unpackNaive(
    const uint8_t* inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    T* result) {
  ARROW_CHECK(bitWidth >= 1 && bitWidth <= sizeof(T) * 8);
  ARROW_CHECK(inputBufferLen * 8 >= bitWidth * numValues);

  auto mask = BITPACK_MASKS[bitWidth];

  uint64_t bitPosition = 0;
  for (uint32_t i = 0; i < numValues; i++) {
    T val = (*inputBits >> bitPosition) & mask;
    bitPosition += bitWidth;
    while (bitPosition > 8) {
      inputBits++;
      val |= (*inputBits << (8 - (bitPosition - bitWidth))) & mask;
      bitPosition -= 8;
    }
    result[i] = val;
  }
  return numValues;
}

template <>
inline void unpack<uint8_t>(
    const uint8_t* inputBits,
    uint64_t inputBufferLen,
    uint64_t numValues,
    uint8_t bitWidth,
    uint8_t* result) {
  ARROW_CHECK(bitWidth >= 1 && bitWidth <= 8);
  ARROW_CHECK(inputBufferLen * 8 >= bitWidth * numValues);

#if XSIMD_WITH_AVX2

  uint64_t mask = kPdepMask8[bitWidth];
  auto writeEndOffset = result + numValues;

  // Process bitWidth bytes (8 values) a time. Note that for bitWidth 8, the
  // performance of direct memcpy is about the same as this solution.
  while (result + 8 <= writeEndOffset) {
    // Using memcpy() here may result in non-optimized loops by clong.
    uint64_t val = *reinterpret_cast<const uint64_t*>(inputBits);
    *(reinterpret_cast<uint64_t*>(result)) = _pdep_u64(val, mask);
    inputBits += bitWidth;
    result += 8;
  }

  numValues = writeEndOffset - result;
  unpackNaive(
      inputBits, (bitWidth * numValues + 7) / 8, numValues, bitWidth, result);

#else

  unpackNaive<uint8_t>(inputBits, inputBufferLen, numValues, bitWidth, result);

#endif
}



}  // namespace internal
}  // namespace arrow
