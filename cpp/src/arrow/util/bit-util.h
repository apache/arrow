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

#ifndef ARROW_UTIL_BIT_UTIL_H
#define ARROW_UTIL_BIT_UTIL_H

#include <cstdint>
#include <limits>
#include <memory>
#include <vector>

#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;
class MemoryPool;
class MutableBuffer;
class Status;

namespace BitUtil {

static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the ~i byte version of kBitmaks
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

static inline int64_t CeilByte(int64_t size) {
  return (size + 7) & ~7;
}

static inline int64_t BytesForBits(int64_t size) {
  return CeilByte(size) / 8;
}

static inline int64_t Ceil2Bytes(int64_t size) {
  return (size + 15) & ~15;
}

static inline bool GetBit(const uint8_t* bits, int64_t i) {
  return static_cast<bool>(bits[i / 8] & kBitmask[i % 8]);
}

static inline bool BitNotSet(const uint8_t* bits, int64_t i) {
  return (bits[i / 8] & kBitmask[i % 8]) == 0;
}

static inline void ClearBit(uint8_t* bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

static inline void SetBit(uint8_t* bits, int64_t i) {
  bits[i / 8] |= kBitmask[i % 8];
}

static inline void SetBitTo(uint8_t* bits, int64_t i, bool bit_is_set) {
  // See https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  bits[i / 8] ^= static_cast<uint8_t>(-bit_is_set ^ bits[i / 8]) & kBitmask[i % 8];
}

static inline int64_t NextPower2(int64_t n) {
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;
  n++;
  return n;
}

static inline bool IsMultipleOf64(int64_t n) {
  return (n & 63) == 0;
}

static inline bool IsMultipleOf8(int64_t n) {
  return (n & 7) == 0;
}

/// Returns 'value' rounded up to the nearest multiple of 'factor'
inline int64_t RoundUp(int64_t value, int64_t factor) {
  return (value + (factor - 1)) / factor * factor;
}

inline int64_t RoundUpToMultipleOf64(int64_t num) {
  // TODO(wesm): is this definitely needed?
  // DCHECK_GE(num, 0);
  constexpr int64_t round_to = 64;
  constexpr int64_t force_carry_addend = round_to - 1;
  constexpr int64_t truncate_bitmask = ~(round_to - 1);
  constexpr int64_t max_roundable_num = std::numeric_limits<int64_t>::max() - round_to;
  if (num <= max_roundable_num) { return (num + force_carry_addend) & truncate_bitmask; }
  // handle overflow case.  This should result in a malloc error upstream
  return num;
}

void BytesToBits(const std::vector<uint8_t>& bytes, uint8_t* bits);
ARROW_EXPORT Status BytesToBits(const std::vector<uint8_t>&, std::shared_ptr<Buffer>*);

}  // namespace BitUtil

// ----------------------------------------------------------------------
// Bitmap utilities

Status ARROW_EXPORT GetEmptyBitmap(
    MemoryPool* pool, int64_t length, std::shared_ptr<MutableBuffer>* result);

/// Copy a bit range of an existing bitmap
///
/// \param[in] pool memory pool to allocate memory from
/// \param[in] bitmap source data
/// \param[in] offset bit offset into the source data
/// \param[in] length number of bits to copy
/// \param[out] out the resulting copy
///
/// \return Status message
Status ARROW_EXPORT CopyBitmap(MemoryPool* pool, const uint8_t* bitmap, int64_t offset,
    int64_t length, std::shared_ptr<Buffer>* out);

/// Compute the number of 1's in the given data array
///
/// \param[in] data a packed LSB-ordered bitmap as a byte array
/// \param[in] bit_offset a bitwise offset into the bitmap
/// \param[in] length the number of bits to inspect in the bitmap relative to the offset
///
/// \return The number of set (1) bits in the range
int64_t ARROW_EXPORT CountSetBits(
    const uint8_t* data, int64_t bit_offset, int64_t length);

bool ARROW_EXPORT BitmapEquals(const uint8_t* left, int64_t left_offset,
    const uint8_t* right, int64_t right_offset, int64_t bit_length);

}  // namespace arrow

#endif  // ARROW_UTIL_BIT_UTIL_H
