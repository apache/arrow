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

#include <bit>
#include <cstdint>
#include <type_traits>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace detail {

template <typename Integer>
typename std::make_unsigned<Integer>::type as_unsigned(Integer x) {
  return static_cast<typename std::make_unsigned<Integer>::type>(x);
}

}  // namespace detail

namespace bit_util {

//
// Bit-related computations on integer values
//

// Returns the ceil of value/divisor
constexpr int64_t CeilDiv(int64_t value, int64_t divisor) {
  return (value == 0) ? 0 : 1 + (value - 1) / divisor;
}

// Return the number of bytes needed to fit the given number of bits
constexpr int64_t BytesForBits(int64_t bits) {
  // This formula avoids integer overflow on very large `bits`
  return (bits >> 3) + ((bits & 7) != 0);
}

// Returns the smallest power of two that contains v.  If v is already a
// power of two, it is returned as is.
static inline int64_t NextPower2(int64_t n) {
  // Taken from
  // http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
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

constexpr bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

constexpr bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

// Returns a mask for the bit_index lower order bits.
// Valid in the range `[0, 8*sizof(Uint)]` if `kAllowUpperBound`
// otherwise `[0, 8*sizof(Uint)[`
template <typename Uint, bool kAllowUpperBound = false>
constexpr auto LeastSignificantBitMask(Uint bit_index) {
  if constexpr (kAllowUpperBound) {
    if (bit_index == 8 * sizeof(Uint)) {
      return ~Uint{0};
    }
  }
  return (Uint{1} << bit_index) - Uint{1};
}

// Returns 'value' rounded up to the nearest multiple of 'factor'
constexpr int64_t RoundUp(int64_t value, int64_t factor) {
  return CeilDiv(value, factor) * factor;
}

// Returns 'value' rounded down to the nearest multiple of 'factor'
constexpr int64_t RoundDown(int64_t value, int64_t factor) {
  return (value / factor) * factor;
}

// Returns 'value' rounded up to the nearest multiple of 'factor' when factor
// is a power of two.
// The result is undefined on overflow, i.e. if `value > 2**64 - factor`,
// since we cannot return the correct result which would be 2**64.
constexpr int64_t RoundUpToPowerOf2(int64_t value, int64_t factor) {
  return (value + (factor - 1)) & ~(factor - 1);
}

constexpr uint64_t RoundUpToPowerOf2(uint64_t value, uint64_t factor) {
  return (value + (factor - 1)) & ~(factor - 1);
}

constexpr int64_t RoundUpToMultipleOf8(int64_t num) { return RoundUpToPowerOf2(num, 8); }

constexpr int64_t RoundUpToMultipleOf64(int64_t num) {
  return RoundUpToPowerOf2(num, 64);
}

// Returns the number of bytes covering a sliced bitmap. Find the length
// rounded to cover full bytes on both extremities.
//
// The following example represents a slice (offset=10, length=9)
//
// 0       8       16     24
// |-------|-------|------|
//           [       ]          (slice)
//         [             ]      (same slice aligned to bytes bounds, length=16)
//
// The covering bytes is the length (in bytes) of this new aligned slice.
constexpr int64_t CoveringBytes(int64_t offset, int64_t length) {
  return (bit_util::RoundUp(length + offset, 8) - bit_util::RoundDown(offset, 8)) / 8;
}

// Returns the 'num_bits' least-significant bits of 'v'.
static inline uint64_t TrailingBits(uint64_t v, int num_bits) {
  if (ARROW_PREDICT_FALSE(num_bits == 0)) return 0;
  if (ARROW_PREDICT_FALSE(num_bits >= 64)) return v;
  int n = 64 - num_bits;
  return (v << n) >> n;
}

// Returns ceil(log2(x)).
static inline int Log2(uint64_t x) {
  // DCHECK_GT(x, 0);

  // TODO: We can remove this condition once CRAN upgrades its macOS
  // SDK from 11.3.
#if defined(__clang__) && !defined(__cpp_lib_bitops)
  return std::log2p1(x - 1);
#else
  return std::bit_width(x - 1);
#endif
}

//
// Utilities for reading and writing individual bits by their index
// in a memory area.
//

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the bitwise complement version of kBitmask
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247, 239, 223, 191, 127};

// Bitmask selecting the (k - 1) preceding bits in a byte
static constexpr uint8_t kPrecedingBitmask[] = {0, 1, 3, 7, 15, 31, 63, 127};
static constexpr uint8_t kPrecedingWrappingBitmask[] = {255, 1, 3, 7, 15, 31, 63, 127};

// the bitwise complement version of kPrecedingBitmask
static constexpr uint8_t kTrailingBitmask[] = {255, 254, 252, 248, 240, 224, 192, 128};

static constexpr bool GetBit(const uint8_t* bits, uint64_t i) {
  return (bits[i >> 3] >> (i & 0x07)) & 1;
}

// Gets the i-th bit from a byte. Should only be used with i <= 7.
static constexpr bool GetBitFromByte(uint8_t byte, uint8_t i) {
  return byte & kBitmask[i];
}

static inline void ClearBit(uint8_t* bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

static inline void SetBit(uint8_t* bits, int64_t i) { bits[i / 8] |= kBitmask[i % 8]; }

static inline void SetBitTo(uint8_t* bits, int64_t i, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[i / 8] ^= static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) &
                 kBitmask[i % 8];
}

/// \brief set or clear a range of bits quickly
ARROW_EXPORT
void SetBitsTo(uint8_t* bits, int64_t start_offset, int64_t length, bool bits_are_set);

/// \brief Sets all bits in the bitmap to true
ARROW_EXPORT
void SetBitmap(uint8_t* data, int64_t offset, int64_t length);

/// \brief Clears all bits in the bitmap (set to false)
ARROW_EXPORT
void ClearBitmap(uint8_t* data, int64_t offset, int64_t length);

/// Returns a mask with lower i bits set to 1. If i >= sizeof(Word)*8, all-ones will be
/// returned
/// ex:
/// ref: https://stackoverflow.com/a/59523400
template <typename Word>
constexpr Word PrecedingWordBitmask(const unsigned int i) {
  return static_cast<Word>(static_cast<Word>(i < sizeof(Word) * 8)
                           << (i & (sizeof(Word) * 8 - 1))) -
         1;
}
static_assert(PrecedingWordBitmask<uint8_t>(0) == 0x00, "");
static_assert(PrecedingWordBitmask<uint8_t>(4) == 0x0f, "");
static_assert(PrecedingWordBitmask<uint8_t>(8) == 0xff, "");
static_assert(PrecedingWordBitmask<uint16_t>(8) == 0x00ff, "");

/// \brief Create a word with low `n` bits from `low` and high `sizeof(Word)-n` bits
/// from `high`.
/// Word ret
/// for (i = 0; i < sizeof(Word)*8; i++){
///     ret[i]= i < n ? low[i]: high[i];
/// }
template <typename Word>
constexpr Word SpliceWord(int n, Word low, Word high) {
  return (high & ~PrecedingWordBitmask<Word>(n)) | (low & PrecedingWordBitmask<Word>(n));
}

/// \brief Pack integers into a bitmap in batches of 8
template <int batch_size>
void PackBits(const uint32_t* values, uint8_t* out) {
  for (int i = 0; i < batch_size / 8; ++i) {
    *out++ = static_cast<uint8_t>(values[0] | values[1] << 1 | values[2] << 2 |
                                  values[3] << 3 | values[4] << 4 | values[5] << 5 |
                                  values[6] << 6 | values[7] << 7);
    values += 8;
  }
}

constexpr int32_t MaxLEB128ByteLen(int32_t n_bits) {
  return static_cast<int32_t>(CeilDiv(n_bits, 7));
}

template <typename Int>
constexpr int32_t kMaxLEB128ByteLenFor = MaxLEB128ByteLen(sizeof(Int) * 8);

/// Write a integer as LEB128
///
/// Write the input value as LEB128 into the outptut buffer and return the number of bytes
/// written.
/// If the output buffer size is insufficient, return 0 but the output may have been
/// written to.
/// The input value can be a signed integer, but must be non negative.
///
/// \see https://en.wikipedia.org/wiki/LEB128
/// \see MaxLEB128ByteLenFor
template <typename Int>
constexpr int32_t WriteLEB128(Int value, uint8_t* out, int32_t max_out_size) {
  constexpr Int kLow7Mask = Int(0x7F);
  constexpr Int kHigh7Mask = ~kLow7Mask;
  constexpr uint8_t kContinuationBit = 0x80;

  // This encoding does not work for negative values
  if constexpr (std::is_signed_v<Int>) {
    if (ARROW_PREDICT_FALSE(value < 0)) {
      return 0;
    }
  }

  const auto out_first = out;

  // Write as many bytes as we could be for the given input
  while ((value & kHigh7Mask) != Int(0)) {
    // We do not have enough room to write the LEB128
    if (ARROW_PREDICT_FALSE(out - out_first >= max_out_size)) {
      return 0;
    }

    // Write the encoded byte with continuation bit
    *out = static_cast<uint8_t>(value & kLow7Mask) | kContinuationBit;
    ++out;
    // Shift remaining data
    value >>= 7;
  }

  // We do not have enough room to write the LEB128
  if (ARROW_PREDICT_FALSE(out - out_first >= max_out_size)) {
    return 0;
  }

  // Write last non-continuing byte
  *out = static_cast<uint8_t>(value & kLow7Mask);
  ++out;

  return static_cast<int32_t>(out - out_first);
}

/// Parse a leading LEB128
///
/// Take as input a data pointer and the maximum number of bytes that can be read from it
/// (typically the array size).
/// When a valid LEB128 is found at the start of the data, the function writes it to the
/// out pointer and return the number of bytes read.
/// Otherwise, the out pointer is unmodified and zero is returned.
///
/// \see https://en.wikipedia.org/wiki/LEB128
/// \see MaxLEB128ByteLenFor
template <typename Int>
constexpr int32_t ParseLeadingLEB128(const uint8_t* data, int32_t max_data_size,
                                     Int* out) {
  constexpr auto kMaxBytes = kMaxLEB128ByteLenFor<Int>;
  static_assert(kMaxBytes >= 1);
  constexpr uint8_t kLow7Mask = 0x7F;
  constexpr uint8_t kContinuationBit = 0x80;
  constexpr int32_t kSignBitCount = std::is_signed_v<Int> ? 1 : 0;
  // Number of bits allowed for encoding data on the last byte to avoid overflow
  constexpr uint8_t kHighBitCount = (8 * sizeof(Int) - kSignBitCount) % 7;
  // kHighBitCount least significant `0` bits and the rest with `1`
  constexpr uint8_t kHighForbiddenMask = ~((1 << kHighBitCount) - 1);

  // Iteratively building the value
  std::make_unsigned_t<Int> value = 0;

  // Read as many bytes as we could be for the given output.
  for (int32_t i = 0; i < kMaxBytes - 1; i++) {
    // We have not finished reading a valid LEB128, yet we run out of data
    if (ARROW_PREDICT_FALSE(i >= max_data_size)) {
      return 0;
    }

    // Read the byte and set its 7 LSB to in the final value
    const uint8_t byte = data[i];
    value |= static_cast<Int>(byte & kLow7Mask) << (7 * i);

    // Check for lack of continuation flag in MSB
    if ((byte & kContinuationBit) == 0) {
      *out = value;
      return i + 1;
    }
  }

  // Process the last index avoiding overflowing
  constexpr int32_t last = kMaxBytes - 1;

  // We have not finished reading a valid LEB128, yet we run out of data
  if (ARROW_PREDICT_FALSE(last >= max_data_size)) {
    return 0;
  }

  const uint8_t byte = data[last];

  // Need to check if there are bits that would overflow the output.
  // Also checks that there is no continuation.
  if (ARROW_PREDICT_FALSE((byte & kHighForbiddenMask) != 0)) {
    return 0;
  }

  // No longer need to mask since we ensured
  value |= static_cast<Int>(byte) << (7 * last);
  *out = value;
  return last + 1;
}
}  // namespace bit_util
}  // namespace arrow
