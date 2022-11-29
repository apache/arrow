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

#include <immintrin.h>
#include <limits.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

/// Utilities for manipulating bit-packed values. Bit-packing is a technique for
/// compressing integer values that do not use the full range of the integer type.
/// E.g. an array of uint32_t values with range [0, 31] only uses the lower 5 bits
/// of every uint32_t value, or an array of 0/1 booleans only uses the lowest bit
/// of each integer.
///
/// Bit-packing always has a "bit width" parameter that determines the range of
/// representable unsigned values: [0, 2^bit_width - 1]. The packed representation
/// is logically the concatenatation of the lower bits of the input values (in
/// little-endian order). E.g. the values 1, 2, 3, 4 packed with bit width 4 results
/// in the two output bytes: [ 0 0 1 0 | 0 0 0 1 ] [ 0 1 0 0 | 0 0 1 1 ]
///                               2         1           4         3
///
/// Packed values can be split across words, e.g. packing 1, 17 with bit_width 5 results
/// in the two output bytes: [ 0 0 1 | 0 0 0 0 1 ] [ x x x x x x | 1 0 ]
///            lower bits of 17--^         1         next value     ^--upper bits of 17
///
/// Bit widths from 0 to 64 are supported (0 bit width means that every value is 0).
/// The batched unpacking functions operate on batches of 32 values. This batch size
/// is convenient because for every supported bit width, the end of a 32 value batch
/// falls on a byte boundary. It is also large enough to amortise loop overheads.
namespace BitPacking {
static constexpr int MAX_BITWIDTH = sizeof(uint64_t) * 8;
static constexpr int MAX_DICT_BITWIDTH = sizeof(uint32_t) * 8;

constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) {
  return (bits + 7) >> 3;
}

constexpr static inline uint32_t RoundUpNumi32(uint32_t bits) { return (bits + 31) >> 5; }

constexpr static inline bool IsPowerOf2(int64_t value) {
  return (value & (value - 1)) == 0;
}

constexpr static inline int64_t RoundUp(int64_t value, int64_t factor) {
  return (value + (factor - 1)) / factor * factor;
}

constexpr static inline int64_t Ceil(int64_t value, int64_t divisor) {
  return value / divisor + (value % divisor != 0);
}

template <typename T>
constexpr bool IsSupportedUnpackingType() {
  return std::is_same<T, uint8_t>::value || std::is_same<T, uint16_t>::value ||
         std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value;
}

constexpr static inline int64_t NumValuesToUnpack(int bit_width, int64_t in_bytes,
                                                  int64_t num_values) {
  // Check if we have enough input bytes to decode 'num_values'.
  if (bit_width == 0 || RoundUpNumBytes((uint32_t)(num_values * bit_width)) <= in_bytes) {
    // Limited by output space.
    return num_values;
  } else {
    // Limited by the number of input bytes. Compute the number of values that can
    // be unpacked from the input.
    return (in_bytes * CHAR_BIT) / bit_width;
  }
}

template <int BIT_WIDTH, int VALUE_IDX, bool FULL_BATCH>
static inline uint64_t UnpackValue(const uint8_t* __restrict__ in_buf) {
  if (BIT_WIDTH == 0) return 0;

  constexpr int FIRST_BIT_IDX = VALUE_IDX * BIT_WIDTH;
  constexpr int FIRST_WORD_IDX = FIRST_BIT_IDX / 32;
  constexpr int LAST_BIT_IDX = FIRST_BIT_IDX + BIT_WIDTH;
  constexpr int LAST_WORD_IDX = RoundUpNumi32(LAST_BIT_IDX);
  constexpr int WORDS_TO_READ = LAST_WORD_IDX - FIRST_WORD_IDX;
  static_assert(WORDS_TO_READ <= 3, "At most three 32-bit words need to be loaded.");

  constexpr int FIRST_BIT_OFFSET = FIRST_BIT_IDX - FIRST_WORD_IDX * 32;
  constexpr uint64_t mask = BIT_WIDTH == 64 ? ~0L : (1UL << BIT_WIDTH) - 1;
  const uint32_t* const in = reinterpret_cast<const uint32_t*>(in_buf);

  // Avoid reading past the end of the buffer. We can safely read 64 bits if we know
  // that this is a full batch read (so the input buffer is 32 * BIT_WIDTH long) and
  // there is enough space in the buffer from the current reading point.
  // We try to read 64 bits even when it is not necessary because the benchmarks show
  // it is faster.
  constexpr bool CAN_SAFELY_READ_64_BITS =
      FULL_BATCH && FIRST_BIT_IDX - FIRST_BIT_OFFSET + 64 <= BIT_WIDTH * 32;

  // We do not try to read 64 bits when the bit width is a power of two (unless it is
  // necessary) because performance benchmarks show that it is better this way. This
  // seems to be due to compiler optimisation issues, so we can revisit it when we
  // update the compiler version.
  constexpr bool READ_32_BITS =
      WORDS_TO_READ == 1 && (!CAN_SAFELY_READ_64_BITS || IsPowerOf2(BIT_WIDTH));

  if (READ_32_BITS) {
    uint32_t word = in[FIRST_WORD_IDX];
    word >>= FIRST_BIT_OFFSET < 32 ? FIRST_BIT_OFFSET : 0;
    return word & mask;
  }

  uint64_t word = *reinterpret_cast<const uint64_t*>(in + FIRST_WORD_IDX);
  word >>= FIRST_BIT_OFFSET;

  if (WORDS_TO_READ > 2) {
    constexpr int USEFUL_BITS = FIRST_BIT_OFFSET == 0 ? 0 : 64 - FIRST_BIT_OFFSET;
    uint64_t extra_word = in[FIRST_WORD_IDX + 2];
    word |= extra_word << USEFUL_BITS;
  }

  return word & mask;
}

/// Same as Unpack32Values() but templated by BIT_WIDTH.
template <typename OutType, int BIT_WIDTH>
static const uint8_t* Unpack32Values(const uint8_t* __restrict__ in, int64_t in_bytes,
                                     OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
  constexpr int BYTES_TO_READ = RoundUpNumBytes(32 * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  // Call UnpackValue for 0 <= i < 32.
  out[0] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 0, true>(in));
  out[1] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 1, true>(in));
  out[2] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 2, true>(in));
  out[3] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 3, true>(in));
  out[4] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 4, true>(in));
  out[5] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 5, true>(in));
  out[6] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 6, true>(in));
  out[7] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 7, true>(in));
  out[8] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 8, true>(in));
  out[9] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 9, true>(in));
  out[10] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 10, true>(in));
  out[11] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 11, true>(in));
  out[12] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 12, true>(in));
  out[13] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 13, true>(in));
  out[14] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 14, true>(in));
  out[15] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 15, true>(in));
  out[16] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 16, true>(in));
  out[17] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 17, true>(in));
  out[18] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 18, true>(in));
  out[19] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 19, true>(in));
  out[20] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 20, true>(in));
  out[21] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 21, true>(in));
  out[22] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 22, true>(in));
  out[23] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 23, true>(in));
  out[24] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 24, true>(in));
  out[25] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 25, true>(in));
  out[26] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 26, true>(in));
  out[27] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 27, true>(in));
  out[28] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 28, true>(in));
  out[29] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 29, true>(in));
  out[30] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 30, true>(in));
  out[31] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 31, true>(in));
  return in + BYTES_TO_READ;
}
/// Unpack exactly 32 values of 'bit_width' from 'in' to 'out'. 'in' must point to
/// 'in_bytes' of addressable memory, and 'in_bytes' must be at least
/// (32 * bit_width / 8). 'out' must have space for 32 OutType values.
/// 0 <= 'bit_width' <= 64 and 'bit_width' <= # of bits in OutType.
template <typename OutType>
static const uint8_t* Unpack32Values(int bit_width, const uint8_t* __restrict__ in,
                                     int64_t in_bytes, OutType* __restrict__ out) {
  switch (bit_width) {
    case 0:
      return Unpack32Values<OutType, 0>(in, in_bytes, out);
    case 1:
      return Unpack32Values<OutType, 1>(in, in_bytes, out);
    case 2:
      return Unpack32Values<OutType, 2>(in, in_bytes, out);
    case 3:
      return Unpack32Values<OutType, 3>(in, in_bytes, out);
    case 4:
      return Unpack32Values<OutType, 4>(in, in_bytes, out);
    case 5:
      return Unpack32Values<OutType, 5>(in, in_bytes, out);
    case 6:
      return Unpack32Values<OutType, 6>(in, in_bytes, out);
    case 7:
      return Unpack32Values<OutType, 7>(in, in_bytes, out);
    case 8:
      return Unpack32Values<OutType, 8>(in, in_bytes, out);
    case 9:
      return Unpack32Values<OutType, 9>(in, in_bytes, out);
    case 10:
      return Unpack32Values<OutType, 10>(in, in_bytes, out);
    case 11:
      return Unpack32Values<OutType, 11>(in, in_bytes, out);
    case 12:
      return Unpack32Values<OutType, 12>(in, in_bytes, out);
    case 13:
      return Unpack32Values<OutType, 13>(in, in_bytes, out);
    case 14:
      return Unpack32Values<OutType, 14>(in, in_bytes, out);
    case 15:
      return Unpack32Values<OutType, 15>(in, in_bytes, out);
    case 16:
      return Unpack32Values<OutType, 16>(in, in_bytes, out);
    case 17:
      return Unpack32Values<OutType, 17>(in, in_bytes, out);
    case 18:
      return Unpack32Values<OutType, 18>(in, in_bytes, out);
    case 19:
      return Unpack32Values<OutType, 19>(in, in_bytes, out);
    case 20:
      return Unpack32Values<OutType, 20>(in, in_bytes, out);
    case 21:
      return Unpack32Values<OutType, 21>(in, in_bytes, out);
    case 22:
      return Unpack32Values<OutType, 22>(in, in_bytes, out);
    case 23:
      return Unpack32Values<OutType, 23>(in, in_bytes, out);
    case 24:
      return Unpack32Values<OutType, 24>(in, in_bytes, out);
    case 25:
      return Unpack32Values<OutType, 25>(in, in_bytes, out);
    case 26:
      return Unpack32Values<OutType, 26>(in, in_bytes, out);
    case 27:
      return Unpack32Values<OutType, 27>(in, in_bytes, out);
    case 28:
      return Unpack32Values<OutType, 28>(in, in_bytes, out);
    case 29:
      return Unpack32Values<OutType, 29>(in, in_bytes, out);
    case 30:
      return Unpack32Values<OutType, 30>(in, in_bytes, out);
    case 31:
      return Unpack32Values<OutType, 31>(in, in_bytes, out);
    case 32:
      return Unpack32Values<OutType, 32>(in, in_bytes, out);
    default:
      DCHECK(false);
      return in;
  }
}

/// Unpacks 'num_values' values with the given BIT_WIDTH from 'in' to 'out'.
/// 'num_values' must be at most 31. 'in' must point to 'in_bytes' of addressable
/// memory, and 'in_bytes' must be at least ceil(num_values * bit_width / 8).
/// 'out' must have space for 'num_values' OutType values.
/// 0 <= 'bit_width' <= 64 and 'bit_width' <= # of bits in OutType.
template <typename OutType, int BIT_WIDTH>
static const uint8_t* UnpackUpTo31Values(const uint8_t* __restrict__ in, int64_t in_bytes,
                                         int num_values, OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
  constexpr int MAX_BATCH_SIZE = 31;
  const int BYTES_TO_READ = RoundUpNumBytes(num_values * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  DCHECK_LE(num_values, MAX_BATCH_SIZE);

  // Make sure the buffer is at least 1 byte.
  constexpr int TMP_BUFFER_SIZE =
      BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
  uint8_t tmp_buffer[TMP_BUFFER_SIZE];

  const uint8_t* in_buffer = in;
  // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
  // last 32-bit load would go past the end of the buffer.
  if (RoundUp(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
    memcpy(tmp_buffer, in, BYTES_TO_READ);
    in_buffer = tmp_buffer;
  }

  // Use switch with fall-through cases to minimise branching.
  switch (num_values) {
    case 31:
      out[30] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 30, false>(in_buffer));
    case 30:
      out[29] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 29, false>(in_buffer));
    case 29:
      out[28] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 28, false>(in_buffer));
    case 28:
      out[27] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 27, false>(in_buffer));
    case 27:
      out[26] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 26, false>(in_buffer));
    case 26:
      out[25] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 25, false>(in_buffer));
    case 25:
      out[24] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 24, false>(in_buffer));
    case 24:
      out[23] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 23, false>(in_buffer));
    case 23:
      out[22] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 22, false>(in_buffer));
    case 22:
      out[21] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 21, false>(in_buffer));
    case 21:
      out[20] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 20, false>(in_buffer));
    case 20:
      out[19] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 19, false>(in_buffer));
    case 19:
      out[18] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 18, false>(in_buffer));
    case 18:
      out[17] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 17, false>(in_buffer));
    case 17:
      out[16] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 16, false>(in_buffer));
    case 16:
      out[15] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 15, false>(in_buffer));
    case 15:
      out[14] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 14, false>(in_buffer));
    case 14:
      out[13] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 13, false>(in_buffer));
    case 13:
      out[12] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 12, false>(in_buffer));
    case 12:
      out[11] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 11, false>(in_buffer));
    case 11:
      out[10] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 10, false>(in_buffer));
    case 10:
      out[9] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 9, false>(in_buffer));
    case 9:
      out[8] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 8, false>(in_buffer));
    case 8:
      out[7] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 7, false>(in_buffer));
    case 7:
      out[6] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 6, false>(in_buffer));
    case 6:
      out[5] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 5, false>(in_buffer));
    case 5:
      out[4] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 4, false>(in_buffer));
    case 4:
      out[3] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 3, false>(in_buffer));
    case 3:
      out[2] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 2, false>(in_buffer));
    case 2:
      out[1] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 1, false>(in_buffer));
    case 1:
      out[0] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 0, false>(in_buffer));
    case 0:
      break;
    default:
      DCHECK(false);
  }
  return in + BYTES_TO_READ;
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_0(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_set1_epi8(0x00);
    _mm512_storeu_si512(out_pos, tmp);
    in_pos += 2;
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 0>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_1(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 64;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[i], _mm512_set1_epi8(0x01));

    __m128i tmp1 = _mm512_extracti32x4_epi32(tmp, 0);
    __m512i result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 1);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 2);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos, result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp, 3);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos, result);
    out_pos += 16;

    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    if (remainder_values >= 32) {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_2(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint32_t* in32_pos = reinterpret_cast<const uint32_t*>(in);
  OutType* out_pos = out;
  __m512i am =
      _mm512_set_epi32(30, 28, 26, 24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi32(in32_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
    in_pos += 4;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 2>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_3(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(3, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;
  const uint64_t* in64_pos;
  __m512i am =
      _mm512_set_epi32(45, 42, 39, 36, 33, 30, 27, 24, 21, 18, 15, 12, 9, 6, 3, 0);
  __m512i mask = _mm512_set1_epi32(0x00000007);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in64_pos = reinterpret_cast<const uint64_t*>(in_pos);
    __m512i data = _mm512_set1_epi64(in64_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_pos += 6;
    in_bytes -= (BATCH_SIZE * 3) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 3>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_4(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(4, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi32(60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4, 0);
  __m512i mask = _mm512_set1_epi32(0x0000000f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos, cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 4) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 4>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_5(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(5, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(8, 5, 8, 5, 8, 5, 5, 4, 4, 1, 4, 1, 1, 0, 1, 0, 8, 5, 8, 5, 8, 5, 5,
                      4, 4, 1, 4, 1, 1, 0, 1, 0, 8, 5, 8, 5, 8, 5, 5, 4, 4, 1, 4, 1, 1, 0,
                      1, 0, 8, 5, 8, 5, 8, 5, 5, 4, 4, 1, 4, 1, 1, 0, 1, 0);
  __m512i cm = _mm512_set_epi8(
      59, 59, 38, 38, 17, 17, 4, 4, 55, 55, 34, 34, 21, 21, 0, 0, 59, 59, 38, 38, 17, 17,
      4, 4, 55, 55, 34, 34, 21, 21, 0, 0, 59, 59, 38, 38, 17, 17, 4, 4, 55, 55, 34, 34,
      21, 21, 0, 0, 59, 59, 38, 38, 17, 17, 4, 4, 55, 55, 34, 34, 21, 21, 0, 0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 20;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_6(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(6, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(9, 8, 9, 8, 8, 5, 8, 5, 4, 1, 4, 1, 1, 0, 1, 0, 9, 8, 9, 8, 8, 5, 8,
                      5, 4, 1, 4, 1, 1, 0, 1, 0, 9, 8, 9, 8, 8, 5, 8, 5, 4, 1, 4, 1, 1, 0,
                      1, 0, 9, 8, 9, 8, 8, 5, 8, 5, 4, 1, 4, 1, 1, 0, 1, 0);
  __m512i cm = _mm512_set_epi8(
      58, 58, 36, 36, 22, 22, 0, 0, 58, 58, 36, 36, 22, 22, 0, 0, 58, 58, 36, 36, 22, 22,
      0, 0, 58, 58, 36, 36, 22, 22, 0, 0, 58, 58, 36, 36, 22, 22, 0, 0, 58, 58, 36, 36,
      22, 22, 0, 0, 58, 58, 36, 36, 22, 22, 0, 0, 58, 58, 36, 36, 22, 22, 0, 0);
  __m512i mask = _mm512_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 24;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_7(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(7, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(12, 9, 12, 9, 9, 8, 8, 5, 5, 4, 4, 1, 1, 0, 1, 0, 12, 9, 12, 9, 9,
                      8, 8, 5, 5, 4, 4, 1, 1, 0, 1, 0, 12, 9, 12, 9, 9, 8, 8, 5, 5, 4, 4,
                      1, 1, 0, 1, 0, 12, 9, 12, 9, 9, 8, 8, 5, 5, 4, 4, 1, 1, 0, 1, 0);
  __m512i cm = _mm512_set_epi8(
      57, 57, 34, 34, 19, 19, 4, 4, 53, 53, 38, 38, 23, 23, 0, 0, 57, 57, 34, 34, 19, 19,
      4, 4, 53, 53, 38, 38, 23, 23, 0, 0, 57, 57, 34, 34, 19, 19, 4, 4, 53, 53, 38, 38,
      23, 23, 0, 0, 57, 57, 34, 34, 19, 19, 4, 4, 53, 53, 38, 38, 23, 23, 0, 0);
  __m512i mask = _mm512_set1_epi32(0x0000007f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f, in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 28;
    in_bytes -= (BATCH_SIZE * 7) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 7>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_8(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(8, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_si128((__m128i const*)in_pos);
    __m512i data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_si512(out_pos, data1);
    data = _mm_loadu_si128((__m128i const*)(in_pos + 16));
    data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_si512(out_pos + 16, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 8) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 8>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_9(const uint8_t* __restrict__ in,
                                                     int64_t in_bytes, int64_t num_values,
                                                     OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(9, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(8, 7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1, 0, 8, 7, 7, 6, 6, 5, 5,
                      4, 4, 3, 3, 2, 2, 1, 1, 0, 8, 7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1,
                      1, 0, 8, 7, 7, 6, 6, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1, 0);
  __m512i cm = _mm512_set_epi8(
      63, 55, 46, 38, 29, 21, 12, 4, 59, 51, 42, 34, 25, 17, 8, 0, 63, 55, 46, 38, 29, 21,
      12, 4, 59, 51, 42, 34, 25, 17, 8, 0, 63, 55, 46, 38, 29, 21, 12, 4, 59, 51, 42, 34,
      25, 17, 8, 0, 63, 55, 46, 38, 29, 21, 12, 4, 59, 51, 42, 34, 25, 17, 8, 0);
  __m512i mask = _mm512_set1_epi32(0x000001ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x01ff01ff01ff01ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 36;
    in_bytes -= (BATCH_SIZE * 9) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 9>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_10(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(10, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(9, 8, 8, 7, 7, 6, 6, 5, 4, 3, 3, 2, 2, 1, 1, 0, 9, 8, 8, 7, 7, 6, 6,
                      5, 4, 3, 3, 2, 2, 1, 1, 0, 9, 8, 8, 7, 7, 6, 6, 5, 4, 3, 3, 2, 2, 1,
                      1, 0, 9, 8, 8, 7, 7, 6, 6, 5, 4, 3, 3, 2, 2, 1, 1, 0);
  __m512i cm = _mm512_set_epi8(
      62, 54, 44, 36, 26, 18, 8, 0, 62, 54, 44, 36, 26, 18, 8, 0, 62, 54, 44, 36, 26, 18,
      8, 0, 62, 54, 44, 36, 26, 18, 8, 0, 62, 54, 44, 36, 26, 18, 8, 0, 62, 54, 44, 36,
      26, 18, 8, 0, 62, 54, 44, 36, 26, 18, 8, 0, 62, 54, 44, 36, 26, 18, 8, 0);
  __m512i mask = _mm512_set1_epi32(0x000003ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x03ff03ff03ff03ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 40;
    in_bytes -= (BATCH_SIZE * 10) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 10>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_11(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(11, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(10, 9, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 2, 1, 1, 0, 10, 9, 9, 8, 7, 6,
                      6, 5, 5, 4, 3, 2, 2, 1, 1, 0, 10, 9, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2,
                      2, 1, 1, 0, 10, 9, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 2, 1, 1, 0);
  __m512i cm = _mm512_set_epi8(
      61, 53, 42, 34, 31, 23, 12, 4, 57, 49, 46, 38, 27, 19, 8, 0, 61, 53, 42, 34, 31, 23,
      12, 4, 57, 49, 46, 38, 27, 19, 8, 0, 61, 53, 42, 34, 31, 23, 12, 4, 57, 49, 46, 38,
      27, 19, 8, 0, 61, 53, 42, 34, 31, 23, 12, 4, 57, 49, 46, 38, 27, 19, 8, 0);
  __m512i mask = _mm512_set1_epi32(0x000007ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x07ff07ff07ff07ff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 44;
    in_bytes -= (BATCH_SIZE * 11) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 11>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_12(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(12, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am =
      _mm512_set_epi8(11, 10, 10, 9, 8, 7, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0, 11, 10, 10, 9, 8,
                      7, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0, 11, 10, 10, 9, 8, 7, 7, 6, 5, 4, 4,
                      3, 2, 1, 1, 0, 11, 10, 10, 9, 8, 7, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0);
  __m512i cm = _mm512_set_epi8(
      60, 52, 40, 32, 28, 20, 8, 0, 60, 52, 40, 32, 28, 20, 8, 0, 60, 52, 40, 32, 28, 20,
      8, 0, 60, 52, 40, 32, 28, 20, 8, 0, 60, 52, 40, 32, 28, 20, 8, 0, 60, 52, 40, 32,
      28, 20, 8, 0, 60, 52, 40, 32, 28, 20, 8, 0, 60, 52, 40, 32, 28, 20, 8, 0);
  __m512i mask = _mm512_set1_epi32(0x00000fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x0fff0fff0fff0fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 48;
    in_bytes -= (BATCH_SIZE * 12) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 12>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_13(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(13, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 =
      _mm512_set_epi8(12, 11, 10, 9, 9, 8, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0, 12, 11, 10, 9, 9,
                      8, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0, 12, 11, 10, 9, 9, 8, 7, 6, 5, 4, 4,
                      3, 2, 1, 1, 0, 12, 11, 10, 9, 9, 8, 7, 6, 5, 4, 4, 3, 2, 1, 1, 0);
  __m512i am2 =
      _mm512_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 6, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 6, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 5, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 5, 0, 0, 0, 0, 0, 0);
  __m512i cm1 = _mm512_set_epi8(
      59, 51, 46, 38, 25, 17, 12, 4, 63, 55, 42, 34, 29, 21, 8, 0, 59, 51, 46, 38, 25, 17,
      12, 4, 63, 55, 42, 34, 29, 21, 8, 0, 59, 51, 46, 38, 25, 17, 12, 4, 63, 55, 42, 34,
      29, 21, 8, 0, 59, 51, 46, 38, 25, 17, 12, 4, 63, 55, 42, 34, 29, 21, 8, 0);
  __m512i cm2 =
      _mm512_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 55, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 55, 0, 0, 0, 0, 0, 0, 0);
  __m512i mask = _mm512_set1_epi32(0x00001fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff, 0x00ffffffffffffff, 0xffffffffffffffff, 0x00ffffffffffffff,
      0xffffffffffffffff, 0x00ffffffffffffff, 0xffffffffffffffff, 0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000, 0x1f00000000000000, 0x0000000000000000, 0x1f00000000000000,
      0x0000000000000000, 0x1f00000000000000, 0x0000000000000000, 0x1f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x1fff1fff1fff1fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 52;
    in_bytes -= (BATCH_SIZE * 13) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 13>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_14(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(14, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(13, 12, 11, 10, 9, 8, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 13, 12,
                               11, 10, 9, 8, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 13, 12, 11, 10,
                               9, 8, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 13, 12, 11, 10, 9, 8,
                               8, 7, 6, 5, 4, 3, 2, 1, 1, 0);
  __m512i cm = _mm512_set_epi8(
      58, 50, 44, 36, 30, 22, 8, 0, 58, 50, 44, 36, 30, 22, 8, 0, 58, 50, 44, 36, 30, 22,
      8, 0, 58, 50, 44, 36, 30, 22, 8, 0, 58, 50, 44, 36, 30, 22, 8, 0, 58, 50, 44, 36,
      30, 22, 8, 0, 58, 50, 44, 36, 30, 22, 8, 0, 58, 50, 44, 36, 30, 22, 8, 0);
  __m512i mask = _mm512_set1_epi32(0x00003fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x3fff3fff3fff3fff, in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 56;
    in_bytes -= (BATCH_SIZE * 14) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 14>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_15(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(15, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 14,
                                13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 14, 13,
                                12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0, 14, 13, 12,
                                11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 1, 0);
  __m512i am2 =
      _mm512_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 7, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 7, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 6, 0, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 6, 0, 0, 0, 0, 0, 0);
  __m512i cm1 = _mm512_set_epi8(
      57, 49, 42, 34, 27, 19, 12, 4, 61, 53, 46, 38, 31, 23, 8, 0, 57, 49, 42, 34, 27, 19,
      12, 4, 61, 53, 46, 38, 31, 23, 8, 0, 57, 49, 42, 34, 27, 19, 12, 4, 61, 53, 46, 38,
      31, 23, 8, 0, 57, 49, 42, 34, 27, 19, 12, 4, 61, 53, 46, 38, 31, 23, 8, 0);
  __m512i cm2 =
      _mm512_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0, 0, 53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 53, 0, 0, 0,
                      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 53, 0, 0, 0, 0, 0, 0, 0);
  __m512i mask = _mm512_set1_epi32(0x00007fff);
  __m512i mask1 = _mm512_set_epi64(
      0xffffffffffffffff, 0x00ffffffffffffff, 0xffffffffffffffff, 0x00ffffffffffffff,
      0xffffffffffffffff, 0x00ffffffffffffff, 0xffffffffffffffff, 0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(
      0x0000000000000000, 0x7f00000000000000, 0x0000000000000000, 0x7f00000000000000,
      0x0000000000000000, 0x7f00000000000000, 0x0000000000000000, 0x7f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x7fff7fff7fff7fff, in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 0));
    _mm512_storeu_si512(out_pos, _mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em, 1));
    _mm512_storeu_si512(out_pos + 16, _mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 60;
    in_bytes -= (BATCH_SIZE * 15) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 15>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX_16(const uint8_t* __restrict__ in,
                                                      int64_t in_bytes,
                                                      int64_t num_values,
                                                      OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(16, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_loadu_si256((__m256i const*)in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    _mm512_storeu_si512(out_pos, data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 16) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 16>(in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValuesICX(int bit_width,
                                                   const uint8_t* __restrict__ in,
                                                   int64_t in_bytes, int64_t num_values,
                                                   OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
                "Only unsigned integers are supported.");

  switch (bit_width) {
    case 0:
      return UnpackValuesICX_0<OutType>(in, in_bytes, num_values, out);
    case 1:
      return UnpackValuesICX_1<OutType>(in, in_bytes, num_values, out);
    case 2:
      return UnpackValuesICX_2<OutType>(in, in_bytes, num_values, out);
    case 3:
      return UnpackValuesICX_3<OutType>(in, in_bytes, num_values, out);
    case 4:
      return UnpackValuesICX_4<OutType>(in, in_bytes, num_values, out);
    case 5:
      return UnpackValuesICX_5<OutType>(in, in_bytes, num_values, out);
    case 6:
      return UnpackValuesICX_6<OutType>(in, in_bytes, num_values, out);
    case 7:
      return UnpackValuesICX_7<OutType>(in, in_bytes, num_values, out);
    case 8:
      return UnpackValuesICX_8<OutType>(in, in_bytes, num_values, out);
    case 9:
      return UnpackValuesICX_9<OutType>(in, in_bytes, num_values, out);
    case 10:
      return UnpackValuesICX_10<OutType>(in, in_bytes, num_values, out);
    case 11:
      return UnpackValuesICX_11<OutType>(in, in_bytes, num_values, out);
    case 12:
      return UnpackValuesICX_12<OutType>(in, in_bytes, num_values, out);
    case 13:
      return UnpackValuesICX_13<OutType>(in, in_bytes, num_values, out);
    case 14:
      return UnpackValuesICX_14<OutType>(in, in_bytes, num_values, out);
    case 15:
      return UnpackValuesICX_15<OutType>(in, in_bytes, num_values, out);
    case 16:
      return UnpackValuesICX_16<OutType>(in, in_bytes, num_values, out);
    default:
      DCHECK(false);
      return std::make_pair(NULLPTR, -1);
  }
}
}  // namespace BitPacking
}  // namespace internal
}  // namespace arrow
