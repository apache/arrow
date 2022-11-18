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

#include "bpacking_avx512_icx.h"
#include "arrow/util/logging.h"

#include <algorithm>
#include <type_traits>
#include <limits.h>
#include <immintrin.h>

#include <boost/preprocessor/repetition/repeat_from_to.hpp>

#define ALWAYS_INLINE __attribute__((always_inline))
#define UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#define LIKELY(expr) __builtin_expect(!!(expr), 1)

namespace arrow {
namespace internal {

  /// Specialized round up and down functions for frequently used factors,
  /// like 8 (bits->bytes), 32 (bits->i32), and 64 (bits->i64).
  /// Returns the rounded up number of bytes that fit the number of bits.
constexpr static inline uint32_t RoundUpNumBytes(uint32_t bits) {
    return (bits + 7) >> 3;
}

constexpr static inline uint32_t RoundUpNumi32(uint32_t bits) {
    return (bits + 31) >> 5;
}

constexpr static inline bool IsPowerOf2(int64_t value) {
    return (value & (value - 1)) == 0;
}

constexpr static inline int64_t RoundUp(int64_t value, int64_t factor) {
    return (value + (factor - 1)) / factor * factor;
}

constexpr static inline int64_t Ceil(int64_t value, int64_t divisor) {
    return value / divisor + (value % divisor != 0);
}

inline int64_t BitPacking::NumValuesToUnpack(
    int bit_width, int64_t in_bytes, int64_t num_values) {
  // Check if we have enough input bytes to decode 'num_values'.
  if (bit_width == 0 || RoundUpNumBytes(num_values * bit_width) <= in_bytes) {
    // Limited by output space.
    return num_values;
  } else {
    // Limited by the number of input bytes. Compute the number of values that can be
    // unpacked from the input.
    return (in_bytes * CHAR_BIT) / bit_width;
  }
}

template <typename T>
constexpr bool IsSupportedUnpackingType () {
  return std::is_same<T, uint8_t>::value
      || std::is_same<T, uint16_t>::value
      || std::is_same<T, uint32_t>::value
      || std::is_same<T, uint64_t>::value;
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(int bit_width,
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case i:                                       \
    return UnpackValues<OutType, i>(in, in_bytes, num_values, out);

  switch (bit_width) {
    // Expand cases from 0 to 64.
    BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in_pos = Unpack32Values<OutType, BIT_WIDTH>(in_pos, in_bytes, out_pos);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }
  // Then unpack the final partial batch.
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, BIT_WIDTH>(
        in_pos, in_bytes, remainder_values, out_pos);
  }
  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX(int bit_width,
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  switch (bit_width){
    case 0:
      return BitPacking::UnpackValuesICX_0<OutType>(in, in_bytes, num_values, out);
    case 1:
      return BitPacking::UnpackValuesICX_1<OutType>(in, in_bytes, num_values, out);
    case 2:
      return BitPacking::UnpackValuesICX_2<OutType>(in, in_bytes, num_values, out);
    case 3:
      return BitPacking::UnpackValuesICX_3<OutType>(in, in_bytes, num_values, out);
    case 4:
      return BitPacking::UnpackValuesICX_4<OutType>(in, in_bytes, num_values, out);
    case 5:
      return BitPacking::UnpackValuesICX_5<OutType>(in, in_bytes, num_values, out);
    case 6:
      return BitPacking::UnpackValuesICX_6<OutType>(in, in_bytes, num_values, out);
    case 7:
      return BitPacking::UnpackValuesICX_7<OutType>(in, in_bytes, num_values, out);
    case 8:
      return BitPacking::UnpackValuesICX_8<OutType>(in, in_bytes, num_values, out);
    case 9:
      return BitPacking::UnpackValuesICX_9<OutType>(in, in_bytes, num_values, out);
    case 10:
      return BitPacking::UnpackValuesICX_10<OutType>(in, in_bytes, num_values, out);
    case 11:
      return BitPacking::UnpackValuesICX_11<OutType>(in, in_bytes, num_values, out);
    case 12:
      return BitPacking::UnpackValuesICX_12<OutType>(in, in_bytes, num_values, out);
    case 13:
      return BitPacking::UnpackValuesICX_13<OutType>(in, in_bytes, num_values, out);
    case 14:
      return BitPacking::UnpackValuesICX_14<OutType>(in, in_bytes, num_values, out);
    case 15:
      return BitPacking::UnpackValuesICX_15<OutType>(in, in_bytes, num_values, out);
    case 16:
      return BitPacking::UnpackValuesICX_16<OutType>(in, in_bytes, num_values, out);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_0(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_set1_epi8(0x00);
    _mm512_storeu_si512(out_pos,tmp);
    in_pos += 2;
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 0>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);  
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_1(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");
  
  constexpr int BATCH_SIZE = 64;
  const int64_t values_to_read = NumValuesToUnpack(1, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i tmp = _mm512_maskz_abs_epi8(in64_pos[i],_mm512_set1_epi8(0x01));

    __m128i tmp1 = _mm512_extracti32x4_epi32(tmp,0);
    __m512i result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos,result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp,1);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos,result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp,2);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos,result);
    out_pos += 16;

    tmp1 = _mm512_extracti32x4_epi32(tmp,3);
    result = _mm512_cvtepu8_epi32(tmp1);
    _mm512_storeu_si512(out_pos,result);
    out_pos += 16;

    in_bytes -= (BATCH_SIZE * 1) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    if(remainder_values >= 32)
    {
      in_pos = Unpack32Values<OutType, 1>(in_pos, in_bytes, out_pos);
      remainder_values -= 32;
      out_pos += 32;
      in_bytes -= (32 * 1) / CHAR_BIT;
    }
    in_pos = UnpackUpTo31Values<OutType, 1>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_2(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(2, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint32_t* in32_pos = reinterpret_cast<const uint32_t*>(in);
  OutType* out_pos = out;
  __m512i am = _mm512_set_epi32(30,28,26,24,22,20,18,16,14,12,10,8,6,4,2,0);
  __m512i mask = _mm512_set1_epi32(0x00000003);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi32(in32_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 2) / CHAR_BIT;
    in_pos += 4;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 2>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_3(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(3, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;
  const uint64_t* in64_pos;
  __m512i am = _mm512_set_epi32(45,42,39,36,33,30,27,24,21,18,15,12,9,6,3,0);
  __m512i mask = _mm512_set1_epi32(0x00000007);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in64_pos = reinterpret_cast<const uint64_t*>(in_pos);
    __m512i data = _mm512_set1_epi64(in64_pos[0]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_pos += 6;
    in_bytes -= (BATCH_SIZE * 3) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 3>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_4(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(4, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  const uint64_t* in64_pos = reinterpret_cast<const uint64_t*>(in);
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi32(60,56,52,48,44,40,36,32,28,24,20,16,12,8,4,0);
  __m512i mask = _mm512_set1_epi32(0x0000000f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_set1_epi64(in64_pos[i]);
    __m512i cm = _mm512_multishift_epi64_epi8(am, data);
    cm = _mm512_and_epi32(cm, mask);
    _mm512_storeu_si512(out_pos,cm);
    out_pos += BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * 4) / CHAR_BIT;
    in_pos += 8;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 4>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_5(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(5, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  //__m512i am = _mm512_set_epi8(37,36,37,36,37,36,36,33,33,32,33,32,32,29,32,29,28,25,28,25,28,25,25,24,24,21,24,21,21,20,21,20,17,16,17,16,17,16,16,13,13,12,13,12,12,9,12,9,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0);
  __m512i am = _mm512_set_epi8(8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0,8,5,8,5,8,5,5,4,4,1,4,1,1,0,1,0);
  __m512i cm = _mm512_set_epi8(59,59,38,38,17,17,4,4,55,55,34,34,21,21,0,0,59,59,38,38,17,17,4,4,55,55,34,34,21,21,0,0,59,59,38,38,17,17,4,4,55,55,34,34,21,21,0,0,59,59,38,38,17,17,4,4,55,55,34,34,21,21,0,0);
  __m512i mask = _mm512_set1_epi32(0x0000001f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x1f1f1f1f,in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 20;
    in_bytes -= (BATCH_SIZE * 5) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 5>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_6(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(6, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(9,8,9,8,8,5,8,5,4,1,4,1,1,0,1,0,9,8,9,8,8,5,8,5,4,1,4,1,1,0,1,0,9,8,9,8,8,5,8,5,4,1,4,1,1,0,1,0,9,8,9,8,8,5,8,5,4,1,4,1,1,0,1,0);
  __m512i cm = _mm512_set_epi8(58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0,58,58,36,36,22,22,0,0);
  __m512i mask = _mm512_set1_epi32(0x0000003f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x3f3f3f3f,in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 24;
    in_bytes -= (BATCH_SIZE * 6) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 6>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_7(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(7, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(12,9,12,9,9,8,8,5,5,4,4,1,1,0,1,0,12,9,12,9,9,8,8,5,5,4,4,1,1,0,1,0,12,9,12,9,9,8,8,5,5,4,4,1,1,0,1,0,12,9,12,9,9,8,8,5,5,4,4,1,1,0,1,0);
  __m512i cm = _mm512_set_epi8(57,57,34,34,19,19,4,4,53,53,38,38,23,23,0,0,57,57,34,34,19,19,4,4,53,53,38,38,23,23,0,0,57,57,34,34,19,19,4,4,53,53,38,38,23,23,0,0,57,57,34,34,19,19,4,4,53,53,38,38,23,23,0,0);
  __m512i mask = _mm512_set1_epi32(0x0000007f);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_maskz_expandloadu_epi8(0x7f7f7f7f,in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    __m512i bm = _mm512_shuffle_epi8(data1, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 28;
    in_bytes -= (BATCH_SIZE * 7) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 7>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_8(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(8, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m128i data = _mm_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_si512(out_pos,data1);
    data = _mm_loadu_epi8(in_pos+16);
    data1 = _mm512_cvtepu8_epi32(data);
    _mm512_storeu_si512(out_pos+16,data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 8) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 8>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_9(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(9, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(8,7,7,6,6,5,5,4,4,3,3,2,2,1,1,0,8,7,7,6,6,5,5,4,4,3,3,2,2,1,1,0,8,7,7,6,6,5,5,4,4,3,3,2,2,1,1,0,8,7,7,6,6,5,5,4,4,3,3,2,2,1,1,0);
  __m512i cm = _mm512_set_epi8(63,55,46,38,29,21,12,4,59,51,42,34,25,17,8,0,63,55,46,38,29,21,12,4,59,51,42,34,25,17,8,0,63,55,46,38,29,21,12,4,59,51,42,34,25,17,8,0,63,55,46,38,29,21,12,4,59,51,42,34,25,17,8,0);
  __m512i mask = _mm512_set1_epi32(0x000001ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x01ff01ff01ff01ff,in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 36;
    in_bytes -= (BATCH_SIZE * 9) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 9>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_10(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(10, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(9,8,8,7,7,6,6,5,4,3,3,2,2,1,1,0,9,8,8,7,7,6,6,5,4,3,3,2,2,1,1,0,9,8,8,7,7,6,6,5,4,3,3,2,2,1,1,0,9,8,8,7,7,6,6,5,4,3,3,2,2,1,1,0);
  __m512i cm = _mm512_set_epi8(62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0,62,54,44,36,26,18,8,0);
  __m512i mask = _mm512_set1_epi32(0x000003ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x03ff03ff03ff03ff,in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 40;
    in_bytes -= (BATCH_SIZE * 10) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 10>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_11(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(11, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(10,9,9,8,7,6,6,5,5,4,3,2,2,1,1,0,10,9,9,8,7,6,6,5,5,4,3,2,2,1,1,0,10,9,9,8,7,6,6,5,5,4,3,2,2,1,1,0,10,9,9,8,7,6,6,5,5,4,3,2,2,1,1,0);
  __m512i cm = _mm512_set_epi8(61,53,42,34,31,23,12,4,57,49,46,38,27,19,8,0,61,53,42,34,31,23,12,4,57,49,46,38,27,19,8,0,61,53,42,34,31,23,12,4,57,49,46,38,27,19,8,0,61,53,42,34,31,23,12,4,57,49,46,38,27,19,8,0);
  __m512i mask = _mm512_set1_epi32(0x000007ff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x07ff07ff07ff07ff,in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 44;
    in_bytes -= (BATCH_SIZE * 11) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 11>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_12(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(12, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(11,10,10,9,8,7,7,6,5,4,4,3,2,1,1,0,11,10,10,9,8,7,7,6,5,4,4,3,2,1,1,0,11,10,10,9,8,7,7,6,5,4,4,3,2,1,1,0,11,10,10,9,8,7,7,6,5,4,4,3,2,1,1,0);
  __m512i cm = _mm512_set_epi8(60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0,60,52,40,32,28,20,8,0);
  __m512i mask = _mm512_set1_epi32(0x00000fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x0fff0fff0fff0fff,in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 48;
    in_bytes -= (BATCH_SIZE * 12) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 12>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_13(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(13, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(12,11,10,9,9,8,7,6,5,4,4,3,2,1,1,0,12,11,10,9,9,8,7,6,5,4,4,3,2,1,1,0,12,11,10,9,9,8,7,6,5,4,4,3,2,1,1,0,12,11,10,9,9,8,7,6,5,4,4,3,2,1,1,0);
  __m512i am2 = _mm512_set_epi8(0,0,0,0,0,0,0,0,6,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,6,5,0,0,0,0,0,0);
  __m512i cm1 = _mm512_set_epi8(59,51,46,38,25,17,12,4,63,55,42,34,29,21,8,0,59,51,46,38,25,17,12,4,63,55,42,34,29,21,8,0,59,51,46,38,25,17,12,4,63,55,42,34,29,21,8,0,59,51,46,38,25,17,12,4,63,55,42,34,29,21,8,0);
  __m512i cm2 = _mm512_set_epi8(0,0,0,0,0,0,0,0,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,55,0,0,0,0,0,0,0);
  __m512i mask = _mm512_set1_epi32(0x00001fff);
  __m512i mask1 = _mm512_set_epi64(0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(0x0000000000000000,0x1f00000000000000,0x0000000000000000,0x1f00000000000000,0x0000000000000000,0x1f00000000000000,0x0000000000000000,0x1f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x1fff1fff1fff1fff,in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 52;
    in_bytes -= (BATCH_SIZE * 13) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 13>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_14(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(14, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am = _mm512_set_epi8(13,12,11,10,9,8,8,7,6,5,4,3,2,1,1,0,13,12,11,10,9,8,8,7,6,5,4,3,2,1,1,0,13,12,11,10,9,8,8,7,6,5,4,3,2,1,1,0,13,12,11,10,9,8,8,7,6,5,4,3,2,1,1,0);
  __m512i cm = _mm512_set_epi8(58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0,58,50,44,36,30,22,8,0);
  __m512i mask = _mm512_set1_epi32(0x00003fff);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x3fff3fff3fff3fff,in_pos);
    __m512i bm = _mm512_shuffle_epi8(data, am);
    __m512i dm = _mm512_multishift_epi64_epi8(cm, bm);
    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(dm,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 56;
    in_bytes -= (BATCH_SIZE * 14) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 14>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}


template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_15(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(15, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  __m512i am1 = _mm512_set_epi8(14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,0,14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,0,14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,0,14,13,12,11,10,9,8,7,6,5,4,3,2,1,1,0);
  __m512i am2 = _mm512_set_epi8(0,0,0,0,0,0,0,0,7,6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,7,6,0,0,0,0,0,0);
  __m512i cm1 = _mm512_set_epi8(57,49,42,34,27,19,12,4,61,53,46,38,31,23,8,0,57,49,42,34,27,19,12,4,61,53,46,38,31,23,8,0,57,49,42,34,27,19,12,4,61,53,46,38,31,23,8,0,57,49,42,34,27,19,12,4,61,53,46,38,31,23,8,0);
  __m512i cm2 = _mm512_set_epi8(0,0,0,0,0,0,0,0,53,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,53,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,53,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,53,0,0,0,0,0,0,0);
  __m512i mask = _mm512_set1_epi32(0x00007fff);
  __m512i mask1 = _mm512_set_epi64(0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff,0xffffffffffffffff,0x00ffffffffffffff);
  __m512i mask2 = _mm512_set_epi64(0x0000000000000000,0x7f00000000000000,0x0000000000000000,0x7f00000000000000,0x0000000000000000,0x7f00000000000000,0x0000000000000000,0x7f00000000000000);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m512i data = _mm512_maskz_expandloadu_epi8(0x7fff7fff7fff7fff,in_pos);
    __m512i bm1 = _mm512_shuffle_epi8(data, am1);
    __m512i dm1 = _mm512_multishift_epi64_epi8(cm1, bm1);
    dm1 = _mm512_and_epi32(dm1, mask1);

    __m512i bm2 = _mm512_shuffle_epi8(data, am2);
    __m512i dm2 = _mm512_multishift_epi64_epi8(cm2, bm2);
    dm2 = _mm512_and_epi32(dm2, mask2);

    __m512i em = _mm512_or_epi32(dm1, dm2);

    __m512i out1 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em,0));
    _mm512_storeu_si512(out_pos,_mm512_and_epi32(out1, mask));
    __m512i out2 = _mm512_cvtepu16_epi32(_mm512_extracti64x4_epi64(em,1));
    _mm512_storeu_si512(out_pos+16,_mm512_and_epi32(out2, mask));
    out_pos += BATCH_SIZE;
    in_pos += 60;
    in_bytes -= (BATCH_SIZE * 15) / CHAR_BIT;
  }

  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 15>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackValuesICX_16(
    const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
    OutType* __restrict__ out) {
  static_assert(IsSupportedUnpackingType<OutType>(),
      "Only unsigned integers are supported.");

  constexpr int BATCH_SIZE = 16;
  const int64_t values_to_read = NumValuesToUnpack(16, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  OutType* out_pos = out;

  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    __m256i data = _mm256_loadu_epi8(in_pos);
    __m512i data1 = _mm512_cvtepu16_epi32(data);
    _mm512_storeu_si512(out_pos,data1);
    out_pos += BATCH_SIZE;
    in_pos += 32;
    in_bytes -= (BATCH_SIZE * 16) / CHAR_BIT;
  }
  if (remainder_values > 0) {
    in_pos = UnpackUpTo31Values<OutType, 16>(
        in_pos, in_bytes, remainder_values, out_pos);
  }

  return std::make_pair(in_pos, values_to_read);
}

template <typename OutType>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(int bit_width,
    const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ dict,
    int64_t dict_len, int64_t num_values, OutType* __restrict__ out, int64_t stride,
    bool* __restrict__ decode_error) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case i:                                       \
    return UnpackAndDecodeValues<OutType, i>(   \
        in, in_bytes, dict, dict_len, num_values, out, stride, decode_error);

  switch (bit_width) {
    // Expand cases from 0 to MAX_DICT_BITWIDTH.
    BOOST_PP_REPEAT_FROM_TO(0, 33, UNPACK_VALUES_CASE, ignore);
    default:
      DCHECK(false);
      return std::make_pair(nullptr, -1);
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}
template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> BitPacking::UnpackAndDecodeValues(
    const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ dict,
    int64_t dict_len, int64_t num_values, OutType* __restrict__ out, int64_t stride,
    bool* __restrict__ decode_error) {
  constexpr int BATCH_SIZE = 32;
  const int64_t values_to_read = NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
  const int64_t batches_to_read = values_to_read / BATCH_SIZE;
  const int64_t remainder_values = values_to_read % BATCH_SIZE;
  const uint8_t* in_pos = in;
  uint8_t* out_pos = reinterpret_cast<uint8_t*>(out);
  // First unpack as many full batches as possible.
  for (int64_t i = 0; i < batches_to_read; ++i) {
    in_pos = UnpackAndDecode32Values<OutType, BIT_WIDTH>(
        in_pos, in_bytes, dict, dict_len, reinterpret_cast<OutType*>(out_pos), stride,
        decode_error);
    out_pos += stride * BATCH_SIZE;
    in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
  }
  // Then unpack the final partial batch.
  if (remainder_values > 0) {
    in_pos = UnpackAndDecodeUpTo31Values<OutType, BIT_WIDTH>(
        in_pos, in_bytes, dict, dict_len, remainder_values,
        reinterpret_cast<OutType*>(out_pos), stride, decode_error);
  }
  return std::make_pair(in_pos, values_to_read);
}

// Loop body of unrolled loop that unpacks the value. BIT_WIDTH is the bit width of
// the packed values. 'in_buf' is the start of the input buffer and 'out_vals' is the
// start of the output values array. This function unpacks the VALUE_IDX'th packed value
// from 'in_buf'.
//
// This implements essentially the same algorithm as the (Apache-licensed) code in
// bpacking.c at https://github.com/lemire/FrameOfReference/, but is much more compact
// because it uses templates rather than source-level unrolling of all combinations.
//
// After the template parameters are expanded and constants are propagated, all branches
// and offset/shift calculations should be optimized out, leaving only shifts by constants
// and bitmasks by constants. Calls to this must be stamped out manually or with
// BOOST_PP_REPEAT_FROM_TO: experimentation revealed that the GCC 4.9.2 optimiser was
// not able to fully propagate constants and remove branches when this was called from
// inside a for loop with constant bounds with VALUE_IDX changed to a function argument.
//
// We compute how many 32 bit words we have to read, which is either 1, 2 or 3. If it is
// at least 2, the first two 32 bit words are read as one 64 bit word. Even if only one
// word needs to be read, we try to read 64 bits if it does not lead to buffer overflow
// because benchmarks show that it has a positive effect on performance.
//
// If 'FULL_BATCH' is true, this function call is part of unpacking 32 values, otherwise
// up to 31 values. This is needed to optimise the length of the reads (32 or 64 bits) and
// avoid buffer overflow (if we are unpacking 32 values, we can safely assume an input
// buffer of length 32 * BIT_WIDTH).

template <int BIT_WIDTH, int VALUE_IDX, bool FULL_BATCH>
inline uint64_t ALWAYS_INLINE UnpackValue(const uint8_t* __restrict__ in_buf) {
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

  // Avoid reading past the end of the buffer. We can safely read 64 bits if we know that
  // this is a full batch read (so the input buffer is 32 * BIT_WIDTH long) and there is
  // enough space in the buffer from the current reading point.
  // We try to read 64 bits even when it is not necessary because the benchmarks show it
  // is faster.
  constexpr bool CAN_SAFELY_READ_64_BITS = FULL_BATCH
      && FIRST_BIT_IDX - FIRST_BIT_OFFSET + 64 <= BIT_WIDTH * 32;

  // We do not try to read 64 bits when the bit width is a power of two (unless it is
  // necessary) because performance benchmarks show that it is better this way. This seems
  // to be due to compiler optimisation issues, so we can revisit it when we update the
  // compiler version.
  constexpr bool READ_32_BITS = WORDS_TO_READ == 1
      && (!CAN_SAFELY_READ_64_BITS || IsPowerOf2(BIT_WIDTH));

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

template <typename OutType>
inline void ALWAYS_INLINE DecodeValue(OutType* __restrict__ dict, int64_t dict_len,
    uint32_t idx, OutType* __restrict__ out_val, bool* __restrict__ decode_error) {
  if (UNLIKELY(idx >= dict_len)) {
    *decode_error = true;
  } else {
    // Use memcpy() because we can't assume sufficient alignment in some cases (e.g.
    // 16 byte decimals).
    memcpy(out_val, &dict[idx], sizeof(OutType));
  }
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::Unpack32Values(
    const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
  constexpr int BYTES_TO_READ = RoundUpNumBytes(32 * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);

  // Call UnpackValue for 0 <= i < 32.
#pragma push_macro("UNPACK_VALUE_CALL")
#define UNPACK_VALUE_CALL(ignore1, i, ignore2) \
  out[i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, i, true>(in));

  BOOST_PP_REPEAT_FROM_TO(0, 32, UNPACK_VALUE_CALL, ignore);
  return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUE_CALL")
}

template <typename OutType>
const uint8_t* BitPacking::Unpack32Values(int bit_width, const uint8_t* __restrict__ in,
    int64_t in_bytes, OutType* __restrict__ out) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case i: return Unpack32Values<OutType, i>(in, in_bytes, out);
  
  switch (bit_width) {
    // Expand cases from 0 to 64.
    BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default: DCHECK(false); return in;
  }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecode32Values(const uint8_t* __restrict__ in,
    int64_t in_bytes, OutType* __restrict__ dict, int64_t dict_len,
    OutType* __restrict__ out, int64_t stride, bool* __restrict__ decode_error) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  constexpr int BYTES_TO_READ = RoundUpNumBytes(32 * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  // TODO: this could be optimised further by using SIMD instructions.
  // https://lemire.me/blog/2016/08/25/faster-dictionary-decoding-with-simd-instructions/

  static_assert(BIT_WIDTH <= MAX_DICT_BITWIDTH,
      "Too high bit width for dictionary index.");

  // Call UnpackValue() and DecodeValue() for 0 <= i < 32.
#pragma push_macro("DECODE_VALUE_CALL")
#define DECODE_VALUE_CALL(ignore1, i, ignore2)               \
  {                                                          \
    uint32_t idx = UnpackValue<BIT_WIDTH, i, true>(in);            \
    uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + i * stride; \
    DecodeValue(dict, dict_len, idx, reinterpret_cast<OutType*>(out_pos), decode_error); \
  }

  BOOST_PP_REPEAT_FROM_TO(0, 32, DECODE_VALUE_CALL, ignore);
  return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUE_CALL")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackUpTo31Values(const uint8_t* __restrict__ in,
    int64_t in_bytes, int num_values, OutType* __restrict__ out) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  DCHECK_LE(BIT_WIDTH, sizeof(OutType) * CHAR_BIT) << "BIT_WIDTH too high for output";
  constexpr int MAX_BATCH_SIZE = 31;
  const int BYTES_TO_READ = RoundUpNumBytes(num_values * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  DCHECK_LE(num_values, MAX_BATCH_SIZE);

  // Make sure the buffer is at least 1 byte.
  constexpr int TMP_BUFFER_SIZE = BIT_WIDTH ?
    (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
  uint8_t tmp_buffer[TMP_BUFFER_SIZE];

  const uint8_t* in_buffer = in;
  // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
  // last 32-bit load would go past the end of the buffer.
  if (RoundUp(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
    memcpy(tmp_buffer, in, BYTES_TO_READ);
    in_buffer = tmp_buffer;
  }

#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
  case 31 - i: out[30 - i] = \
      static_cast<OutType>(UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer));

  // Use switch with fall-through cases to minimise branching.
  switch (num_values) {
  // Expand cases from 31 down to 1.
    BOOST_PP_REPEAT_FROM_TO(0, 31, UNPACK_VALUES_CASE, ignore);
    case 0: break;
    default: DCHECK(false);
  }
  return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
const uint8_t* BitPacking::UnpackAndDecodeUpTo31Values(const uint8_t* __restrict__ in,
      int64_t in_bytes, OutType* __restrict__ dict, int64_t dict_len, int num_values,
      OutType* __restrict__ out, int64_t stride, bool* __restrict__ decode_error) {
  static_assert(BIT_WIDTH >= 0, "BIT_WIDTH too low");
  static_assert(BIT_WIDTH <= MAX_BITWIDTH, "BIT_WIDTH too high");
  constexpr int MAX_BATCH_SIZE = 31;
  const int BYTES_TO_READ = RoundUpNumBytes(num_values * BIT_WIDTH);
  DCHECK_GE(in_bytes, BYTES_TO_READ);
  DCHECK_LE(num_values, MAX_BATCH_SIZE);

  // Make sure the buffer is at least 1 byte.
  constexpr int TMP_BUFFER_SIZE = BIT_WIDTH ?
    (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
  uint8_t tmp_buffer[TMP_BUFFER_SIZE];

  const uint8_t* in_buffer = in;
  // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
  // last 32-bit load would go past the end of the buffer.
  if (RoundUp(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
    memcpy(tmp_buffer, in, BYTES_TO_READ);
    in_buffer = tmp_buffer;
  }

#pragma push_macro("DECODE_VALUES_CASE")
#define DECODE_VALUES_CASE(ignore1, i, ignore2)                   \
  case 31 - i: {                                                  \
    uint32_t idx = UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer);     \
    uint8_t* out_pos = reinterpret_cast<uint8_t*>(out) + (30 - i) * stride; \
    DecodeValue(dict, dict_len, idx, reinterpret_cast<OutType*>(out_pos), decode_error); \
  }

  // Use switch with fall-through cases to minimise branching.
  switch (num_values) {
    // Expand cases from 31 down to 1.
    BOOST_PP_REPEAT_FROM_TO(0, 31, DECODE_VALUES_CASE, ignore);
    case 0:
      break;
    default:
      DCHECK(false);
  }
  return in + BYTES_TO_READ;
#pragma pop_macro("DECODE_VALUES_CASE")
}
}
} 
