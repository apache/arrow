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

#ifndef ARROW_UTIL_UTF8_H
#define ARROW_UTIL_UTF8_H

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/neon-util.h"
#include "arrow/util/sse-util.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

// Convert a UTF8 string to a wstring (either UTF16 or UTF32, depending
// on the wchar_t width).
ARROW_EXPORT Result<std::wstring> UTF8ToWideString(const std::string& source);

// Similarly, convert a wstring to a UTF8 string.
ARROW_EXPORT Result<std::string> WideStringToUTF8(const std::wstring& source);

namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

// A compact state table allowing UTF8 decoding using two dependent
// lookups per byte.  The first lookup determines the character class
// and the second lookup reads the next state.
// In this table states are multiples of 12.
ARROW_EXPORT extern const uint8_t utf8_small_table[256 + 9 * 12];

// Success / reject states when looked up in the small table
static constexpr uint8_t kUTF8DecodeAccept = 0;
static constexpr uint8_t kUTF8DecodeReject = 12;

// An expanded state table allowing transitions using a single lookup
// at the expense of a larger memory footprint (but on non-random data,
// not all the table will end up accessed and cached).
// In this table states are multiples of 256.
ARROW_EXPORT extern uint16_t utf8_large_table[9 * 256];

// Success / reject states when looked up in the large table
static constexpr uint16_t kUTF8ValidateAccept = 0;
static constexpr uint16_t kUTF8ValidateReject = 256;

static inline uint8_t DecodeOneUTF8Byte(uint8_t byte, uint8_t state, uint32_t* codep) {
  uint8_t type = utf8_small_table[byte];

  *codep = (state != kUTF8DecodeAccept) ? (byte & 0x3fu) | (*codep << 6)
                                        : (0xff >> type) & (byte);

  state = utf8_small_table[256 + state + type];
  return state;
}

static inline uint16_t ValidateOneUTF8Byte(uint8_t byte, uint16_t state) {
  return utf8_large_table[state + byte];
}

ARROW_EXPORT void CheckUTF8Initialized();

}  // namespace internal

// This function needs to be called before doing UTF8 validation.
ARROW_EXPORT void InitializeUTF8();

inline bool ValidateUTF8(const uint8_t* data, int64_t size) {
  static constexpr uint64_t high_bits_64 = 0x8080808080808080ULL;
  // For some reason, defining this variable outside the loop helps clang
  uint64_t mask;

#ifndef NDEBUG
  internal::CheckUTF8Initialized();
#endif

  while (size >= 8) {
    // XXX This is doing an unaligned access.  Contemporary architectures
    // (x86-64, AArch64, PPC64) support it natively and often have good
    // performance nevertheless.
    memcpy(&mask, data, 8);
    if (ARROW_PREDICT_TRUE((mask & high_bits_64) == 0)) {
      // 8 bytes of pure ASCII, move forward
      size -= 8;
      data += 8;
      continue;
    }
    // Non-ASCII run detected.
    // We process at least 4 bytes, to avoid too many spurious 64-bit reads
    // in case the non-ASCII bytes are at the end of the tested 64-bit word.
    // We also only check for rejection at the end since that state is stable
    // (once in reject state, we always remain in reject state).
    // It is guaranteed that size >= 8 when arriving here, which allows
    // us to avoid size checks.
    uint16_t state = internal::kUTF8ValidateAccept;
    // Byte 0
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 1
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 2
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 3
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 4
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 5
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 6
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 7
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // kUTF8ValidateAccept not reached along 4 transitions has to mean a rejection
    assert(state == internal::kUTF8ValidateReject);
    return false;
  }

  // Validate string tail one byte at a time
  // Note the state table is designed so that, once in the reject state,
  // we remain in that state until the end.  So we needn't check for
  // rejection at each char (we don't gain much by short-circuiting here).
  uint16_t state = internal::kUTF8ValidateAccept;
  while (size-- > 0) {
    state = internal::ValidateOneUTF8Byte(*data++, state);
  }
  return ARROW_PREDICT_TRUE(state == internal::kUTF8ValidateAccept);
}

inline bool ValidateUTF8(const util::string_view& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  const size_t length = str.size();

  return ValidateUTF8(data, length);
}

#ifdef ARROW_HAVE_ARM_NEON
/*
 * Map high nibble of "First Byte" to legal character length minus 1
 * 0x00 ~ 0xBF --> 0
 * 0xC0 ~ 0xDF --> 1
 * 0xE0 ~ 0xEF --> 2
 * 0xF0 ~ 0xFF --> 3
 */
static const uint8_t _first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

/* Map "First Byte" to 8-th item of range table (0xC2 ~ 0xF4) */
static const uint8_t _first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

/*
 * Range table, map range index to min and max values
 * Index 0    : 00 ~ 7F (First Byte, ascii)
 * Index 1,2,3: 80 ~ BF (Second, Third, Fourth Byte)
 * Index 4    : A0 ~ BF (Second Byte after E0)
 * Index 5    : 80 ~ 9F (Second Byte after ED)
 * Index 6    : 90 ~ BF (Second Byte after F0)
 * Index 7    : 80 ~ 8F (Second Byte after F4)
 * Index 8    : C2 ~ F4 (First Byte, non ascii)
 * Index 9~15 : illegal: u >= 255 && u <= 0
 */
static const uint8_t _range_min_tbl[] = {
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
};
static const uint8_t _range_max_tbl[] = {
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
};

/*
 * This table is for fast handling four special First Bytes(E0,ED,F0,F4), after
 * which the Second Byte are not 80~BF. It contains "range index adjustment".
 * - The idea is to minus byte with E0, use the result(0~31) as the index to
 *   lookup the "range index adjustment". Then add the adjustment to original
 *   range index to get the correct range.
 * - Range index adjustment
 *   +------------+---------------+------------------+----------------+
 *   | First Byte | original range| range adjustment | adjusted range |
 *   +------------+---------------+------------------+----------------+
 *   | E0         | 2             | 2                | 4              |
 *   +------------+---------------+------------------+----------------+
 *   | ED         | 2             | 3                | 5              |
 *   +------------+---------------+------------------+----------------+
 *   | F0         | 3             | 3                | 6              |
 *   +------------+---------------+------------------+----------------+
 *   | F4         | 4             | 4                | 8              |
 *   +------------+---------------+------------------+----------------+
 * - Below is a uint8x16x2 table, data is interleaved in NEON register. So I'm
 *   putting it vertically. 1st column is for E0~EF, 2nd column for F0~FF.
 */
static const uint8_t _range_adjust_tbl[] = {
    /* index -> 0~15  16~31 <- index */
    2, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0,
};

/* UTF-8 validation optimization with Arm Neon (SIMD)
 *
 * Algorithm and code come from: https://github.com/cyb70289/utf8 (MIT LICENSE)
 *
 * Range table base algorithm:
 *   1. Map each byte of input-string to Range table.
 *   2. Leverage the Neon 'tbl' instruction to lookup table.
 *   3. Find the pattern and set correct table index for each input byte
 *   4. Validate input string.
 *   Validate 2*16 bytes in each iteration.
 *   Data_Size < 32 Bytes: Fall back to ValidateUTF8.
 */
inline bool ValidateNonAscii(const uint8_t* data, int64_t len) {
  if (len >= 32) {
    uint8x16_t prev_input = vdupq_n_u8(0);
    uint8x16_t prev_first_len = vdupq_n_u8(0);

    /* Cached tables */
    const uint8x16_t first_len_tbl = vld1q_u8(_first_len_tbl);
    const uint8x16_t first_range_tbl = vld1q_u8(_first_range_tbl);
    const uint8x16_t range_min_tbl = vld1q_u8(_range_min_tbl);
    const uint8x16_t range_max_tbl = vld1q_u8(_range_max_tbl);
    const uint8x16x2_t range_adjust_tbl = vld2q_u8(_range_adjust_tbl);

    const uint8x16_t const_1 = vdupq_n_u8(1);
    const uint8x16_t const_2 = vdupq_n_u8(2);
    const uint8x16_t const_e0 = vdupq_n_u8(0xE0);

    uint8x16_t error = vdupq_n_u8(0);

    while (len >= 32) {
      /************************* First 16 bytes ***************************/
      const uint8x16_t input = vld1q_u8(data);

      /* input >> 4 */
      uint8x16_t high_nibbles = vshrq_n_u8(input, 4);

      /* first_len = legal character length minus 1 */
      /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
      /* first_len = first_len_tbl[high_nibbles] */
      const uint8x16_t first_len = vqtbl1q_u8(first_len_tbl, high_nibbles);

      /* First Byte: set range index to 8 for bytes within 0xC0 ~ 0xFF */
      /* range = first_range_tbl[high_nibbles] */
      uint8x16_t range = vqtbl1q_u8(first_range_tbl, high_nibbles);

      /* Second Byte: set range index to first_len */
      /* 0 for 00~7F, 1 for C0~DF, 2 for E0~EF, 3 for F0~FF */
      /* range |= (first_len, prev_first_len) << 1 byte */
      range = vorrq_u8(range, vextq_u8(prev_first_len, first_len, 15));

      /* Third Byte: set range index to saturate_sub(first_len, 1) */
      /* 0 for 00~7F, 0 for C0~DF, 1 for E0~EF, 2 for F0~FF */
      uint8x16_t tmp1, tmp2;
      tmp1 = vqsubq_u8(first_len, const_1);
      tmp2 = vqsubq_u8(prev_first_len, const_1);
      range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 14));

      /* Fourth Byte: set range index to saturate_sub(first_len, 2) */
      /* 0 for 00~7F, 0 for C0~DF, 0 for E0~EF, 1 for F0~FF */
      /* tmp1 = saturate_sub(first_len, 2) */
      tmp1 = vqsubq_u8(first_len, const_2);
      tmp2 = vqsubq_u8(prev_first_len, const_2);
      range = vorrq_u8(range, vextq_u8(tmp2, tmp1, 13));

      /*
       * Now we have below range indices caluclated
       * Correct cases:
       * - 8 for C0~FF
       * - 3 for 1st byte after F0~FF
       * - 2 for 1st byte after E0~EF or 2nd byte after F0~FF
       * - 1 for 1st byte after C0~DF or 2nd byte after E0~EF or
       *         3rd byte after F0~FF
       * - 0 for others
       * Error cases:
       *   9,10,11 if non ascii First Byte overlaps
       *   E.g., F1 80 C2 90 --> 8 3 10 2, where 10 indicates error
       */

      /* Adjust Second Byte range for special First Bytes(E0,ED,F0,F4) */
      /* See _range_adjust_tbl[] definition for details */
      /* Overlaps lead to index 9~15, which are illegal in range table */
      uint8x16_t shift1 = vextq_u8(prev_input, input, 15);
      uint8x16_t pos = vsubq_u8(shift1, const_e0);
      range = vaddq_u8(range, vqtbl2q_u8(range_adjust_tbl, pos));

      /* Load min and max values per calculated range index */
      uint8x16_t minv = vqtbl1q_u8(range_min_tbl, range);
      uint8x16_t maxv = vqtbl1q_u8(range_max_tbl, range);

      /* Check range*/
      error = vorrq_u8(error, vcltq_u8(input, minv));
      error = vorrq_u8(error, vcgtq_u8(input, maxv));

      /************************* Next 16 bytes ***************************/
      const uint8x16_t _input = vld1q_u8(data + 16);

      high_nibbles = vshrq_n_u8(_input, 4);

      const uint8x16_t _first_len = vqtbl1q_u8(first_len_tbl, high_nibbles);

      uint8x16_t _range = vqtbl1q_u8(first_range_tbl, high_nibbles);

      _range = vorrq_u8(_range, vextq_u8(first_len, _first_len, 15));

      tmp1 = vqsubq_u8(_first_len, const_1);
      tmp2 = vqsubq_u8(first_len, const_1);
      _range = vorrq_u8(_range, vextq_u8(tmp2, tmp1, 14));

      tmp1 = vqsubq_u8(_first_len, const_2);
      tmp2 = vqsubq_u8(first_len, const_2);
      _range = vorrq_u8(_range, vextq_u8(tmp2, tmp1, 13));

      shift1 = vextq_u8(input, _input, 15);
      pos = vsubq_u8(shift1, const_e0);
      _range = vaddq_u8(_range, vqtbl2q_u8(range_adjust_tbl, pos));

      minv = vqtbl1q_u8(range_min_tbl, _range);
      maxv = vqtbl1q_u8(range_max_tbl, _range);

      error = vorrq_u8(error, vcltq_u8(_input, minv));
      error = vorrq_u8(error, vcgtq_u8(_input, maxv));

      /************************ next iteration *************************/
      prev_input = _input;
      prev_first_len = _first_len;

      data += 32;
      len -= 32;
    }

    /* Delay error check till loop ends */
    if (vmaxvq_u8(error)) return false;

    /* Find previous token (not 80~BF) */
    uint32_t token4;
    vst1q_lane_u32(&token4, vreinterpretq_u32_u8(prev_input), 3);

    const int8_t* token = (const int8_t*)&token4;
    int lookahead = 0;
    if (token[3] > (int8_t)0xBF)
      lookahead = 1;
    else if (token[2] > (int8_t)0xBF)
      lookahead = 2;
    else if (token[1] > (int8_t)0xBF)
      lookahead = 3;

    data -= lookahead;
    len += lookahead;
  }

  /* Check remaining bytes */
  return ValidateUTF8(data, len);
}
#endif  // ARROW_HAVE_ARM_NEON

#if defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_SSE2)
static const int8_t _first_len_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3,
};

static const int8_t _first_range_tbl[] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8,
};

static const int8_t _range_min_tbl[] = {
    0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80,
    0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F,
};
static const int8_t _range_max_tbl[] = {
    0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F,
    0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80,
};

static const int8_t _df_ee_tbl[] = {
    0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0,
};
static const int8_t _ef_fe_tbl[] = {
    0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

inline bool ValidateNonAscii(const uint8_t* data, int64_t len) {
  if (len >= 32) {
    __m128i prev_input = _mm_set1_epi8(0);
    __m128i prev_first_len = _mm_set1_epi8(0);

    const __m128i first_len_tbl = _mm_lddqu_si128((const __m128i*)_first_len_tbl);
    const __m128i first_range_tbl = _mm_lddqu_si128((const __m128i*)_first_range_tbl);
    const __m128i range_min_tbl = _mm_lddqu_si128((const __m128i*)_range_min_tbl);
    const __m128i range_max_tbl = _mm_lddqu_si128((const __m128i*)_range_max_tbl);
    const __m128i df_ee_tbl = _mm_lddqu_si128((const __m128i*)_df_ee_tbl);
    const __m128i ef_fe_tbl = _mm_lddqu_si128((const __m128i*)_ef_fe_tbl);

    __m128i error = _mm_set1_epi8(0);

    while (len >= 32) {
      /***************************** First 16 bytes ****************************/
      const __m128i input = _mm_lddqu_si128((const __m128i*)data);

      __m128i high_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

      __m128i first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);

      __m128i range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);

      range = _mm_or_si128(range, _mm_alignr_epi8(first_len, prev_first_len, 15));

      __m128i tmp1, tmp2;
      tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(1));
      tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(1));
      range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 14));

      tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(2));
      tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(2));
      range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 13));

      __m128i shift1, pos, range2;
      shift1 = _mm_alignr_epi8(input, prev_input, 15);
      pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
      tmp1 = _mm_subs_epu8(pos, _mm_set1_epi8(240));
      range2 = _mm_shuffle_epi8(df_ee_tbl, tmp1);
      tmp2 = _mm_adds_epu8(pos, _mm_set1_epi8(112));
      range2 = _mm_add_epi8(range2, _mm_shuffle_epi8(ef_fe_tbl, tmp2));

      range = _mm_add_epi8(range, range2);

      __m128i minv = _mm_shuffle_epi8(range_min_tbl, range);
      __m128i maxv = _mm_shuffle_epi8(range_max_tbl, range);

      error = _mm_or_si128(error, _mm_cmplt_epi8(input, minv));
      error = _mm_or_si128(error, _mm_cmpgt_epi8(input, maxv));

      /*****************************Next 16 bytes ****************************/
      const __m128i _input = _mm_lddqu_si128((const __m128i*)(data + 16));

      high_nibbles = _mm_and_si128(_mm_srli_epi16(_input, 4), _mm_set1_epi8(0x0F));

      __m128i _first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);

      __m128i _range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);

      _range = _mm_or_si128(_range, _mm_alignr_epi8(_first_len, first_len, 15));

      tmp1 = _mm_subs_epu8(_first_len, _mm_set1_epi8(1));
      tmp2 = _mm_subs_epu8(first_len, _mm_set1_epi8(1));
      _range = _mm_or_si128(_range, _mm_alignr_epi8(tmp1, tmp2, 14));

      tmp1 = _mm_subs_epu8(_first_len, _mm_set1_epi8(2));
      tmp2 = _mm_subs_epu8(first_len, _mm_set1_epi8(2));
      _range = _mm_or_si128(_range, _mm_alignr_epi8(tmp1, tmp2, 13));

      __m128i _range2;
      shift1 = _mm_alignr_epi8(_input, input, 15);
      pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
      tmp1 = _mm_subs_epu8(pos, _mm_set1_epi8(240));
      _range2 = _mm_shuffle_epi8(df_ee_tbl, tmp1);
      tmp2 = _mm_adds_epu8(pos, _mm_set1_epi8(112));
      _range2 = _mm_add_epi8(_range2, _mm_shuffle_epi8(ef_fe_tbl, tmp2));

      _range = _mm_add_epi8(_range, _range2);

      minv = _mm_shuffle_epi8(range_min_tbl, _range);
      maxv = _mm_shuffle_epi8(range_max_tbl, _range);

      error = _mm_or_si128(error, _mm_cmplt_epi8(_input, minv));
      error = _mm_or_si128(error, _mm_cmpgt_epi8(_input, maxv));

      /************************ next iteration **************************/
      prev_input = _input;
      prev_first_len = _first_len;

      data += 32;
      len -= 32;
    }

    if (!_mm_testz_si128(error, error)) return -1;

    int32_t token4 = _mm_extract_epi32(prev_input, 3);
    const int8_t* token = (const int8_t*)&token4;
    int lookahead = 0;
    if (token[3] > (int8_t)0xBF)
      lookahead = 1;
    else if (token[2] > (int8_t)0xBF)
      lookahead = 2;
    else if (token[1] > (int8_t)0xBF)
      lookahead = 3;

    data -= lookahead;
    len += lookahead;
  }

  /* Check remaining bytes */
  return ValidateUTF8(data, len);
}
#endif  // ARROW_HAVE_SSE4_2 || ARROW_HAVE_SSE2

// Skip UTF8 byte order mark, if any.
ARROW_EXPORT
Result<const uint8_t*> SkipUTF8BOM(const uint8_t* data, int64_t size);

}  // namespace util
}  // namespace arrow

#endif
