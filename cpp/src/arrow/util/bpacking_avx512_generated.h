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
//
// Automatically generated file; DO NOT EDIT.

#pragma once

#include <stdint.h>
#include <string.h>

#ifdef _MSC_VER
#include <intrin.h>
#else
#include <immintrin.h>
#endif

#include "arrow/util/ubsan.h"

namespace arrow {
namespace internal {

inline static const uint32_t* unpack0_32_avx512(const uint32_t* in, uint32_t* out) {
  memset(out, 0x0, 32 * sizeof(*out));
  out += 32;

  return in;
}

inline static const uint32_t* unpack1_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(15, 14, 13, 12,
                                11, 10, 9, 8,
                                7, 6, 5, 4,
                                3, 2, 1, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(31, 30, 29, 28,
                                27, 26, 25, 24,
                                23, 22, 21, 20,
                                19, 18, 17, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 1;

  return in;
}

inline static const uint32_t* unpack2_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(30, 28, 26, 24,
                                22, 20, 18, 16,
                                14, 12, 10, 8,
                                6, 4, 2, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(30, 28, 26, 24,
                                22, 20, 18, 16,
                                14, 12, 10, 8,
                                6, 4, 2, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 2;

  return in;
}

inline static const uint32_t* unpack3_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(13, 10, 7, 4,
                                1, 0, 27, 24,
                                21, 18, 15, 12,
                                9, 6, 3, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2,
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(29, 26, 23, 20,
                                17, 14, 11, 8,
                                5, 2, 0, 28,
                                25, 22, 19, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 31 | SafeLoad(in + 2) << 1, SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 3;

  return in;
}

inline static const uint32_t* unpack4_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xf;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(28, 24, 20, 16,
                                12, 8, 4, 0,
                                28, 24, 20, 16,
                                12, 8, 4, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(28, 24, 20, 16,
                                12, 8, 4, 0,
                                28, 24, 20, 16,
                                12, 8, 4, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 4;

  return in;
}

inline static const uint32_t* unpack5_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(11, 6, 1, 0,
                                23, 18, 13, 8,
                                3, 0, 25, 20,
                                15, 10, 5, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4,
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2,
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(27, 22, 17, 12,
                                7, 2, 0, 24,
                                19, 14, 9, 4,
                                0, 26, 21, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 29 | SafeLoad(in + 4) << 3, SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 31 | SafeLoad(in + 3) << 1, SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 5;

  return in;
}

inline static const uint32_t* unpack6_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(26, 20, 14, 8,
                                2, 0, 22, 16,
                                10, 4, 0, 24,
                                18, 12, 6, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4,
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2, SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(26, 20, 14, 8,
                                2, 0, 22, 16,
                                10, 4, 0, 24,
                                18, 12, 6, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 28 | SafeLoad(in + 5) << 4,
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2, SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 6;

  return in;
}

inline static const uint32_t* unpack7_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(9, 2, 0, 20,
                                13, 6, 0, 24,
                                17, 10, 3, 0,
                                21, 14, 7, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 27 | SafeLoad(in + 3) << 5, SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 31 | SafeLoad(in + 2) << 1, SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 28 | SafeLoad(in + 1) << 4,
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(25, 18, 11, 4,
                                0, 22, 15, 8,
                                1, 0, 19, 12,
                                5, 0, 23, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 29 | SafeLoad(in + 6) << 3, SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 26 | SafeLoad(in + 5) << 6,
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2,
                              SafeLoad(in + 3), SafeLoad(in + 3));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 7;

  return in;
}

inline static const uint32_t* unpack8_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0), SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 7), SafeLoad(in + 7),
                              SafeLoad(in + 7), SafeLoad(in + 7),
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 4));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 8;

  return in;
}

inline static const uint32_t* unpack9_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(7, 0, 21, 12,
                                3, 0, 17, 8,
                                0, 22, 13, 4,
                                0, 18, 9, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 4), SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2,
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 26 | SafeLoad(in + 3) << 6,
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 31 | SafeLoad(in + 2) << 1, SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 27 | SafeLoad(in + 1) << 5, SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(23, 14, 5, 0,
                                19, 10, 1, 0,
                                15, 6, 0, 20,
                                11, 2, 0, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 8), SafeLoad(in + 8),
                              SafeLoad(in + 8), SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4,
                              SafeLoad(in + 7), SafeLoad(in + 7),
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 24 | SafeLoad(in + 7) << 8,
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 29 | SafeLoad(in + 6) << 3, SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 25 | SafeLoad(in + 5) << 7, SafeLoad(in + 4));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 9;

  return in;
}

inline static const uint32_t* unpack10_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(22, 12, 2, 0,
                                14, 4, 0, 16,
                                6, 0, 18, 8,
                                0, 20, 10, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8,
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 26 | SafeLoad(in + 3) << 6, SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4,
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2, SafeLoad(in + 0),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(22, 12, 2, 0,
                                14, 4, 0, 16,
                                6, 0, 18, 8,
                                0, 20, 10, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 9), SafeLoad(in + 9),
                              SafeLoad(in + 9), SafeLoad(in + 8) >> 24 | SafeLoad(in + 9) << 8,
                              SafeLoad(in + 8), SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 26 | SafeLoad(in + 8) << 6, SafeLoad(in + 7),
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 28 | SafeLoad(in + 7) << 4,
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 30 | SafeLoad(in + 6) << 2, SafeLoad(in + 5),
                              SafeLoad(in + 5), SafeLoad(in + 5));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 10;

  return in;
}

inline static const uint32_t* unpack11_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(5, 0, 15, 4,
                                0, 14, 3, 0,
                                13, 2, 0, 12,
                                1, 0, 11, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 5), SafeLoad(in + 4) >> 26 | SafeLoad(in + 5) << 6,
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 25 | SafeLoad(in + 4) << 7, SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 24 | SafeLoad(in + 3) << 8,
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 23 | SafeLoad(in + 2) << 9, SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 22 | SafeLoad(in + 1) << 10,
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(21, 10, 0, 20,
                                9, 0, 19, 8,
                                0, 18, 7, 0,
                                17, 6, 0, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 10), SafeLoad(in + 10),
                              SafeLoad(in + 9) >> 31 | SafeLoad(in + 10) << 1, SafeLoad(in + 9),
                              SafeLoad(in + 9), SafeLoad(in + 8) >> 30 | SafeLoad(in + 9) << 2,
                              SafeLoad(in + 8), SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 29 | SafeLoad(in + 8) << 3, SafeLoad(in + 7),
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 28 | SafeLoad(in + 7) << 4,
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 27 | SafeLoad(in + 6) << 5, SafeLoad(in + 5));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 11;

  return in;
}

inline static const uint32_t* unpack12_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xfff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(20, 8, 0, 16,
                                4, 0, 12, 0,
                                20, 8, 0, 16,
                                4, 0, 12, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 28 | SafeLoad(in + 5) << 4, SafeLoad(in + 4),
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8,
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4, SafeLoad(in + 1),
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 24 | SafeLoad(in + 1) << 8,
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(20, 8, 0, 16,
                                4, 0, 12, 0,
                                20, 8, 0, 16,
                                4, 0, 12, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 11), SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 28 | SafeLoad(in + 11) << 4, SafeLoad(in + 10),
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 24 | SafeLoad(in + 10) << 8,
                              SafeLoad(in + 9), SafeLoad(in + 9),
                              SafeLoad(in + 8), SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4, SafeLoad(in + 7),
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 24 | SafeLoad(in + 7) << 8,
                              SafeLoad(in + 6), SafeLoad(in + 6));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 12;

  return in;
}

inline static const uint32_t* unpack13_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(3, 0, 9, 0,
                                15, 2, 0, 8,
                                0, 14, 1, 0,
                                7, 0, 13, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 6), SafeLoad(in + 5) >> 22 | SafeLoad(in + 6) << 10,
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 28 | SafeLoad(in + 5) << 4,
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 21 | SafeLoad(in + 4) << 11, SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 27 | SafeLoad(in + 3) << 5, SafeLoad(in + 2),
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 20 | SafeLoad(in + 2) << 12,
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 26 | SafeLoad(in + 1) << 6,
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(19, 6, 0, 12,
                                0, 18, 5, 0,
                                11, 0, 17, 4,
                                0, 10, 0, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 12), SafeLoad(in + 12),
                              SafeLoad(in + 11) >> 25 | SafeLoad(in + 12) << 7, SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 31 | SafeLoad(in + 11) << 1, SafeLoad(in + 10),
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 24 | SafeLoad(in + 10) << 8,
                              SafeLoad(in + 9), SafeLoad(in + 8) >> 30 | SafeLoad(in + 9) << 2,
                              SafeLoad(in + 8), SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 23 | SafeLoad(in + 8) << 9, SafeLoad(in + 7),
                              SafeLoad(in + 6) >> 29 | SafeLoad(in + 7) << 3, SafeLoad(in + 6));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 13;

  return in;
}

inline static const uint32_t* unpack14_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(18, 4, 0, 8,
                                0, 12, 0, 16,
                                2, 0, 6, 0,
                                10, 0, 14, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 22 | SafeLoad(in + 6) << 10, SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 26 | SafeLoad(in + 5) << 6, SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2, SafeLoad(in + 3),
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 20 | SafeLoad(in + 3) << 12,
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 24 | SafeLoad(in + 2) << 8,
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 28 | SafeLoad(in + 1) << 4,
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(18, 4, 0, 8,
                                0, 12, 0, 16,
                                2, 0, 6, 0,
                                10, 0, 14, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 13), SafeLoad(in + 13),
                              SafeLoad(in + 12) >> 22 | SafeLoad(in + 13) << 10, SafeLoad(in + 12),
                              SafeLoad(in + 11) >> 26 | SafeLoad(in + 12) << 6, SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 30 | SafeLoad(in + 11) << 2, SafeLoad(in + 10),
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 20 | SafeLoad(in + 10) << 12,
                              SafeLoad(in + 9), SafeLoad(in + 8) >> 24 | SafeLoad(in + 9) << 8,
                              SafeLoad(in + 8), SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4,
                              SafeLoad(in + 7), SafeLoad(in + 7));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 14;

  return in;
}

inline static const uint32_t* unpack15_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(1, 0, 3, 0,
                                5, 0, 7, 0,
                                9, 0, 11, 0,
                                13, 0, 15, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 7), SafeLoad(in + 6) >> 18 | SafeLoad(in + 7) << 14,
                              SafeLoad(in + 6), SafeLoad(in + 5) >> 20 | SafeLoad(in + 6) << 12,
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 22 | SafeLoad(in + 5) << 10,
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8,
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 26 | SafeLoad(in + 3) << 6,
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4,
                              SafeLoad(in + 1), SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2,
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(17, 2, 0, 4,
                                0, 6, 0, 8,
                                0, 10, 0, 12,
                                0, 14, 0, 16);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 14), SafeLoad(in + 14),
                              SafeLoad(in + 13) >> 19 | SafeLoad(in + 14) << 13, SafeLoad(in + 13),
                              SafeLoad(in + 12) >> 21 | SafeLoad(in + 13) << 11, SafeLoad(in + 12),
                              SafeLoad(in + 11) >> 23 | SafeLoad(in + 12) << 9, SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 25 | SafeLoad(in + 11) << 7, SafeLoad(in + 10),
                              SafeLoad(in + 9) >> 27 | SafeLoad(in + 10) << 5, SafeLoad(in + 9),
                              SafeLoad(in + 8) >> 29 | SafeLoad(in + 9) << 3, SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 31 | SafeLoad(in + 8) << 1, SafeLoad(in + 7));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 15;

  return in;
}

inline static const uint32_t* unpack16_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 7), SafeLoad(in + 7),
                              SafeLoad(in + 6), SafeLoad(in + 6),
                              SafeLoad(in + 5), SafeLoad(in + 5),
                              SafeLoad(in + 4), SafeLoad(in + 4),
                              SafeLoad(in + 3), SafeLoad(in + 3),
                              SafeLoad(in + 2), SafeLoad(in + 2),
                              SafeLoad(in + 1), SafeLoad(in + 1),
                              SafeLoad(in + 0), SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 15), SafeLoad(in + 15),
                              SafeLoad(in + 14), SafeLoad(in + 14),
                              SafeLoad(in + 13), SafeLoad(in + 13),
                              SafeLoad(in + 12), SafeLoad(in + 12),
                              SafeLoad(in + 11), SafeLoad(in + 11),
                              SafeLoad(in + 10), SafeLoad(in + 10),
                              SafeLoad(in + 9), SafeLoad(in + 9),
                              SafeLoad(in + 8), SafeLoad(in + 8));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 16;

  return in;
}

inline static const uint32_t* unpack17_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 14, 0, 12,
                                0, 10, 0, 8,
                                0, 6, 0, 4,
                                0, 2, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 7) >> 31 | SafeLoad(in + 8) << 1, SafeLoad(in + 7),
                              SafeLoad(in + 6) >> 29 | SafeLoad(in + 7) << 3, SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 27 | SafeLoad(in + 6) << 5, SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 25 | SafeLoad(in + 5) << 7, SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 23 | SafeLoad(in + 4) << 9, SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 21 | SafeLoad(in + 3) << 11, SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 19 | SafeLoad(in + 2) << 13, SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 17 | SafeLoad(in + 1) << 15, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(15, 0, 13, 0,
                                11, 0, 9, 0,
                                7, 0, 5, 0,
                                3, 0, 1, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 16), SafeLoad(in + 15) >> 30 | SafeLoad(in + 16) << 2,
                              SafeLoad(in + 15), SafeLoad(in + 14) >> 28 | SafeLoad(in + 15) << 4,
                              SafeLoad(in + 14), SafeLoad(in + 13) >> 26 | SafeLoad(in + 14) << 6,
                              SafeLoad(in + 13), SafeLoad(in + 12) >> 24 | SafeLoad(in + 13) << 8,
                              SafeLoad(in + 12), SafeLoad(in + 11) >> 22 | SafeLoad(in + 12) << 10,
                              SafeLoad(in + 11), SafeLoad(in + 10) >> 20 | SafeLoad(in + 11) << 12,
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 18 | SafeLoad(in + 10) << 14,
                              SafeLoad(in + 9), SafeLoad(in + 8) >> 16 | SafeLoad(in + 9) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 17;

  return in;
}

inline static const uint32_t* unpack18_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(14, 0, 10, 0,
                                6, 0, 2, 0,
                                0, 12, 0, 8,
                                0, 4, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 8), SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4,
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 24 | SafeLoad(in + 7) << 8,
                              SafeLoad(in + 6), SafeLoad(in + 5) >> 20 | SafeLoad(in + 6) << 12,
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 16 | SafeLoad(in + 5) << 16,
                              SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2, SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 26 | SafeLoad(in + 3) << 6, SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 22 | SafeLoad(in + 2) << 10, SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 18 | SafeLoad(in + 1) << 14, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(14, 0, 10, 0,
                                6, 0, 2, 0,
                                0, 12, 0, 8,
                                0, 4, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 17), SafeLoad(in + 16) >> 28 | SafeLoad(in + 17) << 4,
                              SafeLoad(in + 16), SafeLoad(in + 15) >> 24 | SafeLoad(in + 16) << 8,
                              SafeLoad(in + 15), SafeLoad(in + 14) >> 20 | SafeLoad(in + 15) << 12,
                              SafeLoad(in + 14), SafeLoad(in + 13) >> 16 | SafeLoad(in + 14) << 16,
                              SafeLoad(in + 12) >> 30 | SafeLoad(in + 13) << 2, SafeLoad(in + 12),
                              SafeLoad(in + 11) >> 26 | SafeLoad(in + 12) << 6, SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 22 | SafeLoad(in + 11) << 10, SafeLoad(in + 10),
                              SafeLoad(in + 9) >> 18 | SafeLoad(in + 10) << 14, SafeLoad(in + 9));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 18;

  return in;
}

inline static const uint32_t* unpack19_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 10, 0, 4,
                                0, 0, 11, 0,
                                5, 0, 0, 12,
                                0, 6, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 8) >> 29 | SafeLoad(in + 9) << 3, SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 23 | SafeLoad(in + 8) << 9, SafeLoad(in + 7),
                              SafeLoad(in + 6) >> 17 | SafeLoad(in + 7) << 15, SafeLoad(in + 5) >> 30 | SafeLoad(in + 6) << 2,
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 24 | SafeLoad(in + 5) << 8,
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 18 | SafeLoad(in + 4) << 14,
                              SafeLoad(in + 2) >> 31 | SafeLoad(in + 3) << 1, SafeLoad(in + 2),
                              SafeLoad(in + 1) >> 25 | SafeLoad(in + 2) << 7, SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 19 | SafeLoad(in + 1) << 13, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(13, 0, 7, 0,
                                1, 0, 0, 8,
                                0, 2, 0, 0,
                                9, 0, 3, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 18), SafeLoad(in + 17) >> 26 | SafeLoad(in + 18) << 6,
                              SafeLoad(in + 17), SafeLoad(in + 16) >> 20 | SafeLoad(in + 17) << 12,
                              SafeLoad(in + 16), SafeLoad(in + 15) >> 14 | SafeLoad(in + 16) << 18,
                              SafeLoad(in + 14) >> 27 | SafeLoad(in + 15) << 5, SafeLoad(in + 14),
                              SafeLoad(in + 13) >> 21 | SafeLoad(in + 14) << 11, SafeLoad(in + 13),
                              SafeLoad(in + 12) >> 15 | SafeLoad(in + 13) << 17, SafeLoad(in + 11) >> 28 | SafeLoad(in + 12) << 4,
                              SafeLoad(in + 11), SafeLoad(in + 10) >> 22 | SafeLoad(in + 11) << 10,
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 16 | SafeLoad(in + 10) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 19;

  return in;
}

inline static const uint32_t* unpack20_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xfffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(12, 0, 4, 0,
                                0, 8, 0, 0,
                                12, 0, 4, 0,
                                0, 8, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 9), SafeLoad(in + 8) >> 24 | SafeLoad(in + 9) << 8,
                              SafeLoad(in + 8), SafeLoad(in + 7) >> 16 | SafeLoad(in + 8) << 16,
                              SafeLoad(in + 6) >> 28 | SafeLoad(in + 7) << 4, SafeLoad(in + 6),
                              SafeLoad(in + 5) >> 20 | SafeLoad(in + 6) << 12, SafeLoad(in + 5),
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8,
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 16 | SafeLoad(in + 3) << 16,
                              SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4, SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 20 | SafeLoad(in + 1) << 12, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(12, 0, 4, 0,
                                0, 8, 0, 0,
                                12, 0, 4, 0,
                                0, 8, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 19), SafeLoad(in + 18) >> 24 | SafeLoad(in + 19) << 8,
                              SafeLoad(in + 18), SafeLoad(in + 17) >> 16 | SafeLoad(in + 18) << 16,
                              SafeLoad(in + 16) >> 28 | SafeLoad(in + 17) << 4, SafeLoad(in + 16),
                              SafeLoad(in + 15) >> 20 | SafeLoad(in + 16) << 12, SafeLoad(in + 15),
                              SafeLoad(in + 14), SafeLoad(in + 13) >> 24 | SafeLoad(in + 14) << 8,
                              SafeLoad(in + 13), SafeLoad(in + 12) >> 16 | SafeLoad(in + 13) << 16,
                              SafeLoad(in + 11) >> 28 | SafeLoad(in + 12) << 4, SafeLoad(in + 11),
                              SafeLoad(in + 10) >> 20 | SafeLoad(in + 11) << 12, SafeLoad(in + 10));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 20;

  return in;
}

inline static const uint32_t* unpack21_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 6, 0, 0,
                                7, 0, 0, 8,
                                0, 0, 9, 0,
                                0, 10, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 9) >> 27 | SafeLoad(in + 10) << 5, SafeLoad(in + 9),
                              SafeLoad(in + 8) >> 17 | SafeLoad(in + 9) << 15, SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4,
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 18 | SafeLoad(in + 7) << 14,
                              SafeLoad(in + 5) >> 29 | SafeLoad(in + 6) << 3, SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 19 | SafeLoad(in + 5) << 13, SafeLoad(in + 3) >> 30 | SafeLoad(in + 4) << 2,
                              SafeLoad(in + 3), SafeLoad(in + 2) >> 20 | SafeLoad(in + 3) << 12,
                              SafeLoad(in + 1) >> 31 | SafeLoad(in + 2) << 1, SafeLoad(in + 1),
                              SafeLoad(in + 0) >> 21 | SafeLoad(in + 1) << 11, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(11, 0, 1, 0,
                                0, 2, 0, 0,
                                3, 0, 0, 4,
                                0, 0, 5, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 20), SafeLoad(in + 19) >> 22 | SafeLoad(in + 20) << 10,
                              SafeLoad(in + 19), SafeLoad(in + 18) >> 12 | SafeLoad(in + 19) << 20,
                              SafeLoad(in + 17) >> 23 | SafeLoad(in + 18) << 9, SafeLoad(in + 17),
                              SafeLoad(in + 16) >> 13 | SafeLoad(in + 17) << 19, SafeLoad(in + 15) >> 24 | SafeLoad(in + 16) << 8,
                              SafeLoad(in + 15), SafeLoad(in + 14) >> 14 | SafeLoad(in + 15) << 18,
                              SafeLoad(in + 13) >> 25 | SafeLoad(in + 14) << 7, SafeLoad(in + 13),
                              SafeLoad(in + 12) >> 15 | SafeLoad(in + 13) << 17, SafeLoad(in + 11) >> 26 | SafeLoad(in + 12) << 6,
                              SafeLoad(in + 11), SafeLoad(in + 10) >> 16 | SafeLoad(in + 11) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 21;

  return in;
}

inline static const uint32_t* unpack22_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(10, 0, 0, 8,
                                0, 0, 6, 0,
                                0, 4, 0, 0,
                                2, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 10), SafeLoad(in + 9) >> 20 | SafeLoad(in + 10) << 12,
                              SafeLoad(in + 8) >> 30 | SafeLoad(in + 9) << 2, SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 18 | SafeLoad(in + 8) << 14, SafeLoad(in + 6) >> 28 | SafeLoad(in + 7) << 4,
                              SafeLoad(in + 6), SafeLoad(in + 5) >> 16 | SafeLoad(in + 6) << 16,
                              SafeLoad(in + 4) >> 26 | SafeLoad(in + 5) << 6, SafeLoad(in + 4),
                              SafeLoad(in + 3) >> 14 | SafeLoad(in + 4) << 18, SafeLoad(in + 2) >> 24 | SafeLoad(in + 3) << 8,
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 12 | SafeLoad(in + 2) << 20,
                              SafeLoad(in + 0) >> 22 | SafeLoad(in + 1) << 10, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(10, 0, 0, 8,
                                0, 0, 6, 0,
                                0, 4, 0, 0,
                                2, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 21), SafeLoad(in + 20) >> 20 | SafeLoad(in + 21) << 12,
                              SafeLoad(in + 19) >> 30 | SafeLoad(in + 20) << 2, SafeLoad(in + 19),
                              SafeLoad(in + 18) >> 18 | SafeLoad(in + 19) << 14, SafeLoad(in + 17) >> 28 | SafeLoad(in + 18) << 4,
                              SafeLoad(in + 17), SafeLoad(in + 16) >> 16 | SafeLoad(in + 17) << 16,
                              SafeLoad(in + 15) >> 26 | SafeLoad(in + 16) << 6, SafeLoad(in + 15),
                              SafeLoad(in + 14) >> 14 | SafeLoad(in + 15) << 18, SafeLoad(in + 13) >> 24 | SafeLoad(in + 14) << 8,
                              SafeLoad(in + 13), SafeLoad(in + 12) >> 12 | SafeLoad(in + 13) << 20,
                              SafeLoad(in + 11) >> 22 | SafeLoad(in + 12) << 10, SafeLoad(in + 11));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 22;

  return in;
}

inline static const uint32_t* unpack23_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 2, 0, 0,
                                0, 6, 0, 0,
                                1, 0, 0, 0,
                                5, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 10) >> 25 | SafeLoad(in + 11) << 7, SafeLoad(in + 10),
                              SafeLoad(in + 9) >> 11 | SafeLoad(in + 10) << 21, SafeLoad(in + 8) >> 20 | SafeLoad(in + 9) << 12,
                              SafeLoad(in + 7) >> 29 | SafeLoad(in + 8) << 3, SafeLoad(in + 7),
                              SafeLoad(in + 6) >> 15 | SafeLoad(in + 7) << 17, SafeLoad(in + 5) >> 24 | SafeLoad(in + 6) << 8,
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 10 | SafeLoad(in + 5) << 22,
                              SafeLoad(in + 3) >> 19 | SafeLoad(in + 4) << 13, SafeLoad(in + 2) >> 28 | SafeLoad(in + 3) << 4,
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 14 | SafeLoad(in + 2) << 18,
                              SafeLoad(in + 0) >> 23 | SafeLoad(in + 1) << 9, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(9, 0, 0, 4,
                                0, 0, 0, 8,
                                0, 0, 3, 0,
                                0, 0, 7, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 22), SafeLoad(in + 21) >> 18 | SafeLoad(in + 22) << 14,
                              SafeLoad(in + 20) >> 27 | SafeLoad(in + 21) << 5, SafeLoad(in + 20),
                              SafeLoad(in + 19) >> 13 | SafeLoad(in + 20) << 19, SafeLoad(in + 18) >> 22 | SafeLoad(in + 19) << 10,
                              SafeLoad(in + 17) >> 31 | SafeLoad(in + 18) << 1, SafeLoad(in + 17),
                              SafeLoad(in + 16) >> 17 | SafeLoad(in + 17) << 15, SafeLoad(in + 15) >> 26 | SafeLoad(in + 16) << 6,
                              SafeLoad(in + 15), SafeLoad(in + 14) >> 12 | SafeLoad(in + 15) << 20,
                              SafeLoad(in + 13) >> 21 | SafeLoad(in + 14) << 11, SafeLoad(in + 12) >> 30 | SafeLoad(in + 13) << 2,
                              SafeLoad(in + 12), SafeLoad(in + 11) >> 16 | SafeLoad(in + 12) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 23;

  return in;
}

inline static const uint32_t* unpack24_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 11), SafeLoad(in + 10) >> 16 | SafeLoad(in + 11) << 16,
                              SafeLoad(in + 9) >> 24 | SafeLoad(in + 10) << 8, SafeLoad(in + 9),
                              SafeLoad(in + 8), SafeLoad(in + 7) >> 16 | SafeLoad(in + 8) << 16,
                              SafeLoad(in + 6) >> 24 | SafeLoad(in + 7) << 8, SafeLoad(in + 6),
                              SafeLoad(in + 5), SafeLoad(in + 4) >> 16 | SafeLoad(in + 5) << 16,
                              SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8, SafeLoad(in + 3),
                              SafeLoad(in + 2), SafeLoad(in + 1) >> 16 | SafeLoad(in + 2) << 16,
                              SafeLoad(in + 0) >> 24 | SafeLoad(in + 1) << 8, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 23), SafeLoad(in + 22) >> 16 | SafeLoad(in + 23) << 16,
                              SafeLoad(in + 21) >> 24 | SafeLoad(in + 22) << 8, SafeLoad(in + 21),
                              SafeLoad(in + 20), SafeLoad(in + 19) >> 16 | SafeLoad(in + 20) << 16,
                              SafeLoad(in + 18) >> 24 | SafeLoad(in + 19) << 8, SafeLoad(in + 18),
                              SafeLoad(in + 17), SafeLoad(in + 16) >> 16 | SafeLoad(in + 17) << 16,
                              SafeLoad(in + 15) >> 24 | SafeLoad(in + 16) << 8, SafeLoad(in + 15),
                              SafeLoad(in + 14), SafeLoad(in + 13) >> 16 | SafeLoad(in + 14) << 16,
                              SafeLoad(in + 12) >> 24 | SafeLoad(in + 13) << 8, SafeLoad(in + 12));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 24;

  return in;
}

inline static const uint32_t* unpack25_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 5, 0,
                                0, 0, 1, 0,
                                0, 0, 0, 4,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 11) >> 23 | SafeLoad(in + 12) << 9, SafeLoad(in + 10) >> 30 | SafeLoad(in + 11) << 2,
                              SafeLoad(in + 10), SafeLoad(in + 9) >> 12 | SafeLoad(in + 10) << 20,
                              SafeLoad(in + 8) >> 19 | SafeLoad(in + 9) << 13, SafeLoad(in + 7) >> 26 | SafeLoad(in + 8) << 6,
                              SafeLoad(in + 7), SafeLoad(in + 6) >> 8 | SafeLoad(in + 7) << 24,
                              SafeLoad(in + 5) >> 15 | SafeLoad(in + 6) << 17, SafeLoad(in + 4) >> 22 | SafeLoad(in + 5) << 10,
                              SafeLoad(in + 3) >> 29 | SafeLoad(in + 4) << 3, SafeLoad(in + 3),
                              SafeLoad(in + 2) >> 11 | SafeLoad(in + 3) << 21, SafeLoad(in + 1) >> 18 | SafeLoad(in + 2) << 14,
                              SafeLoad(in + 0) >> 25 | SafeLoad(in + 1) << 7, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(7, 0, 0, 0,
                                3, 0, 0, 0,
                                0, 6, 0, 0,
                                0, 2, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 24), SafeLoad(in + 23) >> 14 | SafeLoad(in + 24) << 18,
                              SafeLoad(in + 22) >> 21 | SafeLoad(in + 23) << 11, SafeLoad(in + 21) >> 28 | SafeLoad(in + 22) << 4,
                              SafeLoad(in + 21), SafeLoad(in + 20) >> 10 | SafeLoad(in + 21) << 22,
                              SafeLoad(in + 19) >> 17 | SafeLoad(in + 20) << 15, SafeLoad(in + 18) >> 24 | SafeLoad(in + 19) << 8,
                              SafeLoad(in + 17) >> 31 | SafeLoad(in + 18) << 1, SafeLoad(in + 17),
                              SafeLoad(in + 16) >> 13 | SafeLoad(in + 17) << 19, SafeLoad(in + 15) >> 20 | SafeLoad(in + 16) << 12,
                              SafeLoad(in + 14) >> 27 | SafeLoad(in + 15) << 5, SafeLoad(in + 14),
                              SafeLoad(in + 13) >> 9 | SafeLoad(in + 14) << 23, SafeLoad(in + 12) >> 16 | SafeLoad(in + 13) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 25;

  return in;
}

inline static const uint32_t* unpack26_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(6, 0, 0, 0,
                                0, 4, 0, 0,
                                0, 0, 2, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 12), SafeLoad(in + 11) >> 12 | SafeLoad(in + 12) << 20,
                              SafeLoad(in + 10) >> 18 | SafeLoad(in + 11) << 14, SafeLoad(in + 9) >> 24 | SafeLoad(in + 10) << 8,
                              SafeLoad(in + 8) >> 30 | SafeLoad(in + 9) << 2, SafeLoad(in + 8),
                              SafeLoad(in + 7) >> 10 | SafeLoad(in + 8) << 22, SafeLoad(in + 6) >> 16 | SafeLoad(in + 7) << 16,
                              SafeLoad(in + 5) >> 22 | SafeLoad(in + 6) << 10, SafeLoad(in + 4) >> 28 | SafeLoad(in + 5) << 4,
                              SafeLoad(in + 4), SafeLoad(in + 3) >> 8 | SafeLoad(in + 4) << 24,
                              SafeLoad(in + 2) >> 14 | SafeLoad(in + 3) << 18, SafeLoad(in + 1) >> 20 | SafeLoad(in + 2) << 12,
                              SafeLoad(in + 0) >> 26 | SafeLoad(in + 1) << 6, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(6, 0, 0, 0,
                                0, 4, 0, 0,
                                0, 0, 2, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 25), SafeLoad(in + 24) >> 12 | SafeLoad(in + 25) << 20,
                              SafeLoad(in + 23) >> 18 | SafeLoad(in + 24) << 14, SafeLoad(in + 22) >> 24 | SafeLoad(in + 23) << 8,
                              SafeLoad(in + 21) >> 30 | SafeLoad(in + 22) << 2, SafeLoad(in + 21),
                              SafeLoad(in + 20) >> 10 | SafeLoad(in + 21) << 22, SafeLoad(in + 19) >> 16 | SafeLoad(in + 20) << 16,
                              SafeLoad(in + 18) >> 22 | SafeLoad(in + 19) << 10, SafeLoad(in + 17) >> 28 | SafeLoad(in + 18) << 4,
                              SafeLoad(in + 17), SafeLoad(in + 16) >> 8 | SafeLoad(in + 17) << 24,
                              SafeLoad(in + 15) >> 14 | SafeLoad(in + 16) << 18, SafeLoad(in + 14) >> 20 | SafeLoad(in + 15) << 12,
                              SafeLoad(in + 13) >> 26 | SafeLoad(in + 14) << 6, SafeLoad(in + 13));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 26;

  return in;
}

inline static const uint32_t* unpack27_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 4,
                                0, 0, 0, 0,
                                0, 2, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 12) >> 21 | SafeLoad(in + 13) << 11, SafeLoad(in + 11) >> 26 | SafeLoad(in + 12) << 6,
                              SafeLoad(in + 10) >> 31 | SafeLoad(in + 11) << 1, SafeLoad(in + 10),
                              SafeLoad(in + 9) >> 9 | SafeLoad(in + 10) << 23, SafeLoad(in + 8) >> 14 | SafeLoad(in + 9) << 18,
                              SafeLoad(in + 7) >> 19 | SafeLoad(in + 8) << 13, SafeLoad(in + 6) >> 24 | SafeLoad(in + 7) << 8,
                              SafeLoad(in + 5) >> 29 | SafeLoad(in + 6) << 3, SafeLoad(in + 5),
                              SafeLoad(in + 4) >> 7 | SafeLoad(in + 5) << 25, SafeLoad(in + 3) >> 12 | SafeLoad(in + 4) << 20,
                              SafeLoad(in + 2) >> 17 | SafeLoad(in + 3) << 15, SafeLoad(in + 1) >> 22 | SafeLoad(in + 2) << 10,
                              SafeLoad(in + 0) >> 27 | SafeLoad(in + 1) << 5, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(5, 0, 0, 0,
                                0, 0, 3, 0,
                                0, 0, 0, 0,
                                1, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 26), SafeLoad(in + 25) >> 10 | SafeLoad(in + 26) << 22,
                              SafeLoad(in + 24) >> 15 | SafeLoad(in + 25) << 17, SafeLoad(in + 23) >> 20 | SafeLoad(in + 24) << 12,
                              SafeLoad(in + 22) >> 25 | SafeLoad(in + 23) << 7, SafeLoad(in + 21) >> 30 | SafeLoad(in + 22) << 2,
                              SafeLoad(in + 21), SafeLoad(in + 20) >> 8 | SafeLoad(in + 21) << 24,
                              SafeLoad(in + 19) >> 13 | SafeLoad(in + 20) << 19, SafeLoad(in + 18) >> 18 | SafeLoad(in + 19) << 14,
                              SafeLoad(in + 17) >> 23 | SafeLoad(in + 18) << 9, SafeLoad(in + 16) >> 28 | SafeLoad(in + 17) << 4,
                              SafeLoad(in + 16), SafeLoad(in + 15) >> 6 | SafeLoad(in + 16) << 26,
                              SafeLoad(in + 14) >> 11 | SafeLoad(in + 15) << 21, SafeLoad(in + 13) >> 16 | SafeLoad(in + 14) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 27;

  return in;
}

inline static const uint32_t* unpack28_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0xfffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(4, 0, 0, 0,
                                0, 0, 0, 0,
                                4, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 13), SafeLoad(in + 12) >> 8 | SafeLoad(in + 13) << 24,
                              SafeLoad(in + 11) >> 12 | SafeLoad(in + 12) << 20, SafeLoad(in + 10) >> 16 | SafeLoad(in + 11) << 16,
                              SafeLoad(in + 9) >> 20 | SafeLoad(in + 10) << 12, SafeLoad(in + 8) >> 24 | SafeLoad(in + 9) << 8,
                              SafeLoad(in + 7) >> 28 | SafeLoad(in + 8) << 4, SafeLoad(in + 7),
                              SafeLoad(in + 6), SafeLoad(in + 5) >> 8 | SafeLoad(in + 6) << 24,
                              SafeLoad(in + 4) >> 12 | SafeLoad(in + 5) << 20, SafeLoad(in + 3) >> 16 | SafeLoad(in + 4) << 16,
                              SafeLoad(in + 2) >> 20 | SafeLoad(in + 3) << 12, SafeLoad(in + 1) >> 24 | SafeLoad(in + 2) << 8,
                              SafeLoad(in + 0) >> 28 | SafeLoad(in + 1) << 4, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(4, 0, 0, 0,
                                0, 0, 0, 0,
                                4, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 27), SafeLoad(in + 26) >> 8 | SafeLoad(in + 27) << 24,
                              SafeLoad(in + 25) >> 12 | SafeLoad(in + 26) << 20, SafeLoad(in + 24) >> 16 | SafeLoad(in + 25) << 16,
                              SafeLoad(in + 23) >> 20 | SafeLoad(in + 24) << 12, SafeLoad(in + 22) >> 24 | SafeLoad(in + 23) << 8,
                              SafeLoad(in + 21) >> 28 | SafeLoad(in + 22) << 4, SafeLoad(in + 21),
                              SafeLoad(in + 20), SafeLoad(in + 19) >> 8 | SafeLoad(in + 20) << 24,
                              SafeLoad(in + 18) >> 12 | SafeLoad(in + 19) << 20, SafeLoad(in + 17) >> 16 | SafeLoad(in + 18) << 16,
                              SafeLoad(in + 16) >> 20 | SafeLoad(in + 17) << 12, SafeLoad(in + 15) >> 24 | SafeLoad(in + 16) << 8,
                              SafeLoad(in + 14) >> 28 | SafeLoad(in + 15) << 4, SafeLoad(in + 14));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 28;

  return in;
}

inline static const uint32_t* unpack29_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x1fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 0,
                                0, 2, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 13) >> 19 | SafeLoad(in + 14) << 13, SafeLoad(in + 12) >> 22 | SafeLoad(in + 13) << 10,
                              SafeLoad(in + 11) >> 25 | SafeLoad(in + 12) << 7, SafeLoad(in + 10) >> 28 | SafeLoad(in + 11) << 4,
                              SafeLoad(in + 9) >> 31 | SafeLoad(in + 10) << 1, SafeLoad(in + 9),
                              SafeLoad(in + 8) >> 5 | SafeLoad(in + 9) << 27, SafeLoad(in + 7) >> 8 | SafeLoad(in + 8) << 24,
                              SafeLoad(in + 6) >> 11 | SafeLoad(in + 7) << 21, SafeLoad(in + 5) >> 14 | SafeLoad(in + 6) << 18,
                              SafeLoad(in + 4) >> 17 | SafeLoad(in + 5) << 15, SafeLoad(in + 3) >> 20 | SafeLoad(in + 4) << 12,
                              SafeLoad(in + 2) >> 23 | SafeLoad(in + 3) << 9, SafeLoad(in + 1) >> 26 | SafeLoad(in + 2) << 6,
                              SafeLoad(in + 0) >> 29 | SafeLoad(in + 1) << 3, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(3, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 1, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 28), SafeLoad(in + 27) >> 6 | SafeLoad(in + 28) << 26,
                              SafeLoad(in + 26) >> 9 | SafeLoad(in + 27) << 23, SafeLoad(in + 25) >> 12 | SafeLoad(in + 26) << 20,
                              SafeLoad(in + 24) >> 15 | SafeLoad(in + 25) << 17, SafeLoad(in + 23) >> 18 | SafeLoad(in + 24) << 14,
                              SafeLoad(in + 22) >> 21 | SafeLoad(in + 23) << 11, SafeLoad(in + 21) >> 24 | SafeLoad(in + 22) << 8,
                              SafeLoad(in + 20) >> 27 | SafeLoad(in + 21) << 5, SafeLoad(in + 19) >> 30 | SafeLoad(in + 20) << 2,
                              SafeLoad(in + 19), SafeLoad(in + 18) >> 4 | SafeLoad(in + 19) << 28,
                              SafeLoad(in + 17) >> 7 | SafeLoad(in + 18) << 25, SafeLoad(in + 16) >> 10 | SafeLoad(in + 17) << 22,
                              SafeLoad(in + 15) >> 13 | SafeLoad(in + 16) << 19, SafeLoad(in + 14) >> 16 | SafeLoad(in + 15) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 29;

  return in;
}

inline static const uint32_t* unpack30_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x3fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(2, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 14), SafeLoad(in + 13) >> 4 | SafeLoad(in + 14) << 28,
                              SafeLoad(in + 12) >> 6 | SafeLoad(in + 13) << 26, SafeLoad(in + 11) >> 8 | SafeLoad(in + 12) << 24,
                              SafeLoad(in + 10) >> 10 | SafeLoad(in + 11) << 22, SafeLoad(in + 9) >> 12 | SafeLoad(in + 10) << 20,
                              SafeLoad(in + 8) >> 14 | SafeLoad(in + 9) << 18, SafeLoad(in + 7) >> 16 | SafeLoad(in + 8) << 16,
                              SafeLoad(in + 6) >> 18 | SafeLoad(in + 7) << 14, SafeLoad(in + 5) >> 20 | SafeLoad(in + 6) << 12,
                              SafeLoad(in + 4) >> 22 | SafeLoad(in + 5) << 10, SafeLoad(in + 3) >> 24 | SafeLoad(in + 4) << 8,
                              SafeLoad(in + 2) >> 26 | SafeLoad(in + 3) << 6, SafeLoad(in + 1) >> 28 | SafeLoad(in + 2) << 4,
                              SafeLoad(in + 0) >> 30 | SafeLoad(in + 1) << 2, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(2, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 29), SafeLoad(in + 28) >> 4 | SafeLoad(in + 29) << 28,
                              SafeLoad(in + 27) >> 6 | SafeLoad(in + 28) << 26, SafeLoad(in + 26) >> 8 | SafeLoad(in + 27) << 24,
                              SafeLoad(in + 25) >> 10 | SafeLoad(in + 26) << 22, SafeLoad(in + 24) >> 12 | SafeLoad(in + 25) << 20,
                              SafeLoad(in + 23) >> 14 | SafeLoad(in + 24) << 18, SafeLoad(in + 22) >> 16 | SafeLoad(in + 23) << 16,
                              SafeLoad(in + 21) >> 18 | SafeLoad(in + 22) << 14, SafeLoad(in + 20) >> 20 | SafeLoad(in + 21) << 12,
                              SafeLoad(in + 19) >> 22 | SafeLoad(in + 20) << 10, SafeLoad(in + 18) >> 24 | SafeLoad(in + 19) << 8,
                              SafeLoad(in + 17) >> 26 | SafeLoad(in + 18) << 6, SafeLoad(in + 16) >> 28 | SafeLoad(in + 17) << 4,
                              SafeLoad(in + 15) >> 30 | SafeLoad(in + 16) << 2, SafeLoad(in + 15));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 30;

  return in;
}

inline static const uint32_t* unpack31_32_avx512(const uint32_t* in, uint32_t* out) {
  using ::arrow::util::SafeLoad;
  uint32_t mask = 0x7fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results;

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 14) >> 17 | SafeLoad(in + 15) << 15, SafeLoad(in + 13) >> 18 | SafeLoad(in + 14) << 14,
                              SafeLoad(in + 12) >> 19 | SafeLoad(in + 13) << 13, SafeLoad(in + 11) >> 20 | SafeLoad(in + 12) << 12,
                              SafeLoad(in + 10) >> 21 | SafeLoad(in + 11) << 11, SafeLoad(in + 9) >> 22 | SafeLoad(in + 10) << 10,
                              SafeLoad(in + 8) >> 23 | SafeLoad(in + 9) << 9, SafeLoad(in + 7) >> 24 | SafeLoad(in + 8) << 8,
                              SafeLoad(in + 6) >> 25 | SafeLoad(in + 7) << 7, SafeLoad(in + 5) >> 26 | SafeLoad(in + 6) << 6,
                              SafeLoad(in + 4) >> 27 | SafeLoad(in + 5) << 5, SafeLoad(in + 3) >> 28 | SafeLoad(in + 4) << 4,
                              SafeLoad(in + 2) >> 29 | SafeLoad(in + 3) << 3, SafeLoad(in + 1) >> 30 | SafeLoad(in + 2) << 2,
                              SafeLoad(in + 0) >> 31 | SafeLoad(in + 1) << 1, SafeLoad(in + 0));
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(1, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(SafeLoad(in + 30), SafeLoad(in + 29) >> 2 | SafeLoad(in + 30) << 30,
                              SafeLoad(in + 28) >> 3 | SafeLoad(in + 29) << 29, SafeLoad(in + 27) >> 4 | SafeLoad(in + 28) << 28,
                              SafeLoad(in + 26) >> 5 | SafeLoad(in + 27) << 27, SafeLoad(in + 25) >> 6 | SafeLoad(in + 26) << 26,
                              SafeLoad(in + 24) >> 7 | SafeLoad(in + 25) << 25, SafeLoad(in + 23) >> 8 | SafeLoad(in + 24) << 24,
                              SafeLoad(in + 22) >> 9 | SafeLoad(in + 23) << 23, SafeLoad(in + 21) >> 10 | SafeLoad(in + 22) << 22,
                              SafeLoad(in + 20) >> 11 | SafeLoad(in + 21) << 21, SafeLoad(in + 19) >> 12 | SafeLoad(in + 20) << 20,
                              SafeLoad(in + 18) >> 13 | SafeLoad(in + 19) << 19, SafeLoad(in + 17) >> 14 | SafeLoad(in + 18) << 18,
                              SafeLoad(in + 16) >> 15 | SafeLoad(in + 17) << 17, SafeLoad(in + 15) >> 16 | SafeLoad(in + 16) << 16);
  results = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);
  _mm512_storeu_si512(out, results);
  out += 16;

  in += 31;

  return in;
}

inline static const uint32_t* unpack32_32_avx512(const uint32_t* in, uint32_t* out) {
  memcpy(out, in, 32 * sizeof(*out));
  in += 32;
  out += 32;

  return in;
}

}  // namespace internal
}  // namespace arrow
