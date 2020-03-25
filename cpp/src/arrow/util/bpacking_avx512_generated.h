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

#include <immintrin.h>

namespace arrow {
namespace internal {

inline const uint32_t* nullunpacker32(const uint32_t* in, uint32_t* out) {
  memset(out, 0x0, 32 * sizeof(*out));
  out += 32;

  return in;
}

inline const uint32_t* unpack1_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(15, 14, 13, 12,
                                11, 10, 9, 8,
                                7, 6, 5, 4,
                                3, 2, 1, 0);
  reg_inls = _mm512_set_epi32(in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(31, 30, 29, 28,
                                27, 26, 25, 24,
                                23, 22, 21, 20,
                                19, 18, 17, 16);
  reg_inls = _mm512_set_epi32(in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 1;

  return in;
}

inline const uint32_t* unpack2_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(30, 28, 26, 24,
                                22, 20, 18, 16,
                                14, 12, 10, 8,
                                6, 4, 2, 0);
  reg_inls = _mm512_set_epi32(in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(30, 28, 26, 24,
                                22, 20, 18, 16,
                                14, 12, 10, 8,
                                6, 4, 2, 0);
  reg_inls = _mm512_set_epi32(in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 2;

  return in;
}

inline const uint32_t* unpack3_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(13, 10, 7, 4,
                                1, 0, 27, 24,
                                21, 18, 15, 12,
                                9, 6, 3, 0);
  reg_inls = _mm512_set_epi32(in[1], in[1],
                              in[1], in[1],
                              in[1], in[0] >> 30 | in[1] << 2,
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(29, 26, 23, 20,
                                17, 14, 11, 8,
                                5, 2, 0, 28,
                                25, 22, 19, 16);
  reg_inls = _mm512_set_epi32(in[2], in[2],
                              in[2], in[2],
                              in[2], in[2],
                              in[2], in[2],
                              in[2], in[2],
                              in[1] >> 31 | in[2] << 1, in[1],
                              in[1], in[1],
                              in[1], in[1]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 3;

  return in;
}

inline const uint32_t* unpack4_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xf;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(28, 24, 20, 16,
                                12, 8, 4, 0,
                                28, 24, 20, 16,
                                12, 8, 4, 0);
  reg_inls = _mm512_set_epi32(in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[1],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(28, 24, 20, 16,
                                12, 8, 4, 0,
                                28, 24, 20, 16,
                                12, 8, 4, 0);
  reg_inls = _mm512_set_epi32(in[3], in[3],
                              in[3], in[3],
                              in[3], in[3],
                              in[3], in[3],
                              in[2], in[2],
                              in[2], in[2],
                              in[2], in[2],
                              in[2], in[2]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 4;

  return in;
}

inline const uint32_t* unpack5_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(11, 6, 1, 0,
                                23, 18, 13, 8,
                                3, 0, 25, 20,
                                15, 10, 5, 0);
  reg_inls = _mm512_set_epi32(in[2], in[2],
                              in[2], in[1] >> 28 | in[2] << 4,
                              in[1], in[1],
                              in[1], in[1],
                              in[1], in[0] >> 30 | in[1] << 2,
                              in[0], in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(27, 22, 17, 12,
                                7, 2, 0, 24,
                                19, 14, 9, 4,
                                0, 26, 21, 16);
  reg_inls = _mm512_set_epi32(in[4], in[4],
                              in[4], in[4],
                              in[4], in[4],
                              in[3] >> 29 | in[4] << 3, in[3],
                              in[3], in[3],
                              in[3], in[3],
                              in[2] >> 31 | in[3] << 1, in[2],
                              in[2], in[2]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 5;

  return in;
}

inline const uint32_t* unpack6_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(26, 20, 14, 8,
                                2, 0, 22, 16,
                                10, 4, 0, 24,
                                18, 12, 6, 0);
  reg_inls = _mm512_set_epi32(in[2], in[2],
                              in[2], in[2],
                              in[2], in[1] >> 28 | in[2] << 4,
                              in[1], in[1],
                              in[1], in[1],
                              in[0] >> 30 | in[1] << 2, in[0],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(26, 20, 14, 8,
                                2, 0, 22, 16,
                                10, 4, 0, 24,
                                18, 12, 6, 0);
  reg_inls = _mm512_set_epi32(in[5], in[5],
                              in[5], in[5],
                              in[5], in[4] >> 28 | in[5] << 4,
                              in[4], in[4],
                              in[4], in[4],
                              in[3] >> 30 | in[4] << 2, in[3],
                              in[3], in[3],
                              in[3], in[3]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 6;

  return in;
}

inline const uint32_t* unpack7_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7f;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(9, 2, 0, 20,
                                13, 6, 0, 24,
                                17, 10, 3, 0,
                                21, 14, 7, 0);
  reg_inls = _mm512_set_epi32(in[3], in[3],
                              in[2] >> 27 | in[3] << 5, in[2],
                              in[2], in[2],
                              in[1] >> 31 | in[2] << 1, in[1],
                              in[1], in[1],
                              in[1], in[0] >> 28 | in[1] << 4,
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(25, 18, 11, 4,
                                0, 22, 15, 8,
                                1, 0, 19, 12,
                                5, 0, 23, 16);
  reg_inls = _mm512_set_epi32(in[6], in[6],
                              in[6], in[6],
                              in[5] >> 29 | in[6] << 3, in[5],
                              in[5], in[5],
                              in[5], in[4] >> 26 | in[5] << 6,
                              in[4], in[4],
                              in[4], in[3] >> 30 | in[4] << 2,
                              in[3], in[3]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 7;

  return in;
}

inline const uint32_t* unpack8_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0);
  reg_inls = _mm512_set_epi32(in[3], in[3],
                              in[3], in[3],
                              in[2], in[2],
                              in[2], in[2],
                              in[1], in[1],
                              in[1], in[1],
                              in[0], in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0,
                                24, 16, 8, 0);
  reg_inls = _mm512_set_epi32(in[7], in[7],
                              in[7], in[7],
                              in[6], in[6],
                              in[6], in[6],
                              in[5], in[5],
                              in[5], in[5],
                              in[4], in[4],
                              in[4], in[4]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 8;

  return in;
}

inline const uint32_t* unpack9_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(7, 0, 21, 12,
                                3, 0, 17, 8,
                                0, 22, 13, 4,
                                0, 18, 9, 0);
  reg_inls = _mm512_set_epi32(in[4], in[3] >> 30 | in[4] << 2,
                              in[3], in[3],
                              in[3], in[2] >> 26 | in[3] << 6,
                              in[2], in[2],
                              in[1] >> 31 | in[2] << 1, in[1],
                              in[1], in[1],
                              in[0] >> 27 | in[1] << 5, in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(23, 14, 5, 0,
                                19, 10, 1, 0,
                                15, 6, 0, 20,
                                11, 2, 0, 16);
  reg_inls = _mm512_set_epi32(in[8], in[8],
                              in[8], in[7] >> 28 | in[8] << 4,
                              in[7], in[7],
                              in[7], in[6] >> 24 | in[7] << 8,
                              in[6], in[6],
                              in[5] >> 29 | in[6] << 3, in[5],
                              in[5], in[5],
                              in[4] >> 25 | in[5] << 7, in[4]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 9;

  return in;
}

inline const uint32_t* unpack10_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(22, 12, 2, 0,
                                14, 4, 0, 16,
                                6, 0, 18, 8,
                                0, 20, 10, 0);
  reg_inls = _mm512_set_epi32(in[4], in[4],
                              in[4], in[3] >> 24 | in[4] << 8,
                              in[3], in[3],
                              in[2] >> 26 | in[3] << 6, in[2],
                              in[2], in[1] >> 28 | in[2] << 4,
                              in[1], in[1],
                              in[0] >> 30 | in[1] << 2, in[0],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(22, 12, 2, 0,
                                14, 4, 0, 16,
                                6, 0, 18, 8,
                                0, 20, 10, 0);
  reg_inls = _mm512_set_epi32(in[9], in[9],
                              in[9], in[8] >> 24 | in[9] << 8,
                              in[8], in[8],
                              in[7] >> 26 | in[8] << 6, in[7],
                              in[7], in[6] >> 28 | in[7] << 4,
                              in[6], in[6],
                              in[5] >> 30 | in[6] << 2, in[5],
                              in[5], in[5]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 10;

  return in;
}

inline const uint32_t* unpack11_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(5, 0, 15, 4,
                                0, 14, 3, 0,
                                13, 2, 0, 12,
                                1, 0, 11, 0);
  reg_inls = _mm512_set_epi32(in[5], in[4] >> 26 | in[5] << 6,
                              in[4], in[4],
                              in[3] >> 25 | in[4] << 7, in[3],
                              in[3], in[2] >> 24 | in[3] << 8,
                              in[2], in[2],
                              in[1] >> 23 | in[2] << 9, in[1],
                              in[1], in[0] >> 22 | in[1] << 10,
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(21, 10, 0, 20,
                                9, 0, 19, 8,
                                0, 18, 7, 0,
                                17, 6, 0, 16);
  reg_inls = _mm512_set_epi32(in[10], in[10],
                              in[9] >> 31 | in[10] << 1, in[9],
                              in[9], in[8] >> 30 | in[9] << 2,
                              in[8], in[8],
                              in[7] >> 29 | in[8] << 3, in[7],
                              in[7], in[6] >> 28 | in[7] << 4,
                              in[6], in[6],
                              in[5] >> 27 | in[6] << 5, in[5]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 11;

  return in;
}

inline const uint32_t* unpack12_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(20, 8, 0, 16,
                                4, 0, 12, 0,
                                20, 8, 0, 16,
                                4, 0, 12, 0);
  reg_inls = _mm512_set_epi32(in[5], in[5],
                              in[4] >> 28 | in[5] << 4, in[4],
                              in[4], in[3] >> 24 | in[4] << 8,
                              in[3], in[3],
                              in[2], in[2],
                              in[1] >> 28 | in[2] << 4, in[1],
                              in[1], in[0] >> 24 | in[1] << 8,
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(20, 8, 0, 16,
                                4, 0, 12, 0,
                                20, 8, 0, 16,
                                4, 0, 12, 0);
  reg_inls = _mm512_set_epi32(in[11], in[11],
                              in[10] >> 28 | in[11] << 4, in[10],
                              in[10], in[9] >> 24 | in[10] << 8,
                              in[9], in[9],
                              in[8], in[8],
                              in[7] >> 28 | in[8] << 4, in[7],
                              in[7], in[6] >> 24 | in[7] << 8,
                              in[6], in[6]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 12;

  return in;
}

inline const uint32_t* unpack13_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(3, 0, 9, 0,
                                15, 2, 0, 8,
                                0, 14, 1, 0,
                                7, 0, 13, 0);
  reg_inls = _mm512_set_epi32(in[6], in[5] >> 22 | in[6] << 10,
                              in[5], in[4] >> 28 | in[5] << 4,
                              in[4], in[4],
                              in[3] >> 21 | in[4] << 11, in[3],
                              in[2] >> 27 | in[3] << 5, in[2],
                              in[2], in[1] >> 20 | in[2] << 12,
                              in[1], in[0] >> 26 | in[1] << 6,
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(19, 6, 0, 12,
                                0, 18, 5, 0,
                                11, 0, 17, 4,
                                0, 10, 0, 16);
  reg_inls = _mm512_set_epi32(in[12], in[12],
                              in[11] >> 25 | in[12] << 7, in[11],
                              in[10] >> 31 | in[11] << 1, in[10],
                              in[10], in[9] >> 24 | in[10] << 8,
                              in[9], in[8] >> 30 | in[9] << 2,
                              in[8], in[8],
                              in[7] >> 23 | in[8] << 9, in[7],
                              in[6] >> 29 | in[7] << 3, in[6]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 13;

  return in;
}

inline const uint32_t* unpack14_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(18, 4, 0, 8,
                                0, 12, 0, 16,
                                2, 0, 6, 0,
                                10, 0, 14, 0);
  reg_inls = _mm512_set_epi32(in[6], in[6],
                              in[5] >> 22 | in[6] << 10, in[5],
                              in[4] >> 26 | in[5] << 6, in[4],
                              in[3] >> 30 | in[4] << 2, in[3],
                              in[3], in[2] >> 20 | in[3] << 12,
                              in[2], in[1] >> 24 | in[2] << 8,
                              in[1], in[0] >> 28 | in[1] << 4,
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(18, 4, 0, 8,
                                0, 12, 0, 16,
                                2, 0, 6, 0,
                                10, 0, 14, 0);
  reg_inls = _mm512_set_epi32(in[13], in[13],
                              in[12] >> 22 | in[13] << 10, in[12],
                              in[11] >> 26 | in[12] << 6, in[11],
                              in[10] >> 30 | in[11] << 2, in[10],
                              in[10], in[9] >> 20 | in[10] << 12,
                              in[9], in[8] >> 24 | in[9] << 8,
                              in[8], in[7] >> 28 | in[8] << 4,
                              in[7], in[7]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 14;

  return in;
}

inline const uint32_t* unpack15_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(1, 0, 3, 0,
                                5, 0, 7, 0,
                                9, 0, 11, 0,
                                13, 0, 15, 0);
  reg_inls = _mm512_set_epi32(in[7], in[6] >> 18 | in[7] << 14,
                              in[6], in[5] >> 20 | in[6] << 12,
                              in[5], in[4] >> 22 | in[5] << 10,
                              in[4], in[3] >> 24 | in[4] << 8,
                              in[3], in[2] >> 26 | in[3] << 6,
                              in[2], in[1] >> 28 | in[2] << 4,
                              in[1], in[0] >> 30 | in[1] << 2,
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(17, 2, 0, 4,
                                0, 6, 0, 8,
                                0, 10, 0, 12,
                                0, 14, 0, 16);
  reg_inls = _mm512_set_epi32(in[14], in[14],
                              in[13] >> 19 | in[14] << 13, in[13],
                              in[12] >> 21 | in[13] << 11, in[12],
                              in[11] >> 23 | in[12] << 9, in[11],
                              in[10] >> 25 | in[11] << 7, in[10],
                              in[9] >> 27 | in[10] << 5, in[9],
                              in[8] >> 29 | in[9] << 3, in[8],
                              in[7] >> 31 | in[8] << 1, in[7]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 15;

  return in;
}

inline const uint32_t* unpack16_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0);
  reg_inls = _mm512_set_epi32(in[7], in[7],
                              in[6], in[6],
                              in[5], in[5],
                              in[4], in[4],
                              in[3], in[3],
                              in[2], in[2],
                              in[1], in[1],
                              in[0], in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0,
                                16, 0, 16, 0);
  reg_inls = _mm512_set_epi32(in[15], in[15],
                              in[14], in[14],
                              in[13], in[13],
                              in[12], in[12],
                              in[11], in[11],
                              in[10], in[10],
                              in[9], in[9],
                              in[8], in[8]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 16;

  return in;
}

inline const uint32_t* unpack17_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 14, 0, 12,
                                0, 10, 0, 8,
                                0, 6, 0, 4,
                                0, 2, 0, 0);
  reg_inls = _mm512_set_epi32(in[7] >> 31 | in[8] << 1, in[7],
                              in[6] >> 29 | in[7] << 3, in[6],
                              in[5] >> 27 | in[6] << 5, in[5],
                              in[4] >> 25 | in[5] << 7, in[4],
                              in[3] >> 23 | in[4] << 9, in[3],
                              in[2] >> 21 | in[3] << 11, in[2],
                              in[1] >> 19 | in[2] << 13, in[1],
                              in[0] >> 17 | in[1] << 15, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(15, 0, 13, 0,
                                11, 0, 9, 0,
                                7, 0, 5, 0,
                                3, 0, 1, 0);
  reg_inls = _mm512_set_epi32(in[16], in[15] >> 30 | in[16] << 2,
                              in[15], in[14] >> 28 | in[15] << 4,
                              in[14], in[13] >> 26 | in[14] << 6,
                              in[13], in[12] >> 24 | in[13] << 8,
                              in[12], in[11] >> 22 | in[12] << 10,
                              in[11], in[10] >> 20 | in[11] << 12,
                              in[10], in[9] >> 18 | in[10] << 14,
                              in[9], in[8] >> 16 | in[9] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 17;

  return in;
}

inline const uint32_t* unpack18_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(14, 0, 10, 0,
                                6, 0, 2, 0,
                                0, 12, 0, 8,
                                0, 4, 0, 0);
  reg_inls = _mm512_set_epi32(in[8], in[7] >> 28 | in[8] << 4,
                              in[7], in[6] >> 24 | in[7] << 8,
                              in[6], in[5] >> 20 | in[6] << 12,
                              in[5], in[4] >> 16 | in[5] << 16,
                              in[3] >> 30 | in[4] << 2, in[3],
                              in[2] >> 26 | in[3] << 6, in[2],
                              in[1] >> 22 | in[2] << 10, in[1],
                              in[0] >> 18 | in[1] << 14, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(14, 0, 10, 0,
                                6, 0, 2, 0,
                                0, 12, 0, 8,
                                0, 4, 0, 0);
  reg_inls = _mm512_set_epi32(in[17], in[16] >> 28 | in[17] << 4,
                              in[16], in[15] >> 24 | in[16] << 8,
                              in[15], in[14] >> 20 | in[15] << 12,
                              in[14], in[13] >> 16 | in[14] << 16,
                              in[12] >> 30 | in[13] << 2, in[12],
                              in[11] >> 26 | in[12] << 6, in[11],
                              in[10] >> 22 | in[11] << 10, in[10],
                              in[9] >> 18 | in[10] << 14, in[9]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 18;

  return in;
}

inline const uint32_t* unpack19_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 10, 0, 4,
                                0, 0, 11, 0,
                                5, 0, 0, 12,
                                0, 6, 0, 0);
  reg_inls = _mm512_set_epi32(in[8] >> 29 | in[9] << 3, in[8],
                              in[7] >> 23 | in[8] << 9, in[7],
                              in[6] >> 17 | in[7] << 15, in[5] >> 30 | in[6] << 2,
                              in[5], in[4] >> 24 | in[5] << 8,
                              in[4], in[3] >> 18 | in[4] << 14,
                              in[2] >> 31 | in[3] << 1, in[2],
                              in[1] >> 25 | in[2] << 7, in[1],
                              in[0] >> 19 | in[1] << 13, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(13, 0, 7, 0,
                                1, 0, 0, 8,
                                0, 2, 0, 0,
                                9, 0, 3, 0);
  reg_inls = _mm512_set_epi32(in[18], in[17] >> 26 | in[18] << 6,
                              in[17], in[16] >> 20 | in[17] << 12,
                              in[16], in[15] >> 14 | in[16] << 18,
                              in[14] >> 27 | in[15] << 5, in[14],
                              in[13] >> 21 | in[14] << 11, in[13],
                              in[12] >> 15 | in[13] << 17, in[11] >> 28 | in[12] << 4,
                              in[11], in[10] >> 22 | in[11] << 10,
                              in[10], in[9] >> 16 | in[10] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 19;

  return in;
}

inline const uint32_t* unpack20_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(12, 0, 4, 0,
                                0, 8, 0, 0,
                                12, 0, 4, 0,
                                0, 8, 0, 0);
  reg_inls = _mm512_set_epi32(in[9], in[8] >> 24 | in[9] << 8,
                              in[8], in[7] >> 16 | in[8] << 16,
                              in[6] >> 28 | in[7] << 4, in[6],
                              in[5] >> 20 | in[6] << 12, in[5],
                              in[4], in[3] >> 24 | in[4] << 8,
                              in[3], in[2] >> 16 | in[3] << 16,
                              in[1] >> 28 | in[2] << 4, in[1],
                              in[0] >> 20 | in[1] << 12, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(12, 0, 4, 0,
                                0, 8, 0, 0,
                                12, 0, 4, 0,
                                0, 8, 0, 0);
  reg_inls = _mm512_set_epi32(in[19], in[18] >> 24 | in[19] << 8,
                              in[18], in[17] >> 16 | in[18] << 16,
                              in[16] >> 28 | in[17] << 4, in[16],
                              in[15] >> 20 | in[16] << 12, in[15],
                              in[14], in[13] >> 24 | in[14] << 8,
                              in[13], in[12] >> 16 | in[13] << 16,
                              in[11] >> 28 | in[12] << 4, in[11],
                              in[10] >> 20 | in[11] << 12, in[10]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 20;

  return in;
}

inline const uint32_t* unpack21_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 6, 0, 0,
                                7, 0, 0, 8,
                                0, 0, 9, 0,
                                0, 10, 0, 0);
  reg_inls = _mm512_set_epi32(in[9] >> 27 | in[10] << 5, in[9],
                              in[8] >> 17 | in[9] << 15, in[7] >> 28 | in[8] << 4,
                              in[7], in[6] >> 18 | in[7] << 14,
                              in[5] >> 29 | in[6] << 3, in[5],
                              in[4] >> 19 | in[5] << 13, in[3] >> 30 | in[4] << 2,
                              in[3], in[2] >> 20 | in[3] << 12,
                              in[1] >> 31 | in[2] << 1, in[1],
                              in[0] >> 21 | in[1] << 11, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(11, 0, 1, 0,
                                0, 2, 0, 0,
                                3, 0, 0, 4,
                                0, 0, 5, 0);
  reg_inls = _mm512_set_epi32(in[20], in[19] >> 22 | in[20] << 10,
                              in[19], in[18] >> 12 | in[19] << 20,
                              in[17] >> 23 | in[18] << 9, in[17],
                              in[16] >> 13 | in[17] << 19, in[15] >> 24 | in[16] << 8,
                              in[15], in[14] >> 14 | in[15] << 18,
                              in[13] >> 25 | in[14] << 7, in[13],
                              in[12] >> 15 | in[13] << 17, in[11] >> 26 | in[12] << 6,
                              in[11], in[10] >> 16 | in[11] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 21;

  return in;
}

inline const uint32_t* unpack22_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(10, 0, 0, 8,
                                0, 0, 6, 0,
                                0, 4, 0, 0,
                                2, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[10], in[9] >> 20 | in[10] << 12,
                              in[8] >> 30 | in[9] << 2, in[8],
                              in[7] >> 18 | in[8] << 14, in[6] >> 28 | in[7] << 4,
                              in[6], in[5] >> 16 | in[6] << 16,
                              in[4] >> 26 | in[5] << 6, in[4],
                              in[3] >> 14 | in[4] << 18, in[2] >> 24 | in[3] << 8,
                              in[2], in[1] >> 12 | in[2] << 20,
                              in[0] >> 22 | in[1] << 10, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(10, 0, 0, 8,
                                0, 0, 6, 0,
                                0, 4, 0, 0,
                                2, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[21], in[20] >> 20 | in[21] << 12,
                              in[19] >> 30 | in[20] << 2, in[19],
                              in[18] >> 18 | in[19] << 14, in[17] >> 28 | in[18] << 4,
                              in[17], in[16] >> 16 | in[17] << 16,
                              in[15] >> 26 | in[16] << 6, in[15],
                              in[14] >> 14 | in[15] << 18, in[13] >> 24 | in[14] << 8,
                              in[13], in[12] >> 12 | in[13] << 20,
                              in[11] >> 22 | in[12] << 10, in[11]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 22;

  return in;
}

inline const uint32_t* unpack23_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 2, 0, 0,
                                0, 6, 0, 0,
                                1, 0, 0, 0,
                                5, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[10] >> 25 | in[11] << 7, in[10],
                              in[9] >> 11 | in[10] << 21, in[8] >> 20 | in[9] << 12,
                              in[7] >> 29 | in[8] << 3, in[7],
                              in[6] >> 15 | in[7] << 17, in[5] >> 24 | in[6] << 8,
                              in[5], in[4] >> 10 | in[5] << 22,
                              in[3] >> 19 | in[4] << 13, in[2] >> 28 | in[3] << 4,
                              in[2], in[1] >> 14 | in[2] << 18,
                              in[0] >> 23 | in[1] << 9, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(9, 0, 0, 4,
                                0, 0, 0, 8,
                                0, 0, 3, 0,
                                0, 0, 7, 0);
  reg_inls = _mm512_set_epi32(in[22], in[21] >> 18 | in[22] << 14,
                              in[20] >> 27 | in[21] << 5, in[20],
                              in[19] >> 13 | in[20] << 19, in[18] >> 22 | in[19] << 10,
                              in[17] >> 31 | in[18] << 1, in[17],
                              in[16] >> 17 | in[17] << 15, in[15] >> 26 | in[16] << 6,
                              in[15], in[14] >> 12 | in[15] << 20,
                              in[13] >> 21 | in[14] << 11, in[12] >> 30 | in[13] << 2,
                              in[12], in[11] >> 16 | in[12] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 23;

  return in;
}

inline const uint32_t* unpack24_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[11], in[10] >> 16 | in[11] << 16,
                              in[9] >> 24 | in[10] << 8, in[9],
                              in[8], in[7] >> 16 | in[8] << 16,
                              in[6] >> 24 | in[7] << 8, in[6],
                              in[5], in[4] >> 16 | in[5] << 16,
                              in[3] >> 24 | in[4] << 8, in[3],
                              in[2], in[1] >> 16 | in[2] << 16,
                              in[0] >> 24 | in[1] << 8, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0,
                                8, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[23], in[22] >> 16 | in[23] << 16,
                              in[21] >> 24 | in[22] << 8, in[21],
                              in[20], in[19] >> 16 | in[20] << 16,
                              in[18] >> 24 | in[19] << 8, in[18],
                              in[17], in[16] >> 16 | in[17] << 16,
                              in[15] >> 24 | in[16] << 8, in[15],
                              in[14], in[13] >> 16 | in[14] << 16,
                              in[12] >> 24 | in[13] << 8, in[12]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 24;

  return in;
}

inline const uint32_t* unpack25_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 5, 0,
                                0, 0, 1, 0,
                                0, 0, 0, 4,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[11] >> 23 | in[12] << 9, in[10] >> 30 | in[11] << 2,
                              in[10], in[9] >> 12 | in[10] << 20,
                              in[8] >> 19 | in[9] << 13, in[7] >> 26 | in[8] << 6,
                              in[7], in[6] >> 8 | in[7] << 24,
                              in[5] >> 15 | in[6] << 17, in[4] >> 22 | in[5] << 10,
                              in[3] >> 29 | in[4] << 3, in[3],
                              in[2] >> 11 | in[3] << 21, in[1] >> 18 | in[2] << 14,
                              in[0] >> 25 | in[1] << 7, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(7, 0, 0, 0,
                                3, 0, 0, 0,
                                0, 6, 0, 0,
                                0, 2, 0, 0);
  reg_inls = _mm512_set_epi32(in[24], in[23] >> 14 | in[24] << 18,
                              in[22] >> 21 | in[23] << 11, in[21] >> 28 | in[22] << 4,
                              in[21], in[20] >> 10 | in[21] << 22,
                              in[19] >> 17 | in[20] << 15, in[18] >> 24 | in[19] << 8,
                              in[17] >> 31 | in[18] << 1, in[17],
                              in[16] >> 13 | in[17] << 19, in[15] >> 20 | in[16] << 12,
                              in[14] >> 27 | in[15] << 5, in[14],
                              in[13] >> 9 | in[14] << 23, in[12] >> 16 | in[13] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 25;

  return in;
}

inline const uint32_t* unpack26_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(6, 0, 0, 0,
                                0, 4, 0, 0,
                                0, 0, 2, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[12], in[11] >> 12 | in[12] << 20,
                              in[10] >> 18 | in[11] << 14, in[9] >> 24 | in[10] << 8,
                              in[8] >> 30 | in[9] << 2, in[8],
                              in[7] >> 10 | in[8] << 22, in[6] >> 16 | in[7] << 16,
                              in[5] >> 22 | in[6] << 10, in[4] >> 28 | in[5] << 4,
                              in[4], in[3] >> 8 | in[4] << 24,
                              in[2] >> 14 | in[3] << 18, in[1] >> 20 | in[2] << 12,
                              in[0] >> 26 | in[1] << 6, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(6, 0, 0, 0,
                                0, 4, 0, 0,
                                0, 0, 2, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[25], in[24] >> 12 | in[25] << 20,
                              in[23] >> 18 | in[24] << 14, in[22] >> 24 | in[23] << 8,
                              in[21] >> 30 | in[22] << 2, in[21],
                              in[20] >> 10 | in[21] << 22, in[19] >> 16 | in[20] << 16,
                              in[18] >> 22 | in[19] << 10, in[17] >> 28 | in[18] << 4,
                              in[17], in[16] >> 8 | in[17] << 24,
                              in[15] >> 14 | in[16] << 18, in[14] >> 20 | in[15] << 12,
                              in[13] >> 26 | in[14] << 6, in[13]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 26;

  return in;
}

inline const uint32_t* unpack27_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 4,
                                0, 0, 0, 0,
                                0, 2, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[12] >> 21 | in[13] << 11, in[11] >> 26 | in[12] << 6,
                              in[10] >> 31 | in[11] << 1, in[10],
                              in[9] >> 9 | in[10] << 23, in[8] >> 14 | in[9] << 18,
                              in[7] >> 19 | in[8] << 13, in[6] >> 24 | in[7] << 8,
                              in[5] >> 29 | in[6] << 3, in[5],
                              in[4] >> 7 | in[5] << 25, in[3] >> 12 | in[4] << 20,
                              in[2] >> 17 | in[3] << 15, in[1] >> 22 | in[2] << 10,
                              in[0] >> 27 | in[1] << 5, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(5, 0, 0, 0,
                                0, 0, 3, 0,
                                0, 0, 0, 0,
                                1, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[26], in[25] >> 10 | in[26] << 22,
                              in[24] >> 15 | in[25] << 17, in[23] >> 20 | in[24] << 12,
                              in[22] >> 25 | in[23] << 7, in[21] >> 30 | in[22] << 2,
                              in[21], in[20] >> 8 | in[21] << 24,
                              in[19] >> 13 | in[20] << 19, in[18] >> 18 | in[19] << 14,
                              in[17] >> 23 | in[18] << 9, in[16] >> 28 | in[17] << 4,
                              in[16], in[15] >> 6 | in[16] << 26,
                              in[14] >> 11 | in[15] << 21, in[13] >> 16 | in[14] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 27;

  return in;
}

inline const uint32_t* unpack28_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(4, 0, 0, 0,
                                0, 0, 0, 0,
                                4, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[13], in[12] >> 8 | in[13] << 24,
                              in[11] >> 12 | in[12] << 20, in[10] >> 16 | in[11] << 16,
                              in[9] >> 20 | in[10] << 12, in[8] >> 24 | in[9] << 8,
                              in[7] >> 28 | in[8] << 4, in[7],
                              in[6], in[5] >> 8 | in[6] << 24,
                              in[4] >> 12 | in[5] << 20, in[3] >> 16 | in[4] << 16,
                              in[2] >> 20 | in[3] << 12, in[1] >> 24 | in[2] << 8,
                              in[0] >> 28 | in[1] << 4, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(4, 0, 0, 0,
                                0, 0, 0, 0,
                                4, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[27], in[26] >> 8 | in[27] << 24,
                              in[25] >> 12 | in[26] << 20, in[24] >> 16 | in[25] << 16,
                              in[23] >> 20 | in[24] << 12, in[22] >> 24 | in[23] << 8,
                              in[21] >> 28 | in[22] << 4, in[21],
                              in[20], in[19] >> 8 | in[20] << 24,
                              in[18] >> 12 | in[19] << 20, in[17] >> 16 | in[18] << 16,
                              in[16] >> 20 | in[17] << 12, in[15] >> 24 | in[16] << 8,
                              in[14] >> 28 | in[15] << 4, in[14]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 28;

  return in;
}

inline const uint32_t* unpack29_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 0,
                                0, 2, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[13] >> 19 | in[14] << 13, in[12] >> 22 | in[13] << 10,
                              in[11] >> 25 | in[12] << 7, in[10] >> 28 | in[11] << 4,
                              in[9] >> 31 | in[10] << 1, in[9],
                              in[8] >> 5 | in[9] << 27, in[7] >> 8 | in[8] << 24,
                              in[6] >> 11 | in[7] << 21, in[5] >> 14 | in[6] << 18,
                              in[4] >> 17 | in[5] << 15, in[3] >> 20 | in[4] << 12,
                              in[2] >> 23 | in[3] << 9, in[1] >> 26 | in[2] << 6,
                              in[0] >> 29 | in[1] << 3, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(3, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 1, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[28], in[27] >> 6 | in[28] << 26,
                              in[26] >> 9 | in[27] << 23, in[25] >> 12 | in[26] << 20,
                              in[24] >> 15 | in[25] << 17, in[23] >> 18 | in[24] << 14,
                              in[22] >> 21 | in[23] << 11, in[21] >> 24 | in[22] << 8,
                              in[20] >> 27 | in[21] << 5, in[19] >> 30 | in[20] << 2,
                              in[19], in[18] >> 4 | in[19] << 28,
                              in[17] >> 7 | in[18] << 25, in[16] >> 10 | in[17] << 22,
                              in[15] >> 13 | in[16] << 19, in[14] >> 16 | in[15] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 29;

  return in;
}

inline const uint32_t* unpack30_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(2, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[14], in[13] >> 4 | in[14] << 28,
                              in[12] >> 6 | in[13] << 26, in[11] >> 8 | in[12] << 24,
                              in[10] >> 10 | in[11] << 22, in[9] >> 12 | in[10] << 20,
                              in[8] >> 14 | in[9] << 18, in[7] >> 16 | in[8] << 16,
                              in[6] >> 18 | in[7] << 14, in[5] >> 20 | in[6] << 12,
                              in[4] >> 22 | in[5] << 10, in[3] >> 24 | in[4] << 8,
                              in[2] >> 26 | in[3] << 6, in[1] >> 28 | in[2] << 4,
                              in[0] >> 30 | in[1] << 2, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(2, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[29], in[28] >> 4 | in[29] << 28,
                              in[27] >> 6 | in[28] << 26, in[26] >> 8 | in[27] << 24,
                              in[25] >> 10 | in[26] << 22, in[24] >> 12 | in[25] << 20,
                              in[23] >> 14 | in[24] << 18, in[22] >> 16 | in[23] << 16,
                              in[21] >> 18 | in[22] << 14, in[20] >> 20 | in[21] << 12,
                              in[19] >> 22 | in[20] << 10, in[18] >> 24 | in[19] << 8,
                              in[17] >> 26 | in[18] << 6, in[16] >> 28 | in[17] << 4,
                              in[15] >> 30 | in[16] << 2, in[15]);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 30;

  return in;
}

inline const uint32_t* unpack31_32(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fffffff;
  __m512i reg_shifts, reg_inls, reg_masks;
  __m512i results[2];

  reg_masks = _mm512_set1_epi32(mask);

  // shift the first 16 outs
  reg_shifts = _mm512_set_epi32(0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[14] >> 17 | in[15] << 15, in[13] >> 18 | in[14] << 14,
                              in[12] >> 19 | in[13] << 13, in[11] >> 20 | in[12] << 12,
                              in[10] >> 21 | in[11] << 11, in[9] >> 22 | in[10] << 10,
                              in[8] >> 23 | in[9] << 9, in[7] >> 24 | in[8] << 8,
                              in[6] >> 25 | in[7] << 7, in[5] >> 26 | in[6] << 6,
                              in[4] >> 27 | in[5] << 5, in[3] >> 28 | in[4] << 4,
                              in[2] >> 29 | in[3] << 3, in[1] >> 30 | in[2] << 2,
                              in[0] >> 31 | in[1] << 1, in[0]);
  results[0] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  // shift the second 16 outs
  reg_shifts = _mm512_set_epi32(1, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0,
                                0, 0, 0, 0);
  reg_inls = _mm512_set_epi32(in[30], in[29] >> 2 | in[30] << 30,
                              in[28] >> 3 | in[29] << 29, in[27] >> 4 | in[28] << 28,
                              in[26] >> 5 | in[27] << 27, in[25] >> 6 | in[26] << 26,
                              in[24] >> 7 | in[25] << 25, in[23] >> 8 | in[24] << 24,
                              in[22] >> 9 | in[23] << 23, in[21] >> 10 | in[22] << 22,
                              in[20] >> 11 | in[21] << 21, in[19] >> 12 | in[20] << 20,
                              in[18] >> 13 | in[19] << 19, in[17] >> 14 | in[18] << 18,
                              in[16] >> 15 | in[17] << 17, in[15] >> 16 | in[16] << 16);
  results[1] = _mm512_and_epi32(_mm512_srlv_epi32(reg_inls, reg_shifts), reg_masks);

  memcpy(out, &results, 32 * sizeof(*out));
  out += 32;
  in += 31;

  return in;
}

inline const uint32_t* unpack32_32(const uint32_t* in, uint32_t* out) {
  memcpy(out, in, 32 * sizeof(*out));
  in += 32;
  out += 32;

  return in;
}

}  // namespace internal
}  // namespace arrow
