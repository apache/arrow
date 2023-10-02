// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdint.h>
#include <string.h>

#include "arm_neon.h"

inline const uint32_t* unpack0_32_neon(const uint32_t* in, uint32_t* out) {
  for (const uint32_t* end = out + 32; out != end; out++) {
    *out = 0;
  }

  return in;
}

inline static const uint32_t* unpack1_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 1, 2, 3};
  uint32_t shifts_2nd[4] = {4, 5, 6, 7};
  uint32_t shifts_3rd[4] = {8, 9, 10, 11};
  uint32_t shifts_4th[4] = {12, 13, 14, 15};
  uint32_t shifts_5th[4] = {16, 17, 18, 19};
  uint32_t shifts_6th[4] = {20, 21, 22, 23};
  uint32_t shifts_7th[4] = {24, 25, 26, 27};
  uint32_t shifts_8th[4] = {28, 29, 30, 31};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = in[0] >> shifts_2nd[1];
  ind[2] = in[0] >> shifts_2nd[2];
  ind[3] = in[0] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[0] >> shifts_3rd[0];
  ind[1] = in[0] >> shifts_3rd[1];
  ind[2] = in[0] >> shifts_3rd[2];
  ind[3] = in[0] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[0] >> shifts_4th[0];
  ind[1] = in[0] >> shifts_4th[1];
  ind[2] = in[0] >> shifts_4th[2];
  ind[3] = in[0] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[0] >> shifts_5th[0];
  ind[1] = in[0] >> shifts_5th[1];
  ind[2] = in[0] >> shifts_5th[2];
  ind[3] = in[0] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[0] >> shifts_6th[0];
  ind[1] = in[0] >> shifts_6th[1];
  ind[2] = in[0] >> shifts_6th[2];
  ind[3] = in[0] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[0] >> shifts_7th[0];
  ind[1] = in[0] >> shifts_7th[1];
  ind[2] = in[0] >> shifts_7th[2];
  ind[3] = in[0] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[0] >> shifts_8th[0];
  ind[1] = in[0] >> shifts_8th[1];
  ind[2] = in[0] >> shifts_8th[2];
  ind[3] = in[0] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 1;

  return in;
}

inline static const uint32_t* unpack2_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 2, 4, 6};
  uint32_t shifts_2nd[4] = {8, 10, 12, 14};
  uint32_t shifts_3rd[4] = {16, 18, 20, 22};
  uint32_t shifts_4th[4] = {24, 26, 28, 30};

  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = in[0] >> shifts_2nd[1];
  ind[2] = in[0] >> shifts_2nd[2];
  ind[3] = in[0] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[0] >> shifts_3rd[0];
  ind[1] = in[0] >> shifts_3rd[1];
  ind[2] = in[0] >> shifts_3rd[2];
  ind[3] = in[0] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[0] >> shifts_4th[0];
  ind[1] = in[0] >> shifts_4th[1];
  ind[2] = in[0] >> shifts_4th[2];
  ind[3] = in[0] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[1] >> shifts_1st[0];
  ind[1] = in[1] >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = in[1] >> shifts_2nd[1];
  ind[2] = in[1] >> shifts_2nd[2];
  ind[3] = in[1] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[1] >> shifts_3rd[0];
  ind[1] = in[1] >> shifts_3rd[1];
  ind[2] = in[1] >> shifts_3rd[2];
  ind[3] = in[1] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[1] >> shifts_4th[0];
  ind[1] = in[1] >> shifts_4th[1];
  ind[2] = in[1] >> shifts_4th[2];
  ind[3] = in[1] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 2;

  return in;
}

inline static const uint32_t* unpack3_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 3, 6, 9};
  uint32_t shifts_2nd[4] = {12, 15, 18, 21};
  uint32_t shifts_3rd[4] = {24, 27, 0, 1};
  uint32_t shifts_4th[4] = {4, 7, 10, 13};
  uint32_t shifts_5th[4] = {16, 19, 22, 25};
  uint32_t shifts_6th[4] = {28, 0, 2, 5};
  uint32_t shifts_7th[4] = {8, 11, 14, 17};
  uint32_t shifts_8th[4] = {20, 23, 26, 29};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = in[0] >> shifts_2nd[1];
  ind[2] = in[0] >> shifts_2nd[2];
  ind[3] = in[0] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[0] >> shifts_3rd[0];
  ind[1] = in[0] >> shifts_3rd[1];
  ind[2] = (in[0] >> 30 | in[1] << 2) >> shifts_3rd[2];
  ind[3] = in[1] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[1] >> shifts_4th[0];
  ind[1] = in[1] >> shifts_4th[1];
  ind[2] = in[1] >> shifts_4th[2];
  ind[3] = in[1] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[1] >> shifts_5th[0];
  ind[1] = in[1] >> shifts_5th[1];
  ind[2] = in[1] >> shifts_5th[2];
  ind[3] = in[1] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[1] >> shifts_6th[0];
  ind[1] = (in[1] >> 31 | in[2] << 1) >> shifts_6th[1];
  ind[2] = in[2] >> shifts_6th[2];
  ind[3] = in[2] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[2] >> shifts_7th[0];
  ind[1] = in[2] >> shifts_7th[1];
  ind[2] = in[2] >> shifts_7th[2];
  ind[3] = in[2] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[2] >> shifts_8th[0];
  ind[1] = in[2] >> shifts_8th[1];
  ind[2] = in[2] >> shifts_8th[2];
  ind[3] = in[2] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 3;

  return in;
}

inline static const uint32_t* unpack4_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xf;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 4, 8, 12};
  uint32_t shifts_2nd[4] = {16, 20, 24, 28};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = in[0] >> shifts_2nd[1];
  ind[2] = in[0] >> shifts_2nd[2];
  ind[3] = in[0] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[1] >> shifts_1st[0];
  ind[1] = in[1] >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = in[1] >> shifts_2nd[1];
  ind[2] = in[1] >> shifts_2nd[2];
  ind[3] = in[1] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[2] >> shifts_1st[0];
  ind[1] = in[2] >> shifts_1st[1];
  ind[2] = in[2] >> shifts_1st[2];
  ind[3] = in[2] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[2] >> shifts_2nd[0];
  ind[1] = in[2] >> shifts_2nd[1];
  ind[2] = in[2] >> shifts_2nd[2];
  ind[3] = in[2] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[3] >> shifts_1st[0];
  ind[1] = in[3] >> shifts_1st[1];
  ind[2] = in[3] >> shifts_1st[2];
  ind[3] = in[3] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[3] >> shifts_2nd[0];
  ind[1] = in[3] >> shifts_2nd[1];
  ind[2] = in[3] >> shifts_2nd[2];
  ind[3] = in[3] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 4;

  return in;
}

inline static const uint32_t* unpack5_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1f;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 5, 10, 15};
  uint32_t shifts_2nd[4] = {20, 25, 0, 3};
  uint32_t shifts_3rd[4] = {8, 13, 18, 23};
  uint32_t shifts_4th[4] = {0, 1, 6, 11};
  uint32_t shifts_5th[4] = {16, 21, 26, 0};
  uint32_t shifts_6th[4] = {4, 9, 14, 19};
  uint32_t shifts_7th[4] = {24, 0, 2, 7};
  uint32_t shifts_8th[4] = {12, 17, 22, 27};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = in[0] >> shifts_2nd[1];
  ind[2] = (in[0] >> 30 | in[1] << 2) >> shifts_2nd[2];
  ind[3] = in[1] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[1] >> shifts_3rd[0];
  ind[1] = in[1] >> shifts_3rd[1];
  ind[2] = in[1] >> shifts_3rd[2];
  ind[3] = in[1] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[1] >> 28 | in[2] << 4) >> shifts_4th[0];
  ind[1] = in[2] >> shifts_4th[1];
  ind[2] = in[2] >> shifts_4th[2];
  ind[3] = in[2] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[2] >> shifts_5th[0];
  ind[1] = in[2] >> shifts_5th[1];
  ind[2] = in[2] >> shifts_5th[2];
  ind[3] = (in[2] >> 31 | in[3] << 1) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[3] >> shifts_6th[0];
  ind[1] = in[3] >> shifts_6th[1];
  ind[2] = in[3] >> shifts_6th[2];
  ind[3] = in[3] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[3] >> shifts_7th[0];
  ind[1] = (in[3] >> 29 | in[4] << 3) >> shifts_7th[1];
  ind[2] = in[4] >> shifts_7th[2];
  ind[3] = in[4] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[4] >> shifts_8th[0];
  ind[1] = in[4] >> shifts_8th[1];
  ind[2] = in[4] >> shifts_8th[2];
  ind[3] = in[4] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 5;

  return in;
}

inline static const uint32_t* unpack6_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3f;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 6, 12, 18};
  uint32_t shifts_2nd[4] = {24, 0, 4, 10};
  uint32_t shifts_3rd[4] = {16, 22, 0, 2};
  uint32_t shifts_4th[4] = {8, 14, 20, 26};

  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[0] >> shifts_2nd[0];
  ind[1] = (in[0] >> 30 | in[1] << 2) >> shifts_2nd[1];
  ind[2] = in[1] >> shifts_2nd[2];
  ind[3] = in[1] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[1] >> shifts_3rd[0];
  ind[1] = in[1] >> shifts_3rd[1];
  ind[2] = (in[1] >> 28 | in[2] << 4) >> shifts_3rd[2];
  ind[3] = in[2] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[2] >> shifts_4th[0];
  ind[1] = in[2] >> shifts_4th[1];
  ind[2] = in[2] >> shifts_4th[2];
  ind[3] = in[2] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[3] >> shifts_1st[0];
  ind[1] = in[3] >> shifts_1st[1];
  ind[2] = in[3] >> shifts_1st[2];
  ind[3] = in[3] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[3] >> shifts_2nd[0];
  ind[1] = (in[3] >> 30 | in[4] << 2) >> shifts_2nd[1];
  ind[2] = in[4] >> shifts_2nd[2];
  ind[3] = in[4] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[4] >> shifts_3rd[0];
  ind[1] = in[4] >> shifts_3rd[1];
  ind[2] = (in[4] >> 28 | in[5] << 4) >> shifts_3rd[2];
  ind[3] = in[5] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[5] >> shifts_4th[0];
  ind[1] = in[5] >> shifts_4th[1];
  ind[2] = in[5] >> shifts_4th[2];
  ind[3] = in[5] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 6;

  return in;
}

inline static const uint32_t* unpack7_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7f;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 7, 14, 21};
  uint32_t shifts_2nd[4] = {0, 3, 10, 17};
  uint32_t shifts_3rd[4] = {24, 0, 6, 13};
  uint32_t shifts_4th[4] = {20, 0, 2, 9};
  uint32_t shifts_5th[4] = {16, 23, 0, 5};
  uint32_t shifts_6th[4] = {12, 19, 0, 1};
  uint32_t shifts_7th[4] = {8, 15, 22, 0};
  uint32_t shifts_8th[4] = {4, 11, 18, 25};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[0] >> 28 | in[1] << 4) >> shifts_2nd[0];
  ind[1] = in[1] >> shifts_2nd[1];
  ind[2] = in[1] >> shifts_2nd[2];
  ind[3] = in[1] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[1] >> shifts_3rd[0];
  ind[1] = (in[1] >> 31 | in[2] << 1) >> shifts_3rd[1];
  ind[2] = in[2] >> shifts_3rd[2];
  ind[3] = in[2] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[2] >> shifts_4th[0];
  ind[1] = (in[2] >> 27 | in[3] << 5) >> shifts_4th[1];
  ind[2] = in[3] >> shifts_4th[2];
  ind[3] = in[3] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[3] >> shifts_5th[0];
  ind[1] = in[3] >> shifts_5th[1];
  ind[2] = (in[3] >> 30 | in[4] << 2) >> shifts_5th[2];
  ind[3] = in[4] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[4] >> shifts_6th[0];
  ind[1] = in[4] >> shifts_6th[1];
  ind[2] = (in[4] >> 26 | in[5] << 6) >> shifts_6th[2];
  ind[3] = in[5] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[5] >> shifts_7th[0];
  ind[1] = in[5] >> shifts_7th[1];
  ind[2] = in[5] >> shifts_7th[2];
  ind[3] = (in[5] >> 29 | in[6] << 3) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[6] >> shifts_8th[0];
  ind[1] = in[6] >> shifts_8th[1];
  ind[2] = in[6] >> shifts_8th[2];
  ind[3] = in[6] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 7;

  return in;
}

inline static const uint32_t* unpack8_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 8, 16, 24};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = in[0] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[1] >> shifts_1st[0];
  ind[1] = in[1] >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[2] >> shifts_1st[0];
  ind[1] = in[2] >> shifts_1st[1];
  ind[2] = in[2] >> shifts_1st[2];
  ind[3] = in[2] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[3] >> shifts_1st[0];
  ind[1] = in[3] >> shifts_1st[1];
  ind[2] = in[3] >> shifts_1st[2];
  ind[3] = in[3] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[4] >> shifts_1st[0];
  ind[1] = in[4] >> shifts_1st[1];
  ind[2] = in[4] >> shifts_1st[2];
  ind[3] = in[4] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[5] >> shifts_1st[0];
  ind[1] = in[5] >> shifts_1st[1];
  ind[2] = in[5] >> shifts_1st[2];
  ind[3] = in[5] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[6] >> shifts_1st[0];
  ind[1] = in[6] >> shifts_1st[1];
  ind[2] = in[6] >> shifts_1st[2];
  ind[3] = in[6] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[7] >> shifts_1st[0];
  ind[1] = in[7] >> shifts_1st[1];
  ind[2] = in[7] >> shifts_1st[2];
  ind[3] = in[7] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 8;

  return in;
}

inline static const uint32_t* unpack9_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 9, 18, 0};
  uint32_t shifts_2nd[4] = {4, 13, 22, 0};
  uint32_t shifts_3rd[4] = {8, 17, 0, 3};
  uint32_t shifts_4th[4] = {12, 21, 0, 7};
  uint32_t shifts_5th[4] = {16, 0, 2, 11};
  uint32_t shifts_6th[4] = {20, 0, 6, 15};
  uint32_t shifts_7th[4] = {0, 1, 10, 19};
  uint32_t shifts_8th[4] = {0, 5, 14, 23};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = (in[0] >> 27 | in[1] << 5) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = in[1] >> shifts_2nd[1];
  ind[2] = in[1] >> shifts_2nd[2];
  ind[3] = (in[1] >> 31 | in[2] << 1) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[2] >> shifts_3rd[0];
  ind[1] = in[2] >> shifts_3rd[1];
  ind[2] = (in[2] >> 26 | in[3] << 6) >> shifts_3rd[2];
  ind[3] = in[3] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[3] >> shifts_4th[0];
  ind[1] = in[3] >> shifts_4th[1];
  ind[2] = (in[3] >> 30 | in[4] << 2) >> shifts_4th[2];
  ind[3] = in[4] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[4] >> shifts_5th[0];
  ind[1] = (in[4] >> 25 | in[5] << 7) >> shifts_5th[1];
  ind[2] = in[5] >> shifts_5th[2];
  ind[3] = in[5] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[5] >> shifts_6th[0];
  ind[1] = (in[5] >> 29 | in[6] << 3) >> shifts_6th[1];
  ind[2] = in[6] >> shifts_6th[2];
  ind[3] = in[6] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[6] >> 24 | in[7] << 8) >> shifts_7th[0];
  ind[1] = in[7] >> shifts_7th[1];
  ind[2] = in[7] >> shifts_7th[2];
  ind[3] = in[7] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[7] >> 28 | in[8] << 4) >> shifts_8th[0];
  ind[1] = in[8] >> shifts_8th[1];
  ind[2] = in[8] >> shifts_8th[2];
  ind[3] = in[8] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 9;

  return in;
}

inline static const uint32_t* unpack10_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 10, 20, 0};
  uint32_t shifts_2nd[4] = {8, 18, 0, 6};
  uint32_t shifts_3rd[4] = {16, 0, 4, 14};
  uint32_t shifts_4th[4] = {0, 2, 12, 22};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[0] >> shifts_1st[2];
  ind[3] = (in[0] >> 30 | in[1] << 2) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = in[1] >> shifts_2nd[1];
  ind[2] = (in[1] >> 28 | in[2] << 4) >> shifts_2nd[2];
  ind[3] = in[2] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[2] >> shifts_3rd[0];
  ind[1] = (in[2] >> 26 | in[3] << 6) >> shifts_3rd[1];
  ind[2] = in[3] >> shifts_3rd[2];
  ind[3] = in[3] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[3] >> 24 | in[4] << 8) >> shifts_4th[0];
  ind[1] = in[4] >> shifts_4th[1];
  ind[2] = in[4] >> shifts_4th[2];
  ind[3] = in[4] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[5] >> shifts_1st[0];
  ind[1] = in[5] >> shifts_1st[1];
  ind[2] = in[5] >> shifts_1st[2];
  ind[3] = (in[5] >> 30 | in[6] << 2) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[6] >> shifts_2nd[0];
  ind[1] = in[6] >> shifts_2nd[1];
  ind[2] = (in[6] >> 28 | in[7] << 4) >> shifts_2nd[2];
  ind[3] = in[7] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[7] >> shifts_3rd[0];
  ind[1] = (in[7] >> 26 | in[8] << 6) >> shifts_3rd[1];
  ind[2] = in[8] >> shifts_3rd[2];
  ind[3] = in[8] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[8] >> 24 | in[9] << 8) >> shifts_4th[0];
  ind[1] = in[9] >> shifts_4th[1];
  ind[2] = in[9] >> shifts_4th[2];
  ind[3] = in[9] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 10;

  return in;
}

inline static const uint32_t* unpack11_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 11, 0, 1};
  uint32_t shifts_2nd[4] = {12, 0, 2, 13};
  uint32_t shifts_3rd[4] = {0, 3, 14, 0};
  uint32_t shifts_4th[4] = {4, 15, 0, 5};
  uint32_t shifts_5th[4] = {16, 0, 6, 17};
  uint32_t shifts_6th[4] = {0, 7, 18, 0};
  uint32_t shifts_7th[4] = {8, 19, 0, 9};
  uint32_t shifts_8th[4] = {20, 0, 10, 21};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = (in[0] >> 22 | in[1] << 10) >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = (in[1] >> 23 | in[2] << 9) >> shifts_2nd[1];
  ind[2] = in[2] >> shifts_2nd[2];
  ind[3] = in[2] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[2] >> 24 | in[3] << 8) >> shifts_3rd[0];
  ind[1] = in[3] >> shifts_3rd[1];
  ind[2] = in[3] >> shifts_3rd[2];
  ind[3] = (in[3] >> 25 | in[4] << 7) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[4] >> shifts_4th[0];
  ind[1] = in[4] >> shifts_4th[1];
  ind[2] = (in[4] >> 26 | in[5] << 6) >> shifts_4th[2];
  ind[3] = in[5] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[5] >> shifts_5th[0];
  ind[1] = (in[5] >> 27 | in[6] << 5) >> shifts_5th[1];
  ind[2] = in[6] >> shifts_5th[2];
  ind[3] = in[6] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[6] >> 28 | in[7] << 4) >> shifts_6th[0];
  ind[1] = in[7] >> shifts_6th[1];
  ind[2] = in[7] >> shifts_6th[2];
  ind[3] = (in[7] >> 29 | in[8] << 3) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[8] >> shifts_7th[0];
  ind[1] = in[8] >> shifts_7th[1];
  ind[2] = (in[8] >> 30 | in[9] << 2) >> shifts_7th[2];
  ind[3] = in[9] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[9] >> shifts_8th[0];
  ind[1] = (in[9] >> 31 | in[10] << 1) >> shifts_8th[1];
  ind[2] = in[10] >> shifts_8th[2];
  ind[3] = in[10] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 11;

  return in;
}

inline static const uint32_t* unpack12_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 12, 0, 4};
  uint32_t shifts_2nd[4] = {16, 0, 8, 20};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = (in[0] >> 24 | in[1] << 8) >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[1] >> shifts_2nd[0];
  ind[1] = (in[1] >> 28 | in[2] << 4) >> shifts_2nd[1];
  ind[2] = in[2] >> shifts_2nd[2];
  ind[3] = in[2] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[3] >> shifts_1st[0];
  ind[1] = in[3] >> shifts_1st[1];
  ind[2] = (in[3] >> 24 | in[4] << 8) >> shifts_1st[2];
  ind[3] = in[4] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[4] >> shifts_2nd[0];
  ind[1] = (in[4] >> 28 | in[5] << 4) >> shifts_2nd[1];
  ind[2] = in[5] >> shifts_2nd[2];
  ind[3] = in[5] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[6] >> shifts_1st[0];
  ind[1] = in[6] >> shifts_1st[1];
  ind[2] = (in[6] >> 24 | in[7] << 8) >> shifts_1st[2];
  ind[3] = in[7] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[7] >> shifts_2nd[0];
  ind[1] = (in[7] >> 28 | in[8] << 4) >> shifts_2nd[1];
  ind[2] = in[8] >> shifts_2nd[2];
  ind[3] = in[8] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[9] >> shifts_1st[0];
  ind[1] = in[9] >> shifts_1st[1];
  ind[2] = (in[9] >> 24 | in[10] << 8) >> shifts_1st[2];
  ind[3] = in[10] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[10] >> shifts_2nd[0];
  ind[1] = (in[10] >> 28 | in[11] << 4) >> shifts_2nd[1];
  ind[2] = in[11] >> shifts_2nd[2];
  ind[3] = in[11] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 12;

  return in;
}

inline static const uint32_t* unpack13_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 13, 0, 7};
  uint32_t shifts_2nd[4] = {0, 1, 14, 0};
  uint32_t shifts_3rd[4] = {8, 0, 2, 15};
  uint32_t shifts_4th[4] = {0, 9, 0, 3};
  uint32_t shifts_5th[4] = {16, 0, 10, 0};
  uint32_t shifts_6th[4] = {4, 17, 0, 11};
  uint32_t shifts_7th[4] = {0, 5, 18, 0};
  uint32_t shifts_8th[4] = {12, 0, 6, 19};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = (in[0] >> 26 | in[1] << 6) >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[1] >> 20 | in[2] << 12) >> shifts_2nd[0];
  ind[1] = in[2] >> shifts_2nd[1];
  ind[2] = in[2] >> shifts_2nd[2];
  ind[3] = (in[2] >> 27 | in[3] << 5) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[3] >> shifts_3rd[0];
  ind[1] = (in[3] >> 21 | in[4] << 11) >> shifts_3rd[1];
  ind[2] = in[4] >> shifts_3rd[2];
  ind[3] = in[4] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[4] >> 28 | in[5] << 4) >> shifts_4th[0];
  ind[1] = in[5] >> shifts_4th[1];
  ind[2] = (in[5] >> 22 | in[6] << 10) >> shifts_4th[2];
  ind[3] = in[6] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[6] >> shifts_5th[0];
  ind[1] = (in[6] >> 29 | in[7] << 3) >> shifts_5th[1];
  ind[2] = in[7] >> shifts_5th[2];
  ind[3] = (in[7] >> 23 | in[8] << 9) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[8] >> shifts_6th[0];
  ind[1] = in[8] >> shifts_6th[1];
  ind[2] = (in[8] >> 30 | in[9] << 2) >> shifts_6th[2];
  ind[3] = in[9] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[9] >> 24 | in[10] << 8) >> shifts_7th[0];
  ind[1] = in[10] >> shifts_7th[1];
  ind[2] = in[10] >> shifts_7th[2];
  ind[3] = (in[10] >> 31 | in[11] << 1) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[11] >> shifts_8th[0];
  ind[1] = (in[11] >> 25 | in[12] << 7) >> shifts_8th[1];
  ind[2] = in[12] >> shifts_8th[2];
  ind[3] = in[12] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 13;

  return in;
}

inline static const uint32_t* unpack14_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 14, 0, 10};
  uint32_t shifts_2nd[4] = {0, 6, 0, 2};
  uint32_t shifts_3rd[4] = {16, 0, 12, 0};
  uint32_t shifts_4th[4] = {8, 0, 4, 18};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = (in[0] >> 28 | in[1] << 4) >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[1] >> 24 | in[2] << 8) >> shifts_2nd[0];
  ind[1] = in[2] >> shifts_2nd[1];
  ind[2] = (in[2] >> 20 | in[3] << 12) >> shifts_2nd[2];
  ind[3] = in[3] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[3] >> shifts_3rd[0];
  ind[1] = (in[3] >> 30 | in[4] << 2) >> shifts_3rd[1];
  ind[2] = in[4] >> shifts_3rd[2];
  ind[3] = (in[4] >> 26 | in[5] << 6) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[5] >> shifts_4th[0];
  ind[1] = (in[5] >> 22 | in[6] << 10) >> shifts_4th[1];
  ind[2] = in[6] >> shifts_4th[2];
  ind[3] = in[6] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[7] >> shifts_1st[0];
  ind[1] = in[7] >> shifts_1st[1];
  ind[2] = (in[7] >> 28 | in[8] << 4) >> shifts_1st[2];
  ind[3] = in[8] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[8] >> 24 | in[9] << 8) >> shifts_2nd[0];
  ind[1] = in[9] >> shifts_2nd[1];
  ind[2] = (in[9] >> 20 | in[10] << 12) >> shifts_2nd[2];
  ind[3] = in[10] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[10] >> shifts_3rd[0];
  ind[1] = (in[10] >> 30 | in[11] << 2) >> shifts_3rd[1];
  ind[2] = in[11] >> shifts_3rd[2];
  ind[3] = (in[11] >> 26 | in[12] << 6) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[12] >> shifts_4th[0];
  ind[1] = (in[12] >> 22 | in[13] << 10) >> shifts_4th[1];
  ind[2] = in[13] >> shifts_4th[2];
  ind[3] = in[13] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 14;

  return in;
}

inline static const uint32_t* unpack15_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 15, 0, 13};
  uint32_t shifts_2nd[4] = {0, 11, 0, 9};
  uint32_t shifts_3rd[4] = {0, 7, 0, 5};
  uint32_t shifts_4th[4] = {0, 3, 0, 1};
  uint32_t shifts_5th[4] = {16, 0, 14, 0};
  uint32_t shifts_6th[4] = {12, 0, 10, 0};
  uint32_t shifts_7th[4] = {8, 0, 6, 0};
  uint32_t shifts_8th[4] = {4, 0, 2, 17};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = (in[0] >> 30 | in[1] << 2) >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[1] >> 28 | in[2] << 4) >> shifts_2nd[0];
  ind[1] = in[2] >> shifts_2nd[1];
  ind[2] = (in[2] >> 26 | in[3] << 6) >> shifts_2nd[2];
  ind[3] = in[3] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[3] >> 24 | in[4] << 8) >> shifts_3rd[0];
  ind[1] = in[4] >> shifts_3rd[1];
  ind[2] = (in[4] >> 22 | in[5] << 10) >> shifts_3rd[2];
  ind[3] = in[5] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[5] >> 20 | in[6] << 12) >> shifts_4th[0];
  ind[1] = in[6] >> shifts_4th[1];
  ind[2] = (in[6] >> 18 | in[7] << 14) >> shifts_4th[2];
  ind[3] = in[7] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[7] >> shifts_5th[0];
  ind[1] = (in[7] >> 31 | in[8] << 1) >> shifts_5th[1];
  ind[2] = in[8] >> shifts_5th[2];
  ind[3] = (in[8] >> 29 | in[9] << 3) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[9] >> shifts_6th[0];
  ind[1] = (in[9] >> 27 | in[10] << 5) >> shifts_6th[1];
  ind[2] = in[10] >> shifts_6th[2];
  ind[3] = (in[10] >> 25 | in[11] << 7) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[11] >> shifts_7th[0];
  ind[1] = (in[11] >> 23 | in[12] << 9) >> shifts_7th[1];
  ind[2] = in[12] >> shifts_7th[2];
  ind[3] = (in[12] >> 21 | in[13] << 11) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[13] >> shifts_8th[0];
  ind[1] = (in[13] >> 19 | in[14] << 13) >> shifts_8th[1];
  ind[2] = in[14] >> shifts_8th[2];
  ind[3] = in[14] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 15;

  return in;
}

inline static const uint32_t* unpack16_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 16, 0, 16};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = in[0] >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = in[1] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[2] >> shifts_1st[0];
  ind[1] = in[2] >> shifts_1st[1];
  ind[2] = in[3] >> shifts_1st[2];
  ind[3] = in[3] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[4] >> shifts_1st[0];
  ind[1] = in[4] >> shifts_1st[1];
  ind[2] = in[5] >> shifts_1st[2];
  ind[3] = in[5] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[6] >> shifts_1st[0];
  ind[1] = in[6] >> shifts_1st[1];
  ind[2] = in[7] >> shifts_1st[2];
  ind[3] = in[7] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[8] >> shifts_1st[0];
  ind[1] = in[8] >> shifts_1st[1];
  ind[2] = in[9] >> shifts_1st[2];
  ind[3] = in[9] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[10] >> shifts_1st[0];
  ind[1] = in[10] >> shifts_1st[1];
  ind[2] = in[11] >> shifts_1st[2];
  ind[3] = in[11] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[12] >> shifts_1st[0];
  ind[1] = in[12] >> shifts_1st[1];
  ind[2] = in[13] >> shifts_1st[2];
  ind[3] = in[13] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[14] >> shifts_1st[0];
  ind[1] = in[14] >> shifts_1st[1];
  ind[2] = in[15] >> shifts_1st[2];
  ind[3] = in[15] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 16;

  return in;
}

inline static const uint32_t* unpack17_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 2, 0};
  uint32_t shifts_2nd[4] = {4, 0, 6, 0};
  uint32_t shifts_3rd[4] = {8, 0, 10, 0};
  uint32_t shifts_4th[4] = {12, 0, 14, 0};
  uint32_t shifts_5th[4] = {0, 1, 0, 3};
  uint32_t shifts_6th[4] = {0, 5, 0, 7};
  uint32_t shifts_7th[4] = {0, 9, 0, 11};
  uint32_t shifts_8th[4] = {0, 13, 0, 15};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 17 | in[1] << 15) >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = (in[1] >> 19 | in[2] << 13) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[2] >> shifts_2nd[0];
  ind[1] = (in[2] >> 21 | in[3] << 11) >> shifts_2nd[1];
  ind[2] = in[3] >> shifts_2nd[2];
  ind[3] = (in[3] >> 23 | in[4] << 9) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[4] >> shifts_3rd[0];
  ind[1] = (in[4] >> 25 | in[5] << 7) >> shifts_3rd[1];
  ind[2] = in[5] >> shifts_3rd[2];
  ind[3] = (in[5] >> 27 | in[6] << 5) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[6] >> shifts_4th[0];
  ind[1] = (in[6] >> 29 | in[7] << 3) >> shifts_4th[1];
  ind[2] = in[7] >> shifts_4th[2];
  ind[3] = (in[7] >> 31 | in[8] << 1) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[8] >> 16 | in[9] << 16) >> shifts_5th[0];
  ind[1] = in[9] >> shifts_5th[1];
  ind[2] = (in[9] >> 18 | in[10] << 14) >> shifts_5th[2];
  ind[3] = in[10] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[10] >> 20 | in[11] << 12) >> shifts_6th[0];
  ind[1] = in[11] >> shifts_6th[1];
  ind[2] = (in[11] >> 22 | in[12] << 10) >> shifts_6th[2];
  ind[3] = in[12] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[12] >> 24 | in[13] << 8) >> shifts_7th[0];
  ind[1] = in[13] >> shifts_7th[1];
  ind[2] = (in[13] >> 26 | in[14] << 6) >> shifts_7th[2];
  ind[3] = in[14] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[14] >> 28 | in[15] << 4) >> shifts_8th[0];
  ind[1] = in[15] >> shifts_8th[1];
  ind[2] = (in[15] >> 30 | in[16] << 2) >> shifts_8th[2];
  ind[3] = in[16] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 17;

  return in;
}

inline static const uint32_t* unpack18_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 4, 0};
  uint32_t shifts_2nd[4] = {8, 0, 12, 0};
  uint32_t shifts_3rd[4] = {0, 2, 0, 6};
  uint32_t shifts_4th[4] = {0, 10, 0, 14};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 18 | in[1] << 14) >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = (in[1] >> 22 | in[2] << 10) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[2] >> shifts_2nd[0];
  ind[1] = (in[2] >> 26 | in[3] << 6) >> shifts_2nd[1];
  ind[2] = in[3] >> shifts_2nd[2];
  ind[3] = (in[3] >> 30 | in[4] << 2) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[4] >> 16 | in[5] << 16) >> shifts_3rd[0];
  ind[1] = in[5] >> shifts_3rd[1];
  ind[2] = (in[5] >> 20 | in[6] << 12) >> shifts_3rd[2];
  ind[3] = in[6] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[6] >> 24 | in[7] << 8) >> shifts_4th[0];
  ind[1] = in[7] >> shifts_4th[1];
  ind[2] = (in[7] >> 28 | in[8] << 4) >> shifts_4th[2];
  ind[3] = in[8] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[9] >> shifts_1st[0];
  ind[1] = (in[9] >> 18 | in[10] << 14) >> shifts_1st[1];
  ind[2] = in[10] >> shifts_1st[2];
  ind[3] = (in[10] >> 22 | in[11] << 10) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[11] >> shifts_2nd[0];
  ind[1] = (in[11] >> 26 | in[12] << 6) >> shifts_2nd[1];
  ind[2] = in[12] >> shifts_2nd[2];
  ind[3] = (in[12] >> 30 | in[13] << 2) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[13] >> 16 | in[14] << 16) >> shifts_3rd[0];
  ind[1] = in[14] >> shifts_3rd[1];
  ind[2] = (in[14] >> 20 | in[15] << 12) >> shifts_3rd[2];
  ind[3] = in[15] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[15] >> 24 | in[16] << 8) >> shifts_4th[0];
  ind[1] = in[16] >> shifts_4th[1];
  ind[2] = (in[16] >> 28 | in[17] << 4) >> shifts_4th[2];
  ind[3] = in[17] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 18;

  return in;
}

inline static const uint32_t* unpack19_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 6, 0};
  uint32_t shifts_2nd[4] = {12, 0, 0, 5};
  uint32_t shifts_3rd[4] = {0, 11, 0, 0};
  uint32_t shifts_4th[4] = {4, 0, 10, 0};
  uint32_t shifts_5th[4] = {0, 3, 0, 9};
  uint32_t shifts_6th[4] = {0, 0, 2, 0};
  uint32_t shifts_7th[4] = {8, 0, 0, 1};
  uint32_t shifts_8th[4] = {0, 7, 0, 13};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 19 | in[1] << 13) >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = (in[1] >> 25 | in[2] << 7) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[2] >> shifts_2nd[0];
  ind[1] = (in[2] >> 31 | in[3] << 1) >> shifts_2nd[1];
  ind[2] = (in[3] >> 18 | in[4] << 14) >> shifts_2nd[2];
  ind[3] = in[4] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[4] >> 24 | in[5] << 8) >> shifts_3rd[0];
  ind[1] = in[5] >> shifts_3rd[1];
  ind[2] = (in[5] >> 30 | in[6] << 2) >> shifts_3rd[2];
  ind[3] = (in[6] >> 17 | in[7] << 15) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[7] >> shifts_4th[0];
  ind[1] = (in[7] >> 23 | in[8] << 9) >> shifts_4th[1];
  ind[2] = in[8] >> shifts_4th[2];
  ind[3] = (in[8] >> 29 | in[9] << 3) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[9] >> 16 | in[10] << 16) >> shifts_5th[0];
  ind[1] = in[10] >> shifts_5th[1];
  ind[2] = (in[10] >> 22 | in[11] << 10) >> shifts_5th[2];
  ind[3] = in[11] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[11] >> 28 | in[12] << 4) >> shifts_6th[0];
  ind[1] = (in[12] >> 15 | in[13] << 17) >> shifts_6th[1];
  ind[2] = in[13] >> shifts_6th[2];
  ind[3] = (in[13] >> 21 | in[14] << 11) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[14] >> shifts_7th[0];
  ind[1] = (in[14] >> 27 | in[15] << 5) >> shifts_7th[1];
  ind[2] = (in[15] >> 14 | in[16] << 18) >> shifts_7th[2];
  ind[3] = in[16] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[16] >> 20 | in[17] << 12) >> shifts_8th[0];
  ind[1] = in[17] >> shifts_8th[1];
  ind[2] = (in[17] >> 26 | in[18] << 6) >> shifts_8th[2];
  ind[3] = in[18] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 19;

  return in;
}

inline static const uint32_t* unpack20_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 8, 0};
  uint32_t shifts_2nd[4] = {0, 4, 0, 12};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 20 | in[1] << 12) >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = (in[1] >> 28 | in[2] << 4) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[2] >> 16 | in[3] << 16) >> shifts_2nd[0];
  ind[1] = in[3] >> shifts_2nd[1];
  ind[2] = (in[3] >> 24 | in[4] << 8) >> shifts_2nd[2];
  ind[3] = in[4] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[5] >> shifts_1st[0];
  ind[1] = (in[5] >> 20 | in[6] << 12) >> shifts_1st[1];
  ind[2] = in[6] >> shifts_1st[2];
  ind[3] = (in[6] >> 28 | in[7] << 4) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[7] >> 16 | in[8] << 16) >> shifts_2nd[0];
  ind[1] = in[8] >> shifts_2nd[1];
  ind[2] = (in[8] >> 24 | in[9] << 8) >> shifts_2nd[2];
  ind[3] = in[9] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[10] >> shifts_1st[0];
  ind[1] = (in[10] >> 20 | in[11] << 12) >> shifts_1st[1];
  ind[2] = in[11] >> shifts_1st[2];
  ind[3] = (in[11] >> 28 | in[12] << 4) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[12] >> 16 | in[13] << 16) >> shifts_2nd[0];
  ind[1] = in[13] >> shifts_2nd[1];
  ind[2] = (in[13] >> 24 | in[14] << 8) >> shifts_2nd[2];
  ind[3] = in[14] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[15] >> shifts_1st[0];
  ind[1] = (in[15] >> 20 | in[16] << 12) >> shifts_1st[1];
  ind[2] = in[16] >> shifts_1st[2];
  ind[3] = (in[16] >> 28 | in[17] << 4) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[17] >> 16 | in[18] << 16) >> shifts_2nd[0];
  ind[1] = in[18] >> shifts_2nd[1];
  ind[2] = (in[18] >> 24 | in[19] << 8) >> shifts_2nd[2];
  ind[3] = in[19] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 20;

  return in;
}

inline static const uint32_t* unpack21_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 10, 0};
  uint32_t shifts_2nd[4] = {0, 9, 0, 0};
  uint32_t shifts_3rd[4] = {8, 0, 0, 7};
  uint32_t shifts_4th[4] = {0, 0, 6, 0};
  uint32_t shifts_5th[4] = {0, 5, 0, 0};
  uint32_t shifts_6th[4] = {4, 0, 0, 3};
  uint32_t shifts_7th[4] = {0, 0, 2, 0};
  uint32_t shifts_8th[4] = {0, 1, 0, 11};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 21 | in[1] << 11) >> shifts_1st[1];
  ind[2] = in[1] >> shifts_1st[2];
  ind[3] = (in[1] >> 31 | in[2] << 1) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[2] >> 20 | in[3] << 12) >> shifts_2nd[0];
  ind[1] = in[3] >> shifts_2nd[1];
  ind[2] = (in[3] >> 30 | in[4] << 2) >> shifts_2nd[2];
  ind[3] = (in[4] >> 19 | in[5] << 13) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[5] >> shifts_3rd[0];
  ind[1] = (in[5] >> 29 | in[6] << 3) >> shifts_3rd[1];
  ind[2] = (in[6] >> 18 | in[7] << 14) >> shifts_3rd[2];
  ind[3] = in[7] >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[7] >> 28 | in[8] << 4) >> shifts_4th[0];
  ind[1] = (in[8] >> 17 | in[9] << 15) >> shifts_4th[1];
  ind[2] = in[9] >> shifts_4th[2];
  ind[3] = (in[9] >> 27 | in[10] << 5) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[10] >> 16 | in[11] << 16) >> shifts_5th[0];
  ind[1] = in[11] >> shifts_5th[1];
  ind[2] = (in[11] >> 26 | in[12] << 6) >> shifts_5th[2];
  ind[3] = (in[12] >> 15 | in[13] << 17) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[13] >> shifts_6th[0];
  ind[1] = (in[13] >> 25 | in[14] << 7) >> shifts_6th[1];
  ind[2] = (in[14] >> 14 | in[15] << 18) >> shifts_6th[2];
  ind[3] = in[15] >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[15] >> 24 | in[16] << 8) >> shifts_7th[0];
  ind[1] = (in[16] >> 13 | in[17] << 19) >> shifts_7th[1];
  ind[2] = in[17] >> shifts_7th[2];
  ind[3] = (in[17] >> 23 | in[18] << 9) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[18] >> 12 | in[19] << 20) >> shifts_8th[0];
  ind[1] = in[19] >> shifts_8th[1];
  ind[2] = (in[19] >> 22 | in[20] << 10) >> shifts_8th[2];
  ind[3] = in[20] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 21;

  return in;
}

inline static const uint32_t* unpack22_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 2};
  uint32_t shifts_2nd[4] = {0, 0, 4, 0};
  uint32_t shifts_3rd[4] = {0, 6, 0, 0};
  uint32_t shifts_4th[4] = {8, 0, 0, 10};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 22 | in[1] << 10) >> shifts_1st[1];
  ind[2] = (in[1] >> 12 | in[2] << 20) >> shifts_1st[2];
  ind[3] = in[2] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[2] >> 24 | in[3] << 8) >> shifts_2nd[0];
  ind[1] = (in[3] >> 14 | in[4] << 18) >> shifts_2nd[1];
  ind[2] = in[4] >> shifts_2nd[2];
  ind[3] = (in[4] >> 26 | in[5] << 6) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[5] >> 16 | in[6] << 16) >> shifts_3rd[0];
  ind[1] = in[6] >> shifts_3rd[1];
  ind[2] = (in[6] >> 28 | in[7] << 4) >> shifts_3rd[2];
  ind[3] = (in[7] >> 18 | in[8] << 14) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[8] >> shifts_4th[0];
  ind[1] = (in[8] >> 30 | in[9] << 2) >> shifts_4th[1];
  ind[2] = (in[9] >> 20 | in[10] << 12) >> shifts_4th[2];
  ind[3] = in[10] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[11] >> shifts_1st[0];
  ind[1] = (in[11] >> 22 | in[12] << 10) >> shifts_1st[1];
  ind[2] = (in[12] >> 12 | in[13] << 20) >> shifts_1st[2];
  ind[3] = in[13] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[13] >> 24 | in[14] << 8) >> shifts_2nd[0];
  ind[1] = (in[14] >> 14 | in[15] << 18) >> shifts_2nd[1];
  ind[2] = in[15] >> shifts_2nd[2];
  ind[3] = (in[15] >> 26 | in[16] << 6) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[16] >> 16 | in[17] << 16) >> shifts_3rd[0];
  ind[1] = in[17] >> shifts_3rd[1];
  ind[2] = (in[17] >> 28 | in[18] << 4) >> shifts_3rd[2];
  ind[3] = (in[18] >> 18 | in[19] << 14) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[19] >> shifts_4th[0];
  ind[1] = (in[19] >> 30 | in[20] << 2) >> shifts_4th[1];
  ind[2] = (in[20] >> 20 | in[21] << 12) >> shifts_4th[2];
  ind[3] = in[21] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 22;

  return in;
}

inline static const uint32_t* unpack23_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 5};
  uint32_t shifts_2nd[4] = {0, 0, 0, 1};
  uint32_t shifts_3rd[4] = {0, 0, 6, 0};
  uint32_t shifts_4th[4] = {0, 0, 2, 0};
  uint32_t shifts_5th[4] = {0, 7, 0, 0};
  uint32_t shifts_6th[4] = {0, 3, 0, 0};
  uint32_t shifts_7th[4] = {8, 0, 0, 0};
  uint32_t shifts_8th[4] = {4, 0, 0, 9};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 23 | in[1] << 9) >> shifts_1st[1];
  ind[2] = (in[1] >> 14 | in[2] << 18) >> shifts_1st[2];
  ind[3] = in[2] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[2] >> 28 | in[3] << 4) >> shifts_2nd[0];
  ind[1] = (in[3] >> 19 | in[4] << 13) >> shifts_2nd[1];
  ind[2] = (in[4] >> 10 | in[5] << 22) >> shifts_2nd[2];
  ind[3] = in[5] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[5] >> 24 | in[6] << 8) >> shifts_3rd[0];
  ind[1] = (in[6] >> 15 | in[7] << 17) >> shifts_3rd[1];
  ind[2] = in[7] >> shifts_3rd[2];
  ind[3] = (in[7] >> 29 | in[8] << 3) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[8] >> 20 | in[9] << 12) >> shifts_4th[0];
  ind[1] = (in[9] >> 11 | in[10] << 21) >> shifts_4th[1];
  ind[2] = in[10] >> shifts_4th[2];
  ind[3] = (in[10] >> 25 | in[11] << 7) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[11] >> 16 | in[12] << 16) >> shifts_5th[0];
  ind[1] = in[12] >> shifts_5th[1];
  ind[2] = (in[12] >> 30 | in[13] << 2) >> shifts_5th[2];
  ind[3] = (in[13] >> 21 | in[14] << 11) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[14] >> 12 | in[15] << 20) >> shifts_6th[0];
  ind[1] = in[15] >> shifts_6th[1];
  ind[2] = (in[15] >> 26 | in[16] << 6) >> shifts_6th[2];
  ind[3] = (in[16] >> 17 | in[17] << 15) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[17] >> shifts_7th[0];
  ind[1] = (in[17] >> 31 | in[18] << 1) >> shifts_7th[1];
  ind[2] = (in[18] >> 22 | in[19] << 10) >> shifts_7th[2];
  ind[3] = (in[19] >> 13 | in[20] << 19) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[20] >> shifts_8th[0];
  ind[1] = (in[20] >> 27 | in[21] << 5) >> shifts_8th[1];
  ind[2] = (in[21] >> 18 | in[22] << 14) >> shifts_8th[2];
  ind[3] = in[22] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 23;

  return in;
}

inline static const uint32_t* unpack24_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 8};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 24 | in[1] << 8) >> shifts_1st[1];
  ind[2] = (in[1] >> 16 | in[2] << 16) >> shifts_1st[2];
  ind[3] = in[2] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[3] >> shifts_1st[0];
  ind[1] = (in[3] >> 24 | in[4] << 8) >> shifts_1st[1];
  ind[2] = (in[4] >> 16 | in[5] << 16) >> shifts_1st[2];
  ind[3] = in[5] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[6] >> shifts_1st[0];
  ind[1] = (in[6] >> 24 | in[7] << 8) >> shifts_1st[1];
  ind[2] = (in[7] >> 16 | in[8] << 16) >> shifts_1st[2];
  ind[3] = in[8] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[9] >> shifts_1st[0];
  ind[1] = (in[9] >> 24 | in[10] << 8) >> shifts_1st[1];
  ind[2] = (in[10] >> 16 | in[11] << 16) >> shifts_1st[2];
  ind[3] = in[11] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[12] >> shifts_1st[0];
  ind[1] = (in[12] >> 24 | in[13] << 8) >> shifts_1st[1];
  ind[2] = (in[13] >> 16 | in[14] << 16) >> shifts_1st[2];
  ind[3] = in[14] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = in[15] >> shifts_1st[0];
  ind[1] = (in[15] >> 24 | in[16] << 8) >> shifts_1st[1];
  ind[2] = (in[16] >> 16 | in[17] << 16) >> shifts_1st[2];
  ind[3] = in[17] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[18] >> shifts_1st[0];
  ind[1] = (in[18] >> 24 | in[19] << 8) >> shifts_1st[1];
  ind[2] = (in[19] >> 16 | in[20] << 16) >> shifts_1st[2];
  ind[3] = in[20] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = in[21] >> shifts_1st[0];
  ind[1] = (in[21] >> 24 | in[22] << 8) >> shifts_1st[1];
  ind[2] = (in[22] >> 16 | in[23] << 16) >> shifts_1st[2];
  ind[3] = in[23] >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 24;

  return in;
}

inline static const uint32_t* unpack25_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1ffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {4, 0, 0, 0};
  uint32_t shifts_3rd[4] = {0, 1, 0, 0};
  uint32_t shifts_4th[4] = {0, 5, 0, 0};
  uint32_t shifts_5th[4] = {0, 0, 2, 0};
  uint32_t shifts_6th[4] = {0, 0, 6, 0};
  uint32_t shifts_7th[4] = {0, 0, 0, 3};
  uint32_t shifts_8th[4] = {0, 0, 0, 7};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 25 | in[1] << 7) >> shifts_1st[1];
  ind[2] = (in[1] >> 18 | in[2] << 14) >> shifts_1st[2];
  ind[3] = (in[2] >> 11 | in[3] << 21) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = in[3] >> shifts_2nd[0];
  ind[1] = (in[3] >> 29 | in[4] << 3) >> shifts_2nd[1];
  ind[2] = (in[4] >> 22 | in[5] << 10) >> shifts_2nd[2];
  ind[3] = (in[5] >> 15 | in[6] << 17) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[6] >> 8 | in[7] << 24) >> shifts_3rd[0];
  ind[1] = in[7] >> shifts_3rd[1];
  ind[2] = (in[7] >> 26 | in[8] << 6) >> shifts_3rd[2];
  ind[3] = (in[8] >> 19 | in[9] << 13) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[9] >> 12 | in[10] << 20) >> shifts_4th[0];
  ind[1] = in[10] >> shifts_4th[1];
  ind[2] = (in[10] >> 30 | in[11] << 2) >> shifts_4th[2];
  ind[3] = (in[11] >> 23 | in[12] << 9) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[12] >> 16 | in[13] << 16) >> shifts_5th[0];
  ind[1] = (in[13] >> 9 | in[14] << 23) >> shifts_5th[1];
  ind[2] = in[14] >> shifts_5th[2];
  ind[3] = (in[14] >> 27 | in[15] << 5) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[15] >> 20 | in[16] << 12) >> shifts_6th[0];
  ind[1] = (in[16] >> 13 | in[17] << 19) >> shifts_6th[1];
  ind[2] = in[17] >> shifts_6th[2];
  ind[3] = (in[17] >> 31 | in[18] << 1) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[18] >> 24 | in[19] << 8) >> shifts_7th[0];
  ind[1] = (in[19] >> 17 | in[20] << 15) >> shifts_7th[1];
  ind[2] = (in[20] >> 10 | in[21] << 22) >> shifts_7th[2];
  ind[3] = in[21] >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[21] >> 28 | in[22] << 4) >> shifts_8th[0];
  ind[1] = (in[22] >> 21 | in[23] << 11) >> shifts_8th[1];
  ind[2] = (in[23] >> 14 | in[24] << 18) >> shifts_8th[2];
  ind[3] = in[24] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 25;

  return in;
}

inline static const uint32_t* unpack26_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3ffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 2, 0, 0};
  uint32_t shifts_3rd[4] = {0, 0, 4, 0};
  uint32_t shifts_4th[4] = {0, 0, 0, 6};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 26 | in[1] << 6) >> shifts_1st[1];
  ind[2] = (in[1] >> 20 | in[2] << 12) >> shifts_1st[2];
  ind[3] = (in[2] >> 14 | in[3] << 18) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 8 | in[4] << 24) >> shifts_2nd[0];
  ind[1] = in[4] >> shifts_2nd[1];
  ind[2] = (in[4] >> 28 | in[5] << 4) >> shifts_2nd[2];
  ind[3] = (in[5] >> 22 | in[6] << 10) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[6] >> 16 | in[7] << 16) >> shifts_3rd[0];
  ind[1] = (in[7] >> 10 | in[8] << 22) >> shifts_3rd[1];
  ind[2] = in[8] >> shifts_3rd[2];
  ind[3] = (in[8] >> 30 | in[9] << 2) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[9] >> 24 | in[10] << 8) >> shifts_4th[0];
  ind[1] = (in[10] >> 18 | in[11] << 14) >> shifts_4th[1];
  ind[2] = (in[11] >> 12 | in[12] << 20) >> shifts_4th[2];
  ind[3] = in[12] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[13] >> shifts_1st[0];
  ind[1] = (in[13] >> 26 | in[14] << 6) >> shifts_1st[1];
  ind[2] = (in[14] >> 20 | in[15] << 12) >> shifts_1st[2];
  ind[3] = (in[15] >> 14 | in[16] << 18) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[16] >> 8 | in[17] << 24) >> shifts_2nd[0];
  ind[1] = in[17] >> shifts_2nd[1];
  ind[2] = (in[17] >> 28 | in[18] << 4) >> shifts_2nd[2];
  ind[3] = (in[18] >> 22 | in[19] << 10) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[19] >> 16 | in[20] << 16) >> shifts_3rd[0];
  ind[1] = (in[20] >> 10 | in[21] << 22) >> shifts_3rd[1];
  ind[2] = in[21] >> shifts_3rd[2];
  ind[3] = (in[21] >> 30 | in[22] << 2) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[22] >> 24 | in[23] << 8) >> shifts_4th[0];
  ind[1] = (in[23] >> 18 | in[24] << 14) >> shifts_4th[1];
  ind[2] = (in[24] >> 12 | in[25] << 20) >> shifts_4th[2];
  ind[3] = in[25] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 26;

  return in;
}

inline static const uint32_t* unpack27_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7ffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 0, 2, 0};
  uint32_t shifts_3rd[4] = {0, 0, 0, 0};
  uint32_t shifts_4th[4] = {4, 0, 0, 0};
  uint32_t shifts_5th[4] = {0, 0, 0, 1};
  uint32_t shifts_6th[4] = {0, 0, 0, 0};
  uint32_t shifts_7th[4] = {0, 3, 0, 0};
  uint32_t shifts_8th[4] = {0, 0, 0, 5};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 27 | in[1] << 5) >> shifts_1st[1];
  ind[2] = (in[1] >> 22 | in[2] << 10) >> shifts_1st[2];
  ind[3] = (in[2] >> 17 | in[3] << 15) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 12 | in[4] << 20) >> shifts_2nd[0];
  ind[1] = (in[4] >> 7 | in[5] << 25) >> shifts_2nd[1];
  ind[2] = in[5] >> shifts_2nd[2];
  ind[3] = (in[5] >> 29 | in[6] << 3) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[6] >> 24 | in[7] << 8) >> shifts_3rd[0];
  ind[1] = (in[7] >> 19 | in[8] << 13) >> shifts_3rd[1];
  ind[2] = (in[8] >> 14 | in[9] << 18) >> shifts_3rd[2];
  ind[3] = (in[9] >> 9 | in[10] << 23) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = in[10] >> shifts_4th[0];
  ind[1] = (in[10] >> 31 | in[11] << 1) >> shifts_4th[1];
  ind[2] = (in[11] >> 26 | in[12] << 6) >> shifts_4th[2];
  ind[3] = (in[12] >> 21 | in[13] << 11) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[13] >> 16 | in[14] << 16) >> shifts_5th[0];
  ind[1] = (in[14] >> 11 | in[15] << 21) >> shifts_5th[1];
  ind[2] = (in[15] >> 6 | in[16] << 26) >> shifts_5th[2];
  ind[3] = in[16] >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[16] >> 28 | in[17] << 4) >> shifts_6th[0];
  ind[1] = (in[17] >> 23 | in[18] << 9) >> shifts_6th[1];
  ind[2] = (in[18] >> 18 | in[19] << 14) >> shifts_6th[2];
  ind[3] = (in[19] >> 13 | in[20] << 19) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[20] >> 8 | in[21] << 24) >> shifts_7th[0];
  ind[1] = in[21] >> shifts_7th[1];
  ind[2] = (in[21] >> 30 | in[22] << 2) >> shifts_7th[2];
  ind[3] = (in[22] >> 25 | in[23] << 7) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[23] >> 20 | in[24] << 12) >> shifts_8th[0];
  ind[1] = (in[24] >> 15 | in[25] << 17) >> shifts_8th[1];
  ind[2] = (in[25] >> 10 | in[26] << 22) >> shifts_8th[2];
  ind[3] = in[26] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 27;

  return in;
}

inline static const uint32_t* unpack28_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0xfffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 0, 0, 4};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 28 | in[1] << 4) >> shifts_1st[1];
  ind[2] = (in[1] >> 24 | in[2] << 8) >> shifts_1st[2];
  ind[3] = (in[2] >> 20 | in[3] << 12) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 16 | in[4] << 16) >> shifts_2nd[0];
  ind[1] = (in[4] >> 12 | in[5] << 20) >> shifts_2nd[1];
  ind[2] = (in[5] >> 8 | in[6] << 24) >> shifts_2nd[2];
  ind[3] = in[6] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = in[7] >> shifts_1st[0];
  ind[1] = (in[7] >> 28 | in[8] << 4) >> shifts_1st[1];
  ind[2] = (in[8] >> 24 | in[9] << 8) >> shifts_1st[2];
  ind[3] = (in[9] >> 20 | in[10] << 12) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[10] >> 16 | in[11] << 16) >> shifts_2nd[0];
  ind[1] = (in[11] >> 12 | in[12] << 20) >> shifts_2nd[1];
  ind[2] = (in[12] >> 8 | in[13] << 24) >> shifts_2nd[2];
  ind[3] = in[13] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[14] >> shifts_1st[0];
  ind[1] = (in[14] >> 28 | in[15] << 4) >> shifts_1st[1];
  ind[2] = (in[15] >> 24 | in[16] << 8) >> shifts_1st[2];
  ind[3] = (in[16] >> 20 | in[17] << 12) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[17] >> 16 | in[18] << 16) >> shifts_2nd[0];
  ind[1] = (in[18] >> 12 | in[19] << 20) >> shifts_2nd[1];
  ind[2] = (in[19] >> 8 | in[20] << 24) >> shifts_2nd[2];
  ind[3] = in[20] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = in[21] >> shifts_1st[0];
  ind[1] = (in[21] >> 28 | in[22] << 4) >> shifts_1st[1];
  ind[2] = (in[22] >> 24 | in[23] << 8) >> shifts_1st[2];
  ind[3] = (in[23] >> 20 | in[24] << 12) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[24] >> 16 | in[25] << 16) >> shifts_2nd[0];
  ind[1] = (in[25] >> 12 | in[26] << 20) >> shifts_2nd[1];
  ind[2] = (in[26] >> 8 | in[27] << 24) >> shifts_2nd[2];
  ind[3] = in[27] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 28;

  return in;
}

inline static const uint32_t* unpack29_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x1fffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 0, 0, 0};
  uint32_t shifts_3rd[4] = {0, 0, 2, 0};
  uint32_t shifts_4th[4] = {0, 0, 0, 0};
  uint32_t shifts_5th[4] = {0, 0, 0, 0};
  uint32_t shifts_6th[4] = {0, 1, 0, 0};
  uint32_t shifts_7th[4] = {0, 0, 0, 0};
  uint32_t shifts_8th[4] = {0, 0, 0, 3};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 29 | in[1] << 3) >> shifts_1st[1];
  ind[2] = (in[1] >> 26 | in[2] << 6) >> shifts_1st[2];
  ind[3] = (in[2] >> 23 | in[3] << 9) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 20 | in[4] << 12) >> shifts_2nd[0];
  ind[1] = (in[4] >> 17 | in[5] << 15) >> shifts_2nd[1];
  ind[2] = (in[5] >> 14 | in[6] << 18) >> shifts_2nd[2];
  ind[3] = (in[6] >> 11 | in[7] << 21) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[7] >> 8 | in[8] << 24) >> shifts_3rd[0];
  ind[1] = (in[8] >> 5 | in[9] << 27) >> shifts_3rd[1];
  ind[2] = in[9] >> shifts_3rd[2];
  ind[3] = (in[9] >> 31 | in[10] << 1) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[10] >> 28 | in[11] << 4) >> shifts_4th[0];
  ind[1] = (in[11] >> 25 | in[12] << 7) >> shifts_4th[1];
  ind[2] = (in[12] >> 22 | in[13] << 10) >> shifts_4th[2];
  ind[3] = (in[13] >> 19 | in[14] << 13) >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[14] >> 16 | in[15] << 16) >> shifts_5th[0];
  ind[1] = (in[15] >> 13 | in[16] << 19) >> shifts_5th[1];
  ind[2] = (in[16] >> 10 | in[17] << 22) >> shifts_5th[2];
  ind[3] = (in[17] >> 7 | in[18] << 25) >> shifts_5th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[18] >> 4 | in[19] << 28) >> shifts_6th[0];
  ind[1] = in[19] >> shifts_6th[1];
  ind[2] = (in[19] >> 30 | in[20] << 2) >> shifts_6th[2];
  ind[3] = (in[20] >> 27 | in[21] << 5) >> shifts_6th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[21] >> 24 | in[22] << 8) >> shifts_7th[0];
  ind[1] = (in[22] >> 21 | in[23] << 11) >> shifts_7th[1];
  ind[2] = (in[23] >> 18 | in[24] << 14) >> shifts_7th[2];
  ind[3] = (in[24] >> 15 | in[25] << 17) >> shifts_7th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[25] >> 12 | in[26] << 20) >> shifts_8th[0];
  ind[1] = (in[26] >> 9 | in[27] << 23) >> shifts_8th[1];
  ind[2] = (in[27] >> 6 | in[28] << 26) >> shifts_8th[2];
  ind[3] = in[28] >> shifts_8th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 29;

  return in;
}

inline static const uint32_t* unpack30_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x3fffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 0, 0, 0};
  uint32_t shifts_3rd[4] = {0, 0, 0, 0};
  uint32_t shifts_4th[4] = {0, 0, 0, 2};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 30 | in[1] << 2) >> shifts_1st[1];
  ind[2] = (in[1] >> 28 | in[2] << 4) >> shifts_1st[2];
  ind[3] = (in[2] >> 26 | in[3] << 6) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 24 | in[4] << 8) >> shifts_2nd[0];
  ind[1] = (in[4] >> 22 | in[5] << 10) >> shifts_2nd[1];
  ind[2] = (in[5] >> 20 | in[6] << 12) >> shifts_2nd[2];
  ind[3] = (in[6] >> 18 | in[7] << 14) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[7] >> 16 | in[8] << 16) >> shifts_3rd[0];
  ind[1] = (in[8] >> 14 | in[9] << 18) >> shifts_3rd[1];
  ind[2] = (in[9] >> 12 | in[10] << 20) >> shifts_3rd[2];
  ind[3] = (in[10] >> 10 | in[11] << 22) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[11] >> 8 | in[12] << 24) >> shifts_4th[0];
  ind[1] = (in[12] >> 6 | in[13] << 26) >> shifts_4th[1];
  ind[2] = (in[13] >> 4 | in[14] << 28) >> shifts_4th[2];
  ind[3] = in[14] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = in[15] >> shifts_1st[0];
  ind[1] = (in[15] >> 30 | in[16] << 2) >> shifts_1st[1];
  ind[2] = (in[16] >> 28 | in[17] << 4) >> shifts_1st[2];
  ind[3] = (in[17] >> 26 | in[18] << 6) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[18] >> 24 | in[19] << 8) >> shifts_2nd[0];
  ind[1] = (in[19] >> 22 | in[20] << 10) >> shifts_2nd[1];
  ind[2] = (in[20] >> 20 | in[21] << 12) >> shifts_2nd[2];
  ind[3] = (in[21] >> 18 | in[22] << 14) >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[22] >> 16 | in[23] << 16) >> shifts_3rd[0];
  ind[1] = (in[23] >> 14 | in[24] << 18) >> shifts_3rd[1];
  ind[2] = (in[24] >> 12 | in[25] << 20) >> shifts_3rd[2];
  ind[3] = (in[25] >> 10 | in[26] << 22) >> shifts_3rd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[26] >> 8 | in[27] << 24) >> shifts_4th[0];
  ind[1] = (in[27] >> 6 | in[28] << 26) >> shifts_4th[1];
  ind[2] = (in[28] >> 4 | in[29] << 28) >> shifts_4th[2];
  ind[3] = in[29] >> shifts_4th[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 30;

  return in;
}

inline static const uint32_t* unpack31_32_neon(const uint32_t* in, uint32_t* out) {
  uint32_t mask = 0x7fffffff;
  uint32_t ind[4];
  uint32_t shifts_1st[4] = {0, 0, 0, 0};
  uint32_t shifts_2nd[4] = {0, 0, 0, 1};
  uint32x4_t reg_shft, reg_masks;
  uint32x4_t results;

  reg_masks = vdupq_n_u32(mask);

  // shift the first 4 outs
  ind[0] = in[0] >> shifts_1st[0];
  ind[1] = (in[0] >> 31 | in[1] << 1) >> shifts_1st[1];
  ind[2] = (in[1] >> 30 | in[2] << 2) >> shifts_1st[2];
  ind[3] = (in[2] >> 29 | in[3] << 3) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 2nd 4 outs
  ind[0] = (in[3] >> 28 | in[4] << 4) >> shifts_1st[0];
  ind[1] = (in[4] >> 27 | in[5] << 5) >> shifts_1st[1];
  ind[2] = (in[5] >> 26 | in[6] << 6) >> shifts_1st[2];
  ind[3] = (in[6] >> 25 | in[7] << 7) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 3rd 4 outs
  ind[0] = (in[7] >> 24 | in[8] << 8) >> shifts_1st[0];
  ind[1] = (in[8] >> 23 | in[9] << 9) >> shifts_1st[1];
  ind[2] = (in[9] >> 22 | in[10] << 10) >> shifts_1st[2];
  ind[3] = (in[10] >> 21 | in[11] << 11) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 4th 4 outs
  ind[0] = (in[11] >> 20 | in[12] << 12) >> shifts_1st[0];
  ind[1] = (in[12] >> 19 | in[13] << 13) >> shifts_1st[1];
  ind[2] = (in[13] >> 18 | in[14] << 14) >> shifts_1st[2];
  ind[3] = (in[14] >> 17 | in[15] << 15) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 5th 4 outs
  ind[0] = (in[15] >> 16 | in[16] << 16) >> shifts_1st[0];
  ind[1] = (in[16] >> 15 | in[17] << 17) >> shifts_1st[1];
  ind[2] = (in[17] >> 14 | in[18] << 18) >> shifts_1st[2];
  ind[3] = (in[18] >> 13 | in[19] << 19) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 6th 4 outs
  ind[0] = (in[19] >> 12 | in[20] << 20) >> shifts_1st[0];
  ind[1] = (in[20] >> 11 | in[21] << 21) >> shifts_1st[1];
  ind[2] = (in[21] >> 10 | in[22] << 22) >> shifts_1st[2];
  ind[3] = (in[22] >> 9 | in[23] << 23) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 7th 4 outs
  ind[0] = (in[23] >> 8 | in[24] << 24) >> shifts_1st[0];
  ind[1] = (in[24] >> 7 | in[25] << 25) >> shifts_1st[1];
  ind[2] = (in[25] >> 6 | in[26] << 26) >> shifts_1st[2];
  ind[3] = (in[26] >> 5 | in[27] << 27) >> shifts_1st[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  // shift the 8th 4 outs
  ind[0] = (in[27] >> 4 | in[28] << 28) >> shifts_2nd[0];
  ind[1] = (in[28] >> 3 | in[29] << 29) >> shifts_2nd[1];
  ind[2] = (in[29] >> 2 | in[30] << 30) >> shifts_2nd[2];
  ind[3] = in[30] >> shifts_2nd[3];
  reg_shft = vld1q_u32(ind);
  results = vandq_u32(reg_shft, reg_masks);
  vst1q_u32(out, results);
  out += 4;

  in += 31;

  return in;
}

inline const uint32_t* unpack32_32_neon(const uint32_t* in, uint32_t* out) {
  for (const uint32_t* end = out + 32; out != end; out++) {
    *out = *in;
    in++;
  }

  return in;
}

int unpack32_neon(const uint32_t* in, uint32_t* out, int batch_size, int num_bits) {
  batch_size = batch_size / 32 * 32;
  int num_loops = batch_size / 32;

  switch (num_bits) {
    case 0:
      for (int i = 0; i < num_loops; ++i) in = unpack0_32_neon(in, out + i * 32);
      break;
    case 1:
      for (int i = 0; i < num_loops; ++i) in = unpack1_32_neon(in, out + i * 32);
      break;
    case 2:
      for (int i = 0; i < num_loops; ++i) in = unpack2_32_neon(in, out + i * 32);
      break;
    case 3:
      for (int i = 0; i < num_loops; ++i) in = unpack3_32_neon(in, out + i * 32);
      break;
    case 4:
      for (int i = 0; i < num_loops; ++i) in = unpack4_32_neon(in, out + i * 32);
      break;
    case 5:
      for (int i = 0; i < num_loops; ++i) in = unpack5_32_neon(in, out + i * 32);
      break;
    case 6:
      for (int i = 0; i < num_loops; ++i) in = unpack6_32_neon(in, out + i * 32);
      break;
    case 7:
      for (int i = 0; i < num_loops; ++i) in = unpack7_32_neon(in, out + i * 32);
      break;
    case 8:
      for (int i = 0; i < num_loops; ++i) in = unpack8_32_neon(in, out + i * 32);
      break;
    case 9:
      for (int i = 0; i < num_loops; ++i) in = unpack9_32_neon(in, out + i * 32);
      break;
    case 10:
      for (int i = 0; i < num_loops; ++i) in = unpack10_32_neon(in, out + i * 32);
      break;
    case 11:
      for (int i = 0; i < num_loops; ++i) in = unpack11_32_neon(in, out + i * 32);
      break;
    case 12:
      for (int i = 0; i < num_loops; ++i) in = unpack12_32_neon(in, out + i * 32);
      break;
    case 13:
      for (int i = 0; i < num_loops; ++i) in = unpack13_32_neon(in, out + i * 32);
      break;
    case 14:
      for (int i = 0; i < num_loops; ++i) in = unpack14_32_neon(in, out + i * 32);
      break;
    case 15:
      for (int i = 0; i < num_loops; ++i) in = unpack15_32_neon(in, out + i * 32);
      break;
    case 16:
      for (int i = 0; i < num_loops; ++i) in = unpack16_32_neon(in, out + i * 32);
      break;
    case 17:
      for (int i = 0; i < num_loops; ++i) in = unpack17_32_neon(in, out + i * 32);
      break;
    case 18:
      for (int i = 0; i < num_loops; ++i) in = unpack18_32_neon(in, out + i * 32);
      break;
    case 19:
      for (int i = 0; i < num_loops; ++i) in = unpack19_32_neon(in, out + i * 32);
      break;
    case 20:
      for (int i = 0; i < num_loops; ++i) in = unpack20_32_neon(in, out + i * 32);
      break;
    case 21:
      for (int i = 0; i < num_loops; ++i) in = unpack21_32_neon(in, out + i * 32);
      break;
    case 22:
      for (int i = 0; i < num_loops; ++i) in = unpack22_32_neon(in, out + i * 32);
      break;
    case 23:
      for (int i = 0; i < num_loops; ++i) in = unpack23_32_neon(in, out + i * 32);
      break;
    case 24:
      for (int i = 0; i < num_loops; ++i) in = unpack24_32_neon(in, out + i * 32);
      break;
    case 25:
      for (int i = 0; i < num_loops; ++i) in = unpack25_32_neon(in, out + i * 32);
      break;
    case 26:
      for (int i = 0; i < num_loops; ++i) in = unpack26_32_neon(in, out + i * 32);
      break;
    case 27:
      for (int i = 0; i < num_loops; ++i) in = unpack27_32_neon(in, out + i * 32);
      break;
    case 28:
      for (int i = 0; i < num_loops; ++i) in = unpack28_32_neon(in, out + i * 32);
      break;
    case 29:
      for (int i = 0; i < num_loops; ++i) in = unpack29_32_neon(in, out + i * 32);
      break;
    case 30:
      for (int i = 0; i < num_loops; ++i) in = unpack30_32_neon(in, out + i * 32);
      break;
    case 31:
      for (int i = 0; i < num_loops; ++i) in = unpack31_32_neon(in, out + i * 32);
      break;
    case 32:
      for (int i = 0; i < num_loops; ++i) in = unpack32_32_neon(in, out + i * 32);
      break;
  }

  return batch_size;
}
