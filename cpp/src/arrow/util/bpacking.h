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

#include "arrow/util/logging.h"
#include "arrow/util/ubsan.h"
#if defined(ARROW_HAVE_AVX512)
#include "arrow/util/bpacking_avx512_generated.h"
#else
#include "arrow/util/bpacking_default.h"
#endif

namespace arrow {
namespace internal {

inline int unpack32(const uint32_t* in, uint32_t* out, int batch_size, int num_bits) {
  batch_size = batch_size / 32 * 32;
  int num_loops = batch_size / 32;

  switch (num_bits) {
    case 0:
      for (int i = 0; i < num_loops; ++i) in = nullunpacker32(in, out + i * 32);
      break;
    case 1:
      for (int i = 0; i < num_loops; ++i) in = unpack1_32(in, out + i * 32);
      break;
    case 2:
      for (int i = 0; i < num_loops; ++i) in = unpack2_32(in, out + i * 32);
      break;
    case 3:
      for (int i = 0; i < num_loops; ++i) in = unpack3_32(in, out + i * 32);
      break;
    case 4:
      for (int i = 0; i < num_loops; ++i) in = unpack4_32(in, out + i * 32);
      break;
    case 5:
      for (int i = 0; i < num_loops; ++i) in = unpack5_32(in, out + i * 32);
      break;
    case 6:
      for (int i = 0; i < num_loops; ++i) in = unpack6_32(in, out + i * 32);
      break;
    case 7:
      for (int i = 0; i < num_loops; ++i) in = unpack7_32(in, out + i * 32);
      break;
    case 8:
      for (int i = 0; i < num_loops; ++i) in = unpack8_32(in, out + i * 32);
      break;
    case 9:
      for (int i = 0; i < num_loops; ++i) in = unpack9_32(in, out + i * 32);
      break;
    case 10:
      for (int i = 0; i < num_loops; ++i) in = unpack10_32(in, out + i * 32);
      break;
    case 11:
      for (int i = 0; i < num_loops; ++i) in = unpack11_32(in, out + i * 32);
      break;
    case 12:
      for (int i = 0; i < num_loops; ++i) in = unpack12_32(in, out + i * 32);
      break;
    case 13:
      for (int i = 0; i < num_loops; ++i) in = unpack13_32(in, out + i * 32);
      break;
    case 14:
      for (int i = 0; i < num_loops; ++i) in = unpack14_32(in, out + i * 32);
      break;
    case 15:
      for (int i = 0; i < num_loops; ++i) in = unpack15_32(in, out + i * 32);
      break;
    case 16:
      for (int i = 0; i < num_loops; ++i) in = unpack16_32(in, out + i * 32);
      break;
    case 17:
      for (int i = 0; i < num_loops; ++i) in = unpack17_32(in, out + i * 32);
      break;
    case 18:
      for (int i = 0; i < num_loops; ++i) in = unpack18_32(in, out + i * 32);
      break;
    case 19:
      for (int i = 0; i < num_loops; ++i) in = unpack19_32(in, out + i * 32);
      break;
    case 20:
      for (int i = 0; i < num_loops; ++i) in = unpack20_32(in, out + i * 32);
      break;
    case 21:
      for (int i = 0; i < num_loops; ++i) in = unpack21_32(in, out + i * 32);
      break;
    case 22:
      for (int i = 0; i < num_loops; ++i) in = unpack22_32(in, out + i * 32);
      break;
    case 23:
      for (int i = 0; i < num_loops; ++i) in = unpack23_32(in, out + i * 32);
      break;
    case 24:
      for (int i = 0; i < num_loops; ++i) in = unpack24_32(in, out + i * 32);
      break;
    case 25:
      for (int i = 0; i < num_loops; ++i) in = unpack25_32(in, out + i * 32);
      break;
    case 26:
      for (int i = 0; i < num_loops; ++i) in = unpack26_32(in, out + i * 32);
      break;
    case 27:
      for (int i = 0; i < num_loops; ++i) in = unpack27_32(in, out + i * 32);
      break;
    case 28:
      for (int i = 0; i < num_loops; ++i) in = unpack28_32(in, out + i * 32);
      break;
    case 29:
      for (int i = 0; i < num_loops; ++i) in = unpack29_32(in, out + i * 32);
      break;
    case 30:
      for (int i = 0; i < num_loops; ++i) in = unpack30_32(in, out + i * 32);
      break;
    case 31:
      for (int i = 0; i < num_loops; ++i) in = unpack31_32(in, out + i * 32);
      break;
    case 32:
      for (int i = 0; i < num_loops; ++i) in = unpack32_32(in, out + i * 32);
      break;
    default:
      DCHECK(false) << "Unsupported num_bits";
  }

  return batch_size;
}

}  // namespace internal
}  // namespace arrow
