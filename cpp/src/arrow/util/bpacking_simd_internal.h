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

#include "arrow/util/dispatch_internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace internal {

template <typename UnpackBits>
static int unpack32_specialized(const uint32_t* in, uint32_t* out, int batch_size,
                                int num_bits) {
  batch_size = batch_size / 32 * 32;
  int num_loops = batch_size / 32;

  switch (num_bits) {
    case 0:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack0_32(in, out + i * 32);
      break;
    case 1:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack1_32(in, out + i * 32);
      break;
    case 2:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack2_32(in, out + i * 32);
      break;
    case 3:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack3_32(in, out + i * 32);
      break;
    case 4:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack4_32(in, out + i * 32);
      break;
    case 5:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack5_32(in, out + i * 32);
      break;
    case 6:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack6_32(in, out + i * 32);
      break;
    case 7:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack7_32(in, out + i * 32);
      break;
    case 8:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack8_32(in, out + i * 32);
      break;
    case 9:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack9_32(in, out + i * 32);
      break;
    case 10:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack10_32(in, out + i * 32);
      break;
    case 11:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack11_32(in, out + i * 32);
      break;
    case 12:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack12_32(in, out + i * 32);
      break;
    case 13:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack13_32(in, out + i * 32);
      break;
    case 14:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack14_32(in, out + i * 32);
      break;
    case 15:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack15_32(in, out + i * 32);
      break;
    case 16:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack16_32(in, out + i * 32);
      break;
    case 17:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack17_32(in, out + i * 32);
      break;
    case 18:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack18_32(in, out + i * 32);
      break;
    case 19:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack19_32(in, out + i * 32);
      break;
    case 20:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack20_32(in, out + i * 32);
      break;
    case 21:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack21_32(in, out + i * 32);
      break;
    case 22:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack22_32(in, out + i * 32);
      break;
    case 23:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack23_32(in, out + i * 32);
      break;
    case 24:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack24_32(in, out + i * 32);
      break;
    case 25:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack25_32(in, out + i * 32);
      break;
    case 26:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack26_32(in, out + i * 32);
      break;
    case 27:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack27_32(in, out + i * 32);
      break;
    case 28:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack28_32(in, out + i * 32);
      break;
    case 29:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack29_32(in, out + i * 32);
      break;
    case 30:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack30_32(in, out + i * 32);
      break;
    case 31:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack31_32(in, out + i * 32);
      break;
    case 32:
      for (int i = 0; i < num_loops; ++i) in = UnpackBits::unpack32_32(in, out + i * 32);
      break;
    default:
      ARROW_DCHECK(false) << "Unsupported num_bits";
  }

  return batch_size;
}

}  // namespace internal
}  // namespace arrow
