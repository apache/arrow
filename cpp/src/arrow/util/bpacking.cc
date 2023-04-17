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

#include "arrow/util/bpacking.h"

#include "arrow/util/bpacking64_default.h"
#include "arrow/util/bpacking_default.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/dispatch.h"
#include "arrow/util/logging.h"

#if defined(ARROW_HAVE_RUNTIME_AVX2)
#include "arrow/util/bpacking_avx2.h"
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
#include "arrow/util/bpacking_avx512.h"
#endif
#if defined(ARROW_HAVE_NEON)
#include "arrow/util/bpacking_neon.h"
#endif

namespace arrow {
namespace internal {

namespace {

int unpack32_default(const uint32_t* in, uint32_t* out, int batch_size, int num_bits) {
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

struct Unpack32DynamicFunction {
  using FunctionType = decltype(&unpack32_default);

  static std::vector<std::pair<DispatchLevel, FunctionType>> implementations() {
    return {{DispatchLevel::NONE, unpack32_default}
#if defined(ARROW_HAVE_RUNTIME_AVX2)
            ,
            {DispatchLevel::AVX2, unpack32_avx2}
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
            ,
            {DispatchLevel::AVX512, unpack32_avx512}
#endif
    };
  }
};

}  // namespace

int unpack32(const uint32_t* in, uint32_t* out, int batch_size, int num_bits) {
#if defined(ARROW_HAVE_NEON)
  return unpack32_neon(in, out, batch_size, num_bits);
#else
  static DynamicDispatch<Unpack32DynamicFunction> dispatch;
  return dispatch.func(in, out, batch_size, num_bits);
#endif
}

namespace {

int unpack64_default(const uint8_t* in, uint64_t* out, int batch_size, int num_bits) {
  batch_size = batch_size / 32 * 32;
  int num_loops = batch_size / 32;

  switch (num_bits) {
    case 0:
      for (int i = 0; i < num_loops; ++i) in = unpack0_64(in, out + i * 32);
      break;
    case 1:
      for (int i = 0; i < num_loops; ++i) in = unpack1_64(in, out + i * 32);
      break;
    case 2:
      for (int i = 0; i < num_loops; ++i) in = unpack2_64(in, out + i * 32);
      break;
    case 3:
      for (int i = 0; i < num_loops; ++i) in = unpack3_64(in, out + i * 32);
      break;
    case 4:
      for (int i = 0; i < num_loops; ++i) in = unpack4_64(in, out + i * 32);
      break;
    case 5:
      for (int i = 0; i < num_loops; ++i) in = unpack5_64(in, out + i * 32);
      break;
    case 6:
      for (int i = 0; i < num_loops; ++i) in = unpack6_64(in, out + i * 32);
      break;
    case 7:
      for (int i = 0; i < num_loops; ++i) in = unpack7_64(in, out + i * 32);
      break;
    case 8:
      for (int i = 0; i < num_loops; ++i) in = unpack8_64(in, out + i * 32);
      break;
    case 9:
      for (int i = 0; i < num_loops; ++i) in = unpack9_64(in, out + i * 32);
      break;
    case 10:
      for (int i = 0; i < num_loops; ++i) in = unpack10_64(in, out + i * 32);
      break;
    case 11:
      for (int i = 0; i < num_loops; ++i) in = unpack11_64(in, out + i * 32);
      break;
    case 12:
      for (int i = 0; i < num_loops; ++i) in = unpack12_64(in, out + i * 32);
      break;
    case 13:
      for (int i = 0; i < num_loops; ++i) in = unpack13_64(in, out + i * 32);
      break;
    case 14:
      for (int i = 0; i < num_loops; ++i) in = unpack14_64(in, out + i * 32);
      break;
    case 15:
      for (int i = 0; i < num_loops; ++i) in = unpack15_64(in, out + i * 32);
      break;
    case 16:
      for (int i = 0; i < num_loops; ++i) in = unpack16_64(in, out + i * 32);
      break;
    case 17:
      for (int i = 0; i < num_loops; ++i) in = unpack17_64(in, out + i * 32);
      break;
    case 18:
      for (int i = 0; i < num_loops; ++i) in = unpack18_64(in, out + i * 32);
      break;
    case 19:
      for (int i = 0; i < num_loops; ++i) in = unpack19_64(in, out + i * 32);
      break;
    case 20:
      for (int i = 0; i < num_loops; ++i) in = unpack20_64(in, out + i * 32);
      break;
    case 21:
      for (int i = 0; i < num_loops; ++i) in = unpack21_64(in, out + i * 32);
      break;
    case 22:
      for (int i = 0; i < num_loops; ++i) in = unpack22_64(in, out + i * 32);
      break;
    case 23:
      for (int i = 0; i < num_loops; ++i) in = unpack23_64(in, out + i * 32);
      break;
    case 24:
      for (int i = 0; i < num_loops; ++i) in = unpack24_64(in, out + i * 32);
      break;
    case 25:
      for (int i = 0; i < num_loops; ++i) in = unpack25_64(in, out + i * 32);
      break;
    case 26:
      for (int i = 0; i < num_loops; ++i) in = unpack26_64(in, out + i * 32);
      break;
    case 27:
      for (int i = 0; i < num_loops; ++i) in = unpack27_64(in, out + i * 32);
      break;
    case 28:
      for (int i = 0; i < num_loops; ++i) in = unpack28_64(in, out + i * 32);
      break;
    case 29:
      for (int i = 0; i < num_loops; ++i) in = unpack29_64(in, out + i * 32);
      break;
    case 30:
      for (int i = 0; i < num_loops; ++i) in = unpack30_64(in, out + i * 32);
      break;
    case 31:
      for (int i = 0; i < num_loops; ++i) in = unpack31_64(in, out + i * 32);
      break;
    case 32:
      for (int i = 0; i < num_loops; ++i) in = unpack32_64(in, out + i * 32);
      break;
    case 33:
      for (int i = 0; i < num_loops; ++i) in = unpack33_64(in, out + i * 32);
      break;
    case 34:
      for (int i = 0; i < num_loops; ++i) in = unpack34_64(in, out + i * 32);
      break;
    case 35:
      for (int i = 0; i < num_loops; ++i) in = unpack35_64(in, out + i * 32);
      break;
    case 36:
      for (int i = 0; i < num_loops; ++i) in = unpack36_64(in, out + i * 32);
      break;
    case 37:
      for (int i = 0; i < num_loops; ++i) in = unpack37_64(in, out + i * 32);
      break;
    case 38:
      for (int i = 0; i < num_loops; ++i) in = unpack38_64(in, out + i * 32);
      break;
    case 39:
      for (int i = 0; i < num_loops; ++i) in = unpack39_64(in, out + i * 32);
      break;
    case 40:
      for (int i = 0; i < num_loops; ++i) in = unpack40_64(in, out + i * 32);
      break;
    case 41:
      for (int i = 0; i < num_loops; ++i) in = unpack41_64(in, out + i * 32);
      break;
    case 42:
      for (int i = 0; i < num_loops; ++i) in = unpack42_64(in, out + i * 32);
      break;
    case 43:
      for (int i = 0; i < num_loops; ++i) in = unpack43_64(in, out + i * 32);
      break;
    case 44:
      for (int i = 0; i < num_loops; ++i) in = unpack44_64(in, out + i * 32);
      break;
    case 45:
      for (int i = 0; i < num_loops; ++i) in = unpack45_64(in, out + i * 32);
      break;
    case 46:
      for (int i = 0; i < num_loops; ++i) in = unpack46_64(in, out + i * 32);
      break;
    case 47:
      for (int i = 0; i < num_loops; ++i) in = unpack47_64(in, out + i * 32);
      break;
    case 48:
      for (int i = 0; i < num_loops; ++i) in = unpack48_64(in, out + i * 32);
      break;
    case 49:
      for (int i = 0; i < num_loops; ++i) in = unpack49_64(in, out + i * 32);
      break;
    case 50:
      for (int i = 0; i < num_loops; ++i) in = unpack50_64(in, out + i * 32);
      break;
    case 51:
      for (int i = 0; i < num_loops; ++i) in = unpack51_64(in, out + i * 32);
      break;
    case 52:
      for (int i = 0; i < num_loops; ++i) in = unpack52_64(in, out + i * 32);
      break;
    case 53:
      for (int i = 0; i < num_loops; ++i) in = unpack53_64(in, out + i * 32);
      break;
    case 54:
      for (int i = 0; i < num_loops; ++i) in = unpack54_64(in, out + i * 32);
      break;
    case 55:
      for (int i = 0; i < num_loops; ++i) in = unpack55_64(in, out + i * 32);
      break;
    case 56:
      for (int i = 0; i < num_loops; ++i) in = unpack56_64(in, out + i * 32);
      break;
    case 57:
      for (int i = 0; i < num_loops; ++i) in = unpack57_64(in, out + i * 32);
      break;
    case 58:
      for (int i = 0; i < num_loops; ++i) in = unpack58_64(in, out + i * 32);
      break;
    case 59:
      for (int i = 0; i < num_loops; ++i) in = unpack59_64(in, out + i * 32);
      break;
    case 60:
      for (int i = 0; i < num_loops; ++i) in = unpack60_64(in, out + i * 32);
      break;
    case 61:
      for (int i = 0; i < num_loops; ++i) in = unpack61_64(in, out + i * 32);
      break;
    case 62:
      for (int i = 0; i < num_loops; ++i) in = unpack62_64(in, out + i * 32);
      break;
    case 63:
      for (int i = 0; i < num_loops; ++i) in = unpack63_64(in, out + i * 32);
      break;
    case 64:
      for (int i = 0; i < num_loops; ++i) in = unpack64_64(in, out + i * 32);
      break;
    default:
      DCHECK(false) << "Unsupported num_bits";
  }

  return batch_size;
}

}  // namespace

int unpack64(const uint8_t* in, uint64_t* out, int batch_size, int num_bits) {
  // TODO: unpack64_neon, unpack64_avx2 and unpack64_avx512
  return unpack64_default(in, out, batch_size, num_bits);
}

}  // namespace internal
}  // namespace arrow
