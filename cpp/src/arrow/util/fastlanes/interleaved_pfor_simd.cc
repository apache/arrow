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

// A vector leg of the interleaved kernel dispatch. This one source is compiled
// once per instruction set the build can target, with that set's flags applied
// per-file by CMake, and names its entry points after the set it was compiled
// for. Which set that is comes from the flags themselves rather than from
// anything written here, so the two cannot disagree: the same condition that
// picks the names below is the condition CMake tests before adding the file.
//
// Only one of these branches is ever live in a given build, which is why one
// file can serve both. util/bpacking_simd_256.cc is registered twice the same
// way, for the same reason.

#if defined(ARROW_HAVE_RUNTIME_SVE128)
#  define ILV_PACK_PLATFORM InterleavedPackBlockSve128
#  define ILV_UNPACK_PLATFORM InterleavedUnpackBlockSve128
#elif defined(ARROW_HAVE_RUNTIME_AVX2)
#  define ILV_PACK_PLATFORM InterleavedPackBlockAvx2
#  define ILV_UNPACK_PLATFORM InterleavedUnpackBlockAvx2
#endif

#if !defined(ILV_PACK_PLATFORM)
#  error "This file must be compiled with a known SIMD micro architecture"
#endif

#include "arrow/util/fastlanes/interleaved_dispatch_internal.h"
#include "arrow/util/fastlanes/interleaved_kernel_table_internal.h"

namespace arrow {
namespace util {
namespace fastlanes {

void ILV_PACK_PLATFORM(uint8_t bit_width, const uint32_t* in, uint32_t* out) {
  return PackLeg<>(bit_width, in, out);
}

void ILV_UNPACK_PLATFORM(uint8_t bit_width, const uint32_t* packed, uint32_t* out,
                         uint32_t bias) {
  return UnpackLeg<>(bit_width, packed, out, bias);
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow

#undef ILV_PACK_PLATFORM
#undef ILV_UNPACK_PLATFORM
