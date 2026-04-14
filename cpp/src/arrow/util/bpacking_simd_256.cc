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

#if defined(ARROW_HAVE_SVE256) || defined(ARROW_HAVE_RUNTIME_SVE256)
#  define UNPACK_PLATFORM unpack_sve256
#elif defined(ARROW_HAVE_RUNTIME_AVX2)
#  define UNPACK_PLATFORM unpack_avx2
#endif

#if defined(UNPACK_PLATFORM)

#  include "arrow/util/bpacking_dispatch_internal.h"
#  include "arrow/util/bpacking_internal.h"
#  include "arrow/util/bpacking_simd_internal.h"
#  include "arrow/util/bpacking_simd_kernel_internal.h"

namespace arrow::internal::bpacking {

template <typename UnpackedUint, int kPackedBitSize>
using Simd256Kernel = Kernel<UnpackedUint, kPackedBitSize, 256>;

template <typename Uint>
void UNPACK_PLATFORM(const uint8_t* in, Uint* out, const UnpackOptions& opts) {
  return unpack_jump<Simd256Kernel>(in, out, opts);
}

template void UNPACK_PLATFORM<bool>(const uint8_t*, bool*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint8_t>(const uint8_t*, uint8_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint16_t>(const uint8_t*, uint16_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint32_t>(const uint8_t*, uint32_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint64_t>(const uint8_t*, uint64_t*, const UnpackOptions&);

}  // namespace arrow::internal::bpacking

#  undef UNPACK_PLATFORM
#endif  // UNPACK_PLATFORM
