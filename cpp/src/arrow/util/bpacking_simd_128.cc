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

#if defined(ARROW_HAVE_NEON)
#  define UNPACK_PLATFORM unpack_neon
#  define KERNEL_PLATFORM KernelNeon
#elif defined(ARROW_HAVE_RVV128) || defined(ARROW_HAVE_RUNTIME_RVV128)
#  define UNPACK_PLATFORM unpack_rvv128
#  define KERNEL_PLATFORM KernelRvv128
#  define ARROW_RVV_MEDIUM_SCALAR_FALLBACK 1
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define UNPACK_PLATFORM unpack_sse4_2
#  define KERNEL_PLATFORM KernelSse42
#endif

#if defined(UNPACK_PLATFORM)

#  include "arrow/util/bpacking_dispatch_internal.h"
#  include "arrow/util/bpacking_scalar_internal.h"
#  include "arrow/util/bpacking_simd_internal.h"
#  include "arrow/util/bpacking_simd_kernel_internal.h"

namespace arrow::internal::bpacking {

template <typename UnpackedUint, int kPackedBitSize>
using KERNEL_PLATFORM = Kernel<UnpackedUint, kPackedBitSize, xsimd::default_arch>;

template <typename Uint>
void UNPACK_PLATFORM(const uint8_t* in, Uint* out, const UnpackOptions& opts) {
#if defined(ARROW_RVV_MEDIUM_SCALAR_FALLBACK)
  // On RISC-V RVV, unpacking single-bit packed integers into uint64_t is faster
  // with the compiler auto-vectorized scalar implementation than with the generic
  // medium kernel. The latter relies on byte swizzling plus per-lane variable
  // shifts, which do not map efficiently to RVV instructions and cause a
  // performance regression (measured on VLEN=128 and VLEN=256 hardware).
  if constexpr (sizeof(Uint) == sizeof(uint64_t)) {
    if (opts.bit_width == 1) {
      return unpack_scalar<Uint>(in, out, opts);
    }
  }
#endif
  return unpack_jump<KERNEL_PLATFORM>(in, out, opts);
}

template void UNPACK_PLATFORM<bool>(const uint8_t*, bool*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint8_t>(const uint8_t*, uint8_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint16_t>(const uint8_t*, uint16_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint32_t>(const uint8_t*, uint32_t*, const UnpackOptions&);
template void UNPACK_PLATFORM<uint64_t>(const uint8_t*, uint64_t*, const UnpackOptions&);

}  // namespace arrow::internal::bpacking

#  undef ARROW_RVV_MEDIUM_SCALAR_FALLBACK
#  undef UNPACK_PLATFORM
#endif  // UNPACK_PLATFORM
