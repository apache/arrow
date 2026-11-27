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
#  define KERNEL_PLATFORM KernelSve256
#elif defined(ARROW_HAVE_RVV256) || defined(ARROW_HAVE_RUNTIME_RVV256)
#  define UNPACK_PLATFORM unpack_rvv256
#  define KERNEL_PLATFORM KernelRvv256
#  define ARROW_RVV_MEDIUM_SCALAR_FALLBACK 1
#elif defined(ARROW_HAVE_RUNTIME_AVX2)
#  define UNPACK_PLATFORM unpack_avx2
#  define KERNEL_PLATFORM KernelAvx2
#endif

#if !defined(UNPACK_PLATFORM)
#  error "This file must be compiled with a known SIMD micro architecture"
#endif

#include <type_traits>

#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/bpacking_simd_kernel_internal.h"

namespace arrow::internal::bpacking {

template <typename UnpackedUint, int kPackedBitSize>
using KERNEL_PLATFORM = Kernel<UnpackedUint, kPackedBitSize, xsimd::default_arch>;

template <typename Uint>
void UNPACK_PLATFORM(const uint8_t* in, Uint* out, const UnpackOptions& opts) {
#if defined(ARROW_RVV_MEDIUM_SCALAR_FALLBACK)
  // On RISC-V RVV (VLEN=256 here), several unpack combinations are faster with
  // the compiler auto-vectorized scalar implementation than with the generic
  // medium kernel, which relies on byte swizzling plus per-lane variable shifts
  // that do not map efficiently to RVV instructions. Measured on VLEN=256
  // hardware; the VLEN=128 kernel file only falls back for Uint64/1-bit.
  constexpr bool kIsUint64 = sizeof(Uint) == sizeof(uint64_t);
  if (kIsUint64 && (opts.bit_width == 1 || opts.bit_width == 2)) {
    return unpack_scalar<Uint>(in, out, opts);
  }
  // Large bit widths on small batches have too much fixed medium-kernel overhead.
  if constexpr (std::is_same_v<Uint, uint16_t>) {
    if (opts.bit_width == 13 && opts.batch_size <= 32) {
      return unpack_scalar<Uint>(in, out, opts);
    }
  } else if constexpr (std::is_same_v<Uint, uint32_t>) {
    if (opts.bit_width == 20 && opts.batch_size <= 32) {
      return unpack_scalar<Uint>(in, out, opts);
    }
  } else if constexpr (std::is_same_v<Uint, uint64_t>) {
    if (opts.bit_width == 20 && opts.batch_size <= 64) {
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

#undef ARROW_RVV_MEDIUM_SCALAR_FALLBACK
#undef UNPACK_PLATFORM
