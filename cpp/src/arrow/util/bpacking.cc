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

#include <array>
#include <type_traits>

#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/cpu_info.h"
#include "arrow/util/dispatch_internal.h"

namespace arrow::internal {

namespace {

template <typename Uint>
struct UnpackDynamicFunction {
  using FunctionType = decltype(&bpacking::unpack_scalar<Uint>);

  static constexpr auto targets() {
    return std::array{
        ARROW_DISPATCH_TARGET_NONE(&bpacking::unpack_scalar<Uint>)    //
        ARROW_DISPATCH_TARGET_NEON(&bpacking::unpack_neon<Uint>)      //
        ARROW_DISPATCH_TARGET_RVV128(&bpacking::unpack_rvv128<Uint>)  //
        // GH-50503: No SVE128 dispatch as it increases code size without
        // increasing performance vs. Neon, and can produce ODR violations.
        ARROW_DISPATCH_TARGET_SVE256(&bpacking::unpack_sve256<Uint>)  //
        ARROW_DISPATCH_TARGET_RVV256(&bpacking::unpack_rvv256<Uint>)  //
        ARROW_DISPATCH_TARGET_SSE4_2(&bpacking::unpack_sse4_2<Uint>)  //
        ARROW_DISPATCH_TARGET_AVX2(&bpacking::unpack_avx2<Uint>)      //
        ARROW_DISPATCH_TARGET_AVX512(&bpacking::unpack_avx512<Uint>)  //
    };
  }
};

}  // namespace

template <typename Uint>
void unpack(const uint8_t* in, Uint* out, const UnpackOptions& opts) {
#if defined(ARROW_HAVE_RVV128) || defined(ARROW_HAVE_RVV256) ||  \
    defined(ARROW_HAVE_RUNTIME_RVV128) ||                        \
    defined(ARROW_HAVE_RUNTIME_RVV256)
  // On RISC-V RVV, the medium SIMD kernel is slower than the scalar
  // implementation (which the compiler auto-vectorizes efficiently) for several
  // unpack combinations. Route them directly to the scalar path to avoid both
  // the slow kernel and the extra SIMD dispatch indirection.
  constexpr bool kIsUint64 = sizeof(Uint) == sizeof(uint64_t);
  // Whether the runtime dispatch would target the RVV256 kernel. CpuInfo::RVV256
  // is set only when the hardware VLEN is 256 (see cpu_info.cc), so this is
  // reliable on both VLEN=128 and VLEN=256 machines.
  static const bool kRvv256 = CpuInfo::GetInstance()->IsSupported(CpuInfo::RVV256);
  if (opts.bit_width == 1 && kIsUint64) {
    // Uint64/1-bit regresses on all RVV VLENs.
    return bpacking::unpack_scalar<Uint>(in, out, opts);
  }
  if (kRvv256) {
    // The following combinations only regress on VLEN=256 hardware; on VLEN=128
    // they are faster than scalar and must keep using the SIMD kernel.
    if (kIsUint64 && opts.bit_width == 2) {
      // Uint64/2-bit regresses on VLEN=256 only.
      return bpacking::unpack_scalar<Uint>(in, out, opts);
    }
    // Large bit widths on small batches have too much fixed medium-kernel
    // overhead to beat the scalar path on VLEN=256.
    if constexpr (std::is_same_v<Uint, uint16_t>) {
      if (opts.bit_width == 13 && opts.batch_size <= 32) {
        return bpacking::unpack_scalar<Uint>(in, out, opts);
      }
    } else if constexpr (std::is_same_v<Uint, uint32_t>) {
      if (opts.bit_width == 20 && opts.batch_size <= 32) {
        return bpacking::unpack_scalar<Uint>(in, out, opts);
      }
    } else if constexpr (std::is_same_v<Uint, uint64_t>) {
      if (opts.bit_width == 20 && opts.batch_size <= 64) {
        return bpacking::unpack_scalar<Uint>(in, out, opts);
      }
    }
  }
#endif
  static const DynamicDispatch<UnpackDynamicFunction<Uint>> dispatch;
  return dispatch(in, out, opts);
}

template void unpack<bool>(const uint8_t*, bool*, const UnpackOptions&);
template void unpack<uint8_t>(const uint8_t*, uint8_t*, const UnpackOptions&);
template void unpack<uint16_t>(const uint8_t*, uint16_t*, const UnpackOptions&);
template void unpack<uint32_t>(const uint8_t*, uint32_t*, const UnpackOptions&);
template void unpack<uint64_t>(const uint8_t*, uint64_t*, const UnpackOptions&);

}  // namespace arrow::internal
