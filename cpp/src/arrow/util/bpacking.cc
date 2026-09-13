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

#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
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
        ARROW_DISPATCH_TARGET_SVE128(&bpacking::unpack_sve128<Uint>)  //
        ARROW_DISPATCH_TARGET_SVE256(&bpacking::unpack_sve256<Uint>)  //
        ARROW_DISPATCH_TARGET_SSE4_2(&bpacking::unpack_sse4_2<Uint>)  //
        ARROW_DISPATCH_TARGET_AVX2(&bpacking::unpack_avx2<Uint>)      //
        // Cap the bit-unpack dispatch at 256 bits. The 512-bit target is the one
        // still driven by the legacy generated kernels in
        // bpacking_simd512_generated_internal.h, which build their SIMD input
        // register from an initializer list of scalar loads (one vmovd + one
        // vpinsrd per element) and are not force-inlined, so each step is an
        // out-of-line call. Measured on Granite Rapids over 102400 values, that
        // makes it 6.26x slower than the AVX2 target (geomean widths 1..31: 6.54
        // vs 40.97 GiB/s) and 0.67x of the *scalar* kernel. Since
        // ARROW_RUNTIME_SIMD_LEVEL defaults to MAX, every AVX-512 machine was
        // preferring it. Re-enable once the 512-bit kernels issue real vector
        // loads -- naively pointing this TU at the Kernel<> machinery used by
        // bpacking_simd_{128,256}.cc is NOT the fix: it measures 1.17 GiB/s,
        // 5.6x worse again, because most widths land on is_oversized() ->
        // NoOpKernel and fall through to the naive path.
        // ARROW_DISPATCH_TARGET_AVX512(&bpacking::unpack_avx512<Uint>)  //
    };
  }
};

template <typename Uint>
struct UnpackBiasDynamicFunction {
  using FunctionType = decltype(&bpacking::unpack_bias_scalar<Uint>);

  static constexpr auto targets() {
    return std::array{
        ARROW_DISPATCH_TARGET_NONE(&bpacking::unpack_bias_scalar<Uint>)    //
        ARROW_DISPATCH_TARGET_NEON(&bpacking::unpack_bias_neon<Uint>)      //
        ARROW_DISPATCH_TARGET_SVE128(&bpacking::unpack_bias_sve128<Uint>)  //
        ARROW_DISPATCH_TARGET_SVE256(&bpacking::unpack_bias_sve256<Uint>)  //
        ARROW_DISPATCH_TARGET_SSE4_2(&bpacking::unpack_bias_sse4_2<Uint>)  //
        ARROW_DISPATCH_TARGET_AVX2(&bpacking::unpack_bias_avx2<Uint>)      //
        // Capped at 256 bits for the reason given in UnpackDynamicFunction above.
        // ARROW_DISPATCH_TARGET_AVX512(&bpacking::unpack_bias_avx512<Uint>)  //
    };
  }
};

}  // namespace

template <typename Uint>
void unpack(const uint8_t* in, Uint* out, const UnpackOptions& opts) {
  static const DynamicDispatch<UnpackDynamicFunction<Uint>> dispatch;
  return dispatch(in, out, opts);
}

template void unpack<bool>(const uint8_t*, bool*, const UnpackOptions&);
template void unpack<uint8_t>(const uint8_t*, uint8_t*, const UnpackOptions&);
template void unpack<uint16_t>(const uint8_t*, uint16_t*, const UnpackOptions&);
template void unpack<uint32_t>(const uint8_t*, uint32_t*, const UnpackOptions&);
template void unpack<uint64_t>(const uint8_t*, uint64_t*, const UnpackOptions&);

template <typename Uint>
void unpack_bias(const uint8_t* in, Uint* out, const UnpackOptions& opts, Uint bias) {
  static const DynamicDispatch<UnpackBiasDynamicFunction<Uint>> dispatch;
  return dispatch(in, out, opts, bias);
}

template void unpack_bias<uint8_t>(const uint8_t*, uint8_t*, const UnpackOptions&,
                                   uint8_t);
template void unpack_bias<uint16_t>(const uint8_t*, uint16_t*, const UnpackOptions&,
                                    uint16_t);
template void unpack_bias<uint32_t>(const uint8_t*, uint32_t*, const UnpackOptions&,
                                    uint32_t);
template void unpack_bias<uint64_t>(const uint8_t*, uint64_t*, const UnpackOptions&,
                                    uint64_t);

}  // namespace arrow::internal
