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
        ARROW_DISPATCH_TARGET_AVX512(&bpacking::unpack_avx512<Uint>)  //
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

}  // namespace arrow::internal
