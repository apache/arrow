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

#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/dispatch_internal.h"

namespace arrow::internal {

namespace {

template <typename Uint>
struct UnpackDynamicFunction {
  using FunctionType = decltype(&unpack_scalar<Uint>);
  using Implementation = std::pair<DispatchLevel, FunctionType>;

  static constexpr auto implementations() {
    return std::array {
      // Current SIMD unpack algorithm works terribly on SSE4.2 due to lack of variable
      // rhsift and poor xsimd fallback.
      Implementation{DispatchLevel::NONE, &unpack_scalar<Uint>},
#if defined(ARROW_HAVE_RUNTIME_AVX2)
          Implementation{DispatchLevel::AVX2, &unpack_avx2<Uint>},
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
          Implementation{DispatchLevel::AVX512, &unpack_avx512<Uint>},
#endif
    };
  }
};

}  // namespace

template <typename Uint>
int unpack(const uint8_t* in, Uint* out, int batch_size, int num_bits) {
#if defined(ARROW_HAVE_NEON)
  return unpack_neon(in, out, batch_size, num_bits);
#else
  static DynamicDispatch<UnpackDynamicFunction<Uint> > dispatch;
  return dispatch.func(in, out, batch_size, num_bits);
#endif
}

template int unpack<bool>(const uint8_t*, bool*, int, int);
template int unpack<uint8_t>(const uint8_t*, uint8_t*, int, int);
template int unpack<uint16_t>(const uint8_t*, uint16_t*, int, int);
template int unpack<uint32_t>(const uint8_t*, uint32_t*, int, int);
template int unpack<uint64_t>(const uint8_t*, uint64_t*, int, int);

}  // namespace arrow::internal
