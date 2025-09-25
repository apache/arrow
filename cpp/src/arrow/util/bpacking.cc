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

struct Unpack32DynamicFunction {
  using FunctionType = decltype(&unpack32_scalar);
  using Implementation = std::pair<DispatchLevel, FunctionType>;

  static auto implementations() {
    return std::array {
      // Current SIMD unpack algorithm works terribly on SSE4.2 due to lack of variable
      // rhsift and poor xsimd fallback.
      Implementation{DispatchLevel::NONE, &unpack32_scalar},
#if defined(ARROW_HAVE_RUNTIME_AVX2)
          Implementation{DispatchLevel::AVX2, &unpack32_avx2},
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
          Implementation{DispatchLevel::AVX512, &unpack32_avx512},
#endif
    };
  }
};

struct Unpack64DynamicFunction {
  using FunctionType = decltype(&unpack64_scalar);
  using Implementation = std::pair<DispatchLevel, FunctionType>;

  static auto implementations() {
    return std::array {
      // Current SIMD unpack algorithm works terribly on SSE4.2 due to lack of variable
      // rhsift and poor xsimd fallback.
      Implementation{DispatchLevel::NONE, &unpack64_scalar},
#if defined(ARROW_HAVE_RUNTIME_AVX2)
          // Note that Avx2 implementation only slightly outperform scalar
          Implementation{DispatchLevel::AVX2, &unpack64_avx2},
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
          Implementation{DispatchLevel::AVX512, &unpack64_avx512},
#endif
    };
  }
};

}  // namespace

template <typename Uint>
ARROW_EXPORT int unpack(const uint8_t* in, Uint* out, int batch_size, int num_bits) {
  if constexpr (std::is_same_v<Uint, uint16_t>) {
    // Current SIMD unpack function do not out beat scalar implementation for uin16_t
    return unpack16_scalar(in, out, batch_size, num_bits);
  }

  if constexpr (std::is_same_v<Uint, uint32_t>) {
#if defined(ARROW_HAVE_NEON)
    return unpack32_neon(in, out, batch_size, num_bits);
#else
    static DynamicDispatch<Unpack32DynamicFunction> dispatch;
    return dispatch.func(in, out, batch_size, num_bits);
#endif
  }

  if constexpr (std::is_same_v<Uint, uint64_t>) {
#if defined(ARROW_HAVE_NEON)
    return unpack64_neon(in, out, batch_size, num_bits);
#else
    static DynamicDispatch<Unpack64DynamicFunction> dispatch;
    return dispatch.func(in, out, batch_size, num_bits);
#endif
  }

  return 0;
}

template int unpack<uint16_t>(const uint8_t*, uint16_t*, int, int);
template int unpack<uint32_t>(const uint8_t*, uint32_t*, int, int);
template int unpack<uint64_t>(const uint8_t*, uint64_t*, int, int);

}  // namespace arrow::internal
