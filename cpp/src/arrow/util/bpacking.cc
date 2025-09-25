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

#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/dispatch_internal.h"

namespace arrow::internal {

namespace {

struct Unpack16DynamicFunction {
  using FunctionType = decltype(&unpack16_scalar);

  static std::vector<std::pair<DispatchLevel, FunctionType>> implementations() {
    return {
        // TODO Strong SSE baseline
        {DispatchLevel::NONE, unpack16_scalar}
#if defined(ARROW_HAVE_RUNTIME_AVX2)
        ,
        {DispatchLevel::AVX2, unpack16_avx2}
#endif
        // TODO AVX 512 not implemented
    };
  }
};

}  // namespace

int unpack16(const uint8_t* in, uint16_t* out, int batch_size, int num_bits) {
#if defined(ARROW_HAVE_NEON)
  return unpack16_neon(in, out, batch_size, num_bits);
#else
  static DynamicDispatch<Unpack16DynamicFunction> dispatch;
  return dispatch.func(in, out, batch_size, num_bits);
#endif
}

namespace {

struct Unpack32DynamicFunction {
  using FunctionType = decltype(&unpack32_scalar);

  static std::vector<std::pair<DispatchLevel, FunctionType>> implementations() {
    // TODO Strong SSE baseline
    return {{DispatchLevel::NONE, unpack32_scalar}
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

int unpack32(const uint8_t* in, uint32_t* out, int batch_size, int num_bits) {
#if defined(ARROW_HAVE_NEON)
  return unpack32_neon(in, out, batch_size, num_bits);
#else
  static DynamicDispatch<Unpack32DynamicFunction> dispatch;
  return dispatch.func(in, out, batch_size, num_bits);
#endif
}

int unpack64(const uint8_t* in, uint64_t* out, int batch_size, int num_bits) {
  // TODO: unpack64_neon, unpack64_avx2 and unpack64_avx512
  return unpack64_scalar(in, out, batch_size, num_bits);
}

}  // namespace arrow::internal
