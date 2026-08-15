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

#pragma once

#include "arrow/util/bpacking_internal.h"
#include "arrow/util/visibility.h"

#include <cstdint>

namespace arrow::internal::bpacking {

#if defined(ARROW_HAVE_NEON)
#  define UNPACK_ARCH128 unpack_neon
#elif defined(ARROW_HAVE_SSE4_2)
#  define UNPACK_ARCH128 unpack_sse4_2
#endif

#if defined(UNPACK_ARCH128)

template <typename Uint>
ARROW_EXPORT void UNPACK_ARCH128(const uint8_t* in, Uint* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH128<bool>(  //
    const uint8_t* in, bool* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH128<uint8_t>(
    const uint8_t* in, uint8_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH128<uint16_t>(
    const uint8_t* in, uint16_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH128<uint32_t>(
    const uint8_t* in, uint32_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH128<uint64_t>(
    const uint8_t* in, uint64_t* out, const UnpackOptions& opts);

#endif  // UNPACK_ARCH128
#undef UNPACK_ARCH128

#if defined(ARROW_HAVE_SVE256) || defined(ARROW_HAVE_RUNTIME_SVE256)
#  define UNPACK_ARCH256 unpack_sve256
#elif defined(UNPACK_ARCH256) || defined(ARROW_HAVE_RUNTIME_AVX2)
#  define UNPACK_ARCH256 unpack_avx2
#endif

#if defined(UNPACK_ARCH256)

template <typename Uint>
ARROW_EXPORT void UNPACK_ARCH256(const uint8_t* in, Uint* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH256<bool>(  //
    const uint8_t* in, bool* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH256<uint8_t>(
    const uint8_t* in, uint8_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH256<uint16_t>(
    const uint8_t* in, uint16_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH256<uint32_t>(
    const uint8_t* in, uint32_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void UNPACK_ARCH256<uint64_t>(
    const uint8_t* in, uint64_t* out, const UnpackOptions& opts);

#endif  // UNPACK_ARCH256
#undef UNPACK_ARCH256

#if defined(ARROW_HAVE_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX512)

template <typename Uint>
ARROW_EXPORT void unpack_avx512(const uint8_t* in, Uint* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack_avx512<bool>(  //
    const uint8_t* in, bool* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack_avx512<uint8_t>(
    const uint8_t* in, uint8_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack_avx512<uint16_t>(
    const uint8_t* in, uint16_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack_avx512<uint32_t>(
    const uint8_t* in, uint32_t* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack_avx512<uint64_t>(
    const uint8_t* in, uint64_t* out, const UnpackOptions& opts);

#endif

}  // namespace arrow::internal::bpacking
