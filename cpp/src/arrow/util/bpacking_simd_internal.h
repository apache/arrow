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

#include "arrow/util/visibility.h"

#include <cstdint>

namespace arrow::internal {

#if defined(ARROW_HAVE_NEON)
ARROW_EXPORT int unpack16_neon(const uint8_t* in, uint16_t* out, int batch_size,
                               int num_bits);

ARROW_EXPORT int unpack32_neon(const uint8_t* in, uint32_t* out, int batch_size,
                               int num_bits);

ARROW_EXPORT int unpack64_neon(const uint8_t* in, uint64_t* out, int batch_size,
                               int num_bits);
#endif

#if defined(ARROW_HAVE_SSE4_2)
ARROW_EXPORT int unpack16_sse4_2(const uint8_t* in, uint16_t* out, int batch_size,
                                 int num_bits);

ARROW_EXPORT int unpack32_sse4_2(const uint8_t* in, uint32_t* out, int batch_size,
                                 int num_bits);

ARROW_EXPORT int unpack64_sse4_2(const uint8_t* in, uint64_t* out, int batch_size,
                                 int num_bits);
#endif

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
ARROW_EXPORT int unpack16_avx2(const uint8_t* in, uint16_t* out, int batch_size,
                               int num_bits);

ARROW_EXPORT int unpack32_avx2(const uint8_t* in, uint32_t* out, int batch_size,
                               int num_bits);

ARROW_EXPORT int unpack64_avx2(const uint8_t* in, uint64_t* out, int batch_size,
                               int num_bits);
#endif

#if defined(ARROW_HAVE_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX512)
ARROW_EXPORT int unpack32_avx512(const uint8_t* in, uint32_t* out, int batch_size,
                                 int num_bits);

ARROW_EXPORT int unpack64_avx512(const uint8_t* in, uint64_t* out, int batch_size,
                                 int num_bits);
#endif

}  // namespace arrow::internal
