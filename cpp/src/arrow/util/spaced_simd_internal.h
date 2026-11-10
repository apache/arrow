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

#include <cstdint>

#include "arrow/util/visibility.h"

// SpacedExpandRightward (in place) and SpacedExpand (out of place) on
// unsigned 4 or 8 byte lanes, one bitmap word at a time, for each dispatch
// target. Every null slot is zeroed.

#define SPACED_EXPAND_DECLARE(TARGET)                                                 \
  template <typename Uint>                                                            \
  ARROW_EXPORT void SpacedExpandRightward##TARGET(                                    \
      Uint* buffer, int num_values, int null_count, const uint8_t* valid_bits,        \
      int64_t valid_bits_offset);                                                     \
  template <typename Uint>                                                            \
  ARROW_EXPORT void SpacedExpand##TARGET(const Uint* src, int num_values,             \
                                         int null_count, const uint8_t* valid_bits,   \
                                         int64_t valid_bits_offset, Uint* output);    \
  extern template ARROW_TEMPLATE_EXPORT void SpacedExpandRightward##TARGET<uint32_t>( \
      uint32_t*, int, int, const uint8_t*, int64_t);                                  \
  extern template ARROW_TEMPLATE_EXPORT void SpacedExpandRightward##TARGET<uint64_t>( \
      uint64_t*, int, int, const uint8_t*, int64_t);                                  \
  extern template ARROW_TEMPLATE_EXPORT void SpacedExpand##TARGET<uint32_t>(          \
      const uint32_t*, int, int, const uint8_t*, int64_t, uint32_t*);                 \
  extern template ARROW_TEMPLATE_EXPORT void SpacedExpand##TARGET<uint64_t>(          \
      const uint64_t*, int, int, const uint8_t*, int64_t, uint64_t*)

namespace arrow::util::internal {

SPACED_EXPAND_DECLARE(None);

#if defined(ARROW_HAVE_NEON)
SPACED_EXPAND_DECLARE(Neon);
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
SPACED_EXPAND_DECLARE(Sse4_2);
#endif

#if defined(ARROW_HAVE_AVX2) || defined(ARROW_HAVE_RUNTIME_AVX2)
SPACED_EXPAND_DECLARE(Avx2);
#endif

#if defined(ARROW_HAVE_AVX512) || defined(ARROW_HAVE_RUNTIME_AVX512)
SPACED_EXPAND_DECLARE(Avx512);
#endif

}  // namespace arrow::util::internal

#undef SPACED_EXPAND_DECLARE
