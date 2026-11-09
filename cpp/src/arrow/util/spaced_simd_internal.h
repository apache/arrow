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

namespace arrow::util::internal {

#if defined(ARROW_HAVE_NEON)
#  define SPACED_EXPAND_ARCH128 SpacedExpandRightwardNeon
#elif defined(ARROW_HAVE_SSE4_2) || defined(ARROW_HAVE_RUNTIME_SSE4_2)
#  define SPACED_EXPAND_ARCH128 SpacedExpandRightwardSse4_2
#endif

#if defined(SPACED_EXPAND_ARCH128)

/// SpacedExpandRightward on unsigned 4 or 8 byte lanes, with 128 bit SIMD.
/// Every null slot is zeroed.
template <typename Uint>
ARROW_EXPORT void SPACED_EXPAND_ARCH128(Uint* buffer, int num_values, int null_count,
                                        const uint8_t* valid_bits,
                                        int64_t valid_bits_offset);

extern template ARROW_TEMPLATE_EXPORT void SPACED_EXPAND_ARCH128<uint32_t>(
    uint32_t* buffer, int num_values, int null_count, const uint8_t* valid_bits,
    int64_t valid_bits_offset);

extern template ARROW_TEMPLATE_EXPORT void SPACED_EXPAND_ARCH128<uint64_t>(
    uint64_t* buffer, int num_values, int null_count, const uint8_t* valid_bits,
    int64_t valid_bits_offset);

#endif  // SPACED_EXPAND_ARCH128
#undef SPACED_EXPAND_ARCH128

}  // namespace arrow::util::internal
