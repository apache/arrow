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

// Runtime bit-width dispatch for the lane-interleaved kernels.
//
// The kernels in fastlanes_kernels_internal.h are intrinsic-free C++ templated
// on the bit width, so one compiled copy is pinned to the instruction set of
// the translation unit that compiled it. They are therefore compiled once per
// instruction set, and this file declares the entry point each of those units
// exports. arrow::internal::DynamicDispatch in pfor/pfor.cc picks between them,
// so ARROW_USER_SIMD_LEVEL reaches these kernels like it reaches the rest of
// Arrow's SIMD code; before this existed, ARROW_SIMD_LEVEL alone fixed their
// width, and a default x86 build ran the interleaved kernel in XMM registers
// while the sequential unpacker it is compared against dispatched to AVX2.
//
// Each entry point takes the bit width as a runtime argument and resolves it
// against a table its own translation unit builds; see
// interleaved_kernel_table_internal.h.

#pragma once

#include <cstdint>

namespace arrow {
namespace util {
namespace fastlanes {

// What the per-width tables hold.
using InterleavedPackFn = void (*)(const uint32_t*, uint32_t*);
using InterleavedUnpackFn = void (*)(const uint32_t*, uint32_t*, uint32_t);

// Compiled at whatever instruction set the build's own flags name. This is the
// dispatch fallback and the only leg guaranteed to exist, since a build can be
// configured with no SIMD level at all.
void InterleavedPackBlockBaseline(uint8_t bit_width, const uint32_t* in, uint32_t* out);
void InterleavedUnpackBlockBaseline(uint8_t bit_width, const uint32_t* packed,
                                    uint32_t* out, uint32_t bias);

// The vector legs, each declared under exactly the condition CMake uses to
// compile it: a build that never compiles a leg must not name one either.
#if defined(ARROW_HAVE_RUNTIME_SVE128)
void InterleavedPackBlockSve128(uint8_t bit_width, const uint32_t* in, uint32_t* out);
void InterleavedUnpackBlockSve128(uint8_t bit_width, const uint32_t* packed,
                                  uint32_t* out, uint32_t bias);
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX2)
void InterleavedPackBlockAvx2(uint8_t bit_width, const uint32_t* in, uint32_t* out);
void InterleavedUnpackBlockAvx2(uint8_t bit_width, const uint32_t* packed, uint32_t* out,
                                uint32_t bias);
#endif

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
