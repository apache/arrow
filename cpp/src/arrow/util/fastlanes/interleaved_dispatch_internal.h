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
// fastlanes_kernels_internal.h holds kernels templated on the bit width, with no
// intrinsics: they are portable C++ whose register width is whatever the
// translation unit's compile-time flags permit. That makes them fast, and it
// also means one compiled copy is pinned to one instruction set. A decoder does
// not know the instruction set until it runs, so the kernels are compiled once
// per instruction set into their own translation unit, and this file declares
// the entry point each of those units exports.
//
// The picking is done by arrow::internal::DynamicDispatch in pfor/pfor.cc, the
// same facility every other Arrow SIMD kernel uses, so ARROW_USER_SIMD_LEVEL
// reaches these kernels like it reaches the rest. Before this existed
// ARROW_SIMD_LEVEL alone decided their width, which on a default x86 build meant
// the interleaved kernel ran in XMM registers while the sequential unpacker it
// is usually compared against dispatched to AVX2 at runtime.
//
// Each entry point takes the bit width as a runtime argument and resolves it
// against a table its own translation unit builds. The table entries are
// Arch-discriminated instantiations for the reason
// fastlanes_kernels_internal.h's header comment gives.

#pragma once

#include <cstdint>

namespace arrow {
namespace util {
namespace fastlanes {

// Signatures of the width-templated kernels, which is what the per-width tables
// in interleaved_kernel_table_internal.h hold.
using InterleavedPackFn = void (*)(const uint32_t*, uint32_t*);
using InterleavedUnpackFn = void (*)(const uint32_t*, uint32_t*, uint32_t);

// The baseline leg, compiled at whatever instruction set the build's own flags
// name. It is the dispatch fallback and the only leg guaranteed to exist: a
// build can be configured with no SIMD level at all, and DynamicDispatch
// requires at least one statically available target.
void InterleavedPackBlockBaseline(uint8_t bit_width, const uint32_t* in, uint32_t* out);
void InterleavedUnpackBlockBaseline(uint8_t bit_width, const uint32_t* packed,
                                    uint32_t* out, uint32_t bias);

// The vector legs. Each is declared under exactly the condition CMake uses to
// compile it, because the append_runtime_*_src macros test the runtime macro
// alone; a build that never compiles a leg must never name one either.
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
