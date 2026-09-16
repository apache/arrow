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

// The per-width dispatch tables, and the body each per-instruction-set
// translation unit wraps under its own name.
//
// Included only by those translation units, never by a caller: it is the file
// whose contents are meant to be compiled more than once, at different flags.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

#include <xsimd/xsimd.hpp>

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/interleaved_dispatch_internal.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace util {
namespace fastlanes {

// A table of function pointers rather than a switch over 32 cases, because the
// table keeps each width's body out of line. Folding all 32 into one dispatch
// function is not free: doing exactly that to Arrow's NEON unpacker cost between
// 1.2x and 2.1x, the compiler having lost the register allocation it finds for a
// body it compiles on its own. Two indirect calls per 1024 values -- one to pick
// the leg, one to pick the width -- are not measurable against that.
//
// Index by bit width: entry w handles width w, and entry 0 is never called
// because a vector of width 0 is constant and never reaches a kernel.
template <typename Arch, size_t... W>
constexpr std::array<InterleavedPackFn, 33> MakePackTable(std::index_sequence<W...>) {
  return {nullptr, &PackBlock<W + 1, Arch>...};
}

template <typename Arch, bool kHasBias, size_t... W>
constexpr std::array<InterleavedUnpackFn, 33> MakeUnpackTable(std::index_sequence<W...>) {
  return {nullptr, &UnpackBlock<W + 1, kHasBias, Arch>...};
}

// Arch defaults to the architecture xsimd derives from the including translation
// unit's own flags, so each leg instantiates a distinct specialization of these
// two templates and of every kernel their tables point at. That is what stops
// the linker from merging two legs into one; xsimd is used for the type and
// nothing else. Verified to differ: xsimd::neon64 at the aarch64 baseline
// against xsimd::detail::sve<128> under -march=armv8-a+sve
// -msve-vector-bits=128.
template <typename Arch = xsimd::default_arch>
void PackLeg(uint8_t bit_width, const uint32_t* in, uint32_t* out) {
  static constexpr auto kTable = MakePackTable<Arch>(std::make_index_sequence<32>{});
  ARROW_DCHECK(bit_width >= 1 && bit_width <= 32);
  kTable[bit_width](in, out);
}

// The frame of reference is folded into the kernel's own store, so a non-zero
// frame costs no second pass over the output. Which of the two tables is used is
// decided here rather than inside the kernel so that the no-bias instantiations
// carry no test in their inner loop.
template <typename Arch = xsimd::default_arch>
void UnpackLeg(uint8_t bit_width, const uint32_t* packed, uint32_t* out, uint32_t bias) {
  static constexpr auto kUnpack =
      MakeUnpackTable<Arch, /*kHasBias=*/false>(std::make_index_sequence<32>{});
  static constexpr auto kUnpackBias =
      MakeUnpackTable<Arch, /*kHasBias=*/true>(std::make_index_sequence<32>{});
  ARROW_DCHECK(bit_width >= 1 && bit_width <= 32);
  if (bias == 0) {
    kUnpack[bit_width](packed, out, 0);
  } else {
    kUnpackBias[bit_width](packed, out, bias);
  }
}

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
