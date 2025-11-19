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

#include <stdexcept>
#include <vector>

#include <benchmark/benchmark.h>

#include "arrow/testing/util.h"
#include "arrow/util/bpacking_internal.h"

#if defined(ARROW_HAVE_RUNTIME_AVX2)
#  include "arrow/util/bpacking_avx2_internal.h"
#  include "arrow/util/cpu_info.h"
#endif
#if defined(ARROW_HAVE_RUNTIME_AVX512)
#  include "arrow/util/bpacking_avx512_internal.h"
#endif
#if defined(ARROW_HAVE_NEON)
#  include "arrow/util/bpacking_neon_internal.h"
#endif

namespace arrow::internal {
namespace {

template <typename Int>
using UnpackFunc = int (*)(const uint8_t*, Int*, int, int);

/// Get the number of bytes associate with a packing.
constexpr int32_t GetNumBytes(int32_t num_values, int32_t bit_width) {
  const auto num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    throw std::invalid_argument("Must pack a multiple of 8 bits.");
  }
  return num_bits / 8;
}

/// Generate random bytes as packed integers.
std::vector<uint8_t> GenerateRandomPackedValues(int32_t num_values, int32_t bit_width) {
  constexpr uint32_t kSeed = 3214;
  const auto num_bytes = GetNumBytes(num_values, bit_width);

  std::vector<uint8_t> out(num_bytes);
  random_bytes(num_bytes, kSeed, out.data());

  return out;
}

const uint8_t* GetNextAlignedByte(const uint8_t* ptr, std::size_t alignment) {
  auto addr = reinterpret_cast<std::uintptr_t>(ptr);

  if (addr % alignment == 0) {
    return ptr;
  }

  auto remainder = addr % alignment;
  auto bytes_to_add = alignment - remainder;

  return ptr + bytes_to_add;
}

template <typename Int>
void BM_Unpack(benchmark::State& state, bool aligned, UnpackFunc<Int> unpack, bool skip,
               std::string skip_msg) {
  if (skip) {
    state.SkipWithMessage(skip_msg);
  }

  const auto bit_width = static_cast<int32_t>(state.range(0));
  const auto num_values = static_cast<int32_t>(state.range(1));

  // Assume std::vector allocation is likely be aligned for greater than a byte.
  // So we allocate more values than necessary and skip to the next byte with the
  // desired (non) alignment to test the proper condition.
  constexpr int32_t kExtraValues = sizeof(Int) * 8;
  const auto packed = GenerateRandomPackedValues(num_values + kExtraValues, bit_width);
  const uint8_t* packed_ptr =
      GetNextAlignedByte(packed.data(), sizeof(Int)) + (aligned ? 0 : 1);

  std::vector<Int> unpacked(num_values, 0);

  for (auto _ : state) {
    unpack(packed_ptr, unpacked.data(), num_values, bit_width);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(num_values * state.iterations());
}

constexpr int32_t kMinRange = 64;
constexpr int32_t kMaxRange = 32768;
constexpr std::initializer_list<int64_t> kBitWidths32 = {1, 2, 8, 20};
constexpr std::initializer_list<int64_t> kBitWidths64 = {1, 2, 8, 20, 47};
static const std::vector<std::vector<int64_t>> kBitWidthsNumValues32 = {
    kBitWidths32,
    benchmark::CreateRange(kMinRange, kMaxRange, /*multi=*/32),
};
static const std::vector<std::vector<int64_t>> kBitWidthsNumValues64 = {
    kBitWidths64,
    benchmark::CreateRange(kMinRange, kMaxRange, /*multi=*/32),
};

/// Nudge for MSVC template inside BENCHMARK_CAPTURE macro.
void BM_UnpackUint32(benchmark::State& state, bool aligned, UnpackFunc<uint32_t> unpack,
                     bool skip = false, std::string skip_msg = "") {
  return BM_Unpack<uint32_t>(state, aligned, unpack, skip, std::move(skip_msg));
}
/// Nudge for MSVC template inside BENCHMARK_CAPTURE macro.
void BM_UnpackUint64(benchmark::State& state, bool aligned, UnpackFunc<uint64_t> unpack,
                     bool skip = false, std::string skip_msg = "") {
  return BM_Unpack<uint64_t>(state, aligned, unpack, skip, std::move(skip_msg));
}

BENCHMARK_CAPTURE(BM_UnpackUint32, ScalarUnaligned, false, unpack32_scalar)
    ->ArgsProduct(kBitWidthsNumValues32);
BENCHMARK_CAPTURE(BM_UnpackUint64, ScalarUnaligned, false, unpack64_scalar)
    ->ArgsProduct(kBitWidthsNumValues64);

#if defined(ARROW_HAVE_RUNTIME_AVX2)
BENCHMARK_CAPTURE(BM_UnpackUint32, Avx2Unaligned, false, unpack32_avx2,
                  !CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX2),
                  "Avx2 not available")
    ->ArgsProduct(kBitWidthsNumValues32);
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
BENCHMARK_CAPTURE(BM_UnpackUint32, Avx512Unaligned, false, unpack32_avx512,
                  !CpuInfo::GetInstance()->IsSupported(CpuInfo::AVX512),
                  "Avx512 not available")
    ->ArgsProduct(kBitWidthsNumValues32);
#endif

#if defined(ARROW_HAVE_NEON)
BENCHMARK_CAPTURE(BM_UnpackUint32, NeonUnaligned, false, unpack32_neon)
    ->ArgsProduct(kBitWidthsNumValues32);
#endif

BENCHMARK_CAPTURE(BM_UnpackUint32, DynamicAligned, true, unpack32)
    ->ArgsProduct(kBitWidthsNumValues32);
BENCHMARK_CAPTURE(BM_UnpackUint32, DynamicUnaligned, false, unpack32)
    ->ArgsProduct(kBitWidthsNumValues32);

BENCHMARK_CAPTURE(BM_UnpackUint64, DynamicAligned, true, unpack64)
    ->ArgsProduct(kBitWidthsNumValues64);
BENCHMARK_CAPTURE(BM_UnpackUint64, DynamicUnaligned, false, unpack64)
    ->ArgsProduct(kBitWidthsNumValues64);

}  // namespace
}  // namespace arrow::internal
