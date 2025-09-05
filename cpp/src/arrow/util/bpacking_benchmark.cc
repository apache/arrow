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
constexpr int32_t getNumBytes(int32_t num_values, int32_t bit_width) {
  auto const num_bits = num_values * bit_width;
  if (num_bits % 8 != 0) {
    throw std::invalid_argument("Must pack a multiple of 8 bits.");
  }
  return num_bits / 8;
}

/// Generate random bytes as packed integers.
std::vector<uint8_t> generateRandomPackedValues(int32_t num_values, int32_t bit_width) {
  constexpr uint32_t kSeed = 3214;
  auto const num_bytes = getNumBytes(num_values, bit_width);

  std::vector<uint8_t> out(num_bytes);
  random_bytes(num_bytes, kSeed, out.data());

  return out;
}

const uint8_t* getNextAlignedByte(const uint8_t* ptr, std::size_t alignment) {
  auto addr = reinterpret_cast<std::uintptr_t>(ptr);

  if (addr % alignment == 0) {
    return ptr;
  }

  auto remainder = addr % alignment;
  auto bytes_to_add = alignment - remainder;

  return ptr + bytes_to_add;
}

template <typename Int>
void BM_Unpack(benchmark::State& state, bool aligned, UnpackFunc<Int> unpack) {
  auto const bit_width = static_cast<int32_t>(state.range(0));
  auto const num_values = static_cast<int32_t>(state.range(1));
  constexpr int32_t kExtraValues = sizeof(Int) * 8;

  auto const packed = generateRandomPackedValues(num_values + kExtraValues, bit_width);
  const uint8_t* packed_ptr =
      getNextAlignedByte(packed.data(), sizeof(Int)) + (aligned ? 0 : 1);

  std::vector<Int> unpacked(num_values, 0);

  for (auto _ : state) {
    unpack(packed_ptr, unpacked.data(), num_values, bit_width);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(num_values);
}

constexpr int32_t MIN_RANGE = 64;
constexpr int32_t MAX_RANGE = 32768;
constexpr std::initializer_list<int64_t> kBitWidths32 = {1, 8, 20};
constexpr std::initializer_list<int64_t> kBitWidths64 = {1, 8, 20, 47};
static const std::vector<std::vector<int64_t>> bitWidthsNumValues32 = {
    kBitWidths32,
    benchmark::CreateRange(MIN_RANGE, MAX_RANGE, /*multi=*/8),
};
static const std::vector<std::vector<int64_t>> bitWidthsNumValues64 = {
    kBitWidths64,
    benchmark::CreateRange(MIN_RANGE, MAX_RANGE, /*multi=*/8),
};

BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_default_unaligned, false,
                  unpack32_default)
    ->ArgsProduct(bitWidthsNumValues32);
BENCHMARK_CAPTURE(BM_Unpack<uint64_t>, unpack64_default_unaligned, false,
                  unpack64_default)
    ->ArgsProduct(bitWidthsNumValues64);

#if defined(ARROW_HAVE_RUNTIME_AVX2)
BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_avx2_unaligned, false, unpack32_avx2)
    ->ArgsProduct(bitWidthsNumValues32);
#endif

#if defined(ARROW_HAVE_RUNTIME_AVX512)
BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_avx512_unaligned, false, unpack32_avx512)
    ->ArgsProduct(bitWidthsNumValues32);
#endif

#if defined(ARROW_HAVE_NEON)
BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_neon_unaligned, false, unpack32_neon)
    ->ArgsProduct(bitWidthsNumValues32);
#endif

BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_aligned, true, unpack32)
    ->ArgsProduct(bitWidthsNumValues32);
BENCHMARK_CAPTURE(BM_Unpack<uint32_t>, unpack32_unaligned, false, unpack32)
    ->ArgsProduct(bitWidthsNumValues32);

BENCHMARK_CAPTURE(BM_Unpack<uint64_t>, unpack64_aligned, true, unpack64)
    ->ArgsProduct(bitWidthsNumValues64);
BENCHMARK_CAPTURE(BM_Unpack<uint64_t>, unpack64_unaligned, false, unpack64)
    ->ArgsProduct(bitWidthsNumValues64);

}  // namespace
}  // namespace arrow::internal
