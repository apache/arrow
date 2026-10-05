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

#include "benchmark/benchmark.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <type_traits>
#include <utility>

#include <xsimd/xsimd.hpp>

#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_internal.h"
#include "arrow/util/bpacking_scalar_internal.h"
#include "arrow/util/bpacking_simd_kernel_internal.h"
#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"
#include "arrow/util/fastlanes/transposed_delta.h"
#include "arrow/util/macros.h"

// Times the interleaved block decoder against Arrow's sequential bit-unpack
// paths at a fixed decoded footprint. Both layouts read w bits per value and
// write whole elements, so a block of 1024 values is 128 * w packed bytes in
// either one and a ratio between two rows compares layouts, not data volume.
// Values decode into the narrowest whole byte that holds w bits, as a reader
// filling a fixed-width column would choose.

namespace arrow::util::fastlanes {

namespace {

// Arrow's sequential decoders may read a whole word past the last value they
// need, so every packed buffer carries this much slack past its last byte.
constexpr size_t kPackedTailSlack = 64;

// The narrowest whole-byte element that holds a w-bit value.
template <uint32_t w>
using NextWholeByte =
    std::conditional_t<(w <= 8), uint8_t,
                       std::conditional_t<(w <= 16), uint16_t, uint32_t>>;

// Buffers are aligned the way Arrow aligns a column's buffers. The interleaved
// decoder stores a whole register at a time, and on a destination that is not
// register-aligned each store straddles two cache lines, which halves its
// throughput and makes a row depend on earlier allocations.
constexpr size_t kBufferAlignment = 64;

template <typename T>
struct AlignedFree {
  void operator()(T* p) const { std::free(p); }
};

template <typename T>
using AlignedArray = std::unique_ptr<T[], AlignedFree<T>>;

template <typename T>
AlignedArray<T> AllocateAligned(size_t num_elements) {
  const size_t bytes = (num_elements * sizeof(T) + kBufferAlignment - 1) /
                       kBufferAlignment * kBufferAlignment;
  auto* p = static_cast<T*>(std::aligned_alloc(kBufferAlignment, bytes));
  if (p == nullptr) std::abort();
  return AlignedArray<T>(p);
}

// Packed bytes are random: both decoders are data-independent at a fixed
// width, and round trips are covered by fastlanes_kernels_test.cc.
template <typename T>
void RandomFill(T* data, size_t n) {
  std::mt19937_64 rng(42);
  auto* bytes = reinterpret_cast<uint8_t*>(data);
  const size_t total = n * sizeof(T);
  size_t i = 0;
  for (; i + sizeof(uint64_t) <= total; i += sizeof(uint64_t)) {
    const uint64_t v = rng();
    std::memcpy(bytes + i, &v, sizeof(v));
  }
  for (; i < total; ++i) bytes[i] = static_cast<uint8_t>(rng());
}

// The sequential decoders are called once per 16384 values rather than once
// over the footprint, because batch_size is an int that the unpacker multiplies
// by the bit width. At 16384 values a call runs at 0.95x to 1.00x of a single
// call over the same data, against 0.58x at 1024 values, so the page size does
// not handicap the sequential side.
constexpr size_t kSequentialPageValues = 16384;

// Kept out of line for the same reason as DecodePage below.
template <typename T, uint32_t w, typename Fn>
ARROW_NOINLINE void DecodePagesSequential(const uint8_t* packed, T* out,
                                          size_t num_values, size_t page_values,
                                          Fn&& decode) {
  // The last page is short when the footprint is not a whole number of pages.
  ::arrow::internal::UnpackOptions opts{.batch_size = static_cast<int>(page_values),
                                        .bit_width = static_cast<int>(w)};
  for (size_t v = 0; v < num_values; v += page_values) {
    const size_t n = std::min(page_values, num_values - v);
    opts.batch_size = static_cast<int>(n);
    decode(packed + v * w / 8, out + v, opts);
  }
}

// The dispatched entry point libarrow ships.
template <typename T, uint32_t w>
void BM_Sequential(benchmark::State& state, size_t out_bytes) {
  const size_t num_values = out_bytes / sizeof(T) / kBlockSize * kBlockSize;
  const size_t page_values = std::min(kSequentialPageValues, num_values);
  const size_t packed_bytes = num_values * w / 8;
  auto packed = AllocateAligned<uint8_t>(packed_bytes + kPackedTailSlack);
  RandomFill(packed.get(), packed_bytes + kPackedTailSlack);
  auto out = AllocateAligned<T>(num_values);
  const auto decode = [](const uint8_t* src, T* dst,
                         const ::arrow::internal::UnpackOptions& o) {
    ::arrow::internal::unpack<T>(src, dst, o);
  };

  for (auto _ : state) {
    DecodePagesSequential<T, w>(packed.get(), out.get(), num_values, page_values, decode);
    benchmark::DoNotOptimize(out[0]);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(num_values * sizeof(T)) *
                          state.iterations());
  state.SetItemsProcessed(static_cast<int64_t>(num_values) * state.iterations());
}

#if defined(ARROW_HAVE_AVX2)
// Arrow's vector kernel compiled in this file with the instruction set pinned
// to AVX2. Under ARROW_SIMD_LEVEL=AVX512 the dispatched entry point is built
// with AVX-512 flags even in bpacking_simd_256.cc, and that code assembles
// vectors a byte at a time, which bpacking.cc already gives as its reason not
// to dispatch to AVX-512. The pin keeps that cost out of a layout comparison.
// It does not cover the code the compiler vectorizes around the kernel, which
// still follows this file's flags.
template <typename T, int w>
using Avx2Kernel = ::arrow::internal::bpacking::Kernel<T, w, xsimd::avx2>;

template <typename T, uint32_t w>
void BM_SequentialSimd(benchmark::State& state, size_t out_bytes) {
  const size_t num_values = out_bytes / sizeof(T) / kBlockSize * kBlockSize;
  const size_t page_values = std::min(kSequentialPageValues, num_values);
  const size_t packed_bytes = num_values * w / 8;
  auto packed = AllocateAligned<uint8_t>(packed_bytes + kPackedTailSlack);
  RandomFill(packed.get(), packed_bytes + kPackedTailSlack);
  auto out = AllocateAligned<T>(num_values);
  const auto decode = [](const uint8_t* src, T* dst,
                         const ::arrow::internal::UnpackOptions& o) {
    ::arrow::internal::bpacking::unpack_width<w, Avx2Kernel, false>(
        src, dst, o.batch_size, o.bit_offset, o.max_read_bytes, T{});
  };

  for (auto _ : state) {
    DecodePagesSequential<T, w>(packed.get(), out.get(), num_values, page_values, decode);
    benchmark::DoNotOptimize(out[0]);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(num_values * sizeof(T)) *
                          state.iterations());
  state.SetItemsProcessed(static_cast<int64_t>(num_values) * state.iterations());
}
#endif  // ARROW_HAVE_AVX2

// The scalar kernel, called directly. At some bit widths the dispatched kernel
// is slower than this one.
template <typename T, uint32_t w>
void BM_SequentialScalar(benchmark::State& state, size_t out_bytes) {
  const size_t num_values = out_bytes / sizeof(T) / kBlockSize * kBlockSize;
  const size_t page_values = std::min(kSequentialPageValues, num_values);
  const size_t packed_bytes = num_values * w / 8;
  auto packed = AllocateAligned<uint8_t>(packed_bytes + kPackedTailSlack);
  RandomFill(packed.get(), packed_bytes + kPackedTailSlack);
  auto out = AllocateAligned<T>(num_values);
  const auto decode = [](const uint8_t* src, T* dst,
                         const ::arrow::internal::UnpackOptions& o) {
    ::arrow::internal::bpacking::unpack_scalar<T>(src, dst, o);
  };

  for (auto _ : state) {
    DecodePagesSequential<T, w>(packed.get(), out.get(), num_values, page_values, decode);
    benchmark::DoNotOptimize(out[0]);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(num_values * sizeof(T)) *
                          state.iterations());
  state.SetItemsProcessed(static_cast<int64_t>(num_values) * state.iterations());
}

// The decode loop gets its own out-of-line function per element and bit width.
// Left inside the registered lambdas, the loops share one inlining budget, and
// the widths registered last compile to a partly scalar loop several times
// slower than the same kernel on its own. The cost is one call per page, which
// the sequential decoder pays as well.
template <typename T, uint32_t w>
ARROW_NOINLINE void DecodePage(const T* packed, T* out, size_t num_blocks) {
  using G = BlockGeometry<T>;
  for (size_t b = 0; b < num_blocks; ++b) {
    UnpackBlock<T, w, false>(packed + b * w * G::kLanes, out + b * kBlockSize);
  }
}

// The same block permuted from the FastLanes lane assignment (kFlOrderRaw) back
// to file order. Only data packed with that assignment, such as transposed
// delta, pays this; a grid filled in input order (kFileOrder, and the
// kForBitPackInterleaved page mode) already decodes in file order. The fused
// kernel permutes in registers, and the fallback goes through a scratch grid as
// InterleavedPforDecode does. Both are 32-bit only.
template <uint32_t w>
ARROW_NOINLINE void DecodePageFileOrder(const uint32_t* packed, int32_t* out,
                                        size_t num_blocks) {
  using G = BlockGeometry<uint32_t>;
#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
  for (size_t b = 0; b < num_blocks; ++b) {
    UnpackBlockFlToFileOrder<w, false>(packed + b * w * G::kLanes, out + b * kBlockSize);
  }
#else
  uint32_t scratch[kBlockSize];
  for (size_t b = 0; b < num_blocks; ++b) {
    UnpackBlock<uint32_t, w, false>(packed + b * w * G::kLanes, scratch);
    Transpose32x32(scratch, out + b * kBlockSize);
  }
#endif
}

template <typename T, uint32_t w>
void BM_Interleaved(benchmark::State& state, size_t out_bytes) {
  using G = BlockGeometry<T>;
  const size_t num_values = out_bytes / sizeof(T);
  const size_t num_blocks = num_values / kBlockSize;
  const size_t packed_elements = num_blocks * w * G::kLanes;
  auto packed = AllocateAligned<T>(packed_elements);
  RandomFill(packed.get(), packed_elements);
  auto out = AllocateAligned<T>(num_values);

  for (auto _ : state) {
    DecodePage<T, w>(packed.get(), out.get(), num_blocks);
    benchmark::DoNotOptimize(out[0]);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(out_bytes) * state.iterations());
  state.SetItemsProcessed(static_cast<int64_t>(num_values) * state.iterations());
}

template <uint32_t w>
void BM_InterleavedFileOrder(benchmark::State& state, size_t out_bytes) {
  using G = BlockGeometry<uint32_t>;
  const size_t num_values = out_bytes / sizeof(uint32_t);
  const size_t num_blocks = num_values / kBlockSize;
  const size_t packed_elements = num_blocks * w * G::kLanes;
  auto packed = AllocateAligned<uint32_t>(packed_elements);
  RandomFill(packed.get(), packed_elements);
  auto out = AllocateAligned<int32_t>(num_values);

  for (auto _ : state) {
    DecodePageFileOrder<w>(packed.get(), out.get(), num_blocks);
    benchmark::DoNotOptimize(out[0]);
    benchmark::ClobberMemory();
  }
  state.SetBytesProcessed(static_cast<int64_t>(out_bytes) * state.iterations());
  state.SetItemsProcessed(static_cast<int64_t>(num_values) * state.iterations());
}

std::string Label(const char* decoder, uint32_t w, size_t element_bits,
                  const char* pinned, size_t pinned_kib) {
  return std::string("bitunpack/") + decoder + "/w=" + std::to_string(w) + "/u" +
         std::to_string(element_bits) + "/" + pinned + "=" + std::to_string(pinned_kib) +
         "KiB";
}

// A width needs one registration per decoder, and w has to be a constant for
// the interleaved kernel, so the widths are walked at compile time.
template <uint32_t w>
void RegisterWidth(size_t out_bytes, const char* pinned, size_t pinned_kib) {
  using T = NextWholeByte<w>;
  constexpr size_t kBits = sizeof(T) * 8;
  // A block is the decode unit, so a footprint below one block of this element
  // width has nothing to measure.
  if (out_bytes / sizeof(T) < kBlockSize) return;

#if defined(ARROW_HAVE_AVX2)
  benchmark::RegisterBenchmark(
      Label("seq_simd", w, kBits, pinned, pinned_kib),
      [out_bytes](benchmark::State& st) { BM_SequentialSimd<T, w>(st, out_bytes); });
#endif
  benchmark::RegisterBenchmark(
      Label("seq_lib", w, kBits, pinned, pinned_kib),
      [out_bytes](benchmark::State& st) { BM_Sequential<T, w>(st, out_bytes); });
  benchmark::RegisterBenchmark(
      Label("seq_scal", w, kBits, pinned, pinned_kib),
      [out_bytes](benchmark::State& st) { BM_SequentialScalar<T, w>(st, out_bytes); });
  if constexpr (std::is_same_v<T, uint32_t>) {
    benchmark::RegisterBenchmark(
        Label("interleaved_file_order", w, kBits, pinned, pinned_kib),
        [out_bytes](benchmark::State& st) { BM_InterleavedFileOrder<w>(st, out_bytes); });
  }
  benchmark::RegisterBenchmark(
      Label("interleaved_fl_order", w, kBits, pinned, pinned_kib),
      [out_bytes](benchmark::State& st) { BM_Interleaved<T, w>(st, out_bytes); });
}

template <uint32_t... Ws>
void RegisterAllWidths(size_t out_bytes, std::integer_sequence<uint32_t, Ws...>) {
  (RegisterWidth<Ws + 1>(out_bytes, "out", out_bytes / 1024), ...);
}

// The width sweep decodes 16 KiB, so destination plus packed source stays under
// 32 KiB at every width and fits in a 48 KiB L1 data cache.
constexpr size_t kWidthSweepOutBytes = 16 * 1024;

// Decoded sizes for the footprint sweep, from inside L1 to 4 MiB, which is
// already larger than a Parquet page. They are not labelled by cache level
// because the packed source adds w bits per value on top, so the level a row
// reaches depends on the width as well.
constexpr size_t kFootprintKiB[] = {16, 32, 48, 64, 128, 256, 512, 1024, 2048, 4096};

template <uint32_t... Ws>
void RegisterFootprintWidths(size_t out_bytes, size_t kib) {
  (RegisterWidth<Ws>(out_bytes, "out", kib), ...);
}

// The widths the footprint sweep uses, and the column each one stands for. Each
// output size gets a width where Arrow's sequential kernel is fast and, for u8
// and u16, one where it is slow, so a row is not mistaken for a property of the
// element size.
//
//    3 -> u8    a status enum, or a dictionary of eight or fewer values
//    4 -> u8    the same column at a width the sequential kernel handles well
//    7 -> u8    a dictionary of up to 128 values
//   11 -> u16   a dictionary of a couple of thousand values
//   12 -> u16   the same column at a width the sequential kernel handles well
//   14 -> u16   a date held as a day offset, or a dictionary near 16k
//   18 -> u32   a key offset by its row group minimum
//   21 -> u32   the same column three bits wider
//
// No width equals its output width, where the decode is a copy, not an unpack.
void RegisterFootprintSweep() {
  for (size_t kib : kFootprintKiB) {
    // The width sweep already registers this size under the same names.
    if (kib * 1024 == kWidthSweepOutBytes) continue;
    RegisterFootprintWidths<3, 4, 7, 11, 12, 14, 18, 21>(kib * 1024, kib);
  }
}

void RegisterBenchmarks() {
  RegisterAllWidths(kWidthSweepOutBytes, std::make_integer_sequence<uint32_t, 32>{});
  RegisterFootprintSweep();
}

}  // namespace

}  // namespace arrow::util::fastlanes

int main(int argc, char** argv) {
  arrow::util::fastlanes::RegisterBenchmarks();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
