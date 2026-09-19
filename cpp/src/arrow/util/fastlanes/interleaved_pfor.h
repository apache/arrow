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

// ---------------------------------------------------------------------------
// Plain (non-delta) PFOR through the FastLanes container, in two value orders:
// input order, and the paper's lane assignment. Everything else is held equal
// -- same per-block frame of reference, same bit width choice, same container
// -- so the difference between them is the ordering and nothing else, which is
// the one question this file answers. lane_delta.h and transposed_delta.h ask
// that question of a delta chain; plain PFOR has no chain for the ordering to
// help, so what is priced here is the permutation FL_ORDER forces. See
// InterleavedPforOrder below for the three variants.
//
// There is no exception handling here: no patch list on the wire and no patch
// pass in the decoder, because a bit width wide enough for the block's largest
// residual is always chosen. Arrow's production PFOR does carry
// exceptions and does patch them, so a ratio taken between anything here and a
// production decoder charges one side for work the other never does, and the
// difference is not the layout. Compare kFileOrder against kFlOrder/kFlOrderRaw
// here; for sequential against interleaved, use the two production paths that
// differ only in PackingMode. bench_groups.sh states the rule and groups the
// benchmarks by it.
//
// The container also appears in production as
// PackingMode::kForBitPackInterleaved, which is what a Parquet reader would
// use. This file is not that code and does not share its wire format: the
// production format has no lane-assignment mode, so the ordering question has
// nowhere else to be asked.
//
// On register width: UnpackBlock in fastlanes_kernels_internal.h contains no
// intrinsics, so its register width and its optimization level are whatever the
// translation unit that compiled it was given. The production path in
// pfor/pfor.cc picks its instruction set at runtime; the code here deliberately
// does not, instantiating the template into the benchmark's own translation
// unit so that every measurement is built at one known set of flags. The cost
// is that it answers to ARROW_SIMD_LEVEL rather than ARROW_USER_SIMD_LEVEL, and
// that a default x86 build (SSE4_2) runs it in XMM registers. Build with
// -DARROW_SIMD_LEVEL=AVX2 or better before comparing against a dispatched
// decoder, and state the level with the figure.
//
// Wire layout for n values (n a multiple of 1024; any tail is stored raw):
//
//   [0, nblocks)                 uint8 bit width per block
//   padded to a 4-byte boundary
//   next 4 * nblocks bytes       int32 min (frame of reference) per block
//   remainder                    per block, bit_width * 32 uint32 packed words
// ---------------------------------------------------------------------------

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/transposed_delta.h"

namespace arrow {
namespace util {
namespace fastlanes {

// kFileOrder   pack in input order, decode straight back to input order.
// kFlOrderRaw  pack with the paper's lane assignment -- lane l holds the
//              contiguous run [32l, 32l+32), reusing transposed_delta.h's
//              Transpose32x32 -- and hand that order to the caller unchanged,
//              for a consumer that does not care about value order. It runs the
//              same PackBlock/UnpackBlock as kFileOrder against a grid that was
//              merely filled differently at encode time, so its decode cost
//              must come out equal to kFileOrder's; that it does is a check on
//              the harness.
// kFlOrder     the same wire bytes as kFlOrderRaw, but decode restores input
//              order. This is what prices FL_ORDER's permutation.
enum class InterleavedPforOrder { kFileOrder, kFlOrderRaw, kFlOrder };

inline uint32_t InterleavedPforBitsFor(uint64_t v) {
  uint32_t bits = 0;
  while (v != 0) {
    ++bits;
    v >>= 1;
  }
  return bits;
}

inline size_t InterleavedPforHeaderSize(size_t n) {
  const size_t nblocks = n / kBlockSize;
  const size_t widths = (nblocks + 3) & ~size_t(3);
  return widths + nblocks * sizeof(int32_t);
}

inline size_t InterleavedPforMaxEncodedSize(size_t n) {
  return InterleavedPforHeaderSize(n) + n * sizeof(uint32_t) +
         (n % kBlockSize) * sizeof(int32_t);
}

#define ILP_PACK_CASE(W)     \
  case W:                    \
    PackBlock<W>(grid, dst); \
    break;

#define ILP_UNPACK_CASE(W)                \
  case W:                                 \
    UnpackBlock<W, true>(src, dst, bias); \
    break;

#define ILP_UNPACK_SCRATCH_CASE(W)            \
  case W:                                     \
    UnpackBlock<W, true>(src, scratch, bias);  \
    break;

#define ILP_UNPACK_FUSED_CASE(W)                            \
  case W:                                                   \
    UnpackBlockFlToFileOrder<W, true>(src, out_blk, bias);   \
    break;

#define ILP_WIDTH_CASES(M)                                                          \
  M(1)                                                                              \
  M(2) M(3) M(4) M(5) M(6) M(7) M(8) M(9) M(10) M(11) M(12) M(13) M(14) M(15) M(16) \
      M(17) M(18) M(19) M(20) M(21) M(22) M(23) M(24) M(25) M(26) M(27) M(28) M(29) \
          M(30) M(31) M(32)

// ---------------------------------------------------------------------------
// Fused FL_ORDER unpack: packed words -> file order in a single pass.
//
// The naive FL_ORDER decode is UnpackBlock into a 4 KiB stack grid followed by
// Transpose32x32 out of it. That materializes the grid in memory purely to read
// it straight back: 128 extra 32-byte stores and 128 extra loads per block, on
// top of the 128 stores the output actually needs. The permutation itself is
// unavoidable -- FL_ORDER exists to place values where the delta chain wants
// them, and file order has to be restored for anyone who needs it -- but the
// round trip through memory is not.
//
// Folding the transpose into the *store addressing* of the row-major kernel is
// the other obvious idea and it is a trap: out[lane * 32 + row] means the 32
// lanes of one row land 128 bytes apart, so each contiguous 32-byte store
// becomes eight 4-byte scatters. That trades 128 stores per block for 1024.
//
// What works is doing the transpose in registers, which is what
// Transpose32x32Avx2 already does -- it just does it on a grid that was written
// to memory first. Here the same 8x8 register block is filled directly by the
// unpack, so the grid never exists:
//
//   for each 8-lane slice lb, for each 8-row block rb:
//     eight registers <- unpack rows rb..rb+7, lanes lb..lb+7   (8-16 loads)
//     the unpacklo/unpackhi/permute2x128 ladder                 (24 shuffles)
//     eight 32-byte stores across out[lb..lb+7][rb..rb+7]       (8 stores)
//
// Load and store counts per block are then identical to the file-order kernel's
// (each packed word read once, each output value written once); the only thing
// FL_ORDER pays over file order is the shuffle ladder. lb is the outer loop on
// purpose: a fixed lb writes out[lb * 32, lb * 32 + 256), one contiguous 1 KiB
// run, whereas rb outer would stride 128 bytes across the whole 4 KiB block.
// ---------------------------------------------------------------------------
#ifdef ARROW_TRANSPOSED_DELTA_AVX2

namespace internal {

// FlUnpackRowSlice lives in transposed_delta.h: the fused delta kernel there
// needs it too, and that header is upstream of this one.

// One 8x8 tile: unpack eight rows' worth of an 8-lane slice, transpose in
// registers, store across eight output rows. The ladder is the same one
// Transpose32x32Avx2 uses; only the source of r0..r7 differs.
template <uint32_t w, bool kHasBias, uint32_t rb>
ARROW_FORCE_INLINE void FlUnpackTile(const uint32_t* ARROW_RESTRICT packed,
                                     int32_t* ARROW_RESTRICT out, size_t lb,
                                     __m256i vmask, __m256i vbias) {
  const __m256i r0 = FlUnpackRowSlice<w, kHasBias, rb + 0>(packed, lb, vmask, vbias);
  const __m256i r1 = FlUnpackRowSlice<w, kHasBias, rb + 1>(packed, lb, vmask, vbias);
  const __m256i r2 = FlUnpackRowSlice<w, kHasBias, rb + 2>(packed, lb, vmask, vbias);
  const __m256i r3 = FlUnpackRowSlice<w, kHasBias, rb + 3>(packed, lb, vmask, vbias);
  const __m256i r4 = FlUnpackRowSlice<w, kHasBias, rb + 4>(packed, lb, vmask, vbias);
  const __m256i r5 = FlUnpackRowSlice<w, kHasBias, rb + 5>(packed, lb, vmask, vbias);
  const __m256i r6 = FlUnpackRowSlice<w, kHasBias, rb + 6>(packed, lb, vmask, vbias);
  const __m256i r7 = FlUnpackRowSlice<w, kHasBias, rb + 7>(packed, lb, vmask, vbias);

  const __m256i t0 = _mm256_unpacklo_epi32(r0, r1);
  const __m256i t1 = _mm256_unpackhi_epi32(r0, r1);
  const __m256i t2 = _mm256_unpacklo_epi32(r2, r3);
  const __m256i t3 = _mm256_unpackhi_epi32(r2, r3);
  const __m256i t4 = _mm256_unpacklo_epi32(r4, r5);
  const __m256i t5 = _mm256_unpackhi_epi32(r4, r5);
  const __m256i t6 = _mm256_unpacklo_epi32(r6, r7);
  const __m256i t7 = _mm256_unpackhi_epi32(r6, r7);

  const __m256i u0 = _mm256_unpacklo_epi64(t0, t2);
  const __m256i u1 = _mm256_unpackhi_epi64(t0, t2);
  const __m256i u2 = _mm256_unpacklo_epi64(t1, t3);
  const __m256i u3 = _mm256_unpackhi_epi64(t1, t3);
  const __m256i u4 = _mm256_unpacklo_epi64(t4, t6);
  const __m256i u5 = _mm256_unpackhi_epi64(t4, t6);
  const __m256i u6 = _mm256_unpacklo_epi64(t5, t7);
  const __m256i u7 = _mm256_unpackhi_epi64(t5, t7);

  int32_t* dst = out + lb * kRowsPerBlock + rb;
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 0 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u0, u4, 0x20));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 1 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u1, u5, 0x20));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 2 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u2, u6, 0x20));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 3 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u3, u7, 0x20));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 4 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u0, u4, 0x31));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 5 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u1, u5, 0x31));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 6 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u2, u6, 0x31));
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + 7 * kRowsPerBlock),
                      _mm256_permute2x128_si256(u3, u7, 0x31));
}

}  // namespace internal

#define ARROW_FASTLANES_FUSED_FL_UNPACK 1

// Unpacks a block that was packed in FL_ORDER and writes it in file order,
// adding `bias` on the way through. Equivalent to UnpackBlock<w, true> into a
// scratch grid followed by Transpose32x32, without the grid.
template <uint32_t w, bool kHasBias>
inline void UnpackBlockFlToFileOrder(const uint32_t* ARROW_RESTRICT packed,
                                     int32_t* ARROW_RESTRICT out, uint32_t bias = 0) {
  static_assert(w >= 1 && w <= 32);
  constexpr uint32_t kMask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);
  const __m256i vmask = _mm256_set1_epi32(static_cast<int>(kMask));
  const __m256i vbias = _mm256_set1_epi32(static_cast<int>(bias));

  for (size_t lb = 0; lb < kLanes; lb += 8) {
    internal::FlUnpackTile<w, kHasBias, 0>(packed, out, lb, vmask, vbias);
    internal::FlUnpackTile<w, kHasBias, 8>(packed, out, lb, vmask, vbias);
    internal::FlUnpackTile<w, kHasBias, 16>(packed, out, lb, vmask, vbias);
    internal::FlUnpackTile<w, kHasBias, 24>(packed, out, lb, vmask, vbias);
  }
}

#endif  // ARROW_TRANSPOSED_DELTA_AVX2

// Returns bytes written, or 0 if some block's span exceeds 32 bits (it never
// does for plain int32 values: max - min fits in 32 bits unconditionally).
template <InterleavedPforOrder kOrder>
inline size_t InterleavedPforEncode(const int32_t* in, size_t n, uint8_t* out) {
  const size_t nblocks = n / kBlockSize;

  uint8_t* widths = out;
  int32_t* mins = reinterpret_cast<int32_t*>(out + ((nblocks + 3) & ~size_t(3)));
  uint32_t* dst = reinterpret_cast<uint32_t*>(out + InterleavedPforHeaderSize(n));

  uint32_t grid[kBlockSize];

  for (size_t b = 0; b < nblocks; ++b) {
    const int32_t* blk = in + b * kBlockSize;

    int64_t vmin = std::numeric_limits<int64_t>::max();
    int64_t vmax = std::numeric_limits<int64_t>::min();
    for (size_t t = 0; t < kBlockSize; ++t) {
      const int64_t v = static_cast<int64_t>(blk[t]);
      if (v < vmin) vmin = v;
      if (v > vmax) vmax = v;
    }
    const uint32_t w = InterleavedPforBitsFor(static_cast<uint64_t>(vmax - vmin));
    widths[b] = static_cast<uint8_t>(w);
    mins[b] = static_cast<int32_t>(vmin);

    if (w > 0) {
      if constexpr (kOrder == InterleavedPforOrder::kFileOrder) {
        for (size_t t = 0; t < kBlockSize; ++t) {
          grid[t] = static_cast<uint32_t>(static_cast<int64_t>(blk[t]) - vmin);
        }
      } else {
        // grid[row * kLanes + lane] takes the residual of the value at
        // original position lane * kRowsPerBlock + row, the same placement
        // TransposedDeltaEncode uses -- so lane l ends up holding the
        // contiguous run [32l, 32l+32) once packed.
        for (size_t lane = 0; lane < kLanes; ++lane) {
          const size_t run = lane * kRowsPerBlock;
          for (size_t row = 0; row < kRowsPerBlock; ++row) {
            grid[row * kLanes + lane] =
                static_cast<uint32_t>(static_cast<int64_t>(blk[run + row]) - vmin);
          }
        }
      }
      switch (w) {
        ILP_WIDTH_CASES(ILP_PACK_CASE)
        default:
          return 0;
      }
      dst += w * kLanes;
    }
  }

  for (size_t b = nblocks; b < ((nblocks + 3) & ~size_t(3)); ++b) widths[b] = 0;

  uint8_t* end = reinterpret_cast<uint8_t*>(dst);
  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(end, in + nblocks * kBlockSize, tail * sizeof(int32_t));
    end += tail * sizeof(int32_t);
  }
  return static_cast<size_t>(end - out);
}

template <InterleavedPforOrder kOrder>
inline void InterleavedPforDecode(const uint8_t* in, size_t n, int32_t* out) {
  const size_t nblocks = n / kBlockSize;

  const uint8_t* widths = in;
  const int32_t* mins =
      reinterpret_cast<const int32_t*>(in + ((nblocks + 3) & ~size_t(3)));
  const uint32_t* src =
      reinterpret_cast<const uint32_t*>(in + InterleavedPforHeaderSize(n));

  // Only kFlOrder can need a scratch grid, and only where the fused kernel is
  // unavailable: kFileOrder owes no permutation, kFlOrderRaw hands the
  // FastLanes order to the caller as-is, and where UnpackBlockFlToFileOrder
  // exists the permutation happens in registers.
#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
  constexpr bool kNeedsScratch = false;
#else
  constexpr bool kNeedsScratch = (kOrder == InterleavedPforOrder::kFlOrder);
#endif
  uint32_t scratch[kNeedsScratch ? kBlockSize : 1];
  ARROW_UNUSED(scratch);

  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = widths[b];
    const uint32_t bias = static_cast<uint32_t>(mins[b]);
    int32_t* out_blk = out + b * kBlockSize;

    if (w == 0) {
      // Every value in the block is the bias, so the FL_ORDER permutation is the
      // identity on it: a constant grid transposes to itself. All three orders
      // fill the output directly and none touches the scratch.
      for (size_t t = 0; t < kBlockSize; ++t) out_blk[t] = static_cast<int32_t>(bias);
      continue;
    }

    if constexpr (kOrder == InterleavedPforOrder::kFlOrder) {
#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
      // Unpack and permute in one pass; no grid is ever written to memory.
      switch (w) {
        ILP_WIDTH_CASES(ILP_UNPACK_FUSED_CASE)
        default:
          break;
      }
#else
      // Portable fallback: materialize the grid, then read it back out.
      // Transpose32x32 is its own inverse for a square grid, so the same call
      // used to build the FL_ORDER layout also reads it back into file order.
      switch (w) {
        ILP_WIDTH_CASES(ILP_UNPACK_SCRATCH_CASE)
        default:
          break;
      }
      Transpose32x32(scratch, out_blk);
#endif
    } else {
      // kFileOrder and kFlOrderRaw both want the grid exactly as UnpackBlock
      // produces it, so the kernel writes straight into the caller's buffer.
      // This used to unpack into a 4 KiB stack grid and then memcpy it out,
      // which stored every value twice and traversed the output an extra time --
      // a width-invariant cost inside the hot loop. UnpackBlock writes
      // out[row * kLanes + lane] contiguously, exactly once per value, with no
      // read-modify-write and no alignment requirement, and both of its pointers
      // are ARROW_RESTRICT, so retargeting it emits the same store sequence
      // against a different base. Measured on Granite Rapids at -O2
      // -march=haswell, 102400 values: w=11 27.0 -> 33.6 GiB/s (1.24x), w=3
      // 32.1 -> 40.8 (1.27x); reproduced on gcc 11.5, gcc 15.2 and clang 21.1,
      // and matched by an independent bandwidth probe (38.7 -> 51.3, 1.33x).
      uint32_t* dst = reinterpret_cast<uint32_t*>(out_blk);
      switch (w) {
        ILP_WIDTH_CASES(ILP_UNPACK_CASE)
        default:
          break;
      }
    }
    src += w * kLanes;
  }

  const size_t tail = n % kBlockSize;
  if (tail > 0) {
    std::memcpy(out + nblocks * kBlockSize, src, tail * sizeof(int32_t));
  }
}

#undef ILP_PACK_CASE
#undef ILP_UNPACK_CASE
#undef ILP_UNPACK_SCRATCH_CASE
#undef ILP_UNPACK_FUSED_CASE
#undef ILP_WIDTH_CASES

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
