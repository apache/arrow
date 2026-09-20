// width_ladder -- the same four layouts as layout_benchmark, but with bit width
// as the only variable, one point per width from 1 to 32.
//
// layout_benchmark answers "how do these layouts compare on realistic columns",
// and its columns carry a mix of widths, so its geomean hides the width axis
// entirely. This file exists because a layout ratio is a function of bit width:
// a narrow width leaves more of the unpack in shifts and masks, a wide one
// leaves more of it in loads and stores, and the two layouts do not shift the
// same amount per value. Any comparison quoted without its width is
// underspecified, and this is the table that supplies it.
//
// Held fixed: 32-bit output, one 1024-value block per unit of work, the same
// per-block frame of reference, an L2-resident working set, and one 4096-aligned
// arena so output-address-mod-4096 cannot move a ratio. Varied: w.
//
// Output width is a second axis and is NOT varied here -- everything writes
// int32, which is what the C++ reader materialises today, not something the
// format requires: an INT(8) or INT(16) column is stored as INT32 with an
// annotation, and the logical-type rules let a reader produce the narrower
// in-memory type. That axis needs a narrower container and a different set of
// kernels, and output_width_matrix.cpp measures it.
//
//   seq_scal  Parquet's continuous LSB-first stream, Arrow's generated scalar
//             unpacker.
//   seq_simd  The same bytes through Arrow's shipped SIMD unpacker. This is the
//             baseline that decides whether Parquet should change layout, and it
//             is the one a ratio should name: against seq_scal every layout wins
//             by about 3x, which says nothing about the layout.
//   intlv     FastLanes interleaved container, values out in file order.
//   fl_unpk   Same container, FL_ORDER lane assignment left in place. Timing
//             control: it runs the identical kernel over identical bytes, so it
//             has to tie with intlv.
//   fl_tpos   FL_ORDER permuted back to file order, fused, in registers.
//
// Every arm is checked bit-exact before anything is timed.
//
// Build with build.sh (./build.sh width_ladder); OPT selects the level.

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <algorithm>
#include <chrono>
#include <functional>
#include <random>
#include <vector>

#include <arrow/util/bpacking_internal.h>
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_scalar_generated_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"

namespace fl = arrow::util::fastlanes;
namespace bp = arrow::internal::bpacking;
using fl::InterleavedPforOrder;
using Clock = std::chrono::steady_clock;

template <typename U, int W>
using KernelScalar = arrow::internal::ScalarUnpackerForWidth<U, W>;

constexpr size_t kBlk = 1024;
constexpr int kReps = 5;
// 128 blocks: 512 KiB of int32 output and at most 512 KiB of packed input, so
// both stay inside this core's 2 MiB L2 at every width. Residency is
// layout_benchmark's axis, not this file's.
constexpr size_t kBlocks = 128;
constexpr size_t kN = kBlocks * kBlk;
constexpr size_t kBytesPerRun = 1024u * 1024 * 1024;  // output bytes per timed run
constexpr int kVariants = 5;
static const char* kName[kVariants] = {"seq_scal", "seq_simd", "intlv", "fl_unpk",
                                       "fl_tpos"};

// Values whose per-block span is exactly 2^w - 1, so both encoders independently
// pick width w and the ladder's x-axis is the width actually packed rather than
// a width requested.
static std::vector<int32_t> GenAtWidth(uint32_t w) {
  std::vector<int32_t> v(kN);
  std::mt19937 rng(0x5EED0000u + w);
  if (w == 0) return v;
  for (size_t b = 0; b < kBlocks; ++b) {
    int32_t* blk = v.data() + b * kBlk;
    if (w == 32) {
      for (size_t t = 0; t < kBlk; ++t) blk[t] = static_cast<int32_t>(rng());
      blk[0] = std::numeric_limits<int32_t>::min();
      blk[1] = std::numeric_limits<int32_t>::max();
    } else {
      const uint32_t hi = (1u << w) - 1;
      for (size_t t = 0; t < kBlk; ++t) blk[t] = static_cast<int32_t>(rng() & hi);
      blk[0] = 0;
      blk[1] = static_cast<int32_t>(hi);
    }
  }
  return v;
}

struct SeqPayload {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;
  std::vector<uint8_t> widths;
  std::vector<int32_t> mins;
};

static uint32_t WidthFor(uint32_t span) {
  uint32_t w = 0;
  while (w < 32 && (span >> w) != 0) ++w;
  return w;
}

// Parquet's layout: per block, the min, the width for (value - min), and the
// residuals as one continuous LSB-first stream. Block-aligned so each block can
// be handed to the unpacker on its own, which is how a page decoder walks them.
static SeqPayload EncodeSequential(const std::vector<int32_t>& v) {
  const size_t nblocks = v.size() / kBlk;
  SeqPayload p;
  p.widths.resize(nblocks);
  p.mins.resize(nblocks);
  p.offsets.resize(nblocks);
  size_t off = 0;
  for (size_t b = 0; b < nblocks; ++b) {
    int32_t mn = v[b * kBlk], mx = v[b * kBlk];
    for (size_t t = 1; t < kBlk; ++t) {
      mn = std::min(mn, v[b * kBlk + t]);
      mx = std::max(mx, v[b * kBlk + t]);
    }
    const uint32_t w =
        WidthFor(static_cast<uint32_t>(mx) - static_cast<uint32_t>(mn));
    p.widths[b] = static_cast<uint8_t>(w);
    p.mins[b] = mn;
    p.offsets[b] = static_cast<uint32_t>(off);
    off += w * kBlk / 8;
  }
  p.bytes.assign(off + 64, 0);
  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = p.widths[b];
    if (w == 0) continue;
    uint8_t* out = p.bytes.data() + p.offsets[b];
    uint64_t acc = 0;
    int bits = 0;
    for (size_t t = 0; t < kBlk; ++t) {
      const uint32_t r = static_cast<uint32_t>(v[b * kBlk + t]) -
                         static_cast<uint32_t>(p.mins[b]);
      acc |= static_cast<uint64_t>(r) << bits;
      bits += static_cast<int>(w);
      while (bits >= 8) {
        *out++ = static_cast<uint8_t>(acc);
        acc >>= 8;
        bits -= 8;
      }
    }
    if (bits) *out = static_cast<uint8_t>(acc);
  }
  return p;
}

// One 4096-aligned arena for every buffer, for the reason layout_benchmark
// states at length: left to a vector each, input- and output-address-mod-4096
// vary per arm and per width and are a larger effect than the layout difference.
static uint8_t* Arena(size_t bytes) {
  static uint8_t* buf = nullptr;
  static size_t cap = 0;
  if (bytes > cap) {
    free(buf);
    void* raw = nullptr;
    if (posix_memalign(&raw, 4096, bytes + 4096) != 0) abort();
    memset(raw, 0, bytes + 4096);
    buf = static_cast<uint8_t*>(raw);
    cap = bytes;
  }
  return buf;
}
static constexpr size_t RoundUpPage(size_t n) { return (n + 4095) & ~size_t(4095); }

template <bool kScalar>
static void DecodeSeq(const SeqPayload& p, const uint8_t* base, size_t n, int32_t* out) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int>(kBlk);
  const size_t nblocks = n / kBlk;
  for (size_t b = 0; b < nblocks; ++b) {
    const uint32_t w = p.widths[b];
    uint32_t* dst = reinterpret_cast<uint32_t*>(out) + b * kBlk;
    const uint32_t bias = static_cast<uint32_t>(p.mins[b]);
    if (w == 0) {
      for (size_t t = 0; t < kBlk; ++t) dst[t] = bias;
      continue;
    }
    o.bit_width = static_cast<int>(w);
    // The vectorized kernel stops short of any byte it has not been told is
    // readable, so the bound has to name bytes past this block or the last
    // vector step of every block falls to a scalar epilog. A real reader can
    // name them -- the packed stream continues to the end of the page -- so what
    // is passed is the distance from this block to the end of the payload.
    const size_t readable = p.bytes.size() - p.offsets[b];
    o.max_read_bytes = static_cast<int>(
        std::min<size_t>(readable, static_cast<size_t>(std::numeric_limits<int>::max())));
    if (kScalar) {
      bp::unpack_jump<KernelScalar, /*kHasBias=*/true>(base + p.offsets[b], dst, o, bias);
    } else {
      arrow::internal::unpack_bias<uint32_t>(base + p.offsets[b], dst, o, bias);
    }
  }
}

int main() {
  printf("width_ladder -- bit width as the only variable, 32-bit output, %zu KiB out\n",
         kN * sizeof(int32_t) / 1024);
  printf("Throughput is output GiB/s. fl_unpk/intlv is the timing control and must "
         "read 1.00x.\n\n");
  printf("%3s %6s %8s %8s %8s %8s %8s   %8s %8s %8s %8s\n", "w", "srcKiB", "seq_scal",
         "seq_simd", "intlv", "fl_unpk", "fl_tpos", "int/scal", "int/simd", "tpos/simd",
         "unpk/int");

  int failures = 0;
  for (uint32_t w = 1; w <= 32; ++w) {
    const std::vector<int32_t> values = GenAtWidth(w);
    const SeqPayload seq = EncodeSequential(values);
    for (size_t b = 0; b < kBlocks; ++b) {
      if (seq.widths[b] != w) {
        fprintf(stderr, "w=%u: block %zu packed at %u, generator is wrong\n", w, b,
                seq.widths[b]);
        return 1;
      }
    }

    std::vector<uint8_t> ib(fl::InterleavedPforMaxEncodedSize(kN));
    std::vector<uint8_t> fb(fl::InterleavedPforMaxEncodedSize(kN));
    const size_t ib_len =
        fl::InterleavedPforEncode<InterleavedPforOrder::kFileOrder>(values.data(), kN,
                                                                   ib.data());
    const size_t fb_len =
        fl::InterleavedPforEncode<InterleavedPforOrder::kFlOrder>(values.data(), kN,
                                                                  fb.data());
    if (ib_len == 0 || fb_len == 0) {
      fprintf(stderr, "w=%u: interleaved encode refused the block\n", w);
      return 1;
    }

    const size_t out_bytes = kN * sizeof(int32_t);
    const size_t out_stride = RoundUpPage(out_bytes);
    uint8_t* arena = Arena(out_stride + RoundUpPage(seq.bytes.size()) +
                           RoundUpPage(ib_len) + RoundUpPage(fb_len));
    int32_t* out = reinterpret_cast<int32_t*>(arena);
    uint8_t* seq_base = arena + out_stride;
    uint8_t* ib_base = seq_base + RoundUpPage(seq.bytes.size());
    uint8_t* fb_base = ib_base + RoundUpPage(ib_len);
    memcpy(seq_base, seq.bytes.data(), seq.bytes.size());
    memcpy(ib_base, ib.data(), ib_len);
    memcpy(fb_base, fb.data(), fb_len);

    std::function<void()> variants[kVariants] = {
        [&] { DecodeSeq<true>(seq, seq_base, kN, out); },
        [&] { DecodeSeq<false>(seq, seq_base, kN, out); },
        [&] {
          fl::InterleavedPforDecode<InterleavedPforOrder::kFileOrder>(ib_base, kN, out);
        },
        [&] {
          fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrderRaw>(fb_base, kN, out);
        },
        [&] {
          fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrder>(fb_base, kN, out);
        },
    };

    // Correctness before speed. fl_unpk returns FL order on purpose and is
    // checked against the permutation instead: in each block, lane l holds the
    // contiguous run [32l, 32l+32).
    for (int a = 0; a < kVariants; ++a) {
      memset(out, 0xCD, out_bytes);
      variants[a]();
      bool ok = true;
      if (a == 3) {
        for (size_t b = 0; b < kBlocks && ok; ++b)
          for (size_t lane = 0; lane < 32 && ok; ++lane)
            for (size_t row = 0; row < 32 && ok; ++row)
              ok = out[b * kBlk + row * 32 + lane] ==
                   values[b * kBlk + lane * 32 + row];
      } else {
        ok = memcmp(out, values.data(), out_bytes) == 0;
      }
      if (!ok) {
        fprintf(stderr, "MISMATCH w=%u variant=%s\n", w, kName[a]);
        ++failures;
      }
    }

    const size_t iters = std::max<size_t>(1, kBytesPerRun / out_bytes);
    double gibs[kVariants];
    for (int a = 0; a < kVariants; ++a) {
      double best = 0;
      for (int r = 0; r < kReps; ++r) {
        auto t0 = Clock::now();
        for (size_t it = 0; it < iters; ++it) variants[a]();
        const double s = std::chrono::duration<double>(Clock::now() - t0).count();
        const double g =
            static_cast<double>(out_bytes) * iters / s / (1024.0 * 1024 * 1024);
        best = std::max(best, g);
      }
      gibs[a] = best;
    }

    printf("%3u %6zu %8.1f %8.1f %8.1f %8.1f %8.1f   %7.2fx %7.2fx %8.2fx %7.2fx\n", w,
           (w * kN / 8) / 1024, gibs[0], gibs[1], gibs[2], gibs[3], gibs[4],
           gibs[2] / gibs[0], gibs[2] / gibs[1], gibs[4] / gibs[1], gibs[3] / gibs[2]);
    fflush(stdout);
  }
  return failures == 0 ? 0 : 1;
}
