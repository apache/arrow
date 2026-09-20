// output_width_matrix -- which bit-packing layout decodes fastest, resolved by
// both widths that matter: the packed bit width and the output element width.
//
// This is a layout question, not an encoding question. There is no frame of
// reference here and no exception handling: every arm does nothing but turn
// packed bits into integers, so a ratio between two arms is the layout and
// nothing else. The corpus harnesses add the frame because a Parquet reader
// applies one; this file removes it because a layout comparison does not need
// it and because the frame add is a near-common term that pulls every ratio
// toward 1.00.
//
// Why output width is an axis at all: a 128-bit register holds 4 uint32 or 16
// uint8, so the same kernel retires four times as many values per store when
// the output element is a byte. A comparison quoted without both widths is
// underspecified -- 3 bits into 8 and 19 bits into 32 are different questions.
//
// Reading narrower than 32 bits is permitted, not a trick: Parquet's INT logical
// type annotation carries a maximum value bit width, writers must not exceed it,
// and the format spec names smaller in-memory representations as an intended use
// of the annotation. A reader also knows each block's packed width, so it can
// pick the output element from the data it is about to unpack.
//
//   seq_scal  Parquet's continuous LSB-first stream, Arrow's generated scalar
//             unpacker. At 8- and 16-bit output this is a shim that unpacks into
//             a 32-bit buffer and narrows in a second loop, so it is slower than
//             its own 32-bit output. That is Arrow's code shape, not a property
//             of the continuous layout; do not read the narrow rows of this arm
//             as a layout result.
//   seq_simd  The same bytes through Arrow's shipped vectorized unpacker, which
//             is natively narrow -- uint8 output really does run 16 lanes to a
//             register. This is the arm a layout claim has to beat.
//   intlv     FastLanes interleaved container, one 1024-value block per unit of
//             work, container word and lane width both following the output
//             element: uint8 -> 128 lanes x 8 rows, uint16 -> 64 x 16,
//             uint32 -> 32 x 32.
//   fl_unpk   The same kernel over bytes packed in lane order. Same instruction
//             stream, different data, so it is the timing control: it has to tie
//             with intlv, and a row where it does not is a row to distrust.
//   fl_tpos   Lane order permuted back to file order, fused, in registers.
//             32-bit output only -- the fused transpose is 32x32 over 32-bit
//             lanes, and narrow output needs 16x16 and 8x8 kernels that are not
//             written. Reported as absent rather than filled with the
//             unpack-to-scratch-and-transpose fallback, which would measure that
//             fallback and not the layout.
//
// Every arm is checked against the reference values before anything is timed.
// The working set is L1-resident at every output width so this is kernel cost.
//
// The same rows are printed twice, in values per cycle and in GiB/s of output,
// because the two units disagree on purpose: a store-limited kernel holds GiB/s
// roughly flat while its values per cycle doubles with every halving of the
// output element. Reading one unit alone hides which of the two is happening.
//
// Build with build.sh (./build.sh output_width_matrix); OPT selects the level.

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <vector>

#include <arrow/util/bpacking_internal.h>
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_scalar_generated_internal.h"
#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"

namespace bp = arrow::internal::bpacking;
namespace fl = arrow::util::fastlanes;
using Clock = std::chrono::steady_clock;

template <typename U, int W>
using KernelScalar = arrow::internal::ScalarUnpackerForWidth<U, W>;

constexpr size_t kBlk = 1024;
// 8 blocks is 32 KiB of 32-bit output and less at narrower widths, so every cell
// stays inside a 64 KiB L1 together with its packed input.
// Working-set size is the third axis, and the matrix below holds it at 8 blocks:
// 32 KiB of 32-bit output and less at narrower elements, so a cell and its packed
// input sit together inside this core's 64 KiB L1 and the table is kernel cost.
// SweepResidency walks the same cells out to main memory, because a kernel limited
// by its store port and one limited by its loads only separate once the output
// stops fitting in cache.
static size_t g_blocks = 8;
static size_t g_n = g_blocks * kBlk;
static int g_reps = 4000;

// Every timed trial moves about this many values whatever the working set, so a
// 16 KiB cell is not sampled a thousand times harder than a 32 MiB one.
constexpr size_t kValsPerTrial = 32u * 1024 * 1024;
static void SetWorkingSet(size_t blocks) {
  g_blocks = blocks;
  g_n = blocks * kBlk;
  g_reps = static_cast<int>(std::max<size_t>(1, kValsPerTrial / g_n));
}
constexpr int kTrials = 5;

// Container geometry follows the output element, the way FastLanes' does.
template <typename T>
struct Geom {
  static constexpr unsigned kT = sizeof(T) * 8;
  static constexpr unsigned kLanes = kBlk / kT;
  static constexpr unsigned kRows = kT;
};

template <typename T, unsigned w>
static constexpr T Mask() {
  return (w == Geom<T>::kT) ? static_cast<T>(~T(0)) : static_cast<T>((1ull << w) - 1ull);
}

// ---------------------------------------------------------------- interleaved

template <typename T, unsigned w>
static void PackInterleaved(const T* in, T* out) {
  using G = Geom<T>;
  std::memset(out, 0, w * G::kLanes * sizeof(T));
  for (unsigned row = 0; row < G::kRows; ++row) {
    const unsigned startBit = row * w, word = startBit / G::kT, shift = startBit % G::kT;
    const unsigned endWord = (startBit + w - 1) / G::kT;
    for (unsigned lane = 0; lane < G::kLanes; ++lane) {
      const T v = static_cast<T>(in[row * G::kLanes + lane] & Mask<T, w>());
      out[word * G::kLanes + lane] |= static_cast<T>(v << shift);
      if (word != endWord)
        out[endWord * G::kLanes + lane] |= static_cast<T>(v >> (G::kT - shift));
    }
  }
}

// kBarrier controls the store pattern, which is a separate question from the
// layout and is easy to mistake for one. In source order each row writes 128
// contiguous bytes and the block goes out front to back, but rows that share a
// packed word share loads, and the 32-way unroll puts every row body in one
// scope, so the compiler commons those loads and interleaves the stores 128
// bytes apart. That is invisible while the destination is cache-resident and
// expensive once it is not. An empty asm with a memory clobber at the end of each
// row gives up the shared loads -- the re-read is L1-resident -- to keep each
// row's 128 bytes as one run.
template <typename T, unsigned w, bool kBarrier = false>
static void UnpackInterleaved(const T* __restrict packed, T* __restrict out) {
  using G = Geom<T>;
#pragma GCC unroll 32
  for (unsigned row = 0; row < G::kRows; ++row) {
    const unsigned startBit = row * w, word = startBit / G::kT, shift = startBit % G::kT;
    const unsigned endWord = (startBit + w - 1) / G::kT;
    if (word == endWord) {
      for (unsigned lane = 0; lane < G::kLanes; ++lane)
        out[row * G::kLanes + lane] =
            static_cast<T>((packed[word * G::kLanes + lane] >> shift) & Mask<T, w>());
    } else {
      const unsigned lowBits = G::kT - shift;
      for (unsigned lane = 0; lane < G::kLanes; ++lane) {
        const T lo = static_cast<T>(packed[word * G::kLanes + lane] >> shift);
        const T hi = static_cast<T>(packed[endWord * G::kLanes + lane] << lowBits);
        out[row * G::kLanes + lane] = static_cast<T>((lo | hi) & Mask<T, w>());
      }
    }
    if constexpr (kBarrier) asm volatile("" ::: "memory");
  }
}

// Reached through a call the compiler cannot see through, so no arm is inlined
// while another is not.
template <typename T, unsigned w, bool kBarrier = false>
__attribute__((noinline)) static void IntlvBlocks(const T* pk, T* out) {
  using G = Geom<T>;
  for (size_t b = 0; b < g_blocks; ++b)
    UnpackInterleaved<T, w, kBarrier>(pk + b * w * G::kLanes, out + b * kBlk);
}

// The same two drivers with the destination held to a window of out_blocks
// blocks, reused for every vector. This is the shape every real decoder has:
// ALP, PFOR and FastLanes all hand back one vector at a time and the consumer
// drains it before the next arrives, so the packed input streams past once while
// the destination stays small however large the column is.
template <typename T, unsigned w, bool kBarrier = false>
__attribute__((noinline)) static void IntlvStream(const T* pk, T* out,
                                                  size_t out_blocks) {
  using G = Geom<T>;
  const size_t mask = out_blocks - 1;
  for (size_t b = 0; b < g_blocks; ++b)
    UnpackInterleaved<T, w, kBarrier>(pk + b * w * G::kLanes,
                                      out + (b & mask) * kBlk);
}

// ----------------------------------------------------------------- continuous

// One LSB-first stream, block-aligned so each block can be handed to the
// unpacker on its own, which is how a page decoder walks them.
template <typename T, unsigned w>
static std::vector<uint8_t> PackContinuous(const std::vector<T>& v) {
  std::vector<uint8_t> bytes(g_blocks * w * kBlk / 8 + 64, 0);
  uint8_t* out = bytes.data();
  for (size_t b = 0; b < g_blocks; ++b) {
    uint64_t acc = 0;
    int bits = 0;
    for (size_t t = 0; t < kBlk; ++t) {
      acc |= static_cast<uint64_t>(v[b * kBlk + t] & Mask<T, w>()) << bits;
      bits += static_cast<int>(w);
      while (bits >= 8) {
        *out++ = static_cast<uint8_t>(acc);
        acc >>= 8;
        bits -= 8;
      }
    }
    if (bits) *out++ = static_cast<uint8_t>(acc);
  }
  return bytes;
}

template <typename T, unsigned w, bool kScalar>
__attribute__((noinline)) static void SeqBlocks(const uint8_t* src, size_t total,
                                                T* out) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int>(kBlk);
  o.bit_width = static_cast<int>(w);
  const size_t stride = w * kBlk / 8;
  for (size_t b = 0; b < g_blocks; ++b) {
    // The vectorized kernel stops short of any byte it has not been told is
    // readable, so the bound has to name bytes past this block or the last
    // vector step of every block falls to a scalar epilog. A real reader can
    // name them: the packed stream continues to the end of the page.
    o.max_read_bytes = static_cast<int>(total - b * stride);
    if constexpr (kScalar) {
      bp::unpack_jump<KernelScalar>(src + b * stride, out + b * kBlk, o);
    } else {
      arrow::internal::unpack<T>(src + b * stride, out + b * kBlk, o);
    }
  }
}

template <typename T, unsigned w>
__attribute__((noinline)) static void SeqStream(const uint8_t* src, size_t total, T* out,
                                                size_t out_blocks) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int>(kBlk);
  o.bit_width = static_cast<int>(w);
  const size_t stride = w * kBlk / 8, mask = out_blocks - 1;
  for (size_t b = 0; b < g_blocks; ++b) {
    o.max_read_bytes = static_cast<int>(total - b * stride);
    arrow::internal::unpack<T>(src + b * stride, out + (b & mask) * kBlk, o);
  }
}

// ------------------------------------------------------------ fused transpose

template <unsigned w>
__attribute__((noinline)) static void TposBlocks(const uint32_t* pk, int32_t* out) {
  for (size_t b = 0; b < g_blocks; ++b)
    fl::UnpackBlockFlToFileOrder<w, /*kHasBias=*/false>(pk + b * w * 32,
                                                        out + b * kBlk, 0);
}

// ------------------------------------------------------------------- plumbing

static double g_ghz = 0.0;

// A dependent chain of single-cycle adds, so elapsed time over iteration count
// is the core clock. Measured rather than assumed, because every val/cyc figure
// below scales linearly with it.
static double MeasureGhz() {
#if defined(__aarch64__) || defined(__x86_64__)
  const uint64_t kIter = 40000000ull;
  uint64_t x = 1;
  auto t0 = Clock::now();
  for (uint64_t i = 0; i < kIter; ++i) {
#if defined(__aarch64__)
    asm volatile("add %0, %0, #1\n\tadd %0, %0, #1\n\tadd %0, %0, #1\n\tadd %0, %0, #1\n\t"
                 "add %0, %0, #1\n\tadd %0, %0, #1\n\tadd %0, %0, #1\n\tadd %0, %0, #1"
                 : "+r"(x));
#else
    asm volatile("addq $1, %0\n\taddq $1, %0\n\taddq $1, %0\n\taddq $1, %0\n\t"
                 "addq $1, %0\n\taddq $1, %0\n\taddq $1, %0\n\taddq $1, %0"
                 : "+r"(x));
#endif
  }
  const double s = std::chrono::duration<double>(Clock::now() - t0).count();
  if (x == 0) abort();  // keep the chain live
  return static_cast<double>(kIter) * 8 / s / 1e9;
#else
  return 0.0;
#endif
}

struct Cell {
  double valcyc[5];  // seq_scal, seq_simd, intlv, fl_unpk, fl_tpos; 0 = absent
  double barrier;    // intlv with the store-ordering barrier; residency sweep only
};

// Times one arm and returns values per cycle, or 0 if it failed validation.
template <typename T>
static double Time(const std::function<void()>& run, const std::vector<T>& ref,
                   const T* out, bool permuted) {
  std::memset(const_cast<T*>(out), 0xCD, g_n * sizeof(T));
  run();
  bool ok = true;
  if (permuted) {
    // Lane order on purpose: in each block, lane l holds the contiguous run
    // [kRows*l, kRows*l + kRows).
    using G = Geom<T>;
    for (size_t b = 0; b < g_blocks && ok; ++b)
      for (unsigned lane = 0; lane < G::kLanes && ok; ++lane)
        for (unsigned row = 0; row < G::kRows && ok; ++row)
          ok = out[b * kBlk + row * G::kLanes + lane] ==
               ref[b * kBlk + lane * G::kRows + row];
  } else {
    ok = std::equal(ref.begin(), ref.end(), out);
  }
  if (!ok) return 0.0;

  for (int r = 0; r < 20; ++r) run();
  double best = 1e30;
  for (int t = 0; t < kTrials; ++t) {
    auto t0 = Clock::now();
    for (int r = 0; r < g_reps; ++r) run();
    best = std::min(best, std::chrono::duration<double>(Clock::now() - t0).count());
  }
  const double vals = static_cast<double>(g_n) * g_reps;
  return vals / best / (g_ghz * 1e9);
}

template <typename T, unsigned w>
static Cell Run() {
  using G = Geom<T>;
  static_assert(w <= G::kT, "packed width cannot exceed the output element");
  Cell c{};

  std::vector<T> ref(g_n), refPerm(g_n);
  std::mt19937 rng(7 + w);
  for (auto& v : ref) v = static_cast<T>(rng() & Mask<T, w>());
  // The same values with lanes carrying contiguous runs, which is what the
  // lane-order arms decode.
  for (size_t b = 0; b < g_blocks; ++b)
    for (unsigned lane = 0; lane < G::kLanes; ++lane)
      for (unsigned row = 0; row < G::kRows; ++row)
        refPerm[b * kBlk + row * G::kLanes + lane] = ref[b * kBlk + lane * G::kRows + row];

  std::vector<T> pk(g_blocks * w * G::kLanes + 64, 0), pkPerm(pk.size(), 0);
  for (size_t b = 0; b < g_blocks; ++b) {
    PackInterleaved<T, w>(ref.data() + b * kBlk, pk.data() + b * w * G::kLanes);
    PackInterleaved<T, w>(refPerm.data() + b * kBlk, pkPerm.data() + b * w * G::kLanes);
  }
  const std::vector<uint8_t> seq = PackContinuous<T, w>(ref);
  std::vector<T> out(g_n + 64, 0);

  c.valcyc[0] = Time<T>([&] { SeqBlocks<T, w, true>(seq.data(), seq.size(), out.data()); },
                        ref, out.data(), false);
  c.valcyc[1] = Time<T>([&] { SeqBlocks<T, w, false>(seq.data(), seq.size(), out.data()); },
                        ref, out.data(), false);
  c.valcyc[2] = Time<T>([&] { IntlvBlocks<T, w>(pk.data(), out.data()); }, ref,
                        out.data(), false);
  c.valcyc[3] = Time<T>([&] { IntlvBlocks<T, w>(pkPerm.data(), out.data()); }, ref,
                        out.data(), true);
  c.barrier = Time<T>([&] { IntlvBlocks<T, w, true>(pk.data(), out.data()); }, ref,
                      out.data(), false);

  if constexpr (std::is_same_v<T, uint32_t>) {
    std::vector<int32_t> o32(g_n + 64, 0);
    std::vector<uint32_t> pkT(pk.size(), 0);
    // The fused kernel undoes a 32x32 transpose, so pack the transposed values.
    std::vector<uint32_t> vT(g_n);
    for (size_t b = 0; b < g_blocks; ++b)
      for (unsigned row = 0; row < 32; ++row)
        for (unsigned lane = 0; lane < 32; ++lane)
          vT[b * kBlk + row * 32 + lane] = ref[b * kBlk + lane * 32 + row];
    for (size_t b = 0; b < g_blocks; ++b)
      PackInterleaved<uint32_t, w>(vT.data() + b * kBlk, pkT.data() + b * w * 32);
    std::vector<int32_t> refI(ref.begin(), ref.end());
    c.valcyc[4] = Time<int32_t>(
        [&] { TposBlocks<w>(pkT.data(), o32.data()); }, refI, o32.data(), false);
  }
  return c;
}

// One measured row. Values per cycle and GiB/s of output come from the same
// timing: the first divides by the measured clock, the second by wall time, and
// they answer different questions. Values per cycle says how much decoding work
// a core retires; GiB/s says how many bytes it has to write to do it. A layout
// that is limited by its store port holds GiB/s roughly constant while values
// per cycle doubles with every halving of the output element, which is exactly
// what separates the two layouts here, so both belong in the table.
struct Row {
  unsigned w, ob;
  Cell c;
};
static std::vector<Row> g_rows;
static void Measure(unsigned w, unsigned ob, const Cell& c) {
  g_rows.push_back({w, ob, c});
}

// Same row, either unit. kGibs converts with the output element width.
static void PrintRow(const Row& r, bool kGibs) {
  const double scale =
      kGibs ? g_ghz * 1e9 * (r.ob / 8.0) / (1024.0 * 1024 * 1024) : 1.0;
  printf("%3u %7u ", r.w, r.ob);
  for (int a = 0; a < 5; ++a) {
    if (r.c.valcyc[a] == 0.0) {
      printf("%9s", (a == 4) ? "-" : "FAIL");
    } else {
      printf("%9.2f", r.c.valcyc[a] * scale);
    }
  }
  // Ratios are unit-free within a row and are printed once, in both tables, so
  // either table can be read on its own.
  const double simd = r.c.valcyc[1], intlv = r.c.valcyc[2];
  printf("   ");
  if (simd > 0 && intlv > 0) {
    printf("%8.2fx", intlv / simd);
  } else {
    printf("%9s", "-");
  }
  if (r.c.valcyc[0] > 0 && intlv > 0) {
    printf("%8.2fx", intlv / r.c.valcyc[0]);
  } else {
    printf("%9s", "-");
  }
  if (r.c.valcyc[3] > 0 && intlv > 0) {
    printf("%8.2fx", r.c.valcyc[3] / intlv);
  } else {
    printf("%9s", "-");
  }
  printf("\n");
}

// Blank line between output-element groups, the way the rows were entered.
static void PrintTable(const char* unit, bool kGibs) {
  printf("%s, higher is better. unpk/int is the timing control and must read "
         "1.00x.\n\n", unit);
  printf("%3s %7s %9s %9s %9s %9s %9s   %9s %9s %9s\n", "w", "out_b", "seq_scal",
         "seq_simd", "intlv", "fl_unpk", "fl_tpos", "int/simd", "int/scal", "unpk/int");
  for (size_t i = 0; i < g_rows.size(); ++i) {
    if (i && g_rows[i].ob != g_rows[i - 1].ob) printf("\n");
    PrintRow(g_rows[i], kGibs);
  }
  printf("\n");
}

// ------------------------------------------------------------ residency sweep
// The matrix above is L1-resident, which answers "what do these kernels cost"
// and not "what limits them". This sweep answers the second question by moving
// the same cells from L1 out past L3, with everything else held fixed.
//
// What each outcome would mean. If the interleaved advantage is a property of the
// core -- fewer instructions retired per value -- the ratio stays put as the
// working set grows, because both arms then wait on the same memory. If instead
// the advantage only exists while the output is close, the ratio decays toward
// 1.00 as the footprint passes each cache level, and the layout is buying cache
// behaviour rather than decode work. The absolute figures answer a third
// question: a kernel pinned at the same GiB/s across three output element widths
// is limited by what it writes, not by what it computes, and that ceiling is a
// property of this core's store path rather than of either layout.
//
// Footprint is output plus packed input, which is what has to be resident.
struct ResRow {
  size_t blocks;
  unsigned w, ob;
  Cell c;
};

template <typename T, unsigned w>
static ResRow ResCell(size_t blocks) {
  SetWorkingSet(blocks);
  return ResRow{blocks, w, sizeof(T) * 8, Run<T, w>()};
}

static void PrintResRow(const ResRow& r) {
  const double out_kib = static_cast<double>(r.blocks) * kBlk * (r.ob / 8.0) / 1024.0;
  const double in_kib = static_cast<double>(r.blocks) * kBlk * r.w / 8.0 / 1024.0;
  const double gibs = g_ghz * 1e9 * (r.ob / 8.0) / (1024.0 * 1024 * 1024);
  const double simd = r.c.valcyc[1], intlv = r.c.valcyc[2], unpk = r.c.valcyc[3];
  const double barr = r.c.barrier;
  printf("%3u %6u %10.0f %8.2f %8.2f %8.2f %9.1f %9.1f %9.2fx %9.2fx %9.2fx\n", r.w, r.ob,
         out_kib + in_kib, simd, intlv, barr, simd * gibs, intlv * gibs,
         simd > 0 ? intlv / simd : 0.0, simd > 0 ? barr / simd : 0.0,
         intlv > 0 ? unpk / intlv : 0.0);
}

static void SweepResidency() {
  printf("Residency sweep -- the same cells from L1 out past L3. Footprint is\n"
         "output plus packed input in KiB. This core: 64 KiB L1d, 2 MiB L2,\n"
         "36 MiB L3 shared. barr is the interleaved kernel with a store-ordering\n"
         "barrier per row, which trades shared loads for contiguous store runs.\n"
         "unpk/int is the timing control and must read 1.00x.\n\n");
  printf("%3s %6s %10s %8s %8s %8s %9s %9s %10s %10s %10s\n", "w", "out_b", "footKiB",
         "simd/c", "intlv/c", "barr/c", "simdGiB/s", "intlGiB/s", "int/simd", "barr/simd",
         "unpk/int");
  // 4 blocks is 16 KiB of 32-bit output, 8192 blocks is 32 MiB of it.
  static const size_t kSets[] = {4, 16, 64, 256, 1024, 4096, 8192};
  for (size_t s : kSets) {
    PrintResRow(ResCell<uint8_t, 3>(s));
    PrintResRow(ResCell<uint8_t, 5>(s));
    PrintResRow(ResCell<uint16_t, 9>(s));
    PrintResRow(ResCell<uint16_t, 13>(s));
    PrintResRow(ResCell<uint32_t, 20>(s));
    PrintResRow(ResCell<uint32_t, 24>(s));
    printf("\n");
    fflush(stdout);
  }
}

// ------------------------------------------------- vector at a time, small out
//
// Both tables above hand the decoder the whole column at once, and past L2 that
// makes the destination the dominant cost. No decoder works that way. ALP, PFOR
// and FastLanes all decode one vector -- 1024 to 8192 values -- hand it to the
// consumer, and reuse the same destination for the next one, so the output stays
// in L1 however long the column is while the packed input streams past once.
// This sweep is that shape, and it is the one a claim about decode rate should
// be made from: a column large enough that its packed form does not fit in cache,
// and a destination window of 1, 2, 4 or 8 vectors.
struct StreamRow {
  unsigned w, ob;
  size_t out_blocks;
  double simd, intlv, barr;
  double in_mib, out_kib;
};

// 16384 blocks is 16.8M values: 6 MiB of packed input at three bits and 42 MiB at
// twenty, so the wide cells come from memory on every pass and the narrow ones
// from L3. Neither is a cache-resident input, which is the point.
constexpr size_t kStreamBlocks = 16384;

template <typename T, unsigned w>
static StreamRow StreamCell(size_t out_blocks) {
  using G = Geom<T>;
  SetWorkingSet(kStreamBlocks);
  StreamRow r{w, sizeof(T) * 8, out_blocks, 0, 0, 0, 0, 0};
  r.in_mib = static_cast<double>(g_n) * w / 8.0 / (1024.0 * 1024);
  r.out_kib = static_cast<double>(out_blocks) * kBlk * sizeof(T) / 1024.0;

  std::vector<T> ref(g_n);
  std::mt19937 rng(7 + w);
  for (auto& v : ref) v = static_cast<T>(rng() & Mask<T, w>());
  std::vector<T> pk(g_blocks * w * G::kLanes + 64, 0);
  for (size_t b = 0; b < g_blocks; ++b)
    PackInterleaved<T, w>(ref.data() + b * kBlk, pk.data() + b * w * G::kLanes);
  const std::vector<uint8_t> seq = PackContinuous<T, w>(ref);
  std::vector<T> out(out_blocks * kBlk + 64, 0);

  // After a full pass, window slot j holds the last block that mapped to it.
  auto check = [&](const std::function<void()>& run) {
    std::memset(out.data(), 0xCD, out.size() * sizeof(T));
    run();
    for (size_t j = 0; j < out_blocks; ++j) {
      const T* want = ref.data() + (g_blocks - out_blocks + j) * kBlk;
      if (!std::equal(want, want + kBlk, out.data() + j * kBlk)) return false;
    }
    return true;
  };
  auto time = [&](const std::function<void()>& run) {
    if (!check(run)) return 0.0;
    run();
    double best = 1e30;
    for (int trial = 0; trial < kTrials; ++trial) {
      auto t0 = Clock::now();
      for (int rep = 0; rep < g_reps; ++rep) run();
      best = std::min(best, std::chrono::duration<double>(Clock::now() - t0).count());
    }
    return static_cast<double>(g_n) * g_reps / best / (g_ghz * 1e9);
  };

  r.simd = time([&] { SeqStream<T, w>(seq.data(), seq.size(), out.data(), out_blocks); });
  r.intlv = time([&] { IntlvStream<T, w>(pk.data(), out.data(), out_blocks); });
  r.barr = time([&] { IntlvStream<T, w, true>(pk.data(), out.data(), out_blocks); });
  return r;
}

static void PrintStreamRow(const StreamRow& r) {
  const double gibs = g_ghz * 1e9 * (r.ob / 8.0) / (1024.0 * 1024 * 1024);
  printf("%3u %6u %7zu %8.1f %8.1f %8.2f %8.2f %8.2f %9.1f %9.1f %10.2fx %10.2fx\n", r.w,
         r.ob, r.out_blocks, r.in_mib, r.out_kib, r.simd, r.intlv, r.barr, r.simd * gibs,
         r.intlv * gibs, r.simd > 0 ? r.intlv / r.simd : 0.0,
         r.simd > 0 ? r.barr / r.simd : 0.0);
}

static void SweepVectorAtATime() {
  printf("Vector at a time -- %zu values of packed input streamed past a window of\n"
         "out_b vectors, which is how every real decoder is driven. inMiB is the\n"
         "packed column, outKiB the destination window.\n\n",
         kStreamBlocks * kBlk);
  printf("%3s %6s %7s %8s %8s %8s %8s %8s %9s %9s %11s %11s\n", "w", "out_b", "out_b",
         "inMiB", "outKiB", "simd/c", "intlv/c", "barr/c", "simdGiB/s", "intlGiB/s",
         "int/simd", "barr/simd");
  static const size_t kWindows[] = {1, 2, 4, 8};
  for (size_t ob : kWindows) {
    PrintStreamRow(StreamCell<uint8_t, 3>(ob));
    PrintStreamRow(StreamCell<uint8_t, 5>(ob));
    PrintStreamRow(StreamCell<uint16_t, 9>(ob));
    PrintStreamRow(StreamCell<uint16_t, 13>(ob));
    PrintStreamRow(StreamCell<uint32_t, 20>(ob));
    PrintStreamRow(StreamCell<uint32_t, 24>(ob));
    printf("\n");
    fflush(stdout);
  }
}

int main() {
  g_ghz = MeasureGhz();
  if (g_ghz <= 0.1) {
    fprintf(stderr, "could not measure the core clock on this target\n");
    return 1;
  }
  printf("output_width_matrix -- bit unpacking only, no frame, no exceptions\n");
  printf("Core clock measured at %.2f GHz. %zu values per pass, L1-resident.\n\n", g_ghz,
         g_n);
  Measure(1, 8, Run<uint8_t, 1>());
  Measure(2, 8, Run<uint8_t, 2>());
  Measure(3, 8, Run<uint8_t, 3>());
  Measure(5, 8, Run<uint8_t, 5>());
  Measure(7, 8, Run<uint8_t, 7>());
  Measure(1, 16, Run<uint16_t, 1>());
  Measure(2, 16, Run<uint16_t, 2>());
  Measure(3, 16, Run<uint16_t, 3>());
  Measure(9, 16, Run<uint16_t, 9>());
  Measure(10, 16, Run<uint16_t, 10>());
  Measure(11, 16, Run<uint16_t, 11>());
  Measure(13, 16, Run<uint16_t, 13>());
  Measure(1, 32, Run<uint32_t, 1>());
  Measure(2, 32, Run<uint32_t, 2>());
  Measure(3, 32, Run<uint32_t, 3>());
  Measure(9, 32, Run<uint32_t, 9>());
  Measure(10, 32, Run<uint32_t, 10>());
  Measure(11, 32, Run<uint32_t, 11>());
  Measure(19, 32, Run<uint32_t, 19>());
  Measure(20, 32, Run<uint32_t, 20>());
  Measure(24, 32, Run<uint32_t, 24>());

  PrintTable("Values per cycle", /*kGibs=*/false);
  PrintTable("GiB/s of output", /*kGibs=*/true);
  SweepResidency();
  SweepVectorAtATime();
  return 0;
}
