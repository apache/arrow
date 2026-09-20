// corpus_output_width -- the 43-column corpus decoded into the narrowest output
// element each column's values allow, against the same columns decoded into
// 32-bit elements.
//
// layout_benchmark measures these columns writing int32, because that is what
// the C++ reader materialises. output_width_matrix shows that the output element
// is the axis a layout ratio moves along, but it does it at synthetic widths.
// This file joins the two: real column shapes, right-sized output.
//
// The rule for "right-sized" is the values, not the residuals. A column whose
// values all fit in a byte may be materialised a byte per value; a column framed
// down to 3-bit residuals around a frame of 100000 may not, because the frame
// comes back before the value is stored. So the choice is made from the column's
// own range, which is what an INT(8) or INT(16) annotation would carry, and most
// of this corpus does not qualify -- that is a result, not a gap.
//
// Every arm adds the per-block frame, so these are reader-shaped figures
// comparable with layout_benchmark's, not the frameless layout figures in
// output_width_matrix.
//
//   seq_scal  Parquet's continuous stream, Arrow's generated scalar unpacker.
//             At narrow output this path unpacks wide and narrows in a second
//             loop -- Arrow's code shape, not a property of the layout.
//   seq_simd  The same bytes through Arrow's shipped vectorized unpacker, which
//             is natively narrow. This is the arm a layout claim has to beat.
//   intlv     FastLanes interleaved container, values out in file order, with
//             the container word following the output element.
//
// Both units are reported for the same timing: values per cycle against a
// self-measured clock, and GiB/s of the bytes written. They disagree by design
// when the output element changes, which is the whole point of the table.
//
// Build with build.sh (./build.sh corpus_output_width); OPT selects the level.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <string>
#include <vector>

#include <arrow/util/bpacking_internal.h>
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_scalar_generated_internal.h"
#include "corpus_generators.h"

namespace bp = arrow::internal::bpacking;
using Clock = std::chrono::steady_clock;

template <typename U, int W>
using KernelScalar = arrow::internal::ScalarUnpackerForWidth<U, W>;

constexpr size_t kBlk = 1024;
// 128 Ki values: 512 KiB of int32 output, less when narrower, so every arm stays
// inside this core's 2 MiB L2. Residency is layout_benchmark's axis, not this
// file's, and the layout advantage is flat across it.
constexpr size_t kN = 128 * 1024;
constexpr size_t kBlocks = kN / kBlk;
constexpr int kTrials = 5;
constexpr size_t kBytesPerRun = 256u * 1024 * 1024;

template <typename T>
struct Geom {
  static constexpr unsigned kT = sizeof(T) * 8;
  static constexpr unsigned kLanes = kBlk / kT;
  static constexpr unsigned kRows = kT;
};

template <typename T>
static T MaskFor(unsigned w) {
  return (w >= Geom<T>::kT) ? static_cast<T>(~T(0)) : static_cast<T>((T(1) << w) - 1);
}

static double g_ghz = 0.0;

// A dependent chain of single-cycle adds, so elapsed time over iteration count
// is the core clock.
static double MeasureGhz() {
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
  if (x == 0) abort();
  return static_cast<double>(kIter) * 8 / s / 1e9;
}

// Per-block frame and width, computed on the values as they will be stored.
struct Frame {
  std::vector<int64_t> min;
  std::vector<uint8_t> width;
};

static unsigned WidthFor(uint64_t span) {
  unsigned w = 0;
  while (w < 64 && (span >> w) != 0) ++w;
  return w;
}

static Frame FrameOf(const std::vector<int32_t>& v) {
  Frame f;
  f.min.resize(kBlocks);
  f.width.resize(kBlocks);
  for (size_t b = 0; b < kBlocks; ++b) {
    int64_t mn = v[b * kBlk], mx = v[b * kBlk];
    for (size_t t = 1; t < kBlk; ++t) {
      mn = std::min<int64_t>(mn, v[b * kBlk + t]);
      mx = std::max<int64_t>(mx, v[b * kBlk + t]);
    }
    f.min[b] = mn;
    f.width[b] = static_cast<uint8_t>(WidthFor(static_cast<uint64_t>(mx - mn)));
  }
  return f;
}

// ---------------------------------------------------------------- interleaved

template <typename T, unsigned w>
static void PackInterleaved(const T* in, T* out) {
  using G = Geom<T>;
  std::memset(out, 0, w * G::kLanes * sizeof(T));
  if constexpr (w == 0) {
    return;
  } else {
  for (unsigned row = 0; row < G::kRows; ++row) {
    const unsigned startBit = row * w, word = startBit / G::kT, shift = startBit % G::kT;
    const unsigned endWord = (startBit + w - 1) / G::kT;
    for (unsigned lane = 0; lane < G::kLanes; ++lane) {
      const T v = static_cast<T>(in[row * G::kLanes + lane] & MaskFor<T>(w));
      out[word * G::kLanes + lane] |= static_cast<T>(v << shift);
      if (word != endWord)
        out[endWord * G::kLanes + lane] |= static_cast<T>(v >> (G::kT - shift));
    }
  }
  }
}

template <typename T, unsigned w>
static void UnpackInterleaved(const T* __restrict packed, T* __restrict out, T bias) {
  using G = Geom<T>;
  if constexpr (w == 0) {
    for (size_t i = 0; i < kBlk; ++i) out[i] = bias;
  } else {
#pragma GCC unroll 32
  for (unsigned row = 0; row < G::kRows; ++row) {
    const unsigned startBit = row * w, word = startBit / G::kT, shift = startBit % G::kT;
    const unsigned endWord = (startBit + w - 1) / G::kT;
    if (word == endWord) {
      for (unsigned lane = 0; lane < G::kLanes; ++lane)
        out[row * G::kLanes + lane] = static_cast<T>(
            ((packed[word * G::kLanes + lane] >> shift) & MaskFor<T>(w)) + bias);
    } else {
      const unsigned lowBits = G::kT - shift;
      for (unsigned lane = 0; lane < G::kLanes; ++lane) {
        const T lo = static_cast<T>(packed[word * G::kLanes + lane] >> shift);
        const T hi = static_cast<T>(packed[endWord * G::kLanes + lane] << lowBits);
        out[row * G::kLanes + lane] = static_cast<T>(((lo | hi) & MaskFor<T>(w)) + bias);
      }
    }
  }
  }
}

// Per-block widths, so the compile-time width has to be reached through a
// switch. Every arm in this file pays one such switch per block.
#define CASE_UNPACK(W)                                                        \
  case W:                                                                     \
    UnpackInterleaved<T, W>(pk + off, out + b * kBlk, bias);                  \
    break;
template <typename T>
__attribute__((noinline)) static void IntlvBlocks(const T* pk,
                                                  const std::vector<size_t>& offs,
                                                  const Frame& f, T* out) {
  for (size_t b = 0; b < kBlocks; ++b) {
    const size_t off = offs[b];
    const T bias = static_cast<T>(f.min[b]);
    switch (f.width[b]) {
      CASE_UNPACK(0) CASE_UNPACK(1) CASE_UNPACK(2) CASE_UNPACK(3) CASE_UNPACK(4)
      CASE_UNPACK(5) CASE_UNPACK(6) CASE_UNPACK(7) CASE_UNPACK(8)
      default:
        if constexpr (sizeof(T) >= 2) {
          switch (f.width[b]) {
            CASE_UNPACK(9) CASE_UNPACK(10) CASE_UNPACK(11) CASE_UNPACK(12)
            CASE_UNPACK(13) CASE_UNPACK(14) CASE_UNPACK(15) CASE_UNPACK(16)
            default:
              if constexpr (sizeof(T) == 4) {
                switch (f.width[b]) {
                  CASE_UNPACK(17) CASE_UNPACK(18) CASE_UNPACK(19) CASE_UNPACK(20)
                  CASE_UNPACK(21) CASE_UNPACK(22) CASE_UNPACK(23) CASE_UNPACK(24)
                  CASE_UNPACK(25) CASE_UNPACK(26) CASE_UNPACK(27) CASE_UNPACK(28)
                  CASE_UNPACK(29) CASE_UNPACK(30) CASE_UNPACK(31) CASE_UNPACK(32)
                  default:
                    abort();
                }
              } else {
                abort();
              }
          }
        } else {
          abort();
        }
    }
  }
}

// ----------------------------------------------------------------- continuous

// One LSB-first stream per block, block-aligned, which is how a page decoder
// walks them.
static std::vector<uint8_t> PackContinuous(const std::vector<uint64_t>& res,
                                           const Frame& f,
                                           std::vector<size_t>* byte_offs) {
  size_t total = 0;
  byte_offs->resize(kBlocks);
  for (size_t b = 0; b < kBlocks; ++b) {
    (*byte_offs)[b] = total;
    total += static_cast<size_t>(f.width[b]) * kBlk / 8;
  }
  std::vector<uint8_t> bytes(total + 64, 0);
  for (size_t b = 0; b < kBlocks; ++b) {
    const unsigned w = f.width[b];
    if (w == 0) continue;
    uint8_t* out = bytes.data() + (*byte_offs)[b];
    uint64_t acc = 0;
    int bits = 0;
    for (size_t t = 0; t < kBlk; ++t) {
      acc |= (res[b * kBlk + t] & ((w == 64) ? ~0ull : ((1ull << w) - 1))) << bits;
      bits += static_cast<int>(w);
      while (bits >= 8) {
        *out++ = static_cast<uint8_t>(acc);
        acc >>= 8;
        bits -= 8;
      }
    }
    if (bits) *out = static_cast<uint8_t>(acc);
  }
  return bytes;
}

template <typename T, bool kScalar>
__attribute__((noinline)) static void SeqBlocks(const uint8_t* src, size_t total,
                                                const std::vector<size_t>& offs,
                                                const Frame& f, T* out) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int>(kBlk);
  for (size_t b = 0; b < kBlocks; ++b) {
    const T bias = static_cast<T>(f.min[b]);
    if (f.width[b] == 0) {
      for (size_t t = 0; t < kBlk; ++t) out[b * kBlk + t] = bias;
      continue;
    }
    o.bit_width = f.width[b];
    // The vectorized kernel stops short of any byte it has not been told is
    // readable, so the bound names bytes past this block: a real reader can,
    // because the packed stream continues to the end of the page.
    o.max_read_bytes = static_cast<int>(total - offs[b]);
    if constexpr (kScalar) {
      bp::unpack_jump<KernelScalar, /*kHasBias=*/true>(src + offs[b], out + b * kBlk, o,
                                                       bias);
    } else {
      arrow::internal::unpack_bias<T>(src + offs[b], out + b * kBlk, o, bias);
    }
  }
}

// -------------------------------------------------------------------- driving

struct Result {
  double valcyc[3];  // seq_scal, seq_simd, intlv
};

// Times one arm and returns values per cycle, or 0 if it failed validation.
template <typename T>
static double Time(const std::function<void()>& run, const std::vector<T>& ref, T* out) {
  std::memset(out, 0xCD, kN * sizeof(T));
  run();
  if (!std::equal(ref.begin(), ref.end(), out)) return 0.0;
  const size_t iters = std::max<size_t>(1, kBytesPerRun / (kN * sizeof(T)));
  double best = 1e30;
  for (int t = 0; t < kTrials; ++t) {
    auto t0 = Clock::now();
    for (size_t i = 0; i < iters; ++i) run();
    best = std::min(best, std::chrono::duration<double>(Clock::now() - t0).count() /
                              static_cast<double>(iters));
  }
  return static_cast<double>(kN) / best / (g_ghz * 1e9);
}

// One output element width for one column, all three arms.
template <typename T>
static Result RunAt(const std::vector<int32_t>& values, const Frame& f) {
  using G = Geom<T>;
  Result r{};
  std::vector<T> ref(kN);
  std::vector<uint64_t> res(kN);
  for (size_t b = 0; b < kBlocks; ++b)
    for (size_t t = 0; t < kBlk; ++t) {
      const size_t i = b * kBlk + t;
      ref[i] = static_cast<T>(values[i]);
      res[i] = static_cast<uint64_t>(static_cast<int64_t>(values[i]) - f.min[b]);
    }

  // Interleaved: the container word follows the output element, so the grid is
  // 128x8 at one byte, 64x16 at two, 32x32 at four.
  std::vector<size_t> intlv_offs(kBlocks);
  size_t words = 0;
  for (size_t b = 0; b < kBlocks; ++b) {
    intlv_offs[b] = words;
    words += static_cast<size_t>(f.width[b]) * G::kLanes;
  }
  std::vector<T> pk(words + 64, 0);
  {
    std::vector<T> resT(kN);
    for (size_t i = 0; i < kN; ++i) resT[i] = static_cast<T>(res[i]);
    for (size_t b = 0; b < kBlocks; ++b) {
      const size_t off = intlv_offs[b];
      T* dst = pk.data();
      switch (f.width[b]) {
        case 0:
          break;
#define CASE_PACK_T(W)                                                       \
  case W:                                                                    \
    PackInterleaved<T, W>(resT.data() + b * kBlk, dst + off);                 \
    break;
          CASE_PACK_T(1) CASE_PACK_T(2) CASE_PACK_T(3) CASE_PACK_T(4) CASE_PACK_T(5)
          CASE_PACK_T(6) CASE_PACK_T(7) CASE_PACK_T(8)
        default:
          if constexpr (sizeof(T) >= 2) {
            switch (f.width[b]) {
              CASE_PACK_T(9) CASE_PACK_T(10) CASE_PACK_T(11) CASE_PACK_T(12)
              CASE_PACK_T(13) CASE_PACK_T(14) CASE_PACK_T(15) CASE_PACK_T(16)
              default:
                if constexpr (sizeof(T) == 4) {
                  switch (f.width[b]) {
                    CASE_PACK_T(17) CASE_PACK_T(18) CASE_PACK_T(19) CASE_PACK_T(20)
                    CASE_PACK_T(21) CASE_PACK_T(22) CASE_PACK_T(23) CASE_PACK_T(24)
                    CASE_PACK_T(25) CASE_PACK_T(26) CASE_PACK_T(27) CASE_PACK_T(28)
                    CASE_PACK_T(29) CASE_PACK_T(30) CASE_PACK_T(31) CASE_PACK_T(32)
                    default:
                      abort();
                  }
                } else {
                  abort();
                }
            }
          } else {
            abort();
          }
      }
    }
  }

  std::vector<size_t> byte_offs;
  const std::vector<uint8_t> seq = PackContinuous(res, f, &byte_offs);
  std::vector<T> out(kN + 64, 0);

  r.valcyc[0] = Time<T>(
      [&] { SeqBlocks<T, true>(seq.data(), seq.size(), byte_offs, f, out.data()); }, ref,
      out.data());
  r.valcyc[1] = Time<T>(
      [&] { SeqBlocks<T, false>(seq.data(), seq.size(), byte_offs, f, out.data()); }, ref,
      out.data());
  r.valcyc[2] = Time<T>([&] { IntlvBlocks<T>(pk.data(), intlv_offs, f, out.data()); },
                        ref, out.data());
  return r;
}

#define D(Name) {#Name, corpus::Gen##Name}
#define DS(Name) {#Name, corpus::delta_shapes::Gen##Name<int32_t>}

struct Dataset {
  const char* name;
  std::vector<int32_t> (*gen)(int64_t);
};

int main() {
  g_ghz = MeasureGhz();
  if (g_ghz <= 0.1) {
    fprintf(stderr, "could not measure the core clock\n");
    return 1;
  }
  const Dataset sets[] = {
      // ClickBench-inspired
      D(ClientIP), D(UrlRegionID), D(CounterID), D(EventDate), D(EventTime),
      D(GoodEvent), D(HID), D(HitColor), D(IPNetworkID), D(JavaEnable), D(OS),
      D(Resolution), D(TrafficSourceID), D(UserAgent),
      // TPC-DS
      D(TpcdsSoldDateSk), D(TpcdsStoreSk), D(TpcdsItemSk), D(TpcdsQuantity),
      D(TpcdsCustomerSk), D(TpcdsExtSalesPrice), D(TpcdsNetProfit), D(TpcdsDYear),
      // TPC-H
      D(TpchLQuantity), D(TpchLExtendedPrice), D(TpchLDiscount), D(TpchLShipDate),
      // NYC taxi
      D(TaxiPickupUnixTime), D(TaxiTripDistanceX100), D(TaxiFareCents),
      // correlated / sorted
      D(SortedUnixTime), D(SortedKeyDups), D(MonotoneRowId), D(NearSortedUnixTime),
      // shapes with structure between neighbouring values
      DS(TrendJitter), DS(Sawtooth),
      {"MeasurementSeries", corpus::delta_shapes::GenMeasurement<int32_t>},
      DS(SortedKeys), DS(SensorDropouts), DS(IdsWithGaps), DS(RandomWalk),
      DS(EventMillis), DS(LowSentinel), DS(Bimodal),
  };
  const size_t nsets = sizeof(sets) / sizeof(sets[0]);

  printf("corpus_output_width -- 43 columns, right-sized output element vs 32-bit\n");
  printf("Core clock measured at %.2f GHz. %zu values per decode, L2-resident, frame "
         "applied.\n", g_ghz, kN);
  printf("Left block is values per cycle, right block GiB/s of output. narrow: the "
         "narrowest\nelement that holds every value of the column.\n\n");
  printf("%-22s %7s %5s  %7s %7s %7s  %7s %7s %7s   %8s %8s %8s\n", "column", "out_b",
         "avg_w", "scal", "simd", "intlv", "scalGiB", "simdGiB", "intlvGiB", "int/simd",
         "int/simd32", "narrowgain");

  int failures = 0;
  std::vector<double> r_narrow, r_wide, gain_intlv, gain_simd;
  for (size_t s = 0; s < nsets; ++s) {
    std::vector<int32_t> v = sets[s].gen(static_cast<int64_t>(kN));
    if (v.size() != kN) {
      fprintf(stderr, "%s returned %zu\n", sets[s].name, v.size());
      return 1;
    }
    const Frame f = FrameOf(v);
    double avg_w = 0;
    for (size_t b = 0; b < kBlocks; ++b) avg_w += f.width[b];
    avg_w /= static_cast<double>(kBlocks);

    int32_t mn = *std::min_element(v.begin(), v.end());
    int32_t mx = *std::max_element(v.begin(), v.end());
    // The narrowest element that holds every value, signed or unsigned: what an
    // INT(8) or INT(16) annotation on the column could legally declare.
    unsigned ob = 32;
    if (mn >= 0 ? mx <= 0xFF : (mn >= -128 && mx <= 127)) {
      ob = 8;
    } else if (mn >= 0 ? mx <= 0xFFFF : (mn >= -32768 && mx <= 32767)) {
      ob = 16;
    }

    const Result wide = RunAt<uint32_t>(v, f);
    Result narrow = wide;
    if (ob == 8) {
      narrow = RunAt<uint8_t>(v, f);
    } else if (ob == 16) {
      narrow = RunAt<uint16_t>(v, f);
    }
    for (int a = 0; a < 3; ++a) {
      if (wide.valcyc[a] == 0.0 || narrow.valcyc[a] == 0.0) {
        fprintf(stderr, "MISMATCH %s arm %d\n", sets[s].name, a);
        ++failures;
      }
    }

    const double gib = g_ghz * 1e9 * (ob / 8.0) / (1024.0 * 1024 * 1024);
    const double rn = narrow.valcyc[2] / narrow.valcyc[1];
    const double rw = wide.valcyc[2] / wide.valcyc[1];
    printf("%-22s %7u %5.1f  %7.2f %7.2f %7.2f  %7.1f %7.1f %7.1f   %7.2fx %7.2fx "
           "%7.2fx\n",
           sets[s].name, ob, avg_w, narrow.valcyc[0], narrow.valcyc[1], narrow.valcyc[2],
           narrow.valcyc[0] * gib, narrow.valcyc[1] * gib, narrow.valcyc[2] * gib, rn, rw,
           narrow.valcyc[2] / wide.valcyc[2]);
    fflush(stdout);
    r_narrow.push_back(rn);
    r_wide.push_back(rw);
    gain_intlv.push_back(narrow.valcyc[2] / wide.valcyc[2]);
    gain_simd.push_back(narrow.valcyc[1] / wide.valcyc[1]);
  }

  auto geo = [](const std::vector<double>& x) {
    double s = 0;
    for (double v : x) s += std::log(v);
    return std::exp(s / static_cast<double>(x.size()));
  };
  printf("\ngeomean interleaved over shipped vectorized: %.3fx at right-sized output, "
         "%.3fx at 32-bit\n", geo(r_narrow), geo(r_wide));
  printf("geomean narrowing gain: interleaved %.3fx, shipped vectorized %.3fx\n",
         geo(gain_intlv), geo(gain_simd));
  return failures == 0 ? 0 : 1;
}
