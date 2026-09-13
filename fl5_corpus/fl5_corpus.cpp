// fl5_corpus -- five bit-unpacking arms, 43 real-shaped columns, three working sets.
//
// This measures BIT-UNPACKING ONLY. Frame-of-reference addition is present in
// every arm (it is one broadcast add fused into the unpack, and all five arms do
// it), but there is no delta / prefix sum anywhere in this file. The question is
// exactly: for the same 1024-value blocks at the same bit widths, which physical
// bit-packing layout decodes fastest?
//
//   seq_scal  Parquet's layout (one continuous LSB-first stream per block),
//             decoded by Arrow's GENERATED SCALAR unpacker. This is the "naive
//             sequential bit-packing" baseline the FastLanes paper quotes its
//             order-of-magnitude against, so it is a column and not a footnote.
//   seq_simd  Identical bytes, decoded by Arrow's SHIPPED SIMD unpacker
//             (arrow::internal::unpack_bias, runtime-dispatched). This is what
//             Parquet actually runs today, and the only honest baseline for
//             "should Parquet change its layout".
//   intlv     FastLanes interleaved container: 1024 values as a 32x32 grid,
//             row-major across 32 lanes. Values come out in file order. No
//             permutation anywhere.
//   fl_unpk   Same container, but encode applied FL_ORDER's lane permutation.
//             Decoded to FL order and handed to the caller as-is -- the
//             order-agnostic consumer (scan / filter / aggregate). This arm
//             exists because the transpose has a cost and the layout deserves to
//             be priced without it.
//   fl_tpos   Byte-identical wire format to fl_unpk, decoded back to FILE order.
//             This is what a positional Parquet decoder must return.
//
// Two invariants this harness enforces rather than assumes:
//   1. All five arms are verified bit-exact against the generated values before
//      anything is timed. A fast wrong answer is not a result.
//   2. intlv, fl_unpk and fl_tpos call the SAME PackBlock<W>/UnpackBlock<W>.
//      They differ only in how encode filled the grid, which is offline and
//      free. So fl_unpk/intlv MUST measure 1.00x. If it does not, the harness is
//      broken and every other number in the run is suspect. It is printed on
//      every line for exactly that reason.
//
// All five arms decode into ONE process-wide 4096-aligned buffer. Letting each
// arm own a std::vector makes output-address-mod-4096 a bigger effect than any
// layout difference being measured, which cost this project several days of
// wrong conclusions.

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <functional>
#include <string>
#include <vector>

#include <arrow/util/bpacking_internal.h>
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_scalar_generated_internal.h"
#include "arrow/util/fastlanes/interleaved_pfor.h"

#include "corpus_generators.h"

namespace fl = arrow::util::fastlanes;
namespace bp = arrow::internal::bpacking;
using fl::InterleavedPforOrder;
using Clock = std::chrono::steady_clock;

// The generated scalar kernel family, named as bpacking_dispatch expects.
template <typename U, int W>
using KernelScalar = arrow::internal::ScalarUnpackerForWidth<U, W>;

constexpr size_t kBlk = 1024;
constexpr int kReps = 5;
constexpr size_t kBytesPerRun = 1024u * 1024 * 1024;  // output bytes per timed run

// ---------------------------------------------------------------------------
// The sequential (Parquet-shaped) payload.
//
// Per 1024-value block: the block minimum, the bit width needed for
// (value - min), and the residuals as one continuous LSB-first stream. That is
// the frame-of-reference PFOR layout Parquet's bit-packed pages use, and it is
// deliberately the SAME per-block min and SAME per-block width the FastLanes
// container picks, so the sequential and container arms decode the same values
// at the same widths and the only difference is the physical layout.
// ---------------------------------------------------------------------------
struct SeqPayload {
  std::vector<uint8_t> bytes;      // packed residuals, block-aligned
  std::vector<uint32_t> offsets;   // byte offset of each block's residuals
  std::vector<uint8_t> widths;
  std::vector<int32_t> mins;
};

static uint32_t WidthFor(uint32_t span) {
  uint32_t w = 0;
  while (w < 32 && (w == 32 || (span >> w) != 0)) ++w;
  return w;
}

static SeqPayload EncodeSequential(const std::vector<int32_t>& v) {
  const size_t nblocks = v.size() / kBlk;
  SeqPayload p;
  p.widths.resize(nblocks);
  p.mins.resize(nblocks);
  p.offsets.resize(nblocks);
  // Blocks are byte-aligned so each block can be handed to unpack_bias
  // independently -- which is also how a real page decoder walks them.
  size_t off = 0;
  for (size_t b = 0; b < nblocks; ++b) {
    int32_t mn = v[b * kBlk], mx = v[b * kBlk];
    for (size_t t = 1; t < kBlk; ++t) {
      mn = std::min(mn, v[b * kBlk + t]);
      mx = std::max(mx, v[b * kBlk + t]);
    }
    const uint32_t span = static_cast<uint32_t>(mx) - static_cast<uint32_t>(mn);
    const uint32_t w = WidthFor(span);
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

// One process-wide, 4096-aligned decode buffer shared by all five arms.
static int32_t* SharedOut(size_t n) {
  static int32_t* buf = nullptr;
  static size_t cap = 0;
  if (n > cap) {
    free(buf);
    void* raw = nullptr;
    if (posix_memalign(&raw, 4096, n * sizeof(int32_t) + 4096) != 0) abort();
    memset(raw, 0, n * sizeof(int32_t) + 4096);
    buf = static_cast<int32_t*>(raw);
    cap = n;
  }
  return buf;
}

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------
template <bool kScalar>
static void DecodeSeq(const SeqPayload& p, size_t n, int32_t* out) {
  arrow::internal::UnpackOptions o;
  o.batch_size = static_cast<int64_t>(kBlk);
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
    if (kScalar) {
      bp::unpack_jump<KernelScalar, /*kHasBias=*/true>(
          p.bytes.data() + p.offsets[b], dst, o, bias);
    } else {
      arrow::internal::unpack_bias<uint32_t>(p.bytes.data() + p.offsets[b], dst, o,
                                            bias);
    }
  }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
struct Row {
  std::string dataset;
  const char* point;
  size_t n;
  double gibs[5];
  double cr;       // 32 / mean bits per value, from the container encoding
  double avg_w;
};

static const char* kArm[5] = {"seq_scal", "seq_simd", "intlv", "fl_unpk", "fl_tpos"};

struct Dataset {
  const char* name;
  std::vector<int32_t> (*gen)(int64_t);
};

#define D(Name) {#Name, corpus::Gen##Name}
#define DS(Name) {#Name, corpus::delta_shapes::Gen##Name<int32_t>}
static const Dataset kDatasets[] = {
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
    DS(TrendJitter), DS(Sawtooth), {"MeasurementSeries", corpus::delta_shapes::GenMeasurement<int32_t>},
    DS(SortedKeys), DS(SensorDropouts), DS(IdsWithGaps), DS(RandomWalk),
    DS(EventMillis), DS(LowSentinel), DS(Bimodal),
};
#undef D
#undef DS
static constexpr size_t kNumDatasets = sizeof(kDatasets) / sizeof(kDatasets[0]);

int main(int argc, char** argv) {
  struct Point {
    const char* name;
    size_t n;
  };
  // 16 KiB fits in a 48 KiB L1d alongside the packed input; 400 KiB is L2 and
  // is also the size the Arrow corpus benchmark uses; 32 MiB is past any L2 and
  // (on a machine with a huge L3) is at least firmly out of the private caches.
  const Point kPoints[] = {
      {"L1", 4 * kBlk},          // 16 KiB out
      {"L2", 100 * kBlk},        // 400 KiB out
      {"DRAM", 8192 * kBlk},     // 32 MiB out
  };

  const char* only = (argc > 1) ? argv[1] : nullptr;
  const char* csv_path = (argc > 2) ? argv[2] : nullptr;
  FILE* csv = csv_path ? fopen(csv_path, "w") : nullptr;
  if (csv) fprintf(csv, "dataset,point,n,avg_bit_width,cr,seq_scal,seq_simd,intlv,fl_unpk,fl_tpos\n");

  printf("# fl5_corpus -- bit-unpacking only, no delta anywhere\n");
  printf("# five arms, one shared 4096-aligned output buffer, best-of-%d\n", kReps);
#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
  printf("# fl_tpos: FUSED in-register transpose (UnpackBlockFlToFileOrder)\n");
#else
  printf("# fl_tpos: UNFUSED fallback -- UnpackBlock into a 4 KiB scratch grid,\n"
         "#          then Transpose32x32 out of it. No fused kernel on this target,\n"
         "#          so this arm is a LOWER BOUND and not the layout's real cost.\n");
#endif
  printf("# %zu datasets x %zu working sets\n#\n", kNumDatasets,
         sizeof(kPoints) / sizeof(kPoints[0]));

  const char* hdr_fmt = "%-22s %-5s %5s %5s | %8s %8s %8s %8s %8s | %8s %8s %8s %8s\n";
  printf(hdr_fmt, "dataset", "point", "W", "cr", kArm[0], kArm[1], kArm[2], kArm[3],
         kArm[4], "unpk/sc", "unpk/sd", "unpk/int", "tpos/sd");
  std::string dashes(140, '-');
  printf("%s\n", dashes.c_str());

  std::vector<Row> rows;
  for (size_t d = 0; d < kNumDatasets; ++d) {
    const Dataset& ds = kDatasets[d];
    if (only && *only && strcmp(only, "all") != 0 && strstr(ds.name, only) == nullptr)
      continue;

    for (const Point& pt : kPoints) {
      const size_t n = pt.n;
      const std::vector<int32_t> values = ds.gen(static_cast<int64_t>(n));
      if (values.size() != n) {
        fprintf(stderr, "generator %s returned %zu for n=%zu\n", ds.name,
                values.size(), n);
        return 1;
      }

      const SeqPayload seq = EncodeSequential(values);
      std::vector<uint8_t> ib(fl::InterleavedPforMaxEncodedSize(n));
      std::vector<uint8_t> fb(fl::InterleavedPforMaxEncodedSize(n));
      const size_t ib_len =
          fl::InterleavedPforEncode<InterleavedPforOrder::kFileOrder>(values.data(), n,
                                                                     ib.data());
      const size_t fb_len =
          fl::InterleavedPforEncode<InterleavedPforOrder::kFlOrder>(values.data(), n,
                                                                   fb.data());
      if (ib_len == 0 || fb_len == 0) {
        fprintf(stderr, "encode failed for %s\n", ds.name);
        return 1;
      }
      if (ib_len != fb_len) {
        fprintf(stderr, "WARNING %s: interleaved and FL_ORDER wire sizes differ "
                        "(%zu vs %zu) -- these arms are not decoding the same "
                        "number of bytes\n", ds.name, ib_len, fb_len);
      }

      double avg_w = 0;
      for (uint8_t w : seq.widths) avg_w += w;
      avg_w /= static_cast<double>(seq.widths.size());

      int32_t* out = SharedOut(n);

      auto a_seq_scal = [&] { DecodeSeq<true>(seq, n, out); };
      auto a_seq_simd = [&] { DecodeSeq<false>(seq, n, out); };
      auto a_intlv = [&] {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFileOrder>(ib.data(), n, out);
      };
      auto a_fl_unpk = [&] {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrderRaw>(fb.data(), n, out);
      };
      auto a_fl_tpos = [&] {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrder>(fb.data(), n, out);
      };
      std::function<void()> arms[5] = {a_seq_scal, a_seq_simd, a_intlv, a_fl_unpk,
                                       a_fl_tpos};

      // --- correctness before speed -----------------------------------------
      // Four of the five arms must reproduce `values` exactly. fl_unpk returns
      // FL order on purpose, so it is checked against the FL_ORDER permutation
      // of `values` instead of against `values`.
      for (int a = 0; a < 5; ++a) {
        memset(out, 0xCD, n * sizeof(int32_t));
        arms[a]();
        if (a == 3) continue;  // fl_unpk: order-agnostic, checked below
        if (memcmp(out, values.data(), n * sizeof(int32_t)) != 0) {
          size_t bad = 0;
          while (bad < n && out[bad] == values[bad]) ++bad;
          fprintf(stderr, "MISMATCH %s/%s arm=%s at %zu: got %d want %d\n", ds.name,
                  pt.name, kArm[a], bad, out[bad], values[bad]);
          return 1;
        }
      }
      {
        // fl_unpk must equal the FL_ORDER permutation of the input: for each
        // block, lane l holds the contiguous run [32l, 32l+32).
        memset(out, 0xCD, n * sizeof(int32_t));
        a_fl_unpk();
        for (size_t b = 0; b < n / kBlk; ++b) {
          for (size_t lane = 0; lane < 32; ++lane) {
            for (size_t r = 0; r < 32; ++r) {
              const int32_t got = out[b * kBlk + r * 32 + lane];
              const int32_t want = values[b * kBlk + lane * 32 + r];
              if (got != want) {
                fprintf(stderr, "MISMATCH %s/%s arm=fl_unpk blk=%zu lane=%zu row=%zu: "
                                "got %d want %d\n", ds.name, pt.name, b, lane, r, got,
                        want);
                return 1;
              }
            }
          }
        }
      }

      // --- timing -----------------------------------------------------------
      // Every timed run moves ~1 GiB of output regardless of working set, so the
      // shortest run (L1, fastest arm) is still ~10 ms. The five arms alternate
      // within each repetition, so drift or thermal effects hit all of them alike.
      const size_t iters = std::max<size_t>(3, kBytesPerRun / (n * 4));
      double best[5] = {0, 0, 0, 0, 0};
      for (int rep = 0; rep < kReps; ++rep) {
        for (int a = 0; a < 5; ++a) {
          arms[a]();  // warm
          const auto t0 = Clock::now();
          for (size_t it = 0; it < iters; ++it) arms[a]();
          const auto t1 = Clock::now();
          const double secs = std::chrono::duration<double>(t1 - t0).count();
          const double gibs =
              static_cast<double>(iters) * n * 4 / secs / (1024.0 * 1024 * 1024);
          best[a] = std::max(best[a], gibs);
        }
      }

      Row row;
      row.dataset = ds.name;
      row.point = pt.name;
      row.n = n;
      for (int a = 0; a < 5; ++a) row.gibs[a] = best[a];
      row.avg_w = avg_w;
      row.cr = static_cast<double>(n) * 4 / static_cast<double>(fb_len);
      rows.push_back(row);

      printf("%-22s %-5s %5.1f %5.2f | %8.1f %8.1f %8.1f %8.1f %8.1f |"
             " %7.2fx %7.2fx %7.2fx %7.2fx\n",
             ds.name, pt.name, avg_w, row.cr, best[0], best[1], best[2], best[3],
             best[4], best[3] / best[0], best[3] / best[1], best[3] / best[2],
             best[4] / best[1]);
      fflush(stdout);
      if (csv) {
        fprintf(csv, "%s,%s,%zu,%.2f,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f\n", ds.name,
                pt.name, n, avg_w, row.cr, best[0], best[1], best[2], best[3], best[4]);
        fflush(csv);
      }
    }
  }

  // --- geomeans per working set --------------------------------------------
  printf("%s\n", dashes.c_str());
  for (const Point& pt : kPoints) {
    double g[5] = {1, 1, 1, 1, 1};
    int cnt = 0;
    for (const Row& r : rows) {
      if (strcmp(r.point, pt.name) != 0) continue;
      for (int a = 0; a < 5; ++a) g[a] *= r.gibs[a];
      ++cnt;
    }
    if (cnt == 0) continue;
    double m[5];
    for (int a = 0; a < 5; ++a) m[a] = std::pow(g[a], 1.0 / cnt);
    printf("%-22s %-5s %5s %5s | %8.1f %8.1f %8.1f %8.1f %8.1f |"
           " %7.2fx %7.2fx %7.2fx %7.2fx   (n=%d)\n",
           "GEOMEAN", pt.name, "", "", m[0], m[1], m[2], m[3], m[4], m[3] / m[0],
           m[3] / m[1], m[3] / m[2], m[4] / m[1], cnt);
  }

  // The validity check, stated as a pass/fail rather than left to the reader.
  printf("\nvalidity: fl_unpk/intlv must be 1.00x (same PackBlock/UnpackBlock)\n");
  for (const Point& pt : kPoints) {
    double worst = 1.0;
    std::string worst_ds;
    int cnt = 0;
    double lg = 0;
    for (const Row& r : rows) {
      if (strcmp(r.point, pt.name) != 0) continue;
      const double q = r.gibs[3] / r.gibs[2];
      lg += std::log(q);
      ++cnt;
      if (std::fabs(std::log(q)) > std::fabs(std::log(worst))) {
        worst = q;
        worst_ds = r.dataset;
      }
    }
    if (cnt == 0) continue;
    printf("  %-5s geomean %.3fx   worst %.3fx on %s\n", pt.name,
           std::exp(lg / cnt), worst, worst_ds.c_str());
  }
  if (csv) fclose(csv);
  return 0;
}
