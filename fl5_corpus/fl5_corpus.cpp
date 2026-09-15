// fl5_corpus -- five bit-unpacking arms, 43 real-shaped columns, three working sets.
//
// This measures BIT-UNPACKING ONLY. Frame-of-reference addition is present in
// every arm (it is one broadcast add fused into the unpack, and all five decode
// arms do
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
//   1. All five decode arms are verified bit-exact against the generated
//      values before anything is timed. A fast wrong answer is not a result.
//   2. intlv, fl_unpk and fl_tpos call the SAME PackBlock<W>/UnpackBlock<W>.
//      They differ only in how encode filled the grid, which is offline and
//      free. So fl_unpk/intlv MUST measure 1.00x. If it does not, the harness is
//      broken and every other number in the run is suspect. It is printed on
//      every line for exactly that reason.
//
// All arms write into ONE process-wide 4096-aligned buffer. Letting each arm
// own a std::vector makes output-address-mod-4096 a bigger effect than any
// layout difference being measured, which cost this project several days of
// wrong conclusions.

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
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

// One process-wide, 4096-aligned arena. EVERY buffer this harness touches --
// all three input payloads and all output slices -- is carved out of it at a
// 4096-byte-aligned offset.
//
// The earlier version shared only the OUTPUT buffer and let each arm's input be
// its own std::vector. That left input-address-mod-4096 free to vary between
// arms and, worse, to vary with working set, because the allocator returns
// differently-placed blocks as the request grows. It produced a ratio that
// swung from 1.36x to 0.47x between two ADJACENT bit widths (11 vs 12) on
// datasets of identical shape, at the largest point and nowhere else -- an
// address artifact reported as a memory-hierarchy result.
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

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------
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
    // The vectorized kernel over-reads by design and stops short of any byte it
    // has not been told is readable, so leaving max_read_bytes at its -1 default
    // pushes the tail of EVERY vector onto a one-value-at-a-time scalar epilog.
    // A block needs exactly 128*w bytes, which is what -1 deduces, so quoting
    // 128*w here would change nothing: the bound has to name bytes PAST the
    // block for the last vector step to be allowed. A real reader can, and does
    // -- the packed stream continues to the end of the page -- so what is passed
    // is the distance from this block to the end of the payload, the same
    // quantity the production decoder derives from its page span.
    const size_t readable = p.bytes.size() - p.offsets[b];
    o.max_read_bytes = static_cast<int>(
        std::min<size_t>(readable, static_cast<size_t>(std::numeric_limits<int>::max())));
    if (kScalar) {
      bp::unpack_jump<KernelScalar, /*kHasBias=*/true>(
          base + p.offsets[b], dst, o, bias);
    } else {
      arrow::internal::unpack_bias<uint32_t>(base + p.offsets[b], dst, o, bias);
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
  double gibs[6];
  double cr;       // 32 / mean bits per value, from the container encoding
  double avg_w;
  double src_mib;  // distinct packed bytes streamed
  double dst_mib;  // distinct output bytes written
};

static constexpr int kArms = 6;
static const char* kArm[kArms] = {"seq_scal", "seq_simd", "intlv",
                                  "fl_unpk", "fl_tpos", "pure_st"};

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
  // The ladder is built around the unit a Parquet reader actually decodes: one
  // data page, which defaults to 1 MiB of ENCODED bytes and is therefore always
  // small. A 32-MiB decode call does not occur in any reader, so growing `n` to
  // 32 MiB does not model "out of cache" -- it models nothing.
  //
  // What genuinely does leave cache is the STREAM: a scan walks page after page
  // and never revisits one, so every page's input is cold however small the page
  // is. So this ladder holds the decode call at page scale and grows the stream
  // instead, by rotating over `src_copies` distinct copies of the payload. It
  // also names both footprints on every line, because labelling a row by its
  // output size while the input moves too is how the previous ladder produced a
  // number nobody could attribute.
  struct Point {
    const char* name;
    size_t n;           // values per decode call -- the page-scale decode unit
    size_t src_target;  // bytes of DISTINCT packed input to rotate over (0 = one copy)
    size_t dst_target;  // bytes of DISTINCT output to rotate over (0 = one copy)
  };
  const Point kPoints[] = {
      {"page16k", 4 * kBlk, 0, 0},
      {"page256k", 64 * kBlk, 0, 0},
      {"page1m", 256 * kBlk, 0, 0},               // ~1 MiB out: a full default page
      {"scan4m", 256 * kBlk, 4ull << 20, 0},      // same page, 4 MiB of cold source
      {"scan48m", 256 * kBlk, 48ull << 20, 0},    // same page, source past this L3
      {"batch48m", 256 * kBlk, 0, 48ull << 20},   // same page, destination past L3
  };

  const char* only = (argc > 1) ? argv[1] : nullptr;
  const char* csv_path = (argc > 2) ? argv[2] : nullptr;
  FILE* csv = csv_path ? fopen(csv_path, "w") : nullptr;
  if (csv)
    fprintf(csv,
            "dataset,point,n,avg_bit_width,cr,src_mib,dst_mib,seq_scal,seq_simd,"
            "intlv,fl_unpk,fl_tpos,pure_st\n");

  printf("# fl5_corpus -- bit-unpacking only, no delta anywhere\n");
  printf("# five decode arms plus a store ceiling, best-of-%d\n", kReps);
  printf("# pure_st writes the same bytes to the same place with no unpacking,\n"
         "# so it is the ceiling the other five are read against.\n");
#ifdef ARROW_FASTLANES_FUSED_FL_UNPACK
  printf("# fl_tpos: FUSED in-register transpose (UnpackBlockFlToFileOrder)\n");
#else
  printf("# fl_tpos: UNFUSED fallback -- UnpackBlock into a 4 KiB scratch grid,\n"
         "#          then Transpose32x32 out of it. No fused kernel on this target,\n"
         "#          so this arm is a LOWER BOUND and not the layout's real cost.\n");
#endif
  printf("# %zu datasets x %zu working sets\n#\n", kNumDatasets,
         sizeof(kPoints) / sizeof(kPoints[0]));

  const char* hdr_fmt =
      "%-22s %-9s %5s %5s %8s %8s | %8s %8s %8s %8s %8s %8s |"
      " %8s %8s %8s %8s %8s %8s\n";
  printf(hdr_fmt, "dataset", "point", "W", "cr", "srcMiB", "dstMiB", kArm[0], kArm[1],
         kArm[2], kArm[3], kArm[4], kArm[5], "unpk/sc", "unpk/sd", "unpk/int",
         "tpos/sd", "sd/ceil", "int/ceil");
  std::string dashes(184, '-');
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

      // --- one arena, every buffer page-aligned inside it ---------------------
      // All three payload strides are page-rounded, so copy k of each arm's
      // input sits at the same offset mod 4096. Input placement is therefore
      // held fixed across arms and across working sets instead of being left to
      // the allocator.
      // Strides are page-rounded and then SKEWED by one page. Without the skew a
      // 1 MiB output slice stride is an exact power of two, so every slice maps
      // onto the same cache sets and the largest point measures set conflicts
      // rather than bandwidth -- it read 0.36x on one column and 1.55x on
      // another of the same width. One page of skew walks the set index forward
      // per slice and costs 0.4% of footprint.
      const size_t kSkew = 4096;
      const size_t seq_stride = RoundUpPage(seq.bytes.size()) + kSkew;
      const size_t ib_stride = RoundUpPage(ib_len) + kSkew;
      const size_t fb_stride = RoundUpPage(fb_len) + kSkew;
      const size_t out_stride = RoundUpPage(n * sizeof(int32_t)) + kSkew;
      auto copies_for = [](size_t target, size_t unit) {
        return (target == 0 || unit == 0) ? size_t{1}
                                          : std::max<size_t>(1, (target + unit - 1) / unit);
      };
      // The source footprint is set by the largest of the three payloads so all
      // three arms stream the same number of distinct bytes.
      const size_t src_unit = std::max(seq_stride, std::max(ib_stride, fb_stride));
      const size_t src_copies = copies_for(pt.src_target, src_unit);
      const size_t dst_copies = copies_for(pt.dst_target, out_stride);

      uint8_t* arena = Arena(src_copies * (seq_stride + ib_stride + fb_stride) +
                             dst_copies * out_stride);
      // The output goes first, so out_base is the same address for every dataset
      // and every point. Placing it after the payloads made its offset a
      // function of the payload sizes, which is a per-dataset variable in a
      // measurement that is supposed to hold placement fixed.
      int32_t* out_base = reinterpret_cast<int32_t*>(arena);
      uint8_t* seq_base = arena + dst_copies * out_stride;
      uint8_t* ib_base = seq_base + src_copies * seq_stride;
      uint8_t* fb_base = ib_base + src_copies * ib_stride;
      for (size_t k = 0; k < src_copies; ++k) {
        memcpy(seq_base + k * seq_stride, seq.bytes.data(), seq.bytes.size());
        memcpy(ib_base + k * ib_stride, ib.data(), ib_len);
        memcpy(fb_base + k * fb_stride, fb.data(), fb_len);
      }
      const double src_mib =
          static_cast<double>(src_copies * src_unit) / (1024.0 * 1024);
      const double dst_mib =
          static_cast<double>(dst_copies * out_stride) / (1024.0 * 1024);

      // Each arm decodes copy (it % src_copies) into slice (it % dst_copies), so
      // a "scan" point really does touch distinct cold bytes every iteration.
      auto slot_out = [&](size_t it) {
        return reinterpret_cast<int32_t*>(reinterpret_cast<uint8_t*>(out_base) +
                                         (it % dst_copies) * out_stride);
      };
      auto a_seq_scal = [&](size_t it) {
        DecodeSeq<true>(seq, seq_base + (it % src_copies) * seq_stride, n, slot_out(it));
      };
      auto a_seq_simd = [&](size_t it) {
        DecodeSeq<false>(seq, seq_base + (it % src_copies) * seq_stride, n, slot_out(it));
      };
      auto a_intlv = [&](size_t it) {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFileOrder>(
            ib_base + (it % src_copies) * ib_stride, n, slot_out(it));
      };
      auto a_fl_unpk = [&](size_t it) {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrderRaw>(
            fb_base + (it % src_copies) * fb_stride, n, slot_out(it));
      };
      auto a_fl_tpos = [&](size_t it) {
        fl::InterleavedPforDecode<InterleavedPforOrder::kFlOrder>(
            fb_base + (it % src_copies) * fb_stride, n, slot_out(it));
      };
      // Speed of light. Writes the same n int32s to the same rotating destination
      // slice with no unpacking whatsoever, so it is the ceiling every other arm
      // is measured against. It reads nothing from the packed source, which is
      // exactly why it is a ceiling and not a sixth decoder: at the scan points
      // the other arms are also paying for a cold source stream this arm never
      // touches. `bias + t` varies per element on purpose -- a constant store
      // would let the compiler call memset, which may pick rep-stos or a
      // non-temporal path, and non-temporal stores measured 0.85x of ordinary
      // ones here. The loop is one add per vector of stores, negligible against
      // the store stream itself.
      auto a_pure_st = [&](size_t it) {
        uint32_t* dst = reinterpret_cast<uint32_t*>(slot_out(it));
        for (size_t b = 0; b < n / kBlk; ++b) {
          const uint32_t bias = static_cast<uint32_t>(seq.mins[b]);
          uint32_t* q = dst + b * kBlk;
          for (size_t t = 0; t < kBlk; ++t) q[t] = bias + static_cast<uint32_t>(t);
        }
      };
      std::function<void(size_t)> arms[kArms] = {a_seq_scal, a_seq_simd, a_intlv,
                                                a_fl_unpk,  a_fl_tpos,  a_pure_st};
      int32_t* out = out_base;

      // --- correctness before speed -----------------------------------------
      // Four of the five decode arms must reproduce `values` exactly. fl_unpk
      // returns FL order on purpose, so it is checked against the FL_ORDER
      // permutation of `values` instead. pure_st is a store ceiling, not a
      // decoder, and is exempt.
      for (int a = 0; a < kArms; ++a) {
        memset(out, 0xCD, n * sizeof(int32_t));
        arms[a](0);
        if (a == 3) continue;  // fl_unpk: order-agnostic, checked below
        if (a == 5) continue;  // pure_st: a ceiling, not a decoder -- writes no
                               //          meaningful values by construction
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
        a_fl_unpk(0);
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
      // shortest run (L1, fastest arm) is still ~10 ms. The arms alternate
      // within each repetition, so drift or thermal effects hit all of them alike.
      // Enough iterations to move ~1 GiB and to visit every distinct copy.
      const size_t iters = std::max(std::max(src_copies, dst_copies),
                                    std::max<size_t>(3, kBytesPerRun / (n * 4)));
      double best[kArms] = {0, 0, 0, 0, 0, 0};
      for (int rep = 0; rep < kReps; ++rep) {
        for (int a = 0; a < kArms; ++a) {
          for (size_t k = 0; k < std::max(src_copies, dst_copies); ++k) arms[a](k);  // warm
          const auto t0 = Clock::now();
          for (size_t it = 0; it < iters; ++it) arms[a](it);
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
      for (int a = 0; a < kArms; ++a) row.gibs[a] = best[a];
      row.avg_w = avg_w;
      row.src_mib = src_mib;
      row.dst_mib = dst_mib;
      row.cr = static_cast<double>(n) * 4 / static_cast<double>(fb_len);
      rows.push_back(row);

      printf("%-22s %-9s %5.1f %5.2f %8.2f %8.2f | %8.1f %8.1f %8.1f %8.1f %8.1f"
             " %8.1f | %7.2fx %7.2fx %7.2fx %7.2fx %7.0f%% %7.0f%%\n",
             ds.name, pt.name, avg_w, row.cr, src_mib, dst_mib, best[0], best[1],
             best[2], best[3], best[4], best[5], best[3] / best[0], best[3] / best[1],
             best[3] / best[2], best[4] / best[1], 100 * best[1] / best[5],
             100 * best[2] / best[5]);
      fflush(stdout);
      if (csv) {
        fprintf(csv,
                "%s,%s,%zu,%.2f,%.4f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f\n",
                ds.name, pt.name, n, avg_w, row.cr, src_mib, dst_mib, best[0], best[1],
                best[2], best[3], best[4], best[5]);
        fflush(csv);
      }
    }
  }

  // --- geomeans per working set --------------------------------------------
  printf("%s\n", dashes.c_str());
  for (const Point& pt : kPoints) {
    double g[kArms] = {1, 1, 1, 1, 1, 1};
    int cnt = 0;
    for (const Row& r : rows) {
      if (strcmp(r.point, pt.name) != 0) continue;
      for (int a = 0; a < kArms; ++a) g[a] *= r.gibs[a];
      ++cnt;
    }
    if (cnt == 0) continue;
    double m[kArms];
    for (int a = 0; a < kArms; ++a) m[a] = std::pow(g[a], 1.0 / cnt);
    printf("%-22s %-9s %5s %5s %8s %8s | %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f |"
           " %7.2fx %7.2fx %7.2fx %7.2fx %7.0f%% %7.0f%%   (n=%d)\n",
           "GEOMEAN", pt.name, "", "", "", "", m[0], m[1], m[2], m[3], m[4], m[5],
           m[3] / m[0], m[3] / m[1], m[3] / m[2], m[4] / m[1], 100 * m[1] / m[5],
           100 * m[2] / m[5], cnt);
  }

  // The validity check, stated as a pass/fail rather than left to the reader.
  printf("\nvalidity: fl_unpk/intlv must be 1.00x (same PackBlock/UnpackBlock)\n");
  bool validity_failed = false;
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
    const double gm = std::exp(lg / cnt);
    const bool pass = std::fabs(std::log(worst)) < std::log(1.05);
    printf("  %-9s geomean %.3fx   worst %.3fx on %-20s  %s\n", pt.name, gm, worst,
           worst_ds.c_str(), pass ? "PASS" : "FAIL");
    if (!pass) validity_failed = true;
  }
  if (validity_failed) {
    printf("\nVALIDITY FAILED: fl_unpk and intlv run the same kernel over the same\n"
           "byte count and must tie within 5%%. A point where they do not is\n"
           "measuring its own buffer placement, not the layout. Do not quote it.\n");
  }
  if (csv) fclose(csv);
  return validity_failed ? 2 : 0;
}
