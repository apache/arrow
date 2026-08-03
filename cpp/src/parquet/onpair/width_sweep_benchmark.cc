// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// Does a wider code make OnPair decode faster? Isolate the code width from
// everything else that changes with the dictionary budget.
//
// The obvious way to ask this -- compare OnPair-auto (which picks a budget per
// column) against OnPair16 -- cannot answer it. A narrower budget trains a
// SMALLER DICTIONARY, so it also changes the tokens, the token count per row, and
// the gather-copy width the decoder picks from max_token_len. Those move decode
// far more than the unpacking does, and they move in both directions, so the
// comparison is confounded and its answer is noise.
//
// This benchmark holds the dictionary fixed and varies only the packing width.
// For each training budget it takes the ONE trained dictionary and its ONE code
// stream, then bit-packs those same codes at every width from their true width up
// to 16 and times DecompressPacked at each. Identical tokens, identical code
// sequence, identical output bytes, identical copy width -- the only difference is
// how many bits each code occupies and which DecompressPackedFixedBits<> template
// the dispatch lands on. Storing a code in more bits than it needs is pure waste
// on the ratio axis, so any decode gain is the whole case for a wider code.
//
// It also prints the confounded comparison alongside, so the two can be read
// against each other: the "own width" column across training budgets is what a
// budget sweep sees, and the widen-in-place rows are what the width alone does.
//
// PIN AND QUIESCE: this is a timing benchmark. Run it under `taskset -c 0` with
// core 0 idle -- `ps -eo pid,psr,pcpu | awk '$2==0 && $3>5'` must print nothing.
//
// Build (one line):
//   g++ -std=c++17 -O3 -march=native -Icpp/src \
//     cpp/src/parquet/onpair/onpair.cc \
//     cpp/src/parquet/onpair/width_sweep_benchmark.cc \
//     /usr/lib64/libzstd.so.1 /usr/lib64/liblz4.so.1 -o /tmp/width_sweep
// Run:
//   taskset -c 0 /tmp/width_sweep /tmp/tmp/corpora30

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "parquet/onpair/bench_common.h"
#include "parquet/onpair/onpair.h"

namespace op = parquet::onpair;

namespace {

// Training budgets to build a dictionary at. Not all of 9..16: each one is a full
// training pass, and three points spanning the range (floor, middle, ceiling) show
// whether the width effect depends on dictionary size. The widths swept per
// dictionary are exhaustive, since that is the axis under test.
constexpr uint8_t kBudgets[] = {9, 12, 16};

struct WidthPoint {
  size_t bits = 0;
  double decode_mibs = 0;
  size_t codes_bytes = 0;  // bit-packed code stream, logical size
};

struct BudgetResult {
  uint8_t budget = 0;
  size_t num_tokens = 0;
  size_t true_bits = 0;    // ceil(log2 num_tokens) -- the width a real page stores
  size_t num_codes = 0;
  size_t max_token_len = 0;
  double bytes_per_token = 0;
  std::vector<WidthPoint> widths;

  const WidthPoint* at(size_t bits) const {
    for (const WidthPoint& w : widths) {
      if (w.bits == bits) return &w;
    }
    return nullptr;
  }
};

// Time DecompressPacked on one (dictionary, code stream, width), the same way
// RunCodec does: fresh output buffer per iteration allocated outside the timed
// region, median of kDecodeIters, throughput over the RAW bytes. Aborts if the
// decode does not reproduce the column exactly -- a width that unpacks to the
// wrong codes would otherwise read as a fast decode.
double TimeDecode(const bench::Corpus& c, const op::CompactDictionary& dict,
                  const std::vector<uint8_t>& packed, size_t num_codes, size_t bits) {
  const size_t out_cap = c.raw_bytes() + op::kDecodePadding + 64;
  {
    std::vector<uint8_t> out(out_cap, 0);
    size_t w = op::DecompressPacked(dict, packed.data(), num_codes, bits, out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "width %zu roundtrip mismatch on %s (w=%zu raw=%zu)\n", bits,
                   c.name.c_str(), w, c.raw_bytes());
      std::abort();
    }
  }
  std::vector<double> mibs;
  for (int it = 0; it < bench::kDecodeIters; ++it) {
    std::vector<uint8_t> out(out_cap, 0);
    auto t0 = bench::Clock::now();
    size_t w = op::DecompressPacked(dict, packed.data(), num_codes, bits, out.data());
    double dt = std::chrono::duration<double>(bench::Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    mibs.push_back(bench::Mib(c.raw_bytes()) / dt);
  }
  return bench::Median(std::move(mibs));
}

BudgetResult SweepBudget(const bench::Corpus& c, uint8_t budget, double threshold) {
  op::Config cfg{budget, threshold, 42};
  op::Column col =
      op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), c.n_rows(), cfg);

  BudgetResult r;
  r.budget = budget;
  r.num_tokens = col.dict.num_tokens();
  r.true_bits = bench::IndexBits(r.num_tokens);
  r.num_codes = col.codes.size();
  r.max_token_len = col.dict.max_token_len;
  r.bytes_per_token = static_cast<double>(c.raw_bytes()) / static_cast<double>(r.num_codes);

  // Widen once; PackValues takes u32. The same values are re-packed at each width,
  // so every width decodes the identical code sequence.
  std::vector<uint32_t> codes32(col.codes.begin(), col.codes.end());

  for (size_t bits = r.true_bits; bits <= 16; ++bits) {
    std::vector<uint8_t> packed = op::PackValues(codes32.data(), codes32.size(), bits);
    WidthPoint p;
    p.bits = bits;
    p.codes_bytes = bench::BitPackedBytes(r.num_codes, bits);
    p.decode_mibs = TimeDecode(c, col.dict, packed, r.num_codes, bits);
    r.widths.push_back(p);
  }
  return r;
}

void PrintCorpus(const bench::Corpus& c, const std::vector<BudgetResult>& results) {
  std::printf("\n%s  %zu rows, %.2f MiB raw\n", c.name.c_str(), c.n_rows(),
              bench::Mib(c.raw_bytes()));
  for (const BudgetResult& r : results) {
    std::printf(
        "  budget %2ub: %6zu tokens, true width %zu b, %zu codes (%.2f raw B/token), "
        "copy width %zu\n",
        r.budget, r.num_tokens, r.true_bits, r.num_codes, r.bytes_per_token, r.max_token_len);
    std::printf("      width :");
    for (const WidthPoint& w : r.widths) std::printf(" %8zu", w.bits);
    std::printf("\n      MiB/s :");
    for (const WidthPoint& w : r.widths) std::printf(" %8.0f", w.decode_mibs);
    std::printf("\n      vs true:");
    const WidthPoint* base = r.at(r.true_bits);
    for (const WidthPoint& w : r.widths) {
      std::printf(" %+7.1f%%", 100.0 * (w.decode_mibs - base->decode_mibs) / base->decode_mibs);
    }
    std::printf("\n      codes  :");
    for (const WidthPoint& w : r.widths) {
      std::printf(" %+7.1f%%",
                  100.0 * (static_cast<double>(w.codes_bytes) - base->codes_bytes) /
                      base->codes_bytes);
    }
    std::printf("   (bit-packed code stream, vs true width)\n");
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::filesystem::path> files = bench::CorpusFiles(bench::CorpusDir(argc, argv));
  if (files.empty()) {
    std::fprintf(stderr, "no .txt corpora found\n");
    return 2;
  }
  std::printf(
      "OnPair decode throughput vs CODE WIDTH ALONE -- one trained dictionary per\n"
      "budget, its code stream re-packed at each width from its true width to 16.\n"
      "Same tokens, same code sequence, same copy width; only the unpacking differs.\n"
      "'codes' is what the wider packing costs on the ratio axis.\n"
      "%d decode iterations, median. Pin to an idle core.\n",
      bench::kDecodeIters);

  // Per-corpus deltas for the summary: going from the true width to 16 bits, and
  // to true+1, on each dictionary.
  struct Delta {
    std::string corpus;
    uint8_t budget;
    size_t true_bits;
    double to_16;
    double to_plus1;
    double codes_cost_16;
  };
  std::vector<Delta> deltas;

  for (const std::filesystem::path& f : files) {
    bench::Corpus c = bench::ReadCorpus(f);
    if (c.n_rows() == 0) {
      std::fprintf(stderr, "%s: no rows\n", f.c_str());
      return 1;
    }
    double threshold = bench::ThresholdFor(c.name);
    std::vector<BudgetResult> results;
    for (uint8_t b : kBudgets) results.push_back(SweepBudget(c, b, threshold));
    PrintCorpus(c, results);
    std::fflush(stdout);

    for (const BudgetResult& r : results) {
      if (r.true_bits >= 16) continue;  // nothing to widen into
      const WidthPoint* base = r.at(r.true_bits);
      const WidthPoint* w16 = r.at(16);
      const WidthPoint* wp1 = r.at(r.true_bits + 1);
      deltas.push_back({c.name, r.budget, r.true_bits,
                        100.0 * (w16->decode_mibs - base->decode_mibs) / base->decode_mibs,
                        100.0 * (wp1->decode_mibs - base->decode_mibs) / base->decode_mibs,
                        100.0 * (static_cast<double>(w16->codes_bytes) - base->codes_bytes) /
                            base->codes_bytes});
    }
  }

  // Summary. The question is whether widening the code buys decode speed, so the
  // headline is the sign and size of the true-width -> 16-bit change, against what
  // that widening costs on the code stream.
  std::printf("\n\n=== Summary: decode change from widening the code, dictionary held fixed ===\n");
  std::printf("%-30s %6s %6s %10s %10s %12s\n", "corpus", "budget", "true b", "->true+1",
              "->16 b", "codes at 16b");
  std::vector<double> all16, allp1, cost16;
  for (const Delta& d : deltas) {
    std::printf("%-30s %5ub %5zub %+9.1f%% %+9.1f%% %+11.1f%%\n", d.corpus.c_str(), d.budget,
                d.true_bits, d.to_plus1, d.to_16, d.codes_cost_16);
    all16.push_back(d.to_16);
    allp1.push_back(d.to_plus1);
    cost16.push_back(d.codes_cost_16);
  }
  if (!all16.empty()) {
    auto stats = [](std::vector<double> v, const char* label) {
      std::sort(v.begin(), v.end());
      int faster = 0;
      for (double x : v) {
        if (x > 0) ++faster;
      }
      std::printf("  %-22s median %+6.1f%%  min %+6.1f%%  max %+6.1f%%  faster on %d/%zu\n", label,
                  bench::Median(v), v.front(), v.back(), faster, v.size());
    };
    std::printf("\n%zu (corpus, budget) pairs where the true width is below 16:\n", all16.size());
    stats(allp1, "decode, true -> true+1");
    stats(all16, "decode, true -> 16 b");
    std::vector<double> c16 = cost16;
    std::sort(c16.begin(), c16.end());
    std::printf("  %-22s median %+6.1f%%  min %+6.1f%%  max %+6.1f%%\n", "code stream at 16 b",
                bench::Median(c16), c16.front(), c16.back());
  }
  return 0;
}
