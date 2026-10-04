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

// Visible round-trip proof for the OnPair port: decode both plain OnPair16 and
// OnPair16-dedup and check EVERY row equals the original bytes, then print a few
// concrete original -> decoded samples so a human can eyeball the recovery.
//
// Every dictionary budget in the valid 9..16 range is checked, not just 16: the
// packed decode loop is templated on the code width, and OnPair-auto picks a
// width per column, so verifying only 16 would leave the width the benchmarks
// actually report unverified. The merge threshold comes from bench_common.h so
// the dictionary trained here is the one the benchmarks measure.
//
// Exits non-zero if any row of any corpus at any width fails to round-trip, so
// this can gate a run.
//
// Build (one line): g++ -std=c++17 -O2 -Icpp/src
//   cpp/src/parquet/onpair/onpair.cc cpp/src/parquet/onpair/verify_roundtrip.cc -o /tmp/verify
// Run:   /tmp/verify bench-fsst-onpair/corpora/tpch_l_shipmode.txt [more files...]

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/onpair/bench_common.h"
#include "parquet/onpair/onpair.h"

namespace op = parquet::onpair;

namespace {

struct Corpus {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;
  size_t rows() const { return offsets.size() - 1; }
  std::string row(size_t i) const {
    return std::string(reinterpret_cast<const char*>(bytes.data() + offsets[i]),
                       offsets[i + 1] - offsets[i]);
  }
};

Corpus Read(const char* path) {
  Corpus c;
  std::ifstream in(path, std::ios::binary);
  c.offsets.push_back(0);
  std::string line;
  while (std::getline(in, line)) {
    c.bytes.insert(c.bytes.end(), line.begin(), line.end());
    c.offsets.push_back(static_cast<uint32_t>(c.bytes.size()));
  }
  return c;
}

// Compare a decoded concatenated buffer to the original, row by row.
// Returns the number of mismatching rows and the first mismatching index.
size_t CheckPerRow(const Corpus& c, const uint8_t* decoded, size_t dn, long* first_bad) {
  *first_bad = -1;
  if (dn != c.bytes.size()) {
    *first_bad = 0;
    return c.rows();
  }
  size_t bad = 0;
  for (size_t i = 0; i < c.rows(); ++i) {
    size_t off = c.offsets[i], len = c.offsets[i + 1] - off;
    if (std::memcmp(decoded + off, c.bytes.data() + off, len) != 0) {
      if (*first_bad < 0) *first_bad = static_cast<long>(i);
      ++bad;
    }
  }
  return bad;
}

std::string Trunc(const std::string& s, size_t n = 42) {
  return s.size() <= n ? s : s.substr(0, n) + "…";
}

void Samples(const Corpus& c, const uint8_t* decoded) {
  size_t r = c.rows();
  size_t idx[4] = {0, r / 3, (2 * r) / 3, r - 1};
  for (size_t k = 0; k < 4; ++k) {
    size_t i = idx[k];
    size_t off = c.offsets[i], len = c.offsets[i + 1] - off;
    std::string dec(reinterpret_cast<const char*>(decoded + off), len);
    std::string orig = c.row(i);
    std::printf("      row %-8zu original=%-44s decoded=%-44s %s\n", i,
                ("\"" + Trunc(orig) + "\"").c_str(), ("\"" + Trunc(dec) + "\"").c_str(),
                orig == dec ? "MATCH" : "*** MISMATCH ***");
  }
}

// Independent scalar reference decode: exact-length copies straight out of the
// stored dictionary, deliberately the dumbest loop that can be written. It shares
// no code with any shipped kernel, which is the point -- the kernels are checked
// against it rather than against each other.
std::vector<uint8_t> ReferenceDecode(const op::CompactDictionary& dict,
                                     const std::vector<uint16_t>& codes, size_t n) {
  std::vector<uint8_t> out;
  for (size_t i = 0; i < n; ++i) {
    const uint8_t* p = dict.token_ptr(codes[i]);
    out.insert(out.end(), p, p + dict.token_len(codes[i]));
  }
  return out;
}

// Decode is served by several kernels chosen by target features and by stream
// length, and they must be interchangeable to the byte. Checked here:
//
//   whole-column     DecompressInto, unpacked u16 codes
//   packed           DecompressPacked, building its own strided view
//   packed, prebuilt DecompressPacked against a view the caller built
//   packed, short    a prefix short enough to trip the guard that skips the view
//                    and decodes straight out of the stored dictionary
//
// The last one matters because nothing else reaches that path: real streams carry
// far more codes than tokens, so the fallback would otherwise never run here.
bool VerifyDecodePaths(const op::Column& col, const char* tag) {
  const size_t ncodes = col.codes.size();
  const size_t ntokens = col.dict.num_tokens();
  size_t bits = 1;
  while ((size_t{1} << bits) < ntokens) ++bits;
  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(cw.data(), cw.size(), bits);
  op::StridedDictionary view;
  view.Build(col.dict);

  std::vector<uint8_t> buf(op::DecodedLen(col) + op::kDecodePadding, 0);
  size_t bad = 0;
  auto agrees = [&](const char* what, const std::vector<uint8_t>& want, size_t got) {
    if (got == want.size() && std::memcmp(buf.data(), want.data(), want.size()) == 0) return;
    std::printf("      %s: disagrees with the scalar reference (%zu vs %zu bytes)\n", what,
                got, want.size());
    ++bad;
  };

  const std::vector<uint8_t> want_all = ReferenceDecode(col.dict, col.codes, ncodes);
  agrees("whole-column", want_all, op::DecompressInto(col, buf.data()));
  agrees("packed", want_all,
         op::DecompressPacked(col.dict, packed.data(), ncodes, bits, buf.data()));
  agrees("packed, prebuilt view", want_all,
         op::DecompressPacked(view, packed.data(), ncodes, bits, buf.data()));

  const size_t nshort = std::min(ncodes, ntokens == 0 ? 0 : ntokens - 1);
  if (nshort != 0) {
    agrees("packed, short stream", ReferenceDecode(col.dict, col.codes, nshort),
           op::DecompressPacked(col.dict, packed.data(), nshort, bits, buf.data()));
  }

  std::printf("    %-15s: 4 decode paths agree  (%zu tokens, %zub codes, max token %zu)  %s\n",
              tag, ntokens, bits, col.dict.max_token_len, bad == 0 ? "[OK]" : "[FAIL]");
  return bad == 0;
}

// OnPair (no dedup): compress then whole-column decode.
bool VerifyOnPair(const Corpus& c, uint8_t bits, double threshold, bool show_samples) {
  op::Config cfg{bits, threshold, 42};
  op::Column col = op::Compress(c.bytes.data(), c.bytes.size(), c.offsets.data(), c.rows(), cfg);
  std::vector<uint8_t> out(op::DecodedLen(col) + op::kDecodePadding, 0);
  size_t dn = op::DecompressInto(col, out.data());
  long bad_at;
  size_t bad = CheckPerRow(c, out.data(), dn, &bad_at);
  std::printf("    OnPair%-2u       : %zu/%zu rows exact  %s\n", bits, c.rows() - bad, c.rows(),
              bad == 0 ? "[OK]" : "[FAIL]");
  if (bad != 0) std::printf("      first mismatching row: %ld\n", bad_at);
  // Every width gets the four-path check, not just 16: each width instantiates a
  // different unpack kernel, so agreement at one width says nothing about another.
  char tag[32];
  std::snprintf(tag, sizeof(tag), "OnPair%u paths", static_cast<unsigned>(bits));
  bool paths = VerifyDecodePaths(col, tag);
  if (show_samples) Samples(c, out.data());
  return paths && bad == 0;
}

// The distinct-value set a dedup cascade OnPairs, plus the per-row ids into it.
// Built once per corpus and reused across widths -- deduplicating 500k rows is
// far more expensive than the training pass being verified.
struct Distinct {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets{0};
  std::vector<uint32_t> refs;
  size_t count() const { return offsets.size() - 1; }
};

Distinct BuildDistinct(const Corpus& c) {
  size_t n = c.rows();
  Distinct d;
  d.refs.resize(n);
  std::unordered_map<std::string, uint32_t> ids;
  for (size_t i = 0; i < n; ++i) {
    std::string v = c.row(i);
    auto it = ids.find(v);
    uint32_t id;
    if (it == ids.end()) {
      id = static_cast<uint32_t>(ids.size());
      d.bytes.insert(d.bytes.end(), v.begin(), v.end());
      d.offsets.push_back(static_cast<uint32_t>(d.bytes.size()));
      ids.emplace(std::move(v), id);
    } else {
      id = it->second;
    }
    d.refs[i] = id;
  }
  return d;
}

// OnPair-dedup: OnPair the distinct values, then gather every row back through
// its id. Checks the gather as well as the decode -- a correct dictionary paired
// with a broken reference column still reconstructs the wrong column.
bool VerifyOnPairDedup(const Corpus& c, const Distinct& d, uint8_t bits, double threshold,
                       bool show_samples) {
  op::Config cfg{bits, threshold, 42};
  op::Column col = op::Compress(d.bytes.data(), d.bytes.size(), d.offsets.data(), d.count(), cfg);
  std::vector<uint8_t> dbuf(op::DecodedLen(col) + op::kDecodePadding, 0);
  op::DecompressInto(col, dbuf.data());  // distinct values, concatenated in id order
  std::vector<uint8_t> out(c.bytes.size() + 16, 0);
  size_t w = 0;
  for (size_t i = 0; i < c.rows(); ++i) {
    uint32_t id = d.refs[i];
    size_t off = d.offsets[id], len = d.offsets[id + 1] - off;
    std::memcpy(out.data() + w, dbuf.data() + off, len);
    w += len;
  }
  long bad_at;
  size_t bad = CheckPerRow(c, out.data(), w, &bad_at);
  std::printf("    OnPair%-2u-dedup : %zu/%zu rows exact  (%zu distinct)  %s\n", bits,
              c.rows() - bad, c.rows(), d.count(), bad == 0 ? "[OK]" : "[FAIL]");
  if (bad != 0) std::printf("      first mismatching row: %ld\n", bad_at);
  if (show_samples) Samples(c, out.data());
  // A distinct-value dictionary is far smaller than a whole column's, so its codes
  // pack into a narrower width than OnPair ever picks -- this is where the low end
  // of the width dispatch gets exercised.
  return VerifyDecodePaths(col, "  ↳ paths") && bad == 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s <corpus.txt> [more...]\n", argv[0]);
    return 2;
  }
  int failures = 0;
  size_t rows_checked = 0;
  for (int a = 1; a < argc; ++a) {
    Corpus c = Read(argv[a]);
    if (c.rows() == 0) {
      std::fprintf(stderr, "%s: no rows read\n", argv[a]);
      ++failures;
      continue;
    }
    // Same rule the benchmarks use, so the trained dictionary matches theirs.
    double threshold = bench::ThresholdFor(std::filesystem::path(argv[a]).stem().string());
    std::printf("\n%s  (%zu rows, %.2f MiB, threshold %.2f)\n", argv[a], c.rows(),
                c.bytes.size() / (1024.0 * 1024.0), threshold);
    Distinct d = BuildDistinct(c);
    for (uint8_t bits = 9; bits <= 16; ++bits) {
      // Samples are the human-readable proof; print them once, at the width the
      // report's fixed-budget rows use, rather than eight times per corpus.
      bool show = (bits == 16);
      if (!VerifyOnPair(c, bits, threshold, show)) ++failures;
      if (!VerifyOnPairDedup(c, d, bits, threshold, show)) ++failures;
      rows_checked += 2 * c.rows();
    }
  }
  std::printf("\n%zu row comparisons, %d failure(s)\n", rows_checked, failures);
  return failures == 0 ? 0 : 1;
}
