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

// Standalone C++ comparison of FSST (Arrow PR #48232 vendored codec), a C++
// OnPair implementation, zstd level 1 and lz4, in one process on identical
// string corpora. Reports compression ratio and encode/decode throughput.
// Corpora are produced by the Rust bench-fsst-onpair/ generator (--dump-corpora).
//
// All codecs run single-threaded; pin the process with `taskset -c 0`, and run
// it twice using the warm second run.
//
// Build (from the Arrow repo root), one line:
//   g++ -std=c++17 -O3 -march=native -Icpp/src -Icpp/thirdparty/fsst
//   cpp/thirdparty/fsst/libfsst.cpp cpp/thirdparty/fsst/fsst_avx512.cpp
//   cpp/src/parquet/onpair/onpair.cc cpp/src/parquet/onpair/prefix_plus.cc
//   cpp/src/parquet/onpair/fsst_onpair_benchmark.cc
//   /usr/lib64/libzstd.so.1 /usr/lib64/liblz4.so.1 -o /tmp/fsst_onpair_bench
//
// Run:  taskset -c 0 /tmp/fsst_onpair_bench <corpora_dir>
//   (corpora_dir defaults to $ONPAIR_BENCH_DIR, then ./bench-fsst-onpair/corpora)

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "fsst.h"
#include "parquet/onpair/bench_common.h"
#include "parquet/onpair/onpair.h"
#include "parquet/onpair/prefix_plus.h"

namespace op = parquet::onpair;

namespace {

// Corpus / Measured / Mib / Median / BitWidth / IndexBits / BitPackedBytes /
// Clock / the iteration counts / ThresholdFor - shared with cascade_benchmark.cc
// so both binaries account bytes identically.
using namespace bench;  // NOLINT(build/namespaces)

// FSST

// Compress the whole corpus into one packed buffer; returns compressed bytes,
// per-row lengths, and the serialized symbol-table size.
struct FsstEncoded {
  std::vector<uint8_t> output;   // packed compressed bytes
  size_t total = 0;              // used bytes in `output`
  size_t table_bytes = 0;        // fsst_export size (symbol table)
};

FsstEncoded FsstEncode(const Corpus& c) {
  size_t n = c.n_rows();
  std::vector<size_t> lenIn(n);
  std::vector<const unsigned char*> strIn(n);
  for (size_t i = 0; i < n; ++i) {
    lenIn[i] = c.offsets[i + 1] - c.offsets[i];
    strIn[i] = c.bytes.data() + c.offsets[i];
  }
  fsst_encoder_t* enc = fsst_create(n, lenIn.data(), strIn.data(), 0);

  // Conservative per-string bound from fsst.h: 7 + 2*len.
  size_t out_cap = 7 * n + 2 * c.raw_bytes() + 16;
  FsstEncoded e;
  e.output.resize(out_cap);
  std::vector<size_t> lenOut(n);
  std::vector<unsigned char*> strOut(n);
  size_t done = fsst_compress(enc, n, lenIn.data(), strIn.data(), out_cap, e.output.data(),
                              lenOut.data(), strOut.data());
  if (done != n) {
    std::fprintf(stderr, "FSST: only compressed %zu/%zu rows\n", done, n);
    std::abort();
  }
  e.total = 0;
  for (size_t i = 0; i < n; ++i) e.total += lenOut[i];

  unsigned char table[FSST_MAXHEADER];
  e.table_bytes = fsst_export(enc, table);
  fsst_destroy(enc);
  return e;
}

Measured RunFsst(const Corpus& c) {
  size_t n = c.n_rows();
  FsstEncoded e = FsstEncode(c);
  Measured m;
  m.label = "FSST";
  m.compressed_bytes = e.table_bytes + e.total + c.len_array_bytes();

  // Encode throughput (train + compress).
  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    FsstEncoded tmp = FsstEncode(c);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(tmp.total) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Rebuild a decoder from the packed stream for decode timing.
  std::vector<size_t> lenIn(n);
  std::vector<const unsigned char*> strIn(n);
  for (size_t i = 0; i < n; ++i) {
    lenIn[i] = c.offsets[i + 1] - c.offsets[i];
    strIn[i] = c.bytes.data() + c.offsets[i];
  }
  fsst_encoder_t* enc2 = fsst_create(n, lenIn.data(), strIn.data(), 0);
  fsst_decoder_t dec = fsst_decoder(enc2);
  fsst_destroy(enc2);

  size_t cap = c.raw_bytes() + 16;
  // Correctness: decoding the whole packed stream reconstructs the input.
  {
    std::vector<uint8_t> out(cap);
    size_t w = fsst_decompress(&dec, e.total, e.output.data(), cap, out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "FSST roundtrip mismatch on %s (w=%zu raw=%zu)\n", c.name.c_str(), w,
                   c.raw_bytes());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> out(cap);
    auto t0 = Clock::now();
    size_t w = fsst_decompress(&dec, e.total, e.output.data(), cap, out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// zstd (block-compression baseline; no random access)

// Compresses the whole concatenated corpus in one frame at the given level.
// Unlike FSST/OnPair this gives no per-row random access - it's a reference for
// what a general-purpose block compressor achieves on the same bytes.
Measured RunZstd(const Corpus& c, int level) {
  size_t bound = ZSTD_compressBound(c.raw_bytes());
  std::vector<uint8_t> comp(bound);
  size_t csize = ZSTD_compress(comp.data(), bound, c.bytes.data(), c.raw_bytes(), level);
  if (ZSTD_isError(csize)) {
    std::fprintf(stderr, "zstd compress error on %s\n", c.name.c_str());
    std::abort();
  }
  Measured m;
  m.label = "zstd(" + std::to_string(level) + ")";
  // Add (n+1) u32 row offsets so row recovery is accounted for, as with the
  // other codecs (zstd's frame decompresses to concatenated plaintext only).
  m.compressed_bytes = csize + c.len_array_bytes();

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    size_t r = ZSTD_compress(comp.data(), bound, c.bytes.data(), c.raw_bytes(), level);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(r) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  size_t cap = c.raw_bytes() + 16;
  {
    std::vector<uint8_t> out(cap);
    size_t w = ZSTD_decompress(out.data(), cap, comp.data(), csize);
    if (ZSTD_isError(w) || w != c.raw_bytes() ||
        std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "zstd roundtrip mismatch on %s\n", c.name.c_str());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> out(cap);
    auto t0 = Clock::now();
    size_t w = ZSTD_decompress(out.data(), cap, comp.data(), csize);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// lz4 (fast block-compression baseline; no random access)

Measured RunLz4(const Corpus& c) {
  int raw = static_cast<int>(c.raw_bytes());
  int bound = LZ4_compressBound(raw);
  std::vector<char> comp(bound);
  int csize = LZ4_compress_default(reinterpret_cast<const char*>(c.bytes.data()), comp.data(), raw,
                                   bound);
  if (csize <= 0) {
    std::fprintf(stderr, "lz4 compress error on %s\n", c.name.c_str());
    std::abort();
  }
  Measured m;
  m.label = "lz4";
  m.compressed_bytes = static_cast<size_t>(csize) + c.len_array_bytes();  // + bit-packed lengths

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    int r = LZ4_compress_default(reinterpret_cast<const char*>(c.bytes.data()), comp.data(), raw,
                                 bound);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(r) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  int cap = raw + 16;
  {
    std::vector<char> out(cap);
    int w = LZ4_decompress_safe(comp.data(), out.data(), csize, cap);
    if (w != raw || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "lz4 roundtrip mismatch on %s\n", c.name.c_str());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<char> out(cap);
    auto t0 = Clock::now();
    int w = LZ4_decompress_safe(comp.data(), out.data(), csize, cap);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// OnPair

Measured RunOnPair(const Corpus& c, uint8_t bits, double threshold) {
  op::Config cfg;
  cfg.max_dict_bits = bits;
  cfg.threshold_fraction = threshold;
  cfg.seed = 42;
  size_t n = c.n_rows();

  op::Column col = op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), n, cfg);
  Measured m;
  m.label = "OnPair" + std::to_string(bits);
  // Realistic bit-packed accounting: codes packed at the true code width for the
  // trained dictionary (not a fixed u16), dictionary offsets bit-packed, and the
  // shared per-row length array (in place of the OnPair code-offset array).
  size_t dict_bytes = col.dict.logical_bytes();
  size_t code_bits = IndexBits(col.dict.num_tokens());
  size_t codes = BitPackedBytes(col.codes.size(), code_bits);
  size_t dict_offsets =
      BitPackedBytes(col.dict.offsets.size(), std::max<size_t>(1, BitWidth(dict_bytes)));
  m.compressed_bytes = dict_bytes + dict_offsets + codes + c.len_array_bytes();

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    op::Column tmp = op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), n, cfg);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(tmp.codes.size()) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Decode the *packed* code stream (unpack `code_bits` per code + gather), so
  // decode pays the real bit-unpacking cost that the packed ratio implies.
  size_t cap = op::DecodedLen(col) + op::kDecodePadding;
  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(cw.data(), cw.size(), code_bits);
  {
    std::vector<uint8_t> out(cap, 0);
    size_t w = op::DecompressPacked(col.dict, packed.data(), col.codes.size(), code_bits, out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "OnPair%u packed roundtrip mismatch on %s (w=%zu raw=%zu)\n", bits,
                   c.name.c_str(), w, c.raw_bytes());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> out(cap, 0);
    auto t0 = Clock::now();
    size_t w = op::DecompressPacked(col.dict, packed.data(), col.codes.size(), code_bits, out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// Bit-packed size of an OnPair column (dict + bit-packed dict offsets + codes at
// the true code width + the shared per-row length array).
size_t OnPairSize(const op::Column& col, const Corpus& c) {
  size_t db = col.dict.logical_bytes();
  return db + BitPackedBytes(col.dict.offsets.size(), std::max<size_t>(1, BitWidth(db))) +
         BitPackedBytes(col.codes.size(), IndexBits(col.dict.num_tokens())) + c.len_array_bytes();
}

// OnPair with the dictionary bit-width chosen per column: try 9..16 and keep the
// width that minimizes bit-packed size, then report that width's ratio/decode.
// This exhaustive full-column sweep is the reliable way to pick the width. A
// cheap "train on a sub-sample and project to full size" picker does NOT
// reproduce it: training is not scale-invariant (the dynamic-threshold controller
// paces against the input size, so a sub-sample yields a differently *shaped*
// dictionary, not a smaller one), and enlarging the sample doesn't fix it - a
// token-gain curve fitted on a sample inherits the same skew. A cheap picker
// therefore needs a verify-against-the-ceiling fail-safe (train at the chosen
// budget and the ceiling, keep whichever stores less), not blind trust.
Measured RunOnPairAuto(const Corpus& c, double threshold) {
  size_t n = c.n_rows();
  uint8_t best_bits = 9;
  size_t best_sz = SIZE_MAX;
  for (uint8_t b = 9; b <= 16; ++b) {
    op::Config cfg{b, threshold, 42};
    op::Column col = op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), n, cfg);
    size_t sz = OnPairSize(col, c);
    if (sz < best_sz) { best_sz = sz; best_bits = b; }
  }

  op::Config cfg{best_bits, threshold, 42};
  op::Column col = op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), n, cfg);
  Measured m;
  // Report the *stored* code width = ceil(log2(tokens trained)), which is what
  // determines size. It can be < the budget when training saturates first.
  size_t stored_bits = IndexBits(col.dict.num_tokens());
  m.label = "OnPair-auto(" + std::to_string(stored_bits) + "b)";
  m.compressed_bytes = OnPairSize(col, c);

  // Encode throughput at the chosen width (a real encoder adds only a cheap
  // one-pass width estimate, not a full re-search, so this is representative).
  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    op::Column tmp = op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), n, cfg);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(tmp.codes.size()) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Packed decode at the chosen stored width.
  size_t cap = op::DecodedLen(col) + op::kDecodePadding;
  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(cw.data(), cw.size(), stored_bits);
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> out(cap, 0);
    auto t0 = Clock::now();
    size_t w = op::DecompressPacked(col.dict, packed.data(), col.codes.size(), stored_bits, out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// dedup-then-OnPair
//
// The layout a real columnar format uses for repetitive columns: encode the
// column as bit-packed references into the set of distinct values, and run
// OnPair over only those distinct values. Removes whole-value repetition (which
// OnPair's <=16-byte substring dictionary can't exploit) before compressing the
// residual substring redundancy. Matches the OnPair README's guidance for
// low-cardinality columns.

inline size_t CeilLog2(size_t x) {
  if (x <= 1) return 1;  // need >=1 bit even for a 2-value dictionary
  return 64 - static_cast<size_t>(__builtin_clzll(x - 1));
}

// Fast allocation-free byte-range hash, consuming 8 bytes per step (with a
// tail) and a final avalanche - far cheaper than a byte-at-a-time FNV for the
// short strings that dominate low-cardinality columns.
inline uint64_t HashBytes(const uint8_t* p, size_t len) {
  uint64_t h = 0x9E3779B97F4A7C15ull ^ (static_cast<uint64_t>(len) * 0xff51afd7ed558ccdull);
  size_t i = 0;
  for (; i + 8 <= len; i += 8) {
    uint64_t w;
    std::memcpy(&w, p + i, 8);
    h = (h ^ w) * 0x100000001b3ull;
  }
  if (i < len) {
    uint64_t w = 0;
    std::memcpy(&w, p + i, len - i);
    h = (h ^ w) * 0x100000001b3ull;
  }
  h ^= h >> 29;
  h *= 0xbf58476d1ce4e5b9ull;
  h ^= h >> 32;
  return h;
}

Measured RunOnPairDedup(const Corpus& c, uint8_t bits, double threshold) {
  op::Config cfg;
  cfg.max_dict_bits = bits;
  cfg.threshold_fraction = threshold;
  cfg.seed = 42;
  size_t n = c.n_rows();

  // Build the distinct-value set in first-seen order + per-row references.
  // Open-addressing (linear-probe) table keyed on the row bytes, assigning ids
  // in first-seen order - same distinct set/order as a std::unordered_map would
  // give (so ratios are identical) but without per-key node allocation or the
  // std::hash<string_view> + pointer-chase overhead, which dominated encode.
  auto build_dedup = [&](std::vector<uint8_t>* d_bytes, std::vector<uint32_t>* d_offsets,
                         std::vector<uint32_t>* refs) {
    size_t cap = 1;
    while (cap < n * 2) cap <<= 1;  // power-of-two, <=50% load
    const uint32_t kEmpty = 0xFFFFFFFFu;
    std::vector<uint32_t> table(cap, kEmpty);  // slot -> distinct id
    uint64_t mask = cap - 1;
    d_offsets->push_back(0);
    refs->resize(n);
    uint32_t n_distinct = 0;
    for (size_t i = 0; i < n; ++i) {
      const uint8_t* row = c.bytes.data() + c.offsets[i];
      size_t len = c.offsets[i + 1] - c.offsets[i];
      uint64_t slot = HashBytes(row, len) & mask;
      uint32_t id;
      for (;;) {
        uint32_t cur = table[slot];
        if (cur == kEmpty) {  // new distinct value
          id = n_distinct++;
          table[slot] = id;
          d_bytes->insert(d_bytes->end(), row, row + len);
          d_offsets->push_back(static_cast<uint32_t>(d_bytes->size()));
          break;
        }
        size_t off = (*d_offsets)[cur];
        size_t clen = (*d_offsets)[cur + 1] - off;
        if (clen == len && std::memcmp(d_bytes->data() + off, row, len) == 0) {
          id = cur;  // seen before
          break;
        }
        slot = (slot + 1) & mask;  // linear probe
      }
      (*refs)[i] = id;
    }
    return static_cast<size_t>(n_distinct);
  };

  std::vector<uint8_t> d_bytes;
  std::vector<uint32_t> d_offsets;
  std::vector<uint32_t> refs;
  size_t n_distinct = build_dedup(&d_bytes, &d_offsets, &refs);

  op::Column col = op::Compress(d_bytes.data(), d_bytes.size(), d_offsets.data(), n_distinct, cfg);

  Measured m;
  m.label = "OnPair" + std::to_string(bits) + "-dedup";
  // Realistic bit-packed accounting, applied to the OnPair-encoded distinct set
  // (dict + true-width codes + bit-packed dict offsets + a distinct-value length
  // array) plus the bit-packed per-row reference (index) column.
  size_t dict_bytes = col.dict.logical_bytes();
  size_t code_bits = IndexBits(col.dict.num_tokens());
  size_t codes = BitPackedBytes(col.codes.size(), code_bits);
  size_t dict_offsets =
      BitPackedBytes(col.dict.offsets.size(), std::max<size_t>(1, BitWidth(dict_bytes)));
  size_t dmax = 0;
  for (size_t j = 0; j + 1 < d_offsets.size(); ++j)
    dmax = std::max<size_t>(dmax, d_offsets[j + 1] - d_offsets[j]);
  size_t distinct_len_bytes = BitPackedBytes(n_distinct, std::max<size_t>(1, BitWidth(dmax)));
  size_t onpair_bytes = dict_bytes + dict_offsets + codes + distinct_len_bytes;
  size_t refs_bytes = BitPackedBytes(n, IndexBits(n_distinct));  // index column
  m.compressed_bytes = onpair_bytes + refs_bytes;

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    std::vector<uint8_t> db;
    std::vector<uint32_t> doff;
    std::vector<uint32_t> rf;
    auto t0 = Clock::now();
    size_t nd = build_dedup(&db, &doff, &rf);
    op::Column tmp = op::Compress(db.data(), db.size(), doff.data(), nd, cfg);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(tmp.codes.size()) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Decode: materialize distinct values once (OnPair decode), then gather rows
  // by reference. dbuf[d_offsets[id]..] holds distinct value `id` (decode
  // reproduces the concatenated distinct bytes in id order).
  //
  // Gather uses a branchless fixed-16-byte copy when the value is <=16 bytes
  // (the common case for the low-cardinality columns where dedup shines): one
  // 128-bit store instead of a variable-length memcpy dispatch. Safe because the
  // OnPair decode buffer is read-padded by kDecodePadding(16) and `out` carries
  // 16 bytes of write padding; the cursor advances by the true length so the
  // over-store is overwritten by the next row (or absorbed by the pad on the
  // last). Values >16 bytes fall back to an exact memcpy.
  size_t dlen = op::DecodedLen(col);
  size_t cap = c.raw_bytes() + 16;
  // Pack the distinct-set code stream and the per-row reference (index) column,
  // so decode pays the real unpacking cost the packed sizes imply.
  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed_codes = op::PackValues(cw.data(), cw.size(), code_bits);
  size_t ref_bits = IndexBits(n_distinct);
  std::vector<uint8_t> packed_refs = op::PackValues(refs.data(), n, ref_bits);

  // Materialize the distinct values (unpack the OnPair code stream), then gather
  // each row by unpacking its reference and copying the referenced value. <=16-byte
  // values use one branchless 128-bit store (dbuf/out are 16-byte padded).
  auto decode = [&](uint8_t* dbuf, uint8_t* out) -> size_t {
    op::DecompressPacked(col.dict, packed_codes.data(), col.codes.size(), code_bits, dbuf);
    size_t w = 0, bp = 0;
    for (size_t i = 0; i < n; ++i) {
      uint32_t id = op::GetBits(packed_refs.data(), bp, ref_bits);
      bp += ref_bits;
      size_t off = d_offsets[id];
      size_t len = d_offsets[id + 1] - off;
      const uint8_t* src = dbuf + off;
      if (len <= 16) std::memcpy(out + w, src, 16);
      else std::memcpy(out + w, src, len);
      w += len;
    }
    return w;
  };

  {
    std::vector<uint8_t> dbuf(dlen + op::kDecodePadding, 0), out(cap);
    size_t w = decode(dbuf.data(), out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "OnPair%u-dedup packed roundtrip mismatch on %s\n", bits, c.name.c_str());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> dbuf(dlen + op::kDecodePadding, 0), out(cap);
    auto t0 = Clock::now();
    size_t w = decode(dbuf.data(), out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// FSST+ / OnPair+ (common-prefix extraction, DICT mode)
//
// Y. L. Alexandre, "FSST+: Enhancing String Compression Through Common Prefix
// Extraction," MSc thesis, CWI, 2025. We evaluate the thesis's "DICT FSST+"
// path (the one it recommends for columnar integration, sec 5.3.5): the column
// is dictionary-encoded (distinct values + bit-packed row references), the
// distinct set is sorted, and prefix extraction is applied to that sorted
// dictionary. Sorting the dictionary is free of row-order concerns because the
// references carry the mapping - which is exactly why the thesis notes the
// within-block-sort limitation "does not apply" to DICT FSST+.
//
// The DP cleaving (prefix_plus::CleaveSorted, thesis sec 5.2.2) is shared. FSST+
// cleaves the FSST-compressed distinct values (prefix/suffix are spans of the
// compressed stream; the escape-255 guard keeps a symbol whole). OnPair+ cleaves
// raw bytes and OnPair-compresses the {shared prefixes + per-value suffixes} as
// one column - a deviation forced by OnPair emitting codes, not a splittable
// byte stream. Both count a bit-packed row-reference (index) column, matching
// OnPair16-dedup, so ratios are directly comparable.

// Shared distinct-value builder (first-seen order) + per-row references. Same
// open-addressing table as RunOnPairDedup's local build_dedup.
struct Dedup {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;  // n_distinct + 1
  std::vector<uint32_t> refs;     // n_rows -> distinct id
  size_t n_distinct = 0;
};

Dedup BuildDedup(const Corpus& c) {
  size_t n = c.n_rows();
  Dedup d;
  d.offsets.push_back(0);
  d.refs.resize(n);
  size_t cap = 1;
  while (cap < n * 2) cap <<= 1;
  const uint32_t kEmpty = 0xFFFFFFFFu;
  std::vector<uint32_t> table(cap, kEmpty);
  uint64_t mask = cap - 1;
  uint32_t n_distinct = 0;
  for (size_t i = 0; i < n; ++i) {
    const uint8_t* row = c.bytes.data() + c.offsets[i];
    size_t len = c.offsets[i + 1] - c.offsets[i];
    uint64_t slot = HashBytes(row, len) & mask;
    uint32_t id;
    for (;;) {
      uint32_t cur = table[slot];
      if (cur == kEmpty) {
        id = n_distinct++;
        table[slot] = id;
        d.bytes.insert(d.bytes.end(), row, row + len);
        d.offsets.push_back(static_cast<uint32_t>(d.bytes.size()));
        break;
      }
      size_t off = d.offsets[cur];
      size_t clen = d.offsets[cur + 1] - off;
      if (clen == len && std::memcmp(d.bytes.data() + off, row, len) == 0) { id = cur; break; }
      slot = (slot + 1) & mask;
    }
    d.refs[i] = id;
  }
  d.n_distinct = n_distinct;
  return d;
}

namespace pp = parquet::prefix_plus;

// ---- FSST+ ----------------------------------------------------------------

struct FsstPlusEnc {
  std::vector<uint8_t> comp;        // FSST-compressed distinct values (spans by id)
  std::vector<uint32_t> comp_off;   // distinct id -> byte offset into comp
  std::vector<uint32_t> comp_len;   // distinct id -> compressed length
  std::vector<uint8_t> table;       // fsst_export symbol table
  size_t table_bytes = 0;
  fsst_decoder_t dec{};
  std::vector<uint32_t> order;      // sorted rank -> distinct id (by compressed bytes)
  std::vector<uint32_t> sorted_pos; // distinct id -> sorted rank
  pp::Cleaving cl;                  // over the sorted compressed spans
  size_t nd = 0;
};

FsstPlusEnc EncodeFsstPlus(const Corpus& c, const Dedup& dd) {
  FsstPlusEnc e;
  size_t nd = dd.n_distinct;
  e.nd = nd;

  std::vector<size_t> lenIn(nd);
  std::vector<const unsigned char*> strIn(nd);
  for (size_t i = 0; i < nd; ++i) {
    lenIn[i] = dd.offsets[i + 1] - dd.offsets[i];
    strIn[i] = dd.bytes.data() + dd.offsets[i];
  }
  fsst_encoder_t* enc = fsst_create(nd, lenIn.data(), strIn.data(), 0);
  size_t out_cap = 7 * nd + 2 * dd.bytes.size() + 16;
  e.comp.assign(out_cap, 0);
  std::vector<size_t> lenOut(nd);
  std::vector<unsigned char*> strOut(nd);
  size_t done = fsst_compress(enc, nd, lenIn.data(), strIn.data(), out_cap, e.comp.data(),
                              lenOut.data(), strOut.data());
  if (done != nd) {
    std::fprintf(stderr, "FSST+ compressed %zu/%zu distinct on %s\n", done, nd, c.name.c_str());
    std::abort();
  }
  e.comp_off.resize(nd);
  e.comp_len.resize(nd);
  for (size_t i = 0; i < nd; ++i) {
    e.comp_off[i] = static_cast<uint32_t>(strOut[i] - e.comp.data());
    e.comp_len[i] = static_cast<uint32_t>(lenOut[i]);
  }
  unsigned char tbl[FSST_MAXHEADER];
  e.table_bytes = fsst_export(enc, tbl);
  e.table.assign(tbl, tbl + e.table_bytes);
  e.dec = fsst_decoder(enc);
  fsst_destroy(enc);

  // Sort distinct ids by their compressed bytes so shared compressed prefixes
  // are adjacent (FSST maps equal inputs to equal compressed forms).
  e.order.resize(nd);
  for (size_t i = 0; i < nd; ++i) e.order[i] = static_cast<uint32_t>(i);
  const uint8_t* base = e.comp.data();
  std::sort(e.order.begin(), e.order.end(), [&](uint32_t a, uint32_t b) {
    size_t la = e.comp_len[a], lb = e.comp_len[b];
    int cmp = std::memcmp(base + e.comp_off[a], base + e.comp_off[b], std::min(la, lb));
    if (cmp != 0) return cmp < 0;
    return la < lb;
  });
  e.sorted_pos.resize(nd);
  for (size_t k = 0; k < nd; ++k) e.sorted_pos[e.order[k]] = static_cast<uint32_t>(k);

  std::vector<const uint8_t*> sptr(nd);
  std::vector<size_t> slen(nd);
  for (size_t k = 0; k < nd; ++k) {
    sptr[k] = base + e.comp_off[e.order[k]];
    slen[k] = e.comp_len[e.order[k]];
  }
  e.cl = pp::CleaveSorted(sptr.data(), slen.data(), nd, pp::kMaxPrefix, /*guard_escape255=*/true);
  return e;
}

// Exact FSST+ stored size (thesis sec 3.1 layout) + bit-packed row references.
size_t FsstPlusSize(const FsstPlusEnc& e, const Corpus& c) {
  size_t nd = e.nd;
  size_t num_blocks = (nd + pp::kBlockSize - 1) / pp::kBlockSize;
  size_t bytes = 2 + 4 * num_blocks + 4;  // num_blocks + block_start_offsets[] + data_end_offset
  for (size_t bstart = 0; bstart < nd; bstart += pp::kBlockSize) {
    size_t bn = std::min(pp::kBlockSize, nd - bstart);
    bytes += 1 + 2 * bn;  // num_strings + suffix_data_area_offsets[]
    for (size_t k = bstart; k < bstart + bn; ++k) {
      uint32_t p = e.cl.prefix_len[k];
      uint32_t clen = e.comp_len[e.order[k]];
      bytes += 1;                                     // prefix_length
      if (p > 0) bytes += 2;                          // jump_back_offset
      bytes += clen - p;                              // compressed suffix
      if (p > 0 && e.cl.chunk_first[k] == k) bytes += p;  // shared prefix, stored once
    }
  }
  bytes += e.table_bytes;
  return bytes + BitPackedBytes(c.n_rows(), IndexBits(nd));
}

Measured RunFsstPlus(const Corpus& c) {
  size_t n = c.n_rows();
  Dedup dd = BuildDedup(c);
  FsstPlusEnc e = EncodeFsstPlus(c, dd);
  Measured m;
  m.label = "FSST+";
  m.compressed_bytes = FsstPlusSize(e, c);

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    Dedup d2 = BuildDedup(c);
    FsstPlusEnc e2 = EncodeFsstPlus(c, d2);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(e2.nd) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Decode: reconstruct each sorted distinct value from its stored (prefix once
  // + own suffix) compressed form and FSST-decode it, then gather rows by ref.
  size_t distinct_total = dd.bytes.size();
  size_t cap = c.raw_bytes() + 16;
  const uint8_t* base = e.comp.data();
  auto decode = [&](std::vector<uint8_t>& dictbuf, std::vector<uint32_t>& voff,
                    uint8_t* out) -> size_t {
    voff.assign(e.nd + 1, 0);
    std::vector<uint8_t> tmp;
    tmp.reserve(pp::kMaxPrefix + 256);
    size_t w = 0;
    for (size_t k = 0; k < e.nd; ++k) {
      uint32_t p = e.cl.prefix_len[k];
      uint32_t rep = e.cl.chunk_first[k];
      uint32_t clen = e.comp_len[e.order[k]];
      const uint8_t* cbytes = base + e.comp_off[e.order[k]];
      tmp.clear();
      if (p > 0) {
        const uint8_t* rbytes = base + e.comp_off[e.order[rep]];
        tmp.insert(tmp.end(), rbytes, rbytes + p);
      }
      tmp.insert(tmp.end(), cbytes + p, cbytes + clen);
      size_t dl = fsst_decompress(&e.dec, tmp.size(), tmp.data(), dictbuf.size() - w,
                                  dictbuf.data() + w);
      w += dl;
      voff[k + 1] = static_cast<uint32_t>(w);
    }
    size_t o = 0;
    for (size_t i = 0; i < n; ++i) {
      uint32_t k = e.sorted_pos[dd.refs[i]];
      size_t off = voff[k], len = voff[k + 1] - off;
      std::memcpy(out + o, dictbuf.data() + off, len);
      o += len;
    }
    return o;
  };

  {
    std::vector<uint8_t> dictbuf(distinct_total + 32, 0), out(cap);
    std::vector<uint32_t> voff;
    size_t w = decode(dictbuf, voff, out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "FSST+ roundtrip mismatch on %s (w=%zu raw=%zu)\n", c.name.c_str(), w,
                   c.raw_bytes());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> dictbuf(distinct_total + 32, 0), out(cap);
    std::vector<uint32_t> voff;
    auto t0 = Clock::now();
    size_t w = decode(dictbuf, voff, out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// ---- OnPair+ --------------------------------------------------------------

struct OnPairPlusEnc {
  op::Column col;                          // {shared prefixes + suffixes} as one column
  std::vector<uint32_t> piece_off;         // num_pieces + 1 byte offsets (also decoded boundaries)
  size_t num_pieces = 0;
  std::vector<uint32_t> order, sorted_pos;  // distinct id <-> sorted rank (by raw bytes)
  pp::Cleaving cl;                         // over raw sorted values
  std::vector<uint32_t> prefix_piece;      // sorted rank of a chunk rep -> its prefix piece index
  size_t n_prefix_pieces = 0;              // suffix piece of sorted value k == n_prefix_pieces + k
  size_t nd = 0;
};

OnPairPlusEnc EncodeOnPairPlus(const Corpus& c, const Dedup& dd, double threshold) {
  (void)c;
  OnPairPlusEnc e;
  size_t nd = dd.n_distinct;
  e.nd = nd;

  const uint8_t* base = dd.bytes.data();
  e.order.resize(nd);
  for (size_t i = 0; i < nd; ++i) e.order[i] = static_cast<uint32_t>(i);
  std::sort(e.order.begin(), e.order.end(), [&](uint32_t a, uint32_t b) {
    size_t la = dd.offsets[a + 1] - dd.offsets[a], lb = dd.offsets[b + 1] - dd.offsets[b];
    int cmp = std::memcmp(base + dd.offsets[a], base + dd.offsets[b], std::min(la, lb));
    if (cmp != 0) return cmp < 0;
    return la < lb;
  });
  e.sorted_pos.resize(nd);
  for (size_t k = 0; k < nd; ++k) e.sorted_pos[e.order[k]] = static_cast<uint32_t>(k);

  std::vector<const uint8_t*> sptr(nd);
  std::vector<size_t> slen(nd);
  for (size_t k = 0; k < nd; ++k) {
    uint32_t id = e.order[k];
    sptr[k] = base + dd.offsets[id];
    slen[k] = dd.offsets[id + 1] - dd.offsets[id];
  }
  e.cl = pp::CleaveSorted(sptr.data(), slen.data(), nd, pp::kMaxPrefix, /*guard_escape255=*/false);

  // Pieces: each chunk's shared prefix once, then every value's suffix.
  std::vector<uint8_t> pbytes;
  std::vector<uint32_t> poff;
  poff.push_back(0);
  e.prefix_piece.assign(nd, 0xFFFFFFFFu);
  uint32_t pc = 0;
  for (size_t k = 0; k < nd; ++k) {
    if (e.cl.prefix_len[k] > 0 && e.cl.chunk_first[k] == k) {
      e.prefix_piece[k] = pc++;
      pbytes.insert(pbytes.end(), sptr[k], sptr[k] + e.cl.prefix_len[k]);
      poff.push_back(static_cast<uint32_t>(pbytes.size()));
    }
  }
  e.n_prefix_pieces = pc;
  for (size_t k = 0; k < nd; ++k) {
    uint32_t p = e.cl.prefix_len[k];
    pbytes.insert(pbytes.end(), sptr[k] + p, sptr[k] + slen[k]);
    poff.push_back(static_cast<uint32_t>(pbytes.size()));
  }
  e.num_pieces = poff.size() - 1;
  e.piece_off = std::move(poff);

  op::Config cfg{16, threshold, 42};
  e.col = op::Compress(pbytes.data(), pbytes.size(), e.piece_off.data(), e.num_pieces, cfg);
  return e;
}

// OnPair+ stored size: the shared OnPair model (dict + bit-packed offsets +
// codes for prefixes-once + suffixes), a bit-packed piece-boundary array (in
// place of FSST+'s compressed byte spans), the same per-value prefix_length /
// jump-back overhead and block headers as FSST+, and the row-reference column.
size_t OnPairPlusSize(const OnPairPlusEnc& e, const Corpus& c) {
  const op::Column& col = e.col;
  size_t dict_bytes = col.dict.logical_bytes();
  size_t code_bits = IndexBits(col.dict.num_tokens());
  size_t codes = BitPackedBytes(col.codes.size(), code_bits);
  size_t dict_offsets =
      BitPackedBytes(col.dict.offsets.size(), std::max<size_t>(1, BitWidth(dict_bytes)));
  size_t piece_bound =
      BitPackedBytes(e.num_pieces, std::max<size_t>(1, BitWidth(col.codes.size())));
  size_t nd = e.nd;
  size_t num_blocks = (nd + pp::kBlockSize - 1) / pp::kBlockSize;
  size_t structural = 2 + 4 * num_blocks + 4;
  for (size_t bstart = 0; bstart < nd; bstart += pp::kBlockSize) {
    size_t bn = std::min(pp::kBlockSize, nd - bstart);
    structural += 1;  // num_strings
    for (size_t k = bstart; k < bstart + bn; ++k)
      structural += 1 + (e.cl.prefix_len[k] > 0 ? 2 : 0);
  }
  size_t refs = BitPackedBytes(c.n_rows(), IndexBits(nd));
  return dict_bytes + dict_offsets + codes + piece_bound + structural + refs;
}

Measured RunOnPairPlus(const Corpus& c, double threshold) {
  size_t n = c.n_rows();
  Dedup dd = BuildDedup(c);
  OnPairPlusEnc e = EncodeOnPairPlus(c, dd, threshold);
  Measured m;
  m.label = "OnPair+";
  m.compressed_bytes = OnPairPlusSize(e, c);

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    Dedup d2 = BuildDedup(c);
    OnPairPlusEnc e2 = EncodeOnPairPlus(c, d2, threshold);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(e2.num_pieces) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  // Decode: OnPair-materialize all pieces (unpacking the code stream), then for
  // each sorted value concatenate its prefix piece + suffix piece, and gather
  // rows by reference.
  size_t piece_total = op::DecodedLen(e.col);
  size_t distinct_total = dd.bytes.size();
  size_t cap = c.raw_bytes() + 16;
  size_t code_bits = IndexBits(e.col.dict.num_tokens());
  std::vector<uint32_t> cw(e.col.codes.begin(), e.col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(cw.data(), cw.size(), code_bits);

  auto decode = [&](std::vector<uint8_t>& piecebuf, std::vector<uint8_t>& dictbuf,
                    std::vector<uint32_t>& voff, uint8_t* out) -> size_t {
    op::DecompressPacked(e.col.dict, packed.data(), e.col.codes.size(), code_bits, piecebuf.data());
    voff.assign(e.nd + 1, 0);
    size_t w = 0;
    for (size_t k = 0; k < e.nd; ++k) {
      uint32_t p = e.cl.prefix_len[k];
      if (p > 0) {
        uint32_t pi = e.prefix_piece[e.cl.chunk_first[k]];
        std::memcpy(dictbuf.data() + w, piecebuf.data() + e.piece_off[pi], p);
        w += p;
      }
      uint32_t sfx = static_cast<uint32_t>(e.n_prefix_pieces) + static_cast<uint32_t>(k);
      size_t soff = e.piece_off[sfx], slen = e.piece_off[sfx + 1] - soff;
      std::memcpy(dictbuf.data() + w, piecebuf.data() + soff, slen);
      w += slen;
      voff[k + 1] = static_cast<uint32_t>(w);
    }
    size_t o = 0;
    for (size_t i = 0; i < n; ++i) {
      uint32_t k = e.sorted_pos[dd.refs[i]];
      size_t off = voff[k], len = voff[k + 1] - off;
      std::memcpy(out + o, dictbuf.data() + off, len);
      o += len;
    }
    return o;
  };

  {
    std::vector<uint8_t> piecebuf(piece_total + op::kDecodePadding, 0);
    std::vector<uint8_t> dictbuf(distinct_total + 32, 0), out(cap);
    std::vector<uint32_t> voff;
    size_t w = decode(piecebuf, dictbuf, voff, out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "OnPair+ roundtrip mismatch on %s (w=%zu raw=%zu)\n", c.name.c_str(), w,
                   c.raw_bytes());
      std::abort();
    }
  }
  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> piecebuf(piece_total + op::kDecodePadding, 0);
    std::vector<uint8_t> dictbuf(distinct_total + 32, 0), out(cap);
    std::vector<uint32_t> voff;
    auto t0 = Clock::now();
    size_t w = decode(piecebuf, dictbuf, voff, out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

}  // namespace

int main(int argc, char** argv) {
  std::string dir = CorpusDir(argc, argv);
  std::vector<std::filesystem::path> files = CorpusFiles(dir);
  if (files.empty()) {
    std::fprintf(stderr, "no .txt corpora in %s\n", dir.c_str());
    return 1;
  }

  std::printf("%-26s %10s %10s   %7s %9s %9s\n", "corpus", "rows", "raw MiB", "ratio",
              "enc MiB/s", "dec MiB/s");
  std::printf("%s\n", std::string(90, '-').c_str());

  for (const auto& path : files) {
    Corpus c = ReadCorpus(path);
    double threshold = ThresholdFor(c.name);

    Measured fsst = RunFsst(c);
    Measured zstd1 = RunZstd(c, 1);
    Measured lz4 = RunLz4(c);
    Measured op16 = RunOnPair(c, 16, threshold);
    Measured opauto = RunOnPairAuto(c, threshold);
    Measured op16d = RunOnPairDedup(c, 16, threshold);
    Measured fsstp = RunFsstPlus(c);
    Measured oppl = RunOnPairPlus(c, threshold);

    std::printf("%-26s %10zu %10.2f\n", c.name.c_str(), c.n_rows(), Mib(c.raw_bytes()));
    for (const Measured* m : {&fsst, &zstd1, &lz4, &op16, &opauto, &op16d, &fsstp, &oppl}) {
      double ratio = static_cast<double>(c.raw_bytes()) / static_cast<double>(m->compressed_bytes);
      std::printf("  %-24s %10s %10.2f  %7.3fx %9.1f %9.1f\n", m->label.c_str(), "",
                  Mib(m->compressed_bytes), ratio, m->encode_mibs, m->decode_mibs);
    }
    double r_fsst = static_cast<double>(c.raw_bytes()) / fsst.compressed_bytes;
    double r_zstd = static_cast<double>(c.raw_bytes()) / zstd1.compressed_bytes;
    double r_op16 = static_cast<double>(c.raw_bytes()) / op16.compressed_bytes;
    std::printf("  -> OnPair16 vs FSST:    ratio %+.1f%%, encode %+.1f%%, decode %+.1f%%\n",
                (r_op16 / r_fsst - 1.0) * 100.0, (op16.encode_mibs / fsst.encode_mibs - 1.0) * 100.0,
                (op16.decode_mibs / fsst.decode_mibs - 1.0) * 100.0);
    std::printf("  -> OnPair16 vs zstd(1): ratio %+.1f%%, encode %+.1f%%, decode %+.1f%%\n",
                (r_op16 / r_zstd - 1.0) * 100.0,
                (op16.encode_mibs / zstd1.encode_mibs - 1.0) * 100.0,
                (op16.decode_mibs / zstd1.decode_mibs - 1.0) * 100.0);
    double r_op16d = static_cast<double>(c.raw_bytes()) / op16d.compressed_bytes;
    std::printf("  -> OnPair16-dedup vs zstd(1): ratio %+.1f%%, decode %+.1f%%\n",
                (r_op16d / r_zstd - 1.0) * 100.0,
                (op16d.decode_mibs / zstd1.decode_mibs - 1.0) * 100.0);
    double r_fsstp = static_cast<double>(c.raw_bytes()) / fsstp.compressed_bytes;
    double r_oppl = static_cast<double>(c.raw_bytes()) / oppl.compressed_bytes;
    std::printf("  -> FSST+ vs FSST: ratio %+.1f%%; FSST+ vs zstd(1): ratio %+.1f%%\n",
                (r_fsstp / r_fsst - 1.0) * 100.0, (r_fsstp / r_zstd - 1.0) * 100.0);
    std::printf("  -> OnPair+ vs OnPair16-dedup: ratio %+.1f%%; OnPair+ vs zstd(1): ratio %+.1f%%\n\n",
                (r_oppl / r_op16d - 1.0) * 100.0, (r_oppl / r_zstd - 1.0) * 100.0);
  }
  return 0;
}
