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

// Does it pay to cascade a generic codec (zstd/lz4) over a string encoding?
//
// The companion benchmark (fsst_onpair_benchmark.cc) measures FSST, OnPair, zstd
// and lz4 as alternatives. This one measures them as *compositions*, which is
// what a Parquet writer actually does: an encoding produces a page payload and a
// page compressor then runs over the whole payload. Two groups:
//
//   A. FSST / OnPair followed by zstd(1) or lz4. Note what this costs: the
//      generic layer is whole-page, so the per-row random access that is the
//      reason to pick FSST or OnPair in the first place is gone -- you must
//      inflate the page before touching one row. The question is whether the
//      extra ratio pays for that. OnPair is also measured with a byte-aligned
//      layout (u16 codes, u32 dictionary offsets) rather than bit-packed,
//      because bit-packed streams are close to incompressible and testing only
//      the packed layout would understate what a cascade can do.
//
//   B. Parquet's own byte-array encodings -- PLAIN, DELTA_LENGTH_BYTE_ARRAY,
//      DELTA_BYTE_ARRAY, RLE_DICTIONARY -- alone and under zstd(1)/lz4, driven
//      through the real parquet::Encoder/Decoder API rather than reimplemented.
//      PLAIN+ZSTD is what Parquet writes for string columns today, so this is
//      the baseline a new encoding has to beat; the companion benchmark's
//      zstd-over-concatenated-bytes column is a projection of a page, not one.
//
// Accounting matches the companion benchmark so ratios are comparable across the
// two binaries (see bench_common.h), with one deliberate difference: PLAIN and
// the DELTA_* family embed their own lengths, so they are NOT charged the
// separate bit-packed row-length array that FSST/zstd/lz4/OnPair are. Charging
// it would count row boundaries twice. Every number here is therefore the
// complete page payload needed to reconstruct the column.
//
// Decode is timed as "reconstruct the whole column into a contiguous buffer",
// including the generic decompression and the per-page decoder setup. The
// contiguous copy matters for the Parquet encodings: their Decode() hands back
// pointers into the decoder's own buffer, so timing that alone would report PLAIN
// as nearly free rather than as the memcpy it is.
//
// Build (from the Arrow repo root), one line -- needs libparquet, unlike the
// companion benchmark. libarrow is linked by path because the build directory
// carries two sonames and letting -larrow choose warns about the conflict:
//   g++ -std=c++17 -O3 -march=native -Icpp/src -Icpp/build-bench/src
//   -Icpp/thirdparty/fsst cpp/thirdparty/fsst/libfsst.cpp
//   cpp/thirdparty/fsst/fsst_avx512.cpp cpp/src/parquet/onpair/onpair.cc
//   cpp/src/parquet/onpair/cascade_benchmark.cc
//   cpp/build-bench/release/libparquet.so cpp/build-bench/release/libarrow.so.2300
//   /usr/lib64/libzstd.so.1 /usr/lib64/liblz4.so.1
//   -Wl,-rpath,$PWD/cpp/build-bench/release -o /tmp/cascade_bench
//
// Run:  taskset -c 0 /tmp/cascade_bench <corpora_dir>   (run twice, use the 2nd)

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "fsst.h"
#include "parquet/encoding.h"
#include "parquet/onpair/bench_common.h"
#include "parquet/onpair/onpair.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace op = parquet::onpair;

namespace {

using namespace bench;  // NOLINT(build/namespaces)

// The generic (page-level) codec layered over an encoding's payload.

enum class Generic { kNone, kZstd1, kLz4 };

const char* GenericSuffix(Generic g) {
  switch (g) {
    case Generic::kNone:
      return "";
    case Generic::kZstd1:
      return "+zstd(1)";
    default:
      return "+lz4";
  }
}

std::vector<uint8_t> GenericCompress(const uint8_t* src, size_t n, Generic g) {
  if (g == Generic::kNone) return std::vector<uint8_t>(src, src + n);
  std::vector<uint8_t> out;
  if (g == Generic::kZstd1) {
    out.resize(ZSTD_compressBound(n));
    size_t c = ZSTD_compress(out.data(), out.size(), src, n, 1);
    if (ZSTD_isError(c)) {
      std::fprintf(stderr, "zstd compress error\n");
      std::abort();
    }
    out.resize(c);
  } else {
    out.resize(static_cast<size_t>(LZ4_compressBound(static_cast<int>(n))));
    int c = LZ4_compress_default(reinterpret_cast<const char*>(src),
                                 reinterpret_cast<char*>(out.data()),
                                 static_cast<int>(n), static_cast<int>(out.size()));
    if (c <= 0) {
      std::fprintf(stderr, "lz4 compress error\n");
      std::abort();
    }
    out.resize(static_cast<size_t>(c));
  }
  return out;
}

// `raw_size` is the payload's uncompressed length, which a real page header
// carries, so knowing it here is not cheating.
void GenericDecompress(const uint8_t* src, size_t csize, uint8_t* dst, size_t raw_size,
                       Generic g) {
  if (g == Generic::kNone) {
    std::memcpy(dst, src, csize);
    return;
  }
  if (g == Generic::kZstd1) {
    size_t w = ZSTD_decompress(dst, raw_size, src, csize);
    if (ZSTD_isError(w) || w != raw_size) {
      std::fprintf(stderr, "zstd decompress error\n");
      std::abort();
    }
    return;
  }
  int w = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                             reinterpret_cast<char*>(dst), static_cast<int>(csize),
                             static_cast<int>(raw_size));
  if (w != static_cast<int>(raw_size)) {
    std::fprintf(stderr, "lz4 decompress error\n");
    std::abort();
  }
}

// A codec under test: `build` produces the encoded page payload from the corpus
// (the encode side, training included), `decode` reconstructs the concatenated
// column bytes from a payload buffer and returns the byte count written.
using BuildFn = std::function<std::vector<uint8_t>()>;
using DecodeFn = std::function<size_t(const uint8_t* payload, size_t payload_size,
                                      uint8_t* out)>;

// Extra slack after the payload copy: OnPair's packed-code reader over-reads up
// to 4 bytes past the last code, and its dictionary decode over-reads one token.
constexpr size_t kPad = 64;

// `extra_bytes` is charged on top of the compressed payload without being part
// of it. Only used to reproduce the companion benchmark's zstd/lz4 accounting,
// which charges an uncompressed row-length array alongside a frame that holds
// only the value bytes.
Measured RunCodec(const Corpus& c, const std::string& label, Generic g, const BuildFn& build,
                  const DecodeFn& decode, size_t extra_bytes = 0) {
  Measured m;
  m.label = label;

  std::vector<uint8_t> payload = build();
  std::vector<uint8_t> comp = GenericCompress(payload.data(), payload.size(), g);
  m.compressed_bytes = comp.size() + extra_bytes;

  std::vector<double> enc;
  for (int it = 0; it < kEncodeIters; ++it) {
    auto t0 = Clock::now();
    std::vector<uint8_t> p = build();
    std::vector<uint8_t> cc = GenericCompress(p.data(), p.size(), g);
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(cc.size()) : "memory");
    enc.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.encode_mibs = Median(std::move(enc));

  const size_t out_cap = c.raw_bytes() + op::kDecodePadding + kPad;
  {
    std::vector<uint8_t> scratch(payload.size() + kPad, 0);
    std::vector<uint8_t> out(out_cap, 0);
    GenericDecompress(comp.data(), comp.size(), scratch.data(), payload.size(), g);
    size_t w = decode(scratch.data(), payload.size(), out.data());
    if (w != c.raw_bytes() || std::memcmp(out.data(), c.bytes.data(), c.raw_bytes()) != 0) {
      std::fprintf(stderr, "%s roundtrip mismatch on %s (w=%zu raw=%zu)\n", m.label.c_str(),
                   c.name.c_str(), w, c.raw_bytes());
      std::abort();
    }
  }

  std::vector<double> dec_r;
  for (int it = 0; it < kDecodeIters; ++it) {
    std::vector<uint8_t> scratch(payload.size() + kPad, 0);
    std::vector<uint8_t> out(out_cap, 0);
    auto t0 = Clock::now();
    GenericDecompress(comp.data(), comp.size(), scratch.data(), payload.size(), g);
    size_t w = decode(scratch.data(), payload.size(), out.data());
    double dt = std::chrono::duration<double>(Clock::now() - t0).count();
    asm volatile("" ::"r"(w) : "memory");
    dec_r.push_back(Mib(c.raw_bytes()) / dt);
  }
  m.decode_mibs = Median(std::move(dec_r));
  return m;
}

// Little-endian fixed-width field helpers for the payload headers. The headers
// are a handful of bytes per page and are charged, so no codec gets a free ride
// on self-description.

void PutU32(std::vector<uint8_t>* v, uint32_t x) {
  uint8_t b[4];
  std::memcpy(b, &x, 4);
  v->insert(v->end(), b, b + 4);
}
uint32_t GetU32(const uint8_t* p) {
  uint32_t x;
  std::memcpy(&x, p, 4);
  return x;
}

// Append the bit-packed per-row length array that every value-preserving codec
// is charged for (see Corpus::len_array_bytes).
void AppendLengths(const Corpus& c, std::vector<uint8_t>* v) {
  size_t bits = std::max<size_t>(1, BitWidth(c.max_row_len()));
  std::vector<uint32_t> lens(c.n_rows());
  for (size_t i = 0; i < c.n_rows(); ++i) lens[i] = c.offsets[i + 1] - c.offsets[i];
  std::vector<uint8_t> packed = op::PackValues(lens.data(), lens.size(), bits);
  packed.resize(BitPackedBytes(c.n_rows(), bits));
  v->insert(v->end(), packed.begin(), packed.end());
}

// Byte-aligned analog of the above: lengths at the smallest whole-byte width.
void AppendLengthsByteAligned(const Corpus& c, std::vector<uint8_t>* v) {
  size_t w = std::max<size_t>(1, (BitWidth(c.max_row_len()) + 7) / 8);
  for (size_t i = 0; i < c.n_rows(); ++i) {
    uint32_t len = c.offsets[i + 1] - c.offsets[i];
    for (size_t b = 0; b < w; ++b) v->push_back(static_cast<uint8_t>(len >> (8 * b)));
  }
}

// Group A codec 1: FSST
//
// Payload: [u32 table_bytes][u32 stream_bytes][symbol table][code stream][lengths]

std::vector<uint8_t> BuildFsstPayload(const Corpus& c) {
  size_t n = c.n_rows();
  std::vector<size_t> lenIn(n);
  std::vector<const unsigned char*> strIn(n);
  for (size_t i = 0; i < n; ++i) {
    lenIn[i] = c.offsets[i + 1] - c.offsets[i];
    strIn[i] = c.bytes.data() + c.offsets[i];
  }
  fsst_encoder_t* enc = fsst_create(n, lenIn.data(), strIn.data(), 0);

  size_t out_cap = 7 * n + 2 * c.raw_bytes() + 16;  // fsst.h bound: 7 + 2*len per string
  std::vector<uint8_t> stream(out_cap);
  std::vector<size_t> lenOut(n);
  std::vector<unsigned char*> strOut(n);
  size_t done = fsst_compress(enc, n, lenIn.data(), strIn.data(), out_cap, stream.data(),
                              lenOut.data(), strOut.data());
  if (done != n) {
    std::fprintf(stderr, "FSST: only compressed %zu/%zu rows\n", done, n);
    std::abort();
  }
  size_t total = 0;
  for (size_t i = 0; i < n; ++i) total += lenOut[i];

  unsigned char table[FSST_MAXHEADER];
  size_t table_bytes = fsst_export(enc, table);
  fsst_destroy(enc);

  std::vector<uint8_t> v;
  v.reserve(8 + table_bytes + total + c.len_array_bytes());
  PutU32(&v, static_cast<uint32_t>(table_bytes));
  PutU32(&v, static_cast<uint32_t>(total));
  v.insert(v.end(), table, table + table_bytes);
  v.insert(v.end(), stream.begin(), stream.begin() + total);
  AppendLengths(c, &v);
  return v;
}

size_t DecodeFsstPayload(const uint8_t* p, size_t /*size*/, uint8_t* out, size_t out_cap) {
  size_t table_bytes = GetU32(p);
  size_t stream_bytes = GetU32(p + 4);
  fsst_decoder_t dec;
  fsst_import(&dec, p + 8);
  return fsst_decompress(&dec, stream_bytes, const_cast<unsigned char*>(p + 8 + table_bytes),
                         out_cap, out);
}

// Group A codec 2: OnPair, bit-packed (the layout Tables 1-3 report)
//
// Payload: [u32 tokens][u32 dict_bytes][u32 codes][u8 code_bits][u8 off_bits][pad*2]
//          [dictionary blob][packed dict offsets][packed codes][lengths]

// Choose the dictionary budget the way the companion benchmark's OnPair-auto
// does: train at every width in 9..16 and keep the one that stores least.
size_t OnPairSize(const op::Column& col, const Corpus& c) {
  size_t db = col.dict.logical_bytes();
  return db + BitPackedBytes(col.dict.offsets.size(), std::max<size_t>(1, BitWidth(db))) +
         BitPackedBytes(col.codes.size(), IndexBits(col.dict.num_tokens())) +
         c.len_array_bytes();
}

uint8_t PickOnPairBits(const Corpus& c, double threshold) {
  uint8_t best_bits = 9;
  size_t best_sz = SIZE_MAX;
  for (uint8_t b = 9; b <= 16; ++b) {
    op::Config cfg{b, threshold, 42};
    op::Column col =
        op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), c.n_rows(), cfg);
    size_t sz = OnPairSize(col, c);
    if (sz < best_sz) {
      best_sz = sz;
      best_bits = b;
    }
  }
  return best_bits;
}

constexpr size_t kOnPairHeader = 16;

std::vector<uint8_t> BuildOnPairPayload(const Corpus& c, const op::Config& cfg,
                                        bool byte_aligned) {
  op::Column col =
      op::Compress(c.bytes.data(), c.raw_bytes(), c.offsets.data(), c.n_rows(), cfg);
  size_t dict_bytes = col.dict.logical_bytes();
  size_t code_bits = byte_aligned ? 16 : IndexBits(col.dict.num_tokens());
  size_t off_bits = byte_aligned ? 32 : std::max<size_t>(1, BitWidth(dict_bytes));

  std::vector<uint8_t> v;
  v.reserve(kOnPairHeader + dict_bytes + col.codes.size() * 2 + c.len_array_bytes());
  PutU32(&v, static_cast<uint32_t>(col.dict.num_tokens()));
  PutU32(&v, static_cast<uint32_t>(dict_bytes));
  PutU32(&v, static_cast<uint32_t>(col.codes.size()));
  v.push_back(static_cast<uint8_t>(code_bits));
  v.push_back(static_cast<uint8_t>(off_bits));
  v.push_back(0);
  v.push_back(0);

  v.insert(v.end(), col.dict.bytes.begin(), col.dict.bytes.begin() + dict_bytes);

  std::vector<uint32_t> offs(col.dict.offsets.begin(), col.dict.offsets.end());
  std::vector<uint8_t> packed_offs = op::PackValues(offs.data(), offs.size(), off_bits);
  packed_offs.resize(BitPackedBytes(offs.size(), off_bits));
  v.insert(v.end(), packed_offs.begin(), packed_offs.end());

  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed_codes = op::PackValues(cw.data(), cw.size(), code_bits);
  packed_codes.resize(BitPackedBytes(cw.size(), code_bits));
  v.insert(v.end(), packed_codes.begin(), packed_codes.end());

  if (byte_aligned) {
    AppendLengthsByteAligned(c, &v);
  } else {
    AppendLengths(c, &v);
  }
  return v;
}

// Rebuilds the dictionary from the payload (a reader must, since the decoder
// needs read-padded token bytes and materialized offsets) and decodes the packed
// code stream in place.
size_t DecodeOnPairPayload(const uint8_t* p, size_t /*size*/, uint8_t* out) {
  size_t num_tokens = GetU32(p);
  size_t dict_bytes = GetU32(p + 4);
  size_t num_codes = GetU32(p + 8);
  size_t code_bits = p[12];
  size_t off_bits = p[13];

  const uint8_t* dict_blob = p + kOnPairHeader;
  const uint8_t* packed_offs = dict_blob + dict_bytes;
  size_t offs_bytes = BitPackedBytes(num_tokens + 1, off_bits);
  const uint8_t* packed_codes = packed_offs + offs_bytes;

  op::CompactDictionary dict;
  dict.bytes.resize(dict_bytes + op::kDecodePadding, 0);
  std::memcpy(dict.bytes.data(), dict_blob, dict_bytes);
  dict.offsets.resize(num_tokens + 1);
  for (size_t t = 0; t <= num_tokens; ++t) {
    dict.offsets[t] = op::GetBits(packed_offs, t * off_bits, off_bits);
  }
  dict.RecomputeMaxTokenLen();

  return op::DecompressPacked(dict, packed_codes, num_codes, code_bits, out);
}

// Group A codec 3: dictionary-encode, then OnPair the distinct values.
//
// The layout a columnar format uses for repetitive columns: bit-packed references
// into the distinct set, with OnPair run over only the distinct values. Measured
// here as well as in the companion benchmark so that a decode number for it can
// sit in the same table as the Parquet-native pages, which are 4.6-7.5% slower in
// this binary.
//
// Payload: [u32 distinct][u32 rows][u32 distinct_bytes][u8 ref_bits][u8 dlen_bits]
//          [pad*2][OnPair payload of the distinct set][packed distinct lengths]
//          [packed row references]
//
// Note what is NOT charged: the per-row length array. Row lengths are recovered
// from a row's reference plus the distinct-value lengths, so charging both would
// count boundaries twice. This matches the companion benchmark's accounting.

// Allocation-free byte-range hash, 8 bytes per step plus a tail and a final
// avalanche -- cheaper than byte-at-a-time for the short values that dominate
// low-cardinality columns. Same function the companion benchmark uses, so both
// binaries build the identical distinct set in the identical order.
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

struct DedupSet {
  std::vector<uint8_t> bytes;      // distinct values, concatenated in first-seen order
  std::vector<uint32_t> offsets;   // n_distinct + 1
  std::vector<uint32_t> refs;      // one per row
  size_t n_distinct = 0;
};

// Open-addressing (linear-probe) table keyed on the row bytes, assigning ids in
// first-seen order -- the same distinct set and order an unordered_map would
// give, so ratios match, without per-key allocation.
DedupSet BuildDedupSet(const Corpus& c) {
  size_t n = c.n_rows();
  DedupSet d;
  size_t cap = 1;
  while (cap < n * 2) cap <<= 1;  // power of two, <=50% load
  const uint32_t kEmpty = 0xFFFFFFFFu;
  std::vector<uint32_t> table(cap, kEmpty);
  uint64_t mask = cap - 1;
  d.offsets.push_back(0);
  d.refs.resize(n);
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
      if (clen == len && std::memcmp(d.bytes.data() + off, row, len) == 0) {
        id = cur;
        break;
      }
      slot = (slot + 1) & mask;  // linear probe
    }
    d.refs[i] = id;
  }
  d.n_distinct = n_distinct;
  return d;
}

constexpr size_t kDictOnPairHeader = 16;

std::vector<uint8_t> BuildDictOnPairPayload(const Corpus& c, const op::Config& cfg) {
  DedupSet d = BuildDedupSet(c);
  op::Column col = op::Compress(d.bytes.data(), d.bytes.size(), d.offsets.data(),
                                d.n_distinct, cfg);
  size_t dict_bytes = col.dict.logical_bytes();
  size_t code_bits = IndexBits(col.dict.num_tokens());
  size_t off_bits = std::max<size_t>(1, BitWidth(dict_bytes));

  size_t dmax = 0;
  for (size_t j = 0; j + 1 < d.offsets.size(); ++j)
    dmax = std::max<size_t>(dmax, d.offsets[j + 1] - d.offsets[j]);
  size_t dlen_bits = std::max<size_t>(1, BitWidth(dmax));
  size_t ref_bits = IndexBits(d.n_distinct);

  std::vector<uint8_t> v;
  PutU32(&v, static_cast<uint32_t>(d.n_distinct));
  PutU32(&v, static_cast<uint32_t>(c.n_rows()));
  PutU32(&v, static_cast<uint32_t>(d.bytes.size()));
  v.push_back(static_cast<uint8_t>(ref_bits));
  v.push_back(static_cast<uint8_t>(dlen_bits));
  v.push_back(0);
  v.push_back(0);

  PutU32(&v, static_cast<uint32_t>(col.dict.num_tokens()));
  PutU32(&v, static_cast<uint32_t>(dict_bytes));
  PutU32(&v, static_cast<uint32_t>(col.codes.size()));
  v.push_back(static_cast<uint8_t>(code_bits));
  v.push_back(static_cast<uint8_t>(off_bits));
  v.push_back(0);
  v.push_back(0);

  v.insert(v.end(), col.dict.bytes.begin(), col.dict.bytes.begin() + dict_bytes);

  std::vector<uint32_t> offs(col.dict.offsets.begin(), col.dict.offsets.end());
  std::vector<uint8_t> packed_offs = op::PackValues(offs.data(), offs.size(), off_bits);
  packed_offs.resize(BitPackedBytes(offs.size(), off_bits));
  v.insert(v.end(), packed_offs.begin(), packed_offs.end());

  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed_codes = op::PackValues(cw.data(), cw.size(), code_bits);
  packed_codes.resize(BitPackedBytes(cw.size(), code_bits));
  v.insert(v.end(), packed_codes.begin(), packed_codes.end());

  std::vector<uint32_t> dlens(d.n_distinct);
  for (size_t j = 0; j < d.n_distinct; ++j) dlens[j] = d.offsets[j + 1] - d.offsets[j];
  std::vector<uint8_t> packed_dlens = op::PackValues(dlens.data(), dlens.size(), dlen_bits);
  packed_dlens.resize(BitPackedBytes(dlens.size(), dlen_bits));
  v.insert(v.end(), packed_dlens.begin(), packed_dlens.end());

  std::vector<uint8_t> packed_refs = op::PackValues(d.refs.data(), d.refs.size(), ref_bits);
  packed_refs.resize(BitPackedBytes(d.refs.size(), ref_bits));
  v.insert(v.end(), packed_refs.begin(), packed_refs.end());
  return v;
}

// The dedup cascade's own budget sweep. PickOnPairBits cannot be reused: it scores
// OnPairSize over the whole column, whereas this payload trains OnPair over the
// *distinct set* and then adds packed references and distinct-value lengths, so
// the width that stores the column least is not the width that stores this least.
//
// It scores the assembled payload rather than a size formula on purpose -- the
// payload has five sections, and a parallel formula would drift from
// BuildDictOnPairPayload the first time either one changed. The build is wasted
// work, but this runs in per-corpus setup, not in the timed region.
uint8_t PickDictOnPairBits(const Corpus& c, double threshold) {
  uint8_t best_bits = 9;
  size_t best_sz = SIZE_MAX;
  for (uint8_t b = 9; b <= 16; ++b) {
    size_t sz = BuildDictOnPairPayload(c, op::Config{b, threshold, 42}).size();
    if (sz < best_sz) {
      best_sz = sz;
      best_bits = b;
    }
  }
  return best_bits;
}

// Materialize the distinct values (one OnPair decode), rebuild their offsets from
// the packed length array, then gather rows by unpacking each reference. Values of
// <=16 bytes take one branchless 128-bit store; `scratch` and `out` are padded.
//
// Unlike the companion benchmark, the offset rebuild is inside the timed region:
// this payload is self-describing, so a reader really does pay it. The difference
// is a prefix sum over the distinct set, which is negligible where dedup wins and
// only matters on all-distinct columns, where dedup loses regardless.
size_t DecodeDictOnPairPayload(const uint8_t* p, uint8_t* out,
                               std::vector<uint8_t>* scratch) {
  size_t n_distinct = GetU32(p);
  size_t n_rows = GetU32(p + 4);
  size_t distinct_bytes = GetU32(p + 8);
  size_t ref_bits = p[12];
  size_t dlen_bits = p[13];

  const uint8_t* q = p + kDictOnPairHeader;
  size_t num_tokens = GetU32(q);
  size_t dict_bytes = GetU32(q + 4);
  size_t num_codes = GetU32(q + 8);
  size_t code_bits = q[12];
  size_t off_bits = q[13];

  const uint8_t* dict_blob = q + kOnPairHeader;
  const uint8_t* packed_offs = dict_blob + dict_bytes;
  const uint8_t* packed_codes = packed_offs + BitPackedBytes(num_tokens + 1, off_bits);
  const uint8_t* packed_dlens = packed_codes + BitPackedBytes(num_codes, code_bits);
  const uint8_t* packed_refs = packed_dlens + BitPackedBytes(n_distinct, dlen_bits);

  op::CompactDictionary dict;
  dict.bytes.resize(dict_bytes + op::kDecodePadding, 0);
  std::memcpy(dict.bytes.data(), dict_blob, dict_bytes);
  dict.offsets.resize(num_tokens + 1);
  for (size_t t = 0; t <= num_tokens; ++t) {
    dict.offsets[t] = op::GetBits(packed_offs, t * off_bits, off_bits);
  }
  dict.RecomputeMaxTokenLen();

  if (scratch->size() < distinct_bytes + op::kDecodePadding) {
    scratch->assign(distinct_bytes + op::kDecodePadding, 0);
  }
  op::DecompressPacked(dict, packed_codes, num_codes, code_bits, scratch->data());

  std::vector<uint32_t> doff(n_distinct + 1);
  doff[0] = 0;
  for (size_t j = 0; j < n_distinct; ++j) {
    doff[j + 1] = doff[j] + op::GetBits(packed_dlens, j * dlen_bits, dlen_bits);
  }

  const uint8_t* dbuf = scratch->data();
  size_t w = 0, bp = 0;
  for (size_t i = 0; i < n_rows; ++i) {
    uint32_t id = op::GetBits(packed_refs, bp, ref_bits);
    bp += ref_bits;
    size_t off = doff[id];
    size_t len = doff[id + 1] - off;
    if (len <= 16) {
      std::memcpy(out + w, dbuf + off, 16);
    } else {
      std::memcpy(out + w, dbuf + off, len);
    }
    w += len;
  }
  return w;
}

// Group B: Parquet's own byte-array encodings, through the real encoder/decoder.

std::shared_ptr<parquet::ColumnDescriptor> ByteArrayDescr() {
  auto node = parquet::schema::PrimitiveNode::Make("ba", parquet::Repetition::REQUIRED,
                                                   parquet::Type::BYTE_ARRAY);
  return std::make_shared<parquet::ColumnDescriptor>(node, /*max_definition_level=*/0,
                                                     /*max_repetition_level=*/0);
}

std::vector<parquet::ByteArray> CorpusValues(const Corpus& c) {
  std::vector<parquet::ByteArray> vals(c.n_rows());
  for (size_t i = 0; i < c.n_rows(); ++i) {
    vals[i] = parquet::ByteArray(c.offsets[i + 1] - c.offsets[i], c.bytes.data() + c.offsets[i]);
  }
  return vals;
}

// Copy the decoded ByteArrays into one contiguous buffer. PLAIN and
// DELTA_LENGTH_BYTE_ARRAY hand back pointers into the page, so without this a
// "decode" would be pointer arithmetic and the comparison meaningless.
size_t Gather(const std::vector<parquet::ByteArray>& vals, uint8_t* out) {
  size_t w = 0;
  for (const parquet::ByteArray& v : vals) {
    std::memcpy(out + w, v.ptr, v.len);
    w += v.len;
  }
  return w;
}

// PLAIN / DELTA_LENGTH_BYTE_ARRAY / DELTA_BYTE_ARRAY: a single self-describing
// buffer, so nothing is added to it -- these encodings carry their own lengths.
std::vector<uint8_t> BuildPqPayload(const Corpus& c,
                                    const std::vector<parquet::ByteArray>& vals,
                                    parquet::Encoding::type e) {
  auto enc = parquet::MakeTypedEncoder<parquet::ByteArrayType>(e);
  enc->Put(vals.data(), static_cast<int>(c.n_rows()));
  auto buf = enc->FlushValues();
  return std::vector<uint8_t>(buf->data(), buf->data() + buf->size());
}

size_t DecodePqPayload(const Corpus& c, parquet::Encoding::type e, const uint8_t* p,
                       size_t size, uint8_t* out) {
  int n = static_cast<int>(c.n_rows());
  auto dec = parquet::MakeTypedDecoder<parquet::ByteArrayType>(e);
  dec->SetData(n, p, static_cast<int>(size));
  std::vector<parquet::ByteArray> vals(n);
  int got = dec->Decode(vals.data(), n);
  if (got != n) {
    std::fprintf(stderr, "parquet decode short read (%d of %d)\n", got, n);
    std::abort();
  }
  return Gather(vals, out);
}

// RLE_DICTIONARY is two streams -- a dictionary page and an index page -- so the
// payload concatenates them behind a header, and both are charged.
//
// Payload: [u32 dict_bytes][u32 idx_bytes][u32 num_entries][u32 dict page encoding]
//          [dict page][indices]
//
// `dict_page_enc` is PLAIN for the RLE_DICTIONARY rows, which is the only thing a
// writer may emit: a BYTE_ARRAY dictionary page is PLAIN-encoded by spec. Passing
// anything else measures a *hypothetical* format change -- the fair opponent for
// replacing the dictionary page with an OnPair blob, since both ask the same
// question of the spec. It is stored in the header rather than threaded through
// the decode lambda so the payload stays self-describing; the word it occupies
// was already there as padding, so the RLE_DICTIONARY sizes do not move.
constexpr size_t kDictHeader = 16;

std::vector<uint8_t> BuildDictPayload(const Corpus& c,
                                      const std::vector<parquet::ByteArray>& vals,
                                      const parquet::ColumnDescriptor* descr,
                                      parquet::Encoding::type dict_page_enc) {
  auto base = parquet::MakeEncoder(parquet::Type::BYTE_ARRAY, parquet::Encoding::PLAIN,
                                   /*use_dictionary=*/true, descr);
  auto* enc = dynamic_cast<parquet::TypedEncoder<parquet::ByteArrayType>*>(base.get());
  auto* dict_enc = dynamic_cast<parquet::DictEncoder<parquet::ByteArrayType>*>(base.get());
  enc->Put(vals.data(), static_cast<int>(c.n_rows()));

  size_t dict_bytes = static_cast<size_t>(dict_enc->dict_encoded_size());
  std::vector<uint8_t> dict_page(dict_bytes);
  dict_enc->WriteDict(dict_page.data());
  int num_entries = dict_enc->num_entries();

  std::vector<uint8_t> idx(static_cast<size_t>(enc->EstimatedDataEncodedSize()) + 16);
  int idx_bytes = dict_enc->WriteIndices(idx.data(), static_cast<int>(idx.size()));
  if (idx_bytes <= 0) {
    std::fprintf(stderr, "RLE_DICTIONARY: WriteIndices failed\n");
    std::abort();
  }

  // Re-encode the dictionary entries under another encoding. The detour through
  // PLAIN is unavoidable: WriteDict is the only public way to get the entries in
  // id order, and that order has to survive or the index stream stops matching.
  // It makes this variant's *encode* throughput pessimistic -- a writer for such
  // a format would encode the entries once -- but leaves ratio and decode exact.
  if (dict_page_enc != parquet::Encoding::PLAIN) {
    auto pd = parquet::MakeTypedDecoder<parquet::ByteArrayType>(parquet::Encoding::PLAIN);
    pd->SetData(num_entries, dict_page.data(), static_cast<int>(dict_bytes));
    std::vector<parquet::ByteArray> entries(num_entries);
    if (pd->Decode(entries.data(), num_entries) != num_entries) {
      std::fprintf(stderr, "dictionary page re-encode: short read\n");
      std::abort();
    }
    auto re = parquet::MakeTypedEncoder<parquet::ByteArrayType>(dict_page_enc);
    re->Put(entries.data(), num_entries);
    auto buf = re->FlushValues();
    dict_page.assign(buf->data(), buf->data() + buf->size());
    dict_bytes = dict_page.size();
  }

  std::vector<uint8_t> v;
  v.reserve(kDictHeader + dict_bytes + static_cast<size_t>(idx_bytes));
  PutU32(&v, static_cast<uint32_t>(dict_bytes));
  PutU32(&v, static_cast<uint32_t>(idx_bytes));
  PutU32(&v, static_cast<uint32_t>(num_entries));
  PutU32(&v, static_cast<uint32_t>(dict_page_enc));
  v.insert(v.end(), dict_page.begin(), dict_page.end());
  v.insert(v.end(), idx.begin(), idx.begin() + idx_bytes);
  return v;
}

size_t DecodeDictPayload(const Corpus& c, const parquet::ColumnDescriptor* descr,
                         const uint8_t* p, size_t /*size*/, uint8_t* out) {
  size_t dict_bytes = GetU32(p);
  size_t idx_bytes = GetU32(p + 4);
  int num_entries = static_cast<int>(GetU32(p + 8));
  auto dict_page_enc = static_cast<parquet::Encoding::type>(GetU32(p + 12));
  const uint8_t* dict_page = p + kDictHeader;
  const uint8_t* idx = dict_page + dict_bytes;

  // SetDict below takes any TypedDecoder, so the dictionary page's encoding is
  // free to vary while the index path stays identical.
  auto dict_dec = parquet::MakeTypedDecoder<parquet::ByteArrayType>(dict_page_enc);
  dict_dec->SetData(num_entries, dict_page, static_cast<int>(dict_bytes));

  int n = static_cast<int>(c.n_rows());
  auto dec = parquet::MakeDictDecoder<parquet::ByteArrayType>(descr);
  dec->SetDict(dict_dec.get());
  dec->SetData(n, idx, static_cast<int>(idx_bytes));
  std::vector<parquet::ByteArray> vals(n);
  int got = dec->Decode(vals.data(), n);
  if (got != n) {
    std::fprintf(stderr, "RLE_DICTIONARY decode short read (%d of %d)\n", got, n);
    std::abort();
  }
  return Gather(vals, out);
}

// zstd / lz4 over the concatenated value bytes, in two accountings.
//
// The companion benchmark charges zstd/lz4 a *separate, uncompressed* bit-packed
// row-length array, because a raw frame of concatenated bytes cannot recover row
// boundaries on its own. A real Parquet page puts the lengths inside the page, so
// the page compressor sees them too -- and on columns of near-constant width that
// array is almost free once compressed. Both are reported here: the split-length
// figure is what Tables 1-3 print, the in-page figure is what Parquet stores.

std::vector<uint8_t> BuildConcatPayload(const Corpus& c, bool with_lengths) {
  std::vector<uint8_t> v = c.bytes;
  if (with_lengths) AppendLengths(c, &v);
  return v;
}

}  // namespace

int main(int argc, char** argv) {
  std::string dir = CorpusDir(argc, argv);
  std::vector<std::filesystem::path> files = CorpusFiles(dir);
  if (files.empty()) {
    std::fprintf(stderr, "no .txt corpora in %s\n", dir.c_str());
    return 1;
  }
  auto descr = ByteArrayDescr();

  std::printf("%-30s %10s %10s   %7s %9s %9s\n", "corpus", "rows", "raw MiB", "ratio",
              "enc MiB/s", "dec MiB/s");
  std::printf("%s\n", std::string(92, '-').c_str());

  for (const auto& path : files) {
    Corpus c = ReadCorpus(path);
    double threshold = ThresholdFor(c.name);
    std::vector<parquet::ByteArray> vals = CorpusValues(c);
    const size_t out_cap = c.raw_bytes() + op::kDecodePadding + kPad;

    op::Config cfg{PickOnPairBits(c, threshold), threshold, 42};
    op::Config cfg16{16, threshold, 42};
    op::Config cfg_dop{PickDictOnPairBits(c, threshold), threshold, 42};
    std::vector<uint8_t> dop_scratch;

    auto fsst_build = [&] { return BuildFsstPayload(c); };
    auto fsst_decode = [&](const uint8_t* p, size_t s, uint8_t* o) {
      return DecodeFsstPayload(p, s, o, out_cap);
    };
    auto op_build = [&] { return BuildOnPairPayload(c, cfg, /*byte_aligned=*/false); };
    auto op16_build = [&] { return BuildOnPairPayload(c, cfg16, /*byte_aligned=*/false); };
    auto opba_build = [&] { return BuildOnPairPayload(c, cfg, /*byte_aligned=*/true); };
    auto op_decode = [&](const uint8_t* p, size_t s, uint8_t* o) {
      return DecodeOnPairPayload(p, s, o);
    };
    auto dop_build = [&] { return BuildDictOnPairPayload(c, cfg16); };
    auto dop_auto_build = [&] { return BuildDictOnPairPayload(c, cfg_dop); };
    auto dop_decode = [&](const uint8_t* p, size_t /*s*/, uint8_t* o) {
      return DecodeDictOnPairPayload(p, o, &dop_scratch);
    };
    auto concat_build = [&] { return BuildConcatPayload(c, /*with_lengths=*/true); };
    auto bytes_build = [&] { return BuildConcatPayload(c, /*with_lengths=*/false); };
    auto concat_decode = [&](const uint8_t* p, size_t /*s*/, uint8_t* o) {
      std::memcpy(o, p, c.raw_bytes());
      return c.raw_bytes();
    };
    auto pq = [&](parquet::Encoding::type e) {
      return std::make_pair(BuildFn([&, e] { return BuildPqPayload(c, vals, e); }),
                            DecodeFn([&, e](const uint8_t* p, size_t s, uint8_t* o) {
                              return DecodePqPayload(c, e, p, s, o);
                            }));
    };
    auto dict_of = [&](parquet::Encoding::type dpe) {
      return std::make_pair(
          BuildFn([&, dpe] { return BuildDictPayload(c, vals, descr.get(), dpe); }),
          DecodeFn([&](const uint8_t* p, size_t s, uint8_t* o) {
            return DecodeDictPayload(c, descr.get(), p, s, o);
          }));
    };
    auto dict_plain = dict_of(parquet::Encoding::PLAIN);
    auto dict_dlba = dict_of(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY);
    const BuildFn& dict_build = dict_plain.first;
    const DecodeFn& dict_decode = dict_plain.second;

    std::vector<Measured> ms;
    // Reference points, measured here so every ratio in this table shares one
    // accounting. The two [split len] rows reproduce the companion benchmark's
    // zstd/lz4 columns exactly (uncompressed row-length array charged on the
    // side); the two [len in page] rows are the same codecs with the lengths
    // inside the compressed page, which is what Parquet actually writes. FSST
    // and OnPair alone already match Tables 1-3, since their payload carries the
    // bit-packed length array uncompressed either way.
    ms.push_back(RunCodec(c, "zstd(1) [split len]", Generic::kZstd1, bytes_build, concat_decode,
                          c.len_array_bytes()));
    ms.push_back(RunCodec(c, "lz4 [split len]", Generic::kLz4, bytes_build, concat_decode,
                          c.len_array_bytes()));
    ms.push_back(
        RunCodec(c, "zstd(1) [len in page]", Generic::kZstd1, concat_build, concat_decode));
    ms.push_back(RunCodec(c, "lz4 [len in page]", Generic::kLz4, concat_build, concat_decode));
    ms.push_back(RunCodec(c, "FSST", Generic::kNone, fsst_build, fsst_decode));
    ms.push_back(RunCodec(c, "OnPair-auto", Generic::kNone, op_build, op_decode));
    ms.push_back(RunCodec(c, "OnPair16", Generic::kNone, op16_build, op_decode));
    ms.push_back(RunCodec(c, "DICT+OnPair", Generic::kNone, dop_build, dop_decode));
    ms.push_back(RunCodec(c, "DICT+OnPair-auto", Generic::kNone, dop_auto_build, dop_decode));

    // Group A: cascade a generic codec over FSST / OnPair.
    ms.push_back(RunCodec(c, "FSST+zstd(1)", Generic::kZstd1, fsst_build, fsst_decode));
    ms.push_back(RunCodec(c, "FSST+lz4", Generic::kLz4, fsst_build, fsst_decode));
    ms.push_back(RunCodec(c, "OnPair-auto+zstd(1)", Generic::kZstd1, op_build, op_decode));
    ms.push_back(RunCodec(c, "OnPair-auto+lz4", Generic::kLz4, op_build, op_decode));
    ms.push_back(RunCodec(c, "OnPair-bytealign", Generic::kNone, opba_build, op_decode));
    ms.push_back(
        RunCodec(c, "OnPair-bytealign+zstd(1)", Generic::kZstd1, opba_build, op_decode));

    // Group B: Parquet's own byte-array encodings, alone and cascaded.
    struct PqCase {
      const char* label;
      parquet::Encoding::type enc;
    };
    for (const PqCase& pc :
         {PqCase{"PLAIN", parquet::Encoding::PLAIN},
          PqCase{"DELTA_LENGTH_BYTE_ARRAY", parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY},
          PqCase{"DELTA_BYTE_ARRAY", parquet::Encoding::DELTA_BYTE_ARRAY}}) {
      auto fns = pq(pc.enc);
      for (Generic g : {Generic::kNone, Generic::kZstd1, Generic::kLz4}) {
        ms.push_back(RunCodec(c, pc.label + std::string(GenericSuffix(g)), g, fns.first,
                              fns.second));
      }
    }
    for (Generic g : {Generic::kNone, Generic::kZstd1, Generic::kLz4}) {
      ms.push_back(RunCodec(c, "RLE_DICTIONARY" + std::string(GenericSuffix(g)), g, dict_build,
                            dict_decode));
    }
    // A dictionary page carrying DELTA_LENGTH_BYTE_ARRAY instead of PLAIN. Not a
    // page any writer can emit -- the spec fixes BYTE_ARRAY dictionary pages at
    // PLAIN -- so these two rows measure a format change, and are the opponent
    // DICT+OnPair deserves: both replace the dictionary page's encoding and leave
    // the index stream alone. Encode throughput here is pessimistic; see
    // BuildDictPayload.
    for (Generic g : {Generic::kNone, Generic::kZstd1}) {
      ms.push_back(RunCodec(c, "DICT+DLBA" + std::string(GenericSuffix(g)), g, dict_dlba.first,
                            dict_dlba.second));
    }

    std::printf("%-30s %10zu %10.2f\n", c.name.c_str(), c.n_rows(), Mib(c.raw_bytes()));
    auto ratio = [&](const Measured& m) {
      return static_cast<double>(c.raw_bytes()) / static_cast<double>(m.compressed_bytes);
    };
    for (const Measured& m : ms) {
      std::printf("  %-32s %10.2f  %8.3fx %9.1f %9.1f\n", m.label.c_str(),
                  Mib(m.compressed_bytes), ratio(m), m.encode_mibs, m.decode_mibs);
    }

    // The two questions this benchmark exists to answer, stated per corpus:
    // what the cascade buys over the encoding alone, and how the best
    // Parquet-native page compares with OnPair alone.
    auto find = [&](const std::string& label) -> const Measured* {
      for (const Measured& m : ms)
        if (m.label == label) return &m;
      return nullptr;
    };
    const Measured* fsst_alone = find("FSST");
    const Measured* fsst_z = find("FSST+zstd(1)");
    const Measured* op_alone = find("OnPair-auto");
    const Measured* op_z = find("OnPair-auto+zstd(1)");
    const Measured* opba_z = find("OnPair-bytealign+zstd(1)");
    std::printf("  -> FSST+zstd vs FSST alone:        ratio %+.1f%%, decode %+.1f%%\n",
                (ratio(*fsst_z) / ratio(*fsst_alone) - 1.0) * 100.0,
                (fsst_z->decode_mibs / fsst_alone->decode_mibs - 1.0) * 100.0);
    std::printf("  -> OnPair+zstd vs OnPair alone:    ratio %+.1f%%, decode %+.1f%%\n",
                (ratio(*op_z) / ratio(*op_alone) - 1.0) * 100.0,
                (op_z->decode_mibs / op_alone->decode_mibs - 1.0) * 100.0);
    std::printf("  -> OnPair byte-aligned+zstd vs bit-packed OnPair alone: ratio %+.1f%%\n",
                (ratio(*opba_z) / ratio(*op_alone) - 1.0) * 100.0);

    const Measured* best_pq = nullptr;
    for (const Measured& m : ms) {
      if (m.label.rfind("PLAIN", 0) != 0 && m.label.rfind("DELTA", 0) != 0 &&
          m.label.rfind("RLE_DICTIONARY", 0) != 0) {
        continue;
      }
      if (best_pq == nullptr || m.compressed_bytes < best_pq->compressed_bytes) best_pq = &m;
    }
    std::printf("  -> best Parquet-native (%s): %.3fx; OnPair alone %+.1f%% vs it\n\n",
                best_pq->label.c_str(), ratio(*best_pq),
                (ratio(*op_alone) / ratio(*best_pq) - 1.0) * 100.0);
    std::fflush(stdout);
  }
  return 0;
}
