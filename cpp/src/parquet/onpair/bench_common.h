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

// Pieces shared by the standalone string-encoding benchmarks: the packed corpus
// representation, the size-accounting helpers, and the timing conventions.
//
// This header exists so that fsst_onpair_benchmark.cc (FSST / zstd / lz4 /
// OnPair, no Arrow dependency) and cascade_benchmark.cc (the same codecs plus
// Parquet's own byte-array encodings, which needs libparquet) cannot drift apart
// on how bytes are counted. A ratio is only comparable across the two binaries
// if every codec in both is charged the same way, so the accounting lives here
// and nowhere else.

#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

// zstd: declare the small, ABI-stable subset we use so the standalone build
// needs only the installed libzstd (no dev header). Link libzstd.so directly.
extern "C" {
size_t ZSTD_compress(void* dst, size_t dstCapacity, const void* src, size_t srcSize, int level);
size_t ZSTD_decompress(void* dst, size_t dstCapacity, const void* src, size_t compressedSize);
size_t ZSTD_compressBound(size_t srcSize);
unsigned ZSTD_isError(size_t code);
// lz4 (fast block compressor) - ABI-stable subset; link the installed liblz4.
int LZ4_compressBound(int inputSize);
int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);
int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity);
}

namespace bench {

using Clock = std::chrono::steady_clock;

constexpr int kEncodeIters = 3;
constexpr int kDecodeIters = 10;

inline double Mib(size_t bytes) { return static_cast<double>(bytes) / (1024.0 * 1024.0); }

inline double Median(std::vector<double> v) {
  std::sort(v.begin(), v.end());
  return v[v.size() / 2];
}

// Bits to store a value in [0, x]  (x==0 -> 0 bits).
inline size_t BitWidth(uint64_t x) {
  return x == 0 ? 0 : 64 - static_cast<size_t>(__builtin_clzll(x));
}
// Bits to index `count` distinct symbols [0, count)  (== ceil(log2 count), >=1).
inline size_t IndexBits(size_t count) {
  return count <= 1 ? 1
                    : (64 - static_cast<size_t>(
                                __builtin_clzll(static_cast<uint64_t>(count - 1))));
}
inline size_t BitPackedBytes(size_t n, size_t bits) { return (n * bits + 7) / 8; }

// A packed corpus: concatenated bytes + (n+1) u32 offsets.
struct Corpus {
  std::string name;
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;
  size_t n_rows() const { return offsets.size() - 1; }
  size_t raw_bytes() const { return bytes.size(); }
  size_t max_row_len() const {
    size_t m = 0;
    for (size_t i = 0; i < n_rows(); ++i) m = std::max<size_t>(m, offsets[i + 1] - offsets[i]);
    return m;
  }
  // Realistic per-row row-length side array (delta offsets), bit-packed at the
  // width of the longest row. Charged to every value-preserving codec (FSST,
  // zstd, lz4, OnPair) so the row boundaries are accounted the way a real
  // columnar format stores them - not as raw (n+1) u32.
  //
  // NOT charged to Parquet's own byte-array encodings: PLAIN and the DELTA_*
  // family embed their lengths in the encoded payload, so adding this on top
  // would count row boundaries twice.
  size_t len_array_bytes() const {
    return BitPackedBytes(n_rows(), std::max<size_t>(1, BitWidth(max_row_len())));
  }
};

// Read a newline-delimited file (one row per line) into a packed corpus.
inline Corpus ReadCorpus(const std::filesystem::path& path) {
  Corpus c;
  c.name = path.stem().string();
  std::ifstream in(path, std::ios::binary);
  c.offsets.push_back(0);
  std::string line;
  while (std::getline(in, line)) {
    c.bytes.insert(c.bytes.end(), line.begin(), line.end());
    c.offsets.push_back(static_cast<uint32_t>(c.bytes.size()));
  }
  return c;
}

struct Measured {
  std::string label;
  size_t compressed_bytes = 0;
  double encode_mibs = 0;
  double decode_mibs = 0;
};

// TPC-H columns train with a 0.2 sample fraction; the URL corpus with 0.5
// (matching the Rust harness).
inline double ThresholdFor(const std::string& name) {
  return name.rfind("tpch_", 0) == 0 ? 0.2 : 0.5;
}

// Collect the .txt corpora in `dir`, sorted, so both binaries iterate the same
// set in the same order.
inline std::vector<std::filesystem::path> CorpusFiles(const std::string& dir) {
  std::vector<std::filesystem::path> files;
  for (const auto& e : std::filesystem::directory_iterator(dir)) {
    if (e.path().extension() == ".txt") files.push_back(e.path());
  }
  std::sort(files.begin(), files.end());
  return files;
}

// Resolve the corpus directory: argv[1], then $ONPAIR_BENCH_DIR, then the
// generator's default output path.
inline std::string CorpusDir(int argc, char** argv) {
  if (argc > 1) return argv[1];
  if (const char* env = std::getenv("ONPAIR_BENCH_DIR")) return env;
  return "bench-fsst-onpair/corpora";
}

}  // namespace bench
