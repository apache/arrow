// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// C++ implementation of the OnPair short-string compression codec (encode +
// decode paths only), for a like-for-like comparison against FSST inside Arrow's
// benchmark harness.
//
// Algorithm: F. Gargiulo and R. Venturini, "OnPair: Short Strings Compression
// for Fast Random Access," arXiv:2508.02280, 2025. This implements the paper's
// core scheme: a dictionary of the 256 single bytes plus frequent merged pairs,
// greedy longest-prefix tokenization into u16 codes, and a table-lookup decode.
//
// NOT a production Parquet encoder - this is a benchmark artifact.
//
// Conformant with the paper: the two-tier longest-prefix index (sec 3.4.1, a hash
// map for <=8-byte tokens + 8-byte-prefix buckets with suffixes sorted
// descending), the 16-byte max token of OnPair16 (sec 3.2.2), and the fixed-16-byte
// SIMD gather-copy decode (sec 3.5, Alg. 3) - advance by the token's true length,
// relying on 16-byte source read-padding and output write-padding.
//
// DEVIATIONS FROM THE PAPER (engineering choices; they do not affect the code
// format or correctness, only the trained dictionary and encode-time behavior):
//   D1. Merge threshold. The paper (sec 3.2.1) fixes it per dataset as
//       max(2, floor(log2(S_MiB))). This port instead uses an adaptive
//       controller paced to a byte budget (`DynamicThresholdController`), so the
//       trained dictionary differs from the paper's.
//   D2. Long-bucket overflow. The paper's OnPair16 caps each bucket at 128
//       suffixes (sec 3.4.4), dropping extras; this port promotes an over-full
//       bucket to a trie (`PROMOTE_THRESHOLD`), keeping all suffixes.
//   D3. Static perfect-hash LPM. The paper finalizes long-pattern lookup with a
//       minimal perfect hash for the read-only parsing phase (sec 3.4.3); this port
//       keeps std::unordered_map (the paper notes that path is Rust-only).
//   D4. Training-sample selection uses a fixed-seed splitmix64 *full*
//       Fisher-Yates shuffle (`PartialShuffle` over all rows). Shuffle *extent*,
//       not the RNG choice, is what affects the trained dictionary: a full
//       shuffle avoids skew on sequentially-ordered columns (the Rust crate
//       partial-shuffles only a tail prefix, skewing patterned data like
//       Customer#000…). The exact RNG is not specified by the paper, so output
//       is deterministic but not bit-identical to the crate.
//
// Little-endian hosts only.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace parquet::onpair {

/// A dictionary entry id and, equivalently, a code in the code stream.
using Token = uint16_t;

/// Maximum byte length of any dictionary token, and the fixed width the decoder
/// over-reads per token.
constexpr size_t kMaxTokenSize = 16;

/// Trailing slack an output buffer needs beyond the decoded length: the decoder
/// over-stores a fixed 16-byte chunk for the final token.
constexpr size_t kDecodePadding = kMaxTokenSize;

/// Training configuration. Mirrors the reference `Config`.
struct Config {
  /// Dictionary-size budget: at most 2^max_dict_bits tokens. Valid range 9..=16.
  uint8_t max_dict_bits = 12;
  /// Dynamic-threshold byte-sampling fraction, in (0, 1].
  double threshold_fraction = 0.15;
  /// Deterministic sampling seed.
  uint64_t seed = 42;

  static Config Dict12() { return Config{12, 0.15, 42}; }
  static Config Dict16() { return Config{16, 0.15, 42}; }
};

/// The token table a code stream indexes into: Arrow-binary layout (flat bytes +
/// u32 offsets). `bytes` is read-padded by kMaxTokenSize so the decoder's fixed
/// 16-byte over-read stays in bounds.
struct CompactDictionary {
  std::vector<uint8_t> bytes;    // read-padded
  std::vector<uint32_t> offsets;  // length num_tokens + 1

  /// Length of the longest token present, which is what the decoder's gather-copy
  /// sizes its fixed copy width from. Often well below kMaxTokenSize: TPC-H
  /// c_address tops out at 5 bytes, and copying 16 there moves 8x the bytes it
  /// needs to.
  ///
  /// This must never UNDERSTATE the true maximum -- doing so would make the
  /// decoder copy less than a token's length and silently truncate. It therefore
  /// defaults to the conservative kMaxTokenSize, so a dictionary that never calls
  /// RecomputeMaxTokenLen still decodes correctly and merely forgoes the
  /// narrowing. A stored format would carry this in its header rather than
  /// recompute it, which is why the decoder reads it instead of scanning: an
  /// O(tokens) scan per decode call costs 1-3% on dictionaries of 20-60k tokens.
  size_t max_token_len = kMaxTokenSize;

  /// Derive max_token_len from `offsets`. Call after building or replacing them.
  void RecomputeMaxTokenLen() {
    size_t m = 0;
    for (size_t t = 0; t + 1 < offsets.size(); ++t) {
      const size_t len = offsets[t + 1] - offsets[t];
      if (len > m) m = len;
    }
    // An empty dictionary decodes nothing; stay conservative rather than pick a
    // width from no evidence.
    max_token_len = m == 0 ? kMaxTokenSize : m;
  }

  size_t num_tokens() const { return offsets.empty() ? 0 : offsets.size() - 1; }
  const uint8_t* token_ptr(Token id) const { return bytes.data() + offsets[id]; }
  size_t token_len(Token id) const { return offsets[id + 1] - offsets[id]; }
  /// Logical (unpadded) byte size of the dictionary blob.
  size_t logical_bytes() const { return offsets.empty() ? 0 : offsets.back(); }
};

/// A compressed string column. `codes` is the row-concatenated code stream;
/// row k is codes[row_offsets[k] .. row_offsets[k+1]].
struct Column {
  CompactDictionary dict;
  std::vector<uint16_t> codes;
  std::vector<uint32_t> row_offsets;  // length num_rows + 1

  size_t num_rows() const {
    return row_offsets.empty() ? 0 : row_offsets.size() - 1;
  }
};

/// Wall-clock seconds spent in each phase of Compress. The three sum to the
/// whole call. Only for attributing encode cost; pass null in a timed run.
struct EncodeProfile {
  double train_s = 0;     ///< greedy pairing pass over the shuffled sample
  double rebuild_s = 0;   ///< sort the dictionary, rebuild the matcher over it
  double tokenize_s = 0;  ///< tokenize every row against the frozen dictionary
};

/// Train a dictionary against (bytes, offsets) and greedily tokenize every row.
/// `offsets` has length num_rows + 1; row i is bytes[offsets[i]..offsets[i+1]].
Column Compress(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                size_t num_rows, const Config& cfg, EncodeProfile* profile = nullptr);

/// Exact decoded byte length of the whole column (sum of token lengths).
size_t DecodedLen(const Column& col);

/// Decode the whole column into `out`, returning bytes written.
/// Precondition: out capacity >= DecodedLen(col) + kDecodePadding.
size_t DecompressInto(const Column& col, uint8_t* out);

// --- Bit-packed code stream (what a real stored format uses) ----------------
// The in-memory Column holds u16 codes; on storage the code stream is packed at
// the true code width (ceil(log2 num_tokens)). These pack/unpack it so decode
// pays the real unpacking cost, keeping ratio and decode mutually consistent.

/// Read `nbits` (<=25) at bit offset `bitpos`, little-endian / LSB-first.
/// `p` must have 4 readable bytes at the containing word.
inline uint32_t GetBits(const uint8_t* p, size_t bitpos, size_t nbits) {
  uint32_t w;
  std::memcpy(&w, p + (bitpos >> 3), 4);
  return (w >> (bitpos & 7)) & (nbits >= 32 ? 0xFFFFFFFFu : ((1u << nbits) - 1));
}

/// Pack `n` values (each < 2^bits, bits in 1..=25) LSB-first. The result has 4
/// trailing pad bytes (so a 4-byte window at the last value is in bounds); the
/// logical size is (n*bits+7)/8.
std::vector<uint8_t> PackValues(const uint32_t* vals, size_t n, size_t bits);

/// Decode a bit-packed code stream: read `bits` per code and gather-copy the
/// token. `packed` needs >=4 trailing pad bytes; `out` >= DecodedLen + padding.
size_t DecompressPacked(const CompactDictionary& dict, const uint8_t* packed,
                        size_t ncodes, size_t bits, uint8_t* out);

}  // namespace parquet::onpair
