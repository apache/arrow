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

// OnPair short-string compression for benchmark comparisons with FSST. The
// implementation follows arXiv:2508.02280 but uses adaptive merge thresholds,
// trie-backed overflow buckets, and deterministic full-row sampling. Tokens are
// at most 16 bytes. Packed streams are little-endian and require the documented
// input and output padding. The current decode kernels require a little-endian
// host.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
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
  /// Dictionary-size budget: at most 2^max_dict_bits tokens. Valid range 8..=16.
  /// A budget of 8 needs prune_absent_literals to be any use, since 256 mandatory
  /// single-byte tokens fill a 256-code space exactly and leave no room for a pair.
  uint8_t max_dict_bits = 12;
  /// Dynamic-threshold byte-sampling fraction, in (0, 1].
  double threshold_fraction = 0.15;
  /// Deterministic sampling seed.
  uint64_t seed = 42;
  /// Seed the dictionary with only the byte values that actually occur, instead
  /// of all 256.
  ///
  /// Every occurring byte still receives a literal token, so encoding remains
  /// total without an escape code even at the 8-bit budget.
  bool prune_absent_literals = false;

  static Config Dict12() { return Config{12, 0.15, 42}; }
  static Config Dict16() { return Config{16, 0.15, 42}; }
};

/// The token table a code stream indexes into: Arrow-binary layout (flat bytes +
/// u32 offsets). `bytes` is read-padded by kMaxTokenSize so the decoder's fixed
/// 16-byte over-read stays in bounds.
struct CompactDictionary {
  std::vector<uint8_t> bytes;     // read-padded
  std::vector<uint32_t> offsets;  // length num_tokens + 1

  /// Length of the longest token present, which is what the decoder's gather-copy
  /// sizes its fixed copy width from. Often well below kMaxTokenSize: TPC-H
  /// c_address tops out at 5 bytes, and copying 16 there moves 8x the bytes it
  /// needs to.
  ///
  /// Must not be smaller than the true maximum. The conservative default keeps a
  /// dictionary safe until RecomputeMaxTokenLen is called.
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

/// Decode-side dictionary with fixed-width slots and a parallel length array.
/// This is derived from CompactDictionary and is not part of the stored format.
struct StridedDictionary {
  /// One slot per token. Also the decoder's fixed over-read width, and a power of
  /// two <= 64, so a 64-byte-aligned base puts every slot inside a single line.
  static constexpr size_t kStride = kMaxTokenSize;

  std::vector<uint8_t> storage;  ///< backing bytes; `slots` is aligned into this
  uint8_t* slots = nullptr;      ///< kStride bytes per token, 64-byte aligned
  std::vector<uint8_t> lens;     ///< parallel token lengths, one byte each
  /// Same meaning and same conservative default as CompactDictionary's: the
  /// portable kernel's fixed copy width comes from it, so it must never
  /// understate the true maximum.
  size_t max_token_len = kMaxTokenSize;

  size_t num_tokens() const { return lens.size(); }

  /// Populate from a stored dictionary. O(tokens), once per column.
  void Build(const CompactDictionary& dict);
};

/// A compressed string column. `codes` is the row-concatenated code stream;
/// row k is codes[row_offsets[k] .. row_offsets[k+1]].
struct Column {
  CompactDictionary dict;
  std::vector<uint16_t> codes;
  std::vector<uint32_t> row_offsets;  // length num_rows + 1

  size_t num_rows() const { return row_offsets.empty() ? 0 : row_offsets.size() - 1; }
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

/// Tokenize every row against a token set trained elsewhere, skipping OnPair's
/// own training. `token_bytes`/`token_offsets` are a raw token list in the same
/// layout as CompactDictionary but without the read padding; the returned
/// column's dictionary is its canonical (sorted, padded) form, so token ids are
/// reassigned and the caller's numbering is not preserved.
///
/// This exists so an alternative dictionary trainer can be measured against
/// OnPair's with the parsing pass and the decode pass held literally identical.
/// The token set must contain all 256 single bytes, which is what lets both
/// tokenize without an escape mechanism.
Column CompressWithTokens(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                          size_t num_rows, const std::vector<uint8_t>& token_bytes,
                          const std::vector<uint32_t>& token_offsets);

/// Exact decoded byte length of the whole column (sum of token lengths).
size_t DecodedLen(const Column& col);

/// Decode the whole column into `out`, returning bytes written.
/// `out_capacity` must include kDecodePadding beyond the decoded bytes.
size_t DecompressInto(const Column& col, uint8_t* out, size_t out_capacity);

// --- Bit-packed code stream (what a real stored format uses) ----------------
// The in-memory Column holds u16 codes; on storage the code stream is packed at
// the true code width (ceil(log2 num_tokens)). These pack/unpack it so decode
// pays the real unpacking cost, keeping ratio and decode mutually consistent.

/// Read `nbits` (<=25) at bit offset `bitpos`, little-endian / LSB-first.
/// `p` must have 4 readable bytes at the containing word.
inline uint32_t GetBits(const uint8_t* p, size_t size, size_t bitpos, size_t nbits) {
  const size_t byte_offset = bitpos >> 3;
  if (p == nullptr || nbits == 0 || nbits > 25 || byte_offset > size ||
      size - byte_offset < sizeof(uint32_t)) {
    throw std::invalid_argument("Invalid OnPair bit range");
  }
  uint32_t w;
  std::memcpy(&w, p + byte_offset, sizeof(w));
  return (w >> (bitpos & 7)) & (nbits >= 32 ? 0xFFFFFFFFu : ((1u << nbits) - 1));
}

/// Pack `n` values (each < 2^bits, bits in 1..=25) LSB-first. The result has 4
/// trailing pad bytes (so a 4-byte window at the last value is in bounds); the
/// logical size is (n*bits+7)/8.
std::vector<uint8_t> PackValues(const uint32_t* vals, size_t n, size_t bits);

/// Decode a bit-packed code stream: read `bits` per code and gather-copy the
/// token. `packed_size` must include the final four-byte read window, and
/// `out_capacity` must include kDecodePadding beyond the decoded bytes.
///
/// Builds a StridedDictionary internally and decodes through it, except when the
/// stream is short enough that the O(tokens) build outweighs what it saves, in
/// which case the blob-and-offsets kernel runs directly. Decode a series of pages
/// against one dictionary through the overload below instead, so the build is paid
/// once rather than per page.
size_t DecompressPacked(const CompactDictionary& dict, const uint8_t* packed,
                        size_t packed_size, size_t ncodes, size_t bits, uint8_t* out,
                        size_t out_capacity);

/// Same, against a view built once by the caller. Output is byte-identical to the
/// CompactDictionary overload.
size_t DecompressPacked(const StridedDictionary& dict, const uint8_t* packed,
                        size_t packed_size, size_t ncodes, size_t bits, uint8_t* out,
                        size_t out_capacity);

// Dictionary-budget selection trades stored size against estimated decode cost.

/// One trained dictionary-budget candidate.
///
/// `stored_bytes` is the caller's to fill: how a column is framed -- length side
/// array, dictionary offsets, page headers -- belongs to the format, not here, and
/// a selector that guessed at it could rank candidates by the wrong quantity.
struct BudgetCandidate {
  uint8_t budget = 0;      ///< Config::max_dict_bits used for this candidate
  uint8_t code_width = 0;  ///< bits per stored code, ceil(log2(num_tokens))
  uint32_t num_tokens = 0;
  uint64_t num_codes = 0;
  uint64_t stored_bytes = 0;
};

/// Relative decode cost of a candidate, in arbitrary units, from quantities
/// training already produced.
///
/// Only comparisons between candidates are meaningful; the constants are fitted
/// to the microarchitecture documented in the implementation.
double DecodeCostEstimate(uint64_t num_codes, uint32_t num_tokens);

/// How the selector trades decode speed for stored bytes.
struct SelectionPolicy {
  /// Admit a candidate only if its predicted decode cost is within this fraction
  /// of the best any candidate achieves; among those admitted, take the smallest.
  ///
  /// Positive infinity selects solely by stored size.
  double max_decode_regression = 0.30;
};

/// Index of the chosen candidate, or `n` if there are none.
///
/// Always returns a Pareto-optimal candidate: it is the smallest of the admitted
/// set, so nothing admitted is smaller, and ties on size break toward lower decode
/// cost.
size_t SelectBudget(const BudgetCandidate* candidates, size_t n,
                    const SelectionPolicy& policy);

/// Bits per stored code for a dictionary of `num_tokens` tokens: ceil(log2), at
/// least 1.
///
/// Not the budget. The budget caps the dictionary; this is what actually gets
/// written, and the two differ on every column whose training saturates below its
/// cap -- an enum trained at budget 16 still writes 6-bit codes.
uint8_t CodeWidth(size_t num_tokens);

/// Stored bytes of one candidate, counting only what varies with the budget: the
/// dictionary blob, its offset array, and the bit-packed code stream.
///
/// This is deliberately not a page size. A real frame also carries a row-length
/// side array and headers, and those are the same bytes for every candidate. A term
/// equal across all candidates cannot change which one is smallest, so omitting it
/// costs the ranking nothing and keeps this function from pretending to know a
/// format it does not. Anything that reports a compression ratio, or that frames
/// candidates differently should supply its own through
/// BudgetSearchOptions::stored_bytes.
uint64_t VaryingStoredBytes(const CompactDictionary& dict, uint64_t num_codes);
inline uint64_t VaryingStoredBytes(const Column& col) {
  return VaryingStoredBytes(col.dict, col.codes.size());
}

/// Which dictionary budgets to train and how to choose between them.
struct BudgetSearchOptions {
  /// Inclusive budget range, clamped to the legal 8..16.
  uint8_t min_budget = 8;
  uint8_t max_budget = 16;

  /// Everything except the budget. `base.max_dict_bits` is set for each candidate.
  ///
  /// Pruning is enabled because 256 mandatory literals fill an 8-bit budget and
  /// leave no code for a pair.
  Config base = Config{/*max_dict_bits=*/12, /*threshold_fraction=*/0.15, /*seed=*/42,
                       /*prune_absent_literals=*/true};

  SelectionPolicy policy;

  /// Stored size of a candidate, if the caller frames columns differently from
  /// VaryingStoredBytes. Receives the trained dictionary and the number of codes
  /// tokenizing produced; `ctx` is passed through untouched.
  uint64_t (*stored_bytes)(const CompactDictionary& dict, uint64_t num_codes,
                           void* ctx) = nullptr;
  void* stored_bytes_ctx = nullptr;
};

/// The candidates and the selector's decision, for logging and tests.
///
/// Worth logging per column: a selector regression should show up as choices
/// drifting, which this makes visible, rather than as stored bytes quietly moving,
/// which it does not.
struct SelectionReport {
  /// One entry per budget in [min_budget, max_budget], in budget order.
  std::vector<BudgetCandidate> candidates;
  size_t chosen = 0;       ///< index into `candidates`
  size_t bytes_only = 0;   ///< index a size-only selector would have taken
  double chosen_cost = 0;  ///< predicted decode cost of the chosen candidate
  double best_cost = 0;    ///< cheapest predicted cost among the candidates
  /// Dictionaries actually trained. Fewer than the candidate count when results coincide,
  /// which happens whenever the trainer saturates below its budget.
  size_t distinct_dictionaries = 0;
  double encode_s = 0;  ///< whole call, both passes
};

/// Train each requested budget and return the candidate selected by `opts.policy`.
///
/// The first pass records each candidate's code count. The selected dictionary is
/// then encoded again to avoid retaining every candidate's code stream.
Column CompressAuto(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                    size_t num_rows, const BudgetSearchOptions& opts,
                    SelectionReport* report = nullptr);

}  // namespace parquet::onpair
