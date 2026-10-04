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
  /// Full residency is what lets the tokenizer run without an escape mechanism,
  /// but it is stronger than needed: a byte that never occurs is never asked for.
  /// Dropping the absent ones spends the freed codes on pairs, which is the only
  /// thing that shortens the code stream. The effect grows as the budget narrows
  /// -- at 9 bits it turns 256 pair slots into up to 503, and at 8 bits it is the
  /// difference between a usable rung and no rung at all.
  ///
  /// This needs no escape mechanism to stay total. A byte alphabet holds at most
  /// 256 values and the narrowest budget has exactly 256 codes, so the bytes that
  /// occur always fit -- including the worst case of a column using all 256, which
  /// gets 256 literals, no pair slots and no compression, but still encodes. Every
  /// code therefore means exactly one token at every budget, which is what keeps
  /// the stream a fixed stride and a row seekable without its neighbours.
  bool prune_absent_literals = false;

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

/// A decode-side view of the dictionary: one cache line per token instead of two.
///
/// CompactDictionary makes the decoder read two independent random locations per
/// token -- a u32 offsets pair from one array, then the payload from a
/// variable-stride blob. That gather, not the store traffic, is what the decode
/// loop waits on. Giving every token a fixed 16-byte slot and its length a byte in
/// a dense side array collapses the pair into one line, which is the layout FSST's
/// decoder has always used (fixed-stride `symbol[]` plus `len[]`).
///
/// This is deliberately NOT the stored form. A 16-byte slot plus a length byte is
/// ~17 bytes per token against ~12 for blob-plus-offsets, so serializing it would
/// add hundreds of KiB on a 65k-token dictionary and move every compression ratio.
/// It is built once per column from the stored form and thrown away, so it costs
/// footprint only for the duration of a decode and nothing at all on disk.
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
Column CompressWithTokens(const uint8_t* bytes, const uint32_t* offsets, size_t num_rows,
                          const std::vector<uint8_t>& token_bytes,
                          const std::vector<uint32_t>& token_offsets);

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
///
/// Builds a StridedDictionary internally and decodes through it, except when the
/// stream is short enough that the O(tokens) build outweighs what it saves, in
/// which case the blob-and-offsets kernel runs directly. Decode a series of pages
/// against one dictionary through the overload below instead, so the build is paid
/// once rather than per page.
size_t DecompressPacked(const CompactDictionary& dict, const uint8_t* packed,
                        size_t ncodes, size_t bits, uint8_t* out);

/// Same, against a view built once by the caller. Output is byte-identical to the
/// CompactDictionary overload.
size_t DecompressPacked(const StridedDictionary& dict, const uint8_t* packed,
                        size_t ncodes, size_t bits, uint8_t* out);

// --- Choosing a dictionary budget -------------------------------------------
//
// Training every rung and keeping the smallest output is the obvious selector and
// the wrong one, because size and decode speed do not sit at the same end of the
// ladder. The decoder's unit of work is a token, not a byte: it reads one code,
// gathers one fixed-width slot, and advances the output by that token's length.
// Narrowing the budget shortens the average token, so the same column needs more
// codes -- and stored size still falls, because the dictionary shrinks faster than
// the code stream grows. So the narrow rungs buy ratio with dictionary
// amortization and pay for it in code count, which is the one quantity decode is
// linear in.
//
// Measured across 30 corpora and all nine rungs, the two axes are not even
// comparably sized: decode varies 1.94x across the rungs of a median column while
// stored size varies 1.36x. Picking on bytes alone therefore optimizes the axis
// with less headroom, and on the columns where the ladder's ends diverge it gives
// up a lot: sha256_hex, c_name and s_name each decode 1.40-1.53x slower at their
// smallest rung than at their fastest. It is also not a conservative default in the
// direction you would guess: of the 24 corpora whose rungs differ by more than 5%
// at all, the fastest rung is budget 12 or wider on all 24 and never the narrow
// end. The remaining six are the low-cardinality enums, where the trainer saturates
// below every budget and all nine rungs are the same dictionary.

/// One trained rung, reduced to what the selector compares.
///
/// `stored_bytes` is the caller's to fill: how a column is framed -- length side
/// array, dictionary offsets, page headers -- belongs to the format, not here, and
/// a selector that guessed at it would rank rungs by the wrong quantity.
struct BudgetCandidate {
  uint8_t budget = 0;      ///< the Config::max_dict_bits this rung was trained at
  uint8_t code_width = 0;  ///< bits per stored code, ceil(log2(num_tokens))
  uint32_t num_tokens = 0;
  uint64_t num_codes = 0;
  uint64_t stored_bytes = 0;
};

/// Relative decode cost of a candidate, in arbitrary units, from quantities
/// training already produced.
///
/// The point of predicting rather than timing is that the encoder can afford to
/// train nine dictionaries but not to decode the whole column nine times. It turns
/// out not to be a compromise: against an oracle that timed every rung, selecting
/// on this estimate picked a rung with identical median decode and at worst 6.6%
/// off, and the same ratio on every corpus.
///
/// Only ratios between candidates are meaningful. The absolute scale and the
/// working-set knee are fitted to one machine (see the constants in the .cc), and
/// what carries across machines is that the cache term is *bounded*: per-code cost
/// rises about 1.42x from a 256-token dictionary to a saturated one, while code
/// counts across a ladder vary around 2x. A different cache hierarchy moves that
/// bound; it does not reorder the large code-count differences, which is where the
/// selector's decisions come from.
double DecodeCostEstimate(uint64_t num_codes, uint32_t num_tokens);

/// How the selector trades decode speed for stored bytes.
struct SelectionPolicy {
  /// Admit a candidate only if its predicted decode cost is within this fraction
  /// of the best any candidate achieves; among those admitted, take the smallest.
  ///
  /// A cap rather than a weighted sum, because a cap is auditable: "this column is
  /// never more than 30% off the fastest decode this codec can give it" is a
  /// sentence a reviewer can check against a measurement, and a relative exchange
  /// rate between bytes and nanoseconds is not.
  ///
  /// Measured over 30 corpora against the published bytes-only baseline (budgets
  /// 9..16, all bytes resident, 4.276x median ratio at 7810 MiB/s), every figure
  /// timed in one process:
  ///
  ///   cap    median ratio   median decode   worst rung vs its own fastest
  ///   0.05      4.200x        9311 MiB/s        1.07x
  ///   0.30      4.381x        7979 MiB/s        1.29x
  ///   inf       4.391x        7323 MiB/s        1.52x
  ///
  /// The default is 0.30 because it beats the published baseline on both axes at
  /// once, so turning the selector on cannot be read as a ratio regression, and
  /// because no column of the 30 comes out worse than that baseline on both axes at
  /// either 0.30 or 0.05. It is also rarely binding: at 0.30 the cap moves the choice
  /// on 3 of the 30 columns, and they are the three where the ladder's ends diverge
  /// most. Drop to 0.05 where decode is worth more than ratio at the margin -- it
  /// buys 19% decode for 1.8% of ratio. Set it to infinity for pure ratio, which is
  /// what costs 6% of decode and puts six columns past 1.20x.
  double max_decode_regression = 0.30;
};

/// Index of the chosen candidate, or `n` if there are none.
///
/// Always returns a Pareto-optimal candidate: it is the smallest of the admitted
/// set, so nothing admitted is smaller, and ties on size break toward lower decode
/// cost.
size_t SelectBudget(const BudgetCandidate* candidates, size_t n, const SelectionPolicy& policy);

/// Bits per stored code for a dictionary of `num_tokens` tokens: ceil(log2), at
/// least 1.
///
/// Not the budget. The budget caps the dictionary; this is what actually gets
/// written, and the two differ on every column whose training saturates below its
/// cap -- an enum trained at budget 16 still writes 6-bit codes.
uint8_t CodeWidth(size_t num_tokens);

/// Stored bytes of one rung, counting only what varies with the budget: the
/// dictionary blob, its offset array, and the bit-packed code stream.
///
/// This is deliberately not a page size. A real frame also carries a row-length
/// side array and headers, and those are the same bytes at every rung -- a term
/// equal across all candidates cannot change which one is smallest, so omitting it
/// costs the ranking nothing and keeps this function from pretending to know a
/// format it does not. Anything that reports a compression ratio, or that frames
/// rungs differently from each other, should supply its own through
/// LadderOptions::stored_bytes.
uint64_t VaryingStoredBytes(const CompactDictionary& dict, uint64_t num_codes);
inline uint64_t VaryingStoredBytes(const Column& col) {
  return VaryingStoredBytes(col.dict, col.codes.size());
}

/// Which rungs to train, and how to choose between them.
struct LadderOptions {
  /// Inclusive budget range, clamped to the legal 8..16.
  uint8_t min_budget = 8;
  uint8_t max_budget = 16;

  /// Everything except the budget. `base.max_dict_bits` is ignored -- the ladder
  /// sets it per rung.
  ///
  /// Pruning is on here although Config defaults it off, because the bottom of the
  /// ladder does not exist without it: 256 mandatory literals fill an 8-bit budget
  /// exactly and leave no code for a pair, so that rung could not compress at all.
  Config base = Config{/*max_dict_bits=*/12, /*threshold_fraction=*/0.15, /*seed=*/42,
                       /*prune_absent_literals=*/true};

  SelectionPolicy policy;

  /// Stored size of a rung, if the caller frames columns differently from
  /// VaryingStoredBytes. Receives the trained dictionary and the number of codes
  /// tokenizing produced; `ctx` is passed through untouched.
  uint64_t (*stored_bytes)(const CompactDictionary& dict, uint64_t num_codes, void* ctx) = nullptr;
  void* stored_bytes_ctx = nullptr;
};

/// The whole ladder and what it decided, for logging and for tests.
///
/// Worth logging per column: a selector regression should show up as choices
/// drifting, which this makes visible, rather than as stored bytes quietly moving,
/// which it does not.
struct SelectionReport {
  /// One entry per budget in [min_budget, max_budget], in budget order.
  std::vector<BudgetCandidate> candidates;
  size_t chosen = 0;      ///< index into `candidates`
  size_t bytes_only = 0;  ///< index a size-only selector would have taken
  double chosen_cost = 0;  ///< predicted decode cost of the chosen rung
  double best_cost = 0;    ///< cheapest predicted cost in the ladder
  /// Dictionaries actually trained. Below the ladder size when rungs coincide,
  /// which happens whenever the trainer saturates below its budget.
  size_t distinct_dictionaries = 0;
  double encode_s = 0;  ///< whole call, both passes
};

/// Train every rung of the ladder, choose one by `opts.policy`, and return that
/// rung's compressed column.
///
/// Costs about ten single-budget Compress calls on a nine-rung ladder: one per rung
/// to learn its code count, plus one to re-encode the winner. Re-encoding rather
/// than keeping every rung's code stream is the reason -- holding nine of those on a
/// large column costs gigabytes, and which rung wins is not known until the ladder
/// is complete, since the decode cap moves as cheaper rungs appear. Encode is
/// explicitly the side we are willing to spend on.
Column CompressAuto(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                    size_t num_rows, const LadderOptions& opts,
                    SelectionReport* report = nullptr);

}  // namespace parquet::onpair
