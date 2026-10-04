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

#include "parquet/onpair/fsst16.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace parquet::fsst16 {
namespace {

// The first 256 codes are the literal bytes; learned symbols start here.
constexpr size_t kCodeBase = 256;
// Longest run of one row the sample generator takes in a single bite. The
// reference's constant.
constexpr size_t kSampleLine = 512;

// The reference's integer hash, used for sample selection and round skipping so
// that both pick the same rows it would.
inline uint64_t FsstHash(uint64_t w) {
  const uint64_t p = w * 2971215073ull;
  return p ^ (p >> 15);
}

// Byte-range hash for the symbol indexes below: eight bytes a step with a tail,
// then an avalanche.
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

struct Sym {
  uint8_t b[kMaxSymbolLen];
  uint8_t len;
};

// Concatenation, truncated at the length cap exactly as the reference truncates
// at eight bytes rather than rejecting the pair.
inline Sym Concat(const Sym& a, const Sym& b, size_t cap) {
  Sym s;
  size_t n = std::min<size_t>(static_cast<size_t>(a.len) + b.len, cap);
  std::memcpy(s.b, a.b, a.len);
  if (n > a.len) std::memcpy(s.b + a.len, b.b, n - a.len);
  s.len = static_cast<uint8_t>(n);
  return s;
}

// Longest-prefix index over the symbol table.
//
// The reference indexes with a two-byte lookup array plus a three-byte-prefix
// hash, both of which lean on a symbol fitting in one machine word. A 16-byte
// symbol does not fit, so this keeps one open-addressed set per symbol length
// and probes them longest-first, skipping lengths the table has none of. It only
// ever runs over the training sample - tens of KiB, five times - so a few probes
// per position costs far less than constraining what a symbol may be.
class SymbolIndex {
 public:
  explicit SymbolIndex(const std::vector<Sym>* syms) : syms_(syms), slots_(kSlots, 0) {}

  void Clear() {
    std::fill(slots_.begin(), slots_.end(), 0u);
    len_present_ = 0;
    max_len_ = 1;
  }

  /// False if a symbol with these exact bytes is already indexed.
  bool Insert(const Sym& s, uint32_t code) {
    size_t i = Probe(s.b, s.len);
    if (slots_[i] != 0) return false;
    slots_[i] = code + 1;
    len_present_ |= uint32_t{1} << s.len;
    if (s.len > max_len_) max_len_ = s.len;
    return true;
  }

  /// Code of the longest symbol that prefixes [p, p+n). Falls back to the
  /// literal code for the first byte, which is always resident.
  uint32_t Find(const uint8_t* p, size_t n) const {
    size_t hi = std::min<size_t>(max_len_, n);
    for (size_t len = hi; len >= 2; --len) {
      if (((len_present_ >> len) & 1u) == 0) continue;
      size_t i = Probe(p, len);
      if (slots_[i] != 0) return slots_[i] - 1;
    }
    return p[0];
  }

 private:
  // Twice the 65536-symbol ceiling, so the set never passes half load and the
  // probe chains stay short without any rehashing.
  static constexpr size_t kSlots = size_t{1} << 17;

  size_t Probe(const uint8_t* p, size_t len) const {
    size_t i = HashBytes(p, len) & (kSlots - 1);
    while (slots_[i] != 0) {
      const Sym& s = (*syms_)[slots_[i] - 1];
      if (s.len == len && std::memcmp(s.b, p, len) == 0) break;
      i = (i + 1) & (kSlots - 1);
    }
    return i;
  }

  const std::vector<Sym>* syms_;
  std::vector<uint32_t> slots_;  // code + 1, or 0 for empty
  uint32_t len_present_ = 0;     // bit L set when some symbol has length L
  uint32_t max_len_ = 1;
};

// Adjacent-pair frequencies, sparse (V1).
//
// Keyed by the two codes packed into one word. Occupied slots are listed as they
// are first written so that clearing between rounds and walking the pairs for
// candidate generation both cost the number of distinct pairs rather than the
// table size.
class PairCounts {
 public:
  void Reset(size_t expected_pairs) {
    size_t want = 1024;
    const size_t ceiling = size_t{1} << 22;  // grows past this only if a column needs it
    while (want < 2 * expected_pairs && want < ceiling) want <<= 1;
    if (want > slots_) {
      Allocate(want);
    } else {
      Clear();
    }
  }

  void Clear() {
    for (size_t i : used_) {
      keys_[i] = 0;
      vals_[i] = 0;
    }
    used_.clear();
  }

  void Inc(uint32_t c1, uint32_t c2) {
    if (2 * used_.size() >= slots_) Grow();
    const uint64_t key = (static_cast<uint64_t>(c1) << 16 | c2) + 1;  // 0 marks empty
    size_t i = Slot(key);
    if (keys_[i] == 0) {
      keys_[i] = key;
      used_.push_back(i);
    }
    ++vals_[i];
  }

  const std::vector<size_t>& used() const { return used_; }
  uint32_t left(size_t slot) const { return static_cast<uint32_t>((keys_[slot] - 1) >> 16); }
  uint32_t right(size_t slot) const { return static_cast<uint32_t>((keys_[slot] - 1) & 0xFFFF); }
  uint32_t count(size_t slot) const { return vals_[slot]; }

 private:
  void Allocate(size_t want) {
    slots_ = want;
    keys_.assign(slots_, 0);
    vals_.assign(slots_, 0);
    used_.clear();
  }

  size_t Slot(uint64_t key) const {
    size_t i = FsstHash(key) & (slots_ - 1);
    while (keys_[i] != 0 && keys_[i] != key) i = (i + 1) & (slots_ - 1);
    return i;
  }

  // Kept at or below half load so probe chains stay short.
  void Grow() {
    std::vector<uint64_t> old_keys = std::move(keys_);
    std::vector<uint32_t> old_vals = std::move(vals_);
    std::vector<size_t> old_used = std::move(used_);
    Allocate(slots_ * 2);
    for (size_t o : old_used) {
      const size_t i = Slot(old_keys[o]);
      keys_[i] = old_keys[o];
      vals_[i] = old_vals[o];
      used_.push_back(i);
    }
  }

  size_t slots_ = 0;
  std::vector<uint64_t> keys_;  // packed pair + 1
  std::vector<uint32_t> vals_;
  std::vector<size_t> used_;
};

struct Cand {
  Sym sym;
  uint64_t score;
};

// Candidate set for one round, deduplicating by symbol bytes and summing the
// scores of duplicates, as the reference's candidate set does.
class CandSet {
 public:
  void Reset(size_t expected) {
    size_t want = 16;
    while (want < 2 * expected + 16) want <<= 1;
    slots_.assign(want, kEmpty);
    cands_.clear();
  }

  void AddOrInc(const Sym& s, uint64_t score) {
    const size_t mask = slots_.size() - 1;
    size_t i = HashBytes(s.b, s.len) & mask;
    while (slots_[i] != kEmpty) {
      Cand& c = cands_[slots_[i]];
      if (c.sym.len == s.len && std::memcmp(c.sym.b, s.b, s.len) == 0) {
        c.score += score;
        return;
      }
      i = (i + 1) & mask;
    }
    slots_[i] = static_cast<uint32_t>(cands_.size());
    cands_.push_back({s, score});
  }

  std::vector<Cand>& cands() { return cands_; }

 private:
  static constexpr uint32_t kEmpty = 0xFFFFFFFFu;
  std::vector<uint32_t> slots_;
  std::vector<Cand> cands_;
};

class Trainer {
 public:
  explicit Trainer(const Config& cfg)
      : cfg_(cfg),
        cap_(std::min<size_t>(std::max<size_t>(cfg.max_symbol_len, 2), kMaxSymbolLen)),
        index_(&syms_) {
    count1_.assign(cfg_.max_symbols, 0);
  }

  Tokens Run(const uint8_t* data, const uint32_t* offsets, size_t n) {
    BuildSample(data, offsets, n);
    size_t sample_bytes = 0;
    for (const auto& l : lines_) sample_bytes += l.second;
    pairs_.Reset(2 * sample_bytes + 16);

    ResetTable();

    int64_t best_gain = INT64_MIN;
    std::vector<Sym> best_syms = syms_;
    std::vector<uint32_t> best_count1 = count1_;

    // Five rounds at sample fractions 8, 38, 68, 98, 128; the last measures the
    // table it inherits without proposing a new one.
    for (size_t frac = 8;; frac += 30) {
      std::fill(count1_.begin(), count1_.end(), 0u);
      pairs_.Clear();
      const int64_t gain = CompressCount(frac);
      if (gain >= best_gain) {
        best_gain = gain;
        best_syms = syms_;
        best_count1 = count1_;
      }
      if (frac >= 128) break;
      MakeTable(frac);
    }

    // Rebuild the winning table from its own single-symbol counts, dropping the
    // symbols the winning round never actually used.
    syms_ = std::move(best_syms);
    count1_ = std::move(best_count1);
    pairs_.Clear();
    MakeTable(128);

    return Emit();
  }

 private:
  // Sample selection, following the reference: take the whole column when it is
  // under the target, otherwise fill the target with randomly chosen runs of
  // randomly chosen rows.
  void BuildSample(const uint8_t* data, const uint32_t* offsets, size_t n) {
    const size_t total = n == 0 ? 0 : offsets[n];
    if (total <= cfg_.sample_target) {
      for (size_t i = 0; i < n; ++i) {
        if (offsets[i + 1] > offsets[i]) {
          lines_.emplace_back(data + offsets[i], offsets[i + 1] - offsets[i]);
        }
      }
      return;
    }

    std::vector<std::pair<size_t, size_t>> spans;  // (offset in buf, length)
    buf_.reserve(cfg_.sample_target + kSampleLine);
    uint64_t rnd = FsstHash(cfg_.seed);
    while (buf_.size() < cfg_.sample_target) {
      rnd = FsstHash(rnd);
      size_t row = rnd % n;
      while (offsets[row + 1] == offsets[row]) {
        if (++row == n) row = 0;
      }
      const size_t len = offsets[row + 1] - offsets[row];
      const size_t chunks = 1 + (len - 1) / kSampleLine;
      rnd = FsstHash(rnd);
      const size_t chunk = kSampleLine * (rnd % chunks);
      const size_t take = std::min(len - chunk, kSampleLine);
      const uint8_t* src = data + offsets[row] + chunk;
      spans.emplace_back(buf_.size(), take);
      buf_.insert(buf_.end(), src, src + take);
    }
    // Resolved after the buffer stops growing, since inserting reallocates it.
    for (const auto& sp : spans) lines_.emplace_back(buf_.data() + sp.first, sp.second);
  }

  void ResetTable() {
    syms_.resize(kCodeBase);
    for (size_t i = 0; i < kCodeBase; ++i) {
      syms_[i].len = 1;
      syms_[i].b[0] = static_cast<uint8_t>(i);
    }
    index_.Clear();
  }

  void AddSym(const Sym& s) {
    if (s.len < 2) return;  // V4: the literals are already resident
    const uint32_t code = static_cast<uint32_t>(syms_.size());
    syms_.push_back(s);
    if (!index_.Insert(s, code)) syms_.pop_back();
  }

  // Round skipping: the reference's per-row draw in 1..128.
  static size_t Rnd128(size_t i, size_t frac) {
    return 1 + (FsstHash((i + 1) * frac) & 127);
  }

  // Compress the sample with the current table, counting single symbols and
  // adjacent pairs, and return the gain: bytes saved against storing the sample
  // raw, given that every code costs two bytes.
  int64_t CompressCount(size_t frac) {
    int64_t gain = 0;
    for (size_t i = 0; i < lines_.size(); ++i) {
      if (frac < 128 && Rnd128(i, frac) > frac) continue;
      const uint8_t* cur = lines_[i].first;
      const uint8_t* end = cur + lines_[i].second;
      if (cur >= end) continue;
      const uint8_t* start = cur;

      uint32_t code1 = index_.Find(cur, end - cur);
      cur += syms_[code1].len;
      gain += static_cast<int64_t>(syms_[code1].len) - 2;

      for (;;) {
        // Not extending this symbol is one option, so count it alone.
        ++count1_[code1];
        // Taking just its first byte is the other, unless they are the same.
        if (syms_[code1].len != 1) ++count1_[*start];

        if (cur == end) break;

        start = cur;
        const uint32_t code2 = index_.Find(cur, end - cur);
        cur += syms_[code2].len;
        gain += static_cast<int64_t>(cur - start) - 2;

        if (frac < 128) {  // the last round proposes nothing, so counts no pairs
          pairs_.Inc(code1, code2);
          if (cur - start > 1) pairs_.Inc(code1, *start);
        }
        code1 = code2;
      }
    }
    return gain;
  }

  // Clear the table and refill it from the highest-scoring candidates.
  void MakeTable(size_t frac) {
    const uint64_t min_count = (5 * frac) / 128;
    cands_.Reset(syms_.size() + pairs_.used().size());

    // Every counted symbol is a candidate to keep.
    for (size_t p1 = 0; p1 < syms_.size(); ++p1) {
      const uint32_t c1 = count1_[p1];
      if (c1 == 0) continue;
      const Sym& s1 = syms_[p1];
      // V4: promoting single bytes is the reference's way of holding its escape
      // rate down. Kept so scores match, though a length-1 candidate is never
      // admitted here.
      const uint64_t cnt = (s1.len == 1 ? 8ull : 1ull) * c1;
      if (cnt < min_count) continue;
      cands_.AddOrInc(s1, cnt * s1.len);
    }

    // Every counted pair is a candidate to merge (V2). The last round proposes
    // nothing, matching the reference's refusal to grow symbols there.
    if (frac < 128) {
      for (size_t slot : pairs_.used()) {
        const uint32_t cnt = pairs_.count(slot);
        if (cnt < min_count) continue;
        const uint32_t p1 = pairs_.left(slot);
        if (count1_[p1] == 0) continue;
        const Sym& s1 = syms_[p1];
        if (s1.len >= cap_) continue;  // cannot be extended
        const Sym s3 = Concat(s1, syms_[pairs_.right(slot)], cap_);
        cands_.AddOrInc(s3, static_cast<uint64_t>(cnt) * s3.len);
      }
    }

    std::vector<Cand>& cs = cands_.cands();
    // Highest score first; V5 for the tie-break.
    std::sort(cs.begin(), cs.end(), [](const Cand& a, const Cand& b) {
      if (a.score != b.score) return a.score > b.score;
      if (a.sym.len != b.sym.len) return a.sym.len < b.sym.len;
      return std::memcmp(a.sym.b, b.sym.b, a.sym.len) < 0;
    });

    ResetTable();
    for (const Cand& c : cs) {
      if (syms_.size() >= cfg_.max_symbols) break;
      AddSym(c.sym);
    }
  }

  Tokens Emit() const {
    Tokens t;
    t.bytes.reserve(kCodeBase + (syms_.size() - kCodeBase) * cap_);
    t.offsets.reserve(syms_.size() + 1);
    t.offsets.push_back(0);
    for (const Sym& s : syms_) {
      t.bytes.insert(t.bytes.end(), s.b, s.b + s.len);
      t.offsets.push_back(static_cast<uint32_t>(t.bytes.size()));
    }
    return t;
  }

  const Config& cfg_;
  const size_t cap_;  // effective max symbol length

  std::vector<uint8_t> buf_;                            // sample backing store
  std::vector<std::pair<const uint8_t*, size_t>> lines_;  // sample rows

  std::vector<Sym> syms_;  // 0..255 literals, then learned symbols
  SymbolIndex index_;
  std::vector<uint32_t> count1_;
  PairCounts pairs_;
  CandSet cands_;
};

}  // namespace

Tokens Train(const uint8_t* bytes, const uint32_t* offsets, size_t num_rows,
             const Config& cfg) {
  Trainer t(cfg);
  return t.Run(bytes, offsets, num_rows);
}

}  // namespace parquet::fsst16
