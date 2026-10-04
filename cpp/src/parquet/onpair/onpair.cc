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

#include "parquet/onpair/onpair.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <numeric>

namespace parquet::onpair {
namespace {

constexpr size_t kBucketPrefixLen = 8;
constexpr size_t kPromoteThreshold = 128;

// Little-endian packing helpers

/// Pack the low min(len, data_len, 8) bytes of `data` into a little-endian u64;
/// higher bytes read as zero.
inline uint64_t LoadLeU64(const uint8_t* data, size_t data_len, size_t len) {
  size_t n = (len >= kBucketPrefixLen && data_len >= kBucketPrefixLen)
                 ? kBucketPrefixLen
                 : std::min(len, data_len);
  uint64_t v = 0;
  std::memcpy(&v, data, n);  // little-endian host
  return v;
}

/// Mask of the low len*8 bits in a u64.
inline uint64_t MaskU64(size_t len) {
  return len >= 8 ? ~uint64_t{0} : ((uint64_t{1} << (len * 8)) - 1);
}

/// Count of matching low bytes between two packed suffixes.
inline size_t MatchingLowBytes(uint64_t x) {
  return x == 0 ? 8 : (static_cast<size_t>(__builtin_ctzll(x)) >> 3);
}

// Flat u64 -> u32 hash table
//
// The tokenizer probes its lookup tables several times per token, so probe cost
// dominates encode time. std::unordered_map is the wrong shape for that: the
// bucket array load and the node load are dependent, so every miss costs two
// serialized cache misses, and at tens of thousands of tokens neither fits in
// cache. Open addressing with the key and value in one 16-byte slot makes the
// common case a single load. Insert order still decides which of two equal keys
// wins, so swapping this in cannot change the tokenization.

constexpr uint32_t kFlatEmpty = ~uint32_t{0};

inline uint64_t MixU64(uint64_t x) {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb9fe1a85ec53ULL;
  x ^= x >> 33;
  return x;
}

class FlatU64Map {
 public:
  FlatU64Map() : slots_(kMinSlots), mask_(kMinSlots - 1) {}

  bool empty() const { return size_ == 0; }

  /// Value for `key`, or kFlatEmpty when absent.
  uint32_t Find(uint64_t key) const {
    size_t i = MixU64(key) & mask_;
    for (;;) {
      const Slot& s = slots_[i];
      if (s.val == kFlatEmpty) return kFlatEmpty;
      if (s.key == key) return s.val;
      i = (i + 1) & mask_;
    }
  }

  /// Insert `key`, or overwrite the value already stored under it.
  void Put(uint64_t key, uint32_t val) {
    size_t i = MixU64(key) & mask_;
    for (;;) {
      Slot& s = slots_[i];
      if (s.val == kFlatEmpty) {
        s.key = key;
        s.val = val;
        ++size_;
        // Linear probing degrades sharply past half full; grow well before then.
        if (size_ * 2 > slots_.size()) Grow();
        return;
      }
      if (s.key == key) {
        s.val = val;
        return;
      }
      i = (i + 1) & mask_;
    }
  }

 private:
  struct Slot {
    uint64_t key = 0;
    uint32_t val = kFlatEmpty;
  };
  static constexpr size_t kMinSlots = 64;

  void Grow() {
    std::vector<Slot> old(slots_.size() * 2);
    old.swap(slots_);
    mask_ = slots_.size() - 1;
    for (const Slot& s : old) {
      if (s.val == kFlatEmpty) continue;
      size_t i = MixU64(s.key) & mask_;
      while (slots_[i].val != kFlatEmpty) i = (i + 1) & mask_;
      slots_[i] = s;
    }
  }

  std::vector<Slot> slots_;
  size_t mask_;
  size_t size_ = 0;
};

// Flat pair-frequency counter for the training loop
//
// The trainer touches this once per token boundary, so it sits on the same hot
// path as the matcher and wants the same treatment. Deleting a promoted pair is a
// reset to zero rather than a real erase: a caller cannot tell an absent key from
// a zero count, so the two are equivalent, and it keeps linear probing free of
// tombstones (promotions are also rare - at most one per dictionary entry).

inline uint32_t MixU32(uint32_t x) {
  x ^= x >> 16;
  x *= 0x7feb352dU;
  x ^= x >> 15;
  x *= 0x846ca68bU;
  x ^= x >> 16;
  return x;
}

class FlatFreqMap {
 public:
  FlatFreqMap() : slots_(kMinSlots), mask_(kMinSlots - 1) {}

  /// Saturating increment of `key`'s count (absent == 0), returning the new value.
  uint8_t Bump(uint32_t key) {
    size_t i = MixU32(key) & mask_;
    for (;;) {
      Slot& s = slots_[i];
      if (s.count == kEmptyCount) {
        s.key = key;
        s.count = 1;
        ++size_;
        if (size_ * 2 > slots_.size()) Grow();
        return 1;
      }
      if (s.key == key) {
        if (s.count < 255) ++s.count;
        return static_cast<uint8_t>(s.count);
      }
      i = (i + 1) & mask_;
    }
  }

  /// Forget `key`'s count. Precondition: Bump(key) was called at least once.
  void Reset(uint32_t key) {
    size_t i = MixU32(key) & mask_;
    for (;;) {
      Slot& s = slots_[i];
      if (s.count == kEmptyCount) return;
      if (s.key == key) {
        s.count = 0;
        return;
      }
      i = (i + 1) & mask_;
    }
  }

 private:
  struct Slot {
    uint32_t key = 0;
    uint16_t count = kEmptyCount;
  };
  static constexpr uint16_t kEmptyCount = 0xFFFF;
  static constexpr size_t kMinSlots = 1024;

  void Grow() {
    std::vector<Slot> old(slots_.size() * 2);
    old.swap(slots_);
    mask_ = slots_.size() - 1;
    for (const Slot& s : old) {
      if (s.count == kEmptyCount) continue;
      size_t i = MixU32(s.key) & mask_;
      while (slots_[i].count != kEmptyCount) i = (i + 1) & mask_;
      slots_[i] = s;
    }
  }

  std::vector<Slot> slots_;
  size_t mask_;
  size_t size_ = 0;
};

// Longest-prefix matcher
// Two-tier index per the paper (sec 3.4.1): a hash map for tokens <=8 bytes, and
// 8-byte-prefix buckets (suffixes sorted descending) for 9..16-byte tokens.
// DEVIATION FROM PAPER (D3): the paper's OnPair16 caps each long bucket at 128
// suffixes (sec 3.4.4, dropping extras); this port instead promotes an over-full
// bucket to a trie (PROMOTE_THRESHOLD), keeping all suffixes. Also, the paper's
// static parsing phase (sec 3.4.3) finalizes long-pattern lookup with a minimal
// perfect hash; this port keeps an ordinary hash table (the paper notes the
// perfect-hash path is Rust-only). Encode-time behavior only.

// Prefix filter
//
// One byte per possible two-byte prefix of the data: bit (len-1) is set when some
// token of exactly `len` bytes (2..kBucketPrefixLen) starts with those two bytes,
// and bit 0 when some token longer than kBucketPrefixLen does. Length 1 is left
// out, since a single-byte match always exists and is probed anyway. Packing the
// eight live bits into a byte rather than a u16 halves the table to 64 KB.
//
// Folding the prefix into fewer slots would stay correct - the filter only ever
// SKIPS work, so a collision costs a wasted probe and can never change the answer
// - but measurably loses: at 16 KB and below the index arithmetic costs more than
// the smaller footprint saves, because the live prefix set is already small.

constexpr size_t kPrefixSlots = size_t{1} << 16;
constexpr uint8_t kMaskLongBit = 1;

struct LongEntry {
  uint64_t suffix;
  uint8_t slen;
  Token token;
};

struct TrieNode {
  int token = -1;  // -1 == none
  std::vector<std::pair<uint8_t, uint32_t>> children;
};

struct Bucket {
  std::vector<LongEntry> entries;
  int32_t trie_root = -1;  // >=0 once promoted
};

class LongestPrefixMatcher {
 public:
  /// Empty matcher pre-loaded with the 256 single-byte tokens (ids 0..255).
  static LongestPrefixMatcher New() {
    LongestPrefixMatcher m;
    for (uint16_t i = 0; i <= 255; ++i) {
      m.short_by_len_[1].Put(static_cast<uint64_t>(static_cast<uint8_t>(i)), i);
    }
    m.next_id_ = 256;
    return m;
  }

  /// Build from a complete dictionary: token at index i receives id i.
  static LongestPrefixMatcher FromDictionary(const CompactDictionary& dict) {
    LongestPrefixMatcher m;
    size_t n = dict.num_tokens();
    for (size_t i = 0; i < n; ++i) {
      m.InsertInternal(dict.token_ptr(static_cast<Token>(i)), dict.token_len(static_cast<Token>(i)),
                       static_cast<Token>(i));
    }
    m.next_id_ = static_cast<uint32_t>(n);
    return m;
  }

  /// Insert `data` (len bytes) and assign it the next available token id.
  Token Insert(const uint8_t* data, size_t len) {
    Token id = static_cast<Token>(next_id_++);
    InsertInternal(data, len, id);
    return id;
  }

  size_t size() const { return next_id_; }

  /// Longest token whose bytes are a prefix of `data`, with its length.
  std::pair<Token, size_t> FindLongestMatch(const uint8_t* data, size_t data_len) const {
    size_t max_len = std::min(data_len, kMaxTokenSize);
    uint64_t low64 = LoadLeU64(data, data_len, std::min(max_len, kBucketPrefixLen));

    // Every token of 2 bytes or more shares its first two bytes with the data, so
    // one array read rules out the lengths at which no token can possibly match.
    uint32_t present = max_len >= 2 ? prefix_mask_[low64 & 0xFFFF] : 0;

    if (max_len > kBucketPrefixLen && (present & kMaskLongBit) != 0) {
      uint32_t bucket = long_map_.Find(low64);
      if (bucket != kFlatEmpty) {
        const uint8_t* suf = data + kBucketPrefixLen;
        size_t suf_len = max_len - kBucketPrefixLen;
        const Bucket& b = buckets_[bucket];
        std::pair<Token, size_t> hit{0, 0};
        bool found;
        if (b.trie_root < 0) {
          found = SearchLinear(b.entries, LoadLeU64(suf, suf_len, suf_len), suf_len, &hit);
        } else {
          found = SearchTrie(static_cast<uint32_t>(b.trie_root), suf, suf_len, &hit);
        }
        if (found) {
          return {hit.first, kBucketPrefixLen + hit.second};
        }
      }
    }

    // Descend only through the occupied lengths. Bit (len-1) holds length `len`,
    // so clearing bit 0 also drops the long-token bit.
    size_t short_max = std::min(max_len, kBucketPrefixLen);
    uint32_t cand = present & (((uint32_t{1} << short_max) - 1) & ~uint32_t{1});
    while (cand != 0) {
      size_t len = 32 - static_cast<size_t>(__builtin_clz(cand));
      cand &= ~(uint32_t{1} << (len - 1));
      uint32_t tok = short_by_len_[len].Find(low64 & MaskU64(len));
      if (tok != kFlatEmpty) {
        return {static_cast<Token>(tok), len};
      }
    }
    uint32_t one = short_by_len_[1].Find(low64 & 0xFF);
    if (one != kFlatEmpty) return {static_cast<Token>(one), 1};
    // Precondition: every single-byte token is present, so the probe above hits.
    return {static_cast<Token>(data[0]), 1};
  }

 private:
  // short_by_len_[len] maps the low-`len`-byte packed key to a token, for len 1..8.
  FlatU64Map short_by_len_[kBucketPrefixLen + 1];
  FlatU64Map long_map_;  // 8-byte prefix -> index into buckets_
  std::vector<Bucket> buckets_;
  std::vector<TrieNode> pool_;
  std::vector<uint8_t> prefix_mask_ = std::vector<uint8_t>(kPrefixSlots, 0);
  uint32_t next_id_ = 0;

  void InsertInternal(const uint8_t* data, size_t len, Token id) {
    if (len >= 2) {
      uint32_t p = static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8);
      uint32_t bit = len <= kBucketPrefixLen ? (uint32_t{1} << (len - 1)) : kMaskLongBit;
      prefix_mask_[p] |= static_cast<uint8_t>(bit);
    }
    if (len <= kBucketPrefixLen) {
      uint64_t key = LoadLeU64(data, len, len);
      short_by_len_[len].Put(key, id);
      return;
    }
    uint64_t prefix = LoadLeU64(data, len, kBucketPrefixLen);
    size_t slen = len - kBucketPrefixLen;
    uint64_t suffix = LoadLeU64(data + kBucketPrefixLen, slen, slen);
    uint32_t bi = long_map_.Find(prefix);
    if (bi == kFlatEmpty) {
      bi = static_cast<uint32_t>(buckets_.size());
      buckets_.emplace_back();
      long_map_.Put(prefix, bi);
    }
    Bucket& b = buckets_[bi];
    if (b.trie_root < 0) {
      // Keep descending-by-length order so the first linear match is longest. The
      // bucket is already ordered, so place the new entry rather than re-sorting
      // the whole thing on every insert - this runs inside the training loop.
      // Two entries can only share a length if they also share a suffix, i.e. if
      // the same token bytes were inserted twice, so where equal lengths land
      // relative to each other is not observable.
      LongEntry e{suffix, static_cast<uint8_t>(slen), id};
      auto by_len_desc = [](const LongEntry& a, const LongEntry& c) {
        return a.slen > c.slen;
      };
      auto at = std::upper_bound(b.entries.begin(), b.entries.end(), e, by_len_desc);
      b.entries.insert(at, e);
      if (b.entries.size() > kPromoteThreshold) {
        BuildTrie(&b);
      }
    } else {
      uint8_t buf[8];
      std::memcpy(buf, &suffix, 8);
      TrieInsert(static_cast<uint32_t>(b.trie_root), buf, slen, id);
    }
  }

  bool SearchLinear(const std::vector<LongEntry>& entries, uint64_t val, size_t max_slen,
                    std::pair<Token, size_t>* out) const {
    for (const LongEntry& e : entries) {
      size_t elen = e.slen;
      if (elen <= max_slen && MatchingLowBytes(val ^ e.suffix) >= elen) {
        *out = {e.token, elen};
        return true;
      }
    }
    return false;
  }

  bool SearchTrie(uint32_t root, const uint8_t* suf, size_t suf_len,
                  std::pair<Token, size_t>* out) const {
    bool have = false;
    uint32_t cur = root;
    for (size_t pos = 0; pos < suf_len; ++pos) {
      uint32_t child;
      if (!TrieFindChild(cur, suf[pos], &child)) break;
      cur = child;
      if (pool_[cur].token >= 0) {
        *out = {static_cast<Token>(pool_[cur].token), pos + 1};
        have = true;
      }
    }
    return have;
  }

  bool TrieFindChild(uint32_t node, uint8_t byte, uint32_t* out) const {
    for (const auto& kv : pool_[node].children) {
      if (kv.first == byte) {
        *out = kv.second;
        return true;
      }
    }
    return false;
  }

  uint32_t TrieAlloc() {
    uint32_t idx = static_cast<uint32_t>(pool_.size());
    pool_.emplace_back();
    return idx;
  }

  void TrieInsert(uint32_t root, const uint8_t* suf, size_t slen, Token token) {
    uint32_t cur = root;
    for (size_t i = 0; i < slen; ++i) {
      uint32_t child;
      if (TrieFindChild(cur, suf[i], &child)) {
        cur = child;
      } else {
        uint32_t new_idx = TrieAlloc();
        pool_[cur].children.emplace_back(suf[i], new_idx);
        cur = new_idx;
      }
    }
    pool_[cur].token = static_cast<int>(token);
  }

  void BuildTrie(Bucket* b) {
    uint32_t root = TrieAlloc();
    for (const LongEntry& e : b->entries) {
      uint8_t buf[8];
      std::memcpy(buf, &e.suffix, 8);
      TrieInsert(root, buf, e.slen, e.token);
    }
    b->entries.clear();
    b->entries.shrink_to_fit();
    b->trie_root = static_cast<int32_t>(root);
  }
};

// Merge-threshold controller - DEVIATION FROM PAPER (D1; see onpair.h)

class DynamicThresholdController {
 public:
  DynamicThresholdController(size_t capacity, size_t total_bytes, double scan_fraction)
      : capacity_(capacity),
        scan_budget_(static_cast<size_t>(static_cast<double>(total_bytes) * scan_fraction)),
        check_interval_(std::max<size_t>(capacity / 128, 64)),
        next_checkpoint_(check_interval_) {}

  uint8_t get() const { return threshold_; }
  bool budget_exhausted() const { return bytes_scanned_ > scan_budget_; }
  void on_bytes_scanned(size_t n) { bytes_scanned_ += n; }

  void on_entry_created() {
    ++entries_created_;
    if (entries_created_ >= next_checkpoint_) Rebalance();
  }

 private:
  size_t capacity_;
  size_t scan_budget_;
  size_t check_interval_;
  uint8_t threshold_ = 2;
  size_t entries_created_ = 0;
  size_t bytes_scanned_ = 0;
  size_t entries_at_check_ = 0;
  size_t bytes_at_check_ = 0;
  size_t next_checkpoint_;

  void Rebalance() {
    size_t delta_e = entries_created_ - entries_at_check_;
    size_t delta_b = bytes_scanned_ - bytes_at_check_;
    double recent_rate =
        delta_b > 0 ? static_cast<double>(delta_e) / static_cast<double>(delta_b) : 1e9;
    size_t e_rem = capacity_ > entries_created_ ? capacity_ - entries_created_ : 1;
    size_t b_rem = scan_budget_ > bytes_scanned_ ? scan_budget_ - bytes_scanned_ : 1;
    double target_rate = static_cast<double>(e_rem) / static_cast<double>(b_rem);
    double ratio = target_rate > 0.0 ? recent_rate / target_rate : 1e9;

    if (ratio > 2.0 && threshold_ < 255) {
      ++threshold_;
    } else if (ratio < 0.5 && threshold_ > 2) {
      --threshold_;
    }
    entries_at_check_ = entries_created_;
    bytes_at_check_ = bytes_scanned_;
    next_checkpoint_ = entries_created_ + check_interval_;
  }
};

// Seeded PRNG for training-sample shuffle - DEVIATION FROM PAPER (D4)

inline uint64_t SplitMix64(uint64_t* state) {
  uint64_t z = (*state += 0x9E3779B97F4A7C15ull);
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
  return z ^ (z >> 31);
}

/// Partial Fisher-Yates: randomize the first `k` positions of `order`.
void PartialShuffle(std::vector<uint32_t>* order, size_t k, uint64_t seed) {
  size_t n = order->size();
  uint64_t state = seed;
  size_t limit = std::min(k, n);
  for (size_t i = 0; i < limit; ++i) {
    size_t span = n - i;
    size_t j = i + static_cast<size_t>(SplitMix64(&state) % span);
    std::swap((*order)[i], (*order)[j]);
  }
}

// Dictionary finalization

/// Sort tokens bytewise-lexicographically, returning fresh (bytes, offsets).
void SortTokens(const std::vector<uint8_t>& bytes, const std::vector<uint32_t>& offsets,
                std::vector<uint8_t>* out_bytes, std::vector<uint32_t>* out_offsets) {
  size_t n = offsets.size() - 1;
  auto tok_begin = [&](size_t id) { return bytes.data() + offsets[id]; };
  auto tok_len = [&](size_t id) { return offsets[id + 1] - offsets[id]; };

  std::vector<size_t> perm(n);
  std::iota(perm.begin(), perm.end(), 0);
  std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
    size_t la = tok_len(a), lb = tok_len(b);
    int cmp = std::memcmp(tok_begin(a), tok_begin(b), std::min(la, lb));
    if (cmp != 0) return cmp < 0;
    return la < lb;
  });

  out_bytes->clear();
  out_bytes->reserve(bytes.size());
  out_offsets->clear();
  out_offsets->reserve(n + 1);
  out_offsets->push_back(0);
  for (size_t old : perm) {
    out_bytes->insert(out_bytes->end(), tok_begin(old), tok_begin(old) + tok_len(old));
    out_offsets->push_back(static_cast<uint32_t>(out_bytes->size()));
  }
}

/// Append zero padding so the fixed 16-byte over-read of any token is in bounds.
void PadRaw(std::vector<uint8_t>* bytes, const std::vector<uint32_t>& offsets) {
  size_t need = static_cast<size_t>(offsets.back()) + kMaxTokenSize;
  if (bytes->size() < need) bytes->resize(need, 0);
}

// Dictionary construction / training (paper sec 3.2)

struct TrainResult {
  CompactDictionary dict;
  LongestPrefixMatcher lpm;
};

TrainResult Train(const uint8_t* data, const uint32_t* offsets, size_t n,
                  const Config& cfg, EncodeProfile* profile) {
  using Clock = std::chrono::steady_clock;
  auto t0 = Clock::now();
  size_t dict_capacity = size_t{1} << cfg.max_dict_bits;

  std::vector<uint8_t> dict_bytes;
  dict_bytes.reserve(dict_capacity * kMaxTokenSize);
  std::vector<uint32_t> dict_offsets;
  dict_offsets.reserve(dict_capacity + 1);
  dict_offsets.push_back(0);
  for (uint16_t i = 0; i <= 255; ++i) {
    dict_bytes.push_back(static_cast<uint8_t>(i));
    dict_offsets.push_back(static_cast<uint32_t>(dict_bytes.size()));
  }
  LongestPrefixMatcher lpm = LongestPrefixMatcher::New();

  size_t total_bytes = n == 0 ? 0 : offsets[n];
  size_t capacity = dict_capacity - 256;
  DynamicThresholdController ctrl(capacity, total_bytes, cfg.threshold_fraction);
  uint8_t threshold = ctrl.get();

  std::vector<uint32_t> order(n);
  std::iota(order.begin(), order.end(), 0u);
  // Full Fisher-Yates shuffle of the entire training order (D4 in onpair.h). The
  // dynamic byte budget still stops scanning well before the end, so only a
  // sample is trained on - but drawing that sample from a *full* shuffle avoids
  // skew on sequentially-ordered columns. (The Rust reference crate partial-shuffles
  // only ~0.3n rows and leaves them in the slice's TAIL while the trainer reads from
  // the head, so on ordered data like Customer#000... it trains mostly on
  // low-numbered rows and builds a skewed dictionary. This port already shuffles
  // into the head; a full shuffle matches the reference C++ std::shuffle over all
  // rows and removes any doubt.)
  PartialShuffle(&order, n, cfg.seed);

  FlatFreqMap freq;

  bool full_dictionary = false;
  bool budget_exhausted = false;

  for (uint32_t idx : order) {
    if (full_dictionary || budget_exhausted) break;

    size_t s_start = offsets[idx];
    size_t s_end = offsets[idx + 1];
    if (s_end == s_start) continue;
    const uint8_t* str = data + s_start;
    size_t len = s_end - s_start;

    auto [prev_id, prev_len] = lpm.FindLongestMatch(str, len);
    size_t pos = prev_len;

    ctrl.on_bytes_scanned(prev_len);
    if (ctrl.budget_exhausted()) {
      budget_exhausted = true;
      break;
    }

    while (pos < len) {
      auto [curr_id, curr_len] = lpm.FindLongestMatch(str + pos, len - pos);

      ctrl.on_bytes_scanned(curr_len);
      if (ctrl.budget_exhausted()) {
        budget_exhausted = true;
        break;
      }

      size_t pair_len = prev_len + curr_len;
      if (pair_len <= kMaxTokenSize) {
        uint32_t key = (static_cast<uint32_t>(prev_id) << 16) | static_cast<uint32_t>(curr_id);
        uint8_t count = freq.Bump(key);
        if (count >= threshold) {
          size_t pair_start = pos - prev_len;
          Token new_id = lpm.Insert(str + pair_start, pair_len);
          dict_bytes.insert(dict_bytes.end(), str + pair_start, str + pos + curr_len);
          dict_offsets.push_back(static_cast<uint32_t>(dict_bytes.size()));

          if (lpm.size() == dict_capacity) {
            full_dictionary = true;
            break;
          }
          ctrl.on_entry_created();
          threshold = ctrl.get();

          freq.Reset(key);
          prev_id = new_id;
          prev_len = pair_len;
          pos += curr_len;
          continue;
        }
      }
      prev_id = curr_id;
      prev_len = curr_len;
      pos += curr_len;
    }
  }

  std::vector<uint8_t> sorted_bytes;
  std::vector<uint32_t> sorted_offsets;
  auto t1 = Clock::now();
  SortTokens(dict_bytes, dict_offsets, &sorted_bytes, &sorted_offsets);
  PadRaw(&sorted_bytes, sorted_offsets);

  CompactDictionary dict;
  dict.bytes = std::move(sorted_bytes);
  dict.offsets = std::move(sorted_offsets);
  dict.RecomputeMaxTokenLen();
  LongestPrefixMatcher final_lpm = LongestPrefixMatcher::FromDictionary(dict);
  if (profile != nullptr) {
    profile->train_s = std::chrono::duration<double>(t1 - t0).count();
    profile->rebuild_s = std::chrono::duration<double>(Clock::now() - t1).count();
  }
  return TrainResult{std::move(dict), std::move(final_lpm)};
}

// Parsing: greedy longest-prefix tokenization (paper sec 3.3)

void EncodeStrings(const uint8_t* data, const uint32_t* offsets, size_t n,
                   const LongestPrefixMatcher& lpm, std::vector<uint16_t>* codes,
                   std::vector<uint32_t>* row_offsets) {
  row_offsets->push_back(0);
  for (size_t i = 0; i < n; ++i) {
    size_t s = offsets[i];
    size_t e = offsets[i + 1];
    size_t pos = s;
    while (pos < e) {
      auto [tok, mlen] = lpm.FindLongestMatch(data + pos, e - pos);
      codes->push_back(tok);
      pos += mlen;
    }
    row_offsets->push_back(static_cast<uint32_t>(codes->size()));
  }
}

}  // namespace

// Public API

Column Compress(const uint8_t* bytes, size_t /*bytes_len*/, const uint32_t* offsets,
                size_t num_rows, const Config& cfg, EncodeProfile* profile) {
  TrainResult tr = Train(bytes, offsets, num_rows, cfg, profile);
  Column col;
  col.dict = std::move(tr.dict);
  col.codes.reserve(num_rows == 0 ? 0 : offsets[num_rows]);
  col.row_offsets.reserve(num_rows + 1);
  auto t0 = std::chrono::steady_clock::now();
  EncodeStrings(bytes, offsets, num_rows, tr.lpm, &col.codes, &col.row_offsets);
  if (profile != nullptr) {
    profile->tokenize_s =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  }
  return col;
}

size_t DecodedLen(const Column& col) {
  size_t sum = 0;
  for (uint16_t c : col.codes) sum += col.dict.token_len(c);
  return sum;
}

namespace {

// Shared body of DecompressInto, parameterised on the gather-copy width for the
// same reason DecompressPackedFixed is. See CompactDictionary::max_token_len.
template <size_t kCopy>
size_t DecompressIntoFixed(const Column& col, uint8_t* out) {
  const CompactDictionary& dict = col.dict;
  size_t w = 0;
  for (uint16_t code : col.codes) {
    const uint8_t* src = dict.token_ptr(code);
    size_t len = dict.token_len(code);
    std::memcpy(out + w, src, kCopy);  // fixed over-copy, kCopy >= every token
    w += len;
  }
  return w;
}

}  // namespace

size_t DecompressInto(const Column& col, uint8_t* out) {
  const size_t maxlen = col.dict.max_token_len;
  if (maxlen <= 4) return DecompressIntoFixed<4>(col, out);
  if (maxlen <= 8) return DecompressIntoFixed<8>(col, out);
  return DecompressIntoFixed<kMaxTokenSize>(col, out);
}

std::vector<uint8_t> PackValues(const uint32_t* vals, size_t n, size_t bits) {
  std::vector<uint8_t> out((n * bits + 7) / 8 + 4, 0);
  size_t bitpos = 0;
  for (size_t i = 0; i < n; ++i) {
    size_t byte = bitpos >> 3, off = bitpos & 7;
    uint32_t w;
    std::memcpy(&w, out.data() + byte, 4);
    w |= (vals[i] << off);  // vals[i] < 2^bits, bits<=25, off<=7 -> fits in u32
    std::memcpy(out.data() + byte, &w, 4);
    bitpos += bits;
  }
  return out;
}

namespace {

// The gather-copy writes a fixed width per token so the copy length is a compile
// time constant, but that width only has to cover the longest token this
// dictionary actually holds -- not kMaxTokenSize. On corpora whose tokens are
// short the difference dominates decode: c_address averages 1.99 bytes per token,
// so a 16-byte copy moves 8x the bytes it needs to.
//
// Measured, this loop is store-bandwidth-bound. Across five unrelated corpora
// (over-copy factor) x (decode MiB/s) came out constant at ~10.6 GiB/s of store
// traffic, and the corpora with the highest over-copy decode slowest. Narrowing
// the width is therefore worth close to the bytes it saves.
//
// The width is chosen once per stream from the dictionary, so there is no
// per-token branch: a predicate on token length would be nearly free on corpora
// where it always goes one way and expensive on the ones that split (urls sit at
// 41% short, the worst possible mix).
template <size_t kCopy>
size_t DecompressPackedFixed(const CompactDictionary& dict, const uint8_t* packed, size_t ncodes,
                             size_t bits, uint8_t* out) {
  size_t bitpos = 0, w = 0;
  const uint32_t mask = (bits >= 32) ? 0xFFFFFFFFu : ((1u << bits) - 1);
  for (size_t i = 0; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, packed + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & mask;  // unpack the code
    bitpos += bits;
    const uint8_t* src = dict.token_ptr(static_cast<Token>(code));
    size_t len = dict.token_len(static_cast<Token>(code));
    std::memcpy(out + w, src, kCopy);  // fixed over-copy, kCopy >= every token
    w += len;
  }
  return w;
}

// As above but with the code width a compile-time constant, so the mask folds to a
// literal and `bitpos += kBits` strength-reduces. Dispatched once per stream, the
// same way the copy width is.
//
// Tried and rejected here, so it is not re-attempted: unpacking codes a block at a
// time before gathering, to break the `w += len` store-address dependency and to
// prefetch the token bytes. It lost 22% with the offsets prefetched and 41% with
// dict.bytes prefetched (worst case -65%), across all 20 corpora. The premise was
// wrong -- this loop is store-bound, not latency-bound, which is the same thing the
// copy-width measurement showed. Breaking a dependency chain buys nothing against a
// store-bandwidth limit, and the extra pass plus 64 prefetches per block only add
// traffic.
template <size_t kCopy, size_t kBits>
size_t DecompressPackedFixedBits(const CompactDictionary& dict, const uint8_t* packed,
                                 size_t ncodes, uint8_t* out) {
  constexpr uint32_t kMask = (kBits >= 32) ? 0xFFFFFFFFu : ((uint32_t{1} << kBits) - 1);
  const uint8_t* offsets_raw = reinterpret_cast<const uint8_t*>(dict.offsets.data());
  const uint8_t* dict_bytes = dict.bytes.data();
  size_t bitpos = 0, w = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, packed + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & kMask;
    bitpos += kBits;
    // offsets[code] and offsets[code + 1] are adjacent u32s, so one 8-byte load
    // yields the token's start and end together. token_ptr/token_len would issue
    // two loads for what is almost always a single cache line.
    uint64_t pair;
    std::memcpy(&pair, offsets_raw + size_t{code} * sizeof(uint32_t), sizeof(pair));
    const uint32_t start = static_cast<uint32_t>(pair);
    const size_t len = static_cast<uint32_t>(pair >> 32) - start;
    std::memcpy(out + w, dict_bytes + start, kCopy);
    w += len;
  }
  return w;
}

// Resolve `bits` to a constant for the widths a trained dictionary can produce
// (kMinDictBits..kMaxDictBits), falling back to the runtime-width loop otherwise so
// no input is rejected.
template <size_t kCopy>
size_t DecompressPackedDispatchBits(const CompactDictionary& dict, const uint8_t* packed,
                                    size_t ncodes, size_t bits, uint8_t* out) {
  switch (bits) {
    case 9: return DecompressPackedFixedBits<kCopy, 9>(dict, packed, ncodes, out);
    case 10: return DecompressPackedFixedBits<kCopy, 10>(dict, packed, ncodes, out);
    case 11: return DecompressPackedFixedBits<kCopy, 11>(dict, packed, ncodes, out);
    case 12: return DecompressPackedFixedBits<kCopy, 12>(dict, packed, ncodes, out);
    case 13: return DecompressPackedFixedBits<kCopy, 13>(dict, packed, ncodes, out);
    case 14: return DecompressPackedFixedBits<kCopy, 14>(dict, packed, ncodes, out);
    case 15: return DecompressPackedFixedBits<kCopy, 15>(dict, packed, ncodes, out);
    case 16: return DecompressPackedFixedBits<kCopy, 16>(dict, packed, ncodes, out);
    default: return DecompressPackedFixed<kCopy>(dict, packed, ncodes, bits, out);
  }
}

}  // namespace

size_t DecompressPacked(const CompactDictionary& dict, const uint8_t* packed, size_t ncodes,
                        size_t bits, uint8_t* out) {
  // Read the width, do not scan for it: an O(tokens) scan here costs 1-3% on
  // dictionaries of 20-60k tokens, which is charged to decode for something a
  // stored format keeps in its header. See CompactDictionary::max_token_len.
  const size_t maxlen = dict.max_token_len;
  // Only widths a single store can carry. A 12-byte copy moves 25% fewer bytes
  // than 16 but needs two stores, and measured that loses 4-6% on every corpus it
  // applied to (c_mktsegment, c_phone, p_container) -- so this is not purely a
  // bandwidth effect and one wide store beats two narrow ones. Narrowing to 8 is
  // worth 25-28% on the corpora that allow it.
  //
  // `out` needs kDecodePadding of slack either way, and dict.bytes is read-padded
  // by kMaxTokenSize, so every width here is in bounds.
  if (maxlen <= 4) return DecompressPackedDispatchBits<4>(dict, packed, ncodes, bits, out);
  if (maxlen <= 8) return DecompressPackedDispatchBits<8>(dict, packed, ncodes, bits, out);
  return DecompressPackedDispatchBits<kMaxTokenSize>(dict, packed, ncodes, bits, out);
}

}  // namespace parquet::onpair
