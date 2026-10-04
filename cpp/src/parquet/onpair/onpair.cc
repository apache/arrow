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
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <utility>

// The decode loop stores exactly one token's length per iteration when a
// predicated store is available, which removes the fixed over-copy entirely. This
// is a compile-time guard on purpose: the portable path below is byte-identical
// and only slower, so a build without SVE loses speed and nothing else.
#if defined(__ARM_FEATURE_SVE)
#  include <arm_sve.h>
#endif

namespace parquet::onpair {
namespace {

constexpr size_t kBucketPrefixLen = 8;
constexpr size_t kPromoteThreshold = 128;

void ValidateConfig(const Config& cfg) {
  if (cfg.max_dict_bits < 8 || cfg.max_dict_bits > 16) {
    throw std::invalid_argument("OnPair dictionary width must be between 8 and 16 bits");
  }
  if (!std::isfinite(cfg.threshold_fraction) || cfg.threshold_fraction <= 0.0 ||
      cfg.threshold_fraction > 1.0) {
    throw std::invalid_argument("OnPair threshold fraction must be in (0, 1]");
  }
}

void ValidateRows(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                  size_t num_rows) {
  if (offsets == nullptr) {
    throw std::invalid_argument("OnPair offsets must not be null");
  }
  if (bytes == nullptr && bytes_len != 0) {
    throw std::invalid_argument("OnPair bytes must not be null");
  }
  if (offsets[0] != 0 || offsets[num_rows] > bytes_len) {
    throw std::invalid_argument("OnPair offsets are outside the input buffer");
  }
  for (size_t i = 0; i < num_rows; ++i) {
    if (offsets[i] > offsets[i + 1]) {
      throw std::invalid_argument("OnPair offsets must be nondecreasing");
    }
  }
}

void ValidateDictionary(const CompactDictionary& dict) {
  if (dict.offsets.empty() || dict.offsets.front() != 0 ||
      dict.offsets.back() > dict.bytes.size()) {
    throw std::invalid_argument("Invalid OnPair dictionary offsets");
  }
  size_t actual_max = 0;
  for (size_t i = 0; i + 1 < dict.offsets.size(); ++i) {
    if (dict.offsets[i] >= dict.offsets[i + 1] ||
        dict.offsets[i + 1] - dict.offsets[i] > kMaxTokenSize) {
      throw std::invalid_argument(
          "OnPair dictionary tokens must contain between 1 and 16 bytes");
    }
    actual_max = std::max<size_t>(actual_max, dict.offsets[i + 1] - dict.offsets[i]);
  }
  if (dict.num_tokens() > 65536 || dict.max_token_len < actual_max ||
      dict.max_token_len > kMaxTokenSize ||
      dict.bytes.size() - dict.offsets.back() < kMaxTokenSize) {
    throw std::invalid_argument("Invalid OnPair dictionary metadata or padding");
  }
}

void ValidateCodes(const CompactDictionary& dict, const uint16_t* codes, size_t count) {
  ValidateDictionary(dict);
  for (size_t i = 0; i < count; ++i) {
    if (codes[i] >= dict.num_tokens()) {
      throw std::invalid_argument("OnPair code is outside the dictionary");
    }
  }
}

void ValidatePackedArguments(size_t num_tokens, size_t bits) {
  if (bits == 0 || bits > 16 || bits < CodeWidth(num_tokens)) {
    throw std::invalid_argument("Invalid OnPair packed code width");
  }
}

size_t RequiredPackedSize(size_t count, size_t bits) {
  if (count == 0) return 0;
  if (count - 1 > std::numeric_limits<size_t>::max() / bits) {
    throw std::invalid_argument("OnPair packed stream is too large");
  }
  const size_t last_byte = ((count - 1) * bits) / 8;
  if (last_byte > std::numeric_limits<size_t>::max() - sizeof(uint32_t)) {
    throw std::invalid_argument("OnPair packed stream is too large");
  }
  return last_byte + sizeof(uint32_t);
}

void ValidatePackedCodes(const uint8_t* packed, size_t packed_size, size_t count,
                         size_t bits, size_t num_tokens) {
  ValidatePackedArguments(num_tokens, bits);
  if (packed == nullptr && count != 0) {
    throw std::invalid_argument("OnPair packed input must not be null");
  }
  if (packed_size < RequiredPackedSize(count, bits)) {
    throw std::invalid_argument("OnPair packed input is truncated");
  }
  for (size_t i = 0; i < count; ++i) {
    if (GetBits(packed, packed_size, i * bits, bits) >= num_tokens) {
      throw std::invalid_argument("OnPair packed code is outside the dictionary");
    }
  }
}

size_t RequiredOutputSize(size_t decoded_size) {
  if (decoded_size > std::numeric_limits<size_t>::max() - kDecodePadding) {
    throw std::invalid_argument("OnPair decoded output is too large");
  }
  return decoded_size == 0 ? 0 : decoded_size + kDecodePadding;
}

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

  /// Empty matcher pre-loaded with `lits` only, ids assigned in the given order.
  /// The ids are positions in `lits`, not the byte values themselves, so
  /// FindLongestMatch's fallback would report a wrong id rather than report
  /// nothing. Callers must seed every byte the data contains; ChooseLiterals does.
  static LongestPrefixMatcher NewWithLiterals(const std::vector<uint8_t>& lits) {
    LongestPrefixMatcher m;
    for (size_t i = 0; i < lits.size(); ++i) {
      m.short_by_len_[1].Put(static_cast<uint64_t>(lits[i]), static_cast<uint32_t>(i));
    }
    m.next_id_ = static_cast<uint32_t>(lits.size());
    return m;
  }

  /// Build from a complete dictionary: token at index i receives id i.
  static LongestPrefixMatcher FromDictionary(const CompactDictionary& dict) {
    LongestPrefixMatcher m;
    size_t n = dict.num_tokens();
    for (size_t i = 0; i < n; ++i) {
      m.InsertInternal(dict.token_ptr(static_cast<Token>(i)),
                       dict.token_len(static_cast<Token>(i)), static_cast<Token>(i));
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
          found =
              SearchLinear(b.entries, LoadLeU64(suf, suf_len, suf_len), suf_len, &hit);
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
    // Precondition: every byte the data contains has a single-byte token, so the
    // probe above hits. Reachable only for a byte outside the seed set, where the
    // id below would be wrong -- see NewWithLiterals.
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
        scan_budget_(
            static_cast<size_t>(static_cast<double>(total_bytes) * scan_fraction)),
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

/// The single-byte tokens to seed: the byte values the column actually contains.
///
/// An absent byte costs a code for nothing, and at a narrow budget those codes are
/// the whole game. This set never overflows the budget -- there are at most 256 byte
/// values and the narrowest budget holds exactly 256 codes -- so no occurring byte
/// ever goes without a token and the tokenizer stays total with no escape code.
std::vector<uint8_t> ChooseLiterals(const uint8_t* data, size_t total_bytes,
                                    size_t budget) {
  bool present[256] = {};
  for (size_t i = 0; i < total_bytes; ++i) present[data[i]] = true;

  std::vector<uint8_t> lits;
  for (int b = 0; b < 256; ++b) {
    if (present[b]) lits.push_back(static_cast<uint8_t>(b));
  }
  assert(lits.size() <= budget && "a byte alphabet cannot outgrow a 256-code budget");
  (void)budget;
  return lits;
}

TrainResult Train(const uint8_t* data, const uint32_t* offsets, size_t n,
                  const Config& cfg, EncodeProfile* profile) {
  using Clock = std::chrono::steady_clock;
  auto t0 = Clock::now();
  size_t dict_capacity = size_t{1} << cfg.max_dict_bits;

  size_t total_bytes = n == 0 ? 0 : offsets[n];

  std::vector<uint8_t> lits;
  if (cfg.prune_absent_literals) {
    lits = ChooseLiterals(data, total_bytes, dict_capacity);
  } else {
    lits.resize(256);
    for (int i = 0; i < 256; ++i) lits[i] = static_cast<uint8_t>(i);
  }

  std::vector<uint8_t> dict_bytes;
  dict_bytes.reserve(dict_capacity * kMaxTokenSize);
  std::vector<uint32_t> dict_offsets;
  dict_offsets.reserve(dict_capacity + 1);
  dict_offsets.push_back(0);
  for (uint8_t b : lits) {
    dict_bytes.push_back(b);
    dict_offsets.push_back(static_cast<uint32_t>(dict_bytes.size()));
  }
  LongestPrefixMatcher lpm = cfg.prune_absent_literals
                                 ? LongestPrefixMatcher::NewWithLiterals(lits)
                                 : LongestPrefixMatcher::New();

  size_t capacity = dict_capacity - lits.size();
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

  // The literals can fill the budget outright -- 256 of them at an 8-bit budget --
  // in which case the check inside the loop is already true and would let the first
  // insert overrun.
  bool full_dictionary = capacity == 0;
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
        uint32_t key =
            (static_cast<uint32_t>(prev_id) << 16) | static_cast<uint32_t>(curr_id);
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

Column Compress(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                size_t num_rows, const Config& cfg, EncodeProfile* profile) {
  ValidateConfig(cfg);
  ValidateRows(bytes, bytes_len, offsets, num_rows);
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

Column CompressWithTokens(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                          size_t num_rows, const std::vector<uint8_t>& token_bytes,
                          const std::vector<uint32_t>& token_offsets) {
  if (token_offsets.empty() || token_offsets.front() != 0 ||
      token_offsets.back() != token_bytes.size() || token_offsets.size() > 65537) {
    throw std::invalid_argument("Invalid OnPair token offsets");
  }
  std::array<bool, 256> has_literal{};
  for (size_t i = 0; i + 1 < token_offsets.size(); ++i) {
    const size_t length = token_offsets[i + 1] - token_offsets[i];
    if (token_offsets[i] > token_offsets[i + 1] || length == 0 ||
        length > kMaxTokenSize) {
      throw std::invalid_argument("OnPair tokens must contain between 1 and 16 bytes");
    }
    if (token_offsets[i + 1] - token_offsets[i] == 1) {
      has_literal[token_bytes[token_offsets[i]]] = true;
    }
  }
  if (!std::all_of(has_literal.begin(), has_literal.end(),
                   [](bool present) { return present; })) {
    throw std::invalid_argument("OnPair token set must contain every byte literal");
  }
  ValidateRows(bytes, bytes_len, offsets, num_rows);
  std::vector<uint8_t> sorted_bytes;
  std::vector<uint32_t> sorted_offsets;
  SortTokens(token_bytes, token_offsets, &sorted_bytes, &sorted_offsets);
  PadRaw(&sorted_bytes, sorted_offsets);

  Column col;
  col.dict.bytes = std::move(sorted_bytes);
  col.dict.offsets = std::move(sorted_offsets);
  col.dict.RecomputeMaxTokenLen();
  LongestPrefixMatcher lpm = LongestPrefixMatcher::FromDictionary(col.dict);

  col.codes.reserve(num_rows == 0 ? 0 : offsets[num_rows]);
  col.row_offsets.reserve(num_rows + 1);
  EncodeStrings(bytes, offsets, num_rows, lpm, &col.codes, &col.row_offsets);
  return col;
}

size_t DecodedLen(const Column& col) {
  ValidateCodes(col.dict, col.codes.data(), col.codes.size());
  size_t sum = 0;
  for (uint16_t c : col.codes) {
    const size_t length = col.dict.token_len(c);
    if (sum > std::numeric_limits<size_t>::max() - length) {
      throw std::invalid_argument("OnPair decoded output is too large");
    }
    sum += length;
  }
  return sum;
}

void StridedDictionary::Build(const CompactDictionary& dict) {
  ValidateDictionary(dict);
  // Align `slots` to a cache line so that a kStride-byte slot, kStride being a
  // power of two no larger than a line, never straddles two lines. std::vector
  // only promises alignment for its element type, so over-allocate by one line
  // and point into `storage` at the first aligned byte.
  constexpr size_t kAlign = 64;
  const size_t ntokens = dict.num_tokens();
  const size_t slot_bytes = (ntokens * kStride + kAlign - 1) / kAlign * kAlign;
  storage.assign(slot_bytes + kAlign, 0);
  const size_t misalign = reinterpret_cast<uintptr_t>(storage.data()) % kAlign;
  slots = storage.data() + (misalign == 0 ? 0 : kAlign - misalign);
  lens.resize(ntokens);
  for (size_t t = 0; t < ntokens; ++t) {
    // Tokens are capped at kMaxTokenSize == kStride by training, so a token fills
    // at most its own slot and a length always fits in a byte. The zero-fill above
    // is what makes the unused tail of a slot well-defined for a fixed-width read.
    const size_t len = dict.offsets[t + 1] - dict.offsets[t];
    std::memcpy(slots + t * kStride, dict.bytes.data() + dict.offsets[t], len);
    lens[t] = static_cast<uint8_t>(len);
  }
  max_token_len = dict.max_token_len;
}

namespace {

// Building the strided view is one pass over the dictionary, so it pays for itself
// only when the code stream is long enough to amortise it. Below this the blob
// kernel runs directly. The crossover measured at roughly one code per token; the
// worst build-charged case, a two-token dictionary over a short column, reaches
// this threshold without slowing down.
bool StridedViewWorthBuilding(size_t ncodes, size_t ntokens) { return ncodes >= ntokens; }

// Shared body of DecompressInto over the strided view, parameterised on the copy
// width for the same reason the packed kernels are. See max_token_len.
template <size_t kCopy>
size_t DecompressIntoStrided(const StridedDictionary& dict, const Column& col,
                             uint8_t* out) {
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  size_t w = 0;
  for (uint16_t code : col.codes) {
    std::memcpy(out + w, slots + size_t{code} * StridedDictionary::kStride, kCopy);
    w += lens[code];
  }
  return w;
}

#if defined(__ARM_FEATURE_SVE)
// As above, storing exactly the token's length. One predicate serves both sides:
// the load cannot read past the slot and the store writes no byte it does not own,
// so there is no over-copy and no width dispatch.
size_t DecompressIntoStridedExact(const StridedDictionary& dict, const Column& col,
                                  uint8_t* out) {
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  size_t w = 0;
  for (uint16_t code : col.codes) {
    const uint8_t* src = slots + size_t{code} * StridedDictionary::kStride;
    const uint32_t len = lens[code];
    svbool_t pg = svwhilelt_b8_u32(0u, len);
    svst1_u8(pg, out + w, svld1_u8(pg, src));
    w += len;
  }
  return w;
}
#endif

// Shared body of DecompressInto over the stored dictionary, for the streams too
// short to earn the strided view.
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

size_t DecompressInto(const Column& col, uint8_t* out, size_t out_capacity) {
  ValidateCodes(col.dict, col.codes.data(), col.codes.size());
  const size_t decoded_size = DecodedLen(col);
  if ((out == nullptr && decoded_size != 0) ||
      out_capacity < RequiredOutputSize(decoded_size)) {
    throw std::invalid_argument("OnPair output buffer is too small");
  }
  const size_t maxlen = col.dict.max_token_len;
  if (!StridedViewWorthBuilding(col.codes.size(), col.dict.num_tokens())) {
    if (maxlen <= 4) return DecompressIntoFixed<4>(col, out);
    if (maxlen <= 8) return DecompressIntoFixed<8>(col, out);
    return DecompressIntoFixed<kMaxTokenSize>(col, out);
  }
  StridedDictionary view;
  view.Build(col.dict);
#if defined(__ARM_FEATURE_SVE)
  return DecompressIntoStridedExact(view, col, out);
#else
  if (maxlen <= 4) return DecompressIntoStrided<4>(view, col, out);
  if (maxlen <= 8) return DecompressIntoStrided<8>(view, col, out);
  return DecompressIntoStrided<kMaxTokenSize>(view, col, out);
#endif
}

std::vector<uint8_t> PackValues(const uint32_t* vals, size_t n, size_t bits) {
  if (bits == 0 || bits > 25 || (vals == nullptr && n != 0) ||
      n > (std::numeric_limits<size_t>::max() - 7) / bits) {
    throw std::invalid_argument("Invalid OnPair bit-packing arguments");
  }
  std::vector<uint8_t> out((n * bits + 7) / 8 + 4, 0);
  size_t bitpos = 0;
  for (size_t i = 0; i < n; ++i) {
    if (vals[i] >= (uint32_t{1} << bits)) {
      throw std::invalid_argument("OnPair value does not fit the packed width");
    }
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

// Random dictionary access, rather than output stores, limits this loop. Keep each
// token in a fixed slot and its length in a dense array so decoding needs one
// unpredictable payload lookup. Block prefetching cannot hide a lookup whose index
// becomes available only one code before use and would add another pass.
template <size_t kCopy>
size_t DecompressPackedFixed(const CompactDictionary& dict, const uint8_t* packed,
                             size_t ncodes, size_t bits, uint8_t* out) {
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
    // two loads for what is almost always a single cache line. The payload still
    // lives elsewhere, which is the second random line the strided view removes.
    uint64_t pair;
    std::memcpy(&pair, offsets_raw + size_t{code} * sizeof(uint32_t), sizeof(pair));
    const uint32_t start = static_cast<uint32_t>(pair);
    const size_t len = static_cast<uint32_t>(pair >> 32) - start;
    std::memcpy(out + w, dict_bytes + start, kCopy);
    w += len;
  }
  return w;
}

// The same loop against the strided view: one random line per token, and the length
// read from a dense byte array small enough to stay resident. The bit-unpack
// prologue is unchanged, so this differs from the kernel above in the gather alone.
template <size_t kCopy, size_t kBits>
size_t DecompressStridedBits(const StridedDictionary& dict, const uint8_t* packed,
                             size_t ncodes, uint8_t* out) {
  constexpr uint32_t kMask = (kBits >= 32) ? 0xFFFFFFFFu : ((uint32_t{1} << kBits) - 1);
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  size_t bitpos = 0, w = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, packed + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & kMask;
    bitpos += kBits;
    std::memcpy(out + w, slots + size_t{code} * StridedDictionary::kStride, kCopy);
    w += lens[code];
  }
  return w;
}

#if defined(__ARM_FEATURE_SVE)
// And with a predicated store, which is where most of the remaining gain is. One
// predicate covers the load and the store, so the loop reads only the token's own
// bytes and writes only the bytes it owns: the fixed over-copy is gone, and with it
// the reason to dispatch on max_token_len at all.
template <size_t kBits>
size_t DecompressStridedExactBits(const StridedDictionary& dict, const uint8_t* packed,
                                  size_t ncodes, uint8_t* out) {
  constexpr uint32_t kMask = (kBits >= 32) ? 0xFFFFFFFFu : ((uint32_t{1} << kBits) - 1);
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  size_t bitpos = 0, w = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, packed + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & kMask;
    bitpos += kBits;
    const uint8_t* src = slots + size_t{code} * StridedDictionary::kStride;
    const uint32_t len = lens[code];
    // kStride <= the SVE minimum vector length of 16 bytes, so this predicate never
    // needs more lanes than the hardware has.
    svbool_t pg = svwhilelt_b8_u32(0u, len);
    svst1_u8(pg, out + w, svld1_u8(pg, src));
    w += len;
  }
  return w;
}
#endif

// Which decode form to build. Set on the command line to measure one against
// another with nothing else in the build changed.
//
//   0  the per-code loops above, no unroll at all
//   1  groups of one phase period (below); at a 16-bit width the period is a single
//      code, so this leaves those columns on the per-code loop
//   2  groups of at least ONPAIR_GROUP_CODES codes at every width, 16 included
#ifndef ONPAIR_GROUP_UNROLL
#  define ONPAIR_GROUP_UNROLL 2
#endif

// 0 asks for the phase period itself, the smallest group that makes every offset
// and shift constant. A nonzero value asks for at least that many codes, rounded up
// to a whole period, so the request is a floor rather than the group emitted: four
// emits eight codes at the widths whose period is eight and four at 14 and 16, where
// the request is taken literally.
//
// Four is the smallest request that benefits both predicated and portable stores.
// Larger requests increase code size and leave longer scalar remainders without a
// consistent throughput gain.
#if ONPAIR_GROUP_UNROLL >= 2
#  ifndef ONPAIR_GROUP_CODES
#    define ONPAIR_GROUP_CODES 4
#  endif
constexpr size_t kGroupCodesRequest = ONPAIR_GROUP_CODES;
#else
constexpr size_t kGroupCodesRequest = 0;
#endif

// Group unroll on the bit cursor's phase period.
//
// At a fixed code width the (byte offset, intra-byte shift) pair a code is read
// with repeats with period 8 / gcd(kBits, 8) codes: 8 at the widths coprime with 8,
// 4 at 10 and 14, 2 at 12, 1 at 16. Unrolling by that period turns every offset and
// every shift into a compile-time constant and advances the stream pointer once per
// group instead of once per code, so the cursor arithmetic the loops above do per
// code disappears.
//
// Any multiple of the period preserves constant offsets. A minimum group size also
// exposes independent dictionary reads when the period is one, as it is at 16 bits.
// The implementation remains a single pass with no staging or prefetch traffic.
//
// A group's last code is read by a 4-byte load at a constant offset, which runs at
// most 3 bytes past the group at every width and group size used here. PackValues
// carries 4 zero-filled tail bytes and the per-code loops read just as far, so the
// precondition on `packed` is unchanged.
template <size_t kBits, size_t kRequest>
struct PackedCodeGroup {
  static constexpr size_t Gcd(size_t a, size_t b) { return b == 0 ? a : Gcd(b, a % b); }
  static constexpr size_t kPeriod = 8 / Gcd(kBits, 8);
  // A group has to consume a whole number of bytes so the next group starts at bit
  // offset zero again. That holds for any multiple of the period, so round up.
  static constexpr size_t kCodes =
      kRequest == 0 ? kPeriod : ((kRequest + kPeriod - 1) / kPeriod) * kPeriod;
  static constexpr size_t kBytes = kBits * kCodes / 8;
  static_assert(kBytes * 8 == kBits * kCodes, "a group must be a whole number of bytes");
};

template <size_t kBits, size_t J>
inline uint32_t GroupCodeAt(const uint8_t* p) {
  constexpr size_t kOff = (J * kBits) / 8;
  constexpr size_t kShift = (J * kBits) % 8;
  static_assert(kBits >= 1 && kBits <= 25, "the mask below would overflow");
  static_assert(kShift + kBits <= 32, "one 4-byte load must cover the code");
  constexpr uint32_t kMask = (uint32_t{1} << kBits) - 1;
  uint32_t word;
  std::memcpy(&word, p + kOff, sizeof(word));
  return (word >> kShift) & kMask;
}

// One call per code in the group, in stream order. A fold over the comma operator
// is sequenced left to right, which the callers rely on: the write cursor advances
// by each token's own length in turn.
template <size_t kBits, typename Emit, size_t... J>
inline void ForEachCodeInGroup(const uint8_t* p, Emit emit, std::index_sequence<J...>) {
  (emit(GroupCodeAt<kBits, J>(p)), ...);
}

// The strided portable loop, group-unrolled.
template <size_t kCopy, size_t kBits>
size_t DecompressStridedGroupBits(const StridedDictionary& dict, const uint8_t* packed,
                                  size_t ncodes, uint8_t* out) {
  using Group = PackedCodeGroup<kBits, kGroupCodesRequest>;
  // With the phase period asked for and a 16-bit width, the group is a single code:
  // no phase to fold, and the form would be the per-code loop with the shift known
  // to be zero. It measured indistinguishable from the plain loop, so at a group of
  // one this defers rather than emitting the same code a second time. Any request of
  // two or more never lands here.
  if constexpr (Group::kCodes <= 1) {
    return DecompressStridedBits<kCopy, kBits>(dict, packed, ncodes, out);
  }
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  const uint8_t* p = packed;
  size_t w = 0;
  auto emit = [&](uint32_t code) {
    std::memcpy(out + w, slots + size_t{code} * StridedDictionary::kStride, kCopy);
    w += lens[code];
  };
  for (size_t g = 0, ngroups = ncodes / Group::kCodes; g < ngroups; ++g) {
    ForEachCodeInGroup<kBits>(p, emit, std::make_index_sequence<Group::kCodes>{});
    p += Group::kBytes;
  }
  // A whole number of groups consumes a whole number of bytes, so the cursor is
  // byte aligned here and the tail starts its own bit offset from zero.
  constexpr uint32_t kMask = (uint32_t{1} << kBits) - 1;
  size_t bitpos = 0;
  for (size_t i = (ncodes / Group::kCodes) * Group::kCodes; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, p + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & kMask;
    bitpos += kBits;
    std::memcpy(out + w, slots + size_t{code} * StridedDictionary::kStride, kCopy);
    w += lens[code];
  }
  return w;
}

#if defined(__ARM_FEATURE_SVE)
// And the predicated-store loop, group-unrolled. Same two changes composed: one
// random line per token, exactly the token's bytes stored, constant addressing.
template <size_t kBits>
size_t DecompressStridedGroupExactBits(const StridedDictionary& dict,
                                       const uint8_t* packed, size_t ncodes,
                                       uint8_t* out) {
  using Group = PackedCodeGroup<kBits, kGroupCodesRequest>;
  // See above: at a group of one there is no phase to fold, and that form measured
  // slower than the plain loop.
  if constexpr (Group::kCodes <= 1) {
    return DecompressStridedExactBits<kBits>(dict, packed, ncodes, out);
  }
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  const uint8_t* p = packed;
  size_t w = 0;
  auto emit = [&](uint32_t code) {
    const uint32_t len = lens[code];
    svbool_t pg = svwhilelt_b8_u32(0u, len);
    svst1_u8(pg, out + w,
             svld1_u8(pg, slots + size_t{code} * StridedDictionary::kStride));
    w += len;
  };
  for (size_t g = 0, ngroups = ncodes / Group::kCodes; g < ngroups; ++g) {
    ForEachCodeInGroup<kBits>(p, emit, std::make_index_sequence<Group::kCodes>{});
    p += Group::kBytes;
  }
  constexpr uint32_t kMask = (uint32_t{1} << kBits) - 1;
  size_t bitpos = 0;
  for (size_t i = (ncodes / Group::kCodes) * Group::kCodes; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, p + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & kMask;
    bitpos += kBits;
    emit(code);
  }
  return w;
}
#endif

// Runtime fallback for valid widths that training does not produce.
template <size_t kCopy>
size_t DecompressStridedFixed(const StridedDictionary& dict, const uint8_t* packed,
                              size_t ncodes, size_t bits, uint8_t* out) {
  const uint8_t* slots = dict.slots;
  const uint8_t* lens = dict.lens.data();
  const uint32_t mask = (bits >= 32) ? 0xFFFFFFFFu : ((1u << bits) - 1);
  size_t bitpos = 0, w = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    uint32_t word;
    std::memcpy(&word, packed + (bitpos >> 3), 4);
    uint32_t code = (word >> (bitpos & 7)) & mask;
    bitpos += bits;
    std::memcpy(out + w, slots + size_t{code} * StridedDictionary::kStride, kCopy);
    w += lens[code];
  }
  return w;
}

// Resolve `bits` to a constant for the widths a trained dictionary can produce,
// falling back to the runtime-width loop otherwise so no input is rejected.
//
// Seeding only observed bytes permits dictionaries below 256 tokens, so dispatch
// every width that training can produce rather than sending narrow widths through
// the runtime fallback.
#define ONPAIR_DISPATCH_BITS(bits, CALL, FALLBACK) \
  switch (bits) {                                  \
    case 1:                                        \
      return CALL(1);                              \
    case 2:                                        \
      return CALL(2);                              \
    case 3:                                        \
      return CALL(3);                              \
    case 4:                                        \
      return CALL(4);                              \
    case 5:                                        \
      return CALL(5);                              \
    case 6:                                        \
      return CALL(6);                              \
    case 7:                                        \
      return CALL(7);                              \
    case 8:                                        \
      return CALL(8);                              \
    case 9:                                        \
      return CALL(9);                              \
    case 10:                                       \
      return CALL(10);                             \
    case 11:                                       \
      return CALL(11);                             \
    case 12:                                       \
      return CALL(12);                             \
    case 13:                                       \
      return CALL(13);                             \
    case 14:                                       \
      return CALL(14);                             \
    case 15:                                       \
      return CALL(15);                             \
    case 16:                                       \
      return CALL(16);                             \
    default:                                       \
      return FALLBACK;                             \
  }

template <size_t kCopy>
size_t DecompressPackedDispatchBits(const CompactDictionary& dict, const uint8_t* packed,
                                    size_t ncodes, size_t bits, uint8_t* out) {
#define ONPAIR_BLOB(B) DecompressPackedFixedBits<kCopy, B>(dict, packed, ncodes, out)
  ONPAIR_DISPATCH_BITS(bits, ONPAIR_BLOB,
                       DecompressPackedFixed<kCopy>(dict, packed, ncodes, bits, out))
#undef ONPAIR_BLOB
}

template <size_t kCopy>
size_t DecompressStridedDispatchBits(const StridedDictionary& dict, const uint8_t* packed,
                                     size_t ncodes, size_t bits, uint8_t* out){
#if ONPAIR_GROUP_UNROLL
#  define ONPAIR_STRIDED(B) \
    DecompressStridedGroupBits<kCopy, B>(dict, packed, ncodes, out)
#else
#  define ONPAIR_STRIDED(B) DecompressStridedBits<kCopy, B>(dict, packed, ncodes, out)
#endif
    ONPAIR_DISPATCH_BITS(bits, ONPAIR_STRIDED,
                         DecompressStridedFixed<kCopy>(dict, packed, ncodes, bits, out))
#undef ONPAIR_STRIDED
}

// Decode through a view, choosing the exact-length store where the target has one.
size_t DecompressThroughView(const StridedDictionary& dict, const uint8_t* packed,
                             size_t ncodes, size_t bits, uint8_t* out) {
#if defined(__ARM_FEATURE_SVE)
#  if ONPAIR_GROUP_UNROLL
#    define ONPAIR_STRIDED_EXACT(B) \
      DecompressStridedGroupExactBits<B>(dict, packed, ncodes, out)
#  else
#    define ONPAIR_STRIDED_EXACT(B) \
      DecompressStridedExactBits<B>(dict, packed, ncodes, out)
#  endif
  ONPAIR_DISPATCH_BITS(
      bits, ONPAIR_STRIDED_EXACT,
      DecompressStridedFixed<kMaxTokenSize>(dict, packed, ncodes, bits, out))
#  undef ONPAIR_STRIDED_EXACT
#else
  // Read the width, do not scan for it: an O(tokens) scan here costs 1-3% on
  // dictionaries of 20-60k tokens, which is charged to decode for something a
  // stored format keeps in its header. See CompactDictionary::max_token_len.
  //
  // Only widths a single store can carry, for the reason recorded above: a 12-byte
  // copy moves 25% fewer bytes than 16 but needs two stores and lost 4-6% wherever
  // it applied. Narrowing to 8 is worth 25-28% on the corpora that allow it.
  //
  // `out` needs kDecodePadding of slack either way, and a slot is zero-filled out
  // to kStride, so every width here is in bounds and reads defined bytes.
  const size_t maxlen = dict.max_token_len;
  if (maxlen <= 4)
    return DecompressStridedDispatchBits<4>(dict, packed, ncodes, bits, out);
  if (maxlen <= 8)
    return DecompressStridedDispatchBits<8>(dict, packed, ncodes, bits, out);
  return DecompressStridedDispatchBits<kMaxTokenSize>(dict, packed, ncodes, bits, out);
#endif
}

}  // namespace

size_t DecompressPacked(const StridedDictionary& dict, const uint8_t* packed,
                        size_t packed_size, size_t ncodes, size_t bits, uint8_t* out,
                        size_t out_capacity) {
  if ((dict.slots == nullptr && !dict.lens.empty()) ||
      dict.max_token_len > kMaxTokenSize) {
    throw std::invalid_argument("Invalid OnPair strided dictionary");
  }
  ValidatePackedCodes(packed, packed_size, ncodes, bits, dict.num_tokens());
  size_t decoded_size = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    const size_t length = dict.lens[GetBits(packed, packed_size, i * bits, bits)];
    if (decoded_size > std::numeric_limits<size_t>::max() - length) {
      throw std::invalid_argument("OnPair decoded output is too large");
    }
    decoded_size += length;
  }
  if ((out == nullptr && decoded_size != 0) ||
      out_capacity < RequiredOutputSize(decoded_size)) {
    throw std::invalid_argument("OnPair output buffer is too small");
  }
  return DecompressThroughView(dict, packed, ncodes, bits, out);
}

size_t DecompressPacked(const CompactDictionary& dict, const uint8_t* packed,
                        size_t packed_size, size_t ncodes, size_t bits, uint8_t* out,
                        size_t out_capacity) {
  ValidateDictionary(dict);
  ValidatePackedCodes(packed, packed_size, ncodes, bits, dict.num_tokens());
  size_t decoded_size = 0;
  for (size_t i = 0; i < ncodes; ++i) {
    const size_t length =
        dict.token_len(static_cast<Token>(GetBits(packed, packed_size, i * bits, bits)));
    if (decoded_size > std::numeric_limits<size_t>::max() - length) {
      throw std::invalid_argument("OnPair decoded output is too large");
    }
    decoded_size += length;
  }
  if ((out == nullptr && decoded_size != 0) ||
      out_capacity < RequiredOutputSize(decoded_size)) {
    throw std::invalid_argument("OnPair output buffer is too small");
  }
  if (!StridedViewWorthBuilding(ncodes, dict.num_tokens())) {
    // See DecompressThroughView for why the width is read rather than scanned for,
    // and why only 4/8/16 are offered.
    const size_t maxlen = dict.max_token_len;
    if (maxlen <= 4)
      return DecompressPackedDispatchBits<4>(dict, packed, ncodes, bits, out);
    if (maxlen <= 8)
      return DecompressPackedDispatchBits<8>(dict, packed, ncodes, bits, out);
    return DecompressPackedDispatchBits<kMaxTokenSize>(dict, packed, ncodes, bits, out);
  }
  StridedDictionary view;
  view.Build(dict);
  return DecompressThroughView(view, packed, ncodes, bits, out);
}

#undef ONPAIR_DISPATCH_BITS

// --- Choosing a dictionary budget -------------------------------------------

namespace {

// Fitted against 270 measurements -- 30 corpora x 9 budgets -- on Neoverse-V2, of
// median decode seconds against the code count and dictionary size each candidate
// trained to. The shape:
//
//   seconds = num_codes * (kNsBase + kNsPerOctave * max(0, log2(view / kViewKnee)))
//
// A code costs a flat 0.74ns while the strided view the gather walks stays inside
// about 128 KiB, and roughly 0.10ns more per doubling past that -- 1.05ns at the
// 1 MiB view a saturated 16-bit dictionary needs. R^2 = 0.91 on per-code cost.
//
// Re-fit these on a new microarchitecture by running the decode grid and
// regressing per-code cost on log2(view size); the knee is where per-code cost
// stops being flat. Getting them wrong costs selection quality, not correctness --
// the term that dominates is the code count, and no constant scales that away.
constexpr double kNsBase = 0.7417;
constexpr double kNsPerOctave = 0.1017;
constexpr double kViewKnee = 128.0 * 1024.0;

}  // namespace

double DecodeCostEstimate(uint64_t num_codes, uint32_t num_tokens) {
  // The gather walks the decode-side view, not the stored dictionary: a fixed
  // kStride bytes per token plus one length byte. Charging the stored blob instead
  // would understate a dictionary of short tokens, which is exactly the case the
  // narrow budgets produce.
  const double view_bytes = static_cast<double>(num_tokens) *
                            static_cast<double>(StridedDictionary::kStride + 1);
  double ns = kNsBase;
  if (view_bytes > kViewKnee) ns += kNsPerOctave * std::log2(view_bytes / kViewKnee);
  return static_cast<double>(num_codes) * ns;
}

size_t SelectBudget(const BudgetCandidate* candidates, size_t n,
                    const SelectionPolicy& policy) {
  if (n == 0) return 0;
  if (candidates == nullptr || std::isnan(policy.max_decode_regression) ||
      policy.max_decode_regression < 0.0) {
    throw std::invalid_argument("OnPair decode regression limit must be nonnegative");
  }
  double best_cost = std::numeric_limits<double>::infinity();
  for (size_t i = 0; i < n; ++i) {
    best_cost = std::min(
        best_cost, DecodeCostEstimate(candidates[i].num_codes, candidates[i].num_tokens));
  }
  // An infinite cap means "ignore decode", and infinity * anything must stay a
  // limit that admits everything rather than becoming a NaN.
  const double limit = std::isinf(policy.max_decode_regression)
                           ? std::numeric_limits<double>::infinity()
                           : best_cost * (1.0 + policy.max_decode_regression);
  size_t chosen = n;
  uint64_t chosen_bytes = 0;
  double chosen_cost = 0;
  for (size_t i = 0; i < n; ++i) {
    const double cost =
        DecodeCostEstimate(candidates[i].num_codes, candidates[i].num_tokens);
    if (cost > limit) continue;
    const bool better =
        chosen == n || candidates[i].stored_bytes < chosen_bytes ||
        (candidates[i].stored_bytes == chosen_bytes && cost < chosen_cost);
    if (better) {
      chosen = i;
      chosen_bytes = candidates[i].stored_bytes;
      chosen_cost = cost;
    }
  }
  return chosen;
}

namespace {

/// Bits needed to hold `x`, i.e. index of its highest set bit plus one.
size_t BitsFor(uint64_t x) {
  return x == 0 ? 0 : 64 - static_cast<size_t>(__builtin_clzll(x));
}

size_t PackedBytes(uint64_t n, size_t bits) {
  return static_cast<size_t>((n * bits + 7) / 8);
}

}  // namespace

uint8_t CodeWidth(size_t num_tokens) {
  if (num_tokens <= 1) return 1;
  return static_cast<uint8_t>(BitsFor(static_cast<uint64_t>(num_tokens - 1)));
}

uint64_t VaryingStoredBytes(const CompactDictionary& dict, uint64_t num_codes) {
  const uint64_t blob = dict.logical_bytes();
  // The offset array is bit-packed at the width the blob's own size needs, the same
  // way a stored dictionary would carry it; a fixed u32 per offset would charge a
  // 16-bit dictionary 260 KiB it does not need and bias selection toward narrow
  // budgets for a reason that is an artifact of this function.
  const uint64_t offsets =
      PackedBytes(dict.offsets.size(), std::max<size_t>(1, BitsFor(blob)));
  const uint64_t codes = PackedBytes(num_codes, CodeWidth(dict.num_tokens()));
  return blob + offsets + codes;
}

Column CompressAuto(const uint8_t* bytes, size_t bytes_len, const uint32_t* offsets,
                    size_t num_rows, const BudgetSearchOptions& opts,
                    SelectionReport* report) {
  ValidateRows(bytes, bytes_len, offsets, num_rows);
  if (!std::isfinite(opts.base.threshold_fraction) ||
      opts.base.threshold_fraction <= 0.0 || opts.base.threshold_fraction > 1.0) {
    throw std::invalid_argument("OnPair threshold fraction must be in (0, 1]");
  }
  if (std::isnan(opts.policy.max_decode_regression) ||
      opts.policy.max_decode_regression < 0.0) {
    throw std::invalid_argument("OnPair decode regression limit must be nonnegative");
  }
  const auto started = std::chrono::steady_clock::now();

  const uint8_t lo = std::max<uint8_t>(8, opts.min_budget);
  const uint8_t hi = std::max<uint8_t>(lo, std::min<uint8_t>(16, opts.max_budget));

  std::vector<BudgetCandidate> cands;
  cands.reserve(static_cast<size_t>(hi - lo) + 1);
  // Retain dictionaries to recognize candidates that train to the same result.
  // This is much cheaper than retaining every candidate's code stream.
  std::vector<CompactDictionary> dicts;
  // One scratch code stream, reused. Pass 1 needs a candidate's code count and nothing
  // else about its codes.
  std::vector<uint16_t> codes;
  std::vector<uint32_t> row_offsets;
  size_t distinct = 0;

  for (uint8_t b = lo; b <= hi; ++b) {
    Config cfg = opts.base;
    cfg.max_dict_bits = b;
    ValidateConfig(cfg);
    TrainResult tr = Train(bytes, offsets, num_rows, cfg, nullptr);

    // Different budgets can train to the same dictionary. Detect equality because
    // the threshold controller may take different paths to the same result.
    // Tokenization is a pure function of the dictionary, so duplicates reuse the
    // first candidate's measurements.
    size_t twin = dicts.size();
    for (size_t i = 0; i < dicts.size(); ++i) {
      if (dicts[i].offsets == tr.dict.offsets && dicts[i].bytes == tr.dict.bytes) {
        twin = i;
        break;
      }
    }

    BudgetCandidate cand;
    cand.budget = b;
    cand.num_tokens = static_cast<uint32_t>(tr.dict.num_tokens());
    cand.code_width = CodeWidth(tr.dict.num_tokens());
    if (twin != dicts.size()) {
      cand.num_codes = cands[twin].num_codes;
      cand.stored_bytes = cands[twin].stored_bytes;
    } else {
      ++distinct;
      codes.clear();
      row_offsets.clear();
      EncodeStrings(bytes, offsets, num_rows, tr.lpm, &codes, &row_offsets);
      cand.num_codes = codes.size();
      cand.stored_bytes =
          opts.stored_bytes == nullptr
              ? VaryingStoredBytes(tr.dict, cand.num_codes)
              : opts.stored_bytes(tr.dict, cand.num_codes, opts.stored_bytes_ctx);
    }
    dicts.push_back(std::move(tr.dict));
    cands.push_back(cand);
  }

  const size_t chosen = SelectBudget(cands.data(), cands.size(), opts.policy);

  Config winner = opts.base;
  winner.max_dict_bits = cands[chosen].budget;
  Column col = Compress(bytes, bytes_len, offsets, num_rows, winner, nullptr);

  if (report != nullptr) {
    report->candidates = std::move(cands);
    report->chosen = chosen;
    report->distinct_dictionaries = distinct;
    report->best_cost = std::numeric_limits<double>::infinity();
    size_t smallest = 0;
    for (size_t i = 0; i < report->candidates.size(); ++i) {
      const BudgetCandidate& c = report->candidates[i];
      report->best_cost =
          std::min(report->best_cost, DecodeCostEstimate(c.num_codes, c.num_tokens));
      if (c.stored_bytes < report->candidates[smallest].stored_bytes) smallest = i;
    }
    report->bytes_only = smallest;
    report->chosen_cost = DecodeCostEstimate(report->candidates[chosen].num_codes,
                                             report->candidates[chosen].num_tokens);
    report->encode_s =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
  }
  return col;
}

}  // namespace parquet::onpair
