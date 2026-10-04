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

#include "parquet/onpair/prefix_plus.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace parquet::prefix_plus {
namespace {

// Longest common prefix of two strings, capped at `cap`. With `guard`, trim so
// the returned length never falls between an FSST escape byte (255) and its
// literal: if the last matched byte is 255, drop it (thesis Listing 5.4).
size_t Lcp(const uint8_t* a, size_t la, const uint8_t* b, size_t lb, size_t cap, bool guard) {
  size_t m = std::min(std::min(la, lb), cap);
  size_t l = 0;
  while (l < m && a[l] == b[l]) ++l;
  if (guard && l != 0 && a[l - 1] == 255) --l;
  return l;
}

// Thesis DP (sec 5.2.2) for one block: strings are strs[0..bn) / lens[0..bn)
// (already sorted). Writes prefix_len[k] and chunk_first_local[k] for each
// string k in the block; chunk_first_local is a block-local index. Returns the
// number of chunks that carry a non-empty prefix.
size_t CleaveBlock(const uint8_t* const* strs, const size_t* lens, size_t bn, size_t max_prefix,
                   bool guard, uint32_t* prefix_len, uint32_t* chunk_first_local) {
  if (bn == 0) return 0;

  // Consecutive LCPs, then min_lcp[i][j] = shared prefix length of strings i..j
  // as the running minimum of adjacent LCPs (standard range-LCP identity).
  std::vector<size_t> lcp(bn > 0 ? bn - 1 : 0);
  for (size_t i = 0; i + 1 < bn; ++i) {
    lcp[i] = Lcp(strs[i], lens[i], strs[i + 1], lens[i + 1], max_prefix, guard);
  }
  std::vector<std::vector<uint32_t>> min_lcp(bn, std::vector<uint32_t>(bn, 0));
  for (size_t i = 0; i < bn; ++i) {
    min_lcp[i][i] = static_cast<uint32_t>(std::min(lens[i], max_prefix));
    for (size_t j = i + 1; j < bn; ++j) {
      min_lcp[i][j] = std::min<uint32_t>(min_lcp[i][j - 1], static_cast<uint32_t>(lcp[j - 1]));
    }
  }

  std::vector<size_t> len_prefix_sum(bn + 1, 0);
  for (size_t i = 0; i < bn; ++i) len_prefix_sum[i + 1] = len_prefix_sum[i] + lens[i];

  constexpr size_t kInf = std::numeric_limits<size_t>::max();
  std::vector<size_t> dp(bn + 1, kInf), prev(bn + 1, 0), pfx(bn + 1, 0);
  dp[0] = 0;
  for (size_t i = 1; i <= bn; ++i) {
    for (size_t j = 0; j < i; ++j) {
      if (dp[j] == kInf) continue;
      const size_t mcp = min_lcp[j][i - 1];
      const size_t candidates[2] = {0, mcp};
      const int n_cand = mcp > 0 ? 2 : 1;
      for (int c = 0; c < n_cand; ++c) {
        const size_t p = candidates[c];
        const size_t cnt = i - j;
        const size_t per_string_overhead = 1 + (p > 0 ? 2 : 0);  // prefix_length [+ jumpback]
        const size_t overhead = cnt * per_string_overhead;
        const size_t sum_len = len_prefix_sum[i] - len_prefix_sum[j];
        // Store the shared prefix once (p bytes) + all suffixes (sum_len - cnt*p)
        // + overhead == overhead + sum_len - (cnt-1)*p.
        const size_t cost = dp[j] + overhead + sum_len - (cnt - 1) * p;
        if (cost < dp[i]) {
          dp[i] = cost;
          prev[i] = j;
          pfx[i] = p;
        }
      }
    }
  }

  // Backtrack into chunks (start_local, prefix_length), then assign per string.
  size_t idx = bn;
  size_t num_prefix_chunks = 0;
  while (idx > 0) {
    const size_t start = prev[idx];
    const size_t p = pfx[idx];
    if (p > 0) ++num_prefix_chunks;
    for (size_t k = start; k < idx; ++k) {
      prefix_len[k] = static_cast<uint32_t>(p);
      chunk_first_local[k] = static_cast<uint32_t>(start);
    }
    idx = start;
  }
  return num_prefix_chunks;
}

}  // namespace

Cleaving CleaveSorted(const uint8_t* const* strs, const size_t* lens, size_t n, size_t max_prefix,
                      bool guard_escape255) {
  if (max_prefix > kMaxPrefix) max_prefix = kMaxPrefix;
  Cleaving out;
  out.prefix_len.assign(n, 0);
  out.chunk_first.assign(n, 0);
  out.num_prefix_chunks = 0;

  std::vector<uint32_t> local_first(kBlockSize);
  for (size_t base = 0; base < n; base += kBlockSize) {
    const size_t bn = std::min(kBlockSize, n - base);
    out.num_prefix_chunks +=
        CleaveBlock(strs + base, lens + base, bn, max_prefix, guard_escape255,
                    out.prefix_len.data() + base, local_first.data());
    // Lift block-local chunk-first indices to global positions.
    for (size_t k = 0; k < bn; ++k) {
      out.chunk_first[base + k] = static_cast<uint32_t>(base) + local_first[k];
    }
  }
  return out;
}

}  // namespace parquet::prefix_plus
