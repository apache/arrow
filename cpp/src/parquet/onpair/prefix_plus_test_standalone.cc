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

// Standalone validation for the prefix_plus cleaving core. No Arrow deps.
// Build:  g++ -std=c++17 -O2 -Icpp/src \
//           cpp/src/parquet/onpair/prefix_plus.cc \
//           cpp/src/parquet/onpair/prefix_plus_test_standalone.cc -o t
//
// Checks the cleaving invariant that makes decode correct: for every string i,
// its first prefix_len[i] bytes equal the first prefix_len[i] bytes of its
// chunk representative chunk_first[i] (so reconstructing prefix++suffix rebuilds
// the string exactly), plus structural bounds and the FSST-escape guard.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "parquet/onpair/prefix_plus.h"

namespace pp = parquet::prefix_plus;

namespace {

int g_failures = 0;

// Sort `rows` (as the "+" codecs do before cleaving), run CleaveSorted, and
// verify the reconstruction invariant + bounds. `guard` mirrors the FSST-escape
// path.
bool Check(std::vector<std::string> rows, bool guard, const char* name) {
  std::sort(rows.begin(), rows.end());

  std::vector<const uint8_t*> ptrs(rows.size());
  std::vector<size_t> lens(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    ptrs[i] = reinterpret_cast<const uint8_t*>(rows[i].data());
    lens[i] = rows[i].size();
  }

  pp::Cleaving cl =
      pp::CleaveSorted(ptrs.data(), lens.data(), rows.size(), pp::kMaxPrefix, guard);

  if (cl.prefix_len.size() != rows.size() || cl.chunk_first.size() != rows.size()) {
    std::printf("  FAIL %-22s: wrong result size\n", name);
    ++g_failures;
    return false;
  }

  for (size_t i = 0; i < rows.size(); ++i) {
    const uint32_t p = cl.prefix_len[i];
    const uint32_t cf = cl.chunk_first[i];
    // Bounds.
    if (p > lens[i] || p > pp::kMaxPrefix || cf > i) {
      std::printf("  FAIL %-22s: bad cleave at %zu (p=%u cf=%u len=%zu)\n", name, i, p, cf,
                  lens[i]);
      ++g_failures;
      return false;
    }
    // Same block (cleaving never crosses a 128-block boundary).
    if (cf / pp::kBlockSize != i / pp::kBlockSize) {
      std::printf("  FAIL %-22s: chunk crosses block at %zu\n", name, i);
      ++g_failures;
      return false;
    }
    // Reconstruct: prefix (from chunk rep) ++ suffix (own bytes after p) == row.
    std::string rebuilt(rows[cf].data(), p);
    rebuilt.append(rows[i].data() + p, lens[i] - p);
    if (rebuilt != rows[i]) {
      std::printf("  FAIL %-22s: reconstruction mismatch at %zu\n", name, i);
      ++g_failures;
      return false;
    }
    // FSST-escape guard: a prefix must not end right after an escape byte.
    if (guard && p > 0 && static_cast<uint8_t>(rows[i][p - 1]) == 255) {
      std::printf("  FAIL %-22s: prefix splits escape at %zu\n", name, i);
      ++g_failures;
      return false;
    }
  }
  return true;
}

}  // namespace

int main() {
  std::printf("prefix_plus cleaving tests\n");

  // Identical strings (whole string becomes the shared prefix, empty suffixes).
  Check(std::vector<std::string>(200, "Customer#000000042"), false, "identical");

  // Monotonic shared prefix (the target shape).
  {
    std::vector<std::string> v;
    for (int i = 1; i <= 500; ++i) {
      char buf[32];
      std::snprintf(buf, sizeof(buf), "Customer#%09d", i);
      v.emplace_back(buf);
    }
    Check(v, false, "monotonic_prefix");
  }

  // No shared prefix (distinct first bytes) -> every prefix_len should be 0.
  {
    std::vector<std::string> v;
    for (int i = 0; i < 100; ++i) v.push_back(std::string(1, static_cast<char>('A' + i % 26)) +
                                              std::to_string(i));
    Check(v, false, "no_shared_prefix");
  }

  // Prefix longer than 255 must be capped (uint8 prefix_length).
  Check(std::vector<std::string>(64, std::string(400, 'z')), false, "over_255_cap");

  // Empty and near-empty rows.
  Check({"", "", "a", "ab", "abc"}, false, "empties");

  // Multi-block (>128) with grouped prefixes across block boundaries.
  {
    std::vector<std::string> v;
    for (int g = 0; g < 10; ++g)
      for (int i = 0; i < 40; ++i) v.push_back("group" + std::to_string(g) + "/item" +
                                               std::to_string(i));
    Check(v, false, "multi_block");
  }

  // FSST-escape guard: rows containing byte 255 adjacent to shared regions.
  {
    std::vector<std::string> v;
    for (int i = 0; i < 60; ++i) {
      std::string s = "pre";
      s.push_back(static_cast<char>(255));
      s.push_back(static_cast<char>('a' + i % 5));
      s += std::to_string(i);
      v.push_back(std::move(s));
    }
    Check(v, true, "escape_guard");
  }

  // Single row and empty input.
  Check({"lonely"}, false, "single_row");
  Check({}, false, "no_rows");

  if (g_failures == 0) {
    std::printf("ALL PASS\n");
    return 0;
  }
  std::printf("%d FAILURES\n", g_failures);
  return 1;
}
