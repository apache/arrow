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

// Standalone roundtrip validation for the OnPair C++ port. No Arrow deps.
// Build:  g++ -std=c++17 -O2 -I cpp/src onpair.cc onpair_test_standalone.cc -o t
//         (run from repo root, adjust -I to reach parquet/onpair/onpair.h)

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "parquet/onpair/onpair.h"

namespace op = parquet::onpair;

namespace {

int g_failures = 0;

// Pack a set of rows into (bytes, offsets) and roundtrip through OnPair.
bool Roundtrip(const std::vector<std::string>& rows, uint8_t bits, const char* name) {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;
  offsets.push_back(0);
  for (const auto& r : rows) {
    bytes.insert(bytes.end(), r.begin(), r.end());
    offsets.push_back(static_cast<uint32_t>(bytes.size()));
  }
  op::Config cfg;
  cfg.max_dict_bits = bits;
  cfg.threshold_fraction = 0.5;
  cfg.seed = 42;

  op::Column col = op::Compress(bytes.data(), bytes.size(), offsets.data(),
                                rows.size(), cfg);

  // Whole-column decode.
  size_t dlen = op::DecodedLen(col);
  std::vector<uint8_t> out(dlen + op::kDecodePadding, 0);
  size_t w = op::DecompressInto(col, out.data());
  bool ok = (w == bytes.size()) && (std::memcmp(out.data(), bytes.data(), bytes.size()) == 0);
  if (!ok) {
    std::printf("  FAIL %-22s bits=%2u: decoded %zu vs raw %zu%s\n", name, bits, w,
                bytes.size(), (w == bytes.size() ? " (content mismatch)" : ""));
    ++g_failures;
    return false;
  }
  // Sanity: every code indexes the dictionary.
  for (uint16_t c : col.codes) {
    if (c >= col.dict.num_tokens()) {
      std::printf("  FAIL %-22s bits=%2u: code out of range\n", name, bits);
      ++g_failures;
      return false;
    }
  }
  return true;
}

std::vector<std::string> SyntheticUrls(size_t n) {
  const char* hosts[] = {"https://www.yandex.ru", "https://www.google.com",
                         "https://news.ycombinator.com", "http://m.yandex.ru",
                         "ftp://files.example.com"};
  const char* paths[] = {"/", "/search?q=", "/api/v1/data", "/blog/post-", "/users/"};
  std::vector<std::string> out;
  uint64_t x = 0x9E3779B97F4A7C15ull;
  for (size_t i = 0; i < n; ++i) {
    x += 0x9E3779B97F4A7C15ull;
    std::string s = hosts[(x) % 5];
    s += paths[(x >> 16) % 5];
    s += std::to_string(static_cast<uint16_t>(x >> 48));
    out.push_back(std::move(s));
  }
  return out;
}

}  // namespace

int main() {
  std::printf("OnPair C++ port roundtrip tests\n");

  for (uint8_t bits = 9; bits <= 16; ++bits) {
    // Mixed lengths incl. empty, 1-byte, boundary 8/9, 16, >16.
    Roundtrip({"", "a", "ab", "12345678", "123456789", "0123456789abcdef",
               "0123456789abcdefGHIJ", "hello world hello world"},
              bits, "mixed_lengths");

    // Binary with NUL bytes.
    std::vector<std::string> bin;
    for (int i = 0; i < 40; ++i) {
      std::string s;
      for (int j = 0; j < 30; ++j) s.push_back(static_cast<char>((i * 7 + j * 3) & 0xFF));
      bin.push_back(std::move(s));
    }
    Roundtrip(bin, bits, "binary_nul");

    // Homogeneous (heavy merges).
    Roundtrip(std::vector<std::string>(50, std::string(40, 'a')), bits, "homogeneous");

    // Shared long prefix (exercises long bucket / trie promotion).
    std::vector<std::string> shared;
    for (int i = 0; i < 300; ++i) shared.push_back("https://prefix/" + std::to_string(i));
    Roundtrip(shared, bits, "shared_long_prefix");

    // Synthetic URLs.
    Roundtrip(SyntheticUrls(20000), bits, "synthetic_urls");

    // All-empty and single-empty edge cases.
    Roundtrip({"", "", ""}, bits, "all_empty");
    Roundtrip({}, bits, "no_rows");
  }

  if (g_failures == 0) {
    std::printf("ALL PASS\n");
    return 0;
  }
  std::printf("%d FAILURES\n", g_failures);
  return 1;
}
