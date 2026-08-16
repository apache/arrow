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

// Microbenchmark comparing the current REPLACE implementation against the
// pre-change one, to measure the cost of the match-counting scan the fix added
// to size the output buffer.
//
//   BM_ReplaceNew = replace_utf8_utf8_utf8 (upper bound or counting scan, then a
//                   single write pass)
//   BM_ReplaceOld = replace_with_max_len_utf8_utf8_utf8(..., capacity, ...) given
//                   an exact buffer: the pre-change algorithm with no counting
//                   scan. Compare the two rows per case to read the scan's cost.
//
// Unlike the projector-level micro_benchmarks, this calls the precompiled
// functions directly, so the build compiles string_ops.cc with GANDIVA_UNIT_TEST.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {
namespace {

struct ReplaceCase {
  const char* name;
  int64_t text_len;
  int stride;  // a match (the first byte of `from`) every `stride` bytes
  const char* from;
  const char* to;
};

const std::vector<ReplaceCase>& Cases() {
  static const std::vector<ReplaceCase> cases = {
      // Small expansion (to_len - from_len <= from_len): no scan, upper bound.
      {"small/dense expand a->ab", 256, 1, "a", "ab"},
      {"small/sparse expand a->ab", 256, 64, "a", "ab"},
      {"medium/dense expand a->ab", 64 * 1024, 1, "a", "ab"},
      {"medium/sparse expand a->ab", 64 * 1024, 64, "a", "ab"},
      {"large/dense expand a->ab", 4 * 1024 * 1024, 1, "a", "ab"},
      {"large/sparse expand a->ab", 4 * 1024 * 1024, 64, "a", "ab"},
      // Big expansion (to_len - from_len > from_len): falls back to the scan.
      {"large/dense bigexp a->abcd", 4 * 1024 * 1024, 1, "a", "abcd"},
      {"large/sparse bigexp a->abcd", 4 * 1024 * 1024, 64, "a", "abcd"},
      // Shrink (to_len <= from_len): no scan.
      {"large/dense shrink ab->a", 4 * 1024 * 1024, 2, "ab", "a"},
  };
  return cases;
}

// Builds a `len`-byte string with `match` once every `stride` bytes.
std::string MakeText(int64_t len, int stride, char match, char filler) {
  std::string s(static_cast<size_t>(len), filler);
  for (int64_t i = 0; i < len; i += stride) {
    s[static_cast<size_t>(i)] = match;
  }
  return s;
}

// Exact output size, so the "old" arm gets a buffer large enough to complete.
int32_t ExactCapacity(const std::string& text, const char* from, int flen, int olen) {
  int64_t matches = 0;
  auto tlen = static_cast<int32_t>(text.size());
  if (flen > 0 && flen <= tlen) {
    for (int32_t i = 0; i <= tlen - flen;) {
      if (memcmp(text.data() + i, from, flen) == 0) {
        ++matches;
        i += flen;
      } else {
        ++i;
      }
    }
  }
  return static_cast<int32_t>(tlen + matches * (olen - flen));
}

void BM_ReplaceNew(benchmark::State& state) {
  const ReplaceCase& c = Cases()[state.range(0)];
  auto flen = static_cast<int>(strlen(c.from));
  auto olen = static_cast<int>(strlen(c.to));
  std::string text = MakeText(c.text_len, c.stride, c.from[0], 'x');
  auto tlen = static_cast<int32_t>(text.size());
  ExecutionContext ctx;
  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  // One warm-up call doubling as a correctness guard.
  int32_t out_len = 0;
  replace_utf8_utf8_utf8(ctx_ptr, text.data(), tlen, c.from, flen, c.to, olen, &out_len);
  if (ctx.has_error()) {
    state.SkipWithError(ctx.get_error().c_str());
    return;
  }

  for (auto _ : state) {
    ctx.Reset();
    const char* out = replace_utf8_utf8_utf8(ctx_ptr, text.data(), tlen, c.from, flen,
                                             c.to, olen, &out_len);
    benchmark::DoNotOptimize(out);
    benchmark::DoNotOptimize(out_len);
  }
  state.SetBytesProcessed(state.iterations() * tlen);
  state.SetLabel(c.name);
}

void BM_ReplaceOld(benchmark::State& state) {
  const ReplaceCase& c = Cases()[state.range(0)];
  auto flen = static_cast<int>(strlen(c.from));
  auto olen = static_cast<int>(strlen(c.to));
  std::string text = MakeText(c.text_len, c.stride, c.from[0], 'x');
  auto tlen = static_cast<int32_t>(text.size());
  int32_t capacity = ExactCapacity(text, c.from, flen, olen);
  ExecutionContext ctx;
  auto ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  int32_t out_len = 0;
  replace_with_max_len_utf8_utf8_utf8(ctx_ptr, text.data(), tlen, c.from, flen, c.to,
                                      olen, capacity, &out_len);
  if (ctx.has_error()) {
    state.SkipWithError(ctx.get_error().c_str());
    return;
  }

  for (auto _ : state) {
    ctx.Reset();
    const char* out = replace_with_max_len_utf8_utf8_utf8(
        ctx_ptr, text.data(), tlen, c.from, flen, c.to, olen, capacity, &out_len);
    benchmark::DoNotOptimize(out);
    benchmark::DoNotOptimize(out_len);
  }
  state.SetBytesProcessed(state.iterations() * tlen);
  state.SetLabel(c.name);
}

}  // namespace

BENCHMARK(BM_ReplaceNew)
    ->DenseRange(0, static_cast<int64_t>(Cases().size()) - 1)
    ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_ReplaceOld)
    ->DenseRange(0, static_cast<int64_t>(Cases().size()) - 1)
    ->Unit(benchmark::kMicrosecond);

}  // namespace gandiva
