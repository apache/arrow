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
// pre-change one, to measure the cost of the O(n) match-counting scan that the
// fix added to size the output buffer exactly.
//
//   new = replace_utf8_utf8_utf8            (counting scan + exact allocation)
//   old = replace_with_max_len_utf8_utf8_utf8(..., capacity, ...)
//         The old wrapper was literally a call to this with a fixed cap of
//         65535; given a buffer large enough to hold the result it is the
//         pre-change algorithm with no counting scan, so new-vs-old isolates
//         the scan overhead. (The shipped old cap of 65535 additionally errored
//         out for any result above ~64 KB -- the bug this change fixes.)
//
// Reported time includes one ExecutionContext::Reset() per call in both arms
// (it cancels out in the delta), mirroring per-call arena reuse.

#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "gandiva/execution_context.h"
#include "gandiva/precompiled/types.h"

namespace gandiva {
namespace {

// Builds a `len`-byte string whose `match` byte occurs once every `stride`
// bytes (the rest filled with `filler`), giving len/stride non-overlapping
// matches.
std::string MakeText(int64_t len, int stride, char match, char filler) {
  std::string s(static_cast<size_t>(len), filler);
  for (int64_t i = 0; i < len; i += stride) {
    s[static_cast<size_t>(i)] = match;
  }
  return s;
}

double TimeNsPerCall(const std::function<void()>& fn, int iters) {
  auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < iters; ++i) {
    fn();
  }
  auto t1 = std::chrono::steady_clock::now();
  return std::chrono::duration<double, std::nano>(t1 - t0).count() / iters;
}

struct Case {
  std::string name;
  int64_t text_len;
  int stride;  // a match every `stride` bytes
  std::string from;
  std::string to;
  int iters;
};

}  // namespace

TEST(StringOpsBenchmark, Replace) {
  ExecutionContext ctx;
  int64_t ctx_ptr = reinterpret_cast<int64_t>(&ctx);

  const int64_t kSmall = 256;
  const int64_t kMedium = 64 * 1024;
  const int64_t kLarge = 4 * 1024 * 1024;

  std::vector<Case> cases = {
      // Expansion cases (to longer than from) -- these take the O(n) scan path.
      {"small/dense  expand a->ab", kSmall, 1, "a", "ab", 200000},
      {"small/sparse expand a->ab", kSmall, 64, "a", "ab", 200000},
      {"medium/dense  expand a->ab", kMedium, 1, "a", "ab", 4000},
      {"medium/sparse expand a->ab", kMedium, 64, "a", "ab", 4000},
      {"large/dense  expand a->ab", kLarge, 1, "a", "ab", 80},
      {"large/sparse expand a->ab", kLarge, 64, "a", "ab", 80},
      // Big-expansion cases (to_len - from_len > from_len) -- these fall back to
      // the exact match-counting scan, so new should still show the scan delta.
      {"large/dense  bigexp a->abcd", kLarge, 1, "a", "abcd", 80},
      {"large/sparse bigexp a->abcd", kLarge, 64, "a", "abcd", 80},
      // Shrink case (to no longer than from) -- new skips the scan entirely.
      {"large/dense  shrink ab->a", kLarge, 2, "ab", "a", 80},
  };

  printf("\n%-28s %10s %9s %12s %12s %9s\n", "case", "text_len", "matches", "new ns/call",
         "old ns/call", "delta");
  printf("%s\n", std::string(92, '-').c_str());

  for (const auto& c : cases) {
    std::string text = MakeText(c.text_len, c.stride, c.from[0], 'x');
    const char* tp = text.data();
    auto tlen = static_cast<int32_t>(text.size());
    const char* fp = c.from.data();
    auto flen = static_cast<int32_t>(c.from.size());
    const char* op = c.to.data();
    auto olen = static_cast<int32_t>(c.to.size());

    // Count matches and compute the exact output size to give the "old" arm a
    // buffer large enough to complete (precomputed -- not part of the timing).
    int64_t matches = 0;
    if (flen > 0 && flen <= tlen) {
      for (int32_t i = 0; i <= tlen - flen;) {
        if (memcmp(tp + i, fp, flen) == 0) {
          ++matches;
          i += flen;
        } else {
          ++i;
        }
      }
    }
    auto out_capacity = static_cast<int32_t>(tlen + matches * (olen - flen));

    auto run_new = [&]() {
      int32_t out_len = 0;
      replace_utf8_utf8_utf8(ctx_ptr, tp, tlen, fp, flen, op, olen, &out_len);
      ctx.Reset();
    };
    auto run_old = [&]() {
      int32_t out_len = 0;
      replace_with_max_len_utf8_utf8_utf8(ctx_ptr, tp, tlen, fp, flen, op, olen,
                                          out_capacity, &out_len);
      ctx.Reset();
    };

    // Warm up and verify correctness. Check has_error()/out_len BEFORE Reset(),
    // since Reset() clears the error and the timed lambdas reset internally.
    int32_t new_len = 0;
    replace_utf8_utf8_utf8(ctx_ptr, tp, tlen, fp, flen, op, olen, &new_len);
    ASSERT_FALSE(ctx.has_error()) << c.name << ": " << ctx.get_error();
    ctx.Reset();
    int32_t old_len = 0;
    replace_with_max_len_utf8_utf8_utf8(ctx_ptr, tp, tlen, fp, flen, op, olen,
                                        out_capacity, &old_len);
    ASSERT_FALSE(ctx.has_error()) << c.name << ": " << ctx.get_error();
    ctx.Reset();
    ASSERT_EQ(new_len, old_len) << c.name << ": new/old output lengths differ";

    double new_ns = TimeNsPerCall(run_new, c.iters);
    double old_ns = TimeNsPerCall(run_old, c.iters);
    double delta = old_ns > 0 ? (new_ns - old_ns) / old_ns * 100.0 : 0.0;

    printf("%-28s %10" PRId64 " %9" PRId64 " %12.1f %12.1f %+8.1f%%\n", c.name.c_str(),
           c.text_len, matches, new_ns, old_ns, delta);
  }
  printf("\n");
}

}  // namespace gandiva
