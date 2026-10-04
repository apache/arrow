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

// Tests the selection and re-encoding performed by CompressAuto. The returned
// column must match the selected candidate.
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "parquet/onpair/onpair.h"

namespace op = parquet::onpair;

namespace {

int failures = 0;

void Check(bool ok, const char* what) {
  if (!ok) {
    std::printf("  FAIL %s\n", what);
    ++failures;
  }
}

struct Rows {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets{0};
  void Add(const std::string& s) {
    bytes.insert(bytes.end(), s.begin(), s.end());
    offsets.push_back(static_cast<uint32_t>(bytes.size()));
  }
  size_t n() const { return offsets.size() - 1; }
};

// Enough shared structure that the trainer has pairs worth taking, and enough
// distinct tail that a wide budget can spend more codes than a narrow one.
Rows MakeCorpus() {
  static const char* kHosts[] = {"example.com", "mail.example.org", "shop.acme.io"};
  Rows r;
  uint64_t x = 12345;
  for (int i = 0; i < 60000; ++i) {
    x = x * 6364136223846793005ull + 1442695040888963407ull;
    char buf[96];
    std::snprintf(buf, sizeof(buf), "user_%06u@%s/session/%08x", i % 40000, kHosts[i % 3],
                  static_cast<unsigned>(x >> 32));
    r.Add(buf);
  }
  return r;
}

// Adds framing bytes that are constant across candidates.
uint64_t PlusConstant(const op::CompactDictionary& dict, uint64_t num_codes, void* ctx) {
  return op::VaryingStoredBytes(dict, num_codes) + *static_cast<uint64_t*>(ctx);
}

bool RoundTrips(const op::Column& col, const Rows& r) {
  const size_t width = op::CodeWidth(col.dict.num_tokens());
  std::vector<uint32_t> wide(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(wide.data(), wide.size(), width);
  std::vector<uint8_t> out(op::DecodedLen(col) + r.bytes.size() + op::kDecodePadding, 0);
  const size_t w = op::DecompressPacked(col.dict, packed.data(), packed.size(),
                                        col.codes.size(), width, out.data(), out.size());
  return w == r.bytes.size() && std::memcmp(out.data(), r.bytes.data(), w) == 0;
}

void TestReportMatchesColumn(const Rows& r) {
  op::BudgetSearchOptions opts;
  op::SelectionReport rep;
  op::Column col = op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(),
                                    r.n(), opts, &rep);

  Check(rep.candidates.size() == 9, "the default range has nine candidates");
  Check(rep.chosen < rep.candidates.size(), "the chosen index is in range");
  const op::BudgetCandidate& c = rep.candidates[rep.chosen];
  Check(col.dict.num_tokens() == c.num_tokens,
        "the returned dictionary matches the choice");
  Check(col.codes.size() == c.num_codes, "the returned code stream matches the choice");
  Check(op::CodeWidth(col.dict.num_tokens()) == c.code_width,
        "reported code width is the real one");
  Check(RoundTrips(col, r), "the chosen column round-trips");

  Check(rep.chosen_cost <=
            rep.best_cost * (1.0 + opts.policy.max_decode_regression) * 1.000001,
        "the chosen candidate is inside the cap");
  for (const auto& o : rep.candidates) {
    Check(o.stored_bytes >= rep.candidates[rep.bytes_only].stored_bytes,
          "bytes_only names the smallest candidate");
  }
  Check(rep.distinct_dictionaries <= rep.candidates.size(),
        "deduplication cannot train extra candidates");
  Check(rep.encode_s > 0, "the report times the call");
}

void TestConstantFramingTermIsIrrelevant(const Rows& r) {
  // The library default omits the row-length side array and page headers on the
  // A term equal across candidates cannot change which candidate is smallest.
  // Add a large constant and the pick must not move.
  op::SelectionReport a, b;
  op::BudgetSearchOptions plain;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), plain, &a);

  uint64_t big = 4u << 20;
  op::BudgetSearchOptions shifted = plain;
  shifted.stored_bytes = &PlusConstant;
  shifted.stored_bytes_ctx = &big;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), shifted, &b);

  Check(a.candidates[a.chosen].budget == b.candidates[b.chosen].budget,
        "constant framing does not move the choice");
  Check(a.candidates[a.bytes_only].budget == b.candidates[b.bytes_only].budget,
        "nor does it move what a bytes-only selector would take");
}

void TestPolicyReachesTheEnds(const Rows& r) {
  op::BudgetSearchOptions fast, small;
  fast.policy.max_decode_regression = 0.0;
  small.policy.max_decode_regression = std::numeric_limits<double>::infinity();
  op::SelectionReport f, s;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), fast, &f);
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), small, &s);

  Check(s.candidates[s.chosen].stored_bytes <= f.candidates[f.chosen].stored_bytes,
        "an unbounded cap never stores more than a zero cap");
  Check(f.chosen_cost <= s.chosen_cost,
        "a zero cap never decodes slower than an unbounded one");
  Check(s.chosen == s.bytes_only, "an unbounded cap is the bytes-only selector");
}

void TestSingleRungLadder(const Rows& r) {
  // A single-candidate range must agree with Compress at that budget.
  op::BudgetSearchOptions one;
  one.min_budget = 11;
  one.max_budget = 11;
  op::SelectionReport rep;
  op::Column via_auto = op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(),
                                         r.n(), one, &rep);

  op::Config cfg = one.base;
  cfg.max_dict_bits = 11;
  op::Column direct =
      op::Compress(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), cfg);

  Check(rep.candidates.size() == 1, "a single budget trains one candidate");
  Check(via_auto.dict.bytes == direct.dict.bytes && via_auto.codes == direct.codes,
        "a single budget equals Compress at that budget");
  Check(rep.candidates[0].stored_bytes == op::VaryingStoredBytes(direct),
        "the default stored size is VaryingStoredBytes of that column");
}

void TestBudgetsAreClamped(const Rows& r) {
  op::BudgetSearchOptions wild;
  wild.min_budget = 0;
  wild.max_budget = 200;
  op::SelectionReport rep;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), wild, &rep);
  Check(rep.candidates.size() == 9, "budgets outside 8..16 clamp to the legal range");
  Check(rep.candidates.front().budget == 8 && rep.candidates.back().budget == 16,
        "the clamped range spans 8 to 16");
}

void TestCodeWidth() {
  Check(op::CodeWidth(0) == 1 && op::CodeWidth(1) == 1,
        "an empty or unit dictionary is 1 bit");
  Check(op::CodeWidth(256) == 8, "256 tokens fit in 8 bits");
  Check(op::CodeWidth(257) == 9, "257 do not");
  Check(op::CodeWidth(65536) == 16, "a saturated 16-bit dictionary is 16 bits");
}

}  // namespace

int main() {
  std::printf("CompressAuto tests\n");
  Rows r = MakeCorpus();
  TestCodeWidth();
  TestReportMatchesColumn(r);
  TestConstantFramingTermIsIrrelevant(r);
  TestPolicyReachesTheEnds(r);
  TestSingleRungLadder(r);
  TestBudgetsAreClamped(r);
  std::printf("%s (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
  return failures == 0 ? 0 : 1;
}
