// CompressAuto: the selector as the encoder actually runs it.
//
// selector_test.cc pins the pure choice; this pins the wiring around it. The
// property that matters and is easy to get wrong is that the column handed back is
// the rung the report says was chosen -- the ladder is measured in one pass and the
// winner re-encoded in another, so a trainer that were not deterministic would
// return a column the selector never saw.
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
    std::snprintf(buf, sizeof(buf), "user_%06u@%s/session/%08x", i % 40000,
                  kHosts[i % 3], static_cast<unsigned>(x >> 32));
    r.Add(buf);
  }
  return r;
}

// A framing term that is the same for every rung, which is the case the library
// default leaves out on purpose.
uint64_t PlusConstant(const op::CompactDictionary& dict, uint64_t num_codes, void* ctx) {
  return op::VaryingStoredBytes(dict, num_codes) + *static_cast<uint64_t*>(ctx);
}

bool RoundTrips(const op::Column& col, const Rows& r) {
  const size_t width = op::CodeWidth(col.dict.num_tokens());
  std::vector<uint32_t> wide(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(wide.data(), wide.size(), width);
  std::vector<uint8_t> out(op::DecodedLen(col) + r.bytes.size() + op::kDecodePadding, 0);
  const size_t w =
      op::DecompressPacked(col.dict, packed.data(), col.codes.size(), width, out.data());
  return w == r.bytes.size() && std::memcmp(out.data(), r.bytes.data(), w) == 0;
}

void TestReportMatchesColumn(const Rows& r) {
  op::LadderOptions opts;
  op::SelectionReport rep;
  op::Column col = op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), opts,
                                    &rep);

  Check(rep.candidates.size() == 9, "a default ladder has nine rungs");
  Check(rep.chosen < rep.candidates.size(), "the chosen index is in range");
  const op::BudgetCandidate& c = rep.candidates[rep.chosen];
  Check(col.dict.num_tokens() == c.num_tokens, "the returned dictionary is the chosen rung's");
  Check(col.codes.size() == c.num_codes, "the returned code stream is the chosen rung's");
  Check(op::CodeWidth(col.dict.num_tokens()) == c.code_width, "reported code width is the real one");
  Check(RoundTrips(col, r), "the chosen column round-trips");

  Check(rep.chosen_cost <= rep.best_cost * (1.0 + opts.policy.max_decode_regression) * 1.000001,
        "the chosen rung is inside the cap");
  for (const auto& o : rep.candidates) {
    Check(o.stored_bytes >= rep.candidates[rep.bytes_only].stored_bytes,
          "bytes_only names the smallest rung");
  }
  Check(rep.distinct_dictionaries <= rep.candidates.size(), "dedup cannot train extra rungs");
  Check(rep.encode_s > 0, "the report times the call");
}

void TestConstantFramingTermIsIrrelevant(const Rows& r) {
  // The library default omits the row-length side array and page headers on the
  // grounds that a term equal across rungs cannot change which rung is smallest.
  // Add a large constant and the pick must not move.
  op::SelectionReport a, b;
  op::LadderOptions plain;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), plain, &a);

  uint64_t big = 4u << 20;
  op::LadderOptions shifted = plain;
  shifted.stored_bytes = &PlusConstant;
  shifted.stored_bytes_ctx = &big;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), shifted, &b);

  Check(a.candidates[a.chosen].budget == b.candidates[b.chosen].budget,
        "a framing term equal on every rung does not move the choice");
  Check(a.candidates[a.bytes_only].budget == b.candidates[b.bytes_only].budget,
        "nor does it move what a bytes-only selector would take");
}

void TestPolicyReachesTheEnds(const Rows& r) {
  op::LadderOptions fast, small;
  fast.policy.max_decode_regression = 0.0;
  small.policy.max_decode_regression = std::numeric_limits<double>::infinity();
  op::SelectionReport f, s;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), fast, &f);
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), small, &s);

  Check(s.candidates[s.chosen].stored_bytes <= f.candidates[f.chosen].stored_bytes,
        "an unbounded cap never stores more than a zero cap");
  Check(f.chosen_cost <= s.chosen_cost, "a zero cap never decodes slower than an unbounded one");
  Check(s.chosen == s.bytes_only, "an unbounded cap is the bytes-only selector");
}

void TestSingleRungLadder(const Rows& r) {
  // A one-rung ladder has to agree with plain Compress at that budget, which is the
  // check that CompressAuto adds a choice and changes nothing else.
  op::LadderOptions one;
  one.min_budget = 11;
  one.max_budget = 11;
  op::SelectionReport rep;
  op::Column via_auto =
      op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), one, &rep);

  op::Config cfg = one.base;
  cfg.max_dict_bits = 11;
  op::Column direct = op::Compress(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), cfg);

  Check(rep.candidates.size() == 1, "a one-rung ladder trains one rung");
  Check(via_auto.dict.bytes == direct.dict.bytes && via_auto.codes == direct.codes,
        "a one-rung ladder equals Compress at that budget");
  Check(rep.candidates[0].stored_bytes == op::VaryingStoredBytes(direct),
        "the default stored size is VaryingStoredBytes of that column");
}

void TestBudgetsAreClamped(const Rows& r) {
  op::LadderOptions wild;
  wild.min_budget = 0;
  wild.max_budget = 200;
  op::SelectionReport rep;
  op::CompressAuto(r.bytes.data(), r.bytes.size(), r.offsets.data(), r.n(), wild, &rep);
  Check(rep.candidates.size() == 9, "budgets outside 8..16 clamp to the legal ladder");
  Check(rep.candidates.front().budget == 8 && rep.candidates.back().budget == 16,
        "the clamped ladder spans 8 to 16");
}

void TestCodeWidth() {
  Check(op::CodeWidth(0) == 1 && op::CodeWidth(1) == 1, "an empty or unit dictionary is 1 bit");
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
