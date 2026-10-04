// SelectBudget is a pure function over trained candidates, so its behaviour can be
// pinned without training or timing anything. These are the properties the rest of
// the design leans on.
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <limits>
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

op::BudgetCandidate Cand(uint8_t budget, uint32_t tokens, uint64_t codes, uint64_t stored) {
  op::BudgetCandidate c;
  c.budget = budget;
  c.code_width = budget;
  c.num_tokens = tokens;
  c.num_codes = codes;
  c.stored_bytes = stored;
  return c;
}

size_t Pick(const std::vector<op::BudgetCandidate>& c, double cap) {
  op::SelectionPolicy p;
  p.max_decode_regression = cap;
  return op::SelectBudget(c.data(), c.size(), p);
}

// The shape every real column has: as the budget widens the dictionary grows and
// the code count falls, and stored size bottoms out somewhere in between.
std::vector<op::BudgetCandidate> Ladder() {
  return {
      Cand(8, 256, 1600, 1000),    // smallest, most codes
      Cand(9, 512, 1470, 1010),
      Cand(10, 1024, 1372, 1040),
      Cand(13, 8192, 1092, 1100),
      Cand(16, 65536, 1010, 1300),  // fewest codes, biggest
  };
}

void TestEnds() {
  auto l = Ladder();
  Check(Pick(l, std::numeric_limits<double>::infinity()) == 0,
        "an infinite cap ignores decode and takes the smallest");
  // Not the fewest codes: index 4 has 1010 of them but a saturated dictionary, and
  // the per-code penalty for that outweighs the 8% code saving over index 3. The
  // cheapest rung sits inside the ladder, which is the whole reason the estimate
  // carries a dictionary term at all.
  Check(Pick(l, 0.0) == 3, "a zero cap takes the cheapest predicted rung, not the narrowest stream");
  // A cap must never turn into a NaN comparison that admits or rejects everything
  // by accident, which is the failure mode of writing best * (1 + inf).
  Check(Pick(l, std::numeric_limits<double>::infinity()) != l.size(),
        "an infinite cap still admits candidates");
}

void TestMonotone() {
  auto l = Ladder();
  // Widening the cap can only ever admit more candidates, so the chosen size is
  // non-increasing in the cap. This is what makes the knob safe to tune.
  uint64_t prev = std::numeric_limits<uint64_t>::max();
  for (double cap : {0.0, 0.02, 0.05, 0.10, 0.20, 0.50, 1.0, 10.0}) {
    uint64_t bytes = l[Pick(l, cap)].stored_bytes;
    Check(bytes <= prev, "chosen size is non-increasing as the cap widens");
    prev = bytes;
  }
}

void TestParetoAndTies() {
  // Same size, different decode cost: the cheaper decode must win, or the selector
  // would return a dominated candidate.
  std::vector<op::BudgetCandidate> tie = {Cand(9, 512, 2000, 500), Cand(12, 4096, 1000, 500)};
  Check(tie[Pick(tie, 1.0)].budget == 12, "a size tie breaks toward cheaper decode");

  // One candidate is always admissible: it defines the best cost itself.
  std::vector<op::BudgetCandidate> one = {Cand(11, 2048, 999, 77)};
  Check(Pick(one, 0.0) == 0, "a lone candidate is admitted at any cap");

  Check(op::SelectBudget(nullptr, 0, op::SelectionPolicy{}) == 0, "no candidates returns n");
}

void TestDictionaryTermIsBounded() {
  // The dictionary term is real but bounded: across the entire realizable range,
  // 256 tokens to a saturated 65536, per-code cost rises about 1.42x. Code counts
  // across a ladder routinely vary 2x, so a code saving larger than that bound
  // always wins no matter what it does to the dictionary. This bound is what makes
  // the estimate portable -- a machine with a different cache hierarchy moves the
  // 1.42x, not the ordering of the large code-count differences.
  double per_code_small = op::DecodeCostEstimate(1000, 256) / 1000.0;
  double per_code_saturated = op::DecodeCostEstimate(1000, 65536) / 1000.0;
  double span = per_code_saturated / per_code_small;
  Check(span > 1.2 && span < 1.6, "per-code cost spans 1.2x-1.6x over the whole token range");
  Check(op::DecodeCostEstimate(500, 65536) < op::DecodeCostEstimate(1000, 256),
        "halving the code count beats the worst dictionary penalty");
  // Below the knee the cost is flat, so equal code counts compare equal.
  Check(op::DecodeCostEstimate(1000, 256) == op::DecodeCostEstimate(1000, 1024),
        "dictionaries under the knee cost the same per code");
}

void TestFlatLadder() {
  // The low-cardinality columns train to the same dictionary at every budget. The
  // selector must be indifferent rather than arbitrary: identical candidates mean
  // the first, so the choice is the narrowest budget that achieves it.
  std::vector<op::BudgetCandidate> flat;
  for (uint8_t b = 8; b <= 16; ++b) flat.push_back(Cand(b, 36, 500000, 400000));
  Check(flat[Pick(flat, 0.05)].budget == 8, "an all-equal ladder picks the narrowest budget");
}

}  // namespace

int main() {
  std::printf("selector tests\n");
  TestEnds();
  TestMonotone();
  TestParetoAndTies();
  TestDictionaryTermIsBounded();
  TestFlatLadder();
  std::printf("%s (%d failures)\n", failures == 0 ? "PASS" : "FAIL", failures);
  return failures == 0 ? 0 : 1;
}
