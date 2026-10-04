// Tests for the narrow end of the dictionary-budget ladder.
//
// Seeding only the byte values a column uses is what makes a 256-code budget usable
// at all, and it has a second consequence worth pinning down: the encoding needs no
// escape mechanism, at any budget. A byte alphabet holds at most 256 values and the
// narrowest budget has exactly 256 codes, so every occurring byte always gets a
// token. One code always means one token, which is what keeps a row reachable
// without decoding its neighbours -- so these check that property directly, on the
// worst case for it, rather than inferring it from a corpus that never gets close.
//
//   g++ -std=c++17 -O3 -I<arrow>/cpp/src narrow_rung_test.cc onpair.cc -o /tmp/narrow_test
#include <cstdio>
#include <cstring>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "parquet/onpair/onpair.h"

namespace op = parquet::onpair;

namespace {

int g_failures = 0;

void Check(bool ok, const std::string& what) {
  if (!ok) {
    std::printf("  FAIL  %s\n", what.c_str());
    ++g_failures;
  }
}

size_t IndexBits(size_t count) {
  return count <= 1 ? 1 : (64 - static_cast<size_t>(__builtin_clzll(count - 1)));
}

struct Result {
  size_t tokens = 0, width = 0, codes = 0;
};

// Decodes the whole column, then decodes single rows in isolation. The second part
// is the one that fails if a code ever stops meaning exactly one token.
Result Roundtrip(const std::vector<uint8_t>& data, const std::vector<uint32_t>& offs, uint8_t bits,
                 bool prune, const std::string& name) {
  op::Config cfg;
  cfg.max_dict_bits = bits;
  cfg.prune_absent_literals = prune;
  op::Column col = op::Compress(data.data(), data.size(), offs.data(), offs.size() - 1, cfg);

  Result r{col.dict.num_tokens(), IndexBits(col.dict.num_tokens()), col.codes.size()};
  std::vector<uint32_t> cw(col.codes.begin(), col.codes.end());
  std::vector<uint8_t> packed = op::PackValues(cw.data(), cw.size(), r.width);

  std::vector<uint8_t> out(data.size() + op::kDecodePadding + 64, 0xAA);
  size_t w = op::DecompressPacked(col.dict, packed.data(), col.codes.size(), r.width, out.data());
  Check(w == data.size(),
        name + ": decoded " + std::to_string(w) + " bytes, expected " + std::to_string(data.size()));
  Check(w == data.size() && std::memcmp(out.data(), data.data(), data.size()) == 0,
        name + ": bytes differ");

  size_t nrows = col.num_rows();
  size_t step = nrows > 64 ? nrows / 64 : 1;
  for (size_t k = 0; k + 1 < col.row_offsets.size(); k += step) {
    size_t first = col.row_offsets[k], last = col.row_offsets[k + 1];
    if (last == first) continue;
    std::vector<uint32_t> sub(cw.begin() + first, cw.begin() + last);
    std::vector<uint8_t> subpacked = op::PackValues(sub.data(), sub.size(), r.width);
    size_t rowlen = offs[k + 1] - offs[k];
    std::vector<uint8_t> rowout(rowlen + op::kDecodePadding + 64, 0xBB);
    size_t rw = op::DecompressPacked(col.dict, subpacked.data(), last - first, r.width,
                                     rowout.data());
    Check(rw == rowlen && std::memcmp(rowout.data(), data.data() + offs[k], rowlen) == 0,
          name + ": row " + std::to_string(k) + " does not decode on its own");
  }
  return r;
}

size_t DistinctBytes(const std::vector<uint8_t>& d) {
  return std::set<uint8_t>(d.begin(), d.end()).size();
}

}  // namespace

int main() {
  std::printf("narrow-rung tests\n");

  {
    // The worst case for the narrowest budget: every byte value occurs, so the
    // literals fill all 256 codes and no pair can be admitted. It has to encode
    // anyway, with one code per byte and nothing gained.
    std::vector<uint8_t> data;
    std::vector<uint32_t> offs{0};
    std::mt19937_64 rng(7);
    for (int b = 0; b < 256; ++b) {
      for (int r = 0; r < 3; ++r) data.push_back(static_cast<uint8_t>(b));
      offs.push_back(static_cast<uint32_t>(data.size()));
    }
    for (int i = 0; i < 4000; ++i) {
      size_t len = 4 + (rng() % 40);
      for (size_t j = 0; j < len; ++j) {
        uint64_t v = rng();
        data.push_back(static_cast<uint8_t>((v % 100 < 90) ? ('a' + (v >> 8) % 6)
                                                           : (v >> 16) % 256));
      }
      offs.push_back(static_cast<uint32_t>(data.size()));
    }
    Check(DistinctBytes(data) == 256, "setup: expected all 256 byte values");

    Result r8 = Roundtrip(data, offs, 8, true, "all-256-bytes budget 8");
    Check(r8.tokens == 256, "all-256-bytes budget 8: expected 256 tokens, got " +
                                std::to_string(r8.tokens));
    Check(r8.width == 8, "all-256-bytes budget 8: expected an 8-bit code");
    Check(r8.codes == data.size(), "all-256-bytes budget 8: expected one code per byte");
    std::printf("  all 256 byte values at budget 8: %zu tokens, %zu-bit code, %zu codes for %zu "
                "bytes\n", r8.tokens, r8.width, r8.codes, data.size());

    // The same data at wider budgets has room for pairs, and must still round-trip.
    for (uint8_t b : {9, 12, 16}) {
      Result r = Roundtrip(data, offs, b, true, "all-256-bytes budget " + std::to_string(b));
      Check(r.codes < data.size(), "budget " + std::to_string(b) + ": no pair was admitted");
    }
  }

  {
    // A column whose alphabet is far smaller than the budget: pruning should take
    // the code width below 8, which a fully-resident dictionary can never do.
    std::vector<uint8_t> data;
    std::vector<uint32_t> offs{0};
    const char* modes[] = {"AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB", "REG AIR"};
    std::mt19937_64 rng(11);
    for (int i = 0; i < 20000; ++i) {
      const char* m = modes[rng() % 7];
      data.insert(data.end(), m, m + std::strlen(m));
      offs.push_back(static_cast<uint32_t>(data.size()));
    }
    Result pruned = Roundtrip(data, offs, 8, true, "small alphabet, pruned");
    Result full = Roundtrip(data, offs, 9, false, "small alphabet, fully resident");
    Check(pruned.width < 8, "pruning did not take the width below 8 (got " +
                                std::to_string(pruned.width) + ")");
    Check(full.width >= 9, "full residency should floor the width at 9 here");
    std::printf("  %zu distinct bytes: pruned to %zu tokens / %zu-bit code, "
                "fully resident %zu tokens / %zu-bit code\n",
                DistinctBytes(data), pruned.tokens, pruned.width, full.tokens, full.width);
  }

  {
    // Degenerate shapes at the narrowest budget.
    Roundtrip({'x'}, {0, 1}, 8, true, "single byte");
    Roundtrip({'q', 'q', 'q'}, {0, 0, 0, 3, 3}, 8, true, "empty rows around content");
  }

  {
    // Pruning changes the dictionary, never the bytes that come back out.
    std::vector<uint8_t> data;
    std::vector<uint32_t> offs{0};
    std::mt19937_64 rng(99);
    const char* words[] = {"alpha", "beta", "gamma", "delta", "epsilon"};
    for (int i = 0; i < 3000; ++i) {
      for (int j = 0; j < 4; ++j) {
        const char* w = words[rng() % 5];
        data.insert(data.end(), w, w + std::strlen(w));
      }
      offs.push_back(static_cast<uint32_t>(data.size()));
    }
    for (uint8_t b = 8; b <= 16; ++b) {
      Roundtrip(data, offs, b, true, "text pruned budget " + std::to_string(b));
      if (b >= 9) Roundtrip(data, offs, b, false, "text resident budget " + std::to_string(b));
    }
  }

  std::printf("%s (%d failures)\n", g_failures == 0 ? "PASS" : "FAIL", g_failures);
  return g_failures == 0 ? 0 : 1;
}
