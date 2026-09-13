// Does calling unpack_bias once per 1024-value block handicap the sequential arm?
//
// fl5_corpus calls it per block, because with real data every block picks its own
// width and its own frame-of-reference minimum, so a page decoder has no choice.
// But Arrow's own pfor.cc measures faster than that, and if the difference is
// per-call overhead rather than the layout, then fl5's sequential column is unfair
// and its headline ratio is inflated. Three arms, same bytes, same width:
//
//   per_block   one unpack_bias call per 1024 values, runtime width  (= fl5)
//   whole_buf   one unpack_bias call for the entire buffer, runtime width
//   per_blk_ct  one call per block but width a compile-time constant via
//               unpack_jump<KernelDefault>, isolating dispatch from granularity
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <random>
#include <vector>
#include <functional>
#include <xsimd/xsimd.hpp>
#include <arrow/util/bpacking_internal.h>
#include "arrow/util/bpacking_dispatch_internal.h"
#include "arrow/util/bpacking_simd_internal.h"
#include "arrow/util/bpacking_simd_kernel_internal.h"

namespace bp = arrow::internal::bpacking;
using Clock = std::chrono::steady_clock;
template <typename U, int W>
using KernelDefault = bp::Kernel<U, W, xsimd::default_arch>;

template <uint32_t W>
static void Run(size_t n, double* a, double* b, double* c) {
  std::mt19937 rng(7 + W);
  const uint32_t span = (W == 32) ? 0xFFFFFFFFu : ((1u << W) - 1);
  std::vector<uint32_t> vals(n);
  for (auto& v : vals) v = rng() & span;
  std::vector<uint8_t> packed(n * W / 8 + 64, 0);
  {
    uint64_t acc = 0; int bits = 0; uint8_t* p = packed.data();
    for (size_t i = 0; i < n; ++i) {
      acc |= static_cast<uint64_t>(vals[i]) << bits; bits += W;
      while (bits >= 8) { *p++ = static_cast<uint8_t>(acc); acc >>= 8; bits -= 8; }
    }
    if (bits) *p = static_cast<uint8_t>(acc);
  }
  void* raw; if (posix_memalign(&raw, 4096, n * 4 + 4096) != 0) abort();
  uint32_t* out = static_cast<uint32_t*>(raw);

  arrow::internal::UnpackOptions blk, all;
  blk.batch_size = 1024; blk.bit_width = W;
  all.batch_size = static_cast<int64_t>(n); all.bit_width = W;

  auto per_block = [&] {
    for (size_t i = 0; i < n; i += 1024)
      arrow::internal::unpack_bias<uint32_t>(packed.data() + i * W / 8, out + i, blk, 5u);
  };
  auto whole_buf = [&] {
    arrow::internal::unpack_bias<uint32_t>(packed.data(), out, all, 5u);
  };
  auto per_blk_ct = [&] {
    for (size_t i = 0; i < n; i += 1024)
      bp::unpack_jump<KernelDefault, true>(packed.data() + i * W / 8, out + i, blk, 5u);
  };

  const size_t iters = std::max<size_t>(3, (1024u * 1024 * 1024) / (n * 4));
  double best[3] = {0, 0, 0};
  for (int rep = 0; rep < 5; ++rep) {
    int k = 0;
    for (auto fn : {std::function<void()>(per_block), std::function<void()>(whole_buf),
                    std::function<void()>(per_blk_ct)}) {
      fn();
      auto t0 = Clock::now();
      for (size_t it = 0; it < iters; ++it) fn();
      double s = std::chrono::duration<double>(Clock::now() - t0).count();
      best[k] = std::max(best[k], (double)iters * n * 4 / s / (1 << 30));
      ++k;
    }
  }
  free(raw);
  *a = best[0]; *b = best[1]; *c = best[2];
}

int main() {
  struct P { const char* nm; size_t n; };
  const P pts[] = {{"L1", 4096}, {"L2", 102400}, {"DRAM", 8u << 20}};
  printf("%-5s %2s | %9s %9s %10s | %8s %8s\n", "point", "W", "per_block", "whole_buf",
         "per_blk_ct", "whole/blk", "ct/blk");
  for (const P& p : pts) {
    double g[3] = {1, 1, 1}; int n = 0; double a, b, c;
#define R(W) Run<W>(p.n, &a, &b, &c); \
    printf("%-5s %2d | %9.1f %9.1f %10.1f | %7.2fx %7.2fx\n", p.nm, W, a, b, c, b/a, c/a); \
    g[0]*=a; g[1]*=b; g[2]*=c; ++n;
    R(1) R(4) R(7) R(11) R(12) R(18) R(20) R(24) R(31)
#undef R
    double m0=std::pow(g[0],1.0/n), m1=std::pow(g[1],1.0/n), m2=std::pow(g[2],1.0/n);
    printf("%-5s gm | %9.1f %9.1f %10.1f | %7.2fx %7.2fx\n\n", p.nm, m0, m1, m2,
           m1/m0, m2/m0);
  }
  return 0;
}
