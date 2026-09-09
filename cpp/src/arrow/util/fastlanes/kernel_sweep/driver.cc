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

// Three-arm kernel sweep: portable sequential unpack (arm 1, this TU) vs.
// the in-tree interleaved kernel (arm 2, this TU) vs. Arrow's dispatched
// unpacker (arm 3, prebuilt in libarrow, called through its real header).
//
// Arm 1's object code (seq_unpack.o) and arm 3's object code (libarrow.so)
// are both fixed ahead of time and linked in unchanged. Only this TU, and
// so only arm 2, recompiles when the driver is rebuilt at a different
// optimization level -- matching how the sweep isolates what changes
// between its -O2 and -O3 rows.
//
// Not wired into the CMake build: perf_event_open is Linux-only and needs
// a container/host that allows self-process hardware counters (this one
// rejects perf_event_attr.exclude_idle=1 with EOPNOTSUPP but otherwise
// allows PERF_TYPE_HARDWARE; if counters come back unavailable elsewhere,
// that flag is the first thing to check). Build and run from a shared
// Arrow build directory <build> with libarrow already built there:
//
//   g++ -std=c++20 -O2 -DNDEBUG -march=armv8-a -ftree-vectorize \
//       -fno-semantic-interposition -I <arrow>/cpp/src -c seq_unpack.cc \
//       -o seq_unpack_o2.o
//
//   g++ -std=c++20 -O2 -DNDEBUG -march=armv8-a -ftree-vectorize \
//       -fno-semantic-interposition -I <arrow>/cpp/src driver.cc \
//       seq_unpack_o2.o -L <build>/release -larrow \
//       -Wl,-rpath,<build>/release -o driver_o2
//
//   g++ -std=c++20 -O3 -DNDEBUG -march=armv8-a -ftree-vectorize \
//       -fno-semantic-interposition -I <arrow>/cpp/src driver.cc \
//       seq_unpack_o2.o -L <build>/release -larrow \
//       -Wl,-rpath,<build>/release -o driver_o3
//
// seq_unpack_o2.o and libarrow stay fixed across both links; only this TU
// (arm 2) is recompiled at each level.
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <random>
#include <utility>
#include <vector>

#include "arrow/util/bpacking_internal.h"
#include "arrow/util/fastlanes/fastlanes_kernels_internal.h"
#include "perf_counters.h"
#include "seq_unpack.h"

namespace fl = arrow::util::fastlanes;

static constexpr int kBlocks = 16;
static constexpr int N = kBlocks * 1024;
static constexpr int kIters = 200;
static constexpr int kReps = 15;

struct Sample {
  double instr_per_value;
  double ipc;
};

template <class Fn>
static Sample measure(PerfCounters* perf, Fn&& fn) {
  fn();  // warm up: fault in pages, prime branch predictors
  std::vector<double> instr_per_value(kReps), ipc(kReps);
  for (int r = 0; r < kReps; ++r) {
    uint64_t instructions = 0, cycles = 0;
    perf->start();
    for (int it = 0; it < kIters; ++it) fn();
    perf->stop(&instructions, &cycles);
    instr_per_value[r] = static_cast<double>(instructions) / kIters / N;
    ipc[r] = cycles ? static_cast<double>(instructions) / static_cast<double>(cycles) : 0.0;
  }
  std::sort(instr_per_value.begin(), instr_per_value.end());
  std::sort(ipc.begin(), ipc.end());
  return {instr_per_value[kReps / 2], ipc[kReps / 2]};
}

template <uint32_t w>
static void run(PerfCounters* perf) {
  std::mt19937 rng(w * 2654435761u);
  const uint32_t mask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);
  std::uniform_int_distribution<uint32_t> dist(0, mask);
  std::vector<uint32_t> in(N);
  for (auto& x : in) x = dist(rng);

  std::vector<uint32_t> pk_seq(static_cast<size_t>(N) * w / 32 + 8, 0);
  std::vector<uint32_t> pk_int(static_cast<size_t>(kBlocks) * w * fl::kLanes, 0);
  kSeqPack[w](in.data(), pk_seq.data(), N);
  for (int b = 0; b < kBlocks; ++b) {
    fl::PackBlock<w>(in.data() + b * 1024, pk_int.data() + static_cast<size_t>(b) * w * fl::kLanes);
  }

  std::vector<uint32_t> out_seq(N), out_int(N), out_arrow(N);

  auto unpack_seq_call = [&] { kSeqUnpack[w](pk_seq.data(), out_seq.data(), N); };
  auto unpack_int_call = [&] {
    for (int b = 0; b < kBlocks; ++b) {
      fl::UnpackBlock<w>(pk_int.data() + static_cast<size_t>(b) * w * fl::kLanes,
                          out_int.data() + b * 1024);
    }
  };
  auto unpack_arrow_call = [&] {
    arrow::internal::unpack<uint32_t>(reinterpret_cast<const uint8_t*>(pk_seq.data()),
                                       out_arrow.data(),
                                       arrow::internal::UnpackOptions{N, static_cast<int>(w), 0, -1});
  };

  // Correctness gates: each arm reproduces the input, and all three agree.
  unpack_seq_call();
  unpack_int_call();
  unpack_arrow_call();
  const bool ok_seq = std::memcmp(out_seq.data(), in.data(), N * sizeof(uint32_t)) == 0;
  const bool ok_int = std::memcmp(out_int.data(), in.data(), N * sizeof(uint32_t)) == 0;
  const bool ok_arrow = std::memcmp(out_arrow.data(), in.data(), N * sizeof(uint32_t)) == 0;
  const bool ok_cross = std::memcmp(out_seq.data(), out_int.data(), N * sizeof(uint32_t)) == 0 &&
                         std::memcmp(out_seq.data(), out_arrow.data(), N * sizeof(uint32_t)) == 0;

  const Sample seq = measure(perf, unpack_seq_call);
  const Sample intl = measure(perf, unpack_int_call);
  const Sample arw = measure(perf, unpack_arrow_call);

  printf("%2u  %6.2f %5.2f  %6.2f %5.2f  %6.2f %5.2f  %s%s%s%s\n", w, seq.instr_per_value, seq.ipc,
         intl.instr_per_value, intl.ipc, arw.instr_per_value, arw.ipc, ok_seq ? "" : " SEQ-BAD",
         ok_int ? "" : " INT-BAD", ok_arrow ? "" : " ARROW-BAD", ok_cross ? "" : " CROSS-BAD");
}

template <uint32_t... Ws>
static void run_all(PerfCounters* perf, std::integer_sequence<uint32_t, Ws...>) {
  (run<Ws + 1>(perf), ...);
}

int main() {
  PerfCounters perf;
  if (!perf.ok()) {
    std::fprintf(stderr, "hardware counters unavailable, aborting\n");
    return 1;
  }
  printf(" w   seq(i/v) ipc   intlv(i/v) ipc   arrow(i/v) ipc\n");
  run_all(&perf, std::make_integer_sequence<uint32_t, 32>{});
  return 0;
}
