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

// Portable sequential bit-pack/unpack, arm 1 of the three-arm kernel sweep.
//
// Plain scalar bit-packing: value i's word/shift/straddle depend on i alone,
// with no lane structure and no vectorization hint. This is the baseline
// that the interleaved container (arm 2) and Arrow's dispatched unpacker
// (arm 3) are both compared against.
//
// Compiled once, at a fixed level, into its own object file. The driver
// links this .o unchanged into both the -O2 and -O3 builds, so this arm's
// machine code never varies between rows of the sweep.
#include "seq_unpack.h"

#include <cstring>

SeqPackFn kSeqPack[33] = {};
SeqUnpackFn kSeqUnpack[33] = {};

namespace {

template <uint32_t w>
void pack_seq(const uint32_t* in, uint32_t* out, int n) {
  const size_t words = ((size_t)n * w + 31) / 32 + 1;
  std::memset(out, 0, words * 4);
  const uint32_t mask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);
  uint64_t bit = 0;
  for (int i = 0; i < n; ++i) {
    const uint64_t v = in[i] & mask;
    const size_t word = static_cast<size_t>(bit / 32);
    const uint32_t sh = static_cast<uint32_t>(bit % 32);
    out[word] |= static_cast<uint32_t>(v << sh);
    if (sh + w > 32) out[word + 1] |= static_cast<uint32_t>(v >> (32 - sh));
    bit += w;
  }
}

template <uint32_t w>
void unpack_seq(const uint32_t* packed, uint32_t* out, int n) {
  const uint32_t mask = (w == 32) ? 0xFFFFFFFFu : ((1u << w) - 1);
  uint64_t bit = 0;
  for (int i = 0; i < n; ++i) {
    const size_t word = static_cast<size_t>(bit / 32);
    const uint32_t sh = static_cast<uint32_t>(bit % 32);
    uint64_t v;
    if (sh + w <= 32) {
      v = packed[word] >> sh;
    } else {
      const uint64_t lo = packed[word] >> sh;
      const uint64_t hi = static_cast<uint64_t>(packed[word + 1]) << (32 - sh);
      v = lo | hi;
    }
    out[i] = static_cast<uint32_t>(v & mask);
    bit += w;
  }
}

template <uint32_t W>
void register_width() {
  kSeqPack[W] = &pack_seq<W>;
  kSeqUnpack[W] = &unpack_seq<W>;
  if constexpr (W < 32) register_width<W + 1>();
}

struct Installer {
  Installer() { register_width<1>(); }
} kInstaller;

}  // namespace
