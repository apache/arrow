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

// Common-prefix extraction ("+") for string codecs, the shared core behind
// FSST+ and OnPair+ in the benchmark harness.
//
// Algorithm: Y. L. Alexandre, "FSST+: Enhancing String Compression Through
// Common Prefix Extraction," MSc thesis, CWI, 2025. Within a sorted block of at
// most 128 strings, an optimal set of "similarity chunks" (ranges sharing a
// prefix) is chosen by dynamic programming (thesis sec 5.2.2): each shared
// prefix is stored once and every string keeps a 1-byte prefix length, an
// optional 2-byte jump-back offset, and its own suffix. The DP minimises total
// stored bytes and runs in O(B^2) per block (B = 128, constant), hence O(N)
// overall.
//
// This header exposes only the codec-agnostic cleaving decision. FSST+ runs it
// over FSST-compressed bytes (with `guard_escape255` so a prefix never splits an
// FSST escape from its literal); OnPair+ runs it over raw bytes. The two codecs'
// assembly and size accounting live in the benchmark, next to the other codecs.
//
// Little-endian hosts only.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace parquet::prefix_plus {

/// Strings per block, and the window within which a prefix may be shared. The
/// thesis picks 128 (sec 5.4.1): a block fits in L1 and the O(B^2) DP stays cheap.
constexpr size_t kBlockSize = 128;

/// Longest shareable prefix. Bounded by the u8 that stores a string's prefix
/// length in the block (thesis sec 3.1).
constexpr size_t kMaxPrefix = 255;

/// The chosen cleaving for a whole sorted collection. Indices are positions in
/// the sorted collection (0..n).
struct Cleaving {
  /// prefix_len[i] = bytes string i shares with (and borrows from) its chunk.
  std::vector<uint32_t> prefix_len;
  /// chunk_first[i] = index of the first string of string i's chunk; its first
  /// prefix_len[i] bytes are the shared prefix, stored once for the chunk.
  std::vector<uint32_t> chunk_first;
  /// Number of similarity chunks with a non-empty shared prefix (diagnostic:
  /// equals the count of prefixes actually stored).
  size_t num_prefix_chunks = 0;
};

/// Run the thesis DP (sec 5.2.2) over the sorted collection `strs`/`lens`
/// (length n), independently per block of kBlockSize. `max_prefix` caps prefix
/// length (<= kMaxPrefix). When `guard_escape255` is set, a candidate prefix is
/// trimmed so it never ends between an FSST escape byte (255) and the literal it
/// escapes -- required when cleaving FSST-compressed bytes, and a no-op for raw
/// bytes. The collection must already be sorted so that strings sharing a prefix
/// are adjacent.
Cleaving CleaveSorted(const uint8_t* const* strs, const size_t* lens, size_t n,
                      size_t max_prefix, bool guard_escape255);

}  // namespace parquet::prefix_plus
