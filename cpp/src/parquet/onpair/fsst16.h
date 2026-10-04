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

// Benchmark-only FSST trainer for a 16-bit code space. The vendored FSST trainer
// uses a dense pair-frequency matrix and 64-bit symbols, so widening it would
// require prohibitive memory and would retain an eight-byte symbol limit. This
// implementation instead uses sparse pair counts and explicit byte arrays.
//
// Codes 0 through 255 represent literal bytes; higher codes represent learned
// symbols. The trainer follows FSST's progressive sampling and scoring, but it
// bounds-checks matches, uses deterministic shorter-first lexicographic tie
// breaking, and does not renumber fixed-width codes. It returns a token list so
// comparisons can share the same tokenizer and decoder.
//
// Reference: P. Boncz, T. Neumann, V. Leis, "FSST: Fast Random Access String
// Compression", VLDB 2020.
//
// Little-endian hosts only.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace parquet::fsst16 {

/// Longest symbol the reference can represent, and the cap this trainer allows
/// as an upper bound on `Config::max_symbol_len`.
constexpr size_t kMaxSymbolLen = 16;

struct Config {
  /// Longest symbol the trainer may build. 8 is the reference's own cap and
  /// isolates the effect of the wider code; 16 removes that cap so the only
  /// remaining difference from a 16-byte dictionary codec is the training.
  int max_symbol_len = 8;
  /// Bytes of the column the trainer looks at. The reference's default is 16
  /// KiB regardless of column size.
  size_t sample_target = size_t{1} << 14;
  /// Table ceiling, counting the 256 resident literals.
  size_t max_symbols = size_t{1} << 16;
  /// Sample-selection seed. The reference's constant.
  uint64_t seed = 4637947;
};

/// A trained table as a flat token list: the 256 literals in code order first,
/// then the learned symbols. Token id is the code.
struct Tokens {
  std::vector<uint8_t> bytes;
  std::vector<uint32_t> offsets;  // length num_tokens + 1

  size_t num_tokens() const { return offsets.empty() ? 0 : offsets.size() - 1; }
};

/// Train a table against (bytes, offsets). `offsets` has length num_rows + 1;
/// row i is bytes[offsets[i]..offsets[i+1]].
Tokens Train(const uint8_t* bytes, const uint32_t* offsets, size_t num_rows,
             const Config& cfg);

}  // namespace parquet::fsst16
