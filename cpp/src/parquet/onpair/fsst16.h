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

// FSST's symbol-table training algorithm, lifted to a 16-bit code space.
//
// Reference: P. Boncz, T. Neumann, V. Leis, "FSST: Fast Random Access String
// Compression", VLDB 2020. The vendored reference implementation trains a
// 255-symbol table addressed by one output byte, with one code reserved to
// escape a literal byte. This trains the same way but over a table addressed by
// two output bytes, which is what a like-for-like comparison against a
// 16-bit-code dictionary codec needs.
//
// WHY THIS IS A SEPARATE IMPLEMENTATION, NOT A PARAMETER
//
// The reference cannot be widened in place. Two of its structures are tied to
// the narrow code space:
//
//   * The pair-frequency counter is a dense code-by-code matrix. It is a few
//     hundred KiB at a 9-bit code space and tens of GiB at a 16-bit one, so a
//     16-bit table needs a sparse counter and a candidate-generation loop that
//     walks occupied entries instead of the full square.
//   * A symbol is stored in a single 64-bit word, which caps it at 8 bytes. A
//     16-byte symbol needs a wider representation, and the byte-at-a-time
//     longest-match index built around that word has to be replaced.
//
// The vendored reference is therefore left untouched, so the 8-bit baseline it
// produces stays exactly what it was.
//
// CODE SPACE
//
// Codes 0..255 are the literal bytes; codes 256 and up are learned symbols.
// This is the natural reading of the reference's own escape mechanism at two
// bytes per code: there, escaping a literal costs a code plus the byte, so the
// trainer works hard to keep literals rare. Here a literal costs one code, the
// same as any symbol, so escapes disappear and the 256 single bytes are simply
// always resident. Every emitted code is the same fixed width, so the output
// size is exactly two bytes times the number of codes, and the trainer's
// objective reduces to emitting as few codes as possible.
//
// TRAINING SHAPE PRESERVED FROM THE REFERENCE
//
// Progressive sampling over five rounds at increasing sample fractions; each
// round compresses the sample with the current table while counting single-
// symbol and adjacent-pair frequencies; candidate symbols are the counted
// symbols plus every counted pair concatenated; a candidate's score is its
// count times its length; single-byte candidates are scored eight times up;
// candidates below a round-scaled minimum count are discarded; the table is
// cleared and refilled from the highest-scoring candidates each round; the
// round with the best measured gain is kept and rebuilt from its own counts at
// the end.
//
// DEVIATIONS, and why each is forced or harmless
//
//   V1. Sparse pair counter. Open-addressed rather than a dense matrix, for the
//       reason above. Counts are 32-bit and do not saturate, where the
//       reference's pair counts saturate at twelve bits. A pair frequent enough
//       to saturate is selected either way, so this can only change the
//       relative order of two already-selected candidates.
//   V2. Candidate generation walks the occupied pair entries rather than nesting
//       a right-code loop inside a left-code loop. The set of candidates is the
//       same; the order in which they are first seen is not, which matters only
//       for ties.
//   V3. No terminator byte. The reference picks the least frequent byte as a
//       terminator, forces it into every table, and refuses to build a
//       multi-byte symbol containing it, so that its match loop can read past
//       the end of a string. This bounds-checks its match loop instead, which
//       removes the special case entirely.
//   V4. Single-byte candidates are scored and ranked but never admitted, since
//       the 256 literals are already resident. The eight-times promotion is kept
//       so the pair-generation path sees the same scores, but it cannot change
//       the table.
//   V5. Ties in candidate score are broken by shorter-first then lexicographic
//       byte order, rather than by the reference's numeric ordering of the
//       symbol's packed word. Both are arbitrary; a total order is all that is
//       needed for a deterministic table.
//   V6. No code renumbering at the end. The reference renumbers so that its
//       most frequent symbols land in the range addressable by a single byte;
//       with a fixed two-byte code, code order cannot affect the output size.
//
// The trained table is emitted as a token list, so the tokenizer and decoder of
// the 16-bit dictionary codec it is being compared against can consume it
// directly. That is deliberate: the parsing pass and the decode pass are then
// literally the same code for both, and every difference that remains is a
// difference in how the table was chosen.
//
// NOT a production encoder - this is a benchmark artifact.
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
