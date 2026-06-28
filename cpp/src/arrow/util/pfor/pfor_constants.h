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

// Constants for PFOR (Patched Frame of Reference) compression

#pragma once

#include <cstdint>
#include <type_traits>

namespace arrow {
namespace util {
namespace pfor {

/// \brief Constants used throughout PFOR compression
class PforConstants {
 public:
  /// Number of elements compressed together as a unit.
  static constexpr int64_t kPforVectorSize = 1024;

  /// log2(kPforVectorSize)
  static constexpr uint8_t kDefaultLogVectorSize = 10;

  /// Minimum allowed log vector size
  static constexpr uint8_t kMinLogVectorSize = 3;

  /// Maximum allowed log vector size
  static constexpr uint8_t kMaxLogVectorSize = 15;

  /// Type used to store vector data offsets (supports pages up to 4GB)
  using OffsetType = uint32_t;

  /// Type used to store exception positions within a compressed vector.
  using PositionType = int16_t;

  /// Page header size in bytes.
  static constexpr int64_t kHeaderSize = 7;

  /// Packing mode: FOR + bit-packing (currently the only mode).
  static constexpr uint8_t kPackingModeForBitPack = 0;

  /// Loop unroll factor for compiler hints in decode loops.
  static constexpr int64_t kLoopUnrolls = 4;
};

/// \brief Per-vector packing mode for PforCompression::EncodeVector.
///
/// Stored in the high bit of PforVectorInfo::bit_width (bit 7). BitPack = 0
/// preserves the prior on-disk layout so existing buffers continue to
/// round-trip; new values are added with higher numeric codes.
enum class PackingMode : uint8_t {
  /// Sequential little-endian bit-packed stream — the original PFOR layout.
  /// Decoded with arrow::internal::unpack.
  BitPack = 0,

  /// FastLanes lane-interleaved 1024-bit format (Afroozeh & Boncz, VLDB '23).
  /// Auto-vectorizable kernel; only valid when num_elements equals the
  /// FastLanes block size (1024). Falls back to BitPack for shorter vectors.
  FastLanes = 1,
};

/// \brief Output value order requested by the decoder.
///
/// Affects only FastLanes-encoded vectors (BitPack always returns flat).
/// Selecting `Transposed` skips the per-vector FL_ORDER gather and is the
/// only way to take full advantage of the FastLanes layout: the gather is
/// scalar and dominates end-to-end decode cost. Downstream operators must
/// be permutation-aware (work on values in stream order, i.e. apply
/// fromTransposed32(t) when they need the original index).
enum class OutputOrder : uint8_t {
  /// Default. Decoded values appear in their original input order:
  /// output[i] == input[i] for every i, for both PackingMode variants.
  Flat = 0,

  /// FastLanes transposed (stream) order. For FastLanes-encoded vectors,
  /// output[t] == input[fromTransposed32(t)] (per 1024-block, no scatter
  /// on decode). For BitPack-encoded vectors there is no permutation, so
  /// they still produce flat output.
  Transposed = 1,
};

/// \brief Type traits for PFOR integer types
template <typename T>
struct PforTypeTraits {};

template <>
struct PforTypeTraits<int32_t> {
  using UnsignedType = uint32_t;
  static constexpr uint8_t kMaxBitWidth = 32;
  static constexpr uint8_t kValueByteWidth = 4;

  /// PforVectorInfo size: 4B FOR + 1B bitWidth + 2B numExceptions = 7 bytes
  static constexpr int64_t kVectorInfoSize = 7;

  static uint8_t BitsRequired(uint32_t value) {
    if (value == 0) return 0;
    return static_cast<uint8_t>(32 - __builtin_clz(value));
  }
};

template <>
struct PforTypeTraits<int64_t> {
  using UnsignedType = uint64_t;
  static constexpr uint8_t kMaxBitWidth = 64;
  static constexpr uint8_t kValueByteWidth = 8;

  /// PforVectorInfo size: 8B FOR + 1B bitWidth + 2B numExceptions = 11 bytes
  static constexpr int64_t kVectorInfoSize = 11;

  static uint8_t BitsRequired(uint64_t value) {
    if (value == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(value));
  }
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
