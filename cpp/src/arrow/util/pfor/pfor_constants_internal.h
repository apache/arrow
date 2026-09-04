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

#include <bit>
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
  using PositionType = uint16_t;

  /// Type used to store the number of exceptions in a compressed vector.
  ///
  /// Unsigned: a vector holds up to 2^kMaxLogVectorSize elements and every one
  /// of them can be an exception, so a count of 32768 has to be representable.
  using ExceptionCountType = uint16_t;

  /// Largest vector the format allows, in elements.
  static constexpr int32_t kMaxVectorSize = 1 << kMaxLogVectorSize;

  /// Page header size in bytes: packing_mode, log_vector_size and
  /// value_byte_width one byte each, then num_elements. Derived from the field
  /// types rather than written out, so that widening a field cannot leave the
  /// constant behind; StoreHeader and LoadHeader are the two readers of it.
  static constexpr int64_t kHeaderSize =
      sizeof(uint8_t) + sizeof(uint8_t) + sizeof(uint8_t) + sizeof(int32_t);
};

/// \brief Layout of the bit-packed payload, recorded in the page header.
///
/// One byte per page, not per vector: a reader picks its unpacking kernel once
/// and the choice is uniform for the page, which is also what makes a reader
/// that does not implement a layout able to reject the page up front instead of
/// misreading it.
enum class PackingMode : uint8_t {
  /// Frame of reference plus a sequential little-endian bit-packed stream --
  /// the layout every Parquet implementation already has, expanded here by
  /// arrow::internal::unpack. Values of a vector occupy consecutive bit
  /// positions in stream order.
  kForBitPack = 0,

  /// Frame of reference plus the lane-interleaved container from FastLanes
  /// (Afroozeh & Boncz, VLDB '23), packed by arrow::util::fastlanes. Same
  /// payload size as kForBitPack and the same value order on the way out; the
  /// bits of one vector are distributed across 32 lanes so that a register load
  /// brings in values the unpacker needs together.
  ///
  /// Only legal for a page whose vector size is exactly the interleaved block
  /// size (1024) and whose values are 4 bytes wide. A vector shorter than a
  /// full block -- which is to say the tail vector of a page -- uses
  /// kForBitPack, so a page mixes the two, and the decoder derives which is
  /// which from the element count rather than reading a per-vector flag.
  kForBitPackInterleaved = 1,
};

/// \brief Whether `mode` names a layout this build can read and write.
inline bool IsSupportedPackingMode(uint8_t mode) {
  return mode == static_cast<uint8_t>(PackingMode::kForBitPack) ||
         mode == static_cast<uint8_t>(PackingMode::kForBitPackInterleaved);
}

/// \brief Type traits for PFOR integer types
template <typename T>
struct PforTypeTraits {};

template <>
struct PforTypeTraits<int32_t> {
  using UnsignedType = uint32_t;
  static constexpr uint8_t kMaxBitWidth = 32;
  /// PforVectorInfo size: frame of reference, bit-width byte, exception count.
  static constexpr int64_t kVectorInfoSize =
      sizeof(int32_t) + sizeof(uint8_t) + sizeof(PforConstants::ExceptionCountType);

  static uint8_t BitsRequired(uint32_t value) {
    return static_cast<uint8_t>(std::bit_width(value));
  }
};

template <>
struct PforTypeTraits<int64_t> {
  using UnsignedType = uint64_t;
  static constexpr uint8_t kMaxBitWidth = 64;
  /// PforVectorInfo size: frame of reference, bit-width byte, exception count.
  static constexpr int64_t kVectorInfoSize =
      sizeof(int64_t) + sizeof(uint8_t) + sizeof(PforConstants::ExceptionCountType);

  static uint8_t BitsRequired(uint64_t value) {
    return static_cast<uint8_t>(std::bit_width(value));
  }
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
