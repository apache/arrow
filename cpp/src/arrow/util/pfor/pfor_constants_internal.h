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

  /// Packing mode: FOR + bit-packing (currently the only mode).
  static constexpr uint8_t kPackingModeForBitPack = 0;
};

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
