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
  static constexpr uint32_t kPforVectorSize = 1024;

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

  /// Maximum number of exceptions per vector (uint16 limit).
  static constexpr uint16_t kMaxExceptions = 65535;

  /// Page header size in bytes.
  static constexpr uint8_t kHeaderSize = 7;

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
  static constexpr uint8_t kValueByteWidth = 4;

  /// PforVectorInfo size: 4B FOR + 1B bitWidth + 2B numExceptions = 7 bytes
  static constexpr uint8_t kVectorInfoSize = 7;

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
  static constexpr uint8_t kVectorInfoSize = 11;

  static uint8_t BitsRequired(uint64_t value) {
    if (value == 0) return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(value));
  }
};

}  // namespace pfor
}  // namespace util
}  // namespace arrow
