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

// ALP constants and type parameters.

#pragma once

#include <concepts>
#include <cstdint>

#include "arrow/util/logging.h"

namespace arrow::util::alp {

template <typename T>
concept AlpFloatingType = std::same_as<T, float> || std::same_as<T, double>;

// Wire-format constants

/// Compression mode stored in the page header.
enum class AlpMode : uint8_t { kAlp = 0 };

/// Integer encoding applied to decimal-encoded values.
enum class AlpIntegerEncoding : uint8_t { kForBitPack = 0 };

struct AlpFormatConstants {
  /// Recommended default vector size.
  static constexpr int32_t kDefaultVectorSize = 1024;

  /// Minimum log2(vector size), corresponding to 8 elements.
  static constexpr uint8_t kMinLogVectorSize = 3;

  /// Maximum log2(vector size), corresponding to 32768 elements.
  static constexpr uint8_t kMaxLogVectorSize = 15;

  /// Offset type for the vector-offset table.
  using OffsetType = uint32_t;

  /// Exception position type.
  using PositionType = uint16_t;
};

static_assert(sizeof(AlpFormatConstants::OffsetType) == 4);
static_assert(sizeof(AlpFormatConstants::PositionType) == 2);

// Sampling and parameter-search tuning

struct AlpSamplingOptions {
  /// Input elements per sampling chunk.
  static constexpr int64_t kChunkSize = 4096;

  /// Elements per sampling interval.
  static constexpr int64_t kIntervalSize = 122880;

  /// Samples collected per chunk.
  static constexpr int64_t kSamplesPerChunk = 256;

  /// Chunks sampled per interval.
  static constexpr int64_t kSampleChunksPerInterval = 8;

  /// Consecutive non-improving combinations before an early exit.
  static constexpr uint8_t kEarlyExitThreshold = 4;

  /// Maximum exponent-factor combinations retained in a preset.
  static constexpr uint8_t kMaxCombinations = 5;

  static_assert(kMaxCombinations > kEarlyExitThreshold,
                "early exit requires at least one additional candidate");
};

// Power-of-ten constants

/// Correctly rounded powers of ten used by ALP.
struct AlpPowerOfTen {
  /// Returns 10^power as float for power in [-10, 10].
  static float Float(int8_t power) {
    ARROW_DCHECK(power >= -10 && power <= 10)
        << "power out of range: " << static_cast<int>(power);
    static constexpr float kTable[21] = {
        0.0000000001F, 0.000000001F,  0.00000001F,   0.0000001F, 0.000001F,  0.00001F,
        0.0001F,       0.001F,        0.01F,         0.1F,       1.0F,       10.0F,
        100.0F,        1000.0F,       10000.0F,      100000.0F,  1000000.0F, 10000000.0F,
        100000000.0F,  1000000000.0F, 10000000000.0F};
    return kTable[power + 10];
  }

  /// Returns 10^power as double for power in [-20, 20].
  static double Double(int8_t power) {
    ARROW_DCHECK(power >= -20 && power <= 20)
        << "power out of range: " << static_cast<int>(power);
    static constexpr double kTable[41] = {
        0.00000000000000000001,
        0.0000000000000000001,
        0.000000000000000001,
        0.00000000000000001,
        0.0000000000000001,
        0.000000000000001,
        0.00000000000001,
        0.0000000000001,
        0.000000000001,
        0.00000000001,
        0.0000000001,
        0.000000001,
        0.00000001,
        0.0000001,
        0.000001,
        0.00001,
        0.0001,
        0.001,
        0.01,
        0.1,
        1.0,
        10.0,
        100.0,
        1000.0,
        10000.0,
        100000.0,
        1000000.0,
        10000000.0,
        100000000.0,
        1000000000.0,
        10000000000.0,
        100000000000.0,
        1000000000000.0,
        10000000000000.0,
        100000000000000.0,
        1000000000000000.0,
        10000000000000000.0,
        100000000000000000.0,
        1000000000000000000.0,
        10000000000000000000.0,
        100000000000000000000.0,
    };
    return kTable[power + 20];
  }
};

// Type-specific constants

template <typename FloatingPointType>
struct AlpTypedConstants {};

template <>
struct AlpTypedConstants<float> {
  /// Added and subtracted by FastRound() in float precision to round to the
  /// nearest integer.
  static constexpr float kMagicNumber = 12582912.0f;  // 2^22 + 2^23

  static constexpr uint8_t kMaxExponent = 10;

  // One float ULP (unit in the last place) inside the int32_t range.
  // FastRound() may move a value by one ULP, so this headroom prevents an
  // out-of-range conversion.
  static constexpr float kEncodingUpperLimit = 2147483392.0f;  // 2^31 - 2^8
  static constexpr float kEncodingLowerLimit = -2147483392.0f;

  /// Returns 10^power.
  static float GetExponent(uint8_t power) {
    return AlpPowerOfTen::Float(static_cast<int8_t>(power));
  }

  /// Returns 10^(-power).
  static float GetFactor(uint8_t power) {
    return AlpPowerOfTen::Float(static_cast<int8_t>(-static_cast<int8_t>(power)));
  }

  using EncodedUnsigned = uint32_t;
  using EncodedSigned = int32_t;
};

template <>
struct AlpTypedConstants<double> {
  /// Added and subtracted by FastRound() in double precision to round to the
  /// nearest integer.
  static constexpr double kMagicNumber = 6755399441055744.0;  // 2^51 + 2^52

  static constexpr uint8_t kMaxExponent = 18;

  // One double ULP (unit in the last place) inside the int64_t range.
  // FastRound() may move a value by one ULP, so this headroom prevents an
  // out-of-range conversion.
  static constexpr double kEncodingUpperLimit = 9223372036854773760.0;  // 2^63 - 2^11
  static constexpr double kEncodingLowerLimit = -9223372036854773760.0;

  /// Returns 10^power.
  static double GetExponent(uint8_t power) {
    return AlpPowerOfTen::Double(static_cast<int8_t>(power));
  }

  /// Returns 10^(-power).
  static double GetFactor(uint8_t power) {
    return AlpPowerOfTen::Double(static_cast<int8_t>(-static_cast<int8_t>(power)));
  }

  using EncodedUnsigned = uint64_t;
  using EncodedSigned = int64_t;
};

static_assert(AlpTypedConstants<double>::kMaxExponent <= 18,
              "ALP's int64 factor constants support exponents up to 18");

}  // namespace arrow::util::alp
