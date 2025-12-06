#pragma once

#include <cstdint>

#include "arrow/util/logging.h"

namespace arrow {
namespace util {
namespace alp {

class AlpConstants {
 public:
  /// Number of elements compressed together as a unit. This value is fixed for compatibility.
  static constexpr uint64_t kAlpVectorSize = 1024;

  /// Number of elements to use when determining sampling parameters.
  static constexpr uint64_t kSamplerVectorSize = 4096;

  /// Total number of elements in a rowgroup for sampling purposes.
  static constexpr uint64_t kSamplerRowgroupSize = 122880;

  /// Number of samples to collect per vector during the sampling phase.
  static constexpr uint64_t kSamplerSamplesPerVector = 256;

  /// Number of sample vectors to collect per rowgroup.
  static constexpr uint64_t kSamplerSampleVectorsPerRowgroup = 8;

  /// Version number for the ALP compression format.
  static constexpr uint64_t kAlpVersion = 1;

  /// Type used to store exception positions within a compressed vector.
  using PositionType = uint16_t;

  /// Threshold for early exit during sampling when compression quality is poor.
  static constexpr uint8_t kSamplingEarlyExitThreshold = 4;

  /// Maximum number of exponent-factor combinations to try during compression.
  static constexpr uint8_t kMaxCombinations = 5;

  /// Loop unroll factor for tight loops in ALP compression/decompression.
  /// ALP has multiple tight loops that profit from unrolling. Setting this might affect
  /// performance, so benchmarking is recommended. The gains from kLoopUnrolls = 4 are marginal.
  static constexpr uint64_t kLoopUnrolls = 4;

  static uint64_t powerOfTenUB8(const uint8_t power) {
    ARROW_DCHECK(power <= 19) << "power_out_of_range: " << static_cast<int>(power);
    static constexpr uint64_t kTable[20] = {1,
                                            10,
                                            100,
                                            1'000,
                                            10'000,
                                            100'000,
                                            1'000'000,
                                            10'000'000,
                                            100'000'000,
                                            1'000'000'000,
                                            10'000'000'000,
                                            100'000'000'000,
                                            1'000'000'000'000,
                                            10'000'000'000'000,
                                            100'000'000'000'000,
                                            1'000'000'000'000'000,
                                            10'000'000'000'000'000,
                                            100'000'000'000'000'000,
                                            1'000'000'000'000'000'000,
                                            10'000'000'000'000'000'000ULL};

    return kTable[power];
  }

  static float powerOfTenFloat(int8_t power) {
    ARROW_DCHECK(power >= -10 && power <= 10) << "power_out_of_range: " << static_cast<int>(power);
    static constexpr float kTable[21] = {
        0.0000000001F, 0.000000001F,  0.00000001F,   0.0000001F, 0.000001F,  0.00001F,
        0.0001F,       0.001F,        0.01F,         0.1F,       1.0F,       10.0F,
        100.0F,        1000.0F,       10000.0F,      100000.0F,  1000000.0F, 10000000.0F,
        100000000.0F,  1000000000.0F, 10000000000.0F};

    return kTable[power + 10];
  }

  static double powerOfTenDouble(const int8_t power) {
    ARROW_DCHECK(power >= -20 && power <= 20) << "power_out_of_range: " << static_cast<int>(power);
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

  static int64_t getFactor(const int8_t power) { return powerOfTenUB8(power); }
};

template <typename FloatingPointType>
struct AlpTypedConstants {};

template <>
struct AlpTypedConstants<float> {
  /// Magic number used for fast rounding of floats to nearest integer:
  /// rounded(n) = static_cast<int32_t>(n + kMagicNumber - kMagicNumber).
  static constexpr float kMagicNumber = 12582912.0f;  // 2^22 + 2^23

  static constexpr uint8_t kMaxExponent = 10;

  /// Largest float value that can be safely converted to int32.
  static constexpr float kEncodingUpperLimit = 2147483520.0f;
  static constexpr float kEncodingLowerLimit = -2147483520.0f;

  static float getExponent(const uint8_t power) { return AlpConstants::powerOfTenFloat(power); }

  static float getFactor(const uint8_t power) {
    // This double cast is necessary since subtraction on int8_t does not necessarily yield an
    // int8_t.
    return AlpConstants::powerOfTenFloat(static_cast<int8_t>(-static_cast<int8_t>(power)));
  }

  using FloatingToExact = uint32_t;
  using FloatingToSignedExact = int32_t;
};

template <>
class AlpTypedConstants<double> {
 public:
  /// Magic number used for fast rounding of doubles to nearest integer:
  /// rounded(n) = static_cast<int64_t>(n + kMagicNumber - kMagicNumber).
  static constexpr double kMagicNumber = 6755399441055744.0;  // 2^51 + 2^52

  static constexpr uint8_t kMaxExponent = 18;  // 10^18 is the maximum int64

  /// Largest double value that can be safely converted to int64.
  static constexpr double kEncodingUpperLimit = 9223372036854774784.0;
  static constexpr double kEncodingLowerLimit = -9223372036854774784.0;

  static double getExponent(const uint8_t power) { return AlpConstants::powerOfTenDouble(power); }

  static double getFactor(const uint8_t power) {
    return AlpConstants::powerOfTenDouble(static_cast<int8_t>(-static_cast<int8_t>(power)));
  }

  using FloatingToExact = uint64_t;
  using FloatingToSignedExact = int64_t;
};

}  // namespace alp
}  // namespace util
}  // namespace arrow
