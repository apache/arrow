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

#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <limits>
#include <type_traits>

#include "arrow/util/endian.h"
#include "arrow/util/macros.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

/// \brief Class representing an IEEE half-precision float, encoded as a `uint16_t`
///
/// The exact format is as follows (from LSB to MSB):
/// - bits 0-10:  mantissa
/// - bits 10-15: exponent
/// - bit 15:     sign
///
class ARROW_EXPORT Float16 {
 public:
  Float16() = default;
  explicit Float16(float f) : Float16(FromFloat(f)) {}
  explicit Float16(double d) : Float16(FromDouble(d)) {}
  template <typename T,
            typename std::enable_if_t<std::is_convertible_v<T, double>>* = NULLPTR>
  explicit Float16(T v) : Float16(static_cast<double>(v)) {}

  /// \brief Create a `Float16` from its exact binary representation
  constexpr static Float16 FromBits(uint16_t bits) { return Float16{bits, bool{}}; }
  /// \brief Create a `Float16` from a 32-bit float (may lose precision)
  static Float16 FromFloat(float f);
  /// \brief Create a `Float16` from a 64-bit float (may lose precision)
  static Float16 FromDouble(double d);

  /// \brief Read a `Float16` from memory in native-endian byte order
  static Float16 FromBytes(const uint8_t* src) {
    return FromBits(SafeLoadAs<uint16_t>(src));
  }

  /// \brief Read a `Float16` from memory in little-endian byte order
  static Float16 FromLittleEndian(const uint8_t* src) {
    return FromBits(::arrow::bit_util::FromLittleEndian(SafeLoadAs<uint16_t>(src)));
  }

  /// \brief Read a `Float16` from memory in big-endian byte order
  static Float16 FromBigEndian(const uint8_t* src) {
    return FromBits(::arrow::bit_util::FromBigEndian(SafeLoadAs<uint16_t>(src)));
  }

  /// \brief Return the value's binary representation as a `uint16_t`
  constexpr uint16_t bits() const { return bits_; }

  /// \brief Return true if the value is negative (sign bit is set)
  constexpr bool signbit() const { return (bits_ & 0x8000) != 0; }

  /// \brief Return true if the value is NaN
  constexpr bool is_nan() const { return (bits_ & 0x7fff) > 0x7c00; }
  /// \brief Return true if the value is positive/negative infinity
  constexpr bool is_infinity() const { return (bits_ & 0x7fff) == 0x7c00; }
  /// \brief Return true if the value is finite and not NaN
  constexpr bool is_finite() const { return (bits_ & 0x7c00) != 0x7c00; }
  /// \brief Return true if the value is positive/negative zero
  constexpr bool is_zero() const { return (bits_ & 0x7fff) == 0; }

  /// \brief Convert to a 32-bit float
  float ToFloat() const;
  /// \brief Convert to a 64-bit float
  double ToDouble() const;

  explicit operator float() const { return ToFloat(); }
  explicit operator double() const { return ToDouble(); }

  /// \brief Copy the value's bytes in native-endian byte order
  void ToBytes(uint8_t* dest) const { std::memcpy(dest, &bits_, sizeof(bits_)); }
  /// \brief Return the value's bytes in native-endian byte order
  constexpr std::array<uint8_t, 2> ToBytes() const {
#if ARROW_LITTLE_ENDIAN
    return ToLittleEndian();
#else
    return ToBigEndian();
#endif
  }

  /// \brief Copy the value's bytes in little-endian byte order
  void ToLittleEndian(uint8_t* dest) const {
    const auto bytes = ToLittleEndian();
    std::memcpy(dest, bytes.data(), bytes.size());
  }
  /// \brief Return the value's bytes in little-endian byte order
  constexpr std::array<uint8_t, 2> ToLittleEndian() const {
    return {uint8_t(bits_ & 0xff), uint8_t(bits_ >> 8)};
  }

  /// \brief Copy the value's bytes in big-endian byte order
  void ToBigEndian(uint8_t* dest) const {
    const auto bytes = ToBigEndian();
    std::memcpy(dest, bytes.data(), bytes.size());
  }
  /// \brief Return the value's bytes in big-endian byte order
  constexpr std::array<uint8_t, 2> ToBigEndian() const {
    return {uint8_t(bits_ >> 8), uint8_t(bits_ & 0xff)};
  }

  constexpr Float16 operator-() const { return FromBits(bits_ ^ 0x8000); }
  constexpr Float16 operator+() const { return FromBits(bits_); }

  friend constexpr bool operator==(Float16 lhs, Float16 rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return Float16::CompareEq(lhs, rhs);
  }
  friend constexpr bool operator!=(Float16 lhs, Float16 rhs) { return !(lhs == rhs); }

  friend constexpr bool operator<(Float16 lhs, Float16 rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return Float16::CompareLt(lhs, rhs);
  }
  friend constexpr bool operator>(Float16 lhs, Float16 rhs) { return rhs < lhs; }

  friend constexpr bool operator<=(Float16 lhs, Float16 rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return !Float16::CompareLt(rhs, lhs);
  }
  friend constexpr bool operator>=(Float16 lhs, Float16 rhs) { return rhs <= lhs; }

  ARROW_FRIEND_EXPORT friend std::ostream& operator<<(std::ostream& os, Float16 arg);

  static constexpr Float16 zero() { return FromBits(0); }
  static constexpr Float16 one() { return FromBits(0x3c00); }

 protected:
  uint16_t bits_;

 private:
  constexpr Float16(uint16_t bits, bool) : bits_(bits) {}

  // Comparison helpers that assume neither operand is NaN
  static constexpr bool CompareEq(Float16 lhs, Float16 rhs) {
    return (lhs.bits() == rhs.bits()) || (lhs.is_zero() && rhs.is_zero());
  }
  static constexpr bool CompareLt(Float16 lhs, Float16 rhs) {
    if (lhs.signbit()) {
      if (rhs.signbit()) {
        // Both are negative
        return lhs.bits() > rhs.bits();
      } else {
        // Handle +/-0
        return !lhs.is_zero() || rhs.bits() != 0;
      }
    } else if (rhs.signbit()) {
      return false;
    } else {
      // Both are positive
      return lhs.bits() < rhs.bits();
    }
  }
};

static_assert(std::is_standard_layout_v<Float16>);
static_assert(std::is_trivial_v<Float16>);
static_assert(sizeof(Float16) == sizeof(uint16_t));

}  // namespace util
}  // namespace arrow

template <>
class std::numeric_limits<arrow::util::Float16> {
  using T = arrow::util::Float16;

 public:
  static constexpr bool is_specialized = true;
  static constexpr bool is_signed = true;
  static constexpr bool is_integer = false;
  static constexpr bool is_exact = false;
  static constexpr bool has_infinity = true;
  static constexpr bool has_quiet_NaN = true;
  static constexpr bool has_signaling_NaN = true;
  static constexpr std::float_denorm_style has_denorm = std::denorm_present;
  static constexpr bool has_denorm_loss = false;
  static constexpr bool is_iec559 = true;
  static constexpr bool is_bounded = true;
  static constexpr bool is_modulo = false;
  static constexpr int radix = 2;

  // Float16 has 10 explicit mantissa bits + 1 implicit bit = 11 bits precision
  static constexpr int digits = 11;
  // Number of decimal digits that can be represented: floor(10 * log10(2))
  static constexpr int digits10 = 3;
  // Number of decimal digits to fully represent the type: ceil(11 * log10(2) + 1)
  static constexpr int max_digits10 = 5;

  // Exponent range: bias = 15, min subnormal exponent = -14, min normal = -13
  static constexpr int min_exponent = -13;
  static constexpr int min_exponent10 = -4;
  // Max exponent before infinity: field value 30 -> 30 - 15 + 1 = 16
  static constexpr int max_exponent = 16;
  static constexpr int max_exponent10 = 4;

  static constexpr bool traps = false;
  static constexpr bool tinyness_before = false;
  static constexpr std::float_round_style round_style = std::round_to_nearest;

  static constexpr T min() { return T::FromBits(0b0000010000000000); }
  static constexpr T max() { return T::FromBits(0b0111101111111111); }
  static constexpr T lowest() { return -max(); }
  static constexpr T epsilon() { return T::FromBits(0b0001010000000000); }      // 2^-10
  static constexpr T round_error() { return T::FromBits(0b0011100000000000); }  // 0.5
  static constexpr T denorm_min() { return T::FromBits(0b0000000000000001); }

  static constexpr T infinity() { return T::FromBits(0b0111110000000000); }
  static constexpr T quiet_NaN() { return T::FromBits(0b0111111111111111); }
  static constexpr T signaling_NaN() { return T::FromBits(0b0111110000000001); }
};
