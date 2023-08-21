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
#include "arrow/util/ubsan.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

/// \brief Base class for an IEEE half-precision float, encoded as a `uint16_t`
///
/// The exact format is as follows (from MSB to LSB):
/// - bit 0:     sign
/// - bits 1-5:  exponent
/// - bits 6-15: mantissa
///
/// NOTE: Methods in the class should not mutate the unerlying value or produce copies.
/// Such functionality is delegated to subclasses.
class ARROW_EXPORT Float16Base {
 public:
  Float16Base() = default;
  constexpr explicit Float16Base(uint16_t value) : value_(value) {}

  /// \brief Return the value's integer representation
  constexpr uint16_t bits() const { return value_; }
  constexpr explicit operator uint16_t() const { return bits(); }

  /// \brief Return true if the value is negative (sign bit is set)
  constexpr bool signbit() const { return (value_ & 0x8000) != 0; }

  /// \brief Return true if the value is NaN
  constexpr bool is_nan() const {
    return (value_ & 0x7c00) == 0x7c00 && (value_ & 0x03ff) != 0;
  }
  /// \brief Return true if the value is positive/negative infinity
  constexpr bool is_infinity() const { return (value_ & 0x7fff) == 0x7c00; }
  /// \brief Return true if the value is positive/negative zero
  constexpr bool is_zero() const { return (value_ & 0x7fff) == 0; }

  /// \brief Copy the value's bytes in native-endian byte order
  void ToBytes(uint8_t* dest) const { std::memcpy(dest, &value_, sizeof(value_)); }
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
    Float16Base{bit_util::ToLittleEndian(value_)}.ToBytes(dest);
  }
  /// \brief Return the value's bytes in little-endian byte order
  constexpr std::array<uint8_t, 2> ToLittleEndian() const {
#if ARROW_LITTLE_ENDIAN
    return {uint8_t(value_ & 0xff), uint8_t(value_ >> 8)};
#else
    return {uint8_t(value_ >> 8), uint8_t(value_ & 0xff)};
#endif
  }

  /// \brief Copy the value's bytes in big-endian byte order
  void ToBigEndian(uint8_t* dest) const {
    Float16Base{bit_util::ToBigEndian(value_)}.ToBytes(dest);
  }
  /// \brief Return the value's bytes in big-endian byte order
  constexpr std::array<uint8_t, 2> ToBigEndian() const {
#if ARROW_LITTLE_ENDIAN
    return {uint8_t(value_ >> 8), uint8_t(value_ & 0xff)};
#else
    return {uint8_t(value_ & 0xff), uint8_t(value_ >> 8)};
#endif
  }

  float ToFloat() const;

  friend constexpr bool operator==(Float16Base lhs, Float16Base rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return Float16Base::CompareEq(lhs, rhs);
  }
  friend constexpr bool operator!=(Float16Base lhs, Float16Base rhs) {
    return !(lhs == rhs);
  }

  friend constexpr bool operator<(Float16Base lhs, Float16Base rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return Float16Base::CompareLt(lhs, rhs);
  }
  friend constexpr bool operator>(Float16Base lhs, Float16Base rhs) { return rhs < lhs; }

  friend constexpr bool operator<=(Float16Base lhs, Float16Base rhs) {
    if (lhs.is_nan() || rhs.is_nan()) return false;
    return !Float16Base::CompareLt(rhs, lhs);
  }
  friend constexpr bool operator>=(Float16Base lhs, Float16Base rhs) {
    return rhs <= lhs;
  }

  ARROW_FRIEND_EXPORT friend std::ostream& operator<<(std::ostream& os, Float16Base arg);

 protected:
  uint16_t value_;

 private:
  // Comparison helpers that assume neither operand is NaN
  static constexpr bool CompareEq(Float16Base lhs, Float16Base rhs) {
    return (lhs.bits() == rhs.bits()) || (lhs.is_zero() && rhs.is_zero());
  }
  static constexpr bool CompareLt(Float16Base lhs, Float16Base rhs) {
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

/// \brief Wrapper class for an IEEE half-precision float, encoded as a `uint16_t`
class ARROW_EXPORT Float16 : public Float16Base {
 public:
  using Float16Base::Float16Base;

  constexpr Float16 operator-() const { return Float16(value_ ^ 0x8000); }
  constexpr Float16 operator+() const { return Float16(value_); }

  static Float16 FromFloat(float f);

  /// \brief Read a `Float16` from memory in native-endian byte order
  static Float16 FromBytes(const uint8_t* src) {
    return Float16(SafeLoadAs<uint16_t>(src));
  }

  /// \brief Read a `Float16` from memory in little-endian byte order
  static Float16 FromLittleEndian(const uint8_t* src) {
    return Float16(bit_util::FromLittleEndian(SafeLoadAs<uint16_t>(src)));
  }

  /// \brief Read a `Float16` from memory in big-endian byte order
  static Float16 FromBigEndian(const uint8_t* src) {
    return Float16(bit_util::FromBigEndian(SafeLoadAs<uint16_t>(src)));
  }
};

static_assert(std::is_trivial_v<Float16>);

}  // namespace util
}  // namespace arrow

// TODO: Not complete
template <>
class std::numeric_limits<arrow::util::Float16> {
  using T = arrow::util::Float16;

 public:
  static constexpr bool is_specialized = true;
  static constexpr bool is_signed = true;
  static constexpr bool has_infinity = true;
  static constexpr bool has_quiet_NaN = true;

  static constexpr T min() { return T(0b0000010000000000); }
  static constexpr T max() { return T(0b0111101111111111); }
  static constexpr T lowest() { return -max(); }

  static constexpr T infinity() { return T(0b0111110000000000); }

  static constexpr T quiet_NaN() { return T(0b0111111111111111); }
};
