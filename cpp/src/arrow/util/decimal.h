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

#ifndef ARROW_DECIMAL_H
#define ARROW_DECIMAL_H

#include <array>
#include <cstdint>
#include <string>
#include <type_traits>

#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

/// Represents a signed 128-bit integer in two's complement.
/// Calculations wrap around and overflow is ignored.
///
/// For a discussion of the algorithms, look at Knuth's volume 2,
/// Semi-numerical Algorithms section 4.3.1.
///
/// Adapted from the Apache ORC C++ implementation
class ARROW_EXPORT Decimal128 {
 public:
  /// \brief Create an Decimal128 from the two's complement representation.
  constexpr Decimal128(int64_t high, uint64_t low) : high_bits_(high), low_bits_(low) {}

  /// \brief Empty constructor creates an Decimal128 with a value of 0.
  constexpr Decimal128() : Decimal128(0, 0) {}

  /// \brief Convert any integer value into an Decimal128.
  template <typename T,
            typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
  constexpr Decimal128(T value)
      : Decimal128(static_cast<int64_t>(value) >= 0 ? 0 : -1,
                   static_cast<uint64_t>(value)) {}

  /// \brief Parse the number from a base 10 string representation.
  explicit Decimal128(const std::string& value);

  /// \brief Create an Decimal128 from an array of bytes. Bytes are assumed to be in
  /// little endian byte order.
  explicit Decimal128(const uint8_t* bytes);

  /// \brief Negate the current value
  Decimal128& Negate();

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  Decimal128& operator+=(const Decimal128& right);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  Decimal128& operator-=(const Decimal128& right);

  /// \brief Multiply this number by another number. The result is truncated to 128 bits.
  Decimal128& operator*=(const Decimal128& right);

  /// Divide this number by right and return the result. This operation is
  /// not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param divisor the number to divide by
  /// \param remainder the remainder after the division
  Status Divide(const Decimal128& divisor, Decimal128* result,
                Decimal128* remainder) const;

  /// \brief In-place division.
  Decimal128& operator/=(const Decimal128& right);

  /// \brief Cast the value to char. This is used when converting the value a string.
  explicit operator char() const;

  /// \brief Bitwise or between two Decimal128.
  Decimal128& operator|=(const Decimal128& right);

  /// \brief Bitwise and between two Decimal128.
  Decimal128& operator&=(const Decimal128& right);

  /// \brief Shift left by the given number of bits.
  Decimal128& operator<<=(uint32_t bits);

  /// \brief Shift right by the given number of bits. Negative values will
  Decimal128& operator>>=(uint32_t bits);

  /// \brief Get the high bits of the two's complement representation of the number.
  int64_t high_bits() const { return high_bits_; }

  /// \brief Get the low bits of the two's complement representation of the number.
  uint64_t low_bits() const { return low_bits_; }

  /// \brief Return the raw bytes of the value in little-endian byte order.
  std::array<uint8_t, 16> ToBytes() const;

  /// \brief Convert the Decimal128 value to a base 10 decimal string with the given
  /// precision and scale.
  std::string ToString(int precision, int scale) const;

  /// \brief Convert a decimal string to an Decimal128 value, optionally including
  /// precision and scale if they're passed in and not null.
  static Status FromString(const std::string& s, Decimal128* out,
                           int* precision = NULLPTR, int* scale = NULLPTR);

 private:
  int64_t high_bits_;
  uint64_t low_bits_;
};

ARROW_EXPORT bool operator==(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT bool operator!=(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT bool operator<(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT bool operator<=(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT bool operator>(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT bool operator>=(const Decimal128& left, const Decimal128& right);

ARROW_EXPORT Decimal128 operator-(const Decimal128& operand);
ARROW_EXPORT Decimal128 operator+(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT Decimal128 operator-(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT Decimal128 operator*(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT Decimal128 operator/(const Decimal128& left, const Decimal128& right);
ARROW_EXPORT Decimal128 operator%(const Decimal128& left, const Decimal128& right);

}  // namespace arrow

#endif  //  ARROW_DECIMAL_H
