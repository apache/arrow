/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ARROW_INT128_H
#define ARROW_INT128_H

#include <cstdint>
#include <string>

#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace decimal {

/// Represents a signed 128-bit integer in two's complement.
/// Calculations wrap around and overflow is ignored.
///
/// For a discussion of the algorithms, look at Knuth's volume 2,
/// Semi-numerical Algorithms section 4.3.1.
///
/// Adapted from the Apache ORC C++ implementation
class ARROW_EXPORT Int128 {
 public:
  constexpr Int128() : Int128(0, 0) {}

  /// \brief Convert a signed 64 bit value into an Int128.
  constexpr Int128(int64_t value)
      : Int128(value >= 0 ? 0 : -1, static_cast<uint64_t>(value)) {}

  /// \brief Convert a signed 32 bit value into an Int128.
  constexpr Int128(int32_t value) : Int128(static_cast<int64_t>(value)) {}

  /// \brief Create an Int128 from the two's complement representation.
  constexpr Int128(int64_t high, uint64_t low) : high_bits_(high), low_bits_(low) {}

  /// \brief Parse the number from a base 10 string representation.
  explicit Int128(const std::string& value);

  /// \brief Create an Int128 from an array of bytes
  explicit Int128(const uint8_t* bytes);

  /// \brief Negate the current value
  Int128& Negate();

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  Int128& operator+=(const Int128& right);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  Int128& operator-=(const Int128& right);

  /// \brief Multiply this number by another number. The result is truncated to 128 bits.
  Int128& operator*=(const Int128& right);

  /// Divide this number by right and return the result. This operation is
  /// not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// @param right the number to divide by
  /// @param remainder the remainder after the division
  Status Divide(const Int128& divisor, Int128* result, Int128* remainder) const;

  /// \brief In-place division.
  Int128& operator/=(const Int128& right);

  /// \brief Cast the value to char. This is used when converting the value a string.
  explicit operator char() const;

  /// \brief Bitwise or between two Int128.
  Int128& operator|=(const Int128& right);

  /// \brief Bitwise and between two Int128.
  Int128& operator&=(const Int128& right);

  /// \brief Shift left by the given number of bits.
  Int128& operator<<=(uint32_t bits);

  /// \brief Shift right by the given number of bits. Negative values will
  Int128& operator>>=(uint32_t bits);

  /// \brief Get the high bits of the two's complement representation of the number.
  int64_t high_bits() const { return high_bits_; }

  /// \brief Get the low bits of the two's complement representation of the number.
  uint64_t low_bits() const { return low_bits_; }

  /// \brief Put the raw bytes of the value into a pointer to uint8_t.
  void ToBytes(uint8_t** out) const;

 private:
  int64_t high_bits_;
  uint64_t low_bits_;
};

ARROW_EXPORT bool operator==(const Int128& left, const Int128& right);
ARROW_EXPORT bool operator!=(const Int128& left, const Int128& right);
ARROW_EXPORT bool operator<(const Int128& left, const Int128& right);
ARROW_EXPORT bool operator<=(const Int128& left, const Int128& right);
ARROW_EXPORT bool operator>(const Int128& left, const Int128& right);
ARROW_EXPORT bool operator>=(const Int128& left, const Int128& right);

ARROW_EXPORT Int128 operator-(const Int128& operand);
ARROW_EXPORT Int128 operator+(const Int128& left, const Int128& right);
ARROW_EXPORT Int128 operator-(const Int128& left, const Int128& right);
ARROW_EXPORT Int128 operator*(const Int128& left, const Int128& right);
ARROW_EXPORT Int128 operator/(const Int128& left, const Int128& right);
ARROW_EXPORT Int128 operator%(const Int128& left, const Int128& right);

}  // namespace decimal
}  // namespace arrow

#endif  //  ARROW_INT128_H
