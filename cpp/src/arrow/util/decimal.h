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
#include <iostream>
#include <string>
#include <type_traits>

#include "arrow/status.h"
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
  /// \brief Create a Decimal128 from the two's complement representation.
  /// \param high The upper 64 bits of the Decimal128 value
  /// \param low The low 64 bits of the Decimal128 value
  template <
      typename Signed, typename Unsigned,
      typename = typename std::enable_if<
          std::is_integral<Signed>::value && std::is_signed<Signed>::value, Signed>::type,
      typename = typename std::enable_if<std::is_integral<Unsigned>::value &&
                                             !std::is_signed<Unsigned>::value,
                                         Unsigned>::type>
  constexpr Decimal128(Signed high, Unsigned low) : high_bits_(high), low_bits_(low) {}

  /// \brief Empty constructor creates a Decimal128 with a value of 0.
  constexpr Decimal128() : Decimal128(0LL, 0ULL) {}

  /// \brief Convert any integer value into a Decimal128.
  /// \param value Any integer value to convert to a Decimal128
  template <typename T,
            typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
  constexpr Decimal128(T value)
      : Decimal128(
            std::is_signed<T>::value && static_cast<int64_t>(value) < 0LL ? -1LL : 0LL,
            static_cast<uint64_t>(value)) {}

  /// \brief Parse the number from a base 10 string representation.
  /// \param value The string to construct a Decimal128 from
  /// \see Decimal128::FromString
  explicit Decimal128(const std::string& value);

  /// \brief Create a Decimal128 from an array of bytes
  /// This constructor assumes that bytes are in big-endian byte order. Its behavior
  /// is undefined if bytes are not in big-endian byte order
  /// \param bytes Raw bytes of a decimal in big-endian order
  /// \param length Number of bytes to read
  explicit Decimal128(const uint8_t* bytes, int32_t length);

  /// \brief Negate the current value
  Decimal128& Negate();

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  Decimal128& operator+=(const Decimal128& right);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  Decimal128& operator-=(const Decimal128& right);

  /// \brief Multiply this number by another number. The result is truncated to 128 bits.
  Decimal128& operator*=(const Decimal128& right);

  /// \brief Divide this number by right and return the result.
  /// This operation is not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param[in] divisor The number to divide by
  /// \param[out] result The result of the division operation
  /// \param[out] remainder The remainder after the division. Optional.
  Status Divide(const Decimal128& divisor, Decimal128* result,
                Decimal128* remainder = nullptr) const;

  /// \brief In-place division.
  Decimal128& operator/=(const Decimal128& right);

  /// \brief Cast the Decimal128 to a char. This is used when displaying the value.
  /// \return The Decimal128 value as a char.
  explicit operator char() const;

  /// \brief Bitwise or between two Decimal128.
  Decimal128& operator|=(const Decimal128& right);

  /// \brief Bitwise and between two Decimal128.
  Decimal128& operator&=(const Decimal128& right);

  /// \brief Shift left by the given number of bits.
  Decimal128& operator<<=(uint32_t bits);

  /// \brief Shift right by the given number of bits.
  Decimal128& operator>>=(uint32_t bits);

  /// \brief Get the high bits of the two's complement representation of the number.
  constexpr int64_t high_bits() const { return high_bits_; }

  /// \brief Get the low bits of the two's complement representation of the number.
  constexpr uint64_t low_bits() const { return low_bits_; }

  /// \brief Return the raw bytes of the value.
  /// \return The raw bytes of the Decimal128 value in big-endian order.
  std::array<uint8_t, 16> ToBytes() const;

  /// \brief Convert the Decimal128 value to a base 10 decimal string.
  /// \param precision The total number of decimal digits to print.
  /// \param scale The number of decimal digits to print after the decimal point.
  std::string ToString(int precision, int scale) const;

  /// \brief Convert a decimal string to a Decimal128 value.
  /// \param[in] s The string to convert to a Decimal128 value.
  /// \param[out] out The resulting decimal value converted from a string.
  /// \param[out] precision The decimal precision of the input string.
  /// \param[out] scale The number of decimal digits to the right of the decimal point.
  static Status FromString(const std::string& s, Decimal128* out,
                           int* precision = nullptr, int* scale = nullptr);

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

static inline std::ostream& operator<<(std::ostream& os, const Decimal128& value) {
  std::string string_value = value.ToString(38, 0);
  return os << "Decimal128(\""
            << string_value.erase(0, string_value.find_first_not_of('0')) << "\")";
}

namespace DecimalUtil {

ARROW_EXPORT int32_t DecimalSize(int32_t precision);

}  // namespace DecimalUtil

}  // namespace arrow

#endif  //  ARROW_DECIMAL_H
