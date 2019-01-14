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

#ifndef ARROW_DECIMAL_BASIC_H
#define ARROW_DECIMAL_BASIC_H

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>

#include "arrow/util/macros.h"
#include "arrow/util/type_traits.h"
#include "arrow/util/visibility.h"

namespace arrow {

enum class DecimalStatus {
  kSuccess,
  kDivideByZero,
  kBuildArrayFiveInts,
  kBuildArrayUnsupportedLength,
  kRescaleDataLoss,
};

/// This class is also compiled into IR - so, it should not have cpp references like
/// streams, boost, ..
class ARROW_EXPORT DecimalBasic128 {
 public:
  /// \brief Create an DecimalBasic128 from the two's complement representation.
  constexpr DecimalBasic128(int64_t high, uint64_t low) noexcept
      : low_bits_(low), high_bits_(high) {}

  /// \brief Empty constructor creates an DecimalBasic128 with a value of 0.
  constexpr DecimalBasic128() noexcept : DecimalBasic128(0, 0) {}

  /// \brief Convert any integer value into an DecimalBasic128.
  template <typename T,
            typename = typename std::enable_if<std::is_integral<T>::value, T>::type>
  constexpr DecimalBasic128(T value) noexcept
      : DecimalBasic128(static_cast<int64_t>(value) >= 0 ? 0 : -1,
                        static_cast<uint64_t>(value)) {}

  /// \brief Create an DecimalBasic128 from an array of bytes. Bytes are assumed to be in
  /// little endian byte order.
  explicit DecimalBasic128(const uint8_t* bytes);

  /// \brief Negate the current value
  DecimalBasic128& Negate();

  /// \brief Absolute value
  DecimalBasic128& Abs();

  /// \brief Add a number to this one. The result is truncated to 128 bits.
  DecimalBasic128& operator+=(const DecimalBasic128& right);

  /// \brief Subtract a number from this one. The result is truncated to 128 bits.
  DecimalBasic128& operator-=(const DecimalBasic128& right);

  /// \brief Multiply this number by another number. The result is truncated to 128 bits.
  DecimalBasic128& operator*=(const DecimalBasic128& right);

  /// Divide this number by right and return the result. This operation is
  /// not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param divisor the number to divide by
  /// \param remainder the remainder after the division
  DecimalStatus Divide(const DecimalBasic128& divisor, DecimalBasic128* result,
                       DecimalBasic128* remainder) const;

  /// \brief In-place division.
  DecimalBasic128& operator/=(const DecimalBasic128& right);

  /// \brief Bitwise or between two DecimalBasic128.
  DecimalBasic128& operator|=(const DecimalBasic128& right);

  /// \brief Bitwise and between two DecimalBasic128.
  DecimalBasic128& operator&=(const DecimalBasic128& right);

  /// \brief Shift left by the given number of bits.
  DecimalBasic128& operator<<=(uint32_t bits);

  /// \brief Shift right by the given number of bits. Negative values will
  DecimalBasic128& operator>>=(uint32_t bits);

  /// \brief Get the high bits of the two's complement representation of the number.
  inline int64_t high_bits() const { return high_bits_; }

  /// \brief Get the low bits of the two's complement representation of the number.
  inline uint64_t low_bits() const { return low_bits_; }

  /// \brief Return the raw bytes of the value in little-endian byte order.
  std::array<uint8_t, 16> ToBytes() const;
  void ToBytes(uint8_t* out) const;

  /// \brief seperate the integer and fractional parts for the given scale.
  void GetWholeAndFraction(int32_t scale, DecimalBasic128* whole,
                           DecimalBasic128* fraction) const;

  /// \brief Scale multiplier for given scale value.
  static const DecimalBasic128& GetScaleMultiplier(int32_t scale);

  /// \brief Convert DecimalBasic128 from one scale to another
  DecimalStatus Rescale(int32_t original_scale, int32_t new_scale,
                        DecimalBasic128* out) const;

  /// \brief Scale up.
  DecimalBasic128 IncreaseScaleBy(int32_t increase_by) const;

  /// \brief Scale down.
  /// - If 'round' is true, the right-most digits are dropped and the result value is
  ///   rounded up (+1 for +ve, -1 for -ve) based on the value of the dropped digits
  ///   (>= 10^reduce_by / 2).
  /// - If 'round' is false, the right-most digits are simply dropped.
  DecimalBasic128 ReduceScaleBy(int32_t reduce_by, bool round = true) const;

  /// \brief count the number of leading binary zeroes.
  int32_t CountLeadingBinaryZeros() const;

 private:
  uint64_t low_bits_;
  int64_t high_bits_;
};

ARROW_EXPORT bool operator==(const DecimalBasic128& left, const DecimalBasic128& right);
ARROW_EXPORT bool operator!=(const DecimalBasic128& left, const DecimalBasic128& right);
ARROW_EXPORT bool operator<(const DecimalBasic128& left, const DecimalBasic128& right);
ARROW_EXPORT bool operator<=(const DecimalBasic128& left, const DecimalBasic128& right);
ARROW_EXPORT bool operator>(const DecimalBasic128& left, const DecimalBasic128& right);
ARROW_EXPORT bool operator>=(const DecimalBasic128& left, const DecimalBasic128& right);

ARROW_EXPORT DecimalBasic128 operator-(const DecimalBasic128& operand);
ARROW_EXPORT DecimalBasic128 operator~(const DecimalBasic128& operand);
ARROW_EXPORT DecimalBasic128 operator+(const DecimalBasic128& left,
                                       const DecimalBasic128& right);
ARROW_EXPORT DecimalBasic128 operator-(const DecimalBasic128& left,
                                       const DecimalBasic128& right);
ARROW_EXPORT DecimalBasic128 operator*(const DecimalBasic128& left,
                                       const DecimalBasic128& right);
ARROW_EXPORT DecimalBasic128 operator/(const DecimalBasic128& left,
                                       const DecimalBasic128& right);
ARROW_EXPORT DecimalBasic128 operator%(const DecimalBasic128& left,
                                       const DecimalBasic128& right);

}  // namespace arrow

#endif  //  ARROW_DECIMAL_BASIC_H
