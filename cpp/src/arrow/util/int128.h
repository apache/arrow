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

#include <string>

#include "arrow/status.h"

namespace arrow {
namespace decimal {

/**
 * Represents a signed 128-bit integer in two's complement.
 * Calculations wrap around and overflow is ignored.
 *
 * For a discussion of the algorithms, look at Knuth's volume 2,
 * Semi-numerical Algorithms section 4.3.1.
 *
 * Adapted from the Apache ORC C++ implementation
 */
class Int128 {
 public:
  constexpr Int128() : Int128(0, 0) {}

  /**
   * Convert a signed 64 bit value into an Int128.
   * @param right
   */
  constexpr Int128(int64_t value)
      : Int128(value >= 0 ? 0 : -1, static_cast<uint64_t>(value)) {}

  /**
   * Convert a signed 32 bit value into an Int128.
   * @param right
   */
  constexpr Int128(int32_t value) : Int128(static_cast<int64_t>(value)) {}

  /**
   * Create from the two's complement representation.
   * @param high
   * @param low
   */
  constexpr Int128(int64_t high, uint64_t low) : highbits_(high), lowbits_(low) {}

  /**
   * Parse the number from a base 10 string representation.
   * @param value
   */
  explicit Int128(const std::string& value);

  /**
   * Create from an array of bytes
   * @param bytes
   */
  explicit Int128(const uint8_t* bytes);

  Int128& negate();

  /**
   * Add a number to this one. The result is truncated to 128 bits.
   * @param right the number to add
   * @return *this
   */
  Int128& operator+=(const Int128& right);

  /**
   * Subtract a number from this one. The result is truncated to 128 bits.
   * @param right the number to subtract
   * @return *this
   */
  Int128& operator-=(const Int128& right);

  /**
   * Multiply this number by a number. The result is truncated to 128 bits.
   * @param right the number to multiply by
   * @return *this
   */
  Int128& operator*=(const Int128& right);

  /**
   * Divide this number by right and return the result. This operation is
   * not destructive.
   *
   * The answer rounds to zero. Signs work like:
   *    21 /  5 ->  4,  1
   *   -21 /  5 -> -4, -1
   *    21 / -5 -> -4,  1
   *   -21 / -5 ->  4, -1
   * @param right the number to divide by
   * @param remainder the remainder after the division
   */
  Status divide(const Int128& divisor, Int128* result, Int128* remainder) const;

  Int128& operator/=(const Int128& right);

  /**
   * Cast the value to char. This is used when converting the value a string.
   * @return char
   */
  explicit operator char() const;

  /**
   * Bitwise or between two Int128.
   * @param right the number to or in
   * @return *this
   */
  Int128& operator|=(const Int128& right);

  /**
   * Bitwise and between two Int128.
   * @param right the number to and in
   * @return *this
   */
  Int128& operator&=(const Int128& right);

  /**
   * Shift left by the given number of bits.
   * Values larger than 2 ** 127 will shift into the sign bit.
   * @param bits
   */
  Int128& operator<<=(uint32_t bits);

  /**
   * Shift right by the given number of bits. Negative values will
   * sign extend and fill with one bits.
   * @param bits
   */
  Int128& operator>>=(uint32_t bits);

  /**
   * Get the high bits of the two's complement representation of the number.
   */
  int64_t highbits() const { return highbits_; }

  /**
   * Get the low bits of the two's complement representation of the number.
   */
  uint64_t lowbits() const { return lowbits_; }

  /**
   * Get the raw bytes of the value
   * @param[out] out Pointer to an already allocated array of bytes. Must not be nullptr.
   */
  void ToBytes(uint8_t** out) const;

 private:
  int64_t highbits_;
  uint64_t lowbits_;
};

bool operator==(const Int128& left, const Int128& right);
bool operator!=(const Int128& left, const Int128& right);
bool operator<(const Int128& left, const Int128& right);
bool operator<=(const Int128& left, const Int128& right);
bool operator>(const Int128& left, const Int128& right);
bool operator>=(const Int128& left, const Int128& right);

Int128 operator-(const Int128& operand);
Int128 operator+(const Int128& left, const Int128& right);
Int128 operator-(const Int128& left, const Int128& right);
Int128 operator*(const Int128& left, const Int128& right);
Int128 operator/(const Int128& left, const Int128& right);
Int128 operator%(const Int128& left, const Int128& right);

}  // namespace decimal
}  // namespace arrow

#endif  //  ARROW_INT128_H
