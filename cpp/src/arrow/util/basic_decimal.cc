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

#include "arrow/util/basic_decimal.h"

#include <algorithm>
#include <array>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <string>

#include "arrow/util/bit_util.h"
#include "arrow/util/config.h"  // for ARROW_USE_NATIVE_INT128
#include "arrow/util/decimal_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::AddWithOverflow;
using internal::SafeLeftShift;
using internal::SafeSignedAdd;
using internal::SafeSignedSubtract;
using internal::SubtractWithOverflow;

#ifdef ARROW_USE_NATIVE_INT128
static constexpr uint64_t kInt64Mask = 0xFFFFFFFFFFFFFFFF;
#else
static constexpr uint64_t kInt32Mask = 0xFFFFFFFF;
#endif

// same as kDecimal128PowersOfTen[38] - 1
static constexpr BasicDecimal128 kMaxDecimal128Value{5421010862427522170LL,
                                                     687399551400673280ULL - 1};

BasicDecimal128& BasicDecimal128::Negate() {
  uint64_t result_lo = ~low_bits() + 1;
  int64_t result_hi = ~high_bits();
  if (result_lo == 0) {
    result_hi = SafeSignedAdd<int64_t>(result_hi, 1);
  }
  *this = BasicDecimal128(result_hi, result_lo);
  return *this;
}

BasicDecimal128& BasicDecimal128::Abs() { return *this < 0 ? Negate() : *this; }

BasicDecimal128 BasicDecimal128::Abs(const BasicDecimal128& in) {
  BasicDecimal128 result(in);
  return result.Abs();
}

bool BasicDecimal128::FitsInPrecision(int32_t precision) const {
  DCHECK_GT(precision, 0);
  DCHECK_LE(precision, 38);
  return BasicDecimal128::Abs(*this) < kDecimal128PowersOfTen[precision];
}

BasicDecimal128& BasicDecimal128::operator+=(const BasicDecimal128& right) {
  int64_t result_hi = SafeSignedAdd(high_bits(), right.high_bits());
  uint64_t result_lo = low_bits() + right.low_bits();
  result_hi = SafeSignedAdd<int64_t>(result_hi, result_lo < low_bits());
  *this = BasicDecimal128(result_hi, result_lo);
  return *this;
}

BasicDecimal128& BasicDecimal128::operator-=(const BasicDecimal128& right) {
  int64_t result_hi = SafeSignedSubtract(high_bits(), right.high_bits());
  uint64_t result_lo = low_bits() - right.low_bits();
  result_hi = SafeSignedSubtract<int64_t>(result_hi, result_lo > low_bits());
  *this = BasicDecimal128(result_hi, result_lo);
  return *this;
}

BasicDecimal128& BasicDecimal128::operator/=(const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  auto s = Divide(right, this, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return *this;
}

BasicDecimal128& BasicDecimal128::operator|=(const BasicDecimal128& right) {
  array_[0] |= right.array_[0];
  array_[1] |= right.array_[1];
  return *this;
}

BasicDecimal128& BasicDecimal128::operator&=(const BasicDecimal128& right) {
  array_[0] &= right.array_[0];
  array_[1] &= right.array_[1];
  return *this;
}

BasicDecimal128& BasicDecimal128::operator<<=(uint32_t bits) {
  if (bits != 0) {
    uint64_t result_lo;
    int64_t result_hi;
    if (bits < 64) {
      result_hi = SafeLeftShift(high_bits(), bits);
      result_hi |= (low_bits() >> (64 - bits));
      result_lo = low_bits() << bits;
    } else if (bits < 128) {
      result_hi = static_cast<int64_t>(low_bits() << (bits - 64));
      result_lo = 0;
    } else {
      result_hi = 0;
      result_lo = 0;
    }
    *this = BasicDecimal128(result_hi, result_lo);
  }
  return *this;
}

BasicDecimal128& BasicDecimal128::operator>>=(uint32_t bits) {
  if (bits != 0) {
    uint64_t result_lo;
    int64_t result_hi;
    if (bits < 64) {
      result_lo = low_bits() >> bits;
      result_lo |= static_cast<uint64_t>(high_bits()) << (64 - bits);
      result_hi = high_bits() >> bits;
    } else if (bits < 128) {
      result_lo = static_cast<uint64_t>(high_bits() >> (bits - 64));
      result_hi = high_bits() >> 63;
    } else {
      result_hi = high_bits() >> 63;
      result_lo = static_cast<uint64_t>(result_hi);
    }
    *this = BasicDecimal128(result_hi, result_lo);
  }
  return *this;
}

namespace {

// Convenience wrapper type over 128 bit unsigned integers. We opt not to
// replace the uint128_t type in int128_internal.h because it would require
// significantly more implementation work to be done. This class merely
// provides the minimum necessary set of functions to perform 128+ bit
// multiplication operations when there may or may not be native support.
#ifdef ARROW_USE_NATIVE_INT128
struct uint128_t {
  uint128_t() {}
  uint128_t(uint64_t hi, uint64_t lo) : val_((static_cast<__uint128_t>(hi) << 64) | lo) {}
  explicit uint128_t(const BasicDecimal128& decimal) {
    val_ = (static_cast<__uint128_t>(decimal.high_bits()) << 64) | decimal.low_bits();
  }

  explicit uint128_t(uint64_t value) : val_(value) {}

  uint64_t hi() { return val_ >> 64; }
  uint64_t lo() { return val_ & kInt64Mask; }

  uint128_t& operator+=(const uint128_t& other) {
    val_ += other.val_;
    return *this;
  }

  uint128_t& operator*=(const uint128_t& other) {
    val_ *= other.val_;
    return *this;
  }

  __uint128_t val_;
};

#else
// Multiply two 64 bit word components into a 128 bit result, with high bits
// stored in hi and low bits in lo.
inline void ExtendAndMultiply(uint64_t x, uint64_t y, uint64_t* hi, uint64_t* lo) {
  // Perform multiplication on two 64 bit words x and y into a 128 bit result
  // by splitting up x and y into 32 bit high/low bit components,
  // allowing us to represent the multiplication as
  // x * y = x_lo * y_lo + x_hi * y_lo * 2^32 + y_hi * x_lo * 2^32
  // + x_hi * y_hi * 2^64
  //
  // Now, consider the final output as lo_lo || lo_hi || hi_lo || hi_hi
  // Therefore,
  // lo_lo is (x_lo * y_lo)_lo,
  // lo_hi is ((x_lo * y_lo)_hi + (x_hi * y_lo)_lo + (x_lo * y_hi)_lo)_lo,
  // hi_lo is ((x_hi * y_hi)_lo + (x_hi * y_lo)_hi + (x_lo * y_hi)_hi)_hi,
  // hi_hi is (x_hi * y_hi)_hi
  const uint64_t x_lo = x & kInt32Mask;
  const uint64_t y_lo = y & kInt32Mask;
  const uint64_t x_hi = x >> 32;
  const uint64_t y_hi = y >> 32;

  const uint64_t t = x_lo * y_lo;
  const uint64_t t_lo = t & kInt32Mask;
  const uint64_t t_hi = t >> 32;

  const uint64_t u = x_hi * y_lo + t_hi;
  const uint64_t u_lo = u & kInt32Mask;
  const uint64_t u_hi = u >> 32;

  const uint64_t v = x_lo * y_hi + u_lo;
  const uint64_t v_hi = v >> 32;

  *hi = x_hi * y_hi + u_hi + v_hi;
  *lo = (v << 32) + t_lo;
}

struct uint128_t {
  uint128_t() {}
  uint128_t(uint64_t hi, uint64_t lo) : hi_(hi), lo_(lo) {}
  explicit uint128_t(const BasicDecimal128& decimal) {
    hi_ = decimal.high_bits();
    lo_ = decimal.low_bits();
  }

  uint64_t hi() const { return hi_; }
  uint64_t lo() const { return lo_; }

  uint128_t& operator+=(const uint128_t& other) {
    // To deduce the carry bit, we perform "65 bit" addition on the low bits and
    // seeing if the resulting high bit is 1. This is accomplished by shifting the
    // low bits to the right by 1 (chopping off the lowest bit), then adding 1 if the
    // result of adding the two chopped bits would have produced a carry.
    uint64_t carry = (((lo_ & other.lo_) & 1) + (lo_ >> 1) + (other.lo_ >> 1)) >> 63;
    hi_ += other.hi_ + carry;
    lo_ += other.lo_;
    return *this;
  }

  uint128_t& operator*=(const uint128_t& other) {
    uint128_t r;
    ExtendAndMultiply(lo_, other.lo_, &r.hi_, &r.lo_);
    r.hi_ += (hi_ * other.lo_) + (lo_ * other.hi_);
    *this = r;
    return *this;
  }

  uint64_t hi_;
  uint64_t lo_;
};
#endif

// Multiplies two N * 64 bit unsigned integer types, represented by a uint64_t
// array into a same sized output. Elements in the array should be in
// native endian order, and output will be the same. Overflow in multiplication
// will result in the lower N * 64 bits of the result being set.
template <int N>
inline void MultiplyUnsignedArray(const std::array<uint64_t, N>& lh,
                                  const std::array<uint64_t, N>& rh,
                                  std::array<uint64_t, N>* result) {
  const auto lh_le = bit_util::little_endian::Make(lh);
  const auto rh_le = bit_util::little_endian::Make(rh);
  auto result_le = bit_util::little_endian::Make(result);

  for (int j = 0; j < N; ++j) {
    uint64_t carry = 0;
    for (int i = 0; i < N - j; ++i) {
      uint128_t tmp(lh_le[i]);
      tmp *= uint128_t(rh_le[j]);
      tmp += uint128_t(result_le[i + j]);
      tmp += uint128_t(carry);
      result_le[i + j] = tmp.lo();
      carry = tmp.hi();
    }
  }
}

}  // namespace

BasicDecimal128& BasicDecimal128::operator*=(const BasicDecimal128& right) {
  // Since the max value of BasicDecimal128 is supposed to be 1e38 - 1 and the
  // min the negation taking the absolute values here should always be safe.
  const bool negate = Sign() != right.Sign();
  BasicDecimal128 x = BasicDecimal128::Abs(*this);
  BasicDecimal128 y = BasicDecimal128::Abs(right);
  uint128_t r(x);
  r *= uint128_t{y};
  *this = BasicDecimal128(static_cast<int64_t>(r.hi()), r.lo());
  if (negate) {
    Negate();
  }
  return *this;
}

/// Expands the given native endian array of uint64_t into a big endian array of
/// uint32_t. The value of input array is expected to be non-negative. The result_array
/// will remove leading zeros from the input array.
/// \param value_array a native endian array to represent the value
/// \param result_array a big endian array of length N*2 to set with the value
/// \result the output length of the array
template <size_t N>
static int64_t FillInArray(const std::array<uint64_t, N>& value_array,
                           uint32_t* result_array) {
  const auto value_array_le = bit_util::little_endian::Make(value_array);
  int64_t next_index = 0;
  // 1st loop to find out 1st non-negative value in input
  int64_t i = N - 1;
  for (; i >= 0; i--) {
    if (value_array_le[i] != 0) {
      if (value_array_le[i] <= std::numeric_limits<uint32_t>::max()) {
        result_array[next_index++] = static_cast<uint32_t>(value_array_le[i]);
        i--;
      }
      break;
    }
  }
  // 2nd loop to fill in the rest of the array.
  for (int64_t j = i; j >= 0; j--) {
    result_array[next_index++] = static_cast<uint32_t>(value_array_le[j] >> 32);
    result_array[next_index++] = static_cast<uint32_t>(value_array_le[j]);
  }
  return next_index;
}

/// Expands the given value into a big endian array of ints so that we can work on
/// it. The array will be converted to an absolute value and the was_negative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array a big endian array of length 4 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const BasicDecimal128& value, uint32_t* array,
                           bool& was_negative) {
  BasicDecimal128 abs_value = BasicDecimal128::Abs(value);
  was_negative = value.high_bits() < 0;
  uint64_t high = static_cast<uint64_t>(abs_value.high_bits());
  uint64_t low = abs_value.low_bits();

  // FillInArray(std::array<uint64_t, N>& value_array, uint32_t* result_array) is not
  // called here as the following code has better performance, to avoid regression on
  // BasicDecimal128 Division.
  if (high != 0) {
    if (high > std::numeric_limits<uint32_t>::max()) {
      array[0] = static_cast<uint32_t>(high >> 32);
      array[1] = static_cast<uint32_t>(high);
      array[2] = static_cast<uint32_t>(low >> 32);
      array[3] = static_cast<uint32_t>(low);
      return 4;
    }

    array[0] = static_cast<uint32_t>(high);
    array[1] = static_cast<uint32_t>(low >> 32);
    array[2] = static_cast<uint32_t>(low);
    return 3;
  }

  if (low > std::numeric_limits<uint32_t>::max()) {
    array[0] = static_cast<uint32_t>(low >> 32);
    array[1] = static_cast<uint32_t>(low);
    return 2;
  }

  if (low == 0) {
    return 0;
  }

  array[0] = static_cast<uint32_t>(low);
  return 1;
}

/// Expands the given value into a big endian array of ints so that we can work on
/// it. The array will be converted to an absolute value and the was_negative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array a big endian array of length 8 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const BasicDecimal256& value, uint32_t* array,
                           bool& was_negative) {
  BasicDecimal256 positive_value = value;
  was_negative = false;
  if (positive_value.IsNegative()) {
    positive_value.Negate();
    was_negative = true;
  }
  return FillInArray<4>(positive_value.native_endian_array(), array);
}

/// Shift the number in the array left by bits positions.
/// \param array the number to shift, must have length elements
/// \param length the number of entries in the array
/// \param bits the number of bits to shift (0 <= bits < 32)
static void ShiftArrayLeft(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = 0; i < length - 1; ++i) {
      array[i] = (array[i] << bits) | (array[i + 1] >> (32 - bits));
    }
    array[length - 1] <<= bits;
  }
}

/// Shift the number in the array right by bits positions.
/// \param array the number to shift, must have length elements
/// \param length the number of entries in the array
/// \param bits the number of bits to shift (0 <= bits < 32)
static inline void ShiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = length - 1; i > 0; --i) {
      array[i] = (array[i] >> bits) | (array[i - 1] << (32 - bits));
    }
    array[0] >>= bits;
  }
}

/// \brief Fix the signs of the result and remainder at the end of the division based on
/// the signs of the dividend and divisor.
template <class DecimalClass>
static inline void FixDivisionSigns(DecimalClass* result, DecimalClass* remainder,
                                    bool dividend_was_negative,
                                    bool divisor_was_negative) {
  if (dividend_was_negative != divisor_was_negative) {
    result->Negate();
  }

  if (dividend_was_negative) {
    remainder->Negate();
  }
}

/// \brief Build a native endian array of uint64_t from a big endian array of uint32_t.
template <size_t N>
static DecimalStatus BuildFromArray(std::array<uint64_t, N>* result_array,
                                    const uint32_t* array, int64_t length) {
  for (int64_t i = length - 2 * N - 1; i >= 0; i--) {
    if (array[i] != 0) {
      return DecimalStatus::kOverflow;
    }
  }
  int64_t next_index = length - 1;
  size_t i = 0;
  auto result_array_le = bit_util::little_endian::Make(result_array);
  for (; i < N && next_index >= 0; i++) {
    uint64_t lower_bits = array[next_index--];
    result_array_le[i] =
        (next_index < 0)
            ? lower_bits
            : ((static_cast<uint64_t>(array[next_index--]) << 32) + lower_bits);
  }
  for (; i < N; i++) {
    result_array_le[i] = 0;
  }
  return DecimalStatus::kSuccess;
}

/// \brief Build a BasicDecimal128 from a big endian array of uint32_t.
static DecimalStatus BuildFromArray(BasicDecimal128* value, const uint32_t* array,
                                    int64_t length) {
  std::array<uint64_t, 2> result_array;
  auto status = BuildFromArray(&result_array, array, length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  const auto result_array_le = bit_util::little_endian::Make(result_array);
  *value = {static_cast<int64_t>(result_array_le[1]), result_array_le[0]};
  return DecimalStatus::kSuccess;
}

/// \brief Build a BasicDecimal256 from a big endian array of uint32_t.
static DecimalStatus BuildFromArray(BasicDecimal256* value, const uint32_t* array,
                                    int64_t length) {
  std::array<uint64_t, 4> result_array;
  auto status = BuildFromArray(&result_array, array, length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  *value = BasicDecimal256(result_array);
  return DecimalStatus::kSuccess;
}

/// \brief Do a division where the divisor fits into a single 32 bit value.
template <class DecimalClass>
static inline DecimalStatus SingleDivide(const uint32_t* dividend,
                                         int64_t dividend_length, uint32_t divisor,
                                         DecimalClass* remainder,
                                         bool dividend_was_negative,
                                         bool divisor_was_negative,
                                         DecimalClass* result) {
  uint64_t r = 0;
  constexpr int64_t kDecimalArrayLength = DecimalClass::kBitWidth / sizeof(uint32_t) + 1;
  uint32_t result_array[kDecimalArrayLength];
  for (int64_t j = 0; j < dividend_length; j++) {
    r <<= 32;
    r += dividend[j];
    result_array[j] = static_cast<uint32_t>(r / divisor);
    r %= divisor;
  }
  auto status = BuildFromArray(result, result_array, dividend_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }

  *remainder = static_cast<int64_t>(r);
  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return DecimalStatus::kSuccess;
}

/// \brief Do a decimal division with remainder.
template <class DecimalClass>
static inline DecimalStatus DecimalDivide(const DecimalClass& dividend,
                                          const DecimalClass& divisor,
                                          DecimalClass* result, DecimalClass* remainder) {
  constexpr int64_t kDecimalArrayLength = DecimalClass::kBitWidth / sizeof(uint32_t);
  // Split the dividend and divisor into integer pieces so that we can
  // work on them.
  uint32_t dividend_array[kDecimalArrayLength + 1];
  uint32_t divisor_array[kDecimalArrayLength];
  bool dividend_was_negative;
  bool divisor_was_negative;
  // leave an extra zero before the dividend
  dividend_array[0] = 0;
  int64_t dividend_length =
      FillInArray(dividend, dividend_array + 1, dividend_was_negative) + 1;
  int64_t divisor_length = FillInArray(divisor, divisor_array, divisor_was_negative);

  // Handle some of the easy cases.
  if (dividend_length <= divisor_length) {
    *remainder = dividend;
    *result = 0;
    return DecimalStatus::kSuccess;
  }

  if (divisor_length == 0) {
    return DecimalStatus::kDivideByZero;
  }

  if (divisor_length == 1) {
    return SingleDivide(dividend_array, dividend_length, divisor_array[0], remainder,
                        dividend_was_negative, divisor_was_negative, result);
  }

  int64_t result_length = dividend_length - divisor_length;
  uint32_t result_array[kDecimalArrayLength];
  DCHECK_LE(result_length, kDecimalArrayLength);

  // Normalize by shifting both by a multiple of 2 so that
  // the digit guessing is better. The requirement is that
  // divisor_array[0] is greater than 2**31.
  int64_t normalize_bits = bit_util::CountLeadingZeros(divisor_array[0]);
  ShiftArrayLeft(divisor_array, divisor_length, normalize_bits);
  ShiftArrayLeft(dividend_array, dividend_length, normalize_bits);

  // compute each digit in the result
  for (int64_t j = 0; j < result_length; ++j) {
    // Guess the next digit. At worst it is two too large
    uint32_t guess = std::numeric_limits<uint32_t>::max();
    const auto high_dividend =
        static_cast<uint64_t>(dividend_array[j]) << 32 | dividend_array[j + 1];
    if (dividend_array[j] != divisor_array[0]) {
      guess = static_cast<uint32_t>(high_dividend / divisor_array[0]);
    }

    // catch all of the cases where guess is two too large and most of the
    // cases where it is one too large
    auto rhat = static_cast<uint32_t>(high_dividend -
                                      guess * static_cast<uint64_t>(divisor_array[0]));
    while (static_cast<uint64_t>(divisor_array[1]) * guess >
           (static_cast<uint64_t>(rhat) << 32) + dividend_array[j + 2]) {
      --guess;
      rhat += divisor_array[0];
      if (static_cast<uint64_t>(rhat) < divisor_array[0]) {
        break;
      }
    }

    // subtract off the guess * divisor from the dividend
    uint64_t mult = 0;
    for (int64_t i = divisor_length - 1; i >= 0; --i) {
      mult += static_cast<uint64_t>(guess) * divisor_array[i];
      uint32_t prev = dividend_array[j + i + 1];
      dividend_array[j + i + 1] -= static_cast<uint32_t>(mult);
      mult >>= 32;
      if (dividend_array[j + i + 1] > prev) {
        ++mult;
      }
    }
    uint32_t prev = dividend_array[j];
    dividend_array[j] -= static_cast<uint32_t>(mult);

    // if guess was too big, we add back divisor
    if (dividend_array[j] > prev) {
      --guess;
      uint32_t carry = 0;
      for (int64_t i = divisor_length - 1; i >= 0; --i) {
        const auto sum =
            static_cast<uint64_t>(divisor_array[i]) + dividend_array[j + i + 1] + carry;
        dividend_array[j + i + 1] = static_cast<uint32_t>(sum);
        carry = static_cast<uint32_t>(sum >> 32);
      }
      dividend_array[j] += carry;
    }

    result_array[j] = guess;
  }

  // denormalize the remainder
  ShiftArrayRight(dividend_array, dividend_length, normalize_bits);

  // return result and remainder
  auto status = BuildFromArray(result, result_array, result_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }
  status = BuildFromArray(remainder, dividend_array, dividend_length);
  if (status != DecimalStatus::kSuccess) {
    return status;
  }

  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return DecimalStatus::kSuccess;
}

DecimalStatus BasicDecimal128::Divide(const BasicDecimal128& divisor,
                                      BasicDecimal128* result,
                                      BasicDecimal128* remainder) const {
  return DecimalDivide(*this, divisor, result, remainder);
}

bool operator<(const BasicDecimal128& left, const BasicDecimal128& right) {
  return left.high_bits() < right.high_bits() ||
         (left.high_bits() == right.high_bits() && left.low_bits() < right.low_bits());
}

bool operator<=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator>(left, right);
}

bool operator>(const BasicDecimal128& left, const BasicDecimal128& right) {
  return operator<(right, left);
}

bool operator>=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator<(left, right);
}

BasicDecimal128 operator-(const BasicDecimal128& operand) {
  BasicDecimal128 result(operand.high_bits(), operand.low_bits());
  return result.Negate();
}

BasicDecimal128 operator~(const BasicDecimal128& operand) {
  BasicDecimal128 result(~operand.high_bits(), ~operand.low_bits());
  return result;
}

BasicDecimal128 operator+(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result += right;
  return result;
}

BasicDecimal128 operator-(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result -= right;
  return result;
}

BasicDecimal128 operator*(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 result(left.high_bits(), left.low_bits());
  result *= right;
  return result;
}

BasicDecimal128 operator/(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  BasicDecimal128 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return result;
}

BasicDecimal128 operator%(const BasicDecimal128& left, const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  BasicDecimal128 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return remainder;
}

template <class DecimalClass>
static bool RescaleWouldCauseDataLoss(const DecimalClass& value, int32_t delta_scale,
                                      const DecimalClass& multiplier,
                                      DecimalClass* result) {
  if (delta_scale < 0) {
    DCHECK_NE(multiplier, 0);
    DecimalClass remainder;
    auto status = value.Divide(multiplier, result, &remainder);
    DCHECK_EQ(status, DecimalStatus::kSuccess);
    return remainder != 0;
  }

  *result = value * multiplier;
  return (value < 0) ? *result > value : *result < value;
}

template <class DecimalClass>
DecimalStatus DecimalRescale(const DecimalClass& value, int32_t original_scale,
                             int32_t new_scale, DecimalClass* out) {
  DCHECK_NE(out, nullptr);

  if (original_scale == new_scale) {
    *out = value;
    return DecimalStatus::kSuccess;
  }

  const int32_t delta_scale = new_scale - original_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);

  DecimalClass multiplier = DecimalClass::GetScaleMultiplier(abs_delta_scale);

  const bool rescale_would_cause_data_loss =
      RescaleWouldCauseDataLoss(value, delta_scale, multiplier, out);

  // Fail if we overflow or truncate
  if (ARROW_PREDICT_FALSE(rescale_would_cause_data_loss)) {
    return DecimalStatus::kRescaleDataLoss;
  }

  return DecimalStatus::kSuccess;
}

DecimalStatus BasicDecimal128::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal128* out) const {
  return DecimalRescale(*this, original_scale, new_scale, out);
}

void BasicDecimal128::GetWholeAndFraction(int scale, BasicDecimal128* whole,
                                          BasicDecimal128* fraction) const {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  BasicDecimal128 multiplier(kDecimal128PowersOfTen[scale]);
  auto s = Divide(multiplier, whole, fraction);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
}

const BasicDecimal128& BasicDecimal128::GetScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  return kDecimal128PowersOfTen[scale];
}

const BasicDecimal128& BasicDecimal128::GetHalfScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  return kDecimal128HalfPowersOfTen[scale];
}

const BasicDecimal128& BasicDecimal128::GetMaxValue() { return kMaxDecimal128Value; }

BasicDecimal128 BasicDecimal128::GetMaxValue(int32_t precision) {
  DCHECK_GE(precision, 0);
  DCHECK_LE(precision, 38);
  return kDecimal128PowersOfTen[precision] - 1;
}

BasicDecimal128 BasicDecimal128::IncreaseScaleBy(int32_t increase_by) const {
  DCHECK_GE(increase_by, 0);
  DCHECK_LE(increase_by, 38);

  return (*this) * kDecimal128PowersOfTen[increase_by];
}

BasicDecimal128 BasicDecimal128::ReduceScaleBy(int32_t reduce_by, bool round) const {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 38);

  if (reduce_by == 0) {
    return *this;
  }

  BasicDecimal128 divisor(kDecimal128PowersOfTen[reduce_by]);
  BasicDecimal128 result;
  BasicDecimal128 remainder;
  auto s = Divide(divisor, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  if (round) {
    auto divisor_half = kDecimal128HalfPowersOfTen[reduce_by];
    if (remainder.Abs() >= divisor_half) {
      result += Sign();
    }
  }
  return result;
}

int32_t BasicDecimal128::CountLeadingBinaryZeros() const {
  DCHECK_GE(*this, BasicDecimal128(0));

  if (high_bits() == 0) {
    return bit_util::CountLeadingZeros(low_bits()) + 64;
  } else {
    return bit_util::CountLeadingZeros(static_cast<uint64_t>(high_bits()));
  }
}

BasicDecimal256& BasicDecimal256::Negate() {
  auto array_le = bit_util::little_endian::Make(&array_);
  uint64_t carry = 1;
  for (size_t i = 0; i < array_.size(); ++i) {
    uint64_t& elem = array_le[i];
    elem = ~elem + carry;
    carry &= (elem == 0);
  }
  return *this;
}

BasicDecimal256& BasicDecimal256::Abs() { return *this < 0 ? Negate() : *this; }

BasicDecimal256 BasicDecimal256::Abs(const BasicDecimal256& in) {
  BasicDecimal256 result(in);
  return result.Abs();
}

BasicDecimal256& BasicDecimal256::operator+=(const BasicDecimal256& right) {
  auto array_le = bit_util::little_endian::Make(&array_);
  const auto right_array_le = bit_util::little_endian::Make(right.array_);
  uint64_t carry = 0;
  for (size_t i = 0; i < array_.size(); i++) {
    const uint64_t right_value = right_array_le[i];
    uint64_t sum = right_value + carry;
    carry = 0;
    if (sum < right_value) {
      carry += 1;
    }
    sum += array_le[i];
    if (sum < array_le[i]) {
      carry += 1;
    }
    array_le[i] = sum;
  }
  return *this;
}

BasicDecimal256& BasicDecimal256::operator-=(const BasicDecimal256& right) {
  *this += -right;
  return *this;
}

BasicDecimal256& BasicDecimal256::operator<<=(uint32_t bits) {
  if (bits == 0) {
    return *this;
  }
  const int cross_word_shift = bits / 64;
  if (cross_word_shift >= kNumWords) {
    array_ = {0, 0, 0, 0};
    return *this;
  }
  uint32_t in_word_shift = bits % 64;
  auto array_le = bit_util::little_endian::Make(&array_);
  for (int i = kNumWords - 1; i >= cross_word_shift; i--) {
    // Account for shifts larger then 64 bits
    array_le[i] = array_le[i - cross_word_shift];
    array_le[i] <<= in_word_shift;
    if (in_word_shift != 0 && i >= cross_word_shift + 1) {
      array_le[i] |= array_le[i - (cross_word_shift + 1)] >> (64 - in_word_shift);
    }
  }
  for (int i = cross_word_shift - 1; i >= 0; i--) {
    array_le[i] = 0;
  }
  return *this;
}

BasicDecimal256& BasicDecimal256::operator>>=(uint32_t bits) {
  if (bits == 0) {
    return *this;
  }
  const uint64_t extended =
      static_cast<uint64_t>(static_cast<int64_t>(array_[kHighWordIndex]) >> 63);
  const int cross_word_shift = bits / 64;
  if (cross_word_shift >= kNumWords) {
    array_.fill(extended);
    return *this;
  }
  const uint32_t in_word_shift = bits % 64;
  const auto array_le = little_endian_array();
  // Initialize with sign-extended words
  WordArray shifted_le;
  shifted_le.fill(extended);
  // Iterate from LSW to MSW
  for (int i = cross_word_shift; i < kNumWords; ++i) {
    shifted_le[i - cross_word_shift] = array_le[i] >> in_word_shift;
    if (in_word_shift != 0) {
      const uint64_t carry_bits = (i + 1 < kNumWords ? array_le[i + 1] : extended)
                                  << (64 - in_word_shift);
      shifted_le[i - cross_word_shift] |= carry_bits;
    }
  }
  array_ = bit_util::little_endian::ToNative(shifted_le);
  return *this;
}

BasicDecimal256& BasicDecimal256::operator*=(const BasicDecimal256& right) {
  // Since the max value of BasicDecimal256 is supposed to be 1e76 - 1 and the
  // min the negation taking the absolute values here should always be safe.
  const bool negate = Sign() != right.Sign();
  BasicDecimal256 x = BasicDecimal256::Abs(*this);
  BasicDecimal256 y = BasicDecimal256::Abs(right);

  std::array<uint64_t, 4> res{0, 0, 0, 0};
  MultiplyUnsignedArray<4>(x.array_, y.array_, &res);
  array_ = res;
  if (negate) {
    Negate();
  }
  return *this;
}

DecimalStatus BasicDecimal256::Divide(const BasicDecimal256& divisor,
                                      BasicDecimal256* result,
                                      BasicDecimal256* remainder) const {
  return DecimalDivide(*this, divisor, result, remainder);
}

DecimalStatus BasicDecimal256::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal256* out) const {
  return DecimalRescale(*this, original_scale, new_scale, out);
}

BasicDecimal256 BasicDecimal256::IncreaseScaleBy(int32_t increase_by) const {
  DCHECK_GE(increase_by, 0);
  DCHECK_LE(increase_by, 76);

  return (*this) * kDecimal256PowersOfTen[increase_by];
}

BasicDecimal256 BasicDecimal256::ReduceScaleBy(int32_t reduce_by, bool round) const {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 76);

  if (reduce_by == 0) {
    return *this;
  }

  BasicDecimal256 divisor(kDecimal256PowersOfTen[reduce_by]);
  BasicDecimal256 result;
  BasicDecimal256 remainder;
  auto s = Divide(divisor, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  if (round) {
    auto divisor_half = kDecimal256HalfPowersOfTen[reduce_by];
    if (remainder.Abs() >= divisor_half) {
      result += Sign();
    }
  }
  return result;
}

bool BasicDecimal256::FitsInPrecision(int32_t precision) const {
  DCHECK_GT(precision, 0);
  DCHECK_LE(precision, 76);
  return BasicDecimal256::Abs(*this) < kDecimal256PowersOfTen[precision];
}

void BasicDecimal256::GetWholeAndFraction(int scale, BasicDecimal256* whole,
                                          BasicDecimal256* fraction) const {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 76);

  BasicDecimal256 multiplier(kDecimal256PowersOfTen[scale]);
  auto s = Divide(multiplier, whole, fraction);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
}

const BasicDecimal256& BasicDecimal256::GetScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 76);

  return kDecimal256PowersOfTen[scale];
}

const BasicDecimal256& BasicDecimal256::GetHalfScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 76);

  return kDecimal256HalfPowersOfTen[scale];
}

BasicDecimal256 BasicDecimal256::GetMaxValue(int32_t precision) {
  DCHECK_GE(precision, 0);
  DCHECK_LE(precision, 76);
  return kDecimal256PowersOfTen[precision] + (-1);
}

BasicDecimal256 operator*(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 result = left;
  result *= right;
  return result;
}

bool operator<(const BasicDecimal256& left, const BasicDecimal256& right) {
  const auto lhs_le = bit_util::little_endian::Make(left.native_endian_array());
  const auto rhs_le = bit_util::little_endian::Make(right.native_endian_array());
  return lhs_le[3] != rhs_le[3]
             ? static_cast<int64_t>(lhs_le[3]) < static_cast<int64_t>(rhs_le[3])
         : lhs_le[2] != rhs_le[2] ? lhs_le[2] < rhs_le[2]
         : lhs_le[1] != rhs_le[1] ? lhs_le[1] < rhs_le[1]
                                  : lhs_le[0] < rhs_le[0];
}

BasicDecimal256 operator-(const BasicDecimal256& operand) {
  BasicDecimal256 result(operand);
  return result.Negate();
}

BasicDecimal256 operator~(const BasicDecimal256& operand) {
  const std::array<uint64_t, 4>& arr = operand.native_endian_array();
  BasicDecimal256 result({~arr[0], ~arr[1], ~arr[2], ~arr[3]});
  return result;
}

BasicDecimal256& BasicDecimal256::operator/=(const BasicDecimal256& right) {
  BasicDecimal256 remainder;
  auto s = Divide(right, this, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return *this;
}

BasicDecimal256 operator+(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 sum = left;
  sum += right;
  return sum;
}

BasicDecimal256 operator/(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 remainder;
  BasicDecimal256 result;
  auto s = left.Divide(right, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return result;
}

// Explicitly instantiate template base class, for DLL linking on Windows
template class GenericBasicDecimal<BasicDecimal128, 128>;
template class GenericBasicDecimal<BasicDecimal256, 256>;

}  // namespace arrow
