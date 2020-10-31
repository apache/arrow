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
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace arrow {

using internal::SafeLeftShift;
using internal::SafeSignedAdd;

static const BasicDecimal128 ScaleMultipliers[] = {
    BasicDecimal128(1LL),
    BasicDecimal128(10LL),
    BasicDecimal128(100LL),
    BasicDecimal128(1000LL),
    BasicDecimal128(10000LL),
    BasicDecimal128(100000LL),
    BasicDecimal128(1000000LL),
    BasicDecimal128(10000000LL),
    BasicDecimal128(100000000LL),
    BasicDecimal128(1000000000LL),
    BasicDecimal128(10000000000LL),
    BasicDecimal128(100000000000LL),
    BasicDecimal128(1000000000000LL),
    BasicDecimal128(10000000000000LL),
    BasicDecimal128(100000000000000LL),
    BasicDecimal128(1000000000000000LL),
    BasicDecimal128(10000000000000000LL),
    BasicDecimal128(100000000000000000LL),
    BasicDecimal128(1000000000000000000LL),
    BasicDecimal128(0LL, 10000000000000000000ULL),
    BasicDecimal128(5LL, 7766279631452241920ULL),
    BasicDecimal128(54LL, 3875820019684212736ULL),
    BasicDecimal128(542LL, 1864712049423024128ULL),
    BasicDecimal128(5421LL, 200376420520689664ULL),
    BasicDecimal128(54210LL, 2003764205206896640ULL),
    BasicDecimal128(542101LL, 1590897978359414784ULL),
    BasicDecimal128(5421010LL, 15908979783594147840ULL),
    BasicDecimal128(54210108LL, 11515845246265065472ULL),
    BasicDecimal128(542101086LL, 4477988020393345024ULL),
    BasicDecimal128(5421010862LL, 7886392056514347008ULL),
    BasicDecimal128(54210108624LL, 5076944270305263616ULL),
    BasicDecimal128(542101086242LL, 13875954555633532928ULL),
    BasicDecimal128(5421010862427LL, 9632337040368467968ULL),
    BasicDecimal128(54210108624275LL, 4089650035136921600ULL),
    BasicDecimal128(542101086242752LL, 4003012203950112768ULL),
    BasicDecimal128(5421010862427522LL, 3136633892082024448ULL),
    BasicDecimal128(54210108624275221LL, 12919594847110692864ULL),
    BasicDecimal128(542101086242752217LL, 68739955140067328ULL),
    BasicDecimal128(5421010862427522170LL, 687399551400673280ULL)};

static const BasicDecimal128 ScaleMultipliersHalf[] = {
    BasicDecimal128(0ULL),
    BasicDecimal128(5ULL),
    BasicDecimal128(50ULL),
    BasicDecimal128(500ULL),
    BasicDecimal128(5000ULL),
    BasicDecimal128(50000ULL),
    BasicDecimal128(500000ULL),
    BasicDecimal128(5000000ULL),
    BasicDecimal128(50000000ULL),
    BasicDecimal128(500000000ULL),
    BasicDecimal128(5000000000ULL),
    BasicDecimal128(50000000000ULL),
    BasicDecimal128(500000000000ULL),
    BasicDecimal128(5000000000000ULL),
    BasicDecimal128(50000000000000ULL),
    BasicDecimal128(500000000000000ULL),
    BasicDecimal128(5000000000000000ULL),
    BasicDecimal128(50000000000000000ULL),
    BasicDecimal128(500000000000000000ULL),
    BasicDecimal128(5000000000000000000ULL),
    BasicDecimal128(2LL, 13106511852580896768ULL),
    BasicDecimal128(27LL, 1937910009842106368ULL),
    BasicDecimal128(271LL, 932356024711512064ULL),
    BasicDecimal128(2710LL, 9323560247115120640ULL),
    BasicDecimal128(27105LL, 1001882102603448320ULL),
    BasicDecimal128(271050LL, 10018821026034483200ULL),
    BasicDecimal128(2710505LL, 7954489891797073920ULL),
    BasicDecimal128(27105054LL, 5757922623132532736ULL),
    BasicDecimal128(271050543LL, 2238994010196672512ULL),
    BasicDecimal128(2710505431LL, 3943196028257173504ULL),
    BasicDecimal128(27105054312LL, 2538472135152631808ULL),
    BasicDecimal128(271050543121LL, 6937977277816766464ULL),
    BasicDecimal128(2710505431213LL, 14039540557039009792ULL),
    BasicDecimal128(27105054312137LL, 11268197054423236608ULL),
    BasicDecimal128(271050543121376LL, 2001506101975056384ULL),
    BasicDecimal128(2710505431213761LL, 1568316946041012224ULL),
    BasicDecimal128(27105054312137610LL, 15683169460410122240ULL),
    BasicDecimal128(271050543121376108LL, 9257742014424809472ULL),
    BasicDecimal128(2710505431213761085LL, 343699775700336640ULL)};

#ifdef ARROW_USE_NATIVE_INT128
static constexpr uint64_t kInt64Mask = 0xFFFFFFFFFFFFFFFF;
#else
static constexpr uint64_t kInt32Mask = 0xFFFFFFFF;
#endif

// same as ScaleMultipliers[38] - 1
static constexpr BasicDecimal128 kMaxValue =
    BasicDecimal128(5421010862427522170LL, 687399551400673280ULL - 1);

#if ARROW_LITTLE_ENDIAN
BasicDecimal128::BasicDecimal128(const uint8_t* bytes)
    : BasicDecimal128(reinterpret_cast<const int64_t*>(bytes)[1],
                      reinterpret_cast<const uint64_t*>(bytes)[0]) {}
#else
BasicDecimal128::BasicDecimal128(const uint8_t* bytes)
    : BasicDecimal128(reinterpret_cast<const int64_t*>(bytes)[0],
                      reinterpret_cast<const uint64_t*>(bytes)[1]) {}
#endif

std::array<uint8_t, 16> BasicDecimal128::ToBytes() const {
  std::array<uint8_t, 16> out{{0}};
  ToBytes(out.data());
  return out;
}

void BasicDecimal128::ToBytes(uint8_t* out) const {
  DCHECK_NE(out, nullptr);
#if ARROW_LITTLE_ENDIAN
  reinterpret_cast<uint64_t*>(out)[0] = low_bits_;
  reinterpret_cast<int64_t*>(out)[1] = high_bits_;
#else
  reinterpret_cast<int64_t*>(out)[0] = high_bits_;
  reinterpret_cast<uint64_t*>(out)[1] = low_bits_;
#endif
}

BasicDecimal128& BasicDecimal128::Negate() {
  low_bits_ = ~low_bits_ + 1;
  high_bits_ = ~high_bits_;
  if (low_bits_ == 0) {
    high_bits_ = SafeSignedAdd<int64_t>(high_bits_, 1);
  }
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
  return BasicDecimal128::Abs(*this) < ScaleMultipliers[precision];
}

BasicDecimal128& BasicDecimal128::operator+=(const BasicDecimal128& right) {
  const uint64_t sum = low_bits_ + right.low_bits_;
  high_bits_ = SafeSignedAdd<int64_t>(high_bits_, right.high_bits_);
  if (sum < low_bits_) {
    high_bits_ = SafeSignedAdd<int64_t>(high_bits_, 1);
  }
  low_bits_ = sum;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator-=(const BasicDecimal128& right) {
  const uint64_t diff = low_bits_ - right.low_bits_;
  high_bits_ -= right.high_bits_;
  if (diff > low_bits_) {
    --high_bits_;
  }
  low_bits_ = diff;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator/=(const BasicDecimal128& right) {
  BasicDecimal128 remainder;
  auto s = Divide(right, this, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  return *this;
}

BasicDecimal128& BasicDecimal128::operator|=(const BasicDecimal128& right) {
  low_bits_ |= right.low_bits_;
  high_bits_ |= right.high_bits_;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator&=(const BasicDecimal128& right) {
  low_bits_ &= right.low_bits_;
  high_bits_ &= right.high_bits_;
  return *this;
}

BasicDecimal128& BasicDecimal128::operator<<=(uint32_t bits) {
  if (bits != 0) {
    if (bits < 64) {
      high_bits_ = SafeLeftShift(high_bits_, bits);
      high_bits_ |= (low_bits_ >> (64 - bits));
      low_bits_ <<= bits;
    } else if (bits < 128) {
      high_bits_ = static_cast<int64_t>(low_bits_) << (bits - 64);
      low_bits_ = 0;
    } else {
      high_bits_ = 0;
      low_bits_ = 0;
    }
  }
  return *this;
}

BasicDecimal128& BasicDecimal128::operator>>=(uint32_t bits) {
  if (bits != 0) {
    if (bits < 64) {
      low_bits_ >>= bits;
      low_bits_ |= static_cast<uint64_t>(high_bits_ << (64 - bits));
      high_bits_ = static_cast<int64_t>(static_cast<uint64_t>(high_bits_) >> bits);
    } else if (bits < 128) {
      low_bits_ = static_cast<uint64_t>(high_bits_ >> (bits - 64));
      high_bits_ = static_cast<int64_t>(high_bits_ >= 0L ? 0L : -1L);
    } else {
      high_bits_ = static_cast<int64_t>(high_bits_ >= 0L ? 0L : -1L);
      low_bits_ = static_cast<uint64_t>(high_bits_);
    }
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
// little endian order, and output will be the same. Overflow in multiplication
// will result in the lower N * 64 bits of the result being set.
template <int N>
inline void MultiplyUnsignedArray(const std::array<uint64_t, N>& lh,
                                  const std::array<uint64_t, N>& rh,
                                  std::array<uint64_t, N>* result) {
  for (int j = 0; j < N; ++j) {
    uint64_t carry = 0;
    for (int i = 0; i < N - j; ++i) {
      uint128_t tmp(lh[i]);
      tmp *= uint128_t(rh[j]);
      tmp += uint128_t((*result)[i + j]);
      tmp += uint128_t(carry);
      (*result)[i + j] = tmp.lo();
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
  high_bits_ = r.hi();
  low_bits_ = r.lo();
  if (negate) {
    Negate();
  }
  return *this;
}

/// Expands the given value into an array of ints so that we can work on
/// it. The array will be converted to an absolute value and the wasNegative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array an array of length 4 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const BasicDecimal128& value, uint32_t* array,
                           bool& was_negative) {
  uint64_t high;
  uint64_t low;
  const int64_t highbits = value.high_bits();
  const uint64_t lowbits = value.low_bits();

  if (highbits < 0) {
    low = ~lowbits + 1;
    high = static_cast<uint64_t>(~highbits);
    if (low == 0) {
      ++high;
    }
    was_negative = true;
  } else {
    low = lowbits;
    high = static_cast<uint64_t>(highbits);
    was_negative = false;
  }

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

  if (low >= std::numeric_limits<uint32_t>::max()) {
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
static void ShiftArrayRight(uint32_t* array, int64_t length, int64_t bits) {
  if (length > 0 && bits != 0) {
    for (int64_t i = length - 1; i > 0; --i) {
      array[i] = (array[i] >> bits) | (array[i - 1] << (32 - bits));
    }
    array[0] >>= bits;
  }
}

/// \brief Fix the signs of the result and remainder at the end of the division based on
/// the signs of the dividend and divisor.
static void FixDivisionSigns(BasicDecimal128* result, BasicDecimal128* remainder,
                             bool dividend_was_negative, bool divisor_was_negative) {
  if (dividend_was_negative != divisor_was_negative) {
    result->Negate();
  }

  if (dividend_was_negative) {
    remainder->Negate();
  }
}

/// \brief Build a BasicDecimal128 from a list of ints.
static DecimalStatus BuildFromArray(BasicDecimal128* value, uint32_t* array,
                                    int64_t length) {
  switch (length) {
    case 0:
      *value = {static_cast<int64_t>(0)};
      break;
    case 1:
      *value = {static_cast<int64_t>(array[0])};
      break;
    case 2:
      *value = {static_cast<int64_t>(0),
                (static_cast<uint64_t>(array[0]) << 32) + array[1]};
      break;
    case 3:
      *value = {static_cast<int64_t>(array[0]),
                (static_cast<uint64_t>(array[1]) << 32) + array[2]};
      break;
    case 4:
      *value = {(static_cast<int64_t>(array[0]) << 32) + array[1],
                (static_cast<uint64_t>(array[2]) << 32) + array[3]};
      break;
    case 5:
      if (array[0] != 0) {
        return DecimalStatus::kOverflow;
      }
      *value = {(static_cast<int64_t>(array[1]) << 32) + array[2],
                (static_cast<uint64_t>(array[3]) << 32) + array[4]};
      break;
    default:
      return DecimalStatus::kOverflow;
  }

  return DecimalStatus::kSuccess;
}

/// \brief Do a division where the divisor fits into a single 32 bit value.
static DecimalStatus SingleDivide(const uint32_t* dividend, int64_t dividend_length,
                                  uint32_t divisor, BasicDecimal128* remainder,
                                  bool dividend_was_negative, bool divisor_was_negative,
                                  BasicDecimal128* result) {
  uint64_t r = 0;
  uint32_t result_array[5];
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

DecimalStatus BasicDecimal128::Divide(const BasicDecimal128& divisor,
                                      BasicDecimal128* result,
                                      BasicDecimal128* remainder) const {
  // Split the dividend and divisor into integer pieces so that we can
  // work on them.
  uint32_t dividend_array[5];
  uint32_t divisor_array[4];
  bool dividend_was_negative;
  bool divisor_was_negative;
  // leave an extra zero before the dividend
  dividend_array[0] = 0;
  int64_t dividend_length =
      FillInArray(*this, dividend_array + 1, dividend_was_negative) + 1;
  int64_t divisor_length = FillInArray(divisor, divisor_array, divisor_was_negative);

  // Handle some of the easy cases.
  if (dividend_length <= divisor_length) {
    *remainder = *this;
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
  uint32_t result_array[4];
  DCHECK_LE(result_length, 4);

  // Normalize by shifting both by a multiple of 2 so that
  // the digit guessing is better. The requirement is that
  // divisor_array[0] is greater than 2**31.
  int64_t normalize_bits = BitUtil::CountLeadingZeros(divisor_array[0]);
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

bool operator==(const BasicDecimal128& left, const BasicDecimal128& right) {
  return left.high_bits() == right.high_bits() && left.low_bits() == right.low_bits();
}

bool operator!=(const BasicDecimal128& left, const BasicDecimal128& right) {
  return !operator==(left, right);
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

static bool RescaleWouldCauseDataLoss(const BasicDecimal128& value, int32_t delta_scale,
                                      int32_t abs_delta_scale, BasicDecimal128* result) {
  BasicDecimal128 multiplier(ScaleMultipliers[abs_delta_scale]);

  if (delta_scale < 0) {
    DCHECK_NE(multiplier, 0);
    BasicDecimal128 remainder;
    auto status = value.Divide(multiplier, result, &remainder);
    DCHECK_EQ(status, DecimalStatus::kSuccess);
    return remainder != 0;
  }

  *result = value * multiplier;
  return (value < 0) ? *result > value : *result < value;
}

DecimalStatus BasicDecimal128::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal128* out) const {
  DCHECK_NE(out, nullptr);

  if (original_scale == new_scale) {
    *out = *this;
    return DecimalStatus::kSuccess;
  }

  const int32_t delta_scale = new_scale - original_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);

  DCHECK_GE(abs_delta_scale, 1);
  DCHECK_LE(abs_delta_scale, 38);

  BasicDecimal128 result(*this);
  const bool rescale_would_cause_data_loss =
      RescaleWouldCauseDataLoss(result, delta_scale, abs_delta_scale, out);

  // Fail if we overflow or truncate
  if (ARROW_PREDICT_FALSE(rescale_would_cause_data_loss)) {
    return DecimalStatus::kRescaleDataLoss;
  }

  return DecimalStatus::kSuccess;
}

void BasicDecimal128::GetWholeAndFraction(int scale, BasicDecimal128* whole,
                                          BasicDecimal128* fraction) const {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  BasicDecimal128 multiplier(ScaleMultipliers[scale]);
  auto s = Divide(multiplier, whole, fraction);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
}

const BasicDecimal128& BasicDecimal128::GetScaleMultiplier(int32_t scale) {
  DCHECK_GE(scale, 0);
  DCHECK_LE(scale, 38);

  return ScaleMultipliers[scale];
}

const BasicDecimal128& BasicDecimal128::GetMaxValue() { return kMaxValue; }

BasicDecimal128 BasicDecimal128::IncreaseScaleBy(int32_t increase_by) const {
  DCHECK_GE(increase_by, 0);
  DCHECK_LE(increase_by, 38);

  return (*this) * ScaleMultipliers[increase_by];
}

BasicDecimal128 BasicDecimal128::ReduceScaleBy(int32_t reduce_by, bool round) const {
  DCHECK_GE(reduce_by, 0);
  DCHECK_LE(reduce_by, 38);

  if (reduce_by == 0) {
    return *this;
  }

  BasicDecimal128 divisor(ScaleMultipliers[reduce_by]);
  BasicDecimal128 result;
  BasicDecimal128 remainder;
  auto s = Divide(divisor, &result, &remainder);
  DCHECK_EQ(s, DecimalStatus::kSuccess);
  if (round) {
    auto divisor_half = ScaleMultipliersHalf[reduce_by];
    if (remainder.Abs() >= divisor_half) {
      if (result > 0) {
        result += 1;
      } else {
        result -= 1;
      }
    }
  }
  return result;
}

int32_t BasicDecimal128::CountLeadingBinaryZeros() const {
  DCHECK_GE(*this, BasicDecimal128(0));

  if (high_bits_ == 0) {
    return BitUtil::CountLeadingZeros(low_bits_) + 64;
  } else {
    return BitUtil::CountLeadingZeros(static_cast<uint64_t>(high_bits_));
  }
}

#if ARROW_LITTLE_ENDIAN
BasicDecimal256::BasicDecimal256(const uint8_t* bytes)
    : little_endian_array_(
          std::array<uint64_t, 4>({reinterpret_cast<const uint64_t*>(bytes)[0],
                                   reinterpret_cast<const uint64_t*>(bytes)[1],
                                   reinterpret_cast<const uint64_t*>(bytes)[2],
                                   reinterpret_cast<const uint64_t*>(bytes)[3]})) {}
#else
BasicDecimal256::BasicDecimal256(const uint8_t* bytes)
    : little_endian_array_(
          std::array<uint64_t, 4>({reinterpret_cast<const uint64_t*>(bytes)[3],
                                   reinterpret_cast<const uint64_t*>(bytes)[2],
                                   reinterpret_cast<const uint64_t*>(bytes)[1],
                                   reinterpret_cast<const uint64_t*>(bytes)[0]})) {}
#endif

BasicDecimal256& BasicDecimal256::Negate() {
  uint64_t carry = 1;
  for (uint64_t& elem : little_endian_array_) {
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

std::array<uint8_t, 32> BasicDecimal256::ToBytes() const {
  std::array<uint8_t, 32> out{{0}};
  ToBytes(out.data());
  return out;
}

void BasicDecimal256::ToBytes(uint8_t* out) const {
  DCHECK_NE(out, nullptr);
#if ARROW_LITTLE_ENDIAN
  reinterpret_cast<int64_t*>(out)[0] = little_endian_array_[0];
  reinterpret_cast<int64_t*>(out)[1] = little_endian_array_[1];
  reinterpret_cast<int64_t*>(out)[2] = little_endian_array_[2];
  reinterpret_cast<int64_t*>(out)[3] = little_endian_array_[3];
#else
  reinterpret_cast<int64_t*>(out)[0] = little_endian_array_[3];
  reinterpret_cast<int64_t*>(out)[1] = little_endian_array_[2];
  reinterpret_cast<int64_t*>(out)[2] = little_endian_array_[1];
  reinterpret_cast<int64_t*>(out)[3] = little_endian_array_[0];
#endif
}

BasicDecimal256& BasicDecimal256::operator*=(const BasicDecimal256& right) {
  // Since the max value of BasicDecimal256 is supposed to be 1e76 - 1 and the
  // min the negation taking the absolute values here should always be safe.
  const bool negate = Sign() != right.Sign();
  BasicDecimal256 x = BasicDecimal256::Abs(*this);
  BasicDecimal256 y = BasicDecimal256::Abs(right);

  uint128_t r_hi;
  uint128_t r_lo;
  std::array<uint64_t, 4> res{0, 0, 0, 0};
  MultiplyUnsignedArray<4>(x.little_endian_array_, y.little_endian_array_, &res);
  little_endian_array_ = res;
  if (negate) {
    Negate();
  }
  return *this;
}

DecimalStatus BasicDecimal256::Rescale(int32_t original_scale, int32_t new_scale,
                                       BasicDecimal256* out) const {
  if (original_scale == new_scale) {
    return DecimalStatus::kSuccess;
  }
  // TODO: implement.
  return DecimalStatus::kRescaleDataLoss;
}

BasicDecimal256 operator*(const BasicDecimal256& left, const BasicDecimal256& right) {
  BasicDecimal256 result = left;
  result *= right;
  return result;
}

bool operator<(const BasicDecimal256& left, const BasicDecimal256& right) {
  const std::array<uint64_t, 4>& lhs = left.little_endian_array();
  const std::array<uint64_t, 4>& rhs = right.little_endian_array();
  return lhs[3] != rhs[3]
             ? static_cast<int64_t>(lhs[3]) < static_cast<int64_t>(rhs[3])
             : lhs[2] != rhs[2] ? lhs[2] < rhs[2]
                                : lhs[1] != rhs[1] ? lhs[1] < rhs[1] : lhs[0] < rhs[0];
}

}  // namespace arrow
