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

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>

#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"

namespace arrow {

static const Decimal128 ScaleMultipliers[] = {
    Decimal128(0LL),
    Decimal128(10LL),
    Decimal128(100LL),
    Decimal128(1000LL),
    Decimal128(10000LL),
    Decimal128(100000LL),
    Decimal128(1000000LL),
    Decimal128(10000000LL),
    Decimal128(100000000LL),
    Decimal128(1000000000LL),
    Decimal128(10000000000LL),
    Decimal128(100000000000LL),
    Decimal128(1000000000000LL),
    Decimal128(10000000000000LL),
    Decimal128(100000000000000LL),
    Decimal128(1000000000000000LL),
    Decimal128(10000000000000000LL),
    Decimal128(100000000000000000LL),
    Decimal128(1000000000000000000LL),
    Decimal128(0LL, 10000000000000000000ULL),
    Decimal128(5LL, 7766279631452241920ULL),
    Decimal128(54LL, 3875820019684212736ULL),
    Decimal128(542LL, 1864712049423024128ULL),
    Decimal128(5421LL, 200376420520689664ULL),
    Decimal128(54210LL, 2003764205206896640ULL),
    Decimal128(542101LL, 1590897978359414784ULL),
    Decimal128(5421010LL, 15908979783594147840ULL),
    Decimal128(54210108LL, 11515845246265065472ULL),
    Decimal128(542101086LL, 4477988020393345024ULL),
    Decimal128(5421010862LL, 7886392056514347008ULL),
    Decimal128(54210108624LL, 5076944270305263616ULL),
    Decimal128(542101086242LL, 13875954555633532928ULL),
    Decimal128(5421010862427LL, 9632337040368467968ULL),
    Decimal128(54210108624275LL, 4089650035136921600ULL),
    Decimal128(542101086242752LL, 4003012203950112768ULL),
    Decimal128(5421010862427522LL, 3136633892082024448ULL),
    Decimal128(54210108624275221LL, 12919594847110692864ULL),
    Decimal128(542101086242752217LL, 68739955140067328ULL),
    Decimal128(5421010862427522170LL, 687399551400673280ULL)};

static constexpr uint64_t kIntMask = 0xFFFFFFFF;
static constexpr auto kCarryBit = static_cast<uint64_t>(1) << static_cast<uint64_t>(32);

Decimal128::Decimal128(const std::string& str) : Decimal128() {
  Status status(Decimal128::FromString(str, this));
  DCHECK(status.ok()) << status.message();
}

Decimal128::Decimal128(const uint8_t* bytes)
    : Decimal128(BitUtil::FromLittleEndian(reinterpret_cast<const int64_t*>(bytes)[1]),
                 BitUtil::FromLittleEndian(reinterpret_cast<const uint64_t*>(bytes)[0])) {
}

std::array<uint8_t, 16> Decimal128::ToBytes() const {
  std::array<uint8_t, 16> out{{0}};
  ToBytes(out.data());
  return out;
}

void Decimal128::ToBytes(uint8_t* out) const {
  DCHECK_NE(out, nullptr);
  reinterpret_cast<uint64_t*>(out)[0] = BitUtil::ToLittleEndian(low_bits_);
  reinterpret_cast<int64_t*>(out)[1] = BitUtil::ToLittleEndian(high_bits_);
}

static constexpr Decimal128 kTenTo36(static_cast<int64_t>(0xC097CE7BC90715),
                                     0xB34B9F1000000000);
static constexpr Decimal128 kTenTo18(0xDE0B6B3A7640000);

std::string Decimal128::ToIntegerString() const {
  Decimal128 remainder;
  std::stringstream buf;
  bool need_fill = false;

  // get anything above 10 ** 36 and print it
  Decimal128 top;
  Status s = Divide(kTenTo36, &top, &remainder);
  DCHECK(s.ok()) << s.message();

  if (top != 0) {
    buf << static_cast<int64_t>(top);
    remainder.Abs();
    need_fill = true;
  }

  // now get anything above 10 ** 18 and print it
  Decimal128 tail;
  s = remainder.Divide(kTenTo18, &top, &tail);

  if (need_fill || top != 0) {
    if (need_fill) {
      buf << std::setw(18) << std::setfill('0');
    } else {
      need_fill = true;
      tail.Abs();
    }

    buf << static_cast<int64_t>(top);
  }

  // finally print the tail, which is less than 10**18
  if (need_fill) {
    buf << std::setw(18) << std::setfill('0');
  }
  buf << static_cast<int64_t>(tail);
  return buf.str();
}

Decimal128::operator int64_t() const {
  DCHECK(high_bits_ == 0 || high_bits_ == -1)
      << "Trying to cast an Decimal128 greater than the value range of a "
         "int64_t. high_bits_ must be equal to 0 or -1, got: "
      << high_bits_;
  return static_cast<int64_t>(low_bits_);
}

static std::string ToStringNegativeScale(const std::string& str,
                                         int32_t adjusted_exponent, bool is_negative) {
  std::stringstream buf;

  size_t offset = 0;
  buf << str[offset++];

  if (is_negative) {
    buf << str[offset++];
  }

  buf << '.' << str.substr(offset, std::string::npos) << 'E' << std::showpos
      << adjusted_exponent;
  return buf.str();
}

std::string Decimal128::ToString(int32_t scale) const {
  const std::string str(ToIntegerString());

  if (scale == 0) {
    return str;
  }

  const bool is_negative = *this < 0;

  const auto len = static_cast<int32_t>(str.size());
  const auto is_negative_offset = static_cast<int32_t>(is_negative);
  const int32_t adjusted_exponent = -scale + (len - 1 - is_negative_offset);

  /// Note that the -6 is taken from the Java BigDecimal documentation.
  if (scale < 0 || adjusted_exponent < -6) {
    return ToStringNegativeScale(str, adjusted_exponent, is_negative);
  }

  if (is_negative) {
    if (len - 1 > scale) {
      const auto n = static_cast<size_t>(len - scale);
      return str.substr(0, n) + "." + str.substr(n, static_cast<size_t>(scale));
    }

    if (len - 1 == scale) {
      return "-0." + str.substr(1, std::string::npos);
    }

    std::string result("-0." + std::string(static_cast<size_t>(scale - len + 1), '0'));
    return result + str.substr(1, std::string::npos);
  }

  if (len > scale) {
    const auto n = static_cast<size_t>(len - scale);
    return str.substr(0, n) + "." + str.substr(n, static_cast<size_t>(scale));
  }

  if (len == scale) {
    return "0." + str;
  }

  return "0." + std::string(static_cast<size_t>(scale - len), '0') + str;
}

static constexpr auto kInt64DecimalDigits =
    static_cast<size_t>(std::numeric_limits<int64_t>::digits10);
static constexpr int64_t kPowersOfTen[kInt64DecimalDigits + 1] = {1LL,
                                                                  10LL,
                                                                  100LL,
                                                                  1000LL,
                                                                  10000LL,
                                                                  100000LL,
                                                                  1000000LL,
                                                                  10000000LL,
                                                                  100000000LL,
                                                                  1000000000LL,
                                                                  10000000000LL,
                                                                  100000000000LL,
                                                                  1000000000000LL,
                                                                  10000000000000LL,
                                                                  100000000000000LL,
                                                                  1000000000000000LL,
                                                                  10000000000000000LL,
                                                                  100000000000000000LL,
                                                                  1000000000000000000LL};

static void StringToInteger(const std::string& str, Decimal128* out) {
  using std::size_t;

  DCHECK_NE(out, nullptr) << "Decimal128 output variable cannot be nullptr";
  DCHECK_EQ(*out, 0)
      << "When converting a string to Decimal128 the initial output must be 0";

  const size_t length = str.length();

  DCHECK_GT(length, 0) << "length of parsed decimal string should be greater than 0";

  for (size_t posn = 0; posn < length;) {
    const size_t group = std::min(kInt64DecimalDigits, length - posn);
    const int64_t chunk = std::stoll(str.substr(posn, group));
    const int64_t multiple = kPowersOfTen[group];

    *out *= multiple;
    *out += chunk;

    posn += group;
  }
}

namespace {

struct DecimalComponents {
  std::string sign;
  std::string whole_digits;
  std::string fractional_digits;
  std::string exponent_sign;
  std::string exponent_digits;
};

inline bool IsSign(char c) { return c == '-' || c == '+'; }

inline bool IsDot(char c) { return c == '.'; }

inline bool IsDigit(char c) { return c >= '0' && c <= '9'; }

inline bool StartsExponent(char c) { return c == 'e' || c == 'E'; }

inline size_t ParseDigitsRun(const char* s, size_t start, size_t size, std::string* out) {
  size_t pos;
  for (pos = start; pos < size; ++pos) {
    if (!IsDigit(s[pos])) {
      break;
    }
  }
  *out = std::string(s + start, pos - start);
  return pos;
}

bool ParseDecimalComponents(const char* s, size_t size, DecimalComponents* out) {
  size_t pos = 0;

  if (size == 0) {
    return false;
  }
  // Sign of the number
  if (IsSign(s[pos])) {
    out->sign = std::string(s + pos, 1);
    ++pos;
  }
  // First run of digits
  pos = ParseDigitsRun(s, pos, size, &out->whole_digits);
  if (pos == size) {
    return !out->whole_digits.empty();
  }
  // Optional dot (if given in fractional form)
  bool has_dot = IsDot(s[pos]);
  if (has_dot) {
    // Second run of digits
    ++pos;
    pos = ParseDigitsRun(s, pos, size, &out->fractional_digits);
  }
  if (out->whole_digits.empty() && out->fractional_digits.empty()) {
    // Need at least some digits (whole or fractional)
    return false;
  }
  if (pos == size) {
    return true;
  }
  // Optional exponent
  if (StartsExponent(s[pos])) {
    ++pos;
    if (pos == size) {
      return false;
    }
    // Optional exponent sign
    if (IsSign(s[pos])) {
      out->exponent_sign = std::string(s + pos, 1);
      ++pos;
    }
    pos = ParseDigitsRun(s, pos, size, &out->exponent_digits);
    if (out->exponent_digits.empty()) {
      // Need some exponent digits
      return false;
    }
  }
  return pos == size;
}

}  // namespace

Status Decimal128::FromString(const std::string& s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to decimal");
  }

  DecimalComponents dec;
  if (!ParseDecimalComponents(s.data(), s.size(), &dec)) {
    std::stringstream ss;
    ss << "The string '" << s << "' is not a valid decimal number";
    return Status::Invalid(ss.str());
  }
  std::string exponent_value = dec.exponent_sign + dec.exponent_digits;

  // Count number of significant digits (without leading zeros)
  size_t first_non_zero = dec.whole_digits.find_first_not_of('0');
  size_t significant_digits = dec.fractional_digits.size();
  if (first_non_zero != std::string::npos) {
    significant_digits += dec.whole_digits.size() - first_non_zero;
  }

  if (precision != nullptr) {
    *precision = static_cast<int32_t>(significant_digits);
  }

  if (scale != nullptr) {
    if (!exponent_value.empty()) {
      auto adjusted_exponent = static_cast<int32_t>(std::stol(exponent_value));
      auto len = static_cast<int32_t>(significant_digits);
      *scale = -adjusted_exponent + len - 1;
    } else {
      *scale = static_cast<int32_t>(dec.fractional_digits.size());
    }
  }

  if (out != nullptr) {
    *out = 0;
    StringToInteger(dec.whole_digits + dec.fractional_digits, out);
    if (dec.sign == "-") {
      out->Negate();
    }

    if (scale != nullptr && *scale < 0) {
      const int32_t abs_scale = std::abs(*scale);
      *out *= ScaleMultipliers[abs_scale];

      if (precision != nullptr) {
        *precision += abs_scale;
      }
      *scale = 0;
    }
  }

  return Status::OK();
}

Decimal128& Decimal128::Negate() {
  low_bits_ = ~low_bits_ + 1;
  high_bits_ = ~high_bits_;
  if (low_bits_ == 0) {
    ++high_bits_;
  }
  return *this;
}

Decimal128& Decimal128::Abs() { return *this < 0 ? Negate() : *this; }

Decimal128& Decimal128::operator+=(const Decimal128& right) {
  const uint64_t sum = low_bits_ + right.low_bits_;
  high_bits_ += right.high_bits_;
  if (sum < low_bits_) {
    ++high_bits_;
  }
  low_bits_ = sum;
  return *this;
}

Decimal128& Decimal128::operator-=(const Decimal128& right) {
  const uint64_t diff = low_bits_ - right.low_bits_;
  high_bits_ -= right.high_bits_;
  if (diff > low_bits_) {
    --high_bits_;
  }
  low_bits_ = diff;
  return *this;
}

Decimal128& Decimal128::operator/=(const Decimal128& right) {
  Decimal128 remainder;
  Status s = Divide(right, this, &remainder);
  DCHECK(s.ok());
  return *this;
}

Decimal128& Decimal128::operator|=(const Decimal128& right) {
  low_bits_ |= right.low_bits_;
  high_bits_ |= right.high_bits_;
  return *this;
}

Decimal128& Decimal128::operator&=(const Decimal128& right) {
  low_bits_ &= right.low_bits_;
  high_bits_ &= right.high_bits_;
  return *this;
}

Decimal128& Decimal128::operator<<=(uint32_t bits) {
  if (bits != 0) {
    if (bits < 64) {
      high_bits_ <<= bits;
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

Decimal128& Decimal128::operator>>=(uint32_t bits) {
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

Decimal128& Decimal128::operator*=(const Decimal128& right) {
  // Break the left and right numbers into 32 bit chunks
  // so that we can multiply them without overflow.
  const uint64_t L0 = static_cast<uint64_t>(high_bits_) >> 32;
  const uint64_t L1 = static_cast<uint64_t>(high_bits_) & kIntMask;
  const uint64_t L2 = low_bits_ >> 32;
  const uint64_t L3 = low_bits_ & kIntMask;

  const uint64_t R0 = static_cast<uint64_t>(right.high_bits_) >> 32;
  const uint64_t R1 = static_cast<uint64_t>(right.high_bits_) & kIntMask;
  const uint64_t R2 = right.low_bits_ >> 32;
  const uint64_t R3 = right.low_bits_ & kIntMask;

  uint64_t product = L3 * R3;
  low_bits_ = product & kIntMask;

  uint64_t sum = product >> 32;

  product = L2 * R3;
  sum += product;

  product = L3 * R2;
  sum += product;

  low_bits_ += sum << 32;

  high_bits_ = static_cast<int64_t>(sum < product ? kCarryBit : 0);
  if (sum < product) {
    high_bits_ += kCarryBit;
  }

  high_bits_ += static_cast<int64_t>(sum >> 32);
  high_bits_ += L1 * R3 + L2 * R2 + L3 * R1;
  high_bits_ += (L0 * R3 + L1 * R2 + L2 * R1 + L3 * R0) << 32;
  return *this;
}

/// Expands the given value into an array of ints so that we can work on
/// it. The array will be converted to an absolute value and the wasNegative
/// flag will be set appropriately. The array will remove leading zeros from
/// the value.
/// \param array an array of length 4 to set with the value
/// \param was_negative a flag for whether the value was original negative
/// \result the output length of the array
static int64_t FillInArray(const Decimal128& value, uint32_t* array, bool& was_negative) {
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
static void FixDivisionSigns(Decimal128* result, Decimal128* remainder,
                             bool dividend_was_negative, bool divisor_was_negative) {
  if (dividend_was_negative != divisor_was_negative) {
    result->Negate();
  }

  if (dividend_was_negative) {
    remainder->Negate();
  }
}

/// \brief Build a Decimal128 from a list of ints.
static Status BuildFromArray(Decimal128* value, uint32_t* array, int64_t length) {
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
        return Status::Invalid("Can't build Decimal128 with 5 ints.");
      }
      *value = {(static_cast<int64_t>(array[1]) << 32) + array[2],
                (static_cast<uint64_t>(array[3]) << 32) + array[4]};
      break;
    default:
      return Status::Invalid("Unsupported length for building Decimal128");
  }

  return Status::OK();
}

/// \brief Do a division where the divisor fits into a single 32 bit value.
static Status SingleDivide(const uint32_t* dividend, int64_t dividend_length,
                           uint32_t divisor, Decimal128* remainder,
                           bool dividend_was_negative, bool divisor_was_negative,
                           Decimal128* result) {
  uint64_t r = 0;
  uint32_t result_array[5];
  for (int64_t j = 0; j < dividend_length; j++) {
    r <<= 32;
    r += dividend[j];
    result_array[j] = static_cast<uint32_t>(r / divisor);
    r %= divisor;
  }
  RETURN_NOT_OK(BuildFromArray(result, result_array, dividend_length));
  *remainder = static_cast<int64_t>(r);
  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return Status::OK();
}

Status Decimal128::Divide(const Decimal128& divisor, Decimal128* result,
                          Decimal128* remainder) const {
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
    return Status::OK();
  }

  if (divisor_length == 0) {
    return Status::Invalid("Division by 0 in Decimal128");
  }

  if (divisor_length == 1) {
    return SingleDivide(dividend_array, dividend_length, divisor_array[0], remainder,
                        dividend_was_negative, divisor_was_negative, result);
  }

  int64_t result_length = dividend_length - divisor_length;
  uint32_t result_array[4];

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
  RETURN_NOT_OK(BuildFromArray(result, result_array, result_length));
  RETURN_NOT_OK(BuildFromArray(remainder, dividend_array, dividend_length));

  FixDivisionSigns(result, remainder, dividend_was_negative, divisor_was_negative);
  return Status::OK();
}

bool operator==(const Decimal128& left, const Decimal128& right) {
  return left.high_bits() == right.high_bits() && left.low_bits() == right.low_bits();
}

bool operator!=(const Decimal128& left, const Decimal128& right) {
  return !operator==(left, right);
}

bool operator<(const Decimal128& left, const Decimal128& right) {
  return left.high_bits() < right.high_bits() ||
         (left.high_bits() == right.high_bits() && left.low_bits() < right.low_bits());
}

bool operator<=(const Decimal128& left, const Decimal128& right) {
  return !operator>(left, right);
}

bool operator>(const Decimal128& left, const Decimal128& right) {
  return operator<(right, left);
}

bool operator>=(const Decimal128& left, const Decimal128& right) {
  return !operator<(left, right);
}

Decimal128 operator-(const Decimal128& operand) {
  Decimal128 result(operand.high_bits(), operand.low_bits());
  return result.Negate();
}

Decimal128 operator~(const Decimal128& operand) {
  Decimal128 result(~operand.high_bits(), ~operand.low_bits());
  return result;
}

Decimal128 operator+(const Decimal128& left, const Decimal128& right) {
  Decimal128 result(left.high_bits(), left.low_bits());
  result += right;
  return result;
}

Decimal128 operator-(const Decimal128& left, const Decimal128& right) {
  Decimal128 result(left.high_bits(), left.low_bits());
  result -= right;
  return result;
}

Decimal128 operator*(const Decimal128& left, const Decimal128& right) {
  Decimal128 result(left.high_bits(), left.low_bits());
  result *= right;
  return result;
}

Decimal128 operator/(const Decimal128& left, const Decimal128& right) {
  Decimal128 remainder;
  Decimal128 result;
  Status s = left.Divide(right, &result, &remainder);
  DCHECK(s.ok());
  return result;
}

Decimal128 operator%(const Decimal128& left, const Decimal128& right) {
  Decimal128 remainder;
  Decimal128 result;
  Status s = left.Divide(right, &result, &remainder);
  DCHECK(s.ok());
  return remainder;
}

static bool RescaleWouldCauseDataLoss(const Decimal128& value, int32_t delta_scale,
                                      int32_t abs_delta_scale, Decimal128* result) {
  Decimal128 multiplier(ScaleMultipliers[abs_delta_scale]);

  if (delta_scale < 0) {
    DCHECK_NE(multiplier, 0);
    Decimal128 remainder;
    Status status = value.Divide(multiplier, result, &remainder);
    DCHECK(status.ok()) << status.message();
    return remainder != 0;
  }

  *result = value * multiplier;
  return (value < 0) ? *result > value : *result < value;
}

Status Decimal128::Rescale(int32_t original_scale, int32_t new_scale,
                           Decimal128* out) const {
  DCHECK_NE(out, nullptr) << "out is nullptr";
  DCHECK_NE(original_scale, new_scale) << "original_scale != new_scale";

  const int32_t delta_scale = new_scale - original_scale;
  const int32_t abs_delta_scale = std::abs(delta_scale);

  DCHECK_GE(abs_delta_scale, 1);
  DCHECK_LE(abs_delta_scale, 38);

  Decimal128 result(*this);
  const bool rescale_would_cause_data_loss =
      RescaleWouldCauseDataLoss(result, delta_scale, abs_delta_scale, out);

  // Fail if we overflow or truncate
  if (ARROW_PREDICT_FALSE(rescale_would_cause_data_loss)) {
    std::stringstream buf;
    buf << "Rescaling decimal value " << ToString(original_scale)
        << " from original scale of " << original_scale << " to new scale of "
        << new_scale << " would cause data loss";
    return Status::Invalid(buf.str());
  }

  return Status::OK();
}

}  // namespace arrow
