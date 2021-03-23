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
#include <array>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <limits>
#include <ostream>
#include <sstream>
#include <string>

#include "arrow/status.h"
#include "arrow/util/decimal.h"
#include "arrow/util/endian.h"
#include "arrow/util/formatting.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::SafeLeftShift;
using internal::SafeSignedAdd;
using internal::uint128_t;

Decimal128::Decimal128(const std::string& str) : Decimal128() {
  *this = Decimal128::FromString(str).ValueOrDie();
}

static constexpr auto kInt64DecimalDigits =
    static_cast<size_t>(std::numeric_limits<int64_t>::digits10);

static constexpr uint64_t kUInt64PowersOfTen[kInt64DecimalDigits + 1] = {
    // clang-format off
    1ULL,
    10ULL,
    100ULL,
    1000ULL,
    10000ULL,
    100000ULL,
    1000000ULL,
    10000000ULL,
    100000000ULL,
    1000000000ULL,
    10000000000ULL,
    100000000000ULL,
    1000000000000ULL,
    10000000000000ULL,
    100000000000000ULL,
    1000000000000000ULL,
    10000000000000000ULL,
    100000000000000000ULL,
    1000000000000000000ULL
    // clang-format on
};

static constexpr float kFloatPowersOfTen[2 * 38 + 1] = {
    1e-38f, 1e-37f, 1e-36f, 1e-35f, 1e-34f, 1e-33f, 1e-32f, 1e-31f, 1e-30f, 1e-29f,
    1e-28f, 1e-27f, 1e-26f, 1e-25f, 1e-24f, 1e-23f, 1e-22f, 1e-21f, 1e-20f, 1e-19f,
    1e-18f, 1e-17f, 1e-16f, 1e-15f, 1e-14f, 1e-13f, 1e-12f, 1e-11f, 1e-10f, 1e-9f,
    1e-8f,  1e-7f,  1e-6f,  1e-5f,  1e-4f,  1e-3f,  1e-2f,  1e-1f,  1e0f,   1e1f,
    1e2f,   1e3f,   1e4f,   1e5f,   1e6f,   1e7f,   1e8f,   1e9f,   1e10f,  1e11f,
    1e12f,  1e13f,  1e14f,  1e15f,  1e16f,  1e17f,  1e18f,  1e19f,  1e20f,  1e21f,
    1e22f,  1e23f,  1e24f,  1e25f,  1e26f,  1e27f,  1e28f,  1e29f,  1e30f,  1e31f,
    1e32f,  1e33f,  1e34f,  1e35f,  1e36f,  1e37f,  1e38f};

static constexpr double kDoublePowersOfTen[2 * 38 + 1] = {
    1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29, 1e-28,
    1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17,
    1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,  1e-8,  1e-7,  1e-6,
    1e-5,  1e-4,  1e-3,  1e-2,  1e-1,  1e0,   1e1,   1e2,   1e3,   1e4,   1e5,
    1e6,   1e7,   1e8,   1e9,   1e10,  1e11,  1e12,  1e13,  1e14,  1e15,  1e16,
    1e17,  1e18,  1e19,  1e20,  1e21,  1e22,  1e23,  1e24,  1e25,  1e26,  1e27,
    1e28,  1e29,  1e30,  1e31,  1e32,  1e33,  1e34,  1e35,  1e36,  1e37,  1e38};

// On the Windows R toolchain, INFINITY is double type instead of float
static constexpr float kFloatInf = std::numeric_limits<float>::infinity();
static constexpr float kFloatPowersOfTen76[2 * 76 + 1] = {
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         0,         0,         0,         0,
    0,         0,         0,         1e-45f,    1e-44f,    1e-43f,    1e-42f,
    1e-41f,    1e-40f,    1e-39f,    1e-38f,    1e-37f,    1e-36f,    1e-35f,
    1e-34f,    1e-33f,    1e-32f,    1e-31f,    1e-30f,    1e-29f,    1e-28f,
    1e-27f,    1e-26f,    1e-25f,    1e-24f,    1e-23f,    1e-22f,    1e-21f,
    1e-20f,    1e-19f,    1e-18f,    1e-17f,    1e-16f,    1e-15f,    1e-14f,
    1e-13f,    1e-12f,    1e-11f,    1e-10f,    1e-9f,     1e-8f,     1e-7f,
    1e-6f,     1e-5f,     1e-4f,     1e-3f,     1e-2f,     1e-1f,     1e0f,
    1e1f,      1e2f,      1e3f,      1e4f,      1e5f,      1e6f,      1e7f,
    1e8f,      1e9f,      1e10f,     1e11f,     1e12f,     1e13f,     1e14f,
    1e15f,     1e16f,     1e17f,     1e18f,     1e19f,     1e20f,     1e21f,
    1e22f,     1e23f,     1e24f,     1e25f,     1e26f,     1e27f,     1e28f,
    1e29f,     1e30f,     1e31f,     1e32f,     1e33f,     1e34f,     1e35f,
    1e36f,     1e37f,     1e38f,     kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf,
    kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf, kFloatInf};

static constexpr double kDoublePowersOfTen76[2 * 76 + 1] = {
    1e-76, 1e-75, 1e-74, 1e-73, 1e-72, 1e-71, 1e-70, 1e-69, 1e-68, 1e-67, 1e-66, 1e-65,
    1e-64, 1e-63, 1e-62, 1e-61, 1e-60, 1e-59, 1e-58, 1e-57, 1e-56, 1e-55, 1e-54, 1e-53,
    1e-52, 1e-51, 1e-50, 1e-49, 1e-48, 1e-47, 1e-46, 1e-45, 1e-44, 1e-43, 1e-42, 1e-41,
    1e-40, 1e-39, 1e-38, 1e-37, 1e-36, 1e-35, 1e-34, 1e-33, 1e-32, 1e-31, 1e-30, 1e-29,
    1e-28, 1e-27, 1e-26, 1e-25, 1e-24, 1e-23, 1e-22, 1e-21, 1e-20, 1e-19, 1e-18, 1e-17,
    1e-16, 1e-15, 1e-14, 1e-13, 1e-12, 1e-11, 1e-10, 1e-9,  1e-8,  1e-7,  1e-6,  1e-5,
    1e-4,  1e-3,  1e-2,  1e-1,  1e0,   1e1,   1e2,   1e3,   1e4,   1e5,   1e6,   1e7,
    1e8,   1e9,   1e10,  1e11,  1e12,  1e13,  1e14,  1e15,  1e16,  1e17,  1e18,  1e19,
    1e20,  1e21,  1e22,  1e23,  1e24,  1e25,  1e26,  1e27,  1e28,  1e29,  1e30,  1e31,
    1e32,  1e33,  1e34,  1e35,  1e36,  1e37,  1e38,  1e39,  1e40,  1e41,  1e42,  1e43,
    1e44,  1e45,  1e46,  1e47,  1e48,  1e49,  1e50,  1e51,  1e52,  1e53,  1e54,  1e55,
    1e56,  1e57,  1e58,  1e59,  1e60,  1e61,  1e62,  1e63,  1e64,  1e65,  1e66,  1e67,
    1e68,  1e69,  1e70,  1e71,  1e72,  1e73,  1e74,  1e75,  1e76};

namespace {

template <typename Real, typename Derived>
struct DecimalRealConversion {
  static Result<Decimal128> FromPositiveReal(Real real, int32_t precision,
                                             int32_t scale) {
    auto x = real;
    if (scale >= -38 && scale <= 38) {
      x *= Derived::powers_of_ten()[scale + 38];
    } else {
      x *= std::pow(static_cast<Real>(10), static_cast<Real>(scale));
    }
    x = std::nearbyint(x);
    const auto max_abs = Derived::powers_of_ten()[precision + 38];
    if (x <= -max_abs || x >= max_abs) {
      return Status::Invalid("Cannot convert ", real,
                             " to Decimal128(precision = ", precision,
                             ", scale = ", scale, "): overflow");
    }
    // Extract high and low bits
    const auto high = std::floor(std::ldexp(x, -64));
    const auto low = x - std::ldexp(high, 64);

    DCHECK_GE(high, -9.223372036854776e+18);  // -2**63
    DCHECK_LT(high, 9.223372036854776e+18);   // 2**63
    DCHECK_GE(low, 0);
    DCHECK_LT(low, 1.8446744073709552e+19);  // 2**64
    return Decimal128(static_cast<int64_t>(high), static_cast<uint64_t>(low));
  }

  static Result<Decimal128> FromReal(Real x, int32_t precision, int32_t scale) {
    DCHECK_GT(precision, 0);
    DCHECK_LE(precision, 38);

    if (!std::isfinite(x)) {
      return Status::Invalid("Cannot convert ", x, " to Decimal128");
    }
    if (x < 0) {
      ARROW_ASSIGN_OR_RAISE(auto dec, FromPositiveReal(-x, precision, scale));
      return dec.Negate();
    } else {
      // Includes negative zero
      return FromPositiveReal(x, precision, scale);
    }
  }

  static Real ToRealPositive(const Decimal128& decimal, int32_t scale) {
    Real x = static_cast<Real>(decimal.high_bits()) * Derived::two_to_64();
    x += static_cast<Real>(decimal.low_bits());
    if (scale >= -38 && scale <= 38) {
      x *= Derived::powers_of_ten()[-scale + 38];
    } else {
      x *= std::pow(static_cast<Real>(10), static_cast<Real>(-scale));
    }
    return x;
  }

  static Real ToReal(Decimal128 decimal, int32_t scale) {
    if (decimal.high_bits() < 0) {
      // Convert the absolute value to avoid precision loss
      decimal.Negate();
      return -ToRealPositive(decimal, scale);
    } else {
      return ToRealPositive(decimal, scale);
    }
  }
};

struct DecimalFloatConversion
    : public DecimalRealConversion<float, DecimalFloatConversion> {
  static constexpr const float* powers_of_ten() { return kFloatPowersOfTen; }

  static constexpr float two_to_64() { return 1.8446744e+19f; }
};

struct DecimalDoubleConversion
    : public DecimalRealConversion<double, DecimalDoubleConversion> {
  static constexpr const double* powers_of_ten() { return kDoublePowersOfTen; }

  static constexpr double two_to_64() { return 1.8446744073709552e+19; }
};

}  // namespace

Result<Decimal128> Decimal128::FromReal(float x, int32_t precision, int32_t scale) {
  return DecimalFloatConversion::FromReal(x, precision, scale);
}

Result<Decimal128> Decimal128::FromReal(double x, int32_t precision, int32_t scale) {
  return DecimalDoubleConversion::FromReal(x, precision, scale);
}

float Decimal128::ToFloat(int32_t scale) const {
  return DecimalFloatConversion::ToReal(*this, scale);
}

double Decimal128::ToDouble(int32_t scale) const {
  return DecimalDoubleConversion::ToReal(*this, scale);
}

template <size_t n>
static void AppendLittleEndianArrayToString(const std::array<uint64_t, n>& array,
                                            std::string* result) {
  const auto most_significant_non_zero =
      find_if(array.rbegin(), array.rend(), [](uint64_t v) { return v != 0; });
  if (most_significant_non_zero == array.rend()) {
    result->push_back('0');
    return;
  }

  size_t most_significant_elem_idx = &*most_significant_non_zero - array.data();
  std::array<uint64_t, n> copy = array;
  constexpr uint32_t k1e9 = 1000000000U;
  constexpr size_t kNumBits = n * 64;
  // Segments will contain the array split into groups that map to decimal digits,
  // in little endian order. Each segment will hold at most 9 decimal digits.
  // For example, if the input represents 9876543210123456789, then segments will be
  // [123456789, 876543210, 9].
  // The max number of segments needed = ceil(kNumBits * log(2) / log(1e9))
  // = ceil(kNumBits / 29.897352854) <= ceil(kNumBits / 29).
  std::array<uint32_t, (kNumBits + 28) / 29> segments;
  size_t num_segments = 0;
  uint64_t* most_significant_elem = &copy[most_significant_elem_idx];
  do {
    // Compute remainder = copy % 1e9 and copy = copy / 1e9.
    uint32_t remainder = 0;
    uint64_t* elem = most_significant_elem;
    do {
      // Compute dividend = (remainder << 32) | *elem  (a virtual 96-bit integer);
      // *elem = dividend / 1e9;
      // remainder = dividend % 1e9.
      uint32_t hi = static_cast<uint32_t>(*elem >> 32);
      uint32_t lo = static_cast<uint32_t>(*elem & BitUtil::LeastSignificantBitMask(32));
      uint64_t dividend_hi = (static_cast<uint64_t>(remainder) << 32) | hi;
      uint64_t quotient_hi = dividend_hi / k1e9;
      remainder = static_cast<uint32_t>(dividend_hi % k1e9);
      uint64_t dividend_lo = (static_cast<uint64_t>(remainder) << 32) | lo;
      uint64_t quotient_lo = dividend_lo / k1e9;
      remainder = static_cast<uint32_t>(dividend_lo % k1e9);
      *elem = (quotient_hi << 32) | quotient_lo;
    } while (elem-- != copy.data());

    segments[num_segments++] = remainder;
  } while (*most_significant_elem != 0 || most_significant_elem-- != copy.data());

  size_t old_size = result->size();
  size_t new_size = old_size + num_segments * 9;
  result->resize(new_size, '0');
  char* output = &result->at(old_size);
  const uint32_t* segment = &segments[num_segments - 1];
  internal::StringFormatter<UInt32Type> format;
  // First segment is formatted as-is.
  format(*segment, [&output](util::string_view formatted) {
    memcpy(output, formatted.data(), formatted.size());
    output += formatted.size();
  });
  while (segment != segments.data()) {
    --segment;
    // Right-pad formatted segment such that e.g. 123 is formatted as "000000123".
    output += 9;
    format(*segment, [output](util::string_view formatted) {
      memcpy(output - formatted.size(), formatted.data(), formatted.size());
    });
  }
  result->resize(output - result->data());
}

std::string Decimal128::ToIntegerString() const {
  std::string result;
  if (high_bits() < 0) {
    result.push_back('-');
    Decimal128 abs = *this;
    abs.Negate();
    AppendLittleEndianArrayToString<2>(
        {abs.low_bits(), static_cast<uint64_t>(abs.high_bits())}, &result);
  } else {
    AppendLittleEndianArrayToString<2>({low_bits(), static_cast<uint64_t>(high_bits())},
                                       &result);
  }
  return result;
}

Decimal128::operator int64_t() const {
  DCHECK(high_bits() == 0 || high_bits() == -1)
      << "Trying to cast a Decimal128 greater than the value range of a "
         "int64_t. high_bits_ must be equal to 0 or -1, got: "
      << high_bits();
  return static_cast<int64_t>(low_bits());
}

static void AdjustIntegerStringWithScale(int32_t scale, std::string* str) {
  if (scale == 0) {
    return;
  }
  DCHECK(str != nullptr);
  DCHECK(!str->empty());
  const bool is_negative = str->front() == '-';
  const auto is_negative_offset = static_cast<int32_t>(is_negative);
  const auto len = static_cast<int32_t>(str->size());
  const int32_t num_digits = len - is_negative_offset;
  const int32_t adjusted_exponent = num_digits - 1 - scale;

  /// Note that the -6 is taken from the Java BigDecimal documentation.
  if (scale < 0 || adjusted_exponent < -6) {
    // Example 1:
    // Precondition: *str = "123", is_negative_offset = 0, num_digits = 3, scale = -2,
    //               adjusted_exponent = 4
    // After inserting decimal point: *str = "1.23"
    // After appending exponent: *str = "1.23E+4"
    // Example 2:
    // Precondition: *str = "-123", is_negative_offset = 1, num_digits = 3, scale = 9,
    //               adjusted_exponent = -7
    // After inserting decimal point: *str = "-1.23"
    // After appending exponent: *str = "-1.23E-7"
    str->insert(str->begin() + 1 + is_negative_offset, '.');
    str->push_back('E');
    if (adjusted_exponent >= 0) {
      str->push_back('+');
    }
    internal::StringFormatter<Int32Type> format;
    format(adjusted_exponent, [str](util::string_view formatted) {
      str->append(formatted.data(), formatted.size());
    });
    return;
  }

  if (num_digits > scale) {
    const auto n = static_cast<size_t>(len - scale);
    // Example 1:
    // Precondition: *str = "123", len = num_digits = 3, scale = 1, n = 2
    // After inserting decimal point: *str = "12.3"
    // Example 2:
    // Precondition: *str = "-123", len = 4, num_digits = 3, scale = 1, n = 3
    // After inserting decimal point: *str = "-12.3"
    str->insert(str->begin() + n, '.');
    return;
  }

  // Example 1:
  // Precondition: *str = "123", is_negative_offset = 0, num_digits = 3, scale = 4
  // After insert: *str = "000123"
  // After setting decimal point: *str = "0.0123"
  // Example 2:
  // Precondition: *str = "-123", is_negative_offset = 1, num_digits = 3, scale = 4
  // After insert: *str = "-000123"
  // After setting decimal point: *str = "-0.0123"
  str->insert(is_negative_offset, scale - num_digits + 2, '0');
  str->at(is_negative_offset + 1) = '.';
}

std::string Decimal128::ToString(int32_t scale) const {
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

// Iterates over input and for each group of kInt64DecimalDigits multiple out by
// the appropriate power of 10 necessary to add source parsed as uint64 and
// then adds the parsed value of source.
static inline void ShiftAndAdd(const util::string_view& input, uint64_t out[],
                               size_t out_size) {
  for (size_t posn = 0; posn < input.size();) {
    const size_t group_size = std::min(kInt64DecimalDigits, input.size() - posn);
    const uint64_t multiple = kUInt64PowersOfTen[group_size];
    uint64_t chunk = 0;
    ARROW_CHECK(
        internal::ParseValue<UInt64Type>(input.data() + posn, group_size, &chunk));

    for (size_t i = 0; i < out_size; ++i) {
      uint128_t tmp = out[i];
      tmp *= multiple;
      tmp += chunk;
      out[i] = static_cast<uint64_t>(tmp & 0xFFFFFFFFFFFFFFFFULL);
      chunk = static_cast<uint64_t>(tmp >> 64);
    }
    posn += group_size;
  }
}

namespace {

struct DecimalComponents {
  util::string_view whole_digits;
  util::string_view fractional_digits;
  int32_t exponent = 0;
  char sign = 0;
  bool has_exponent = false;
};

inline bool IsSign(char c) { return c == '-' || c == '+'; }

inline bool IsDot(char c) { return c == '.'; }

inline bool IsDigit(char c) { return c >= '0' && c <= '9'; }

inline bool StartsExponent(char c) { return c == 'e' || c == 'E'; }

inline size_t ParseDigitsRun(const char* s, size_t start, size_t size,
                             util::string_view* out) {
  size_t pos;
  for (pos = start; pos < size; ++pos) {
    if (!IsDigit(s[pos])) {
      break;
    }
  }
  *out = util::string_view(s + start, pos - start);
  return pos;
}

bool ParseDecimalComponents(const char* s, size_t size, DecimalComponents* out) {
  size_t pos = 0;

  if (size == 0) {
    return false;
  }
  // Sign of the number
  if (IsSign(s[pos])) {
    out->sign = *(s + pos);
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
    if (pos != size && s[pos] == '+') {
      ++pos;
    }
    out->has_exponent = true;
    return internal::ParseValue<Int32Type>(s + pos, size - pos, &(out->exponent));
  }
  return pos == size;
}

inline Status ToArrowStatus(DecimalStatus dstatus, int num_bits) {
  switch (dstatus) {
    case DecimalStatus::kSuccess:
      return Status::OK();

    case DecimalStatus::kDivideByZero:
      return Status::Invalid("Division by 0 in Decimal", num_bits);

    case DecimalStatus::kOverflow:
      return Status::Invalid("Overflow occurred during Decimal", num_bits, " operation.");

    case DecimalStatus::kRescaleDataLoss:
      return Status::Invalid("Rescaling Decimal", num_bits,
                             " value would cause data loss");
  }
  return Status::OK();
}

}  // namespace

Status Decimal128::FromString(const util::string_view& s, Decimal128* out,
                              int32_t* precision, int32_t* scale) {
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to decimal");
  }

  DecimalComponents dec;
  if (!ParseDecimalComponents(s.data(), s.size(), &dec)) {
    return Status::Invalid("The string '", s, "' is not a valid decimal number");
  }

  // Count number of significant digits (without leading zeros)
  size_t first_non_zero = dec.whole_digits.find_first_not_of('0');
  size_t significant_digits = dec.fractional_digits.size();
  if (first_non_zero != std::string::npos) {
    significant_digits += dec.whole_digits.size() - first_non_zero;
  }
  int32_t parsed_precision = static_cast<int32_t>(significant_digits);

  int32_t parsed_scale = 0;
  if (dec.has_exponent) {
    auto adjusted_exponent = dec.exponent;
    auto len = static_cast<int32_t>(significant_digits);
    parsed_scale = -adjusted_exponent + len - 1;
  } else {
    parsed_scale = static_cast<int32_t>(dec.fractional_digits.size());
  }

  if (out != nullptr) {
    std::array<uint64_t, 2> little_endian_array = {0, 0};
    ShiftAndAdd(dec.whole_digits, little_endian_array.data(), little_endian_array.size());
    ShiftAndAdd(dec.fractional_digits, little_endian_array.data(),
                little_endian_array.size());
    *out =
        Decimal128(static_cast<int64_t>(little_endian_array[1]), little_endian_array[0]);
    if (parsed_scale < 0) {
      *out *= GetScaleMultiplier(-parsed_scale);
    }

    if (dec.sign == '-') {
      out->Negate();
    }
  }

  if (parsed_scale < 0) {
    parsed_precision -= parsed_scale;
    parsed_scale = 0;
  }

  if (precision != nullptr) {
    *precision = parsed_precision;
  }
  if (scale != nullptr) {
    *scale = parsed_scale;
  }

  return Status::OK();
}

Status Decimal128::FromString(const std::string& s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(util::string_view(s), out, precision, scale);
}

Status Decimal128::FromString(const char* s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(util::string_view(s), out, precision, scale);
}

Result<Decimal128> Decimal128::FromString(const util::string_view& s) {
  Decimal128 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return std::move(out);
}

Result<Decimal128> Decimal128::FromString(const std::string& s) {
  return FromString(util::string_view(s));
}

Result<Decimal128> Decimal128::FromString(const char* s) {
  return FromString(util::string_view(s));
}

// Helper function used by Decimal128::FromBigEndian
static inline uint64_t UInt64FromBigEndian(const uint8_t* bytes, int32_t length) {
  // We don't bounds check the length here because this is called by
  // FromBigEndian that has a Decimal128 as its out parameters and
  // that function is already checking the length of the bytes and only
  // passes lengths between zero and eight.
  uint64_t result = 0;
  // Using memcpy instead of special casing for length
  // and doing the conversion in 16, 32 parts, which could
  // possibly create unaligned memory access on certain platforms
  memcpy(reinterpret_cast<uint8_t*>(&result) + 8 - length, bytes, length);
  return ::arrow::BitUtil::FromBigEndian(result);
}

Result<Decimal128> Decimal128::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 16;

  int64_t high, low;

  if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
    return Status::Invalid("Length of byte array passed to Decimal128::FromBigEndian ",
                           "was ", length, ", but must be between ", kMinDecimalBytes,
                           " and ", kMaxDecimalBytes);
  }

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  // 1. Extract the high bytes
  // Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, length - 8);
  const auto high_bits = UInt64FromBigEndian(bytes, high_bits_offset);

  if (high_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    high = high_bits;
  } else {
    high = -1 * (is_negative && length < kMaxDecimalBytes);
    // Shift left enough bits to make room for the incoming int64_t
    high = SafeLeftShift(high, high_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    high |= high_bits;
  }

  // 2. Extract the low bytes
  // Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(length, 8);
  const auto low_bits =
      UInt64FromBigEndian(bytes + high_bits_offset, length - high_bits_offset);

  if (low_bits_offset == 8) {
    // Avoid undefined shift by 64 below
    low = low_bits;
  } else {
    // Sign extend the low bits if necessary
    low = -1 * (is_negative && length < 8);
    // Shift left enough bits to make room for the incoming int64_t
    low = SafeLeftShift(low, low_bits_offset * CHAR_BIT);
    // Preserve the upper bits by inplace OR-ing the int64_t
    low |= low_bits;
  }

  return Decimal128(high, static_cast<uint64_t>(low));
}

Status Decimal128::ToArrowStatus(DecimalStatus dstatus) const {
  return arrow::ToArrowStatus(dstatus, 128);
}

std::ostream& operator<<(std::ostream& os, const Decimal128& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

Decimal256::Decimal256(const std::string& str) : Decimal256() {
  *this = Decimal256::FromString(str).ValueOrDie();
}

std::string Decimal256::ToIntegerString() const {
  std::string result;
  if (static_cast<int64_t>(little_endian_array()[3]) < 0) {
    result.push_back('-');
    Decimal256 abs = *this;
    abs.Negate();
    AppendLittleEndianArrayToString(abs.little_endian_array(), &result);
  } else {
    AppendLittleEndianArrayToString(little_endian_array(), &result);
  }
  return result;
}

std::string Decimal256::ToString(int32_t scale) const {
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

Status Decimal256::FromString(const util::string_view& s, Decimal256* out,
                              int32_t* precision, int32_t* scale) {
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to decimal");
  }

  DecimalComponents dec;
  if (!ParseDecimalComponents(s.data(), s.size(), &dec)) {
    return Status::Invalid("The string '", s, "' is not a valid decimal number");
  }

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
    if (dec.has_exponent) {
      auto adjusted_exponent = dec.exponent;
      auto len = static_cast<int32_t>(significant_digits);
      *scale = -adjusted_exponent + len - 1;
    } else {
      *scale = static_cast<int32_t>(dec.fractional_digits.size());
    }
  }

  if (out != nullptr) {
    std::array<uint64_t, 4> little_endian_array = {0, 0, 0, 0};
    ShiftAndAdd(dec.whole_digits, little_endian_array.data(), little_endian_array.size());
    ShiftAndAdd(dec.fractional_digits, little_endian_array.data(),
                little_endian_array.size());
    *out = Decimal256(little_endian_array);

    if (dec.sign == '-') {
      out->Negate();
    }
  }

  return Status::OK();
}

Status Decimal256::FromString(const std::string& s, Decimal256* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(util::string_view(s), out, precision, scale);
}

Status Decimal256::FromString(const char* s, Decimal256* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(util::string_view(s), out, precision, scale);
}

Result<Decimal256> Decimal256::FromString(const util::string_view& s) {
  Decimal256 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return std::move(out);
}

Result<Decimal256> Decimal256::FromString(const std::string& s) {
  return FromString(util::string_view(s));
}

Result<Decimal256> Decimal256::FromString(const char* s) {
  return FromString(util::string_view(s));
}

Result<Decimal256> Decimal256::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 32;

  std::array<uint64_t, 4> little_endian_array;

  if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
    return Status::Invalid("Length of byte array passed to Decimal128::FromBigEndian ",
                           "was ", length, ", but must be between ", kMinDecimalBytes,
                           " and ", kMaxDecimalBytes);
  }

  // Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  // sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  for (int word_idx = 0; word_idx < 4; word_idx++) {
    const int32_t word_length = std::min(length, static_cast<int32_t>(sizeof(uint64_t)));

    if (word_length == 8) {
      // Full words can be assigned as is (and are UB with the shift below).
      little_endian_array[word_idx] =
          UInt64FromBigEndian(bytes + length - word_length, word_length);
    } else {
      // Sign extend the word its if necessary
      uint64_t word = -1 * is_negative;
      if (length > 0) {
        // Incorporate the actual values if present.
        // Shift left enough bits to make room for the incoming int64_t
        word = SafeLeftShift(word, word_length * CHAR_BIT);
        // Preserve the upper bits by inplace OR-ing the int64_t
        word |= UInt64FromBigEndian(bytes + length - word_length, word_length);
      }
      little_endian_array[word_idx] = word;
    }
    // Move on to the next word.
    length -= word_length;
  }

  return Decimal256(little_endian_array);
}

Status Decimal256::ToArrowStatus(DecimalStatus dstatus) const {
  return arrow::ToArrowStatus(dstatus, 256);
}

namespace {

template <typename Real, typename Derived>
struct Decimal256RealConversion {
  static Result<Decimal256> FromPositiveReal(Real real, int32_t precision,
                                             int32_t scale) {
    auto x = real;
    if (scale >= -76 && scale <= 76) {
      x *= Derived::powers_of_ten()[scale + 76];
    } else {
      x *= std::pow(static_cast<Real>(10), static_cast<Real>(scale));
    }
    x = std::nearbyint(x);
    const auto max_abs = Derived::powers_of_ten()[precision + 76];
    if (x >= max_abs) {
      return Status::Invalid("Cannot convert ", real,
                             " to Decimal256(precision = ", precision,
                             ", scale = ", scale, "): overflow");
    }
    // Extract parts
    const auto part3 = std::floor(std::ldexp(x, -192));
    x -= std::ldexp(part3, 192);
    const auto part2 = std::floor(std::ldexp(x, -128));
    x -= std::ldexp(part2, 128);
    const auto part1 = std::floor(std::ldexp(x, -64));
    x -= std::ldexp(part1, 64);
    const auto part0 = x;

    DCHECK_GE(part3, 0);
    DCHECK_LT(part3, 1.8446744073709552e+19);  // 2**64
    DCHECK_GE(part2, 0);
    DCHECK_LT(part2, 1.8446744073709552e+19);  // 2**64
    DCHECK_GE(part1, 0);
    DCHECK_LT(part1, 1.8446744073709552e+19);  // 2**64
    DCHECK_GE(part0, 0);
    DCHECK_LT(part0, 1.8446744073709552e+19);  // 2**64
    return Decimal256(std::array<uint64_t, 4>{
        static_cast<uint64_t>(part0), static_cast<uint64_t>(part1),
        static_cast<uint64_t>(part2), static_cast<uint64_t>(part3)});
  }

  static Result<Decimal256> FromReal(Real x, int32_t precision, int32_t scale) {
    DCHECK_GT(precision, 0);
    DCHECK_LE(precision, 76);

    if (!std::isfinite(x)) {
      return Status::Invalid("Cannot convert ", x, " to Decimal256");
    }
    if (x < 0) {
      ARROW_ASSIGN_OR_RAISE(auto dec, FromPositiveReal(-x, precision, scale));
      return dec.Negate();
    } else {
      // Includes negative zero
      return FromPositiveReal(x, precision, scale);
    }
  }

  static Real ToRealPositive(const Decimal256& decimal, int32_t scale) {
    DCHECK_GE(decimal, 0);
    Real x = 0;
    const auto& parts = decimal.little_endian_array();
    x += Derived::two_to_192(static_cast<Real>(parts[3]));
    x += Derived::two_to_128(static_cast<Real>(parts[2]));
    x += Derived::two_to_64(static_cast<Real>(parts[1]));
    x += static_cast<Real>(parts[0]);
    if (scale >= -76 && scale <= 76) {
      x *= Derived::powers_of_ten()[-scale + 76];
    } else {
      x *= std::pow(static_cast<Real>(10), static_cast<Real>(-scale));
    }
    return x;
  }

  static Real ToReal(Decimal256 decimal, int32_t scale) {
    if (decimal.little_endian_array()[3] & (1ULL << 63)) {
      // Convert the absolute value to avoid precision loss
      decimal.Negate();
      return -ToRealPositive(decimal, scale);
    } else {
      return ToRealPositive(decimal, scale);
    }
  }
};

struct Decimal256FloatConversion
    : public Decimal256RealConversion<float, Decimal256FloatConversion> {
  static constexpr const float* powers_of_ten() { return kFloatPowersOfTen76; }

  static float two_to_64(float x) { return x * 1.8446744e+19f; }
  static float two_to_128(float x) { return x == 0 ? 0 : INFINITY; }
  static float two_to_192(float x) { return x == 0 ? 0 : INFINITY; }
};

struct Decimal256DoubleConversion
    : public Decimal256RealConversion<double, Decimal256DoubleConversion> {
  static constexpr const double* powers_of_ten() { return kDoublePowersOfTen76; }

  static double two_to_64(double x) { return x * 1.8446744073709552e+19; }
  static double two_to_128(double x) { return x * 3.402823669209385e+38; }
  static double two_to_192(double x) { return x * 6.277101735386681e+57; }
};

}  // namespace

Result<Decimal256> Decimal256::FromReal(float x, int32_t precision, int32_t scale) {
  return Decimal256FloatConversion::FromReal(x, precision, scale);
}

Result<Decimal256> Decimal256::FromReal(double x, int32_t precision, int32_t scale) {
  return Decimal256DoubleConversion::FromReal(x, precision, scale);
}

float Decimal256::ToFloat(int32_t scale) const {
  return Decimal256FloatConversion::ToReal(*this, scale);
}

double Decimal256::ToDouble(int32_t scale) const {
  return Decimal256DoubleConversion::ToReal(*this, scale);
}

std::ostream& operator<<(std::ostream& os, const Decimal256& decimal) {
  os << decimal.ToIntegerString();
  return os;
}
}  // namespace arrow
