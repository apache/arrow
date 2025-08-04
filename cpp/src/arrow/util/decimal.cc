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
#include "arrow/util/decimal_internal.h"
#include "arrow/util/endian.h"
#include "arrow/util/formatting.h"
#include "arrow/util/int128_internal.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/macros.h"
#include "arrow/util/value_parsing.h"

namespace arrow {

using internal::SafeLeftShift;
using internal::SafeSignedAdd;
using internal::uint128_t;

namespace internal {

Status ToArrowStatus(DecimalStatus dstatus) {
  switch (dstatus) {
    case DecimalStatus::kSuccess:
      return Status::OK();

    case DecimalStatus::kDivideByZero:
      return Status::Invalid("Division by 0 in Decimal");

    case DecimalStatus::kOverflow:
      return Status::Invalid("Overflow occurred during Decimal operation");

    case DecimalStatus::kRescaleDataLoss:
      return Status::Invalid("Rescaling Decimal value would cause data loss");

    default:
      return Status::UnknownError("Unknown Decimal error");
  }
}

}  // namespace internal

namespace {

struct BaseDecimalRealConversion {
  // Return 10**exp, with a fast lookup, assuming `exp` is within bounds
  template <typename Real>
  static Real PowerOfTen(int32_t exp) {
    constexpr int N = kPrecomputedPowersOfTen;
    DCHECK(exp >= -N && exp <= N);
    return RealTraits<Real>::powers_of_ten()[exp + N];
  }

  // Return 10**exp, with a fast lookup if possible
  template <typename Real>
  static Real LargePowerOfTen(int32_t exp) {
    constexpr int N = kPrecomputedPowersOfTen;
    if (ARROW_PREDICT_TRUE(exp >= -N && exp <= N)) {
      return RealTraits<Real>::powers_of_ten()[exp + N];
    } else {
      return std::pow(static_cast<Real>(10), static_cast<Real>(exp));
    }
  }
};

template <typename DecimalType, typename Derived>
struct DecimalRealConversion : public BaseDecimalRealConversion {
  using DecimalTypeTraits = DecimalTraits<DecimalType>;

  static constexpr int kMaxPrecision = DecimalType::kMaxPrecision;
  static constexpr int kMaxScale = DecimalType::kMaxScale;

  static const auto& DecimalPowerOfTen(int exp) {
    DCHECK(exp >= 0 && exp <= kMaxPrecision);
    return DecimalTypeTraits::powers_of_ten()[exp];
  }

  template <typename Real>
  static Status OverflowError(Real real, int precision, int scale) {
    return Status::Invalid("Cannot convert ", real, " to ", DecimalTypeTraits::kTypeName,
                           "(precision = ", precision, ", scale = ", scale,
                           "): overflow");
  }

  template <typename Real>
  static Result<DecimalType> FromPositiveReal(Real real, int32_t precision,
                                              int32_t scale) {
    constexpr int kMantissaBits = RealTraits<Real>::kMantissaBits;
    constexpr int kMantissaDigits = RealTraits<Real>::kMantissaDigits;

    // to avoid precision and rounding issues, we'll unconditionally
    // throw Decimal32 to the approx algorithm instead. (GH-44216)
    if constexpr (std::is_base_of_v<BasicDecimal32, DecimalType>) {
      return Derived::FromPositiveRealApprox(real, precision, scale);
    }

    // Problem statement: construct the Decimal with the value
    // closest to `real * 10^scale`.
    if (scale < 0) {
      // Negative scales are not handled below, fall back to approx algorithm
      return Derived::FromPositiveRealApprox(real, precision, scale);
    }

    // 1. Check that `real` is within acceptable bounds.
    const Real limit = PowerOfTen<Real>(precision - scale);
    if (real > limit) {
      // Checking the limit early helps ensure the computations below do not
      // overflow.
      // NOTE: `limit` is allowed here as rounding can make it smaller than
      // the theoretical limit (for example, 1.0e23 < 10^23).
      return OverflowError(real, precision, scale);
    }

    // The algorithm below requires the destination decimal type
    // to be strictly more precise than the source float type
    // (see `kSafeMulByTenTo` calculation).
    if constexpr (kMaxPrecision <= kMantissaDigits) {
      return Derived::FromPositiveRealApprox(real, precision, scale);
    }

    // 2. Losslessly convert `real` to `mant * 2**k`
    int binary_exp = 0;
    const Real real_mant = std::frexp(real, &binary_exp);
    // `real_mant` is within 0.5 and 1 and has M bits of precision.
    // Multiply it by 2^M to get an exact integer.
    const uint64_t mant = static_cast<uint64_t>(std::ldexp(real_mant, kMantissaBits));
    const int k = binary_exp - kMantissaBits;
    // (note that `real = mant * 2^k`)

    // 3. Start with `mant`.
    // We want to end up with `real * 10^scale` i.e. `mant * 2^k * 10^scale`.
    DecimalType x(mant);

    if (k < 0) {
      // k < 0 (i.e. binary_exp < kMantissaBits), is probably the common case
      // when converting to decimal. It implies right-shifting by -k bits,
      // while multiplying by 10^scale. We also must avoid overflow (losing
      // bits on the left) and precision loss (losing bits on the right).
      int right_shift_by = -k;
      int mul_by_ten_to = scale;

      // At this point, `x` has kMantissaDigits significant digits but it can
      // fit kMaxPrecision (excluding sign). We can therefore multiply by up
      // to 10^(kMaxPrecision - kMantissaDigits).
      constexpr int kSafeMulByTenTo = kMaxPrecision - kMantissaDigits;

      if (mul_by_ten_to <= kSafeMulByTenTo) {
        // Scale is small enough, so we can do it all at once.
        x *= DecimalPowerOfTen(mul_by_ten_to);
        x = Derived::RoundedRightShift(x, right_shift_by);
      } else {
        // Scale is too large, we cannot multiply at once without overflow.
        // We use an iterative algorithm which alternately shifts left by
        // multiplying by a power of ten, and shifts right by a number of bits.

        // First multiply `x` by as large a power of ten as possible
        // without overflowing.
        x *= DecimalPowerOfTen(kSafeMulByTenTo);
        mul_by_ten_to -= kSafeMulByTenTo;

        // `x` now has full precision. However, we know we'll only
        // keep `precision` digits at the end. Extraneous bits/digits
        // on the right can be safely shifted away, before multiplying
        // again.
        // NOTE: if `precision` is the full precision then the algorithm will
        // lose the last digit. If `precision` is almost the full precision,
        // there can be an off-by-one error due to rounding.
        const int mul_step = std::max(1, kMaxPrecision - precision);

        // The running exponent, useful to compute by how much we must
        // shift right to make place on the left before the next multiply.
        int total_exp = 0;
        int total_shift = 0;
        while (mul_by_ten_to > 0 && right_shift_by > 0) {
          const int exp = std::min(mul_by_ten_to, mul_step);
          total_exp += exp;
          // The supplementary right shift required so that
          // `x * 10^total_exp / 2^total_shift` fits in the decimal.
          DCHECK_LT(static_cast<size_t>(total_exp), sizeof(kCeilLog2PowersOfTen));
          const int bits =
              std::min(right_shift_by, kCeilLog2PowersOfTen[total_exp] - total_shift);
          total_shift += bits;
          // Right shift to make place on the left, then multiply
          x = Derived::RoundedRightShift(x, bits);
          right_shift_by -= bits;
          // Should not overflow thanks to the precautions taken
          x *= DecimalPowerOfTen(exp);
          mul_by_ten_to -= exp;
        }
        if (mul_by_ten_to > 0) {
          x *= DecimalPowerOfTen(mul_by_ten_to);
        }
        if (right_shift_by > 0) {
          x = Derived::RoundedRightShift(x, right_shift_by);
        }
      }
    } else {
      // k >= 0 implies left-shifting by k bits and multiplying by 10^scale.
      // The order of these operations therefore doesn't matter. We know
      // we won't overflow because of the limit check above, and we also
      // won't lose any significant bits on the right.
      x *= DecimalPowerOfTen(scale);
      x <<= k;
    }

    // Rounding might have pushed `x` just above the max precision, check again
    if (!x.FitsInPrecision(precision)) {
      return OverflowError(real, precision, scale);
    }
    return x;
  }

  template <typename Real>
  static Result<DecimalType> FromReal(Real x, int32_t precision, int32_t scale) {
    DCHECK_GT(precision, 0);
    DCHECK_LE(precision, kMaxPrecision);
    DCHECK_GE(scale, -kMaxScale);
    DCHECK_LE(scale, kMaxScale);

    if (!std::isfinite(x)) {
      return Status::Invalid("Cannot convert ", x, " to Decimal128");
    }
    if (x == 0) {
      return DecimalType{};
    }
    if (x < 0) {
      ARROW_ASSIGN_OR_RAISE(auto dec, FromPositiveReal(-x, precision, scale));
      return dec.Negate();
    } else {
      return FromPositiveReal(x, precision, scale);
    }
  }

  template <typename Real>
  static Real ToReal(const DecimalType& decimal, int32_t scale) {
    DCHECK_GE(scale, -kMaxScale);
    DCHECK_LE(scale, kMaxScale);
    if (decimal.IsNegative()) {
      // Convert the absolute value to avoid precision loss
      auto abs = decimal;
      abs.Negate();
      return -Derived::template ToRealPositive<Real>(abs, scale);
    } else {
      return Derived::template ToRealPositive<Real>(decimal, scale);
    }
  }
};

struct Decimal32RealConversion
    : public DecimalRealConversion<Decimal32, Decimal32RealConversion> {
  using Base = DecimalRealConversion<Decimal32, Decimal32RealConversion>;
  using Base::LargePowerOfTen;
  using Base::PowerOfTen;

  static Decimal32 RoundedRightShift(const Decimal32& x, int bits) {
    // currently we *only* push to the Approx method for Decimal32
    // so this should never get called.
    DCHECK(false);
    return x;
  }

  template <typename Real>
  static Result<Decimal32> FromPositiveRealApprox(Real real, int32_t precision,
                                                  int32_t scale) {
    const auto x = std::nearbyint(real * PowerOfTen<Real>(scale));
    const auto max_abs = PowerOfTen<Real>(precision);
    if (x <= -max_abs || x >= max_abs) {
      return OverflowError(real, precision, scale);
    }

    return Decimal32(static_cast<int32_t>(x));
  }

  template <typename Real>
  static Real ToRealPositiveNoSplit(const Decimal32& decimal, int32_t scale) {
    Real x = static_cast<Real>(decimal.value());
    x *= LargePowerOfTen<Real>(-scale);
    return x;
  }

  template <typename Real>
  static Real ToRealPositive(const Decimal32& decimal, int32_t scale) {
    if (scale <= 0 || uint64_t(decimal.value()) <= RealTraits<Real>::kMaxPreciseInteger) {
      return ToRealPositiveNoSplit<Real>(decimal, scale);
    }

    Decimal32 whole_decimal, fraction_decimal;
    decimal.GetWholeAndFraction(scale, &whole_decimal, &fraction_decimal);

    Real whole = ToRealPositiveNoSplit<Real>(whole_decimal, 0);
    Real fraction = ToRealPositiveNoSplit<Real>(fraction_decimal, scale);

    return whole + fraction;
  }
};

struct Decimal64RealConversion
    : public DecimalRealConversion<Decimal64, Decimal64RealConversion> {
  using Base = DecimalRealConversion<Decimal64, Decimal64RealConversion>;
  using Base::LargePowerOfTen;
  using Base::PowerOfTen;

  static Decimal64 RoundedRightShift(const Decimal64& x, int bits) {
    if (bits == 0) {
      return x;
    }

    int64_t result = x.value();
    uint64_t shifted = 0;
    if (bits > 0) {
      shifted = (static_cast<uint64_t>(result) << (64 - bits));
      result >>= bits;
    }
    constexpr uint64_t kHalf = 0x8000000000000000ULL;
    if (shifted > kHalf) {
      // strictly more than half => round up
      result += 1;
    } else if (shifted == kHalf) {
      // exactly half => round to even
      if ((result & 1) != 0) {
        result += 1;
      }
    } else {
      // strictly less than half => round down
    }
    return Decimal64(result);
  }

  template <typename Real>
  static Result<Decimal64> FromPositiveRealApprox(Real real, int32_t precision,
                                                  int32_t scale) {
    const auto x = std::nearbyint(real * PowerOfTen<Real>(scale));
    const auto max_abs = PowerOfTen<Real>(precision);
    if (x <= -max_abs || x >= max_abs) {
      return OverflowError(real, precision, scale);
    }

    return Decimal64(static_cast<int64_t>(x));
  }

  template <typename Real>
  static Real ToRealPositiveNoSplit(const Decimal64& decimal, int32_t scale) {
    Real x = static_cast<Real>(decimal.value());
    x *= LargePowerOfTen<Real>(-scale);
    return x;
  }

  template <typename Real>
  static Real ToRealPositive(const Decimal64& decimal, int32_t scale) {
    if (scale <= 0 || uint64_t(decimal.value()) <= RealTraits<Real>::kMaxPreciseInteger) {
      return ToRealPositiveNoSplit<Real>(decimal, scale);
    }

    Decimal64 whole_decimal, fraction_decimal;
    decimal.GetWholeAndFraction(scale, &whole_decimal, &fraction_decimal);

    Real whole = ToRealPositiveNoSplit<Real>(whole_decimal, 0);
    Real fraction = ToRealPositiveNoSplit<Real>(fraction_decimal, scale);

    return whole + fraction;
  }
};

struct Decimal128RealConversion
    : public DecimalRealConversion<Decimal128, Decimal128RealConversion> {
  using Base = DecimalRealConversion<Decimal128, Decimal128RealConversion>;
  using Base::LargePowerOfTen;
  using Base::PowerOfTen;

  // Right shift positive `x` by positive `bits`, rounded half to even
  static Decimal128 RoundedRightShift(const Decimal128& x, int bits) {
    if (bits == 0) {
      return x;
    }
    int64_t result_hi = x.high_bits();
    uint64_t result_lo = x.low_bits();
    uint64_t shifted = 0;
    while (bits >= 64) {
      // Retain the information that set bits were shifted right.
      // This is important to detect an exact half.
      shifted = result_lo | (shifted > 0);
      result_lo = result_hi;
      result_hi >>= 63;  // for sign
      bits -= 64;
    }
    if (bits > 0) {
      shifted = (result_lo << (64 - bits)) | (shifted > 0);
      result_lo >>= bits;
      result_lo |= static_cast<uint64_t>(result_hi) << (64 - bits);
      result_hi >>= bits;
    }
    // We almost have our result, but now do the rounding.
    constexpr uint64_t kHalf = 0x8000000000000000ULL;
    if (shifted > kHalf) {
      // Strictly more than half => round up
      result_lo += 1;
      result_hi += (result_lo == 0);
    } else if (shifted == kHalf) {
      // Exactly half => round to even
      if ((result_lo & 1) != 0) {
        result_lo += 1;
        result_hi += (result_lo == 0);
      }
    } else {
      // Strictly less than half => round down
    }
    return Decimal128{result_hi, result_lo};
  }

  template <typename Real>
  static Result<Decimal128> FromPositiveRealApprox(Real real, int32_t precision,
                                                   int32_t scale) {
    // Approximate algorithm that operates in the FP domain (thus subject
    // to precision loss).
    const auto x = std::nearbyint(real * PowerOfTen<double>(scale));
    const auto max_abs = PowerOfTen<double>(precision);
    if (x <= -max_abs || x >= max_abs) {
      return OverflowError(real, precision, scale);
    }
    // Extract high and low bits
    const auto high = std::floor(std::ldexp(x, -64));
    const auto low = x - std::ldexp(high, 64);

    DCHECK_GE(high, 0);
    DCHECK_LT(high, 9.223372036854776e+18);  // 2**63
    DCHECK_GE(low, 0);
    DCHECK_LT(low, 1.8446744073709552e+19);  // 2**64
    return Decimal128(static_cast<int64_t>(high), static_cast<uint64_t>(low));
  }

  template <typename Real>
  static Real ToRealPositiveNoSplit(const Decimal128& decimal, int32_t scale) {
    Real x = RealTraits<Real>::two_to_64(static_cast<Real>(decimal.high_bits()));
    x += static_cast<Real>(decimal.low_bits());
    x *= LargePowerOfTen<Real>(-scale);
    return x;
  }

  /// An approximate conversion from Decimal128 to Real that guarantees:
  /// 1. If the decimal is an integer, the conversion is exact.
  /// 2. If the number of fractional digits is <= RealTraits<Real>::kMantissaDigits (e.g.
  ///    8 for float and 16 for double), the conversion is within 1 ULP of the exact
  ///    value.
  /// 3. Otherwise, the conversion is within 2^(-RealTraits<Real>::kMantissaDigits+1)
  ///    (e.g. 2^-23 for float and 2^-52 for double) of the exact value.
  /// Here "exact value" means the closest representable value by Real.
  template <typename Real>
  static Real ToRealPositive(const Decimal128& decimal, int32_t scale) {
    if (scale <= 0 || (decimal.high_bits() == 0 &&
                       decimal.low_bits() <= RealTraits<Real>::kMaxPreciseInteger)) {
      // No need to split the decimal if it is already an integer (scale <= 0) or if it
      // can be precisely represented by Real
      return ToRealPositiveNoSplit<Real>(decimal, scale);
    }

    // Split decimal into whole and fractional parts to avoid precision loss
    BasicDecimal128 whole_decimal, fraction_decimal;
    decimal.GetWholeAndFraction(scale, &whole_decimal, &fraction_decimal);

    Real whole = ToRealPositiveNoSplit<Real>(whole_decimal, 0);
    Real fraction = ToRealPositiveNoSplit<Real>(fraction_decimal, scale);

    return whole + fraction;
  }
};

}  // namespace

Decimal32::Decimal32(const std::string& str) : Decimal32() {
  *this = FromString(str).ValueOrDie();
}

Result<Decimal32> Decimal32::FromReal(float x, int32_t precision, int32_t scale) {
  return Decimal32RealConversion::FromReal(x, precision, scale);
}

Result<Decimal32> Decimal32::FromReal(double x, int32_t precision, int32_t scale) {
  return Decimal32RealConversion::FromReal(x, precision, scale);
}

float Decimal32::ToFloat(int32_t scale) const {
  return Decimal32RealConversion::ToReal<float>(*this, scale);
}

double Decimal32::ToDouble(int32_t scale) const {
  return Decimal32RealConversion::ToReal<double>(*this, scale);
}

std::string Decimal32::ToIntegerString() const {
  std::string result;
  internal::StringFormatter<Int32Type> format;
  format(value_, [&result](std::string_view formatted) {
    result.append(formatted.data(), formatted.size());
  });
  return result;
}

Decimal32::operator int64_t() const { return static_cast<int64_t>(value_); }

Decimal32::operator Decimal64() const { return Decimal64(static_cast<int64_t>(value_)); }

Decimal64::Decimal64(const std::string& str) : Decimal64() {
  *this = FromString(str).ValueOrDie();
}

Result<Decimal64> Decimal64::FromReal(float x, int32_t precision, int32_t scale) {
  return Decimal64RealConversion::FromReal(x, precision, scale);
}

Result<Decimal64> Decimal64::FromReal(double x, int32_t precision, int32_t scale) {
  return Decimal64RealConversion::FromReal(x, precision, scale);
}

float Decimal64::ToFloat(int32_t scale) const {
  return Decimal64RealConversion::ToReal<float>(*this, scale);
}

double Decimal64::ToDouble(int32_t scale) const {
  return Decimal64RealConversion::ToReal<double>(*this, scale);
}

std::string Decimal64::ToIntegerString() const {
  std::string result;
  internal::StringFormatter<Int64Type> format;
  format(value_, [&result](std::string_view formatted) {
    result.append(formatted.data(), formatted.size());
  });
  return result;
}

Decimal64::operator int64_t() const { return static_cast<int64_t>(value_); }

Decimal128::Decimal128(const std::string& str) : Decimal128() {
  *this = Decimal128::FromString(str).ValueOrDie();
}

Result<Decimal128> Decimal128::FromReal(float x, int32_t precision, int32_t scale) {
  return Decimal128RealConversion::FromReal(x, precision, scale);
}

Result<Decimal128> Decimal128::FromReal(double x, int32_t precision, int32_t scale) {
  return Decimal128RealConversion::FromReal(x, precision, scale);
}

float Decimal128::ToFloat(int32_t scale) const {
  return Decimal128RealConversion::ToReal<float>(*this, scale);
}

double Decimal128::ToDouble(int32_t scale) const {
  return Decimal128RealConversion::ToReal<double>(*this, scale);
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
      uint32_t lo = static_cast<uint32_t>(*elem & bit_util::LeastSignificantBitMask(32));
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
  format(*segment, [&output](std::string_view formatted) {
    memcpy(output, formatted.data(), formatted.size());
    output += formatted.size();
  });
  while (segment != segments.data()) {
    --segment;
    // Right-pad formatted segment such that e.g. 123 is formatted as "000000123".
    output += 9;
    format(*segment, [output](std::string_view formatted) {
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
         "int64_t; high_bits() must be equal to 0 or -1, got: "
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
    // Example 3:
    // Precondition: *str = "0", is_negative_offset = 0, num_digits = 1, scale = -1,
    //               adjusted_exponent = 1
    // After inserting decimal point: *str = "0" // Not inserted
    // After appending exponent: *str = "0E+1"
    if (num_digits > 1) {
      str->insert(str->begin() + 1 + is_negative_offset, '.');
    }
    str->push_back('E');
    if (adjusted_exponent >= 0) {
      str->push_back('+');
    }
    internal::StringFormatter<Int32Type> format;
    format(adjusted_exponent, [str](std::string_view formatted) {
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

std::string Decimal32::ToString(int32_t scale) const {
  if (ARROW_PREDICT_FALSE(scale < -kMaxScale || scale > kMaxScale)) {
    return "<scale out of range, cannot format Decimal32 value>";
  }

  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

std::string Decimal64::ToString(int32_t scale) const {
  if (ARROW_PREDICT_FALSE(scale < -kMaxScale || scale > kMaxScale)) {
    return "<scale out of range, cannot format Decimal64 value>";
  }

  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

std::string Decimal128::ToString(int32_t scale) const {
  if (ARROW_PREDICT_FALSE(scale < -kMaxScale || scale > kMaxScale)) {
    return "<scale out of range, cannot format Decimal128 value>";
  }
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

// Iterates over input and for each group of kInt64DecimalDigits multiple out by
// the appropriate power of 10 necessary to add source parsed as uint64 and
// then adds the parsed value of source.
static inline void ShiftAndAdd(std::string_view input, uint64_t out[], size_t out_size) {
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
  std::string_view whole_digits;
  std::string_view fractional_digits;
  int32_t exponent = 0;
  char sign = 0;
  bool has_exponent = false;
};

inline bool IsSign(char c) { return c == '-' || c == '+'; }

inline bool IsDot(char c) { return c == '.'; }

inline bool IsDigit(char c) { return c >= '0' && c <= '9'; }

inline bool StartsExponent(char c) { return c == 'e' || c == 'E'; }

inline size_t ParseDigitsRun(const char* s, size_t start, size_t size,
                             std::string_view* out) {
  size_t pos;
  for (pos = start; pos < size; ++pos) {
    if (!IsDigit(s[pos])) {
      break;
    }
  }
  *out = std::string_view(s + start, pos - start);
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

template <typename Decimal>
Status DecimalFromString(const char* type_name, std::string_view s, Decimal* out,
                         int32_t* precision, int32_t* scale) {
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to ", type_name);
  }

  DecimalComponents dec;
  if (!ParseDecimalComponents(s.data(), s.size(), &dec)) {
    return Status::Invalid("The string '", s, "' is not a valid ", type_name, " number");
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
    parsed_scale =
        -adjusted_exponent + static_cast<int32_t>(dec.fractional_digits.size());
  } else {
    parsed_scale = static_cast<int32_t>(dec.fractional_digits.size());
  }

  if (out != nullptr) {
    static_assert(Decimal::kBitWidth % 64 == 0, "decimal bit-width not a multiple of 64");
    std::array<uint64_t, Decimal::kBitWidth / 64> little_endian_array{};
    ShiftAndAdd(dec.whole_digits, little_endian_array.data(), little_endian_array.size());
    ShiftAndAdd(dec.fractional_digits, little_endian_array.data(),
                little_endian_array.size());
    *out = Decimal(bit_util::little_endian::ToNative(little_endian_array));
    if (dec.sign == '-') {
      out->Negate();
    }
  }

  if (parsed_scale < 0) {
    // Force the scale to zero, to avoid negative scales (due to compatibility issues
    // with external systems such as databases)
    if (-parsed_scale > Decimal::kMaxScale) {
      return Status::Invalid("The string '", s, "' cannot be represented as ", type_name);
    }
    if (out != nullptr) {
      *out *= Decimal::GetScaleMultiplier(-parsed_scale);
    }
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

template <typename DecimalClass>
Status SimpleDecimalFromString(const char* type_name, std::string_view s,
                               DecimalClass* out, int32_t* precision, int32_t* scale) {
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to ", type_name);
  }

  DecimalComponents dec;
  if (!ParseDecimalComponents(s.data(), s.size(), &dec)) {
    return Status::Invalid("The string '", s, "' is not a valid ", type_name, " number");
  }

  // count number of significant digits (without leading zeros)
  size_t first_non_zero = dec.whole_digits.find_first_not_of('0');
  size_t significant_digits = dec.fractional_digits.size();
  if (first_non_zero != std::string::npos) {
    significant_digits += dec.whole_digits.size() - first_non_zero;
  }
  int32_t parsed_precision = static_cast<int32_t>(significant_digits);

  int32_t parsed_scale = 0;
  if (dec.has_exponent) {
    auto adjusted_exponent = dec.exponent;
    parsed_scale =
        -adjusted_exponent + static_cast<int32_t>(dec.fractional_digits.size());
  } else {
    parsed_scale = static_cast<int32_t>(dec.fractional_digits.size());
  }

  if (out != nullptr) {
    uint64_t value{0};
    ShiftAndAdd(dec.whole_digits, &value, 1);
    ShiftAndAdd(dec.fractional_digits, &value, 1);
    if (value > static_cast<uint64_t>(
                    std::numeric_limits<typename DecimalClass::ValueType>::max())) {
      return Status::Invalid("The string '", s, "' cannot be represented as ", type_name);
    }

    *out = DecimalClass(value);
    if (dec.sign == '-') {
      out->Negate();
    }
  }

  if (parsed_scale < 0) {
    // Force the scale to zero, to avoid negative scales (due to compatibility issues
    // with external systems such as databases)
    if (-parsed_scale > DecimalClass::kMaxScale) {
      return Status::Invalid("The string '", s, "' cannot be represented as ", type_name);
    }
    if (out != nullptr) {
      *out *= DecimalClass::GetScaleMultiplier(-parsed_scale);
    }
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

}  // namespace

Status Decimal32::FromString(std::string_view s, Decimal32* out, int32_t* precision,
                             int32_t* scale) {
  return SimpleDecimalFromString("decimal32", s, out, precision, scale);
}

Status Decimal32::FromString(const std::string& s, Decimal32* out, int32_t* precision,
                             int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Status Decimal32::FromString(const char* s, Decimal32* out, int32_t* precision,
                             int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Result<Decimal32> Decimal32::FromString(std::string_view s) {
  Decimal32 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return out;
}

Result<Decimal32> Decimal32::FromString(const std::string& s) {
  return FromString(std::string_view(s));
}

Result<Decimal32> Decimal32::FromString(const char* s) {
  return FromString(std::string_view(s));
}

Status Decimal64::FromString(std::string_view s, Decimal64* out, int32_t* precision,
                             int32_t* scale) {
  return SimpleDecimalFromString("decimal64", s, out, precision, scale);
}

Status Decimal64::FromString(const std::string& s, Decimal64* out, int32_t* precision,
                             int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Status Decimal64::FromString(const char* s, Decimal64* out, int32_t* precision,
                             int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Result<Decimal64> Decimal64::FromString(std::string_view s) {
  Decimal64 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return out;
}

Result<Decimal64> Decimal64::FromString(const std::string& s) {
  return FromString(std::string_view(s));
}

Result<Decimal64> Decimal64::FromString(const char* s) {
  return FromString(std::string_view(s));
}

Status Decimal128::FromString(std::string_view s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  return DecimalFromString("decimal128", s, out, precision, scale);
}

Status Decimal128::FromString(const std::string& s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Status Decimal128::FromString(const char* s, Decimal128* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Result<Decimal128> Decimal128::FromString(std::string_view s) {
  Decimal128 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return out;
}

Result<Decimal128> Decimal128::FromString(const std::string& s) {
  return FromString(std::string_view(s));
}

Result<Decimal128> Decimal128::FromString(const char* s) {
  return FromString(std::string_view(s));
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
  return ::arrow::bit_util::FromBigEndian(result);
}

Result<Decimal32> Decimal32::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 4;

  if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
    return Status::Invalid("Length of byte array passed to Decimal32::FromBigEndian was ",
                           length, ", but must be between ", kMinDecimalBytes, " and ",
                           kMaxDecimalBytes);
  }

  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;
  int32_t result = is_negative ? 0xffffffff : 0;
  memcpy(reinterpret_cast<uint8_t*>(&result) + kMaxDecimalBytes - length, bytes, length);

  const auto value = bit_util::FromBigEndian(result);
  return Decimal32(value);
}

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal32& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

Result<Decimal64> Decimal64::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 8;

  if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
    return Status::Invalid("Length of byte array passed to Decimal64::FromBigEndian was ",
                           length, ", but must be between ", kMinDecimalBytes, " and ",
                           kMaxDecimalBytes);
  }

  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;
  int64_t result = is_negative ? 0xffffffffffffffffL : 0;
  memcpy(reinterpret_cast<uint8_t*>(&result) + kMaxDecimalBytes - length, bytes, length);

  const auto value = bit_util::FromBigEndian(result);
  return Decimal64(value);
}

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal64& decimal) {
  os << decimal.ToIntegerString();
  return os;
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

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal128& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

Decimal256::Decimal256(const std::string& str) : Decimal256() {
  *this = Decimal256::FromString(str).ValueOrDie();
}

std::string Decimal256::ToIntegerString() const {
  std::string result;
  if (IsNegative()) {
    result.push_back('-');
    Decimal256 abs = *this;
    abs.Negate();
    AppendLittleEndianArrayToString(
        bit_util::little_endian::FromNative(abs.native_endian_array()), &result);
  } else {
    AppendLittleEndianArrayToString(
        bit_util::little_endian::FromNative(native_endian_array()), &result);
  }
  return result;
}

std::string Decimal256::ToString(int32_t scale) const {
  if (ARROW_PREDICT_FALSE(scale < -kMaxScale || scale > kMaxScale)) {
    return "<scale out of range, cannot format Decimal256 value>";
  }
  std::string str(ToIntegerString());
  AdjustIntegerStringWithScale(scale, &str);
  return str;
}

Status Decimal256::FromString(std::string_view s, Decimal256* out, int32_t* precision,
                              int32_t* scale) {
  return DecimalFromString("decimal256", s, out, precision, scale);
}

Status Decimal256::FromString(const std::string& s, Decimal256* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Status Decimal256::FromString(const char* s, Decimal256* out, int32_t* precision,
                              int32_t* scale) {
  return FromString(std::string_view(s), out, precision, scale);
}

Result<Decimal256> Decimal256::FromString(std::string_view s) {
  Decimal256 out;
  RETURN_NOT_OK(FromString(s, &out, nullptr, nullptr));
  return out;
}

Result<Decimal256> Decimal256::FromString(const std::string& s) {
  return FromString(std::string_view(s));
}

Result<Decimal256> Decimal256::FromString(const char* s) {
  return FromString(std::string_view(s));
}

Result<Decimal256> Decimal256::FromBigEndian(const uint8_t* bytes, int32_t length) {
  static constexpr int32_t kMinDecimalBytes = 1;
  static constexpr int32_t kMaxDecimalBytes = 32;

  std::array<uint64_t, 4> little_endian_array;

  if (ARROW_PREDICT_FALSE(length < kMinDecimalBytes || length > kMaxDecimalBytes)) {
    return Status::Invalid("Length of byte array passed to Decimal256::FromBigEndian ",
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

  return Decimal256(bit_util::little_endian::ToNative(little_endian_array));
}

namespace {

struct Decimal256RealConversion
    : public DecimalRealConversion<Decimal256, Decimal256RealConversion> {
  using Base = DecimalRealConversion<Decimal256, Decimal256RealConversion>;
  using Base::LargePowerOfTen;
  using Base::PowerOfTen;

  // Right shift positive `x` by positive `bits`, rounded half to even
  static Decimal256 RoundedRightShift(Decimal256 x, int bits) {
    if (bits == 0) {
      return x;
    }
    const int cross_word_shift = bits / 64;
    if (cross_word_shift >= Decimal256::kNumWords) {
      return Decimal256();
    }
    const uint32_t in_word_shift = bits % 64;
    const auto array_le = x.little_endian_array();
    Decimal256::WordArray shifted_le{};
    uint64_t shifted_out = 0;
    // Iterate from LSW to MSW
    for (int i = 0; i < cross_word_shift; ++i) {
      // Retain the information that non-zero bits were shifted out.
      // This is important for half-to-even rounding.
      shifted_out = (shifted_out > 0) | array_le[i];
    }
    if (in_word_shift != 0) {
      const uint64_t carry_bits = array_le[cross_word_shift] << (64 - in_word_shift);
      shifted_out = (shifted_out > 0) | (shifted_out >> in_word_shift) | carry_bits;
    }
    for (int i = cross_word_shift; i < Decimal256::kNumWords; ++i) {
      shifted_le[i - cross_word_shift] = array_le[i] >> in_word_shift;
      if (in_word_shift != 0 && i + 1 < Decimal256::kNumWords) {
        const uint64_t carry_bits = array_le[i + 1] << (64 - in_word_shift);
        shifted_le[i - cross_word_shift] |= carry_bits;
      }
    }
    auto result = Decimal256(Decimal256::LittleEndianArray, shifted_le);

    // We almost have our result, but now do the rounding.
    constexpr uint64_t kHalf = 0x8000000000000000ULL;
    if (shifted_out > kHalf) {
      // Strictly more than half => round up
      result += 1;
    } else if (shifted_out == kHalf) {
      // Exactly half => round to even
      if ((result.low_bits() & 1) != 0) {
        result += 1;
      }
    } else {
      // Strictly less than half => round down
    }
    return result;
  }

  template <typename Real>
  static Result<Decimal256> FromPositiveRealApprox(Real real, int32_t precision,
                                                   int32_t scale) {
    auto x = std::nearbyint(real * PowerOfTen<double>(scale));
    const auto max_abs = PowerOfTen<double>(precision);
    if (x >= max_abs) {
      return OverflowError(real, precision, scale);
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
    DCHECK_LT(part3, 9.223372036854776e+18);  // 2**63
    DCHECK_GE(part2, 0);
    DCHECK_LT(part2, 1.8446744073709552e+19);  // 2**64
    DCHECK_GE(part1, 0);
    DCHECK_LT(part1, 1.8446744073709552e+19);  // 2**64
    DCHECK_GE(part0, 0);
    DCHECK_LT(part0, 1.8446744073709552e+19);  // 2**64
    return Decimal256(Decimal256::LittleEndianArray,
                      {static_cast<uint64_t>(part0), static_cast<uint64_t>(part1),
                       static_cast<uint64_t>(part2), static_cast<uint64_t>(part3)});
  }

  template <typename Real>
  static Real ToRealPositiveNoSplit(const Decimal256& decimal, int32_t scale) {
    DCHECK_GE(decimal, 0);
    Real x = 0;
    const auto parts_le = bit_util::little_endian::Make(decimal.native_endian_array());
    x += RealTraits<Real>::two_to_192(static_cast<Real>(parts_le[3]));
    x += RealTraits<Real>::two_to_128(static_cast<Real>(parts_le[2]));
    x += RealTraits<Real>::two_to_64(static_cast<Real>(parts_le[1]));
    x += static_cast<Real>(parts_le[0]);
    x *= LargePowerOfTen<Real>(-scale);
    return x;
  }

  /// An approximate conversion from Decimal256 to Real that guarantees:
  /// 1. If the decimal is an integer, the conversion is exact.
  /// 2. If the number of fractional digits is <= RealTraits<Real>::kMantissaDigits (e.g.
  ///    8 for float and 16 for double), the conversion is within 1 ULP of the exact
  ///    value.
  /// 3. Otherwise, the conversion is within 2^(-RealTraits<Real>::kMantissaDigits+1)
  ///    (e.g. 2^-23 for float and 2^-52 for double) of the exact value.
  /// Here "exact value" means the closest representable value by Real.
  template <typename Real>
  static Real ToRealPositive(const Decimal256& decimal, int32_t scale) {
    const auto parts_le = decimal.little_endian_array();
    if (scale <= 0 || (parts_le[3] == 0 && parts_le[2] == 0 && parts_le[1] == 0 &&
                       parts_le[0] < RealTraits<Real>::kMaxPreciseInteger)) {
      // No need to split the decimal if it is already an integer (scale <= 0) or if it
      // can be precisely represented by Real
      return ToRealPositiveNoSplit<Real>(decimal, scale);
    }

    // Split the decimal into whole and fractional parts to avoid precision loss
    BasicDecimal256 whole_decimal, fraction_decimal;
    decimal.GetWholeAndFraction(scale, &whole_decimal, &fraction_decimal);

    Real whole = ToRealPositiveNoSplit<Real>(whole_decimal, 0);
    Real fraction = ToRealPositiveNoSplit<Real>(fraction_decimal, scale);
    return whole + fraction;
  }
};

}  // namespace

Result<Decimal256> Decimal256::FromReal(float x, int32_t precision, int32_t scale) {
  return Decimal256RealConversion::FromReal(x, precision, scale);
}

Result<Decimal256> Decimal256::FromReal(double x, int32_t precision, int32_t scale) {
  return Decimal256RealConversion::FromReal(x, precision, scale);
}

float Decimal256::ToFloat(int32_t scale) const {
  return Decimal256RealConversion::ToReal<float>(*this, scale);
}

double Decimal256::ToDouble(int32_t scale) const {
  return Decimal256RealConversion::ToReal<double>(*this, scale);
}

ARROW_EXPORT std::ostream& operator<<(std::ostream& os, const Decimal256& decimal) {
  os << decimal.ToIntegerString();
  return os;
}

}  // namespace arrow
