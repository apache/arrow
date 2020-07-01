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

#pragma once

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <string>
#include <utility>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/basic_decimal.h"
#include "arrow/util/string_view.h"

namespace arrow {

/// Represents a signed 128-bit integer in two's complement.
/// Calculations wrap around and overflow is ignored.
/// The max decimal precision that can be safely represented is
/// 38 significant digits.
///
/// For a discussion of the algorithms, look at Knuth's volume 2,
/// Semi-numerical Algorithms section 4.3.1.
///
/// Adapted from the Apache ORC C++ implementation
///
/// The implementation is split into two parts :
///
/// 1. BasicDecimal128
///    - can be safely compiled to IR without references to libstdc++.
/// 2. Decimal128
///    - has additional functionality on top of BasicDecimal128 to deal with
///      strings and streams.
class ARROW_EXPORT Decimal128 : public BasicDecimal128 {
 public:
  /// \cond FALSE
  // (need to avoid a duplicate definition in Sphinx)
  using BasicDecimal128::BasicDecimal128;
  /// \endcond

  /// \brief constructor creates a Decimal128 from a BasicDecimal128.
  constexpr Decimal128(const BasicDecimal128& value) noexcept : BasicDecimal128(value) {}

  /// \brief Parse the number from a base 10 string representation.
  explicit Decimal128(const std::string& value);

  /// \brief Empty constructor creates a Decimal128 with a value of 0.
  // This is required on some older compilers.
  constexpr Decimal128() noexcept : BasicDecimal128() {}

  /// Divide this number by right and return the result.
  ///
  /// This operation is not destructive.
  /// The answer rounds to zero. Signs work like:
  ///   21 /  5 ->  4,  1
  ///  -21 /  5 -> -4, -1
  ///   21 / -5 -> -4,  1
  ///  -21 / -5 ->  4, -1
  /// \param[in] divisor the number to divide by
  /// \return the pair of the quotient and the remainder
  Result<std::pair<Decimal128, Decimal128>> Divide(const Decimal128& divisor) const {
    std::pair<Decimal128, Decimal128> result;
    auto dstatus = BasicDecimal128::Divide(divisor, &result.first, &result.second);
    ARROW_RETURN_NOT_OK(ToArrowStatus(dstatus));
    return std::move(result);
  }

  /// \brief Convert the Decimal128 value to a base 10 decimal string with the given
  /// scale.
  std::string ToString(int32_t scale) const;

  /// \brief Convert the value to an integer string
  std::string ToIntegerString() const;

  /// \brief Cast this value to an int64_t.
  explicit operator int64_t() const;

  /// \brief Convert a decimal string to a Decimal128 value, optionally including
  /// precision and scale if they're passed in and not null.
  static Status FromString(const util::string_view& s, Decimal128* out,
                           int32_t* precision, int32_t* scale = NULLPTR);
  static Status FromString(const std::string& s, Decimal128* out, int32_t* precision,
                           int32_t* scale = NULLPTR);
  static Status FromString(const char* s, Decimal128* out, int32_t* precision,
                           int32_t* scale = NULLPTR);
  static Result<Decimal128> FromString(const util::string_view& s);
  static Result<Decimal128> FromString(const std::string& s);
  static Result<Decimal128> FromString(const char* s);

  static Result<Decimal128> FromReal(double real, int32_t precision, int32_t scale);
  static Result<Decimal128> FromReal(float real, int32_t precision, int32_t scale);

  /// \brief Convert from a big-endian byte representation. The length must be
  ///        between 1 and 16.
  /// \return error status if the length is an invalid value
  static Result<Decimal128> FromBigEndian(const uint8_t* data, int32_t length);

  /// \brief Convert Decimal128 from one scale to another
  Result<Decimal128> Rescale(int32_t original_scale, int32_t new_scale) const {
    Decimal128 out;
    auto dstatus = BasicDecimal128::Rescale(original_scale, new_scale, &out);
    ARROW_RETURN_NOT_OK(ToArrowStatus(dstatus));
    return std::move(out);
  }

  /// \brief Convert to a signed integer
  template <typename T, typename = internal::EnableIfIsOneOf<T, int32_t, int64_t>>
  Result<T> ToInteger() const {
    constexpr auto min_value = std::numeric_limits<T>::min();
    constexpr auto max_value = std::numeric_limits<T>::max();
    const auto& self = *this;
    if (self < min_value || self > max_value) {
      return Status::Invalid("Invalid cast from Decimal128 to ", sizeof(T),
                             " byte integer");
    }
    return static_cast<T>(low_bits());
  }

  /// \brief Convert to a signed integer
  template <typename T, typename = internal::EnableIfIsOneOf<T, int32_t, int64_t>>
  Status ToInteger(T* out) const {
    return ToInteger<T>().Value(out);
  }

  /// \brief Convert to a floating-point number (scaled)
  float ToFloat(int32_t scale) const;
  /// \brief Convert to a floating-point number (scaled)
  double ToDouble(int32_t scale) const;

  /// \brief Convert to a floating-point number (scaled)
  template <typename T>
  T ToReal(int32_t scale) const {
    return ToRealConversion<T>::ToReal(*this, scale);
  }

  friend ARROW_EXPORT std::ostream& operator<<(std::ostream& os,
                                               const Decimal128& decimal);

 private:
  /// Converts internal error code to Status
  Status ToArrowStatus(DecimalStatus dstatus) const;

  template <typename T>
  struct ToRealConversion {};
};

template <>
struct Decimal128::ToRealConversion<float> {
  static float ToReal(const Decimal128& dec, int32_t scale) { return dec.ToFloat(scale); }
};

template <>
struct Decimal128::ToRealConversion<double> {
  static double ToReal(const Decimal128& dec, int32_t scale) {
    return dec.ToDouble(scale);
  }
};

}  // namespace arrow
