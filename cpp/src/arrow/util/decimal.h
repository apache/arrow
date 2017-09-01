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

#include <cstdint>
#include <cstdlib>
#include <string>

#include "arrow/status.h"
#include "arrow/util/int128.h"  // IWYU pragma: export
#include "arrow/util/logging.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace decimal {

template <typename T>
struct ARROW_EXPORT Decimal;

ARROW_EXPORT void StringToInteger(const std::string& whole, const std::string& fractional,
                                  int8_t sign, int32_t* out);
ARROW_EXPORT void StringToInteger(const std::string& whole, const std::string& fractional,
                                  int8_t sign, int64_t* out);
ARROW_EXPORT void StringToInteger(const std::string& whole, const std::string& fractional,
                                  int8_t sign, Int128* out);

template <typename T>
ARROW_EXPORT Status FromString(const std::string& s, Decimal<T>* out,
                               int* precision = nullptr, int* scale = nullptr);

template <typename T>
struct ARROW_EXPORT Decimal {
  Decimal() : value() {}
  explicit Decimal(const std::string& s) : value() { DCHECK(FromString(s, this).ok()); }
  explicit Decimal(const char* s) : Decimal(std::string(s)) {}
  explicit Decimal(const T& value) : value(value) {}

  using value_type = T;
  value_type value;
};

using Decimal32 = Decimal<int32_t>;
using Decimal64 = Decimal<int64_t>;
using Decimal128 = Decimal<Int128>;

template <typename T>
struct ARROW_EXPORT DecimalPrecision {};

template <>
struct ARROW_EXPORT DecimalPrecision<int32_t> {
  constexpr static const int minimum = 1;
  constexpr static const int maximum = 9;
};

template <>
struct ARROW_EXPORT DecimalPrecision<int64_t> {
  constexpr static const int minimum = 10;
  constexpr static const int maximum = 18;
};

template <>
struct ARROW_EXPORT DecimalPrecision<Int128> {
  constexpr static const int minimum = 19;
  constexpr static const int maximum = 38;
};

template <typename T>
ARROW_EXPORT std::string ToString(const Decimal<T>& decimal_value, int precision,
                                  int scale) {
  T value = decimal_value.value;

  // Decimal values are sent to clients as strings so in the interest of
  // speed the string will be created without the using stringstream with the
  // whole/fractional_part().
  size_t last_char_idx = precision + (scale > 0)  // Add a space for decimal place
                         + (scale == precision)   // Add a space for leading 0
                         + (value < 0);           // Add a space for negative sign
  std::string str = std::string(last_char_idx, '0');
  // Start filling in the values in reverse order by taking the last digit
  // of the value. Use a positive value and worry about the sign later. At this
  // point the last_char_idx points to the string terminator.
  T remaining_value = value;
  size_t first_digit_idx = 0;
  if (value < 0) {
    remaining_value = -value;
    first_digit_idx = 1;
  }
  if (scale > 0) {
    int remaining_scale = scale;
    do {
      str[--last_char_idx] =
          static_cast<char>(remaining_value % 10 + static_cast<T>('0'));  // Ascii offset
      remaining_value /= 10;
    } while (--remaining_scale > 0);
    str[--last_char_idx] = '.';
    DCHECK_GT(last_char_idx, first_digit_idx) << "Not enough space remaining";
  }
  do {
    str[--last_char_idx] =
        static_cast<char>(remaining_value % 10 + static_cast<T>('0'));  // Ascii offset
    remaining_value /= 10;
    if (remaining_value == 0) {
      // Trim any extra leading 0's.
      if (last_char_idx > first_digit_idx) {
        str.erase(0, last_char_idx - first_digit_idx);
      }

      break;
    }
    // For safety, enforce string length independent of remaining_value.
  } while (last_char_idx > first_digit_idx);
  if (value < 0) str[0] = '-';
  return str;
}

/// Conversion from raw bytes to a Decimal value
ARROW_EXPORT void FromBytes(const uint8_t* bytes, Decimal32* value);
ARROW_EXPORT void FromBytes(const uint8_t* bytes, Decimal64* value);
ARROW_EXPORT void FromBytes(const uint8_t* bytes, Decimal128* decimal);

/// Conversion from a Decimal value to raw bytes
ARROW_EXPORT void ToBytes(const Decimal32& value, uint8_t** bytes);
ARROW_EXPORT void ToBytes(const Decimal64& value, uint8_t** bytes);
ARROW_EXPORT void ToBytes(const Decimal128& decimal, uint8_t** bytes);

}  // namespace decimal
}  // namespace arrow
#endif  // ARROW_DECIMAL_H
