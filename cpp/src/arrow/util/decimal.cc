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

#include <cctype>
#include <cmath>
#include <sstream>

#include "arrow/util/decimal.h"
#include "arrow/util/int128.h"

namespace arrow {
namespace decimal {

template <typename T>
ARROW_EXPORT Status FromString(const std::string& s, Decimal<T>* out, int* precision,
                               int* scale) {
  // Implements this regex: "(\\+?|-?)((0*)(\\d*))(\\.(\\d+))?";
  if (s.empty()) {
    return Status::Invalid("Empty string cannot be converted to decimal");
  }

  int8_t sign = 1;
  std::string::const_iterator charp = s.cbegin();
  std::string::const_iterator end = s.cend();

  char first_char = *charp;
  if (first_char == '+' || first_char == '-') {
    if (first_char == '-') {
      sign = -1;
    }
    ++charp;
  }

  if (charp == end) {
    std::stringstream ss;
    ss << "Single character: '" << first_char << "' is not a valid decimal value";
    return Status::Invalid(ss.str());
  }

  std::string::const_iterator numeric_string_start = charp;

  DCHECK_LT(charp, end);

  // skip leading zeros
  while (charp != end && *charp == '0') {
    ++charp;
  }

  // all zeros and no decimal point
  if (charp == end) {
    if (out != nullptr) {
      out->value = static_cast<T>(0);
    }

    // Not sure what other libraries assign precision to for this case (this case of
    // a string consisting only of one or more zeros)
    if (precision != nullptr) {
      *precision = static_cast<int>(charp - numeric_string_start);
    }

    if (scale != nullptr) {
      *scale = 0;
    }

    return Status::OK();
  }

  std::string::const_iterator whole_part_start = charp;

  while (charp != end && isdigit(*charp)) {
    ++charp;
  }

  std::string::const_iterator whole_part_end = charp;
  std::string whole_part(whole_part_start, whole_part_end);

  if (charp != end && *charp == '.') {
    ++charp;

    if (charp == end) {
      return Status::Invalid(
          "Decimal point must be followed by at least one base ten digit. Reached the "
          "end of the string.");
    }

    if (!isdigit(*charp)) {
      std::stringstream ss;
      ss << "Decimal point must be followed by a base ten digit. Found '" << *charp
         << "'";
      return Status::Invalid(ss.str());
    }
  } else {
    if (charp != end) {
      std::stringstream ss;
      ss << "Expected base ten digit or decimal point but found '" << *charp
         << "' instead.";
      return Status::Invalid(ss.str());
    }
  }

  std::string::const_iterator fractional_part_start = charp;

  // The rest must be digits, because if we have a decimal point it must be followed by
  // digits
  if (charp != end) {
    while (charp != end && isdigit(*charp)) {
      ++charp;
    }

    // The while loop has ended before the end of the string which means we've hit a
    // character that isn't a base ten digit
    if (charp != end) {
      std::stringstream ss;
      ss << "Found non base ten digit character '" << *charp
         << "' before the end of the string";
      return Status::Invalid(ss.str());
    }
  }

  std::string::const_iterator fractional_part_end = charp;
  std::string fractional_part(fractional_part_start, fractional_part_end);

  if (precision != nullptr) {
    *precision = static_cast<int>(whole_part.size() + fractional_part.size());
  }

  if (scale != nullptr) {
    *scale = static_cast<int>(fractional_part.size());
  }

  if (out != nullptr) {
    StringToInteger(whole_part, fractional_part, sign, &out->value);
  }

  return Status::OK();
}

template ARROW_EXPORT Status FromString(const std::string& s, Decimal32* out,
                                        int* precision, int* scale);
template ARROW_EXPORT Status FromString(const std::string& s, Decimal64* out,
                                        int* precision, int* scale);
template ARROW_EXPORT Status FromString(const std::string& s, Decimal128* out,
                                        int* precision, int* scale);

void StringToInteger(const std::string& whole, const std::string& fractional, int8_t sign,
                     int32_t* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());

  if (!whole.empty()) {
    *out = std::stoi(whole) *
           static_cast<int32_t>(pow(10.0, static_cast<double>(fractional.size())));
  }
  if (!fractional.empty()) {
    *out += std::stoi(fractional, nullptr, 10);
  }
  *out *= sign;
}

void StringToInteger(const std::string& whole, const std::string& fractional, int8_t sign,
                     int64_t* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  if (!whole.empty()) {
    *out = static_cast<int64_t>(std::stoll(whole)) *
           static_cast<int64_t>(pow(10.0, static_cast<double>(fractional.size())));
  }
  if (!fractional.empty()) {
    *out += std::stoll(fractional, nullptr, 10);
  }
  *out *= sign;
}

void StringToInteger(const std::string& whole, const std::string& fractional, int8_t sign,
                     Int128* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  *out = Int128(whole + fractional) * sign;
}

void FromBytes(const uint8_t* bytes, Decimal32* decimal) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(decimal, nullptr);
  decimal->value = *reinterpret_cast<const int32_t*>(bytes);
}

void FromBytes(const uint8_t* bytes, Decimal64* decimal) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(decimal, nullptr);
  decimal->value = *reinterpret_cast<const int64_t*>(bytes);
}

void FromBytes(const uint8_t* bytes, Decimal128* decimal) {
  decimal->value = Int128(bytes);
}

void ToBytes(const Decimal32& value, uint8_t** bytes) {
  DCHECK_NE(*bytes, nullptr);
  *reinterpret_cast<int32_t*>(*bytes) = value.value;
}

void ToBytes(const Decimal64& value, uint8_t** bytes) {
  DCHECK_NE(*bytes, nullptr);
  *reinterpret_cast<int64_t*>(*bytes) = value.value;
}

void ToBytes(const Decimal128& decimal, uint8_t** bytes) {
  DCHECK_NE(bytes, nullptr);
  DCHECK_NE(*bytes, nullptr);
  decimal.value.ToBytes(bytes);
}

}  // namespace decimal
}  // namespace arrow
