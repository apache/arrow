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
#include <cstdlib>
#include <sstream>

#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int128.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace DecimalUtil {

void StringToInteger(const std::string& whole, const std::string& fractional, int8_t sign,
                     Int128* out) {
  DCHECK(sign == -1 || sign == 1);
  DCHECK_NE(out, nullptr);
  DCHECK(!whole.empty() || !fractional.empty());
  *out = Int128(whole + fractional) * sign;
}

Status FromString(const std::string& s, Int128* out, int* precision, int* scale) {
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
  charp = std::find_if_not(charp, end, [](char c) { return c == '0'; });

  // all zeros and no decimal point
  if (charp == end) {
    if (out != nullptr) {
      *out = Int128(0);
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

  charp = std::find_if_not(charp, end, [](char c) { return std::isdigit(c) != 0; });

  std::string::const_iterator whole_part_end = charp;
  std::string whole_part(whole_part_start, whole_part_end);

  if (charp != end && *charp == '.') {
    ++charp;

    if (charp == end) {
      return Status::Invalid(
          "Decimal point must be followed by at least one base ten digit. Reached the "
          "end of the string.");
    }

    if (std::isdigit(*charp) == 0) {
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
    charp = std::find_if_not(charp, end, [](char c) { return std::isdigit(c) != 0; });

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
    StringToInteger(whole_part, fractional_part, sign, out);
  }

  return Status::OK();
}

std::string ToString(const Int128& decimal_value, int precision, int scale) {
  Int128 value(decimal_value);

  // Decimal values are sent to clients as strings so in the interest of
  // speed the string will be created without the using stringstream with the
  // whole/fractional_part().
  size_t last_char_idx = precision + (scale > 0)  // Add a space for decimal place
                         + (scale == precision)   // Add a space for leading 0
                         + (value < 0);           // Add a space for negative sign
  std::string str(last_char_idx, '0');

  // Start filling in the values in reverse order by taking the last digit
  // of the value. Use a positive value and worry about the sign later. At this
  // point the last_char_idx points to the string terminator.
  Int128 remaining_value(value);

  size_t first_digit_idx = 0;
  if (value < 0) {
    remaining_value = -value;
    first_digit_idx = 1;
  }

  if (scale > 0) {
    int remaining_scale = scale;
    do {
      str[--last_char_idx] =
          static_cast<char>(remaining_value % 10 + '0');  // Ascii offset
      remaining_value /= 10;
    } while (--remaining_scale > 0);
    str[--last_char_idx] = '.';
    DCHECK_GT(last_char_idx, first_digit_idx) << "Not enough space remaining";
  }

  do {
    str[--last_char_idx] = static_cast<char>(remaining_value % 10 + '0');  // Ascii offset
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

  if (value < 0) {
    str[0] = '-';
  }

  return str;
}

}  // namespace DecimalUtil
}  // namespace arrow
