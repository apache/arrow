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

#ifndef ARROW_UTIL_PARSING_H
#define ARROW_UTIL_PARSING_H

#include <limits>
#include <locale>
#include <sstream>
#include <string>

#include "arrow/type.h"
#include "arrow/type_traits.h"

namespace arrow {
namespace internal {

/// \brief A class providing conversion from strings to some Arrow data types
///
/// Conversion is triggered by calling operator().  It returns true on
/// success, false on failure.
///
/// The class may have a non-trivial construction cost in some cases,
/// so it's recommended to use a single instance many times, if doing bulk
/// conversion.
///
template <typename ARROW_TYPE, typename Enable = void>
class StringConverter;

template <>
class StringConverter<BooleanType> {
 public:
  using value_type = bool;

  bool operator()(const char* s, size_t length, value_type* out) {
    if (length == 1) {
      // "0" or "1"?
      if (s[0] == '0') {
        *out = false;
        return true;
      }
      if (s[0] == '1') {
        *out = true;
        return true;
      }
      return false;
    }
    if (length == 4) {
      // "true"?
      *out = true;
      return ((s[0] == 't' || s[0] == 'T') && (s[1] == 'r' || s[1] == 'R') &&
              (s[2] == 'u' || s[2] == 'U') && (s[3] == 'e' || s[3] == 'E'));
    }
    if (length == 5) {
      // "false"?
      *out = false;
      return ((s[0] == 'f' || s[0] == 'F') && (s[1] == 'a' || s[1] == 'A') &&
              (s[2] == 'l' || s[2] == 'L') && (s[3] == 's' || s[3] == 'S') &&
              (s[4] == 'e' || s[4] == 'E'));
    }
    return false;
  }
};

template <class ARROW_TYPE>
class StringToFloatConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  StringToFloatConverterMixin() { ibuf.imbue(std::locale::classic()); }

  bool operator()(const char* s, size_t length, value_type* out) {
    ibuf.clear();
    ibuf.str(std::string(s, length));
    ibuf >> *out;
    // XXX Should we reset errno on failure?
    return !ibuf.fail() && ibuf.eof();
  }

 protected:
  std::istringstream ibuf;
};

template <>
class StringConverter<FloatType> : public StringToFloatConverterMixin<FloatType> {};

template <>
class StringConverter<DoubleType> : public StringToFloatConverterMixin<DoubleType> {};

// NOTE: HalfFloatType would require a half<->float conversion library

template <class ARROW_TYPE>
class StringConverter<ARROW_TYPE, enable_if_signed_integer<ARROW_TYPE>> {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  StringConverter() { ibuf.imbue(std::locale::classic()); }

  bool operator()(const char* s, size_t length, value_type* out) {
    static constexpr bool need_long_long = sizeof(value_type) > sizeof(long);  // NOLINT
    static constexpr value_type min_value = std::numeric_limits<value_type>::min();
    static constexpr value_type max_value = std::numeric_limits<value_type>::max();

    ibuf.clear();
    ibuf.str(std::string(s, length));
    if (need_long_long) {
      long long res;  // NOLINT
      ibuf >> res;
      *out = static_cast<value_type>(res);  // may downcast
      if (res < min_value || res > max_value) {
        return false;
      }
    } else {
      long res;  // NOLINT
      ibuf >> res;
      *out = static_cast<value_type>(res);  // may downcast
      if (res < min_value || res > max_value) {
        return false;
      }
    }
    // XXX Should we reset errno on failure?
    return !ibuf.fail() && ibuf.eof();
  }

 protected:
  std::istringstream ibuf;
};

template <class ARROW_TYPE>
class StringConverter<ARROW_TYPE, enable_if_unsigned_integer<ARROW_TYPE>> {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  StringConverter() { ibuf.imbue(std::locale::classic()); }

  bool operator()(const char* s, size_t length, value_type* out) {
    static constexpr bool need_long_long =
        sizeof(value_type) > sizeof(unsigned long);  // NOLINT
    static constexpr value_type max_value = std::numeric_limits<value_type>::max();

    ibuf.clear();
    ibuf.str(std::string(s, length));
    // XXX The following unfortunately allows negative input values
    if (need_long_long) {
      unsigned long long res;  // NOLINT
      ibuf >> res;
      *out = static_cast<value_type>(res);  // may downcast
      if (res > max_value) {
        return false;
      }
    } else {
      unsigned long res;  // NOLINT
      ibuf >> res;
      *out = static_cast<value_type>(res);  // may downcast
      if (res > max_value) {
        return false;
      }
    }
    // XXX Should we reset errno on failure?
    return !ibuf.fail() && ibuf.eof();
  }

 protected:
  std::istringstream ibuf;
};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_PARSING_H
