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
#include <type_traits>

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

// Ideas for faster float parsing:
// - http://rapidjson.org/md_doc_internals.html#ParsingDouble
// - https://github.com/google/double-conversion
// - https://github.com/achan001/dtoa-fast

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

namespace detail {

#define PARSE_UNSIGNED_ITERATION(C_TYPE)              \
  if (length > 0) {                                   \
    uint8_t digit = static_cast<uint8_t>(*s++ - '0'); \
    result = static_cast<C_TYPE>(result * 10U);       \
    length--;                                         \
    if (ARROW_PREDICT_FALSE(digit > 9U)) {            \
      /* Non-digit */                                 \
      return false;                                   \
    }                                                 \
    result = static_cast<C_TYPE>(result + digit);     \
  }

#define PARSE_UNSIGNED_ITERATION_LAST(C_TYPE)                                     \
  if (length > 0) {                                                               \
    if (ARROW_PREDICT_FALSE(result > std::numeric_limits<C_TYPE>::max() / 10U)) { \
      /* Overflow */                                                              \
      return false;                                                               \
    }                                                                             \
    uint8_t digit = static_cast<uint8_t>(*s++ - '0');                             \
    result = static_cast<C_TYPE>(result * 10U);                                   \
    C_TYPE new_result = static_cast<C_TYPE>(result + digit);                      \
    if (ARROW_PREDICT_FALSE(--length > 0)) {                                      \
      /* Too many digits */                                                       \
      return false;                                                               \
    }                                                                             \
    if (ARROW_PREDICT_FALSE(digit > 9U)) {                                        \
      /* Non-digit */                                                             \
      return false;                                                               \
    }                                                                             \
    if (ARROW_PREDICT_FALSE(new_result < result)) {                               \
      /* Overflow */                                                              \
      return false;                                                               \
    }                                                                             \
    result = new_result;                                                          \
  }

inline bool ParseUnsigned(const char* s, size_t length, uint8_t* out) {
  uint8_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint8_t);
  PARSE_UNSIGNED_ITERATION(uint8_t);
  PARSE_UNSIGNED_ITERATION_LAST(uint8_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint16_t* out) {
  uint16_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION(uint16_t);
  PARSE_UNSIGNED_ITERATION_LAST(uint16_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint32_t* out) {
  uint32_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);

  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);
  PARSE_UNSIGNED_ITERATION(uint32_t);

  PARSE_UNSIGNED_ITERATION_LAST(uint32_t);
  *out = result;
  return true;
}

inline bool ParseUnsigned(const char* s, size_t length, uint64_t* out) {
  uint64_t result = 0;

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);
  PARSE_UNSIGNED_ITERATION(uint64_t);

  PARSE_UNSIGNED_ITERATION_LAST(uint64_t);
  *out = result;
  return true;
}

#undef PARSE_UNSIGNED_ITERATION
#undef PARSE_UNSIGNED_ITERATION_LAST

}  // namespace detail

template <class ARROW_TYPE>
class StringToUnsignedIntConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;

  bool operator()(const char* s, size_t length, value_type* out) {
    if (ARROW_PREDICT_FALSE(length == 0)) {
      return false;
    }
    // Skip leading zeros
    while (length > 0 && *s == '0') {
      length--;
      s++;
    }
    return detail::ParseUnsigned(s, length, out);
  }
};

template <>
class StringConverter<UInt8Type> : public StringToUnsignedIntConverterMixin<UInt8Type> {};

template <>
class StringConverter<UInt16Type> : public StringToUnsignedIntConverterMixin<UInt16Type> {
};

template <>
class StringConverter<UInt32Type> : public StringToUnsignedIntConverterMixin<UInt32Type> {
};

template <>
class StringConverter<UInt64Type> : public StringToUnsignedIntConverterMixin<UInt64Type> {
};

template <class ARROW_TYPE>
class StringToSignedIntConverterMixin {
 public:
  using value_type = typename ARROW_TYPE::c_type;
  using unsigned_type = typename std::make_unsigned<value_type>::type;

  bool operator()(const char* s, size_t length, value_type* out) {
    static constexpr unsigned_type max_positive =
        static_cast<unsigned_type>(std::numeric_limits<value_type>::max());
    // Assuming two's complement
    static constexpr unsigned_type max_negative = max_positive + 1;
    bool negative = false;
    unsigned_type unsigned_value = 0;

    if (ARROW_PREDICT_FALSE(length == 0)) {
      return false;
    }
    if (*s == '-') {
      negative = true;
      s++;
      if (--length == 0) {
        return false;
      }
    }
    // Skip leading zeros
    while (length > 0 && *s == '0') {
      length--;
      s++;
    }
    if (!ARROW_PREDICT_TRUE(detail::ParseUnsigned(s, length, &unsigned_value))) {
      return false;
    }
    if (negative) {
      if (ARROW_PREDICT_FALSE(unsigned_value > max_negative)) {
        return false;
      }
      *out = static_cast<value_type>(-static_cast<value_type>(unsigned_value));
    } else {
      if (ARROW_PREDICT_FALSE(unsigned_value > max_positive)) {
        return false;
      }
      *out = static_cast<value_type>(unsigned_value);
    }
    return true;
  }
};

template <>
class StringConverter<Int8Type> : public StringToSignedIntConverterMixin<Int8Type> {};

template <>
class StringConverter<Int16Type> : public StringToSignedIntConverterMixin<Int16Type> {};

template <>
class StringConverter<Int32Type> : public StringToSignedIntConverterMixin<Int32Type> {};

template <>
class StringConverter<Int64Type> : public StringToSignedIntConverterMixin<Int64Type> {};

}  // namespace internal
}  // namespace arrow

#endif  // ARROW_UTIL_PARSING_H
