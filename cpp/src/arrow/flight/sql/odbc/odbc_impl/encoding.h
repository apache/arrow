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

#include <cassert>
#include <cstring>
#include <iterator>
#include <string>
#include <vector>
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/vendored/utfcpp/checked.h"

#if defined(__APPLE__)
#  include <atomic>
#endif

namespace arrow::flight::sql::odbc {

#if defined(__APPLE__)
extern std::atomic<size_t> SqlWCharSize;

void ComputeSqlWCharSize();

inline size_t GetSqlWCharSize() {
  if (SqlWCharSize == 0) {
    ComputeSqlWCharSize();
  }

  return SqlWCharSize;
}
#else
constexpr inline size_t GetSqlWCharSize() { return sizeof(char16_t); }
#endif

template <typename CHAR_TYPE>
inline size_t wcsstrlen(const void* wcs_string) {
  size_t len;
  for (len = 0; ((CHAR_TYPE*)wcs_string)[len]; len++) {
  }
  return len;
}

inline size_t wcsstrlen(const void* wcs_string) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return wcsstrlen<char16_t>(wcs_string);
    case sizeof(char32_t):
      return wcsstrlen<char32_t>(wcs_string);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

template <typename CHAR_TYPE>
inline void Utf8ToWcs(const char* utf8_string, size_t length,
                      std::vector<uint8_t>* result) {
  std::basic_string<CHAR_TYPE> string;
  if constexpr (sizeof(CHAR_TYPE) == sizeof(char16_t)) {
    ::utf8::utf8to16(utf8_string, utf8_string + length, std::back_inserter(string));
  } else {
    static_assert(sizeof(CHAR_TYPE) == sizeof(char32_t));
    ::utf8::utf8to32(utf8_string, utf8_string + length, std::back_inserter(string));
  }

  auto length_in_bytes = static_cast<uint32_t>(string.size() * sizeof(CHAR_TYPE));
  const uint8_t* data = (uint8_t*)string.data();

  result->reserve(length_in_bytes);
  result->assign(data, data + length_in_bytes);
}

inline void Utf8ToWcs(const char* utf8_string, size_t length,
                      std::vector<uint8_t>* result) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return Utf8ToWcs<char16_t>(utf8_string, length, result);
    case sizeof(char32_t):
      return Utf8ToWcs<char32_t>(utf8_string, length, result);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

inline void Utf8ToWcs(const char* utf8_string, std::vector<uint8_t>* result) {
  return Utf8ToWcs(utf8_string, strlen(utf8_string), result);
}

template <typename CHAR_TYPE>
inline void WcsToUtf8(const void* wcs_string, size_t length_in_code_units,
                      std::vector<uint8_t>* result) {
  const auto* begin = static_cast<const CHAR_TYPE*>(wcs_string);

  std::string string;
  if constexpr (sizeof(CHAR_TYPE) == sizeof(char16_t)) {
    ::utf8::utf16to8(begin, begin + length_in_code_units, std::back_inserter(string));
  } else {
    static_assert(sizeof(CHAR_TYPE) == sizeof(char32_t));
    ::utf8::utf32to8(begin, begin + length_in_code_units, std::back_inserter(string));
  }

  result->assign(string.begin(), string.end());
}

inline void WcsToUtf8(const void* wcs_string, size_t length_in_code_units,
                      std::vector<uint8_t>* result) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return WcsToUtf8<char16_t>(wcs_string, length_in_code_units, result);
    case sizeof(char32_t):
      return WcsToUtf8<char32_t>(wcs_string, length_in_code_units, result);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

inline void WcsToUtf8(const void* wcs_string, std::vector<uint8_t>* result) {
  return WcsToUtf8(wcs_string, wcsstrlen(wcs_string), result);
}

}  // namespace arrow::flight::sql::odbc
