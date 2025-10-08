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
#include "arrow/flight/sql/odbc/odbc_impl/encoding.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include <codecvt>
#include <cstring>
#include <locale>
#include <memory>
#include <string>

#define _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING

namespace ODBC {

using arrow::flight::sql::odbc::DriverException;
using arrow::flight::sql::odbc::GetSqlWCharSize;
using arrow::flight::sql::odbc::Utf8ToWcs;
using arrow::flight::sql::odbc::WcsToUtf8;

// Return the number of bytes required for the conversion.
template <typename CHAR_TYPE>
inline size_t ConvertToSqlWChar(const std::string_view& str, SQLWCHAR* buffer,
                                SQLLEN buffer_size_in_bytes) {
  thread_local std::vector<uint8_t> wstr;
  Utf8ToWcs<CHAR_TYPE>(str.data(), str.size(), &wstr);
  SQLLEN value_length_in_bytes = wstr.size();

  if (buffer) {
    std::memcpy(buffer, wstr.data(),
                std::min(static_cast<SQLLEN>(wstr.size()), buffer_size_in_bytes));

    // Write a NUL terminator
    if (buffer_size_in_bytes >=
        value_length_in_bytes + static_cast<SQLLEN>(GetSqlWCharSize())) {
      reinterpret_cast<CHAR_TYPE*>(buffer)[value_length_in_bytes / GetSqlWCharSize()] =
          '\0';
    } else {
      SQLLEN num_chars_written = buffer_size_in_bytes / GetSqlWCharSize();
      // If we failed to even write one char, the buffer is too small to hold a
      // NUL-terminator.
      if (num_chars_written > 0) {
        reinterpret_cast<CHAR_TYPE*>(buffer)[num_chars_written - 1] = '\0';
      }
    }
  }
  return value_length_in_bytes;
}

inline size_t ConvertToSqlWChar(const std::string_view& str, SQLWCHAR* buffer,
                                SQLLEN buffer_size_in_bytes) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return ConvertToSqlWChar<char16_t>(str, buffer, buffer_size_in_bytes);
    case sizeof(char32_t):
      return ConvertToSqlWChar<char32_t>(str, buffer, buffer_size_in_bytes);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

/// \brief Convert buffer of SqlWchar to standard string
/// \param[in] wchar_msg SqlWchar to convert
/// \param[in] msg_len Number of characters in wchar_msg
/// \return wchar_msg in std::string format
inline std::string SqlWcharToString(SQLWCHAR* wchar_msg, SQLINTEGER msg_len = SQL_NTS) {
  if (!wchar_msg || wchar_msg[0] == 0 || msg_len == 0) {
    return std::string();
  }

  thread_local std::vector<uint8_t> utf8_str;

  if (msg_len == SQL_NTS) {
    WcsToUtf8((void*)wchar_msg, &utf8_str);
  } else {
    WcsToUtf8((void*)wchar_msg, msg_len, &utf8_str);
  }

  return std::string(utf8_str.begin(), utf8_str.end());
}

inline std::string SqlStringToString(const unsigned char* sql_str,
                                     int32_t sql_str_len = SQL_NTS) {
  std::string res;

  const char* sql_str_c = reinterpret_cast<const char*>(sql_str);

  if (!sql_str) return res;

  if (sql_str_len == SQL_NTS)
    res.assign(sql_str_c);
  else if (sql_str_len > 0)
    res.assign(sql_str_c, sql_str_len);

  return res;
}
}  // namespace ODBC
