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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/encoding.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>
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
using driver::odbcabstraction::DriverException;
using driver::odbcabstraction::GetSqlWCharSize;
using driver::odbcabstraction::Utf8ToWcs;

// Return the number of bytes required for the conversion.
template <typename CHAR_TYPE>
inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer,
                                SQLLEN bufferSizeInBytes) {
  thread_local std::vector<uint8_t> wstr;
  Utf8ToWcs<CHAR_TYPE>(str.data(), str.size(), &wstr);
  SQLLEN valueLengthInBytes = wstr.size();

  if (buffer) {
    memcpy(buffer, wstr.data(),
           std::min(static_cast<SQLLEN>(wstr.size()), bufferSizeInBytes));

    // Write a NUL terminator
    if (bufferSizeInBytes >=
        valueLengthInBytes + static_cast<SQLLEN>(GetSqlWCharSize())) {
      reinterpret_cast<CHAR_TYPE*>(buffer)[valueLengthInBytes / GetSqlWCharSize()] = '\0';
    } else {
      SQLLEN numCharsWritten = bufferSizeInBytes / GetSqlWCharSize();
      // If we failed to even write one char, the buffer is too small to hold a
      // NUL-terminator.
      if (numCharsWritten > 0) {
        reinterpret_cast<CHAR_TYPE*>(buffer)[numCharsWritten - 1] = '\0';
      }
    }
  }
  return valueLengthInBytes;
}

inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer,
                                SQLLEN bufferSizeInBytes) {
  switch (GetSqlWCharSize()) {
    case sizeof(char16_t):
      return ConvertToSqlWChar<char16_t>(str, buffer, bufferSizeInBytes);
    case sizeof(char32_t):
      return ConvertToSqlWChar<char32_t>(str, buffer, bufferSizeInBytes);
    default:
      assert(false);
      throw DriverException("Encoding is unsupported, SQLWCHAR size: " +
                            std::to_string(GetSqlWCharSize()));
  }
}

/// @brief Convert buffer of SqlWchar to standard string
/// @param wchar_msg[in] SqlWchar to convert
/// @param msg_len[in] Number of characters in wchar_msg
/// @return wchar_msg in std::string format
inline std::string SqlWcharToString(SQLWCHAR* wchar_msg, SQLSMALLINT msg_len = SQL_NTS) {
  if (wchar_msg == nullptr) {
    return std::string();
  }

  // Size of SQLWCHAR depends on the platform
  size_t wchar_size = sizeof(SQLWCHAR);

  // Assert that size of SQLWCHAR and wchar_t are the same
  // This assert is unsafe if the driver is built using unixODBC headers
  static_assert(sizeof(SQLWCHAR) == sizeof(wchar_t));

  // Copy SQLWCHAR into a temp wstring
  std::wstring wstring_msg;

  if (msg_len != SQL_NTS) {
    for (int i = 0; wchar_msg[i] != 0 && i < msg_len; i++) {
      wstring_msg.push_back(wchar_msg[i]);
    }
  } else {
    for (int i = 0; wchar_msg[i] != 0; i++) {
      wstring_msg.push_back(wchar_msg[i]);
    }
  }

  // Converter for SQLWCHAR to wstring
  static std::wstring_convert<std::codecvt_utf8<wchar_t>> conv;
  return conv.to_bytes(wstring_msg);
}

inline std::string SqlStringToString(const unsigned char* sqlStr,
                                     int32_t sqlStrLen = SQL_NTS) {
  std::string res;

  const char* sqlStrC = reinterpret_cast<const char*>(sqlStr);

  if (!sqlStr) return res;

  if (sqlStrLen == SQL_NTS)
    res.assign(sqlStrC);
  else if (sqlStrLen > 0)
    res.assign(sqlStrC, sqlStrLen);

  return res;
}

inline size_t CopyStringToBuffer(const std::string& str, char* buf, size_t buflen) {
  if (!buf || !buflen) return 0;

  size_t bytesToCopy = std::min(str.size(), static_cast<size_t>(buflen - 1));

  memcpy(buf, str.data(), bytesToCopy);
  buf[bytesToCopy] = 0;

  return bytesToCopy;
}

}  // namespace ODBC
