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

}  // namespace ODBC
