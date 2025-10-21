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

// Return the number of bytes required for the conversion.
template <typename CHAR_TYPE>
inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer,
                                SQLLEN buffer_size_in_bytes) {
  thread_local std::vector<uint8_t> wstr;
  Utf8ToWcs<CHAR_TYPE>(str.data(), str.size(), &wstr);
  SQLLEN value_length_in_bytes = wstr.size();

  if (buffer) {
    memcpy(buffer, wstr.data(),
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

inline size_t ConvertToSqlWChar(const std::string& str, SQLWCHAR* buffer,
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

}  // namespace ODBC
