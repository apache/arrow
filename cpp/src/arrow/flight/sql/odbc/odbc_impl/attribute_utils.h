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

#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

namespace ODBC {

using arrow::flight::sql::odbc::Diagnostics;
using arrow::flight::sql::odbc::DriverException;
using arrow::flight::sql::odbc::WcsToUtf8;

template <typename T, typename O>
inline void GetAttribute(T attribute_value, SQLPOINTER output, O output_size,
                         O* output_len_ptr) {
  if (output) {
    T* typed_output = reinterpret_cast<T*>(output);
    *typed_output = attribute_value;
  }

  if (output_len_ptr) {
    *output_len_ptr = sizeof(T);
  }
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string_view& attribute_value,
                                  SQLPOINTER output, O output_size, O* output_len_ptr) {
  if (output) {
    size_t output_len_before_null =
        std::min(static_cast<O>(attribute_value.size()), static_cast<O>(output_size - 1));
    std::memcpy(output, attribute_value.data(), output_len_before_null);
    reinterpret_cast<char*>(output)[output_len_before_null] = '\0';
  }

  if (output_len_ptr) {
    *output_len_ptr = static_cast<O>(attribute_value.size());
  }

  if (output && output_size < static_cast<O>(attribute_value.size() + 1)) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string_view& attribute_value,
                                  SQLPOINTER output, O output_size, O* output_len_ptr,
                                  Diagnostics& diagnostics) {
  SQLRETURN result =
      GetAttributeUTF8(attribute_value, output, output_size, output_len_ptr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string_view& attribute_value,
                                      bool is_length_in_bytes, SQLPOINTER output,
                                      O output_size, O* output_len_ptr) {
  size_t length = ConvertToSqlWChar(
      attribute_value, reinterpret_cast<SQLWCHAR*>(output),
      is_length_in_bytes ? output_size : output_size * GetSqlWCharSize());

  if (!is_length_in_bytes) {
    length = length / GetSqlWCharSize();
  }

  if (output_len_ptr) {
    *output_len_ptr = static_cast<O>(length);
  }

  if (output &&
      output_size <
          static_cast<O>(length + (is_length_in_bytes ? GetSqlWCharSize() : 1))) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string_view& attribute_value,
                                      bool is_length_in_bytes, SQLPOINTER output,
                                      O output_size, O* output_len_ptr,
                                      Diagnostics& diagnostics) {
  SQLRETURN result = GetAttributeSQLWCHAR(attribute_value, is_length_in_bytes, output,
                                          output_size, output_len_ptr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN GetStringAttribute(bool is_unicode,
                                    const std::string_view& attribute_value,
                                    bool is_length_in_bytes, SQLPOINTER output,
                                    O output_size, O* output_len_ptr,
                                    Diagnostics& diagnostics) {
  SQLRETURN result = SQL_SUCCESS;
  if (is_unicode) {
    result = GetAttributeSQLWCHAR(attribute_value, is_length_in_bytes, output,
                                  output_size, output_len_ptr);
  } else {
    result = GetAttributeUTF8(attribute_value, output, output_size, output_len_ptr);
  }

  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename T>
inline void SetAttribute(SQLPOINTER new_value, T& attribute_to_write) {
  SQLLEN valueAsLen = reinterpret_cast<SQLLEN>(new_value);
  attribute_to_write = static_cast<T>(valueAsLen);
}

template <typename T>
inline void SetPointerAttribute(SQLPOINTER new_value, T& attribute_to_write) {
  attribute_to_write = static_cast<T>(new_value);
}

inline void SetAttributeUTF8(SQLPOINTER new_value, SQLINTEGER input_length,
                             std::string& attribute_to_write) {
  const char* new_value_as_char = static_cast<const char*>(new_value);
  attribute_to_write.assign(new_value_as_char, input_length == SQL_NTS
                                                   ? strlen(new_value_as_char)
                                                   : input_length);
}

inline void SetAttributeSQLWCHAR(SQLPOINTER new_value, SQLINTEGER input_length_in_bytes,
                                 std::string& attribute_to_write) {
  thread_local std::vector<uint8_t> utf8_str;
  if (input_length_in_bytes == SQL_NTS) {
    WcsToUtf8(new_value, &utf8_str);
  } else {
    WcsToUtf8(new_value, input_length_in_bytes / GetSqlWCharSize(), &utf8_str);
  }
  attribute_to_write.assign((char*)utf8_str.data());
}

template <typename T>
void CheckIfAttributeIsSetToOnlyValidValue(SQLPOINTER value, T allowed_value) {
  if (static_cast<T>(reinterpret_cast<SQLULEN>(value)) != allowed_value) {
    throw DriverException("Optional feature not implemented", "HYC00");
  }
}
}  // namespace ODBC
