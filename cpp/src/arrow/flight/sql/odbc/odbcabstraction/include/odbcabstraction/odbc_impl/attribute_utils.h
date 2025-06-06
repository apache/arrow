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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>
#include <sql.h>
#include <sqlext.h>
#include <algorithm>
#include <cstring>
#include <memory>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h>

namespace ODBC {
using driver::odbcabstraction::WcsToUtf8;

template <typename T, typename O>
inline void GetAttribute(T attributeValue, SQLPOINTER output, O outputSize,
                         O* outputLenPtr) {
  if (output) {
    T* typedOutput = reinterpret_cast<T*>(output);
    *typedOutput = attributeValue;
  }

  if (outputLenPtr) {
    *outputLenPtr = sizeof(T);
  }
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string& attributeValue, SQLPOINTER output,
                                  O outputSize, O* outputLenPtr) {
  if (output) {
    size_t outputLenBeforeNul =
        std::min(static_cast<O>(attributeValue.size()), static_cast<O>(outputSize - 1));
    memcpy(output, attributeValue.c_str(), outputLenBeforeNul);
    reinterpret_cast<char*>(output)[outputLenBeforeNul] = '\0';
  }

  if (outputLenPtr) {
    *outputLenPtr = static_cast<O>(attributeValue.size());
  }

  if (output && outputSize < static_cast<O>(attributeValue.size() + 1)) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeUTF8(const std::string& attributeValue, SQLPOINTER output,
                                  O outputSize, O* outputLenPtr,
                                  driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = GetAttributeUTF8(attributeValue, output, outputSize, outputLenPtr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string& attributeValue,
                                      bool isLengthInBytes, SQLPOINTER output,
                                      O outputSize, O* outputLenPtr) {
  size_t result =
      ConvertToSqlWChar(attributeValue, reinterpret_cast<SQLWCHAR*>(output),
                        isLengthInBytes ? outputSize : outputSize * GetSqlWCharSize());

  if (outputLenPtr) {
    *outputLenPtr = static_cast<O>(isLengthInBytes ? result : result / GetSqlWCharSize());
  }

  if (output &&
      outputSize < static_cast<O>(result + (isLengthInBytes ? GetSqlWCharSize() : 1))) {
    return SQL_SUCCESS_WITH_INFO;
  }
  return SQL_SUCCESS;
}

template <typename O>
inline SQLRETURN GetAttributeSQLWCHAR(const std::string& attributeValue,
                                      bool isLengthInBytes, SQLPOINTER output,
                                      O outputSize, O* outputLenPtr,
                                      driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = GetAttributeSQLWCHAR(attributeValue, isLengthInBytes, output,
                                          outputSize, outputLenPtr);
  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename O>
inline SQLRETURN GetStringAttribute(bool isUnicode, const std::string& attributeValue,
                                    bool isLengthInBytes, SQLPOINTER output, O outputSize,
                                    O* outputLenPtr,
                                    driver::odbcabstraction::Diagnostics& diagnostics) {
  SQLRETURN result = SQL_SUCCESS;
  if (isUnicode) {
    result = GetAttributeSQLWCHAR(attributeValue, isLengthInBytes, output, outputSize,
                                  outputLenPtr);
  } else {
    result = GetAttributeUTF8(attributeValue, output, outputSize, outputLenPtr);
  }

  if (SQL_SUCCESS_WITH_INFO == result) {
    diagnostics.AddTruncationWarning();
  }
  return result;
}

template <typename T>
inline void SetAttribute(SQLPOINTER newValue, T& attributeToWrite) {
  SQLLEN valueAsLen = reinterpret_cast<SQLLEN>(newValue);
  attributeToWrite = static_cast<T>(valueAsLen);
}

template <typename T>
inline void SetPointerAttribute(SQLPOINTER newValue, T& attributeToWrite) {
  attributeToWrite = static_cast<T>(newValue);
}

inline void SetAttributeUTF8(SQLPOINTER newValue, SQLINTEGER inputLength,
                             std::string& attributeToWrite) {
  const char* newValueAsChar = static_cast<const char*>(newValue);
  attributeToWrite.assign(newValueAsChar,
                          inputLength == SQL_NTS ? strlen(newValueAsChar) : inputLength);
}

inline void SetAttributeSQLWCHAR(SQLPOINTER newValue, SQLINTEGER inputLengthInBytes,
                                 std::string& attributeToWrite) {
  thread_local std::vector<uint8_t> utf8_str;
  if (inputLengthInBytes == SQL_NTS) {
    WcsToUtf8(newValue, &utf8_str);
  } else {
    WcsToUtf8(newValue, inputLengthInBytes / GetSqlWCharSize(), &utf8_str);
  }
  attributeToWrite.assign((char*)utf8_str.data());
}

template <typename T>
void CheckIfAttributeIsSetToOnlyValidValue(SQLPOINTER value, T allowed_value) {
  if (static_cast<T>(reinterpret_cast<SQLULEN>(value)) != allowed_value) {
    throw driver::odbcabstraction::DriverException("Optional feature not implemented",
                                                   "HYC00");
  }
}
}  // namespace ODBC
