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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/error_codes.h>
#include <cstdint>
#include <exception>
#include <string>

namespace driver {
namespace odbcabstraction {

/// \brief Base for all driver specific exceptions
class DriverException : public std::exception {
 public:
  explicit DriverException(std::string message, std::string sql_state = "HY000",
                           int32_t native_error = ODBCErrorCodes_GENERAL_ERROR);

  const char* what() const throw() override;

  const std::string& GetMessageText() const;
  const std::string& GetSqlState() const;
  int32_t GetNativeError() const;

 private:
  const std::string msg_text_;
  const std::string sql_state_;
  const int32_t native_error_;
};

/// \brief Authentication specific exception
class AuthenticationException : public DriverException {
 public:
  explicit AuthenticationException(std::string message, std::string sql_state = "28000",
                                   int32_t native_error = ODBCErrorCodes_AUTH);
};

/// \brief Communication link specific exception
class CommunicationException : public DriverException {
 public:
  explicit CommunicationException(std::string message, std::string sql_state = "08S01",
                                  int32_t native_error = ODBCErrorCodes_COMMUNICATION);
};

/// \brief Error when null is retrieved from the database but no indicator was supplied.
/// (This means the driver has no way to report ot the application that there was a NULL
/// value).
class NullWithoutIndicatorException : public DriverException {
 public:
  explicit NullWithoutIndicatorException(
      std::string message = "Indicator variable required but not supplied",
      std::string sql_state = "22002",
      int32_t native_error = ODBCErrorCodes_INDICATOR_NEEDED);
};

}  // namespace odbcabstraction
}  // namespace driver
