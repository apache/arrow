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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h>
#include <utility>

namespace driver {
namespace odbcabstraction {

DriverException::DriverException(std::string message, std::string sql_state,
                                 int32_t native_error)
    : msg_text_(std::move(message)),
      sql_state_(std::move(sql_state)),
      native_error_(native_error) {}

const char* DriverException::what() const throw() { return msg_text_.c_str(); }
const std::string& DriverException::GetMessageText() const { return msg_text_; }
const std::string& DriverException::GetSqlState() const { return sql_state_; }
int32_t DriverException::GetNativeError() const { return native_error_; }

AuthenticationException::AuthenticationException(std::string message,
                                                 std::string sql_state,
                                                 int32_t native_error)
    : DriverException(message, sql_state, native_error) {}

CommunicationException::CommunicationException(std::string message, std::string sql_state,
                                               int32_t native_error)
    : DriverException(
          message + ". Please ensure your encryption settings match the server.",
          sql_state, native_error) {}

NullWithoutIndicatorException::NullWithoutIndicatorException(std::string message,
                                                             std::string sql_state,
                                                             int32_t native_error)
    : DriverException(message, sql_state, native_error) {}
}  // namespace odbcabstraction
}  // namespace driver
