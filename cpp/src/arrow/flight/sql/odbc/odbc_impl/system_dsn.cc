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

#include "arrow/flight/sql/odbc/odbc_impl/system_dsn.h"

#include "arrow/result.h"
#include "arrow/util/utf8.h"

#include <boost/algorithm/string/predicate.hpp>
#include <sstream>

#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"

#ifdef __linux__
// Linux driver manager uses utf16string
#  define CONVERT_UTF8_TO_SQLWCHAR_OR_RETURN(wvar, var)       \
    auto wvar##_result = arrow::util::UTF8StringToUTF16(var); \
    if (!wvar##_result.status().ok()) {                       \
      PostArrowUtilError(wvar##_result.status());             \
      return false;                                           \
    }                                                         \
    std::u16string wvar = wvar##_result.ValueOrDie();

#else
// Windows and macOS
#  define CONVERT_UTF8_TO_SQLWCHAR_OR_RETURN(wvar, var)      \
    auto wvar##_result = arrow::util::UTF8ToWideString(var); \
    if (!wvar##_result.status().ok()) {                      \
      PostArrowUtilError(wvar##_result.status());            \
      return false;                                          \
    }                                                        \
    std::wstring wvar = wvar##_result.ValueOrDie();
#endif  // __linux__

namespace arrow::flight::sql::odbc {

using config::Configuration;

void PostError(DWORD error_code, LPWSTR error_msg) {
#if defined _WIN32
  MessageBox(NULL, error_msg, L"Error!", MB_ICONEXCLAMATION | MB_OK);
#endif  // _WIN32
  SQLPostInstallerError(error_code, error_msg);
}

void PostArrowUtilError(arrow::Status error_status) {
  std::string error_msg = error_status.message();
  CONVERT_SQLWCHAR_STR(werror_msg, error_msg);

  PostError(ODBC_ERROR_GENERAL_ERR,
            const_cast<LPWSTR>(reinterpret_cast<LPCWSTR>(werror_msg.c_str())));
}

void PostLastInstallerError() {
#define BUFFER_SIZE (1024)
  DWORD code;
  std::vector<SQLWCHAR> msg(BUFFER_SIZE);
  SQLInstallerError(1, &code, msg.data(), BUFFER_SIZE, NULL);

#ifdef __linux__
  std::string code_str = std::to_string(code);
  std::u16string code_u16 = arrow::util::UTF8StringToUTF16(code_str).ValueOr(
      u"unknown code. Error during utf8 to utf16 conversion");
  std::u16string error_msg = u"Message: \"" +
                             std::u16string(reinterpret_cast<char16_t*>(msg.data())) +
                             u"\", Code: " + code_u16;
#else
  // Windows/macOS
  std::wstring error_msg =
      L"Message: \"" + std::wstring(msg.data()) + L"\", Code: " + std::to_wstring(code);
#endif  // __linux__

  PostError(code, const_cast<LPWSTR>(reinterpret_cast<LPCWSTR>(error_msg.c_str())));
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::wstring& dsn) {
#ifdef __linux__
  auto dsn_vec = ODBC::ToSqlWCharVector(dsn);
  const SQLWCHAR* dsn_arr = dsn_vec.data();
#else
  // Windows and macOS
  const SQLWCHAR* dsn_arr = dsn.c_str();
#endif
  if (SQLRemoveDSNFromIni(dsn_arr)) {
    return true;
  }

  PostLastInstallerError();
  return false;
}

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const Configuration& config, LPCWSTR driver) {
  const std::string& dsn = config.Get(FlightSqlConnection::DSN);
  CONVERT_UTF8_TO_SQLWCHAR_OR_RETURN(wdsn, dsn);

  if (!SQLWriteDSNToIni(reinterpret_cast<LPCWSTR>(wdsn.c_str()), driver)) {
    PostLastInstallerError();
    return false;
  }

  const auto& map = config.GetProperties();
  for (auto it = map.begin(); it != map.end(); ++it) {
    std::string_view key = it->first;
    if (boost::iequals(FlightSqlConnection::DSN, key) ||
        boost::iequals(FlightSqlConnection::DRIVER, key)) {
      continue;
    }

    CONVERT_UTF8_TO_SQLWCHAR_OR_RETURN(wkey, key);
    CONVERT_UTF8_TO_SQLWCHAR_OR_RETURN(wvalue, it->second);

    if (!SQLWritePrivateProfileString(reinterpret_cast<LPCWSTR>(wdsn.c_str()),
                                      reinterpret_cast<LPCWSTR>(wkey.c_str()),
                                      reinterpret_cast<LPCWSTR>(wvalue.c_str()),
                                      ODBC_INI)) {
      PostLastInstallerError();
      return false;
    }
  }

  return true;
}
}  // namespace arrow::flight::sql::odbc
