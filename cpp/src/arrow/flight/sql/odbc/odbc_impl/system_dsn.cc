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

// platform.h includes windows.h, so it needs to be included
// before winuser.h
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <winuser.h>
#include <utility>
#include "arrow/flight/sql/odbc/odbc_impl/config/configuration.h"
#include "arrow/flight/sql/odbc/odbc_impl/config/connection_string_parser.h"
#include "arrow/flight/sql/odbc/odbc_impl/exceptions.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/ui/dsn_configuration_window.h"
#include "arrow/flight/sql/odbc/odbc_impl/ui/window.h"
#include "arrow/result.h"
#include "arrow/util/utf8.h"

#include <odbcinst.h>
#include <sstream>

using arrow::flight::sql::odbc::DriverException;
using arrow::flight::sql::odbc::FlightSqlConnection;
using arrow::flight::sql::odbc::config::Configuration;
using arrow::flight::sql::odbc::config::ConnectionStringParser;
using arrow::flight::sql::odbc::config::DsnConfigurationWindow;
using arrow::flight::sql::odbc::config::Result;
using arrow::flight::sql::odbc::config::Window;

bool DisplayConnectionWindow(void* window_parent, Configuration& config) {
  HWND hwnd_parent = (HWND)window_parent;

  if (!hwnd_parent) return true;

  try {
    Window parent(hwnd_parent);
    DsnConfigurationWindow window(&parent, config);

    window.Create();

    window.Show();
    window.Update();

    return ProcessMessages(window) == Result::OK;
  } catch (const DriverException& err) {
    std::stringstream buf;
    buf << "SQL State: " << err.GetSqlState() << ", Message: " << err.GetMessageText()
        << ", Code: " << err.GetNativeError();
    std::wstring wmessage =
        arrow::util::UTF8ToWideString(buf.str()).ValueOr(L"Error during load DSN");
    MessageBox(NULL, wmessage.c_str(), L"Error!", MB_ICONEXCLAMATION | MB_OK);

    std::wstring wmessage_text = arrow::util::UTF8ToWideString(err.GetMessageText())
                                     .ValueOr(L"Error during load DSN");
    SQLPostInstallerError(err.GetNativeError(), wmessage_text.c_str());
  }

  return false;
}

void PostError(DWORD error_code, LPCWSTR error_msg) {
  MessageBox(NULL, error_msg, L"Error!", MB_ICONEXCLAMATION | MB_OK);
  SQLPostInstallerError(error_code, error_msg);
}

void PostArrowUtilError(arrow::Status error_status) {
  std::string error_msg = error_status.message();
  std::wstring werror_msg = arrow::util::UTF8ToWideString(error_msg).ValueOr(
      L"Error during utf8 to wide string conversion");

  PostError(ODBC_ERROR_GENERAL_ERR, werror_msg.c_str());
}

void PostLastInstallerError() {
#define BUFFER_SIZE (1024)
  DWORD code;
  wchar_t msg[BUFFER_SIZE];
  SQLInstallerError(1, &code, msg, BUFFER_SIZE, NULL);

  std::wstringstream buf;
  buf << L"Message: \"" << msg << L"\", Code: " << code;
  std::wstring error_msg = buf.str();

  PostError(code, error_msg.c_str());
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::wstring& dsn) {
  if (SQLRemoveDSNFromIni(dsn.c_str())) {
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
  auto wdsn_result = arrow::util::UTF8ToWideString(dsn);
  if (!wdsn_result.status().ok()) {
    PostArrowUtilError(wdsn_result.status());
    return false;
  }
  std::wstring wdsn = wdsn_result.ValueOrDie();

  if (!SQLWriteDSNToIni(wdsn.c_str(), driver)) {
    PostLastInstallerError();
    return false;
  }

  const auto& map = config.GetProperties();
  for (auto it = map.begin(); it != map.end(); ++it) {
    const std::string_view& key = it->first;
    if (boost::iequals(FlightSqlConnection::DSN, key) ||
        boost::iequals(FlightSqlConnection::DRIVER, key)) {
      continue;
    }

    auto wkey_result = arrow::util::UTF8ToWideString(key);
    if (!wkey_result.status().ok()) {
      PostArrowUtilError(wkey_result.status());
      return false;
    }
    std::wstring wkey = wkey_result.ValueOrDie();

    auto wvalue_result = arrow::util::UTF8ToWideString(it->second);
    if (!wvalue_result.status().ok()) {
      PostArrowUtilError(wvalue_result.status());
      return false;
    }
    std::wstring wvalue = wvalue_result.ValueOrDie();

    if (!SQLWritePrivateProfileString(wdsn.c_str(), wkey.c_str(), wvalue.c_str(),
                                      L"ODBC.INI")) {
      PostLastInstallerError();
      return false;
    }
  }

  return true;
}

BOOL INSTAPI ConfigDSNW(HWND hwnd_parent, WORD req, LPCWSTR wdriver,
                        LPCWSTR wattributes) {
  Configuration config;
  ConnectionStringParser parser(config);

  auto attributes_result = arrow::util::WideStringToUTF8(std::wstring(wattributes));
  if (!attributes_result.status().ok()) {
    PostArrowUtilError(attributes_result.status());
    return FALSE;
  }
  std::string attributes = attributes_result.ValueOrDie();

  parser.ParseConfigAttributes(attributes.c_str());

  switch (req) {
    case ODBC_ADD_DSN: {
      config.LoadDefaults();
      if (!DisplayConnectionWindow(hwnd_parent, config) || !RegisterDsn(config, wdriver))
        return FALSE;

      break;
    }

    case ODBC_CONFIG_DSN: {
      const std::string& dsn = config.Get(FlightSqlConnection::DSN);
      auto wdsn_result = arrow::util::UTF8ToWideString(dsn);
      if (!wdsn_result.status().ok()) {
        PostArrowUtilError(wdsn_result.status());
        return FALSE;
      }
      std::wstring wdsn = wdsn_result.ValueOrDie();
      if (!SQLValidDSN(wdsn.c_str())) return FALSE;

      Configuration loaded(config);
      try {
        loaded.LoadDsn(dsn);
      } catch (const DriverException& err) {
        std::string error_msg = err.GetMessageText();
        std::wstring werror_msg =
            arrow::util::UTF8ToWideString(error_msg).ValueOr(L"Error during DSN load");

        PostError(err.GetNativeError(), werror_msg.c_str());
        return FALSE;
      }

      if (!DisplayConnectionWindow(hwnd_parent, loaded) || !UnregisterDsn(wdsn.c_str()) ||
          !RegisterDsn(loaded, wdriver))
        return FALSE;

      break;
    }

    case ODBC_REMOVE_DSN: {
      const std::string& dsn = config.Get(FlightSqlConnection::DSN);
      auto wdsn_result = arrow::util::UTF8ToWideString(dsn);
      if (!wdsn_result.status().ok()) {
        PostArrowUtilError(wdsn_result.status());
        return FALSE;
      }
      std::wstring wdsn = wdsn_result.ValueOrDie();
      if (!SQLValidDSN(wdsn.c_str()) || !UnregisterDsn(wdsn)) return FALSE;

      break;
    }

    default:
      return FALSE;
  }

  return TRUE;
}
