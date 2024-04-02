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
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#include <winuser.h>
#include <utility>

#include "arrow/result.h"
#include "arrow/util/utf8.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/connection_string_parser.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/dsn_configuration_window.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/window.h"
#include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/util/logging.h"

#include <odbcinst.h>
#include <codecvt>
#include <locale>
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
    std::wstring wmessage = arrow::util::UTF8ToWideString(buf.str()).ValueOr(L"");
    MessageBox(NULL, wmessage.c_str(), L"Error!", MB_ICONEXCLAMATION | MB_OK);

    std::wstring wmessage_text =
        arrow::util::UTF8ToWideString(err.GetMessageText()).ValueOr(L"");
    SQLPostInstallerError(err.GetNativeError(), wmessage_text.c_str());
  }

  return false;
}

bool DisplayConnectionWindow(void* window_parent, Configuration& config,
                             Connection::ConnPropertyMap& properties) {
  for (const auto& [key, value] : properties) {
    config.Set(key, value);
  }

  if (DisplayConnectionWindow(window_parent, config)) {
    properties = config.GetProperties();
    return true;
  } else {
    ARROW_LOG(INFO) << "Dialog is cancelled by user";
    return false;
  }
}

BOOL INSTAPI ConfigDSNW(HWND hwnd_parent, WORD req, LPCWSTR wdriver,
                        LPCWSTR wattributes) {
  Configuration config;
  ConnectionStringParser parser(config);
  std::string attributes =
      arrow::util::WideStringToUTF8(std::wstring(wattributes)).ValueOr("");
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
      std::wstring wdsn = arrow::util::UTF8ToWideString(dsn).ValueOr(L"");
      if (!SQLValidDSN(wdsn.c_str())) return FALSE;

      Configuration loaded(config);
      loaded.LoadDsn(dsn);

      if (!DisplayConnectionWindow(hwnd_parent, loaded) || !UnregisterDsn(wdsn.c_str()) ||
          !RegisterDsn(loaded, wdriver))
        return FALSE;

      break;
    }

    case ODBC_REMOVE_DSN: {
      const std::string& dsn = config.Get(FlightSqlConnection::DSN);
      std::wstring wdsn = arrow::util::UTF8ToWideString(dsn).ValueOr(L"");
      if (!SQLValidDSN(wdsn.c_str()) || !UnregisterDsn(wdsn)) return FALSE;

      break;
    }

    default:
      return FALSE;
  }

  return TRUE;
}
