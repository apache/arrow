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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/connection_string_parser.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/dsn_configuration_window.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/ui/window.h"
#include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/logger.h"

#include <odbcinst.h>
#include <codecvt>
#include <locale>
#include <sstream>

using driver::flight_sql::FlightSqlConnection;
using driver::flight_sql::config::Configuration;
using driver::flight_sql::config::ConnectionStringParser;
using driver::flight_sql::config::DsnConfigurationWindow;
using driver::flight_sql::config::Result;
using driver::flight_sql::config::Window;
using driver::odbcabstraction::DriverException;

BOOL CALLBACK ConfigDriver(HWND hwndParent, WORD fRequest, LPCSTR lpszDriver,
                           LPCSTR lpszArgs, LPSTR lpszMsg, WORD cbMsgMax,
                           WORD* pcbMsgOut) {
  return false;
}

bool DisplayConnectionWindow(void* windowParent, Configuration& config) {
  HWND hwndParent = (HWND)windowParent;

  if (!hwndParent) return true;

  try {
    Window parent(hwndParent);
    DsnConfigurationWindow window(&parent, config);

    window.Create();

    window.Show();
    window.Update();

    return ProcessMessages(window) == Result::OK;
  } catch (const DriverException& err) {
    std::stringstream buf;
    buf << "SQL State: " << err.GetSqlState() << ", Message: " << err.GetMessageText()
        << ", Code: " << err.GetNativeError();
    std::string message = buf.str();
    MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);

    SQLPostInstallerError(err.GetNativeError(), err.GetMessageText().c_str());
  }

  return false;
}

bool DisplayConnectionWindow(void* windowParent, Configuration& config,
                             Connection::ConnPropertyMap& properties) {
  for (const auto& [key, value] : properties) {
    config.Set(key, value);
  }

  if (DisplayConnectionWindow(windowParent, config)) {
    properties = config.GetProperties();
    return true;
  } else {
    LOG_INFO("Dialog is cancelled by user");
    return false;
  }
}

BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD req, LPCSTR driver, LPCSTR attributes) {
  Configuration config;
  ConnectionStringParser parser(config);
  parser.ParseConfigAttributes(attributes);

  switch (req) {
    case ODBC_ADD_DSN: {
      config.LoadDefaults();
      if (!DisplayConnectionWindow(hwndParent, config) || !RegisterDsn(config, driver))
        return FALSE;

      break;
    }

    case ODBC_CONFIG_DSN: {
      const std::string& dsn = config.Get(FlightSqlConnection::DSN);
      if (!SQLValidDSN(dsn.c_str())) return FALSE;

      Configuration loaded(config);
      loaded.LoadDsn(dsn);

      if (!DisplayConnectionWindow(hwndParent, loaded) || !UnregisterDsn(dsn.c_str()) ||
          !RegisterDsn(loaded, driver))
        return FALSE;

      break;
    }

    case ODBC_REMOVE_DSN: {
      const std::string& dsn = config.Get(FlightSqlConnection::DSN);
      if (!SQLValidDSN(dsn.c_str()) || !UnregisterDsn(dsn)) return FALSE;

      break;
    }

    default:
      return FALSE;
  }

  return TRUE;
}
