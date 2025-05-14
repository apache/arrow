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
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/exceptions.h"

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
  } catch (driver::odbcabstraction::DriverException& err) {
    std::stringstream buf;
    buf << "Message: " << err.GetMessageText() << ", Code: " << err.GetNativeError();
    std::string message = buf.str();
    MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);

    SQLPostInstallerError(err.GetNativeError(), err.GetMessageText().c_str());
  }

  return false;
}

void PostLastInstallerError() {
#define BUFFER_SIZE (1024)
  DWORD code;
  char msg[BUFFER_SIZE];
  SQLInstallerError(1, &code, msg, BUFFER_SIZE, NULL);

  std::stringstream buf;
  buf << "Message: \"" << msg << "\", Code: " << code;
  std::string errorMsg = buf.str();

  MessageBox(NULL, errorMsg.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);
  SQLPostInstallerError(code, errorMsg.c_str());
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::string& dsn) {
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
bool RegisterDsn(const Configuration& config, LPCSTR driver) {
  const std::string& dsn = config.Get(FlightSqlConnection::DSN);

  if (!SQLWriteDSNToIni(dsn.c_str(), driver)) {
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

    std::string key_str = std::string(key);
    if (!SQLWritePrivateProfileString(dsn.c_str(), key_str.c_str(), it->second.c_str(),
                                      "ODBC.INI")) {
      PostLastInstallerError();
      return false;
    }
  }

  return true;
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
