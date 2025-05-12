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

#include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"

#include <odbcinst.h>
#include <sstream>

using driver::flight_sql::FlightSqlConnection;
using driver::flight_sql::config::Configuration;

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

    if (!SQLWritePrivateProfileString(dsn.c_str(), key.data(), it->second.c_str(),
                                      "ODBC.INI")) {
      PostLastInstallerError();
      return false;
    }
  }

  return true;
}
