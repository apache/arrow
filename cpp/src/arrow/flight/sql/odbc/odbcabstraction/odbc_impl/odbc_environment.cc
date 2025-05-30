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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/driver.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h>
#include <sqlext.h>
#include <algorithm>
#include <utility>

using ODBC::ODBCConnection;
using ODBC::ODBCEnvironment;

using driver::odbcabstraction::Connection;
using driver::odbcabstraction::Diagnostics;
using driver::odbcabstraction::Driver;

// Public
// =========================================================================================
ODBCEnvironment::ODBCEnvironment(std::shared_ptr<Driver> driver)
    : m_driver(std::move(driver)),
      m_diagnostics(new Diagnostics(m_driver->GetDiagnostics().GetVendor(),
                                    m_driver->GetDiagnostics().GetDataSourceComponent(),
                                    driver::odbcabstraction::V_2)),
      m_version(SQL_OV_ODBC2),
      m_connectionPooling(SQL_CP_OFF) {}

Diagnostics& ODBCEnvironment::GetDiagnostics_Impl() { return *m_diagnostics; }

SQLINTEGER ODBCEnvironment::getODBCVersion() const { return m_version; }

void ODBCEnvironment::setODBCVersion(SQLINTEGER version) {
  if (version != m_version) {
    m_version = version;
    m_diagnostics.reset(new Diagnostics(
        m_diagnostics->GetVendor(), m_diagnostics->GetDataSourceComponent(),
        version == SQL_OV_ODBC2 ? driver::odbcabstraction::V_2
                                : driver::odbcabstraction::V_3));
  }
}

SQLINTEGER ODBCEnvironment::getConnectionPooling() const { return m_connectionPooling; }

void ODBCEnvironment::setConnectionPooling(SQLINTEGER connectionPooling) {
  m_connectionPooling = connectionPooling;
}

std::shared_ptr<ODBCConnection> ODBCEnvironment::CreateConnection() {
  std::shared_ptr<Connection> spiConnection = m_driver->CreateConnection(
      m_version == SQL_OV_ODBC2 ? driver::odbcabstraction::V_2
                                : driver::odbcabstraction::V_3);
  std::shared_ptr<ODBCConnection> newConn =
      std::make_shared<ODBCConnection>(*this, spiConnection);
  m_connections.push_back(newConn);
  return newConn;
}

void ODBCEnvironment::DropConnection(ODBCConnection* conn) {
  auto it = std::find_if(m_connections.begin(), m_connections.end(),
                         [&conn](const std::shared_ptr<ODBCConnection>& connection) {
                           return connection.get() == conn;
                         });
  if (m_connections.end() != it) {
    m_connections.erase(it);
  }
}
