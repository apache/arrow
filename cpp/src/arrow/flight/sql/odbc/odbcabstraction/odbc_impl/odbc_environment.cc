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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/driver.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

#include <sqlext.h>
#include <algorithm>
#include <utility>

using ODBC::ODBCConnection;
using ODBC::ODBCEnvironment;

using arrow::flight::sql::odbc::Connection;
using arrow::flight::sql::odbc::Diagnostics;
using arrow::flight::sql::odbc::Driver;

// Public
// =========================================================================================
ODBCEnvironment::ODBCEnvironment(std::shared_ptr<Driver> driver)
    : driver_(std::move(driver)),
      diagnostics_(new Diagnostics(driver_->GetDiagnostics().GetVendor(),
                                   driver_->GetDiagnostics().GetDataSourceComponent(),
                                   arrow::flight::sql::odbc::V_2)),
      version_(SQL_OV_ODBC2),
      connection_pooling_(SQL_CP_OFF) {}

Diagnostics& ODBCEnvironment::GetDiagnosticsImpl() { return *diagnostics_; }

SQLINTEGER ODBCEnvironment::GetODBCVersion() const { return version_; }

void ODBCEnvironment::SetODBCVersion(SQLINTEGER version) {
  if (version != version_) {
    version_ = version;
    diagnostics_.reset(
        new Diagnostics(diagnostics_->GetVendor(), diagnostics_->GetDataSourceComponent(),
                        version == SQL_OV_ODBC2 ? arrow::flight::sql::odbc::V_2
                                                : arrow::flight::sql::odbc::V_3));
  }
}

SQLINTEGER ODBCEnvironment::GetConnectionPooling() const { return connection_pooling_; }

void ODBCEnvironment::SetConnectionPooling(SQLINTEGER connection_pooling) {
  connection_pooling_ = connection_pooling;
}

std::shared_ptr<ODBCConnection> ODBCEnvironment::CreateConnection() {
  std::shared_ptr<Connection> spi_connection =
      driver_->CreateConnection(version_ == SQL_OV_ODBC2 ? arrow::flight::sql::odbc::V_2
                                                         : arrow::flight::sql::odbc::V_3);
  std::shared_ptr<ODBCConnection> new_conn =
      std::make_shared<ODBCConnection>(*this, spi_connection);
  connections_.push_back(new_conn);
  return new_conn;
}

void ODBCEnvironment::DropConnection(ODBCConnection* conn) {
  auto it = std::find_if(connections_.begin(), connections_.end(),
                         [&conn](const std::shared_ptr<ODBCConnection>& connection) {
                           return connection.get() == conn;
                         });
  if (connections_.end() != it) {
    connections_.erase(it);
  }
}
