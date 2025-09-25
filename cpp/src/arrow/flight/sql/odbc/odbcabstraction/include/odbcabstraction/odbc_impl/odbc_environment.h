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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_handle.h"

#include <sql.h>
#include <memory>
#include <vector>

namespace driver {
namespace odbcabstraction {
class Driver;
}
}  // namespace driver

namespace ODBC {
class ODBCConnection;
}

/**
 * @brief An abstraction over an ODBC environment handle.
 */
namespace ODBC {
class ODBCEnvironment : public ODBCHandle<ODBCEnvironment> {
 public:
  explicit ODBCEnvironment(std::shared_ptr<driver::odbcabstraction::Driver> driver);
  driver::odbcabstraction::Diagnostics& GetDiagnosticsImpl();
  SQLINTEGER GetODBCVersion() const;
  void SetODBCVersion(SQLINTEGER version);
  SQLINTEGER GetConnectionPooling() const;
  void SetConnectionPooling(SQLINTEGER pooling);
  std::shared_ptr<ODBCConnection> CreateConnection();
  void DropConnection(ODBCConnection* conn);
  ~ODBCEnvironment() = default;

 private:
  std::vector<std::shared_ptr<ODBCConnection> > connections_;
  std::shared_ptr<driver::odbcabstraction::Driver> driver_;
  std::unique_ptr<driver::odbcabstraction::Diagnostics> diagnostics_;
  SQLINTEGER version_;
  SQLINTEGER connection_pooling_;
};

}  // namespace ODBC
