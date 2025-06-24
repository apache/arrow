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

#include <memory>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h>

namespace driver {
namespace odbcabstraction {

class Connection;

/// \brief High-level representation of an ODBC driver.
class Driver {
 protected:
  Driver() = default;

 public:
  virtual ~Driver() = default;

  /// \brief Create a connection using given ODBC version.
  /// \param odbc_version ODBC version to be used.
  virtual std::shared_ptr<Connection> CreateConnection(OdbcVersion odbc_version) = 0;

  /// \brief Gets the diagnostics for this connection.
  /// \return the diagnostics
  virtual Diagnostics& GetDiagnostics() = 0;

  /// \brief Sets the driver version.
  virtual void SetVersion(std::string version) = 0;

  /// \brief Register a log to be used by the system.
  virtual void RegisterLog() = 0;
};

}  // namespace odbcabstraction
}  // namespace driver
