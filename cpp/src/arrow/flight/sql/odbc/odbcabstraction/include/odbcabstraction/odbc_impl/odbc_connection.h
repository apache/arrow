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

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_handle.h>

#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>
#include <sql.h>
#include <map>
#include <memory>
#include <vector>

namespace ODBC {
class ODBCEnvironment;
class ODBCDescriptor;
class ODBCStatement;
}  // namespace ODBC

/**
 * @brief An abstraction over an ODBC connection handle. This also wraps an SPI
 * Connection.
 */
namespace ODBC {
class ODBCConnection : public ODBCHandle<ODBCConnection> {
 public:
  ODBCConnection(const ODBCConnection&) = delete;
  ODBCConnection& operator=(const ODBCConnection&) = delete;

  ODBCConnection(ODBCEnvironment& environment,
                 std::shared_ptr<arrow::flight::sql::odbc::Connection> spi_connection);

  arrow::flight::sql::odbc::Diagnostics& GetDiagnosticsImpl();

  const std::string& GetDSN() const;
  bool IsConnected() const;
  void Connect(std::string dsn,
               const arrow::flight::sql::odbc::Connection::ConnPropertyMap& properties,
               std::vector<std::string_view>& missing_properties);

  void GetInfo(SQLUSMALLINT info_type, SQLPOINTER value, SQLSMALLINT buffer_length,
               SQLSMALLINT* output_length, bool is_unicode);
  void SetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER string_length,
                      bool isUnicode);
  void GetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER buffer_length,
                      SQLINTEGER* output_length, bool is_unicode);

  ~ODBCConnection() = default;

  inline ODBCStatement& GetTrackingStatement() { return *attribute_tracking_statement_; }

  void Disconnect();

  void ReleaseConnection();

  std::shared_ptr<ODBCStatement> CreateStatement();
  void DropStatement(ODBCStatement* statement);

  std::shared_ptr<ODBCDescriptor> CreateDescriptor();
  void DropDescriptor(ODBCDescriptor* descriptor);

  inline bool IsOdbc2Connection() const { return is_2x_connection_; }

  /// @return the DSN or empty string if Driver was used.
  static std::string GetPropertiesFromConnString(
      const std::string& conn_str,
      arrow::flight::sql::odbc::Connection::ConnPropertyMap& properties);

 private:
  ODBCEnvironment& environment_;
  std::shared_ptr<arrow::flight::sql::odbc::Connection> spi_connection_;
  // Extra ODBC statement that's used to track and validate when statement attributes are
  // set through the connection handle. These attributes get copied to new ODBC statements
  // when they are allocated.
  std::shared_ptr<ODBCStatement> attribute_tracking_statement_;
  std::vector<std::shared_ptr<ODBCStatement> > statements_;
  std::vector<std::shared_ptr<ODBCDescriptor> > descriptors_;
  std::string dsn_;
  const bool is_2x_connection_;
  bool is_connected_;
};

}  // namespace ODBC
