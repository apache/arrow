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
                 std::shared_ptr<driver::odbcabstraction::Connection> spiConnection);

  driver::odbcabstraction::Diagnostics& GetDiagnostics_Impl();

  const std::string& GetDSN() const;
  bool isConnected() const;
  void connect(std::string dsn,
               const driver::odbcabstraction::Connection::ConnPropertyMap& properties,
               std::vector<std::string_view>& missing_properties);

  void GetInfo(SQLUSMALLINT infoType, SQLPOINTER value, SQLSMALLINT bufferLength,
               SQLSMALLINT* outputLength, bool isUnicode);
  void SetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER stringLength,
                      bool isUnicode);
  void GetConnectAttr(SQLINTEGER attribute, SQLPOINTER value, SQLINTEGER bufferLength,
                      SQLINTEGER* outputLength, bool isUnicode);

  ~ODBCConnection() = default;

  inline ODBCStatement& GetTrackingStatement() { return *m_attributeTrackingStatement; }

  void disconnect();

  void releaseConnection();

  std::shared_ptr<ODBCStatement> createStatement();
  void dropStatement(ODBCStatement* statement);

  std::shared_ptr<ODBCDescriptor> createDescriptor();
  void dropDescriptor(ODBCDescriptor* descriptor);

  inline bool IsOdbc2Connection() const { return m_is2xConnection; }

  /// @return the DSN or empty string if Driver was used.
  static std::string getPropertiesFromConnString(
      const std::string& connStr,
      driver::odbcabstraction::Connection::ConnPropertyMap& properties);

 private:
  ODBCEnvironment& m_environment;
  std::shared_ptr<driver::odbcabstraction::Connection> m_spiConnection;
  // Extra ODBC statement that's used to track and validate when statement attributes are
  // set through the connection handle. These attributes get copied to new ODBC statements
  // when they are allocated.
  std::shared_ptr<ODBCStatement> m_attributeTrackingStatement;
  std::vector<std::shared_ptr<ODBCStatement> > m_statements;
  std::vector<std::shared_ptr<ODBCDescriptor> > m_descriptors;
  std::string m_dsn;
  const bool m_is2xConnection;
  bool m_isConnected;
};

}  // namespace ODBC
