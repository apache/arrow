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

#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/utf8.h"

#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <type_traits>

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"

// For DSN registration
#include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"

#define TEST_CONNECT_STR "ARROW_FLIGHT_SQL_ODBC_CONN"
#define TEST_DSN "Apache Arrow Flight SQL Test DSN"

namespace arrow::flight::sql::odbc {
using driver::odbcabstraction::Connection;

class FlightSQLODBCRemoteTestBase : public ::testing::Test {
 public:
  /// \brief Allocate environment and connection handles
  void allocEnvConnHandles();
  /// \brief Connect to Arrow Flight SQL server using connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN", allocate statement handle
  void connect();
  /// \brief Connect to Arrow Flight SQL server using connection string
  void connectWithString(std::string connection_str);
  /// \brief Disconnect from server
  void disconnect();
  /// \brief Get connection string from environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual getConnectionString();
  /// \brief Get invalid connection string based on connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual getInvalidConnectionString();
  /// \brief Return a SQL query that selects all data types
  std::wstring virtual getQueryAllDataTypes();

  /** ODBC Environment. */
  SQLHENV env;

  /** ODBC Connect. */
  SQLHDBC conn;

  /** ODBC Statement. */
  SQLHSTMT stmt;

 protected:
  void SetUp() override;
};

static constexpr std::string_view kAuthHeader = "authorization";
static constexpr std::string_view kBearerPrefix = "Bearer ";
static constexpr std::string_view test_token = "t0k3n";

std::string FindTokenInCallHeaders(const CallHeaders& incoming_headers);

// A server middleware for validating incoming bearer header authentication.
class MockServerMiddleware : public ServerMiddleware {
 public:
  explicit MockServerMiddleware(const CallHeaders& incoming_headers, bool* isValid)
      : isValid_(isValid) {
    incoming_headers_ = incoming_headers;
  }

  void SendingHeaders(AddCallHeaders* outgoing_headers) override;

  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "MockServerMiddleware"; }

 private:
  CallHeaders incoming_headers_;
  bool* isValid_;
};

// Factory for base64 header authentication testing.
class MockServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  MockServerMiddlewareFactory() : isValid_(false) {}

  Status StartCall(const CallInfo& info, const ServerCallContext& context,
                   std::shared_ptr<ServerMiddleware>* middleware) override;

 private:
  bool isValid_;
};

class FlightSQLODBCMockTestBase : public FlightSQLODBCRemoteTestBase {
  // Sets up a mock server for each test case
 public:
  /// \brief Get connection string for mock server
  std::string getConnectionString() override;
  /// \brief Get invalid connection string for mock server
  std::string getInvalidConnectionString() override;
  /// \brief Return a SQL query that selects all data types
  std::wstring getQueryAllDataTypes() override;

  int port;

 protected:
  void SetUp() override;

  void TearDown() override;

 private:
  std::shared_ptr<arrow::flight::sql::example::SQLiteFlightSqlServer> server;
};

template <typename T>
class FlightSQLODBCTestBase : public T {
 public:
  using List = std::list<T>;
};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(FlightSQLODBCTestBase, TestTypes);

/** ODBC read buffer size. */
enum { ODBC_BUFFER_SIZE = 1024 };

/// Compare ConnPropertyMap, key value is case-insensitive
bool compareConnPropertyMap(Connection::ConnPropertyMap map1,
                            Connection::ConnPropertyMap map2);

/// Get error message from ODBC driver using SQLGetDiagRec
std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle);

static constexpr std::string_view error_state_01004 = "01004";
static constexpr std::string_view error_state_01S07 = "01S07";
static constexpr std::string_view error_state_01S02 = "01S02";
static constexpr std::string_view error_state_08003 = "08003";
static constexpr std::string_view error_state_22002 = "22002";
static constexpr std::string_view error_state_24000 = "24000";
static constexpr std::string_view error_state_28000 = "28000";
static constexpr std::string_view error_state_HY000 = "HY000";
static constexpr std::string_view error_state_HY024 = "HY024";
static constexpr std::string_view error_state_HY092 = "HY092";
static constexpr std::string_view error_state_HYC00 = "HYC00";
static constexpr std::string_view error_state_HY114 = "HY114";
static constexpr std::string_view error_state_HY118 = "HY118";

/// Verify ODBC Error State
void VerifyOdbcErrorState(SQLSMALLINT handle_type, SQLHANDLE handle,
                          std::string_view expected_state);

/// \brief Write connection string into DSN
/// \param[in] connection_str the connection string.
/// \return true on success
bool writeDSN(std::string connection_str);

/// \brief Write properties map into DSN
/// \param[in] properties map.
/// \return true on success
bool writeDSN(Connection::ConnPropertyMap properties);
}  // namespace arrow::flight::sql::odbc
