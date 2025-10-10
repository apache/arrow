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
#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <type_traits>

#include "arrow/flight/sql/odbc/odbc_impl/odbc_connection.h"

// For DSN registration
#include "arrow/flight/sql/odbc/odbc_impl/system_dsn.h"

#define TEST_CONNECT_STR "ARROW_FLIGHT_SQL_ODBC_CONN"
#define TEST_DSN "Apache Arrow Flight SQL Test DSN"

namespace arrow::flight::sql::odbc {
using arrow::flight::sql::odbc::Connection;

class FlightSQLODBCRemoteTestBase : public ::testing::Test {
 public:
  /// \brief Allocate environment and connection handles
  void AllocEnvConnHandles(SQLINTEGER odbc_ver = SQL_OV_ODBC3);
  /// \brief Connect to Arrow Flight SQL server using connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN", allocate statement handle.
  /// Connects using ODBC Ver 3 by default
  void Connect(SQLINTEGER odbc_ver = SQL_OV_ODBC3);
  /// \brief Connect to Arrow Flight SQL server using connection string
  void ConnectWithString(std::string connection_str);
  /// \brief Disconnect from server
  void Disconnect();
  /// \brief Get connection string from environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual GetConnectionString();
  /// \brief Get invalid connection string based on connection string defined in
  /// environment variable "ARROW_FLIGHT_SQL_ODBC_CONN"
  std::string virtual GetInvalidConnectionString();
  /// \brief Return a SQL query that selects all data types
  std::wstring virtual GetQueryAllDataTypes();

  /** ODBC Environment. */
  SQLHENV env = 0;

  /** ODBC Connect. */
  SQLHDBC conn = 0;

  /** ODBC Statement. */
  SQLHSTMT stmt = 0;

 protected:
  void SetUp() override;
};

static constexpr std::string_view authorization_header = "authorization";
static constexpr std::string_view bearer_prefix = "Bearer ";
static constexpr std::string_view test_token = "t0k3n";

std::string FindTokenInCallHeaders(const CallHeaders& incoming_headers);

// A server middleware for validating incoming bearer header authentication.
class MockServerMiddleware : public ServerMiddleware {
 public:
  explicit MockServerMiddleware(const CallHeaders& incoming_headers, bool* is_valid)
      : is_valid_(is_valid) {
    incoming_headers_ = incoming_headers;
  }

  void SendingHeaders(AddCallHeaders* outgoing_headers) override;

  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "MockServerMiddleware"; }

 private:
  CallHeaders incoming_headers_;
  bool* is_valid_;
};

// Factory for base64 header authentication testing.
class MockServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  MockServerMiddlewareFactory() : is_valid_(false) {}

  Status StartCall(const CallInfo& info, const ServerCallContext& context,
                   std::shared_ptr<ServerMiddleware>* middleware) override;

 private:
  bool is_valid_;
};

class FlightSQLODBCMockTestBase : public FlightSQLODBCRemoteTestBase {
  // Sets up a mock server for each test case
 public:
  /// \brief Get connection string for mock server
  std::string GetConnectionString() override;
  /// \brief Get invalid connection string for mock server
  std::string GetInvalidConnectionString() override;
  /// \brief Return a SQL query that selects all data types
  std::wstring GetQueryAllDataTypes() override;

  /// \brief Run a SQL query to create default table for table test cases
  void CreateTestTables();

  /// \brief run a SQL query to create a table with all data types
  void CreateTableAllDataType();
  /// \brief run a SQL query to create a table with unicode name
  void CreateUnicodeTable();

  int port;

 protected:
  void SetUp() override;

  void TearDown() override;

 private:
  std::shared_ptr<arrow::flight::sql::example::SQLiteFlightSqlServer> server_;
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
bool CompareConnPropertyMap(Connection::ConnPropertyMap map1,
                            Connection::ConnPropertyMap map2);

/// Get error message from ODBC driver using SQLGetDiagRec
std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle);

static constexpr std::string_view error_state_01004 = "01004";
static constexpr std::string_view error_state_01S07 = "01S07";
static constexpr std::string_view error_state_01S02 = "01S02";
static constexpr std::string_view error_state_07009 = "07009";
static constexpr std::string_view error_state_08003 = "08003";
static constexpr std::string_view error_state_22002 = "22002";
static constexpr std::string_view error_state_24000 = "24000";
static constexpr std::string_view error_state_28000 = "28000";
static constexpr std::string_view error_state_HY000 = "HY000";
static constexpr std::string_view error_state_HY004 = "HY004";
static constexpr std::string_view error_state_HY009 = "HY009";
static constexpr std::string_view error_state_HY010 = "HY010";
static constexpr std::string_view error_state_HY017 = "HY017";
static constexpr std::string_view error_state_HY024 = "HY024";
static constexpr std::string_view error_state_HY090 = "HY090";
static constexpr std::string_view error_state_HY091 = "HY091";
static constexpr std::string_view error_state_HY092 = "HY092";
static constexpr std::string_view error_state_HY106 = "HY106";
static constexpr std::string_view error_state_HY114 = "HY114";
static constexpr std::string_view error_state_HY118 = "HY118";
static constexpr std::string_view error_state_HYC00 = "HYC00";
static constexpr std::string_view error_state_S1004 = "S1004";

/// Verify ODBC Error State
void VerifyOdbcErrorState(SQLSMALLINT handle_type, SQLHANDLE handle,
                          std::string_view expected_state);

/// \brief Write connection string into DSN
/// \param[in] connection_str the connection string.
/// \return true on success
bool WriteDSN(std::string connection_str);

/// \brief Write properties map into DSN
/// \param[in] properties map.
/// \return true on success
bool WriteDSN(Connection::ConnPropertyMap properties);

/// \brief Check wide char vector and convert into wstring
/// \param[in] str_val Vector of SQLWCHAR.
/// \param[in] str_len length of string, in bytes.
/// \return wstring
std::wstring ConvertToWString(const std::vector<SQLWCHAR>& str_val, SQLSMALLINT str_len);

/// \brief Check wide string column.
/// \param[in] stmt Statement.
/// \param[in] col_id Column ID to check.
/// \param[in] expected Expected value.
void CheckStringColumnW(SQLHSTMT stmt, int col_id, const std::wstring& expected);

/// \brief Check wide string column value is null.
/// \param[in] stmt Statement.
/// \param[in] col_id Column ID to check.
void CheckNullColumnW(SQLHSTMT stmt, int col_id);

/// \brief Check int column.
/// \param[in] stmt Statement.
/// \param[in] col_id Column ID to check.
/// \param[in] expected Expected value.
void CheckIntColumn(SQLHSTMT stmt, int col_id, const SQLINTEGER& expected);

/// \brief Check smallint column.
/// \param[in] stmt Statement.
/// \param[in] col_id Column ID to check.
/// \param[in] expected Expected value.
void CheckSmallIntColumn(SQLHSTMT stmt, int col_id, const SQLSMALLINT& expected);

/// \brief Check sql return against expected.
/// \param[in] stmt Statement.
/// \param[in] expected Expected return.
void ValidateFetch(SQLHSTMT stmt, SQLRETURN expected);
}  // namespace arrow::flight::sql::odbc
