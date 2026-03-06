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

// For DSN registration. flight_sql_connection.h needs to included first due to conflicts
// with windows.h
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"

#include "arrow/flight/sql/odbc/tests/odbc_test_suite.h"

// For DSN registration
#include "arrow/flight/sql/odbc/odbc_impl/config/configuration.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_connection.h"

namespace arrow::flight::sql::odbc {

class MockServerEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("0.0.0.0", 0));
    arrow::flight::FlightServerOptions options(location);
    options.auth_handler = std::make_unique<NoOpAuthHandler>();
    options.middleware.push_back(
        {"bearer-auth-server", std::make_shared<MockServerMiddlewareFactory>()});
    ASSERT_OK_AND_ASSIGN(mock_server,
                         arrow::flight::sql::example::SQLiteFlightSqlServer::Create());
    ASSERT_OK(mock_server->Init(options));

    mock_server_port = mock_server->port();
    ASSERT_OK_AND_ASSIGN(location, Location::ForGrpcTcp("localhost", mock_server_port));
    ASSERT_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));
  }

  void TearDown() override {
    ASSERT_OK(mock_server->Shutdown());
    ASSERT_OK(mock_server->Wait());
  }
};

::testing::Environment* mock_env =
    ::testing::AddGlobalTestEnvironment(new MockServerEnvironment);

void ODBCTestBase::AllocEnvConnHandles(SQLINTEGER odbc_ver) {
  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(
      SQL_SUCCESS,
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                    reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(odbc_ver)), 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));
}

void ODBCTestBase::Connect(std::string connect_str, SQLINTEGER odbc_ver) {
  ASSERT_NO_FATAL_FAILURE(AllocEnvConnHandles(odbc_ver));
  ASSERT_NO_FATAL_FAILURE(ConnectWithString(connect_str));
}

void ODBCTestBase::ConnectWithString(std::string connect_str) {
  // Connect string
  std::vector<SQLWCHAR> connect_str0(connect_str.begin(), connect_str.end());

  SQLWCHAR out_str[kOdbcBufferSize];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_SUCCESS,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             kOdbcBufferSize, &out_str_len, SQL_DRIVER_NOPROMPT))
      << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn);
}

void ODBCTestBase::Disconnect() {
  // Disconnect from ODBC
  EXPECT_EQ(SQL_SUCCESS, SQLDisconnect(conn))
      << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn);

  FreeEnvConnHandles();
}

void ODBCTestBase::FreeEnvConnHandles() {
  // Free connection handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

std::string ODBCTestBase::GetConnectionString() {
  std::string connect_str =
      arrow::internal::GetEnvVar(kTestConnectStr.data()).ValueOrDie();
  return connect_str;
}

std::string ODBCTestBase::GetInvalidConnectionString() {
  std::string connect_str = GetConnectionString();
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");
  return connect_str;
}

std::wstring ODBCTestBase::GetQueryAllDataTypes() {
  std::wstring wsql =
      LR"( SELECT
           -- Numeric types
          -128 as stiny_int_min, 127 as stiny_int_max,
          0 as utiny_int_min, 255 as utiny_int_max,

          -32768 as ssmall_int_min, 32767 as ssmall_int_max,
          0 as usmall_int_min, 65535 as usmall_int_max,

          CAST(-2147483648 AS INTEGER) AS sinteger_min,
          CAST(2147483647 AS INTEGER) AS sinteger_max,
          CAST(0 AS BIGINT) AS uinteger_min,
          CAST(4294967295 AS BIGINT) AS uinteger_max,

          CAST(-9223372036854775808 AS BIGINT) AS sbigint_min,
          CAST(9223372036854775807 AS BIGINT) AS sbigint_max,
          CAST(0 AS BIGINT) AS ubigint_min,
          --Use string to represent unsigned big int due to lack of support from
          --remote test server
          '18446744073709551615' AS ubigint_max,

          CAST(-999999999 AS DECIMAL(38, 0)) AS decimal_negative,
          CAST(999999999 AS DECIMAL(38, 0)) AS decimal_positive,

          CAST(-3.40282347E38 AS FLOAT) AS float_min, CAST(3.40282347E38 AS FLOAT) AS float_max,

          CAST(-1.7976931348623157E308 AS DOUBLE) AS double_min,
          CAST(1.7976931348623157E308 AS DOUBLE) AS double_max,

          --Boolean
          CAST(false AS BOOLEAN) AS bit_false,
          CAST(true AS BOOLEAN) AS bit_true,

          --Character types
          'Z' AS c_char, _utf8'你' AS c_wchar,

          _utf8'你好' AS c_wvarchar,

          'XYZ' AS c_varchar,

          --Date / timestamp
          CAST(DATE '1400-01-01' AS DATE) AS date_min,
          CAST(DATE '9999-12-31' AS DATE) AS date_max,

          CAST(TIMESTAMP '1400-01-01 00:00:00' AS TIMESTAMP) AS timestamp_min,
          CAST(TIMESTAMP '9999-12-31 23:59:59' AS TIMESTAMP) AS timestamp_max;
      )";
  return wsql;
}

void ODBCTestBase::SetUp() {
  if (connected) {
    ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt));
  }
}

void ODBCTestBase::TearDown() {
  if (connected) {
    ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_STMT, stmt));
  }
}

void ODBCTestBase::TearDownTestSuite() {
  if (connected) {
    Disconnect();
    connected = false;
  }
}

void FlightSQLODBCRemoteTestBase::CheckForRemoteTest() {
  if (arrow::internal::GetEnvVar(kTestConnectStr.data()).ValueOr("").empty()) {
    skipping_test = true;
    GTEST_SKIP() << "Skipping test: kTestConnectStr not set";
  }
}

void FlightSQLODBCRemoteTestBase::SetUpTestSuite() {
  CheckForRemoteTest();
  if (skipping_test) {
    return;
  }

  std::string connect_str = GetConnectionString();
  Connect(connect_str, SQL_OV_ODBC3);
  connected = true;
}

void FlightSQLOdbcV2RemoteTestBase::SetUpTestSuite() {
  CheckForRemoteTest();
  if (skipping_test) {
    return;
  }

  std::string connect_str = GetConnectionString();
  Connect(connect_str, SQL_OV_ODBC2);
  connected = true;
}

void FlightSQLOdbcEnvConnHandleRemoteTestBase::SetUpTestSuite() {
  CheckForRemoteTest();
  if (skipping_test) {
    return;
  }

  AllocEnvConnHandles();
}

void FlightSQLOdbcEnvConnHandleRemoteTestBase::TearDownTestSuite() {
  if (skipping_test) {
    return;
  }

  FreeEnvConnHandles();
}

std::string FindTokenInCallHeaders(const CallHeaders& incoming_headers) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char& char1, const char& char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  std::string bearer_token("");
  auto auth_header = incoming_headers.find(kAuthorizationHeader);
  if (auth_header != incoming_headers.end()) {
    const std::string auth_val(auth_header->second);
    if (auth_val.size() > kBearerPrefix.length()) {
      if (std::equal(auth_val.begin(), auth_val.begin() + kBearerPrefix.length(),
                     kBearerPrefix.begin(), char_compare)) {
        bearer_token = auth_val.substr(kBearerPrefix.length());
      }
    }
  }
  return bearer_token;
}

void MockServerMiddleware::SendingHeaders(AddCallHeaders* outgoing_headers) {
  std::string bearer_token = FindTokenInCallHeaders(incoming_headers_);
  *is_valid_ = (bearer_token == std::string(kTestToken));
}

Status MockServerMiddlewareFactory::StartCall(
    const CallInfo& info, const ServerCallContext& context,
    std::shared_ptr<ServerMiddleware>* middleware) {
  std::string bearer_token = FindTokenInCallHeaders(context.incoming_headers());
  if (bearer_token == std::string(kTestToken)) {
    *middleware =
        std::make_shared<MockServerMiddleware>(context.incoming_headers(), &is_valid_);
  } else {
    return MakeFlightError(FlightStatusCode::Unauthenticated,
                           "Invalid token for mock server");
  }

  return Status::OK();
}

std::string ODBCMockTestBase::GetConnectionString() {
  std::string connect_str(
      "driver={Apache Arrow Flight SQL ODBC Driver};HOST=localhost;port=" +
      std::to_string(mock_server_port) + ";token=" + std::string(kTestToken) +
      ";useEncryption=false;UseWideChar=true;");
  return connect_str;
}

std::string ODBCMockTestBase::GetInvalidConnectionString() {
  std::string connect_str = GetConnectionString();
  // Append invalid token to connection string
  connect_str += std::string("token=invalid_token;");
  return connect_str;
}

std::wstring ODBCMockTestBase::GetQueryAllDataTypes() {
  std::wstring wsql =
      LR"( SELECT
      -- Numeric types
      -128 AS stiny_int_min, 127 AS stiny_int_max,
      0 AS utiny_int_min, 255 AS utiny_int_max,

      -32768 AS ssmall_int_min, 32767 AS ssmall_int_max,
      0 AS usmall_int_min, 65535 AS usmall_int_max,

      CAST(-2147483648 AS INTEGER) AS sinteger_min,
      CAST(2147483647 AS INTEGER) AS sinteger_max,
      CAST(0 AS INTEGER) AS uinteger_min,
      CAST(4294967295 AS INTEGER) AS uinteger_max,

      CAST(-9223372036854775808 AS INTEGER) AS sbigint_min,
      CAST(9223372036854775807 AS INTEGER) AS sbigint_max,
      CAST(0 AS INTEGER) AS ubigint_min,
      -- stored as TEXT as SQLite doesn't support unsigned big int
      '18446744073709551615' AS ubigint_max,

      CAST('-999999999' AS NUMERIC) AS decimal_negative,
      CAST('999999999' AS NUMERIC) AS decimal_positive,

      CAST(-3.40282347E38 AS REAL) AS float_min,
      CAST(3.40282347E38 AS REAL) AS float_max,

      CAST(-1.7976931348623157E308 AS REAL) AS double_min,
      CAST(1.7976931348623157E308 AS REAL) AS double_max,

      -- Boolean
      0 AS bit_false,
      1 AS bit_true,

      -- Character types
      'Z' AS c_char,
      '你' AS c_wchar,
      '你好' AS c_wvarchar,
      'XYZ' AS c_varchar,

      DATE('1400-01-01') AS date_min,
      DATE('9999-12-31') AS date_max,

      DATETIME('1400-01-01 00:00:00') AS timestamp_min,
      DATETIME('9999-12-31 23:59:59') AS timestamp_max;
      )";
  return wsql;
}

void ODBCMockTestBase::CreateTestTable() {
  ASSERT_OK(mock_server->ExecuteSql(R"(
    CREATE TABLE TestTable (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyName varchar(100),
    value int);

    INSERT INTO TestTable (keyName, value) VALUES ('One', 1);
    INSERT INTO TestTable (keyName, value) VALUES ('Two', 0);
    INSERT INTO TestTable (keyName, value) VALUES ('Three', -1);
  )"));
}

void ODBCMockTestBase::DropTestTable() {
  ASSERT_OK(mock_server->ExecuteSql("DROP TABLE TestTable;"));
}

void ODBCMockTestBase::CreateAllDataTypeTable() {
  // Limitation on mock SQLite server:
  // Only int64, float64, binary, and utf8 Arrow Types are supported by
  // SQLiteFlightSqlServer::Impl::DoGetTables
  ASSERT_OK(mock_server->ExecuteSql(R"(
    CREATE TABLE AllTypesTable(
    bigint_col INTEGER PRIMARY KEY AUTOINCREMENT,
    char_col varchar(100),
    varbinary_col BLOB,
    double_col REAL);

    INSERT INTO AllTypesTable (
    char_col,
    varbinary_col,
    double_col) VALUES (
        '1st Row',
        X'31737420726F77',
        3.14159
    );
  )"));
}

void ODBCMockTestBase::DropAllDataTypeTable() {
  ASSERT_OK(mock_server->ExecuteSql("DROP TABLE AllTypesTable;"));
}

void ODBCMockTestBase::CreateUnicodeTable() {
  std::string unicode_sql = arrow::util::WideStringToUTF8(
                                LR"(
    CREATE TABLE 数据(
    资料 varchar(100));

    INSERT INTO 数据 (资料) VALUES ('第一行');
    INSERT INTO 数据 (资料) VALUES ('二行');
    INSERT INTO 数据 (资料) VALUES ('3rd Row');
  )")
                                .ValueOr("");
  ASSERT_OK(mock_server->ExecuteSql(unicode_sql));
}

void ODBCMockTestBase::DropUnicodeTable() {
  std::string unicode_sql =
      arrow::util::WideStringToUTF8(L"DROP TABLE 数据;").ValueOr("");
  ASSERT_OK(mock_server->ExecuteSql(unicode_sql));
}

void FlightSQLODBCMockTestBase::SetUpTestSuite() {
  std::string connect_str = GetConnectionString();
  Connect(connect_str, SQL_OV_ODBC3);
  connected = true;
}

void FlightSQLOdbcV2MockTestBase::SetUpTestSuite() {
  std::string connect_str = GetConnectionString();
  Connect(connect_str, SQL_OV_ODBC2);
  connected = true;
}

void FlightSQLOdbcEnvConnHandleMockTestBase::SetUpTestSuite() { AllocEnvConnHandles(); }

void FlightSQLOdbcEnvConnHandleMockTestBase::TearDownTestSuite() { FreeEnvConnHandles(); }

bool CompareConnPropertyMap(Connection::ConnPropertyMap map1,
                            Connection::ConnPropertyMap map2) {
  if (map1.size() != map2.size()) return false;

  for (const auto& [key, value] : map1) {
    if (value != map2[key]) return false;
  }

  return true;
}

void VerifyOdbcErrorState(SQLSMALLINT handle_type, SQLHANDLE handle,
                          std::string_view expected_state) {
  using ODBC::SqlWcharToString;

  SQLWCHAR sql_state[7] = {};
  SQLINTEGER native_code;

  SQLWCHAR message[kOdbcBufferSize] = {};
  SQLSMALLINT real_len = 0;

  // On Windows, real_len is in bytes. On Linux, real_len is in chars.
  // So, not using real_len
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message, kOdbcBufferSize,
                &real_len);

  EXPECT_EQ(expected_state, SqlWcharToString(sql_state));
}

std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
  using ODBC::SqlWcharToString;

  SQLWCHAR sql_state[7] = {};
  SQLINTEGER native_code;

  SQLWCHAR message[kOdbcBufferSize] = {};
  SQLSMALLINT real_len = 0;

  // On Windows, real_len is in bytes. On Linux, real_len is in chars.
  // So, not using real_len
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message, kOdbcBufferSize,
                &real_len);

  std::string res = SqlWcharToString(sql_state);

  if (res.empty() || !message[0]) {
    res = "Cannot find ODBC error message";
  } else {
    res.append(": ").append(SqlWcharToString(message));
  }

  return res;
}

bool WriteDSN(std::string connection_str) {
  Connection::ConnPropertyMap properties;

  ODBC::ODBCConnection::GetPropertiesFromConnString(connection_str, properties);
  return WriteDSN(properties);
}

bool WriteDSN(Connection::ConnPropertyMap properties) {
  using arrow::flight::sql::odbc::Connection;
  using arrow::flight::sql::odbc::FlightSqlConnection;
  using arrow::flight::sql::odbc::config::Configuration;
  using ODBC::ODBCConnection;

  Configuration config;
  config.Set(FlightSqlConnection::DSN, std::string(kTestDsn));

  for (const auto& [key, value] : properties) {
    config.Set(key, value);
  }

  std::string driver = config.Get(FlightSqlConnection::DRIVER);
  std::wstring w_driver = arrow::util::UTF8ToWideString(driver).ValueOr(L"");
  return RegisterDsn(config, w_driver.c_str());
}

std::wstring GetStringColumnW(SQLHSTMT stmt, int col_id) {
  SQLWCHAR buf[1024];
  SQLLEN len_indicator = 0;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetData(stmt, col_id, SQL_C_WCHAR, buf, sizeof(buf), &len_indicator));

  if (len_indicator == SQL_NULL_DATA) {
    return L"";
  }

  // indicator is in bytes, so convert to character count
  size_t char_count = static_cast<size_t>(len_indicator) / GetSqlWCharSize();
  return std::wstring(buf, buf + char_count);
}

std::wstring ConvertToWString(const std::vector<SQLWCHAR>& str_val, SQLSMALLINT str_len) {
  std::wstring attr_str;
  if (str_len == 0) {
    attr_str = std::wstring(&str_val[0]);
  } else {
    EXPECT_GT(str_len, 0);
    EXPECT_LE(str_len, static_cast<SQLSMALLINT>(kOdbcBufferSize));
    attr_str =
        std::wstring(str_val.begin(), str_val.begin() + str_len / GetSqlWCharSize());
  }
  return attr_str;
}

void CheckStringColumnW(SQLHSTMT stmt, int col_id, const std::wstring& expected) {
  SQLWCHAR buf[1024];
  SQLLEN buf_len = sizeof(buf) * GetSqlWCharSize();

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(stmt, col_id, SQL_C_WCHAR, buf, buf_len, &buf_len));

  EXPECT_GT(buf_len, 0);

  // returned buf_len is in bytes so convert to length in characters
  size_t char_count = static_cast<size_t>(buf_len) / GetSqlWCharSize();
  std::wstring returned(buf, buf + char_count);

  EXPECT_EQ(expected, returned);
}

void CheckNullColumnW(SQLHSTMT stmt, int col_id) {
  SQLWCHAR buf[1024];
  SQLLEN buf_len = sizeof(buf);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(stmt, col_id, SQL_C_WCHAR, buf, buf_len, &buf_len));

  EXPECT_EQ(SQL_NULL_DATA, buf_len);
}

void CheckIntColumn(SQLHSTMT stmt, int col_id, const SQLINTEGER& expected) {
  SQLINTEGER buf;
  SQLLEN buf_len = sizeof(buf);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(stmt, col_id, SQL_C_LONG, &buf, sizeof(buf), &buf_len));

  EXPECT_EQ(expected, buf);
}

void CheckSmallIntColumn(SQLHSTMT stmt, int col_id, const SQLSMALLINT& expected) {
  SQLSMALLINT buf;
  SQLLEN buf_len = sizeof(buf);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(stmt, col_id, SQL_C_SSHORT, &buf, sizeof(buf), &buf_len));

  EXPECT_EQ(expected, buf);
}

void ValidateFetch(SQLHSTMT stmt, SQLRETURN expected_return) {
  ASSERT_EQ(expected_return, SQLFetch(stmt));
}

}  // namespace arrow::flight::sql::odbc
