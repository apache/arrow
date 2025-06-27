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
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include "arrow/flight/sql/odbc/tests/odbc_test_suite.h"

// For DSN registration
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"

namespace arrow::flight::sql::odbc {

void FlightSQLODBCRemoteTestBase::allocEnvConnHandles() {
  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

void FlightSQLODBCRemoteTestBase::connect() {
  allocEnvConnHandles();
  std::string connect_str = getConnectionString();
  connectWithString(connect_str);
}

void FlightSQLODBCRemoteTestBase::connectWithString(std::string connect_str) {
  // Connect string
  std::vector<SQLWCHAR> connect_str0(connect_str.begin(), connect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  SQLRETURN ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                                   static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                                   ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  // Assert connection is successful before we continue
  ASSERT_TRUE(ret == SQL_SUCCESS);

  // Allocate a statement using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt);

  ASSERT_TRUE(ret == SQL_SUCCESS);
}

void FlightSQLODBCRemoteTestBase::disconnect() {
  // Close statement
  SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

std::string FlightSQLODBCRemoteTestBase::getConnectionString() {
  std::string connect_str = arrow::internal::GetEnvVar(TEST_CONNECT_STR).ValueOrDie();
  return connect_str;
}

std::string FlightSQLODBCRemoteTestBase::getInvalidConnectionString() {
  std::string connect_str = getConnectionString();
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");
  return connect_str;
}

void FlightSQLODBCRemoteTestBase::SetUp() {
  if (arrow::internal::GetEnvVar(TEST_CONNECT_STR).ValueOr("").empty()) {
    GTEST_SKIP() << "Skipping FlightSQLODBCRemoteTestBase test: TEST_CONNECT_STR not set";
  }
}

std::string FindTokenInCallHeaders(const CallHeaders& incoming_headers) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char& char1, const char& char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  std::string bearer_token("");
  auto authHeader = incoming_headers.find(kAuthHeader);
  if (authHeader != incoming_headers.end()) {
    const std::string auth_val(authHeader->second);
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
  *isValid_ = (bearer_token == std::string(test_token));
}

Status MockServerMiddlewareFactory::StartCall(
    const CallInfo& info, const ServerCallContext& context,
    std::shared_ptr<ServerMiddleware>* middleware) {
  std::string bearer_token = FindTokenInCallHeaders(context.incoming_headers());
  if (bearer_token == std::string(test_token)) {
    *middleware =
        std::make_shared<MockServerMiddleware>(context.incoming_headers(), &isValid_);
  } else {
    return MakeFlightError(FlightStatusCode::Unauthenticated,
                           "Invalid token for mock server");
  }

  return Status::OK();
}

std::string FlightSQLODBCMockTestBase::getConnectionString() {
  std::string connect_str(
      "driver={Apache Arrow Flight SQL ODBC Driver};HOST=localhost;port=" +
      std::to_string(port) + ";token=" + std::string(test_token) +
      ";useEncryption=false;");
  return connect_str;
}

std::string FlightSQLODBCMockTestBase::getInvalidConnectionString() {
  std::string connect_str = getConnectionString();
  // Append invalid token to connection string
  connect_str += std::string("token=invalid_token;");
  return connect_str;
}

void FlightSQLODBCMockTestBase::SetUp() {
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("0.0.0.0", 0));
  arrow::flight::FlightServerOptions options(location);
  options.auth_handler = std::make_unique<NoOpAuthHandler>();
  options.middleware.push_back(
      {"bearer-auth-server", std::make_shared<MockServerMiddlewareFactory>()});
  ASSERT_OK_AND_ASSIGN(server,
                       arrow::flight::sql::example::SQLiteFlightSqlServer::Create());
  ASSERT_OK(server->Init(options));

  port = server->port();
  ASSERT_OK_AND_ASSIGN(location, Location::ForGrpcTcp("localhost", port));
  ASSERT_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));
}

void FlightSQLODBCMockTestBase::TearDown() { ASSERT_OK(server->Shutdown()); }

bool compareConnPropertyMap(Connection::ConnPropertyMap map1,
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

  SQLWCHAR message[ODBC_BUFFER_SIZE] = {};
  SQLSMALLINT reallen = 0;

  // On Windows, reallen is in bytes. On Linux, reallen is in chars.
  // So, not using reallen
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message,
                ODBC_BUFFER_SIZE, &reallen);

  std::string res = SqlWcharToString(sql_state);

  EXPECT_EQ(res, expected_state);
}

std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle) {
  using ODBC::SqlWcharToString;

  SQLWCHAR sql_state[7] = {};
  SQLINTEGER native_code;

  SQLWCHAR message[ODBC_BUFFER_SIZE] = {};
  SQLSMALLINT reallen = 0;

  // On Windows, reallen is in bytes. On Linux, reallen is in chars.
  // So, not using reallen
  SQLGetDiagRec(handle_type, handle, 1, sql_state, &native_code, message,
                ODBC_BUFFER_SIZE, &reallen);

  std::string res = SqlWcharToString(sql_state);

  if (res.empty() || !message[0]) {
    res = "Cannot find ODBC error message";
  } else {
    res.append(": ").append(SqlWcharToString(message));
  }

  return res;
}

bool writeDSN(std::string connection_str) {
  Connection::ConnPropertyMap properties;

  ODBC::ODBCConnection::getPropertiesFromConnString(connection_str, properties);
  return writeDSN(properties);
}

bool writeDSN(Connection::ConnPropertyMap properties) {
  using driver::flight_sql::FlightSqlConnection;
  using driver::flight_sql::config::Configuration;
  using driver::odbcabstraction::Connection;
  using ODBC::ODBCConnection;

  Configuration config;
  config.Set(FlightSqlConnection::DSN, std::string(TEST_DSN));

  for (const auto& [key, value] : properties) {
    config.Set(key, value);
  }

  std::string driver = config.Get(FlightSqlConnection::DRIVER);
  std::wstring wDriver = arrow::util::UTF8ToWideString(driver).ValueOr(L"");
  return RegisterDsn(config, wDriver.c_str());
}

}  // namespace arrow::flight::sql::odbc
