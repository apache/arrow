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
#include "arrow/flight/sql/odbc/tests/odbc_test_suite.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagFieldWForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, HEADER_LEVEL, SQL_DIAG_NUMBER, &diag_number,
                        sizeof(SQLINTEGER), &diag_number_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(diag_number, 1);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[ODBC_BUFFER_SIZE];
  SQLSMALLINT server_name_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SERVER_NAME, server_name,
                        ODBC_BUFFER_SIZE, &server_name_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, ODBC_BUFFER_SIZE, &message_text_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_NATIVE, &diag_native,
                        sizeof(diag_native), &diag_native_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(diag_native, 200);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;
  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                        sql_state_size * driver::odbcabstraction::GetSqlWCharSize(),
                        &sql_state_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"28000"));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagFieldWForConnectFailureNTS) {
  // Test is disabled because driver manager on Windows does not pass through SQL_NTS
  // This test case can be potentially used on macOS/Linux
  GTEST_SKIP();
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Retrieve all supported header level and record level data
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_MESSAGE_TEXT SQL_NTS
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  message_text[ODBC_BUFFER_SIZE - 1] = '\0';

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, SQL_NTS, &message_text_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagRecForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = this->getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  ret = SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state, &native_error, message,
                      ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 120);

  EXPECT_EQ(native_error, 200);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"28000"));

  EXPECT_TRUE(!std::wstring(message).empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagRecInputData) {
  // SQLGetDiagRec does not post diagnostic records for itself.
  this->connect();

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  // Pass invalid record number
  SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 0, sql_state, &native_error,
                                message, ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_ERROR);

  // Pass valid record number with null inputs
  ret = SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 1, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  // Invalid handle
  ret = SQLGetDiagRec(0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorInputData) {
  // Test ODBC 2.0 API SQLError. Driver manager maps SQLError to SQLGetDiagRec.
  // SQLError does not post diagnostic records for itself.
  this->connect();

  // Pass valid handles with null inputs
  SQLRETURN ret = SQLError(this->env, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLError(0, this->conn, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLError(0, 0, this->stmt, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_NO_DATA);

  // Invalid handle
  ret = SQLError(0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorEnvErrorFromDriverManager) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect();

  // Attempt to set environment attribute after connection handle allocation
  SQLRETURN ret = SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(this->env, 0, 0, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(native_error, 0);

  // Function sequence error state from driver manager
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"HY010"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorConnError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect();

  // Attempt to set unsupported attribute
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, this->conn, 0, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(native_error, 100);

  // optional feature not supported error state
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"HYC00"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect();

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(native_error, 100);

  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"HY000"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtWarning) {
  // Test ODBC 2.0 API SQLError.
  this->connect();

  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(native_error, 1000100);

  // Verify string truncation warning is reported
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"01004"));

  EXPECT_TRUE(!std::wstring(message).empty());
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorEnvErrorODBCVer2FromDriverManager) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect(SQL_OV_ODBC2);

  // Attempt to set environment attribute after connection handle allocation
  SQLRETURN ret = SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(this->env, 0, 0, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(native_error, 0);

  // Function sequence error state from driver manager
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"S1010"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorConnErrorODBCVer2) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect(SQL_OV_ODBC2);

  // Attempt to set unsupported attribute
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, this->conn, 0, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(native_error, 100);

  // optional feature not supported error state. Driver Manager maps state to S1C00
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"S1C00"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtErrorODBCVer2) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(native_error, 100);

  // Driver Manager maps error state to S1000
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"S1000"));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtWarningODBCVer2) {
  // Test ODBC 2.0 API SQLError.
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ret = SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                 SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(native_error, 1000100);

  // Verify string truncation warning is reported
  EXPECT_EQ(std::wstring(sql_state), std::wstring(L"01004"));

  EXPECT_TRUE(!std::wstring(message).empty());
}

}  // namespace arrow::flight::sql::odbc
