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

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagFieldWForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT));

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, HEADER_LEVEL, SQL_DIAG_NUMBER,
                            &diag_number, sizeof(SQLINTEGER), &diag_number_length));

  EXPECT_EQ(1, diag_number);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[ODBC_BUFFER_SIZE];
  SQLSMALLINT server_name_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SERVER_NAME,
                            server_name, ODBC_BUFFER_SIZE, &server_name_length));

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, ODBC_BUFFER_SIZE, &message_text_length));

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_NATIVE, &diag_native,
                            sizeof(diag_native), &diag_native_length));

  EXPECT_EQ(200, diag_native);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                            sql_state_size * arrow::flight::sql::odbc::GetSqlWCharSize(),
                            &sql_state_length));

  EXPECT_EQ(std::wstring(L"28000"), std::wstring(sql_state));

  // Free connection handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetDiagFieldWForConnectFailureNTS) {
  // Test is disabled because driver manager on Windows does not pass through SQL_NTS
  // This test case can be potentially used on macOS/Linux
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT));

  // Retrieve all supported header level and record level data
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_MESSAGE_TEXT SQL_NTS
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  message_text[ODBC_BUFFER_SIZE - 1] = '\0';

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, SQL_NTS, &message_text_length));

  EXPECT_GT(message_text_length, 100);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(FlightSQLODBCTestBase,
           TestSQLGetDiagFieldWForDescriptorFailureFromDriverManager) {
  this->Connect();
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR,
            SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0, 0, 0));

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, HEADER_LEVEL, SQL_DIAG_NUMBER,
                            &diag_number, sizeof(SQLINTEGER), &diag_number_length));

  EXPECT_EQ(1, diag_number);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[ODBC_BUFFER_SIZE];
  SQLSMALLINT server_name_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_SERVER_NAME,
                            server_name, ODBC_BUFFER_SIZE, &server_name_length));

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, ODBC_BUFFER_SIZE, &message_text_length));

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_NATIVE,
                            &diag_native, sizeof(diag_native), &diag_native_length));

  EXPECT_EQ(0, diag_native);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;
  EXPECT_EQ(
      SQL_SUCCESS,
      SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                      sql_state_size * GetSqlWCharSize(), &sql_state_length));

  EXPECT_EQ(std::wstring(L"IM001"), std::wstring(sql_state));

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase,
           TestSQLGetDiagRecForDescriptorFailureFromDriverManager) {
  this->Connect();
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR,
            SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0, 0, 0));

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagRec(SQL_HANDLE_DESC, descriptor, 1, sql_state, &native_error,
                          message, ODBC_BUFFER_SIZE, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(0, native_error);

  // API not implemented error from driver manager
  EXPECT_EQ(std::wstring(L"IM001"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagRecForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT));

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;
  ASSERT_EQ(SQL_SUCCESS, SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state, &native_error,
                                       message, ODBC_BUFFER_SIZE, &message_length));

  EXPECT_GT(message_length, 120);

  EXPECT_EQ(200, native_error);

  EXPECT_EQ(std::wstring(L"28000"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetDiagRecInputData) {
  // SQLGetDiagRec does not post diagnostic records for itself.
  this->Connect();

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  // Pass invalid record number
  EXPECT_EQ(SQL_ERROR,
            SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 0, sql_state, &native_error,
                          message, ODBC_BUFFER_SIZE, &message_length));

  // Pass valid record number with null inputs
  EXPECT_EQ(SQL_NO_DATA, SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 1, 0, 0, 0, 0, 0));

  // Invalid handle
  EXPECT_EQ(SQL_INVALID_HANDLE, SQLGetDiagRec(0, 0, 0, 0, 0, 0, 0, 0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorInputData) {
  // Test ODBC 2.0 API SQLError. Driver manager maps SQLError to SQLGetDiagRec.
  // SQLError does not post diagnostic records for itself.
  this->Connect();

  // Pass valid handles with null inputs
  EXPECT_EQ(SQL_NO_DATA, SQLError(this->env, 0, 0, 0, 0, 0, 0, 0));

  EXPECT_EQ(SQL_NO_DATA, SQLError(0, this->conn, 0, 0, 0, 0, 0, 0));

  EXPECT_EQ(SQL_NO_DATA, SQLError(0, 0, this->stmt, 0, 0, 0, 0, 0));

  // Invalid handle
  EXPECT_EQ(SQL_INVALID_HANDLE, SQLError(0, 0, 0, 0, 0, 0, 0, 0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorEnvErrorFromDriverManager) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect();

  // Attempt to set environment attribute after connection handle allocation
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                     reinterpret_cast<void*>(SQL_OV_ODBC2), 0));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(this->env, 0, 0, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(0, native_error);

  // Function sequence error state from driver manager
  EXPECT_EQ(std::wstring(L"HY010"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorConnError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect();

  // Attempt to set unsupported attribute
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, 0);

  ASSERT_EQ(SQL_ERROR, ret);

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, this->conn, 0, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(100, native_error);

  // optional feature not supported error state
  EXPECT_EQ(std::wstring(L"HYC00"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect();

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(100, native_error);

  EXPECT_EQ(std::wstring(L"HY000"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtWarning) {
  // Test ODBC 2.0 API SQLError.
  this->Connect();

  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;

  EXPECT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(1000100, native_error);

  // Verify string truncation warning is reported
  EXPECT_EQ(std::wstring(L"01004"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorEnvErrorODBCVer2FromDriverManager) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect(SQL_OV_ODBC2);

  // Attempt to set environment attribute after connection handle allocation
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                     reinterpret_cast<void*>(SQL_OV_ODBC2), 0));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(this->env, 0, 0, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(0, native_error);

  // Function sequence error state from driver manager
  EXPECT_EQ(std::wstring(L"S1010"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorConnErrorODBCVer2) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect(SQL_OV_ODBC2);

  // Attempt to set unsupported attribute
  ASSERT_EQ(SQL_ERROR, SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, 0));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, this->conn, 0, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(100, native_error);

  // optional feature not supported error state. Driver Manager maps state to S1C00
  EXPECT_EQ(std::wstring(L"S1C00"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtErrorODBCVer2) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.
  this->Connect(SQL_OV_ODBC2);

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(100, native_error);

  // Driver Manager maps error state to S1000
  EXPECT_EQ(std::wstring(L"S1000"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLErrorStmtWarningODBCVer2) {
  // Test ODBC 2.0 API SQLError.
  this->Connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;

  EXPECT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(0, 0, this->stmt, sql_state, &native_error, message,
                                  SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(1000100, native_error);

  // Verify string truncation warning is reported
  EXPECT_EQ(std::wstring(L"01004"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());
}

}  // namespace arrow::flight::sql::odbc
