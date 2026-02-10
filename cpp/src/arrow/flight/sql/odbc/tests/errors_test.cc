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

template <typename T>
class ErrorsTest : public T {};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(ErrorsTest, TestTypes);

template <typename T>
class ErrorsOdbcV2Test : public T {};

using TestTypesOdbcV2 =
    ::testing::Types<FlightSQLOdbcV2MockTestBase, FlightSQLOdbcV2RemoteTestBase>;
TYPED_TEST_SUITE(ErrorsOdbcV2Test, TestTypesOdbcV2);

template <typename T>
class ErrorsHandleTest : public T {};

using TestTypesHandle = ::testing::Types<FlightSQLOdbcEnvConnHandleMockTestBase,
                                         FlightSQLOdbcEnvConnHandleRemoteTestBase>;
TYPED_TEST_SUITE(ErrorsHandleTest, TestTypesHandle);

using ODBC::SqlWcharToString;

TYPED_TEST(ErrorsHandleTest, TestSQLGetDiagFieldWForConnectFailure) {
  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[kOdbcBufferSize];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(this->conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             kOdbcBufferSize, &out_str_len, SQL_DRIVER_NOPROMPT));

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, this->conn, HEADER_LEVEL, SQL_DIAG_NUMBER,
                            &diag_number, sizeof(SQLINTEGER), &diag_number_length));

  EXPECT_EQ(1, diag_number);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[kOdbcBufferSize];
  SQLSMALLINT server_name_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, this->conn, RECORD_1, SQL_DIAG_SERVER_NAME,
                            server_name, kOdbcBufferSize, &server_name_length));

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[kOdbcBufferSize];
  SQLSMALLINT message_text_length;

  SQLRETURN ret =
      SQLGetDiagField(SQL_HANDLE_DBC, this->conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                      message_text, kOdbcBufferSize, &message_text_length);

  // dependent on the size of the message it could output SQL_SUCCESS_WITH_INFO
  EXPECT_TRUE(ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, this->conn, RECORD_1, SQL_DIAG_NATIVE,
                            &diag_native, sizeof(diag_native), &diag_native_length));

  EXPECT_EQ(200, diag_native);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;

  EXPECT_EQ(
      SQL_SUCCESS,
      SQLGetDiagField(SQL_HANDLE_DBC, this->conn, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                      sql_state_size * GetSqlWCharSize(), &sql_state_length));

  EXPECT_EQ(kErrorState28000, SqlWcharToString(sql_state));
}

TYPED_TEST(ErrorsHandleTest, DISABLED_TestSQLGetDiagFieldWForConnectFailureNTS) {
  // Test is disabled because driver manager on Windows does not pass through SQL_NTS
  // This test case can be potentially used on macOS/Linux

  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[kOdbcBufferSize];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(this->conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             kOdbcBufferSize, &out_str_len, SQL_DRIVER_NOPROMPT));

  // Retrieve all supported header level and record level data
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_MESSAGE_TEXT SQL_NTS
  SQLWCHAR message_text[kOdbcBufferSize];
  SQLSMALLINT message_text_length;

  message_text[kOdbcBufferSize - 1] = '\0';

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DBC, this->conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, SQL_NTS, &message_text_length));

  EXPECT_GT(message_text_length, 100);
}

// iODBC does not support application allocated descriptors.
#ifndef __APPLE__
TYPED_TEST(ErrorsTest, TestSQLGetDiagFieldWForDescriptorFailureFromDriverManager) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR, SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0,
                                       0, nullptr));

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
  SQLWCHAR server_name[kOdbcBufferSize];
  SQLSMALLINT server_name_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_SERVER_NAME,
                            server_name, kOdbcBufferSize, &server_name_length));

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[kOdbcBufferSize];
  SQLSMALLINT message_text_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, kOdbcBufferSize, &message_text_length));

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

  EXPECT_EQ(kErrorStateIM001, SqlWcharToString(sql_state));

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

TYPED_TEST(ErrorsTest, TestSQLGetDiagRecForDescriptorFailureFromDriverManager) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR, SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0,
                                       0, nullptr));

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[kOdbcBufferSize];
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagRec(SQL_HANDLE_DESC, descriptor, 1, sql_state, &native_error,
                          message, kOdbcBufferSize, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(0, native_error);

  // API not implemented error from driver manager
  EXPECT_EQ(kErrorStateIM001, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}
#endif  // __APPLE__

TYPED_TEST(ErrorsHandleTest, TestSQLGetDiagRecForConnectFailure) {
  // Invalid connect string
  std::string connect_str = this->GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[kOdbcBufferSize];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(this->conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             kOdbcBufferSize, &out_str_len, SQL_DRIVER_NOPROMPT));

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[kOdbcBufferSize];
  SQLSMALLINT message_length;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 1, sql_state, &native_error,
                          message, kOdbcBufferSize, &message_length));

  EXPECT_GT(message_length, 120);

  EXPECT_EQ(200, native_error);

  EXPECT_EQ(kErrorState28000, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsTest, TestSQLGetDiagRecInputData) {
  // SQLGetDiagRec does not post diagnostic records for itself.

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[kOdbcBufferSize];
  SQLSMALLINT message_length;

  // Pass invalid record number
  EXPECT_EQ(SQL_ERROR,
            SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 0, sql_state, &native_error,
                          message, kOdbcBufferSize, &message_length));

  // Pass valid record number with null inputs
  EXPECT_EQ(SQL_NO_DATA, SQLGetDiagRec(SQL_HANDLE_DBC, this->conn, 1, nullptr, nullptr,
                                       nullptr, 0, nullptr));

  // Invalid handle
#ifdef __APPLE__
  // MacOS ODBC driver manager requires connection handle
  EXPECT_EQ(SQL_INVALID_HANDLE,
            SQLGetDiagRec(0, this->conn, 1, nullptr, nullptr, nullptr, 0, nullptr));
#else
  EXPECT_EQ(SQL_INVALID_HANDLE,
            SQLGetDiagRec(0, nullptr, 0, nullptr, nullptr, nullptr, 0, nullptr));
#endif  // __APPLE__
}

TYPED_TEST(ErrorsOdbcV2Test, TestSQLErrorInputData) {
  // Test ODBC 2.0 API SQLError. Driver manager maps SQLError to SQLGetDiagRec.
  // SQLError does not post diagnostic records for itself.

  // Pass valid handles with null inputs
  EXPECT_EQ(SQL_NO_DATA,
            SQLError(this->env, nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr));

  EXPECT_EQ(SQL_NO_DATA, SQLError(nullptr, this->conn, nullptr, nullptr, nullptr, nullptr,
                                  0, nullptr));

#ifdef __APPLE__
  EXPECT_EQ(SQL_NO_DATA, SQLError(SQL_NULL_HENV, this->conn, this->stmt, nullptr, nullptr,
                                  nullptr, 0, nullptr));
#else
  EXPECT_EQ(SQL_NO_DATA, SQLError(nullptr, nullptr, this->stmt, nullptr, nullptr, nullptr,
                                  0, nullptr));
#endif  // __APPLE__

  // Invalid handle
  EXPECT_EQ(SQL_INVALID_HANDLE,
            SQLError(nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, 0, nullptr));
}

TYPED_TEST(ErrorsTest, TestSQLErrorEnvErrorFromDriverManager) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  // Attempt to set environment attribute after connection handle allocation
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                     reinterpret_cast<void*>(SQL_OV_ODBC2), 0));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(this->env, nullptr, nullptr, sql_state, &native_error,
                                  message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 40);

  EXPECT_EQ(0, native_error);

  // Function sequence error state from driver manager
  EXPECT_EQ(kErrorStateHY010, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsTest, TestSQLErrorConnError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  // Attempt to set unsupported attribute
  ASSERT_EQ(SQL_ERROR,
            SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, nullptr));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(nullptr, this->conn, nullptr, sql_state, &native_error,
                                  message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(100, native_error);

  // optional feature not supported error state
  EXPECT_EQ(kErrorStateHYC00, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsTest, TestSQLErrorStmtError) {
  // Test ODBC 2.0 API SQLError.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  std::wstring wsql = L"1";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  SQLRETURN ret = SQLError(nullptr, this->conn, this->stmt, sql_state, &native_error,
                           message, SQL_MAX_MESSAGE_LENGTH, &message_length);

  EXPECT_TRUE(ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO);

  EXPECT_GT(message_length, 70);

  EXPECT_EQ(100, native_error);

  EXPECT_EQ(kErrorStateHY000, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsTest, TestSQLErrorStmtWarning) {
  // Test ODBC 2.0 API SQLError.

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
  ASSERT_EQ(SQL_SUCCESS,
            SQLError(SQL_NULL_HENV, this->conn, this->stmt, sql_state, &native_error,
                     message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(1000100, native_error);

  // Verify string truncation warning is reported
  EXPECT_EQ(kErrorState01004, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsOdbcV2Test, TestSQLErrorEnvErrorFromDriverManager) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  // Attempt to set environment attribute after connection handle allocation
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION,
                                     reinterpret_cast<void*>(SQL_OV_ODBC2), 0));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(this->env, nullptr, nullptr, sql_state, &native_error,
                                  message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 40);

  EXPECT_EQ(0, native_error);

  // Function sequence error state from driver manager
#ifdef _WIN32
  // Windows Driver Manager returns S1010
  EXPECT_EQ(kErrorStateS1010, SqlWcharToString(sql_state));
#else
  // unix Driver Manager returns HY010
  EXPECT_EQ(kErrorStateHY010, SqlWcharToString(sql_state));
#endif  // _WIN32

  EXPECT_FALSE(std::wstring(message).empty());
}

// TODO: verify that `SQLGetConnectOption` is not required by Excel.
#ifndef __APPLE__
TYPED_TEST(ErrorsOdbcV2Test, TestSQLErrorConnError) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  // Known macOS Driver Manager (DM) behavior:
  // Attempts to call SQLGetConnectOption without redirecting the API call to
  // SQLGetConnectAttr. SQLGetConnectOption is not implemented as it is not required by
  // macOS Excel.

  // Attempt to set unsupported attribute
  ASSERT_EQ(SQL_ERROR,
            SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, 0, 0, nullptr));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  SQLSMALLINT message_length = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLError(nullptr, this->conn, nullptr, sql_state, &native_error,
                                  message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(100, native_error);

  // optional feature not supported error state. Driver Manager maps state to S1C00
  EXPECT_EQ(kErrorStateS1C00, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}
#endif  // __APPLE__

TYPED_TEST(ErrorsOdbcV2Test, TestSQLErrorStmtError) {
  // Test ODBC 2.0 API SQLError with ODBC ver 2.
  // Known Windows Driver Manager (DM) behavior:
  // When application passes buffer length greater than SQL_MAX_MESSAGE_LENGTH (512),
  // DM passes 512 as buffer length to SQLError.

  std::wstring wsql = L"SELECT * from non_existent_table;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR sql_state[6] = {0};
  SQLINTEGER native_error = 0;
  SQLSMALLINT message_length = 0;
  SQLWCHAR message[SQL_MAX_MESSAGE_LENGTH] = {0};
  ASSERT_EQ(SQL_SUCCESS, SQLError(SQL_NULL_HENV, this->conn, this->stmt, sql_state, &native_error,
                                  message, SQL_MAX_MESSAGE_LENGTH, &message_length));
  EXPECT_GT(message_length, 70);

  EXPECT_EQ(100, native_error);

  // Driver Manager maps error state to S1000
  EXPECT_EQ(kErrorStateS1000, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

TYPED_TEST(ErrorsOdbcV2Test, TestSQLErrorStmtWarning) {
  // Test ODBC 2.0 API SQLError.

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
  ASSERT_EQ(SQL_SUCCESS,
            SQLError(SQL_NULL_HENV, this->conn, this->stmt, sql_state, &native_error,
                     message, SQL_MAX_MESSAGE_LENGTH, &message_length));

  EXPECT_GT(message_length, 50);

  EXPECT_EQ(1000100, native_error);

  // Verify string truncation warning is reported
  EXPECT_EQ(kErrorState01004, SqlWcharToString(sql_state));

  EXPECT_FALSE(std::wstring(message).empty());
}

}  // namespace arrow::flight::sql::odbc
