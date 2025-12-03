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

#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

template <typename T>
class StatementTest : public T {};

class StatementMockTest : public FlightSQLODBCMockTestBase {};
class StatementRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<StatementMockTest, StatementRemoteTest>;
TYPED_TEST_SUITE(StatementTest, TestTypes);

TYPED_TEST(StatementTest, TestSQLExecDirectSimpleQuery) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  // GH-47713 TODO: Uncomment call to SQLFetch SQLGetData after implementation
  /*
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Verify 1 is returned
  EXPECT_EQ(1, val);

  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
  */
}

TYPED_TEST(StatementTest, TestSQLExecDirectInvalidQuery) {
  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY000);
}

TYPED_TEST(StatementTest, TestSQLExecuteSimpleQuery) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLExecute(this->stmt));

  // GH-47713 TODO: Uncomment call to SQLFetch SQLGetData after implementation
  /*
  // Fetch data
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));

  // Verify 1 is returned
  EXPECT_EQ(1, val);

  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
  */
}

TYPED_TEST(StatementTest, TestSQLPrepareInvalidQuery) {
  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY000);

  ASSERT_EQ(SQL_ERROR, SQLExecute(this->stmt));
  // Verify function sequence error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsInputString) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, input_char_len, buf,
                                      buf_char_len, &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  // returned length is in characters
  std::wstring returned_string(buf, buf + output_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsNTSInputString) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, SQL_NTS, buf, buf_char_len,
                                      &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  // returned length is in characters
  std::wstring returned_string(buf, buf + output_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsInputStringLength) {
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, input_char_len, nullptr, 0,
                                      &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  ASSERT_EQ(SQL_SUCCESS,
            SQLNativeSql(this->conn, input_str, SQL_NTS, nullptr, 0, &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsTruncatedString) {
  const SQLINTEGER small_buf_size_in_char = 11;
  SQLWCHAR small_buf[small_buf_size_in_char];
  SQLINTEGER small_buf_char_len = sizeof(small_buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;

  // Create expected return string based on buf size
  SQLWCHAR expected_string_buf[small_buf_size_in_char];
  wcsncpy(expected_string_buf, input_str, 10);
  expected_string_buf[10] = L'\0';
  std::wstring expected_string(expected_string_buf,
                               expected_string_buf + small_buf_size_in_char);

  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLNativeSql(this->conn, input_str, input_char_len, small_buf,
                         small_buf_char_len, &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorState01004);

  // Returned text length represents full string char length regardless of truncation
  EXPECT_EQ(input_char_len, output_char_len);

  std::wstring returned_string(small_buf, small_buf + small_buf_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsErrorOnBadInputs) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, nullptr, input_char_len, buf,
                                    buf_char_len, &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY009);

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, nullptr, SQL_NTS, buf, buf_char_len,
                                    &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY009);

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, input_str, -100, buf, buf_char_len,
                                    &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY090);
}

TYPED_TEST(StatementTest, TestSQLCloseCursor) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLCloseCursor(this->stmt));
}

TYPED_TEST(StatementTest, TestSQLFreeStmtSQLCloseWithoutCursor) {
  // Verify SQLFreeStmt(SQL_CLOSE) does not throw error with invalid cursor

  ASSERT_EQ(SQL_SUCCESS, SQLFreeStmt(this->stmt, SQL_CLOSE));
}

TYPED_TEST(StatementTest, TestSQLCloseCursorWithoutCursor) {
  ASSERT_EQ(SQL_ERROR, SQLCloseCursor(this->stmt));

  // Verify invalid cursor error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

}  // namespace arrow::flight::sql::odbc
