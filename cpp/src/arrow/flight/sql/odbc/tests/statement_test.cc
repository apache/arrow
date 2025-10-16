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

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsInputString) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / ODBC::GetSqlWCharSize();
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
  SQLINTEGER buf_char_len = sizeof(buf) / ODBC::GetSqlWCharSize();
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
  SQLINTEGER small_buf_char_len = sizeof(small_buf) / ODBC::GetSqlWCharSize();
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
  SQLINTEGER buf_char_len = sizeof(buf) / ODBC::GetSqlWCharSize();
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

}  // namespace arrow::flight::sql::odbc
