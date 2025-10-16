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

TYPED_TEST(StatementTest, TestSQLFetchScrollRowFetching) {
  SQLLEN rows_fetched;
  SQLSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

  std::wstring wsql =
      LR"(
   SELECT 1 AS small_table
   UNION ALL
   SELECT 2
   UNION ALL
   SELECT 3;
 )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  // Fetch row 1
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));
  // Verify 1 is returned
  EXPECT_EQ(1, val);
  // Verify 1 row is fetched
  EXPECT_EQ(1, rows_fetched);

  // Fetch row 2
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 2 is returned
  EXPECT_EQ(2, val);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(1, rows_fetched);

  // Fetch row 3
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 3 is returned
  EXPECT_EQ(3, val);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(1, rows_fetched);

  // Verify result set has no more data beyond row 3
  ASSERT_EQ(SQL_NO_DATA, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

TYPED_TEST(StatementTest, TestSQLFetchScrollUnsupportedOrientation) {
  // SQL_FETCH_PRIOR is the only supported fetch orientation.

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_PRIOR, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  SQLLEN fetch_offset = 1;
  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_RELATIVE, fetch_offset));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_ABSOLUTE, fetch_offset));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_FIRST, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_LAST, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_BOOKMARK, fetch_offset));

  // ODBC Driver Manager returns state HY106 for SQL_FETCH_BOOKMARK
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY106);
}

}  // namespace arrow::flight::sql::odbc
