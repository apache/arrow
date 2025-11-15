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

TYPED_TEST(StatementTest, TestSQLExtendedFetchRowFetching) {
  // Set SQL_ROWSET_SIZE to fetch 3 rows at once

  constexpr SQLULEN rows = 3;
  SQLINTEGER val[rows];
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind[rows];

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, val, buf_len, ind));

  ASSERT_EQ(SQL_SUCCESS, SQLSetStmtAttr(this->stmt, SQL_ROWSET_SIZE,
                                        reinterpret_cast<SQLPOINTER>(rows), 0));

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

  // Fetch row 1-3.
  SQLULEN row_count;
  SQLUSMALLINT row_status[rows];

  ASSERT_EQ(SQL_SUCCESS,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count, row_status));
  EXPECT_EQ(3, row_count);

  for (int i = 0; i < rows; i++) {
    EXPECT_EQ(SQL_SUCCESS, row_status[i]);
  }

  // Verify 1 is returned for row 1
  EXPECT_EQ(1, val[0]);
  // Verify 2 is returned for row 2
  EXPECT_EQ(2, val[1]);
  // Verify 3 is returned for row 3
  EXPECT_EQ(3, val[2]);

  // Verify result set has no more data beyond row 3
  SQLULEN row_count2;
  SQLUSMALLINT row_status2[rows];
  EXPECT_EQ(SQL_NO_DATA,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count2, row_status2));
}

TEST_F(StatementRemoteTest, DISABLED_TestSQLExtendedFetchQueryNullIndicator) {
  // GH-47110: SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  SQLINTEGER val;

  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, 0));

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLULEN row_count1;
  SQLUSMALLINT row_status1[1];

  // SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002 state
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count1, row_status1));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState22002);
}

}  // namespace arrow::flight::sql::odbc
