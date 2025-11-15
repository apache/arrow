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

TYPED_TEST(StatementTest, SQLNumResultColsReturnsColumnsOnSelect) {
  SQLSMALLINT column_count = 0;
  SQLSMALLINT expected_value = 3;
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLNumResultCols(this->stmt, &column_count));

  EXPECT_EQ(expected_value, column_count);
}

TYPED_TEST(StatementTest, SQLNumResultColsReturnsSuccessOnNullptr) {
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLNumResultCols(this->stmt, nullptr));
}

TYPED_TEST(StatementTest, SQLNumResultColsFunctionSequenceErrorOnNoQuery) {
  SQLSMALLINT column_count = 0;
  SQLSMALLINT expected_value = 0;

  ASSERT_EQ(SQL_ERROR, SQLNumResultCols(this->stmt, &column_count));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);

  EXPECT_EQ(expected_value, column_count);
}

}  // namespace arrow::flight::sql::odbc
