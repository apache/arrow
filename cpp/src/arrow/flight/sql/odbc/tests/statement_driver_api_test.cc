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

#include "arrow/flight/sql/odbc/odbc_api_internal.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_statement.h"

#include <cstdint>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

class StatementDriverApiTest : public FlightSQLODBCMockTestBase {
 protected:
  void SetUp() override {
    FlightSQLODBCMockTestBase::SetUp();

    ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLAllocHandle(
                               SQL_HANDLE_ENV, SQL_NULL_HANDLE, &driver_env_));
    ASSERT_EQ(SQL_SUCCESS,
              arrow::flight::sql::odbc::SQLSetEnvAttr(
                  driver_env_, SQL_ATTR_ODBC_VERSION,
                  reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(SQL_OV_ODBC3)), 0));
    ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLAllocHandle(
                               SQL_HANDLE_DBC, driver_env_, &driver_conn_));

    const std::string connect_string = GetConnectionString();
    std::vector<SQLWCHAR> wide_connect_string(connect_string.begin(),
                                              connect_string.end());
    SQLWCHAR output[kOdbcBufferSize];
    SQLSMALLINT output_length;
    ASSERT_EQ(SQL_SUCCESS,
              arrow::flight::sql::odbc::SQLDriverConnect(
                  driver_conn_, nullptr, wide_connect_string.data(),
                  static_cast<SQLSMALLINT>(wide_connect_string.size()), output,
                  kOdbcBufferSize, &output_length, SQL_DRIVER_NOPROMPT));
    driver_connected_ = true;

    ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLAllocHandle(
                               SQL_HANDLE_STMT, driver_conn_, &driver_stmt_));
  }

  void TearDown() override {
    if (driver_stmt_ != SQL_NULL_HSTMT) {
      EXPECT_EQ(SQL_SUCCESS,
                arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_STMT, driver_stmt_));
    }
    if (driver_connected_) {
      EXPECT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLDisconnect(driver_conn_));
    }
    if (driver_conn_ != SQL_NULL_HDBC) {
      EXPECT_EQ(SQL_SUCCESS,
                arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_DBC, driver_conn_));
    }
    if (driver_env_ != SQL_NULL_HENV) {
      EXPECT_EQ(SQL_SUCCESS,
                arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_ENV, driver_env_));
    }
    FlightSQLODBCMockTestBase::TearDown();
  }

  void VerifyDriverErrorState(std::string_view expected_state) {
    ODBC::ODBCStatement* statement = ODBC::ODBCStatement::Of(driver_stmt_);
    ASSERT_TRUE(statement->GetDiagnostics().HasRecord(0));
    EXPECT_EQ(expected_state, statement->GetDiagnostics().GetSQLState(0));
  }

  SQLHENV driver_env_ = SQL_NULL_HENV;
  SQLHDBC driver_conn_ = SQL_NULL_HDBC;
  SQLHSTMT driver_stmt_ = SQL_NULL_HSTMT;
  bool driver_connected_ = false;
};

TEST_F(StatementDriverApiTest, TestSQLFetchWithoutCursor) {
  ASSERT_EQ(SQL_ERROR, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  VerifyDriverErrorState(kErrorState24000);
}

TEST_F(StatementDriverApiTest, TestSQLGetDataWithoutCursor) {
  SQLINTEGER value;
  SQLLEN indicator;
  ASSERT_EQ(SQL_ERROR,
            arrow::flight::sql::odbc::SQLGetData(driver_stmt_, 1, SQL_C_LONG, &value,
                                                 sizeof(value), &indicator));
  VerifyDriverErrorState(kErrorState24000);
}

TEST_F(StatementDriverApiTest, TestChangingAppRowDescriptorRebindsColumns) {
  SQLINTEGER implicit_value = -1;
  SQLLEN implicit_indicator;
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLBindCol(
                             driver_stmt_, 1, SQL_C_LONG, &implicit_value,
                             sizeof(implicit_value), &implicit_indicator));

  ASSIGN_SQLWCHAR_ARR_AND_LEN(wsql,
                              LR"(
   SELECT 1 AS value
   UNION ALL SELECT 2
   UNION ALL SELECT 3
   UNION ALL SELECT 4
   UNION ALL SELECT 5;
 )");
  ASSERT_EQ(SQL_SUCCESS,
            arrow::flight::sql::odbc::SQLExecDirect(driver_stmt_, wsql, wsql_len));

  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  EXPECT_EQ(1, implicit_value);

  SQLHDESC explicit_ard;
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLAllocHandle(
                             SQL_HANDLE_DESC, driver_conn_, &explicit_ard));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLSetStmtAttr(
                             driver_stmt_, SQL_ATTR_APP_ROW_DESC, explicit_ard, 0));
  SQLINTEGER explicit_value = -1;
  SQLLEN explicit_indicator;
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLBindCol(
                             driver_stmt_, 1, SQL_C_LONG, &explicit_value,
                             sizeof(explicit_value), &explicit_indicator));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  EXPECT_EQ(2, explicit_value);

  // Restoring the implicit ARD must rebind the result set to its original buffer.
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLSetStmtAttr(
                             driver_stmt_, SQL_ATTR_APP_ROW_DESC, SQL_NULL_HANDLE, 0));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  EXPECT_EQ(3, implicit_value);
  EXPECT_EQ(2, explicit_value);
  ASSERT_EQ(SQL_SUCCESS,
            arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_DESC, explicit_ard));

  // Releasing an active explicit ARD must also restore and rebind the implicit ARD.
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLAllocHandle(
                             SQL_HANDLE_DESC, driver_conn_, &explicit_ard));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLSetStmtAttr(
                             driver_stmt_, SQL_ATTR_APP_ROW_DESC, explicit_ard, 0));
  explicit_value = -1;
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLBindCol(
                             driver_stmt_, 1, SQL_C_LONG, &explicit_value,
                             sizeof(explicit_value), &explicit_indicator));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  EXPECT_EQ(4, explicit_value);

  ASSERT_EQ(SQL_SUCCESS,
            arrow::flight::sql::odbc::SQLFreeHandle(SQL_HANDLE_DESC, explicit_ard));
  ASSERT_EQ(SQL_SUCCESS, arrow::flight::sql::odbc::SQLFetch(driver_stmt_));
  EXPECT_EQ(5, implicit_value);
  EXPECT_EQ(4, explicit_value);
}

}  // namespace arrow::flight::sql::odbc
