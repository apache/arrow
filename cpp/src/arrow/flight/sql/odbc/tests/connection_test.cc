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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  EXPECT_THAT(env, ::testing::NotNull());
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_connect = SQLAllocConnect(env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_connect);
}

TEST(SQLFreeHandle, TestSQLFreeHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeEnv(env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);

  // Free the created connection using free handle
  SQLRETURN return_free_handle = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, return_free_handle);
}

TYPED_TEST(FlightSQLODBCTestBase, TestFreeNullHandles) {
  // Verifies attempt to free invalid handle does not cause segfault
  // Attempt to free null statement handle
  SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, this->stmt);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);

  // Attempt to free null connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, this->conn);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);

  // Attempt to free null environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, this->env);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);

  // Free the created connection using free connect
  SQLRETURN return_free_connect = SQLFreeConnect(conn);

  EXPECT_EQ(SQL_SUCCESS, return_free_connect);
}

}  // namespace arrow::flight::sql::odbc
