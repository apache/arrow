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
class ConnectionTest : public T {};

// GH-46574 TODO: add remote server test cases using `ConnectionRemoteTest`
class ConnectionRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<FlightSQLODBCMockTestBase, ConnectionRemoteTest>;
TYPED_TEST_SUITE(ConnectionTest, TestTypes);

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env));

  ASSERT_NE(env, nullptr);

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Free a connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocConnect(env, &conn));

  // Free a connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeConnect(conn));

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLFreeHandle, TestFreeNullHandles) {
  SQLHENV env = NULL;
  SQLHDBC conn = NULL;
  SQLHSTMT stmt = NULL;

  // Verifies attempt to free invalid handle does not cause segfault
  // Attempt to free null statement handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_STMT, stmt));

  // Attempt to free null connection handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Attempt to free null environment handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

}  // namespace arrow::flight::sql::odbc
