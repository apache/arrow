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

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>
#include "gtest/gtest.h"

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  EXPECT_TRUE(env != NULL);
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_connect = SQLAllocConnect(env, &conn);

  EXPECT_TRUE(return_alloc_connect == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeEnv(env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free handle
  SQLRETURN return_free_handle = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(return_free_handle == SQL_SUCCESS);
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free connect
  SQLRETURN return_free_connect = SQLFreeConnect(conn);

  EXPECT_TRUE(return_free_connect == SQL_SUCCESS);
}

}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
