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

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS, SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0));

  ASSERT_EQ(SQL_OV_ODBC2, version);

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to supported version
  ASSERT_EQ(SQL_SUCCESS, SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                       reinterpret_cast<void*>(SQL_OV_ODBC2), 0));

  SQLINTEGER version;
  // Check ODBC version is set
  ASSERT_EQ(SQL_SUCCESS, SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0));

  ASSERT_EQ(SQL_OV_ODBC2, version);

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to unsupported version
  ASSERT_EQ(SQL_ERROR,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TYPED_TEST(ConnectionTest, TestSQLGetEnvAttrOutputNTS) {
  SQLINTEGER output_nts;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetEnvAttr(this->env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0));

  ASSERT_EQ(SQL_TRUE, output_nts);
}

TYPED_TEST(ConnectionTest, DISABLED_TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. Windows driver manager ignores the length pointer.
  // This test case can be potentially used on macOS/Linux
  SQLINTEGER length;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length));

  EXPECT_EQ(sizeof(SQLINTEGER), length);
}

TYPED_TEST(ConnectionTest, DISABLED_TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. The Windows driver manager doesn't error out when null pointer is passed.
  // This test case can be potentially used on macOS/Linux
  ASSERT_EQ(SQL_ERROR,
            SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSValid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to output nts to supported version
  ASSERT_EQ(SQL_SUCCESS, SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS,
                                       reinterpret_cast<void*>(SQL_TRUE), 0));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSInvalid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to output nts to unsupported false
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS,
                                     reinterpret_cast<void*>(SQL_FALSE), 0));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrNullValuePointer) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set using bad data pointer
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

}  // namespace arrow::flight::sql::odbc
