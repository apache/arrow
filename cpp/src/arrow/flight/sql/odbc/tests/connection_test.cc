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

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  // ODBC Environment
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, return_get);

  EXPECT_EQ(SQL_OV_ODBC2, version);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_EQ(SQL_SUCCESS, return_set);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0);

  EXPECT_EQ(SQL_ERROR, return_set);
}

}  // namespace arrow::flight::sql::odbc
