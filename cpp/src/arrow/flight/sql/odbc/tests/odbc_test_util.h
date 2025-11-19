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

#include <gtest/gtest.h>

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include "arrow/compute/api.h"

#include <sql.h>
#include <sqlucode.h>

#include <iostream>

namespace arrow::flight::sql::odbc {

// -AL- todo update to actual ODBC allocation
class OdbcUtilEnvironment : public ::testing::Environment {
 public:
  void SetUp() override {
    // -AL- todo add env_v2 for v2 after global setup/teardown works.

    // Allocate an environment handle
    ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

    ASSERT_EQ(SQL_SUCCESS,
              SQLSetEnvAttr(
                  env, SQL_ATTR_ODBC_VERSION,
                  reinterpret_cast<SQLPOINTER>(static_cast<intptr_t>(SQL_OV_ODBC3)), 0));

    // Allocate a connection using alloc handle
    ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));
    std::cout << "-AL- OdbcUtilEnvironment::SetUp\n";
  }

  void TearDown() override {
    // Clear function registry before test exits
    auto reg = arrow::compute::GetFunctionRegistry();
    reg->ClearFunctioRegistry();

    // Free connection handle
    EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

    // Free environment handle
    EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
    std::cout << "-AL- OdbcUtilEnvironment::TearDown\n";
  }

  SQLHENV getEnvHandle() { return env; }

  SQLHDBC getConnHandle() { return conn; }

 private:
  /** ODBC Environment. */
  SQLHENV env = 0;

  /** ODBC Connect. */
  SQLHDBC conn = 0;
};

}  // namespace arrow::flight::sql::odbc
