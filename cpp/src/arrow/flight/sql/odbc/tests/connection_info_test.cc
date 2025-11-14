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
class ConnectionInfoTest : public T {};

class ConnectionInfoMockTest : public FlightSQLODBCMockTestBase {};
using TestTypes = ::testing::Types<ConnectionInfoMockTest, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(ConnectionInfoTest, TestTypes);
// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHdesc) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  // Value returned from driver manager is the desc address
  SQLHDESC local_desc = descriptor;
  EXPECT_EQ(SQL_SUCCESS, SQLGetInfo(this->conn, SQL_HANDLE_DESC, &local_desc, 0, 0));
  EXPECT_GT(local_desc, static_cast<SQLHSTMT>(0));

  // Free descriptor handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

}  // namespace arrow::flight::sql::odbc
