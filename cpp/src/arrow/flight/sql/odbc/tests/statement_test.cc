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

#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectSimpleQuery) {
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  // TODO: after SQLFetch and SQLGetData are implemented, fetch data to verify

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectInvalidQuery) {
  this->connect();

  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY000);

  this->disconnect();
}
}  // namespace arrow::flight::sql::odbc
