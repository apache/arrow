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
class ErrorsTest : public T {};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(ErrorsTest, TestTypes);

template <typename T>
class ErrorsOdbcV2Test : public T {};

using TestTypesOdbcV2 =
    ::testing::Types<FlightSQLOdbcV2MockTestBase, FlightSQLOdbcV2RemoteTestBase>;
TYPED_TEST_SUITE(ErrorsOdbcV2Test, TestTypesOdbcV2);

template <typename T>
class ErrorsHandleTest : public T {};

using TestTypesHandle =
    ::testing::Types<FlightSQLOdbcHandleMockTestBase, FlightSQLOdbcHandleRemoteTestBase>;
TYPED_TEST_SUITE(ErrorsHandleTest, TestTypesHandle);

TYPED_TEST(ErrorsTest, TestSQLGetDiagFieldWForDescriptorFailureFromDriverManager) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR,
            SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0, 0, 0));

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, HEADER_LEVEL, SQL_DIAG_NUMBER,
                            &diag_number, sizeof(SQLINTEGER), &diag_number_length));

  EXPECT_EQ(1, diag_number);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[kOdbcBufferSize];
  SQLSMALLINT server_name_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_SERVER_NAME,
                            server_name, kOdbcBufferSize, &server_name_length));

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[kOdbcBufferSize];
  SQLSMALLINT message_text_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                            message_text, kOdbcBufferSize, &message_text_length));

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_NATIVE,
                            &diag_native, sizeof(diag_native), &diag_native_length));

  EXPECT_EQ(0, diag_native);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;
  EXPECT_EQ(
      SQL_SUCCESS,
      SQLGetDiagField(SQL_HANDLE_DESC, descriptor, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                      sql_state_size * GetSqlWCharSize(), &sql_state_length));

  EXPECT_EQ(std::wstring(L"IM001"), std::wstring(sql_state));

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

TYPED_TEST(ErrorsTest, TestSQLGetDiagRecForDescriptorFailureFromDriverManager) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  EXPECT_EQ(SQL_ERROR,
            SQLGetDescField(descriptor, 1, SQL_DESC_DATETIME_INTERVAL_CODE, 0, 0, 0));

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[kOdbcBufferSize];
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetDiagRec(SQL_HANDLE_DESC, descriptor, 1, sql_state, &native_error,
                          message, kOdbcBufferSize, &message_length));

  EXPECT_GT(message_length, 60);

  EXPECT_EQ(0, native_error);

  // API not implemented error from driver manager
  EXPECT_EQ(std::wstring(L"IM001"), std::wstring(sql_state));

  EXPECT_TRUE(!std::wstring(message).empty());

  // Free descriptor handle
  EXPECT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

}  // namespace arrow::flight::sql::odbc
