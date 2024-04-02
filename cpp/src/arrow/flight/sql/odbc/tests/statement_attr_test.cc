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

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/statement.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

// Helper Functions

// Validate SQLULEN return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLULEN expected_value) {
  SQLULEN value = 0;
  SQLINTEGER string_length = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate SQLLEN return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLLEN expected_value) {
  SQLLEN value = 0;
  SQLINTEGER string_length = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate SQLPOINTER return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLPOINTER expected_value) {
  SQLPOINTER value = nullptr;
  SQLINTEGER string_length = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate unsigned length SQLULEN return value is greater than
void ValidateGetStmtAttrGreaterThan(SQLHSTMT statement, SQLINTEGER attribute,
                                    SQLULEN compared_value) {
  SQLULEN value = 0;
  SQLINTEGER string_length_ptr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &string_length_ptr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(value, compared_value);
}

// Validate error return value and code
void ValidateGetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  std::string_view error_code) {
  SQLULEN value = 0;
  SQLINTEGER string_length_ptr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &string_length_ptr);

  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Validate return value for call to SQLSetStmtAttr with SQLULEN
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLULEN new_value) {
  SQLINTEGER string_length_ptr = sizeof(SQLULEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), string_length_ptr);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate return value for call to SQLSetStmtAttr with SQLLEN
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLLEN new_value) {
  SQLINTEGER string_length_ptr = sizeof(SQLLEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), string_length_ptr);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate return value for call to SQLSetStmtAttr with SQLPOINTER
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLPOINTER value) {
  SQLRETURN ret = SQLSetStmtAttr(statement, attribute, value, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate error return value and code
void ValidateSetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  SQLULEN new_value, std::string_view error_code) {
  SQLINTEGER string_length_ptr = sizeof(SQLULEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), string_length_ptr);

  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Test Cases

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppParamDesc) {
  this->Connect();

  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppRowDesc) {
  this->Connect();

  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncEnable) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ASYNC_ENABLE,
                      static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF));

  this->Disconnect();
}

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtEventUnsupported) {
  this->Connect();

  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->Connect();

  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK,
                               error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCContextUnsupported) {
  this->Connect();

  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT,
                               error_state_HYC00);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrConcurrency) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorScrollable) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorSensitivity) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorType) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrEnableAutoIPD) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrFetchBookmarkPointer) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPParamDesc) {
  this->Connect();

  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPRowDesc) {
  this->Connect();

  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrKeysetSize) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxLength) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxRows) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMetadataID) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrNoscan) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindOffsetPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindType) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamOperationPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamStatusPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsProcessedPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsetSize) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrQueryTimeout) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRetrieveData) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowArraySize) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindOffsetPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindType) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowNumber) {
  this->Connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowOperationPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowStatusPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsFetchedPtr) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrSimulateCursor) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrUseBookmarks) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->Disconnect();
}

// This is a pre ODBC 3 attribute
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsetSize) {
  this->Connect();

  ValidateGetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppParamDesc) {
  SQLULEN app_param_desc = 0;
  SQLINTEGER string_length_ptr;
  this->Connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &app_param_desc, 0,
                                 &string_length_ptr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, static_cast<SQLULEN>(0));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                      static_cast<SQLULEN>(app_param_desc));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppRowDesc) {
  SQLULEN app_row_desc = 0;
  SQLINTEGER string_length_ptr;
  this->Connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &app_row_desc, 0,
                                 &string_length_ptr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, static_cast<SQLULEN>(0));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                      static_cast<SQLULEN>(app_row_desc));

  this->Disconnect();
}

#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncEnableUnsupported) {
  this->Connect();

  // Optional feature not implemented
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_ENABLE, SQL_ASYNC_ENABLE_OFF,
                               error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtEventUnsupported) {
  this->Connect();

  // Driver does not support asynchronous notification
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, 0,
                               error_state_HY118);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->Connect();

  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK, 0,
                               error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCContextUnsupported) {
  this->Connect();

  // Optional feature not implemented
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT, 0,
                               error_state_HYC00);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrConcurrency) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorScrollable) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorSensitivity) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorType) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrEnableAutoIPD) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrFetchBookmarkPointer) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPParamDesc) {
  this->Connect();

  // Invalid use of an automatically allocated descriptor handle
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                               static_cast<SQLULEN>(0), error_state_HY017);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPRowDesc) {
  this->Connect();

  // Invalid use of an automatically allocated descriptor handle
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_ROW_DESC, static_cast<SQLULEN>(0),
                               error_state_HY017);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrKeysetSizeUnsupported) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxLength) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxRows) {
  this->Connect();

  // Cannot set read-only attribute
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMetadataID) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrNoscan) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindOffsetPtr) {
  this->Connect();

  SQLULEN offset = 1000;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindType) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamOperationPtr) {
  this->Connect();

  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT param_operations[param_set_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                   SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamStatusPtr) {
  this->Connect();

  // Driver does not support parameters, so just check array can be saved/retrieved
  constexpr SQLULEN param_status_size = 4;
  SQLUSMALLINT param_status[param_status_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                  SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsProcessedPtr) {
  this->Connect();

  SQLULEN processed_count = 0;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsetSize) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrQueryTimeout) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRetrieveData) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowArraySize) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindOffsetPtr) {
  this->Connect();

  SQLULEN offset = 1000;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindType) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowNumber) {
  this->Connect();

  // Cannot set read-only attribute
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowOperationPtr) {
  this->Connect();

  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT row_operations[param_set_size] = {SQL_ROW_PROCEED, SQL_ROW_IGNORE,
                                                 SQL_ROW_PROCEED, SQL_ROW_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowStatusPtr) {
  this->Connect();

  constexpr SQLULEN row_status_size = 4;
  SQLUSMALLINT values[4] = {0, 0, 0, 0};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsFetchedPtr) {
  this->Connect();

  SQLULEN rows_fetched = 1;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrSimulateCursor) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrUseBookmarks) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->Disconnect();
}

// This is a pre ODBC 3 attribute
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsetSize) {
  this->Connect();

  ValidateSetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->Disconnect();
}

}  // namespace arrow::flight::sql::odbc
