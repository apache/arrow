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
void validateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLULEN expected_value) {
  SQLULEN value = 0;
  SQLINTEGER stringLength = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &stringLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate SQLLEN return value
void validateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLLEN expected_value) {
  SQLLEN value = 0;
  SQLINTEGER stringLength = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &stringLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate SQLPOINTER return value
void validateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLPOINTER expected_value) {
  SQLPOINTER value = nullptr;
  SQLINTEGER stringLength = 0;

  SQLRETURN ret =
      SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &stringLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, expected_value);
}

// Validate unsigned length SQLULEN return value is greater than
void validateGetStmtAttrGreaterThan(SQLHSTMT statement, SQLINTEGER attribute,
                                    SQLULEN compared_value) {
  SQLULEN value = 0;
  SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(value, compared_value);
}

// Validate error return value and code
void validateGetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  std::string_view error_code) {
  SQLULEN value = 0;
  SQLINTEGER stringLengthPtr;

  SQLRETURN ret = SQLGetStmtAttr(statement, attribute, &value, 0, &stringLengthPtr);

  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Validate return value for call to SQLSetStmtAttr with SQLULEN
void validateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLULEN new_value) {
  SQLINTEGER stringLengthPtr = sizeof(SQLULEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate return value for call to SQLSetStmtAttr with SQLLEN
void validateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLLEN new_value) {
  SQLINTEGER stringLengthPtr = sizeof(SQLLEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate return value for call to SQLSetStmtAttr with SQLPOINTER
void validateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLPOINTER value) {
  SQLRETURN ret = SQLSetStmtAttr(statement, attribute, value, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

// Validate error return value and code
void validateSetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  SQLULEN new_value, std::string_view error_code) {
  SQLINTEGER stringLengthPtr = sizeof(SQLULEN);

  SQLRETURN ret = SQLSetStmtAttr(
      statement, attribute, reinterpret_cast<SQLPOINTER>(new_value), stringLengthPtr);

  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Test Cases

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppParamDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAppRowDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncEnable) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ASYNC_ENABLE,
                      static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF));

  this->disconnect();
}

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtEventUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, error_state_HYC00);

  this->disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK,
                               error_state_HYC00);

  this->disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrAsyncStmtPCContextUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT,
                               error_state_HYC00);

  this->disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrConcurrency) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorScrollable) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorSensitivity) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrCursorType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrEnableAutoIPD) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrFetchBookmarkPointer) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPParamDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrIMPRowDesc) {
  this->connect();

  validateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_ROW_DESC,
                                 static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrKeysetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxLength) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMaxRows) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrMetadataID) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrNoscan) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindOffsetPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamBindType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamOperationPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamStatusPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsProcessedPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrParamsetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrQueryTimeout) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRetrieveData) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowArraySize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindOffsetPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowBindType) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowNumber) {
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowOperationPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowStatusPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsFetchedPtr) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(nullptr));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrSimulateCursor) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrUseBookmarks) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->disconnect();
}

// This is a pre ODBC 3 attribute
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetStmtAttrRowsetSize) {
  this->connect();

  validateGetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppParamDesc) {
  SQLULEN app_param_desc = 0;
  SQLINTEGER stringLengthPtr;
  this->connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &app_param_desc, 0,
                                 &stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  validateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, static_cast<SQLULEN>(0));

  validateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                      static_cast<SQLULEN>(app_param_desc));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAppRowDesc) {
  SQLULEN app_row_desc = 0;
  SQLINTEGER stringLengthPtr;
  this->connect();

  SQLRETURN ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &app_row_desc, 0,
                                 &stringLengthPtr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  validateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, static_cast<SQLULEN>(0));

  validateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                      static_cast<SQLULEN>(app_row_desc));

  this->disconnect();
}

#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncEnableUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_ENABLE, SQL_ASYNC_ENABLE_OFF,
                               error_state_HYC00);

  this->disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtEventUnsupported) {
  this->connect();

  // Driver does not support asynchronous notification
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, 0,
                               error_state_HY118);

  this->disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCCallbackUnsupported) {
  this->connect();

  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK, 0,
                               error_state_HYC00);

  this->disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrAsyncStmtPCContextUnsupported) {
  this->connect();

  // Optional feature not implemented
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT, 0,
                               error_state_HYC00);

  this->disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrConcurrency) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorScrollable) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorSensitivity) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrCursorType) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrEnableAutoIPD) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrFetchBookmarkPointer) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPParamDesc) {
  this->connect();

  // Invalid use of an automatically allocated descriptor handle
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                               static_cast<SQLULEN>(0), error_state_HY017);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrIMPRowDesc) {
  this->connect();

  // Invalid use of an automatically allocated descriptor handle
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_ROW_DESC, static_cast<SQLULEN>(0),
                               error_state_HY017);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrKeysetSizeUnsupported) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxLength) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMaxRows) {
  this->connect();

  // Cannot set read-only attribute
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrMetadataID) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrNoscan) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindOffsetPtr) {
  this->connect();

  SQLULEN offset = 1000;

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamBindType) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamOperationPtr) {
  this->connect();

  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT param_operations[param_set_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                   SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamStatusPtr) {
  this->connect();

  // Driver does not support parameters, so just check array can be saved/retrieved
  constexpr SQLULEN param_status_size = 4;
  SQLUSMALLINT param_status[param_status_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                  SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsProcessedPtr) {
  this->connect();

  SQLULEN processed_count = 0;

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  validateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrParamsetSize) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrQueryTimeout) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRetrieveData) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowArraySize) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindOffsetPtr) {
  this->connect();

  SQLULEN offset = 1000;

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowBindType) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowNumber) {
  this->connect();

  // Cannot set read-only attribute
  validateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0),
                               error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowOperationPtr) {
  this->connect();

  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT row_operations[param_set_size] = {SQL_ROW_PROCEED, SQL_ROW_IGNORE,
                                                 SQL_ROW_PROCEED, SQL_ROW_IGNORE};

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowStatusPtr) {
  this->connect();

  constexpr SQLULEN row_status_size = 4;
  SQLUSMALLINT values[4] = {0, 0, 0, 0};

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsFetchedPtr) {
  this->connect();

  SQLULEN rows_fetched = 1;

  validateSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  validateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrSimulateCursor) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrUseBookmarks) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));

  this->disconnect();
}

// This is a pre ODBC 3 attribute
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrRowsetSize) {
  this->connect();

  validateSetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
