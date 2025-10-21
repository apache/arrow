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

#include "arrow/flight/sql/odbc/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/statement.h"

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

template <typename T>
class StatementAttributeTest : public T {
 public:
  using List = std::list<T>;
};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(StatementAttributeTest, TestTypes);

// Helper Functions

// Validate SQLULEN return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLULEN expected_value) {
  SQLULEN value = 0;
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length));

  EXPECT_EQ(expected_value, value);
}

// Validate SQLLEN return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLLEN expected_value) {
  SQLLEN value = 0;
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length));

  EXPECT_EQ(expected_value, value);
}

// Validate SQLPOINTER return value
void ValidateGetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute,
                         SQLPOINTER expected_value) {
  SQLPOINTER value = nullptr;
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, &value, sizeof(value), &string_length));

  EXPECT_EQ(expected_value, value);
}

// Validate unsigned length SQLULEN return value is greater than
void ValidateGetStmtAttrGreaterThan(SQLHSTMT statement, SQLINTEGER attribute,
                                    SQLULEN compared_value) {
  SQLULEN value = 0;
  SQLINTEGER string_length_ptr;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, &value, 0, &string_length_ptr));

  EXPECT_GT(value, compared_value);
}

// Validate error return value and code
void ValidateGetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  std::string_view error_code) {
  SQLULEN value = 0;
  SQLINTEGER string_length_ptr;

  ASSERT_EQ(SQL_ERROR,
            SQLGetStmtAttr(statement, attribute, &value, 0, &string_length_ptr));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Validate return value for call to SQLSetStmtAttr with SQLULEN
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLULEN new_value) {
  SQLINTEGER string_length_ptr = sizeof(SQLULEN);

  EXPECT_EQ(SQL_SUCCESS,
            SQLSetStmtAttr(statement, attribute, reinterpret_cast<SQLPOINTER>(new_value),
                           string_length_ptr));
}

// Validate return value for call to SQLSetStmtAttr with SQLLEN
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLLEN new_value) {
  SQLINTEGER string_length_ptr = sizeof(SQLLEN);

  EXPECT_EQ(SQL_SUCCESS,
            SQLSetStmtAttr(statement, attribute, reinterpret_cast<SQLPOINTER>(new_value),
                           string_length_ptr));
}

// Validate return value for call to SQLSetStmtAttr with SQLPOINTER
void ValidateSetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLPOINTER value) {
  EXPECT_EQ(SQL_SUCCESS, SQLSetStmtAttr(statement, attribute, value, 0));
}

// Validate error return value and code
void ValidateSetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  SQLULEN new_value, std::string_view error_code) {
  SQLINTEGER string_length_ptr = sizeof(SQLULEN);

  ASSERT_EQ(SQL_ERROR,
            SQLSetStmtAttr(statement, attribute, reinterpret_cast<SQLPOINTER>(new_value),
                           string_length_ptr));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}

// Test Cases

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAppParamDesc) {
  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAppRowDesc) {
  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_APP_ROW_DESC,
                                 static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncEnable) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ASYNC_ENABLE,
                      static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF));
}

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtEventUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, error_state_HYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtPCCallbackUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK,
                               error_state_HYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtPCContextUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT,
                               error_state_HYC00);
}
#endif

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrConcurrency) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorScrollable) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorSensitivity) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorType) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrEnableAutoIPD) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrFetchBookmarkPointer) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrIMPParamDesc) {
  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                                 static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrIMPRowDesc) {
  ValidateGetStmtAttrGreaterThan(this->stmt, SQL_ATTR_IMP_ROW_DESC,
                                 static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrKeysetSize) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMaxLength) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMaxRows) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMetadataID) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrNoscan) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamBindOffsetPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamBindType) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamOperationPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamStatusPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamsProcessedPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamsetSize) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrQueryTimeout) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRetrieveData) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowArraySize) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowBindOffsetPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowBindType) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowNumber) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowOperationPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowStatusPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowsFetchedPtr) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(nullptr));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrSimulateCursor) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrUseBookmarks) {
  ValidateGetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));
}

// This is a pre ODBC 3 attribute
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowsetSize) {
  ValidateGetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAppParamDesc) {
  SQLULEN app_param_desc = 0;
  SQLINTEGER string_length_ptr;

  ASSERT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                        &app_param_desc, 0, &string_length_ptr));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, static_cast<SQLULEN>(0));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                      static_cast<SQLULEN>(app_param_desc));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAppRowDesc) {
  SQLULEN app_row_desc = 0;
  SQLINTEGER string_length_ptr;

  ASSERT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &app_row_desc,
                                        0, &string_length_ptr));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, static_cast<SQLULEN>(0));

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                      static_cast<SQLULEN>(app_row_desc));
}

#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncEnableUnsupported) {
  // Optional feature not implemented
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_ENABLE, SQL_ASYNC_ENABLE_OFF,
                               error_state_HYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtEventUnsupported) {
  // Driver does not support asynchronous notification
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, 0,
                               error_state_HY118);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtPCCallbackUnsupported) {
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK, 0,
                               error_state_HYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtPCContextUnsupported) {
  // Optional feature not implemented
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT, 0,
                               error_state_HYC00);
}
#endif

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrConcurrency) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY,
                      static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrCursorScrollable) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE,
                      static_cast<SQLULEN>(SQL_NONSCROLLABLE));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrCursorSensitivity) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY,
                      static_cast<SQLULEN>(SQL_UNSPECIFIED));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrCursorType) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE,
                      static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrEnableAutoIPD) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD,
                      static_cast<SQLULEN>(SQL_FALSE));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrFetchBookmarkPointer) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, static_cast<SQLLEN>(NULL));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrIMPParamDesc) {
  // Invalid use of an automatically allocated descriptor handle
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_PARAM_DESC,
                               static_cast<SQLULEN>(0), error_state_HY017);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrIMPRowDesc) {
  // Invalid use of an automatically allocated descriptor handle
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_ROW_DESC, static_cast<SQLULEN>(0),
                               error_state_HY017);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrKeysetSizeUnsupported) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrMaxLength) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrMaxRows) {
  // Cannot set read-only attribute
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_MAX_ROWS, static_cast<SQLULEN>(0),
                               error_state_HY092);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrMetadataID) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, static_cast<SQLULEN>(SQL_FALSE));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrNoscan) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, static_cast<SQLULEN>(SQL_NOSCAN_OFF));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamBindOffsetPtr) {
  SQLULEN offset = 1000;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamBindType) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE,
                      static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamOperationPtr) {
  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT param_operations[param_set_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                   SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR,
                      static_cast<SQLPOINTER>(param_operations));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamStatusPtr) {
  // Driver does not support parameters, so just check array can be saved/retrieved
  constexpr SQLULEN param_status_size = 4;
  SQLUSMALLINT param_status[param_status_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                  SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamsProcessedPtr) {
  SQLULEN processed_count = 0;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamsetSize) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrQueryTimeout) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRetrieveData) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA,
                      static_cast<SQLULEN>(SQL_RD_ON));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowArraySize) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, static_cast<SQLULEN>(1));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowBindOffsetPtr) {
  SQLULEN offset = 1000;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR,
                      static_cast<SQLPOINTER>(&offset));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowBindType) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowNumber) {
  // Cannot set read-only attribute
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0),
                               error_state_HY092);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowOperationPtr) {
  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT row_operations[param_set_size] = {SQL_ROW_PROCEED, SQL_ROW_IGNORE,
                                                 SQL_ROW_PROCEED, SQL_ROW_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowStatusPtr) {
  constexpr SQLULEN row_status_size = 4;
  SQLUSMALLINT values[4] = {0, 0, 0, 0};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowsFetchedPtr) {
  SQLULEN rows_fetched = 1;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  ValidateGetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrSimulateCursor) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR,
                      static_cast<SQLULEN>(SQL_SC_UNIQUE));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrUseBookmarks) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS,
                      static_cast<SQLULEN>(SQL_UB_OFF));
}

// This is a pre ODBC 3 attribute
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowsetSize) {
  ValidateSetStmtAttr(this->stmt, SQL_ROWSET_SIZE, static_cast<SQLULEN>(1));
}

}  // namespace arrow::flight::sql::odbc
