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
class StatementAttributeTest : public T {};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(StatementAttributeTest, TestTypes);

namespace {
// Helper Functions

// Get SQLULEN return value
void GetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLULEN* value) {
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, value, sizeof(*value), &string_length));
}

// Get SQLLEN return value
void GetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLLEN* value) {
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, value, sizeof(*value), &string_length));
}

// Get SQLPOINTER return value
void GetStmtAttr(SQLHSTMT statement, SQLINTEGER attribute, SQLPOINTER* value) {
  SQLINTEGER string_length = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(statement, attribute, value, SQL_IS_POINTER, &string_length));
}

#if defined(SQL_ATTR_ASYNC_STMT_EVENT) || defined(SQL_ATTR_ASYNC_STMT_PCALLBACK) || \
    defined(SQL_ATTR_ASYNC_STMT_PCONTEXT)
// Validate error return value and code
void ValidateGetStmtAttrErrorCode(SQLHSTMT statement, SQLINTEGER attribute,
                                  std::string_view error_code) {
  SQLULEN value = 0;
  SQLINTEGER string_length_ptr;

  ASSERT_EQ(SQL_ERROR,
            SQLGetStmtAttr(statement, attribute, &value, 0, &string_length_ptr));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
}
#endif  // SQL_ATTR_ASYNC_STMT_EVENT || SQL_ATTR_ASYNC_STMT_PCALLBACK ||
        // SQL_ATTR_ASYNC_STMT_PCONTEXT

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
                                  SQLULEN new_value, SQLRETURN expected_rc,
                                  std::string_view error_code) {
  SQLINTEGER string_length_ptr = sizeof(SQLULEN);

  ASSERT_EQ(expected_rc,
            SQLSetStmtAttr(statement, attribute, reinterpret_cast<SQLPOINTER>(new_value),
                           string_length_ptr))
      << GetOdbcErrorMessage(SQL_HANDLE_STMT, statement);

  if (expected_rc == SQL_ERROR) {
    VerifyOdbcErrorState(SQL_HANDLE_STMT, statement, error_code);
  }
}
}  // namespace

// Test Cases

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAppParamDesc) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAppRowDesc) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncEnable) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_ASYNC_ENABLE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_ASYNC_ENABLE_OFF), value);
}

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtEventUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtPCCallbackUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK,
                               kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrAsyncStmtPCContextUnsupported) {
  // Optional feature not implemented
  ValidateGetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT,
                               kErrorStateHYC00);
}
#endif

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrConcurrency) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_CONCURRENCY, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_CONCUR_READ_ONLY), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorScrollable) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SCROLLABLE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_NONSCROLLABLE), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorSensitivity) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_CURSOR_SENSITIVITY, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_UNSPECIFIED), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrCursorType) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_CURSOR_TYPE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_CURSOR_FORWARD_ONLY), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrEnableAutoIPD) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_ENABLE_AUTO_IPD, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_FALSE), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrFetchBookmarkPointer) {
  SQLLEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_FETCH_BOOKMARK_PTR, &value);

  EXPECT_EQ(static_cast<SQLLEN>(NULL), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrIMPParamDesc) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_IMP_PARAM_DESC, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrIMPRowDesc) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_IMP_ROW_DESC, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrKeysetSize) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_KEYSET_SIZE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(0), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMaxLength) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_MAX_LENGTH, &value);

  EXPECT_EQ(static_cast<SQLULEN>(0), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMaxRows) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_MAX_ROWS, &value);

  EXPECT_EQ(static_cast<SQLULEN>(0), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrMetadataID) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_METADATA_ID, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_FALSE), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrNoscan) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_NOSCAN, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_NOSCAN_OFF), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamBindOffsetPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamBindType) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_TYPE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_PARAM_BIND_BY_COLUMN), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamOperationPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamStatusPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamsProcessedPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrParamsetSize) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAMSET_SIZE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(1), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrQueryTimeout) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_QUERY_TIMEOUT, &value);

  EXPECT_EQ(static_cast<SQLULEN>(0), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRetrieveData) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_RETRIEVE_DATA, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_RD_ON), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowArraySize) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(1), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowBindOffsetPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowBindType) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(0), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowNumber) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_NUMBER, &value);

  EXPECT_EQ(static_cast<SQLULEN>(1), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowOperationPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowStatusPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowsFetchedPtr) {
  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(nullptr), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrSimulateCursor) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_SIMULATE_CURSOR, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_SC_UNIQUE), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrUseBookmarks) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ATTR_USE_BOOKMARKS, &value);

  EXPECT_EQ(static_cast<SQLULEN>(SQL_UB_OFF), value);
}

// This is a pre ODBC 3 attribute
TYPED_TEST(StatementAttributeTest, TestSQLGetStmtAttrRowsetSize) {
  SQLULEN value;
  GetStmtAttr(this->stmt, SQL_ROWSET_SIZE, &value);

  EXPECT_EQ(static_cast<SQLULEN>(1), value);
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
                               SQL_ERROR, kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_EVENT
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtEventUnsupported) {
  // Driver does not support asynchronous notification
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_EVENT, 0, SQL_ERROR,
                               kErrorStateHY118);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtPCCallbackUnsupported) {
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCALLBACK, 0, SQL_ERROR,
                               kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrAsyncStmtPCContextUnsupported) {
  // Optional feature not implemented
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ASYNC_STMT_PCONTEXT, 0, SQL_ERROR,
                               kErrorStateHYC00);
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
                               static_cast<SQLULEN>(0),
#ifdef __APPLE__
                               // iODBC on MacOS returns SQL_INVALID_HANDLE for this case
                               SQL_INVALID_HANDLE,
#else
                               SQL_ERROR,
#endif
                               kErrorStateHY017);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrIMPRowDesc) {
  // Invalid use of an automatically allocated descriptor handle
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_IMP_ROW_DESC, static_cast<SQLULEN>(0),
#ifdef __APPLE__
                               // iODBC on MacOS returns SQL_INVALID_HANDLE for this case
                               SQL_INVALID_HANDLE,
#else
                               SQL_ERROR,
#endif
                               kErrorStateHY017);
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
                               SQL_ERROR, kErrorStateHY092);
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

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_BIND_OFFSET_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(&offset), value);
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

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_OPERATION_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(param_operations), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamStatusPtr) {
  // Driver does not support parameters, so just check array can be saved/retrieved
  constexpr SQLULEN param_status_size = 4;
  SQLUSMALLINT param_status[param_status_size] = {SQL_PARAM_PROCEED, SQL_PARAM_IGNORE,
                                                  SQL_PARAM_PROCEED, SQL_PARAM_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR,
                      static_cast<SQLPOINTER>(param_status));

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAM_STATUS_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(param_status), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrParamsProcessedPtr) {
  SQLULEN processed_count = 0;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
                      static_cast<SQLPOINTER>(&processed_count));

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(&processed_count), value);
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

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_OFFSET_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(&offset), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowBindType) {
  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_BIND_TYPE, static_cast<SQLULEN>(0));
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowNumber) {
  // Cannot set read-only attribute
  ValidateSetStmtAttrErrorCode(this->stmt, SQL_ATTR_ROW_NUMBER, static_cast<SQLULEN>(0),
                               SQL_ERROR, kErrorStateHY092);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowOperationPtr) {
  constexpr SQLULEN param_set_size = 4;
  SQLUSMALLINT row_operations[param_set_size] = {SQL_ROW_PROCEED, SQL_ROW_IGNORE,
                                                 SQL_ROW_PROCEED, SQL_ROW_IGNORE};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR,
                      static_cast<SQLPOINTER>(row_operations));

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_OPERATION_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(row_operations), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowStatusPtr) {
  constexpr SQLULEN row_status_size = 4;
  SQLUSMALLINT values[row_status_size] = {0, 0, 0, 0};

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR,
                      static_cast<SQLPOINTER>(values));

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROW_STATUS_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(values), value);
}

TYPED_TEST(StatementAttributeTest, TestSQLSetStmtAttrRowsFetchedPtr) {
  SQLULEN rows_fetched = 1;

  ValidateSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR,
                      static_cast<SQLPOINTER>(&rows_fetched));

  SQLPOINTER value = nullptr;
  GetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &value);

  EXPECT_EQ(static_cast<SQLPOINTER>(&rows_fetched), value);
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
