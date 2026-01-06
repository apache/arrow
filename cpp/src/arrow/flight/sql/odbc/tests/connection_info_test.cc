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

namespace {
// Helper Functions

// Get SQLUSMALLINT return value
void GetInfo(SQLHDBC connection, SQLUSMALLINT info_type, SQLUSMALLINT* value) {
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(connection, info_type, value, 0, &message_length));
}

// Get SQLUINTEGER return value
void GetInfo(SQLHDBC connection, SQLUSMALLINT info_type, SQLUINTEGER* value) {
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(connection, info_type, value, 0, &message_length));
}

// Get SQLULEN return value
void GetInfo(SQLHDBC connection, SQLUSMALLINT info_type, SQLULEN* value) {
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(connection, info_type, value, 0, &message_length));
}

// Get SQLWCHAR return value
void GetInfo(SQLHDBC connection, SQLUSMALLINT info_type, SQLWCHAR* value,
             SQLSMALLINT buf_len = kOdbcBufferSize) {
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, value, buf_len, &message_length));
}
}  // namespace

// Test disabled until we resolve bus error on MacOS
#ifdef DISABLE_TEST
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTruncation) {
  static constexpr int info_len = 1;
  SQLWCHAR value[info_len] = L"";
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetInfo(this->conn, SQL_KEYWORDS, value, info_len, &message_length));

  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorState01004);
  EXPECT_GT(message_length, 0);
}
#endif

// Driver Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoActiveEnvironments) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_ACTIVE_ENVIRONMENTS, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncDbcFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ASYNC_DBC_FUNCTIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE), value);
}
#endif

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncMode) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ASYNC_MODE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_AM_NONE), value);
}

#ifdef SQL_ASYNC_NOTIFICATION
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncNotification) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ASYNC_NOTIFICATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE), value);
}
#endif

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBatchRowCount) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_BATCH_ROW_COUNT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBatchSupport) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_BATCH_SUPPORT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDataSourceName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DATA_SOURCE_NAME, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverAwarePoolingSupported) {
  // According to Microsoft documentation, ODBC driver does not need to implement
  // SQL_DRIVER_AWARE_POOLING_SUPPORTED and the Driver Manager will ignore the
  // driver's return value for it.

  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE), value);
}
#endif

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHdbc) {
  // Value returned from driver manager is the connection address
  SQLULEN value;
  GetInfo(this->conn, SQL_DRIVER_HDBC, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHdesc) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  // Value returned from driver manager is the desc address
  SQLHDESC local_desc = descriptor;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(this->conn, SQL_HANDLE_DESC, &local_desc, 0, nullptr));
  EXPECT_GT(local_desc, static_cast<SQLHSTMT>(0));

  // Free descriptor handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHenv) {
  // Value returned from driver manager is the env address
  SQLULEN value;
  GetInfo(this->conn, SQL_DRIVER_HENV, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHlib) {
  SQLULEN value;
  GetInfo(this->conn, SQL_DRIVER_HLIB, &value);

  EXPECT_GT(value, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHstmt) {
  // Value returned from driver manager is the stmt address
  SQLHSTMT local_stmt = this->stmt;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(this->conn, SQL_DRIVER_HSTMT, &local_stmt, 0, nullptr));
  EXPECT_GT(local_stmt, static_cast<SQLHSTMT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DRIVER_NAME, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"Arrow Flight ODBC Driver"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverOdbcVer) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DRIVER_ODBC_VER, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"03.80"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverVer) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DRIVER_VER, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"00.09.0000.0"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDynamicCursorAttributes1) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDynamicCursorAttributes2) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoForwardOnlyCursorAttributes1) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_CA1_NEXT), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoForwardOnlyCursorAttributes2) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoFileUsage) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_FILE_USAGE, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoGetDataExtensions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_GETDATA_EXTENSIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSchemaViews) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_INFO_SCHEMA_VIEWS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS),
            value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeysetCursorAttributes1) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeysetCursorAttributes2) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxAsyncConcurrentStatements) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxConcurrentActivities) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxDriverConnections) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_DRIVER_CONNECTIONS, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoOdbcInterfaceConformance) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_OIC_CORE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoOdbcVer) {
  // This is implemented only in the Driver Manager.

  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_ODBC_VER, value);

#ifdef __APPLE__
  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"03.52.0000"), value);
#else
  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"03.80.0000"), value);
#endif  // __APPLE__
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoParamArrayRowCounts) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoParamArraySelects) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_PARAM_ARRAY_SELECTS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoRowUpdates) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_ROW_UPDATES, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSearchPatternEscape) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_SEARCH_PATTERN_ESCAPE, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"\\"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoServerName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_SERVER_NAME, value);

  EXPECT_GT(wcslen(value), 0);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoStaticCursorAttributes1) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoStaticCursorAttributes2) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

// DBMS Product Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDatabaseName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DATABASE_NAME, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDbmsName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DBMS_NAME, value);

  EXPECT_GT(wcslen(value), 0);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDbmsVer) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DBMS_VER, value);

  EXPECT_GT(wcslen(value), 0);
}

// Data Source Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAccessibleProcedures) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_ACCESSIBLE_PROCEDURES, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAccessibleTables) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_ACCESSIBLE_TABLES, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"Y"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBookmarkPersistence) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_BOOKMARK_PERSISTENCE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogTerm) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_CATALOG_TERM, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCollationSeq) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_COLLATION_SEQ, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConcatNullBehavior) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_CONCAT_NULL_BEHAVIOR, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_CB_NULL), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorCommitBehavior) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_CB_CLOSE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorRollbackBehavior) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_CB_CLOSE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorSensitivity) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CURSOR_SENSITIVITY, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_UNSPECIFIED), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDataSourceReadOnly) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DATA_SOURCE_READ_ONLY, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDefaultTxnIsolation) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DEFAULT_TXN_ISOLATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDescribeParameter) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_DESCRIBE_PARAMETER, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMultResultSets) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_MULT_RESULT_SETS, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMultipleActiveTxn) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_MULTIPLE_ACTIVE_TXN, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoNeedLongDataLen) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_NEED_LONG_DATA_LEN, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNullCollation) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_NULL_COLLATION, &value);
  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_NC_START), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoProcedureTerm) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_PROCEDURE_TERM, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSchemaTerm) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_SCHEMA_TERM, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"schema"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoScrollOptions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_SCROLL_OPTIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTableTerm) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_TABLE_TERM, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"table"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTxnCapable) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_TXN_CAPABLE, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_TC_NONE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTxnIsolationOption) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_TXN_ISOLATION_OPTION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoUserName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_USER_NAME, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

// Supported SQL

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAggregateFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_AGGREGATE_FUNCTIONS, &value);

  EXPECT_EQ(value, static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT |
                                            SQL_AF_DISTINCT | SQL_AF_MAX | SQL_AF_MIN |
                                            SQL_AF_SUM));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAlterDomain) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ALTER_DOMAIN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAlterTable) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_ALTER_TABLE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogLocation) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_CATALOG_LOCATION, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogName) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_CATALOG_NAME, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogNameSeparator) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_CATALOG_NAME_SEPARATOR, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCatalogUsage) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CATALOG_USAGE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoColumnAlias) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_COLUMN_ALIAS, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"Y"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCorrelationName) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_CORRELATION_NAME, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_CN_NONE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateAssertion) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_ASSERTION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateCharacterSet) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_CHARACTER_SET, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateCollation) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_COLLATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateDomain) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_DOMAIN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCreateSchema) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_SCHEMA, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(1), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCreateTable) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_TABLE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(1), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateTranslation) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CREATE_TRANSLATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDdlIndex) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DDL_INDEX, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropAssertion) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_ASSERTION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropCharacterSet) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_CHARACTER_SET, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropCollation) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_COLLATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropDomain) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_DOMAIN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropSchema) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_SCHEMA, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropTable) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_TABLE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropTranslation) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_TRANSLATION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropView) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_DROP_VIEW, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoExpressionsInOrderby) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoGroupBy) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_GROUP_BY, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIdentifierCase) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_IDENTIFIER_CASE, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_IC_MIXED), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIdentifierQuoteChar) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"\""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIndexKeywords) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_INDEX_KEYWORDS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_IK_NONE), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoInsertStatement) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_INSERT_STATEMENT, &value);

  EXPECT_EQ(value, static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS |
                                            SQL_IS_INSERT_SEARCHED | SQL_IS_SELECT_INTO));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIntegrity) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_INTEGRITY, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

// Test disabled until we resolve bus error on MacOS
#ifdef DISABLE_TEST
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeywords) {
  // Keyword strings can require 5000 buffer length
  static constexpr int info_len = kOdbcBufferSize * 5;
  SQLWCHAR value[info_len] = L"";
  GetInfo(this->conn, SQL_KEYWORDS, value, info_len);

  EXPECT_GT(wcslen(value), 0);
}
#endif

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoLikeEscapeClause) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_LIKE_ESCAPE_CLAUSE, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"Y"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNonNullableColumns) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_NON_NULLABLE_COLUMNS, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_NNC_NULL), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOjCapabilities) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_OJ_CAPABILITIES, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOrderByColumnsInSelect) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"Y"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOuterJoins) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_OUTER_JOINS, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoProcedures) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_PROCEDURES, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoQuotedIdentifierCase) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_QUOTED_IDENTIFIER_CASE, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(SQL_IC_MIXED), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSchemaUsage) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_SCHEMA_USAGE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSpecialCharacters) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_SPECIAL_CHARACTERS, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSqlConformance) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_SQL_CONFORMANCE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSubqueries) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_SUBQUERIES, &value);

  EXPECT_EQ(value,
            static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                     SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoUnion) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_UNION, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL), value);
}

// SQL Limits

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxBinaryLiteralLen) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_MAX_BINARY_LITERAL_LEN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxCatalogNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_CATALOG_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxCharLiteralLen) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_MAX_CHAR_LITERAL_LEN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxColumnNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMN_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInGroupBy) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInIndex) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMNS_IN_INDEX, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInOrderBy) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInSelect) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMNS_IN_SELECT, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInTable) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_COLUMNS_IN_TABLE, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxCursorNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_CURSOR_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxIdentifierLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_IDENTIFIER_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(65535), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxIndexSize) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_MAX_INDEX_SIZE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxProcedureNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxRowSize) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_MAX_ROW_SIZE, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L""), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxRowSizeIncludesLong) {
  SQLWCHAR value[kOdbcBufferSize] = L"";
  GetInfo(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG, value);

  EXPECT_STREQ(static_cast<const SQLWCHAR*>(L"N"), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxSchemaNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_SCHEMA_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxStatementLen) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_MAX_STATEMENT_LEN, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxTableNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_TABLE_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxTablesInSelect) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_TABLES_IN_SELECT, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxUserNameLen) {
  SQLUSMALLINT value;
  GetInfo(this->conn, SQL_MAX_USER_NAME_LEN, &value);

  EXPECT_EQ(static_cast<SQLUSMALLINT>(0), value);
}

// Scalar Function Information

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_FUNCTIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNumericFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_NUMERIC_FUNCTIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(4058942), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoStringFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_STRING_FUNCTIONS, &value);

  EXPECT_EQ(value, static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                            SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSystemFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_SYSTEM_FUNCTIONS, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTimedateAddIntervals) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_TIMEDATE_ADD_INTERVALS, &value);

  EXPECT_EQ(value, static_cast<SQLUINTEGER>(
                       SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE |
                       SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK |
                       SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTimedateDiffIntervals) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_TIMEDATE_DIFF_INTERVALS, &value);

  EXPECT_EQ(value, static_cast<SQLUINTEGER>(
                       SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND | SQL_FN_TSI_MINUTE |
                       SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY | SQL_FN_TSI_WEEK |
                       SQL_FN_TSI_MONTH | SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoTimedateFunctions) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_TIMEDATE_FUNCTIONS, &value);

  EXPECT_EQ(value,
            static_cast<SQLUINTEGER>(
                SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
                SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME |
                SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK |
                SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
                SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
                SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD |
                SQL_FN_TD_TIMESTAMPDIFF | SQL_FN_TD_WEEK | SQL_FN_TD_YEAR));
}

// Conversion Information

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertBigint) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_BIGINT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(8), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertBinary) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_BINARY, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertBit) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_BIT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertChar) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_CHAR, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertDate) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_DATE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertDecimal) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_DECIMAL, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertDouble) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_DOUBLE, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertFloat) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_FLOAT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertInteger) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_INTEGER, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertIntervalDayTime) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertIntervalYearMonth) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertLongvarbinary) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_LONGVARBINARY, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertLongvarchar) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_LONGVARCHAR, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertNumeric) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_NUMERIC, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertReal) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_REAL, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertSmallint) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_SMALLINT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTime) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_TIME, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTimestamp) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_TIMESTAMP, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTinyint) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_TINYINT, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertVarbinary) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_VARBINARY, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertVarchar) {
  SQLUINTEGER value;
  GetInfo(this->conn, SQL_CONVERT_VARCHAR, &value);

  EXPECT_EQ(static_cast<SQLUINTEGER>(0), value);
}

}  // namespace arrow::flight::sql::odbc
