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

// Helper Functions

// Validate unsigned short SQLUSMALLINT return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLUSMALLINT expected_value) {
  SQLUSMALLINT info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate unsigned long SQLUINTEGER return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLUINTEGER expected_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate unsigned length SQLULEN return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLULEN expected_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_EQ(expected_value, info_value);
}

// Validate wchar string SQLWCHAR return value
void Validate(SQLHDBC connection, SQLUSMALLINT info_type, SQLWCHAR* expected_value) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(connection, info_type, info_value, ODBC_BUFFER_SIZE,
                                    &message_length));

  EXPECT_EQ(*expected_value, *info_value);
}

// Validate unsigned long SQLUINTEGER return value is greater than
void ValidateGreaterThan(SQLHDBC connection, SQLUSMALLINT info_type,
                         SQLUINTEGER compared_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_GT(info_value, compared_value);
}

// Validate unsigned length SQLULEN return value is greater than
void ValidateGreaterThan(SQLHDBC connection, SQLUSMALLINT info_type,
                         SQLULEN compared_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetInfo(connection, info_type, &info_value, 0, &message_length));

  EXPECT_GT(info_value, compared_value);
}

// Validate wchar string SQLWCHAR return value is not empty
void ValidateNotEmptySQLWCHAR(SQLHDBC connection, SQLUSMALLINT info_type,
                              bool allow_truncation) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, info_type, info_value, ODBC_BUFFER_SIZE, &message_length);
  if (allow_truncation && ret == SQL_SUCCESS_WITH_INFO) {
    ASSERT_EQ(SQL_SUCCESS_WITH_INFO, ret);
  } else {
    ASSERT_EQ(SQL_SUCCESS, ret);
  }

  EXPECT_GT(wcslen(info_value), 0);
}

// Driver Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoActiveEnvironments) {
  this->Connect();

  Validate(this->conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncDbcFunctions) {
  this->Connect();

  Validate(this->conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncMode) {
  this->Connect();

  Validate(this->conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));

  this->Disconnect();
}

#ifdef SQL_ASYNC_NOTIFICATION
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncNotification) {
  this->Connect();

  Validate(this->conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchRowCount) {
  this->Connect();

  Validate(this->conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchSupport) {
  this->Connect();

  Validate(this->conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceName) {
  this->Connect();

  Validate(this->conn, SQL_DATA_SOURCE_NAME, (SQLWCHAR*)L"");

  this->Disconnect();
}

#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverAwarePoolingSupported) {
  // A driver does not need to implement SQL_DRIVER_AWARE_POOLING_SUPPORTED and the
  // Driver Manager will not honor to the driver's return value.
  this->Connect();

  Validate(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));

  this->Disconnect();
}
#endif

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdbc) {
  this->Connect();

  // Value returned from driver manager is the connection address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));

  this->Disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdesc) {
  this->Connect();

  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  // Value returned from driver manager is the desc address
  SQLHDESC local_desc = descriptor;
  SQLRETURN ret = SQLGetInfo(this->conn, SQL_HANDLE_DESC, &local_desc, 0, 0);
  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_GT(local_desc, static_cast<SQLHSTMT>(0));

  // Free descriptor handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));

  this->Disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHenv) {
  this->Connect();

  // Value returned from driver manager is the env address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));

  this->Disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHlib) {
  this->Connect();

  ValidateGreaterThan(this->conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));

  this->Disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHstmt) {
  this->Connect();

  // Value returned from driver manager is the stmt address
  SQLHSTMT local_stmt = this->stmt;
  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(this->conn, SQL_DRIVER_HSTMT, &local_stmt, 0, 0));
  EXPECT_GT(local_stmt, static_cast<SQLHSTMT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverName) {
  this->Connect();

  Validate(this->conn, SQL_DRIVER_NAME, (SQLWCHAR*)L"Arrow Flight ODBC Driver");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverOdbcVer) {
  this->Connect();

  Validate(this->conn, SQL_DRIVER_ODBC_VER, (SQLWCHAR*)L"03.80");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverVer) {
  this->Connect();

  Validate(this->conn, SQL_DRIVER_VER, (SQLWCHAR*)L"00.09.0000.0");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes1) {
  this->Connect();

  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes2) {
  this->Connect();

  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes1) {
  this->Connect();

  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes2) {
  this->Connect();

  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoFileUsage) {
  this->Connect();

  Validate(this->conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoGetDataExtensions) {
  this->Connect();

  Validate(this->conn, SQL_GETDATA_EXTENSIONS,
           static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaViews) {
  this->Connect();

  Validate(this->conn, SQL_INFO_SCHEMA_VIEWS,
           static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes1) {
  this->Connect();

  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes2) {
  this->Connect();

  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxAsyncConcurrentStatements) {
  this->Connect();

  Validate(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxConcurrentActivities) {
  this->Connect();

  Validate(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxDriverConnections) {
  this->Connect();

  Validate(this->conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcInterfaceConformance) {
  this->Connect();

  Validate(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE,
           static_cast<SQLUINTEGER>(SQL_OIC_CORE));

  this->Disconnect();
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoOdbcStandardCliConformance) {
  // Type commented out in odbc_connection.cc
  this->Connect();

  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE,
  // static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcVer) {
  // This is implemented only in the Driver Manager.
  this->Connect();

  Validate(this->conn, SQL_ODBC_VER, (SQLWCHAR*)L"03.80.0000");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArrayRowCounts) {
  this->Connect();

  Validate(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS,
           static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArraySelects) {
  this->Connect();

  Validate(this->conn, SQL_PARAM_ARRAY_SELECTS,
           static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoRowUpdates) {
  this->Connect();

  Validate(this->conn, SQL_ROW_UPDATES, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSearchPatternEscape) {
  this->Connect();

  Validate(this->conn, SQL_SEARCH_PATTERN_ESCAPE, (SQLWCHAR*)L"\\");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoServerName) {
  this->Connect();

  ValidateNotEmptySQLWCHAR(this->conn, SQL_SERVER_NAME, false);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes1) {
  this->Connect();

  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes2) {
  this->Connect();

  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

// DBMS Product Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDatabaseName) {
  this->Connect();

  Validate(this->conn, SQL_DATABASE_NAME, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsName) {
  this->Connect();

  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_NAME, false);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsVer) {
  this->Connect();

  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_VER, false);

  this->Disconnect();
}

// Data Source Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleProcedures) {
  this->Connect();

  Validate(this->conn, SQL_ACCESSIBLE_PROCEDURES, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleTables) {
  this->Connect();

  Validate(this->conn, SQL_ACCESSIBLE_TABLES, (SQLWCHAR*)L"Y");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBookmarkPersistence) {
  this->Connect();

  Validate(this->conn, SQL_BOOKMARK_PERSISTENCE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogTerm) {
  this->Connect();

  Validate(this->conn, SQL_CATALOG_TERM, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCollationSeq) {
  this->Connect();

  Validate(this->conn, SQL_COLLATION_SEQ, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConcatNullBehavior) {
  this->Connect();

  Validate(this->conn, SQL_CONCAT_NULL_BEHAVIOR, static_cast<SQLUSMALLINT>(SQL_CB_NULL));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorCommitBehavior) {
  this->Connect();

  Validate(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorRollbackBehavior) {
  this->Connect();

  Validate(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorSensitivity) {
  this->Connect();

  Validate(this->conn, SQL_CURSOR_SENSITIVITY, static_cast<SQLUINTEGER>(SQL_UNSPECIFIED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceReadOnly) {
  this->Connect();

  Validate(this->conn, SQL_DATA_SOURCE_READ_ONLY, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDefaultTxnIsolation) {
  this->Connect();

  Validate(this->conn, SQL_DEFAULT_TXN_ISOLATION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDescribeParameter) {
  this->Connect();

  Validate(this->conn, SQL_DESCRIBE_PARAMETER, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultResultSets) {
  this->Connect();

  Validate(this->conn, SQL_MULT_RESULT_SETS, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultipleActiveTxn) {
  this->Connect();

  Validate(this->conn, SQL_MULTIPLE_ACTIVE_TXN, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoNeedLongDataLen) {
  this->Connect();

  Validate(this->conn, SQL_NEED_LONG_DATA_LEN, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNullCollation) {
  this->Connect();

  Validate(this->conn, SQL_NULL_COLLATION, static_cast<SQLUSMALLINT>(SQL_NC_START));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoProcedureTerm) {
  this->Connect();

  Validate(this->conn, SQL_PROCEDURE_TERM, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaTerm) {
  this->Connect();

  Validate(this->conn, SQL_SCHEMA_TERM, (SQLWCHAR*)L"schema");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoScrollOptions) {
  this->Connect();

  Validate(this->conn, SQL_SCROLL_OPTIONS, static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTableTerm) {
  this->Connect();

  Validate(this->conn, SQL_TABLE_TERM, (SQLWCHAR*)L"table");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnCapable) {
  this->Connect();

  Validate(this->conn, SQL_TXN_CAPABLE, static_cast<SQLUSMALLINT>(SQL_TC_NONE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnIsolationOption) {
  this->Connect();

  Validate(this->conn, SQL_TXN_ISOLATION_OPTION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoUserName) {
  this->Connect();

  Validate(this->conn, SQL_USER_NAME, (SQLWCHAR*)L"");

  this->Disconnect();
}

// Supported SQL

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAggregateFunctions) {
  this->Connect();

  Validate(
      this->conn, SQL_AGGREGATE_FUNCTIONS,
      static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                               SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterDomain) {
  this->Connect();

  Validate(this->conn, SQL_ALTER_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoAlterSchema) {
  // Type commented out in odbc_connection.cc
  this->Connect();

  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ALTER_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterTable) {
  this->Connect();

  Validate(this->conn, SQL_ALTER_TABLE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, DISABLED_TestSQLGetInfoAnsiSqlDatetimeLiterals) {
  // Type commented out in odbc_connection.cc
  this->Connect();

  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ANSI_SQL_DATETIME_LITERALS, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogLocation) {
  this->Connect();

  Validate(this->conn, SQL_CATALOG_LOCATION, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogName) {
  this->Connect();

  Validate(this->conn, SQL_CATALOG_NAME, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogNameSeparator) {
  this->Connect();

  Validate(this->conn, SQL_CATALOG_NAME_SEPARATOR, (SQLWCHAR*)L"");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCatalogUsage) {
  this->Connect();

  Validate(this->conn, SQL_CATALOG_USAGE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoColumnAlias) {
  this->Connect();

  Validate(this->conn, SQL_COLUMN_ALIAS, (SQLWCHAR*)L"Y");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCorrelationName) {
  this->Connect();

  Validate(this->conn, SQL_CORRELATION_NAME, static_cast<SQLUSMALLINT>(SQL_CN_NONE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateAssertion) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCharacterSet) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCollation) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_COLLATION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateDomain) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateSchema) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_SCHEMA, static_cast<SQLUINTEGER>(1));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateTable) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_TABLE, static_cast<SQLUINTEGER>(1));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateTranslation) {
  this->Connect();

  Validate(this->conn, SQL_CREATE_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDdlIndex) {
  this->Connect();

  Validate(this->conn, SQL_DDL_INDEX, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropAssertion) {
  this->Connect();

  Validate(this->conn, SQL_DROP_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCharacterSet) {
  this->Connect();

  Validate(this->conn, SQL_DROP_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCollation) {
  this->Connect();

  Validate(this->conn, SQL_DROP_COLLATION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropDomain) {
  this->Connect();

  Validate(this->conn, SQL_DROP_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropSchema) {
  this->Connect();

  Validate(this->conn, SQL_DROP_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTable) {
  this->Connect();

  Validate(this->conn, SQL_DROP_TABLE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTranslation) {
  this->Connect();

  Validate(this->conn, SQL_DROP_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropView) {
  this->Connect();

  Validate(this->conn, SQL_DROP_VIEW, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoExpressionsInOrderby) {
  this->Connect();

  Validate(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoGroupBy) {
  this->Connect();

  Validate(this->conn, SQL_GROUP_BY,
           static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierCase) {
  this->Connect();

  Validate(this->conn, SQL_IDENTIFIER_CASE, static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierQuoteChar) {
  this->Connect();

  Validate(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, (SQLWCHAR*)L"\"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIndexKeywords) {
  this->Connect();

  Validate(this->conn, SQL_INDEX_KEYWORDS, static_cast<SQLUINTEGER>(SQL_IK_NONE));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoInsertStatement) {
  this->Connect();

  Validate(this->conn, SQL_INSERT_STATEMENT,
           static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED |
                                    SQL_IS_SELECT_INTO));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIntegrity) {
  this->Connect();

  Validate(this->conn, SQL_INTEGRITY, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeywords) {
  this->Connect();

  ValidateNotEmptySQLWCHAR(this->conn, SQL_KEYWORDS, true);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoLikeEscapeClause) {
  this->Connect();

  Validate(this->conn, SQL_LIKE_ESCAPE_CLAUSE, (SQLWCHAR*)L"Y");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNonNullableColumns) {
  this->Connect();

  Validate(this->conn, SQL_NON_NULLABLE_COLUMNS, static_cast<SQLUSMALLINT>(SQL_NNC_NULL));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOjCapabilities) {
  this->Connect();

  Validate(this->conn, SQL_OJ_CAPABILITIES,
           static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOrderByColumnsInSelect) {
  this->Connect();

  Validate(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT, (SQLWCHAR*)L"Y");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOuterJoins) {
  this->Connect();

  Validate(this->conn, SQL_OUTER_JOINS, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoProcedures) {
  this->Connect();

  Validate(this->conn, SQL_PROCEDURES, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoQuotedIdentifierCase) {
  this->Connect();

  Validate(this->conn, SQL_QUOTED_IDENTIFIER_CASE,
           static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSchemaUsage) {
  this->Connect();

  Validate(this->conn, SQL_SCHEMA_USAGE, static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSpecialCharacters) {
  this->Connect();

  Validate(this->conn, SQL_SPECIAL_CHARACTERS, (SQLWCHAR*)L"");

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSqlConformance) {
  this->Connect();

  Validate(this->conn, SQL_SQL_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSubqueries) {
  this->Connect();

  Validate(this->conn, SQL_SUBQUERIES,
           static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                    SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoUnion) {
  this->Connect();

  Validate(this->conn, SQL_UNION,
           static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL));

  this->Disconnect();
}

// SQL Limits

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxBinaryLiteralLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_BINARY_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCatalogNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_CATALOG_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxCharLiteralLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_CHAR_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxColumnNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMN_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInGroupBy) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInIndex) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMNS_IN_INDEX, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInOrderBy) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInSelect) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMNS_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInTable) {
  this->Connect();

  Validate(this->conn, SQL_MAX_COLUMNS_IN_TABLE, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCursorNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_CURSOR_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIdentifierLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_IDENTIFIER_LEN, static_cast<SQLUSMALLINT>(65535));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIndexSize) {
  this->Connect();

  Validate(this->conn, SQL_MAX_INDEX_SIZE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxProcedureNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxRowSize) {
  this->Connect();

  Validate(this->conn, SQL_MAX_ROW_SIZE, (SQLWCHAR*)L"");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxRowSizeIncludesLong) {
  this->Connect();

  Validate(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG, (SQLWCHAR*)L"N");

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxSchemaNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_SCHEMA_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxStatementLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_STATEMENT_LEN, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxTableNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_TABLE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxTablesInSelect) {
  this->Connect();

  Validate(this->conn, SQL_MAX_TABLES_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxUserNameLen) {
  this->Connect();

  Validate(this->conn, SQL_MAX_USER_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->Disconnect();
}

// Scalar Function Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFunctions) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_FUNCTIONS, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNumericFunctions) {
  this->Connect();

  Validate(this->conn, SQL_NUMERIC_FUNCTIONS, static_cast<SQLUINTEGER>(4058942));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoStringFunctions) {
  this->Connect();

  Validate(this->conn, SQL_STRING_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                    SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSystemFunctions) {
  this->Connect();

  Validate(this->conn, SQL_SYSTEM_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateAddIntervals) {
  this->Connect();

  Validate(this->conn, SQL_TIMEDATE_ADD_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateDiffIntervals) {
  this->Connect();

  Validate(this->conn, SQL_TIMEDATE_DIFF_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoTimedateFunctions) {
  this->Connect();

  Validate(this->conn, SQL_TIMEDATE_FUNCTIONS,
           static_cast<SQLUINTEGER>(
               SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
               SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME |
               SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK |
               SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
               SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
               SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD |
               SQL_FN_TD_TIMESTAMPDIFF | SQL_FN_TD_WEEK | SQL_FN_TD_YEAR));

  this->Disconnect();
}

// Conversion Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBigint) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_BIGINT, static_cast<SQLUINTEGER>(8));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBinary) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_BINARY, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertBit) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_BIT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertChar) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_CHAR, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDate) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_DATE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDecimal) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_DECIMAL, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertDouble) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_DOUBLE, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFloat) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_FLOAT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertInteger) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_INTEGER, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertIntervalDayTime) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertIntervalYearMonth) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarbinary) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_LONGVARBINARY, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarchar) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_LONGVARCHAR, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertNumeric) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_NUMERIC, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertReal) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_REAL, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertSmallint) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_SMALLINT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTime) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_TIME, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTimestamp) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_TIMESTAMP, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTinyint) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_TINYINT, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarbinary) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_VARBINARY, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarchar) {
  this->Connect();

  Validate(this->conn, SQL_CONVERT_VARCHAR, static_cast<SQLUINTEGER>(0));

  this->Disconnect();
}

}  // namespace arrow::flight::sql::odbc
