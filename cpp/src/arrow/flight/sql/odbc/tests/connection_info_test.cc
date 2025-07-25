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

// Helper Functions

// Validate unsigned short SQLUSMALLINT return value
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLUSMALLINT expected_value) {
  SQLUSMALLINT info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// Validate unsigned long SQLUINTEGER return value
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLUINTEGER expected_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// Validate unsigned length SQLULEN return value
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLULEN expected_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(info_value, expected_value);
}

// Validate wchar string SQLWCHAR return value
void validate(SQLHDBC connection, SQLUSMALLINT infoType, SQLWCHAR* expected_value) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, infoType, info_value, ODBC_BUFFER_SIZE, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(*info_value, *expected_value);
}

// Validate unsigned long SQLUINTEGER return value is greater than
void validateGreaterThan(SQLHDBC connection, SQLUSMALLINT infoType,
                         SQLUINTEGER compared_value) {
  SQLUINTEGER info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(info_value, compared_value);
}

// Validate unsigned length SQLULEN return value is greater than
void validateGreaterThan(SQLHDBC connection, SQLUSMALLINT infoType,
                         SQLULEN compared_value) {
  SQLULEN info_value;
  SQLSMALLINT message_length;

  SQLRETURN ret = SQLGetInfo(connection, infoType, &info_value, 0, &message_length);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_GT(info_value, compared_value);
}

// Validate wchar string SQLWCHAR return value is not empty
void validateNotEmptySQLWCHAR(SQLHDBC connection, SQLUSMALLINT infoType,
                              bool allowTruncation) {
  SQLWCHAR info_value[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT message_length;

  SQLRETURN ret =
      SQLGetInfo(connection, infoType, info_value, ODBC_BUFFER_SIZE, &message_length);

  if (allowTruncation && ret == SQL_SUCCESS_WITH_INFO) {
    EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  } else {
    EXPECT_EQ(ret, SQL_SUCCESS);
  }

  EXPECT_GT(wcslen(info_value), 0);
}

// Driver Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoActiveEnvironments) {
  this->connect();

  validate(this->conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncDbcFunctions) {
  this->connect();

  validate(this->conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));

  this->disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncMode) {
  this->connect();

  validate(this->conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));

  this->disconnect();
}

#ifdef SQL_ASYNC_NOTIFICATION
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAsyncNotification) {
  this->connect();

  validate(this->conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));

  this->disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchRowCount) {
  this->connect();

  validate(this->conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBatchSupport) {
  this->connect();

  validate(this->conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceName) {
  this->connect();

  validate(this->conn, SQL_DATA_SOURCE_NAME, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverAwarePoolingSupported) {
  // A driver does not need to implement SQL_DRIVER_AWARE_POOLING_SUPPORTED and the
  // Driver Manager will not honor to the driver's return value.
  this->connect();

  validate(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdbc) {
  this->connect();

  // Value returned from driver manager is the connection address
  validateGreaterThan(this->conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHdesc) {
  // TODO This is failing due to no descriptor being created
  // enable after SQL_HANDLE_DESC is supported
  GTEST_SKIP();
  this->connect();

  validate(this->conn, SQL_DRIVER_HDESC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHenv) {
  this->connect();

  // Value returned from driver manager is the env address
  validateGreaterThan(this->conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHlib) {
  this->connect();

  validateGreaterThan(this->conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverHstmt) {
  this->connect();

  // Value returned from driver manager is the stmt address
  SQLHSTMT local_stmt = this->stmt;
  SQLRETURN ret = SQLGetInfo(this->conn, SQL_DRIVER_HSTMT, &local_stmt, 0, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_GT(local_stmt, static_cast<SQLHSTMT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverName) {
  this->connect();

  validate(this->conn, SQL_DRIVER_NAME, L"Arrow Flight ODBC Driver");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverOdbcVer) {
  this->connect();

  validate(this->conn, SQL_DRIVER_ODBC_VER, L"03.80");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDriverVer) {
  this->connect();

  validate(this->conn, SQL_DRIVER_VER, L"00.09.0000.0");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes1) {
  this->connect();

  validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDynamicCursorAttributes2) {
  this->connect();

  validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes1) {
  this->connect();

  validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoForwardOnlyCursorAttributes2) {
  this->connect();

  validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoFileUsage) {
  this->connect();

  validate(this->conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoGetDataExtensions) {
  this->connect();

  validate(this->conn, SQL_GETDATA_EXTENSIONS,
           static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaViews) {
  this->connect();

  validate(this->conn, SQL_INFO_SCHEMA_VIEWS,
           static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes1) {
  this->connect();

  validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeysetCursorAttributes2) {
  this->connect();

  validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxAsyncConcurrentStatements) {
  this->connect();

  validate(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxConcurrentActivities) {
  this->connect();

  validate(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxDriverConnections) {
  this->connect();

  validate(this->conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcInterfaceConformance) {
  this->connect();

  validate(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE,
           static_cast<SQLUINTEGER>(SQL_OIC_CORE));

  this->disconnect();
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcStandardCliConformance) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE,
  // static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoOdbcVer) {
  // This is implemented only in the Driver Manager.
  this->connect();

  validate(this->conn, SQL_ODBC_VER, L"03.80.0000");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArrayRowCounts) {
  this->connect();

  validate(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS,
           static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoParamArraySelects) {
  this->connect();

  validate(this->conn, SQL_PARAM_ARRAY_SELECTS,
           static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoRowUpdates) {
  this->connect();

  validate(this->conn, SQL_ROW_UPDATES, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSearchPatternEscape) {
  this->connect();

  validate(this->conn, SQL_SEARCH_PATTERN_ESCAPE, L"\\");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoServerName) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_SERVER_NAME, false);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes1) {
  this->connect();

  validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoStaticCursorAttributes2) {
  this->connect();

  validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

// DBMS Product Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDatabaseName) {
  this->connect();

  validate(this->conn, SQL_DATABASE_NAME, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsName) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_DBMS_NAME, false);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDbmsVer) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_DBMS_VER, false);

  this->disconnect();
}

// Data Source Information

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleProcedures) {
  this->connect();

  validate(this->conn, SQL_ACCESSIBLE_PROCEDURES, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAccessibleTables) {
  this->connect();

  validate(this->conn, SQL_ACCESSIBLE_TABLES, L"Y");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoBookmarkPersistence) {
  this->connect();

  validate(this->conn, SQL_BOOKMARK_PERSISTENCE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogTerm) {
  this->connect();

  validate(this->conn, SQL_CATALOG_TERM, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCollationSeq) {
  this->connect();

  validate(this->conn, SQL_COLLATION_SEQ, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConcatNullBehavior) {
  this->connect();

  validate(this->conn, SQL_CONCAT_NULL_BEHAVIOR, static_cast<SQLUSMALLINT>(SQL_CB_NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorCommitBehavior) {
  this->connect();

  validate(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorRollbackBehavior) {
  this->connect();

  validate(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCursorSensitivity) {
  this->connect();

  validate(this->conn, SQL_CURSOR_SENSITIVITY, static_cast<SQLUINTEGER>(SQL_UNSPECIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDataSourceReadOnly) {
  this->connect();

  validate(this->conn, SQL_DATA_SOURCE_READ_ONLY, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDefaultTxnIsolation) {
  this->connect();

  validate(this->conn, SQL_DEFAULT_TXN_ISOLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDescribeParameter) {
  this->connect();

  validate(this->conn, SQL_DESCRIBE_PARAMETER, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultResultSets) {
  this->connect();

  validate(this->conn, SQL_MULT_RESULT_SETS, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMultipleActiveTxn) {
  this->connect();

  validate(this->conn, SQL_MULTIPLE_ACTIVE_TXN, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoNeedLongDataLen) {
  this->connect();

  validate(this->conn, SQL_NEED_LONG_DATA_LEN, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNullCollation) {
  this->connect();

  validate(this->conn, SQL_NULL_COLLATION, static_cast<SQLUSMALLINT>(SQL_NC_START));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoProcedureTerm) {
  this->connect();

  validate(this->conn, SQL_PROCEDURE_TERM, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSchemaTerm) {
  this->connect();

  validate(this->conn, SQL_SCHEMA_TERM, L"schema");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoScrollOptions) {
  this->connect();

  validate(this->conn, SQL_SCROLL_OPTIONS, static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTableTerm) {
  this->connect();

  validate(this->conn, SQL_TABLE_TERM, L"table");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnCapable) {
  this->connect();

  validate(this->conn, SQL_TXN_CAPABLE, static_cast<SQLUSMALLINT>(SQL_TC_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTxnIsolationOption) {
  this->connect();

  validate(this->conn, SQL_TXN_ISOLATION_OPTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoUserName) {
  this->connect();

  validate(this->conn, SQL_USER_NAME, L"");

  this->disconnect();
}

// Supported SQL

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAggregateFunctions) {
  this->connect();

  validate(
      this->conn, SQL_AGGREGATE_FUNCTIONS,
      static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                               SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterDomain) {
  this->connect();

  validate(this->conn, SQL_ALTER_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterSchema) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ALTER_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAlterTable) {
  this->connect();

  validate(this->conn, SQL_ALTER_TABLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoAnsiSqlDatetimeLiterals) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ANSI_SQL_DATETIME_LITERALS, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogLocation) {
  this->connect();

  validate(this->conn, SQL_CATALOG_LOCATION, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogName) {
  this->connect();

  validate(this->conn, SQL_CATALOG_NAME, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCatalogNameSeparator) {
  this->connect();

  validate(this->conn, SQL_CATALOG_NAME_SEPARATOR, L"");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCatalogUsage) {
  this->connect();

  validate(this->conn, SQL_CATALOG_USAGE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoColumnAlias) {
  this->connect();

  validate(this->conn, SQL_COLUMN_ALIAS, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCorrelationName) {
  this->connect();

  validate(this->conn, SQL_CORRELATION_NAME, static_cast<SQLUSMALLINT>(SQL_CN_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateAssertion) {
  this->connect();

  validate(this->conn, SQL_CREATE_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCharacterSet) {
  this->connect();

  validate(this->conn, SQL_CREATE_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateCollation) {
  this->connect();

  validate(this->conn, SQL_CREATE_COLLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateDomain) {
  this->connect();

  validate(this->conn, SQL_CREATE_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateSchema) {
  this->connect();

  validate(this->conn, SQL_CREATE_SCHEMA, static_cast<SQLUINTEGER>(1));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoCreateTable) {
  this->connect();

  validate(this->conn, SQL_CREATE_TABLE, static_cast<SQLUINTEGER>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoCreateTranslation) {
  this->connect();

  validate(this->conn, SQL_CREATE_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDdlIndex) {
  this->connect();

  validate(this->conn, SQL_DDL_INDEX, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropAssertion) {
  this->connect();

  validate(this->conn, SQL_DROP_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCharacterSet) {
  this->connect();

  validate(this->conn, SQL_DROP_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropCollation) {
  this->connect();

  validate(this->conn, SQL_DROP_COLLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropDomain) {
  this->connect();

  validate(this->conn, SQL_DROP_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropSchema) {
  this->connect();

  validate(this->conn, SQL_DROP_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTable) {
  this->connect();

  validate(this->conn, SQL_DROP_TABLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropTranslation) {
  this->connect();

  validate(this->conn, SQL_DROP_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoDropView) {
  this->connect();

  validate(this->conn, SQL_DROP_VIEW, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoExpressionsInOrderby) {
  this->connect();

  validate(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoGroupBy) {
  this->connect();

  validate(this->conn, SQL_GROUP_BY,
           static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierCase) {
  this->connect();

  validate(this->conn, SQL_IDENTIFIER_CASE, static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIdentifierQuoteChar) {
  this->connect();

  validate(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, L"\"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIndexKeywords) {
  this->connect();

  validate(this->conn, SQL_INDEX_KEYWORDS, static_cast<SQLUINTEGER>(SQL_IK_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoInsertStatement) {
  this->connect();

  validate(this->conn, SQL_INSERT_STATEMENT,
           static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED |
                                    SQL_IS_SELECT_INTO));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoIntegrity) {
  this->connect();

  validate(this->conn, SQL_INTEGRITY, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoKeywords) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_KEYWORDS, true);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoLikeEscapeClause) {
  this->connect();

  validate(this->conn, SQL_LIKE_ESCAPE_CLAUSE, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNonNullableColumns) {
  this->connect();

  validate(this->conn, SQL_NON_NULLABLE_COLUMNS, static_cast<SQLUSMALLINT>(SQL_NNC_NULL));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOjCapabilities) {
  this->connect();

  validate(this->conn, SQL_OJ_CAPABILITIES,
           static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOrderByColumnsInSelect) {
  this->connect();

  validate(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoOuterJoins) {
  this->connect();

  validate(this->conn, SQL_OUTER_JOINS, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoProcedures) {
  this->connect();

  validate(this->conn, SQL_PROCEDURES, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoQuotedIdentifierCase) {
  this->connect();

  validate(this->conn, SQL_QUOTED_IDENTIFIER_CASE,
           static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSchemaUsage) {
  this->connect();

  validate(this->conn, SQL_SCHEMA_USAGE, static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSpecialCharacters) {
  this->connect();

  validate(this->conn, SQL_SPECIAL_CHARACTERS, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoSqlConformance) {
  this->connect();

  validate(this->conn, SQL_SQL_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSubqueries) {
  this->connect();

  validate(this->conn, SQL_SUBQUERIES,
           static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                    SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoUnion) {
  this->connect();

  validate(this->conn, SQL_UNION,
           static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL));

  this->disconnect();
}

// SQL Limits

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxBinaryLiteralLen) {
  this->connect();

  validate(this->conn, SQL_MAX_BINARY_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCatalogNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_CATALOG_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxCharLiteralLen) {
  this->connect();

  validate(this->conn, SQL_MAX_CHAR_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxColumnNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMN_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInGroupBy) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInIndex) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_INDEX, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInOrderBy) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInSelect) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxColumnsInTable) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_TABLE, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxCursorNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_CURSOR_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIdentifierLen) {
  this->connect();

  validate(this->conn, SQL_MAX_IDENTIFIER_LEN, static_cast<SQLUSMALLINT>(65535));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxIndexSize) {
  this->connect();

  validate(this->conn, SQL_MAX_INDEX_SIZE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxProcedureNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxRowSize) {
  this->connect();

  validate(this->conn, SQL_MAX_ROW_SIZE, L"");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxRowSizeIncludesLong) {
  this->connect();

  validate(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxSchemaNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_SCHEMA_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxStatementLen) {
  this->connect();

  validate(this->conn, SQL_MAX_STATEMENT_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxTableNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_TABLE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoMaxTablesInSelect) {
  this->connect();

  validate(this->conn, SQL_MAX_TABLES_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoMaxUserNameLen) {
  this->connect();

  validate(this->conn, SQL_MAX_USER_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

// Scalar Function Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFunctions) {
  this->connect();

  validate(this->conn, SQL_CONVERT_FUNCTIONS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoNumericFunctions) {
  this->connect();

  validate(this->conn, SQL_NUMERIC_FUNCTIONS, static_cast<SQLUINTEGER>(4058942));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoStringFunctions) {
  this->connect();

  validate(this->conn, SQL_STRING_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                    SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoSystemFunctions) {
  this->connect();

  validate(this->conn, SQL_SYSTEM_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateAddIntervals) {
  this->connect();

  validate(this->conn, SQL_TIMEDATE_ADD_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoTimedateDiffIntervals) {
  this->connect();

  validate(this->conn, SQL_TIMEDATE_DIFF_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoTimedateFunctions) {
  this->connect();

  validate(this->conn, SQL_TIMEDATE_FUNCTIONS,
           static_cast<SQLUINTEGER>(
               SQL_FN_TD_CURRENT_DATE | SQL_FN_TD_CURRENT_TIME |
               SQL_FN_TD_CURRENT_TIMESTAMP | SQL_FN_TD_CURDATE | SQL_FN_TD_CURTIME |
               SQL_FN_TD_DAYNAME | SQL_FN_TD_DAYOFMONTH | SQL_FN_TD_DAYOFWEEK |
               SQL_FN_TD_DAYOFYEAR | SQL_FN_TD_EXTRACT | SQL_FN_TD_HOUR |
               SQL_FN_TD_MINUTE | SQL_FN_TD_MONTH | SQL_FN_TD_MONTHNAME | SQL_FN_TD_NOW |
               SQL_FN_TD_QUARTER | SQL_FN_TD_SECOND | SQL_FN_TD_TIMESTAMPADD |
               SQL_FN_TD_TIMESTAMPDIFF | SQL_FN_TD_WEEK | SQL_FN_TD_YEAR));

  this->disconnect();
}

// Conversion Information

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBigint) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BIGINT, static_cast<SQLUINTEGER>(8));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertBinary) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertBit) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BIT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertChar) {
  this->connect();

  validate(this->conn, SQL_CONVERT_CHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDate) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DATE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertDecimal) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DECIMAL, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertDouble) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DOUBLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertFloat) {
  this->connect();

  validate(this->conn, SQL_CONVERT_FLOAT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertInteger) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTEGER, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertIntervalDayTime) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertIntervalYearMonth) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarbinary) {
  this->connect();

  validate(this->conn, SQL_CONVERT_LONGVARBINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertLongvarchar) {
  this->connect();

  validate(this->conn, SQL_CONVERT_LONGVARCHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetInfoConvertNumeric) {
  this->connect();

  validate(this->conn, SQL_CONVERT_NUMERIC, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertReal) {
  this->connect();

  validate(this->conn, SQL_CONVERT_REAL, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertSmallint) {
  this->connect();

  validate(this->conn, SQL_CONVERT_SMALLINT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTime) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TIME, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTimestamp) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TIMESTAMP, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertTinyint) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TINYINT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarbinary) {
  this->connect();

  validate(this->conn, SQL_CONVERT_VARBINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetInfoConvertVarchar) {
  this->connect();

  validate(this->conn, SQL_CONVERT_VARCHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
