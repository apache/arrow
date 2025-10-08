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
class ConnectionInfoTest : public T {
 public:
  using List = std::list<T>;
};

class ConnectionInfoMockTest : public FlightSQLODBCMockTestBase {};
using TestTypes = ::testing::Types<ConnectionInfoMockTest, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(ConnectionInfoTest, TestTypes);

namespace {
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
void Validate(SQLHDBC connection, SQLUSMALLINT info_type,
              const SQLWCHAR* expected_value) {
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
}  // namespace

// Driver Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoActiveEnvironments) {
  Validate(this->conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));
}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncDbcFunctions) {
  Validate(this->conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));
}
#endif

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncMode) {
  Validate(this->conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));
}

#ifdef SQL_ASYNC_NOTIFICATION
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAsyncNotification) {
  Validate(this->conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));
}
#endif

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBatchRowCount) {
  Validate(this->conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBatchSupport) {
  Validate(this->conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDataSourceName) {
  Validate(this->conn, SQL_DATA_SOURCE_NAME, static_cast<const SQLWCHAR*>(L""));
}

#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverAwarePoolingSupported) {
  // According to Microsoft documentation, ODBC driver does not need to implement
  // SQL_DRIVER_AWARE_POOLING_SUPPORTED and the Driver Manager will ignore the
  // driver's return value for it.

  Validate(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));
}
#endif

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHdbc) {
  // Value returned from driver manager is the connection address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHdesc) {
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
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHenv) {
  // Value returned from driver manager is the env address
  ValidateGreaterThan(this->conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHlib) {
  ValidateGreaterThan(this->conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverHstmt) {
  // Value returned from driver manager is the stmt address
  SQLHSTMT local_stmt = this->stmt;
  ASSERT_EQ(SQL_SUCCESS, SQLGetInfo(this->conn, SQL_DRIVER_HSTMT, &local_stmt, 0, 0));
  EXPECT_GT(local_stmt, static_cast<SQLHSTMT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverName) {
  Validate(this->conn, SQL_DRIVER_NAME,
           static_cast<const SQLWCHAR*>(L"Arrow Flight ODBC Driver"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverOdbcVer) {
  Validate(this->conn, SQL_DRIVER_ODBC_VER, static_cast<const SQLWCHAR*>(L"03.80"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDriverVer) {
  Validate(this->conn, SQL_DRIVER_VER, static_cast<const SQLWCHAR*>(L"00.09.0000.0"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDynamicCursorAttributes1) {
  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDynamicCursorAttributes2) {
  Validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoForwardOnlyCursorAttributes1) {
  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoForwardOnlyCursorAttributes2) {
  Validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoFileUsage) {
  Validate(this->conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoGetDataExtensions) {
  Validate(this->conn, SQL_GETDATA_EXTENSIONS,
           static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSchemaViews) {
  Validate(this->conn, SQL_INFO_SCHEMA_VIEWS,
           static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeysetCursorAttributes1) {
  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeysetCursorAttributes2) {
  Validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxAsyncConcurrentStatements) {
  Validate(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxConcurrentActivities) {
  Validate(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxDriverConnections) {
  Validate(this->conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoOdbcInterfaceConformance) {
  Validate(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE,
           static_cast<SQLUINTEGER>(SQL_OIC_CORE));
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(ConnectionInfoTest, DISABLED_TestSQLGetInfoOdbcStandardCliConformance) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE,
  // static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoOdbcVer) {
  // This is implemented only in the Driver Manager.

  Validate(this->conn, SQL_ODBC_VER, static_cast<const SQLWCHAR*>(L"03.80.0000"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoParamArrayRowCounts) {
  Validate(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS,
           static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoParamArraySelects) {
  Validate(this->conn, SQL_PARAM_ARRAY_SELECTS,
           static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoRowUpdates) {
  Validate(this->conn, SQL_ROW_UPDATES, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSearchPatternEscape) {
  Validate(this->conn, SQL_SEARCH_PATTERN_ESCAPE, static_cast<const SQLWCHAR*>(L"\\"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoServerName) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_SERVER_NAME, false);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoStaticCursorAttributes1) {
  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoStaticCursorAttributes2) {
  Validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));
}

// DBMS Product Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDatabaseName) {
  Validate(this->conn, SQL_DATABASE_NAME, static_cast<const SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDbmsName) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_NAME, false);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDbmsVer) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_DBMS_VER, false);
}

// Data Source Information

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAccessibleProcedures) {
  Validate(this->conn, SQL_ACCESSIBLE_PROCEDURES, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAccessibleTables) {
  Validate(this->conn, SQL_ACCESSIBLE_TABLES, static_cast<const SQLWCHAR*>(L"Y"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoBookmarkPersistence) {
  Validate(this->conn, SQL_BOOKMARK_PERSISTENCE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogTerm) {
  Validate(this->conn, SQL_CATALOG_TERM, static_cast<const SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCollationSeq) {
  Validate(this->conn, SQL_COLLATION_SEQ, static_cast<const SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConcatNullBehavior) {
  Validate(this->conn, SQL_CONCAT_NULL_BEHAVIOR, static_cast<SQLUSMALLINT>(SQL_CB_NULL));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorCommitBehavior) {
  Validate(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorRollbackBehavior) {
  Validate(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCursorSensitivity) {
  Validate(this->conn, SQL_CURSOR_SENSITIVITY, static_cast<SQLUINTEGER>(SQL_UNSPECIFIED));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDataSourceReadOnly) {
  Validate(this->conn, SQL_DATA_SOURCE_READ_ONLY, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDefaultTxnIsolation) {
  Validate(this->conn, SQL_DEFAULT_TXN_ISOLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDescribeParameter) {
  Validate(this->conn, SQL_DESCRIBE_PARAMETER, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMultResultSets) {
  Validate(this->conn, SQL_MULT_RESULT_SETS, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMultipleActiveTxn) {
  Validate(this->conn, SQL_MULTIPLE_ACTIVE_TXN, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoNeedLongDataLen) {
  Validate(this->conn, SQL_NEED_LONG_DATA_LEN, static_cast<const SQLWCHAR*>(L"N"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNullCollation) {
  Validate(this->conn, SQL_NULL_COLLATION, static_cast<SQLUSMALLINT>(SQL_NC_START));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoProcedureTerm) {
  Validate(this->conn, SQL_PROCEDURE_TERM, static_cast<const SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSchemaTerm) {
  Validate(this->conn, SQL_SCHEMA_TERM, static_cast<const SQLWCHAR*>(L"schema"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoScrollOptions) {
  Validate(this->conn, SQL_SCROLL_OPTIONS, static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTableTerm) {
  Validate(this->conn, SQL_TABLE_TERM, static_cast<const SQLWCHAR*>(L"table"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTxnCapable) {
  Validate(this->conn, SQL_TXN_CAPABLE, static_cast<SQLUSMALLINT>(SQL_TC_NONE));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTxnIsolationOption) {
  Validate(this->conn, SQL_TXN_ISOLATION_OPTION, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoUserName) {
  Validate(this->conn, SQL_USER_NAME, static_cast<const SQLWCHAR*>(L""));
}

// Supported SQL

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAggregateFunctions) {
  Validate(
      this->conn, SQL_AGGREGATE_FUNCTIONS,
      static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                               SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAlterDomain) {
  Validate(this->conn, SQL_ALTER_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, DISABLED_TestSQLGetInfoAlterSchema) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ALTER_SCHEMA, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoAlterTable) {
  Validate(this->conn, SQL_ALTER_TABLE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, DISABLED_TestSQLGetInfoAnsiSqlDatetimeLiterals) {
  // Type commented out in odbc_connection.cc
  // Type does not exist in sql.h
  // Validate(this->conn, SQL_ANSI_SQL_DATETIME_LITERALS, static_cast<const
  // SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogLocation) {
  Validate(this->conn, SQL_CATALOG_LOCATION, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogName) {
  Validate(this->conn, SQL_CATALOG_NAME, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCatalogNameSeparator) {
  Validate(this->conn, SQL_CATALOG_NAME_SEPARATOR, static_cast<const SQLWCHAR*>(L""));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCatalogUsage) {
  Validate(this->conn, SQL_CATALOG_USAGE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoColumnAlias) {
  Validate(this->conn, SQL_COLUMN_ALIAS, static_cast<const SQLWCHAR*>(L"Y"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCorrelationName) {
  Validate(this->conn, SQL_CORRELATION_NAME, static_cast<SQLUSMALLINT>(SQL_CN_NONE));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateAssertion) {
  Validate(this->conn, SQL_CREATE_ASSERTION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateCharacterSet) {
  Validate(this->conn, SQL_CREATE_CHARACTER_SET, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateCollation) {
  Validate(this->conn, SQL_CREATE_COLLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateDomain) {
  Validate(this->conn, SQL_CREATE_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCreateSchema) {
  Validate(this->conn, SQL_CREATE_SCHEMA, static_cast<SQLUINTEGER>(1));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoCreateTable) {
  Validate(this->conn, SQL_CREATE_TABLE, static_cast<SQLUINTEGER>(1));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoCreateTranslation) {
  Validate(this->conn, SQL_CREATE_TRANSLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDdlIndex) {
  Validate(this->conn, SQL_DDL_INDEX, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropAssertion) {
  Validate(this->conn, SQL_DROP_ASSERTION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropCharacterSet) {
  Validate(this->conn, SQL_DROP_CHARACTER_SET, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropCollation) {
  Validate(this->conn, SQL_DROP_COLLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropDomain) {
  Validate(this->conn, SQL_DROP_DOMAIN, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropSchema) {
  Validate(this->conn, SQL_DROP_SCHEMA, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropTable) {
  Validate(this->conn, SQL_DROP_TABLE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropTranslation) {
  Validate(this->conn, SQL_DROP_TRANSLATION, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoDropView) {
  Validate(this->conn, SQL_DROP_VIEW, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoExpressionsInOrderby) {
  Validate(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, static_cast<const SQLWCHAR*>(L"N"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoGroupBy) {
  Validate(this->conn, SQL_GROUP_BY,
           static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIdentifierCase) {
  Validate(this->conn, SQL_IDENTIFIER_CASE, static_cast<SQLUSMALLINT>(SQL_IC_MIXED));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIdentifierQuoteChar) {
  Validate(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, static_cast<const SQLWCHAR*>(L"\")"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIndexKeywords) {
  Validate(this->conn, SQL_INDEX_KEYWORDS, static_cast<SQLUINTEGER>(SQL_IK_NONE));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoInsertStatement) {
  Validate(this->conn, SQL_INSERT_STATEMENT,
           static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED |
                                    SQL_IS_SELECT_INTO));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoIntegrity) {
  Validate(this->conn, SQL_INTEGRITY, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoKeywords) {
  ValidateNotEmptySQLWCHAR(this->conn, SQL_KEYWORDS, true);
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoLikeEscapeClause) {
  Validate(this->conn, SQL_LIKE_ESCAPE_CLAUSE, static_cast<const SQLWCHAR*>(L"Y"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNonNullableColumns) {
  Validate(this->conn, SQL_NON_NULLABLE_COLUMNS, static_cast<SQLUSMALLINT>(SQL_NNC_NULL));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOjCapabilities) {
  Validate(this->conn, SQL_OJ_CAPABILITIES,
           static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOrderByColumnsInSelect) {
  Validate(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT,
           static_cast<const SQLWCHAR*>(L"Y"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoOuterJoins) {
  Validate(this->conn, SQL_OUTER_JOINS, static_cast<const SQLWCHAR*>(L"N"));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoProcedures) {
  Validate(this->conn, SQL_PROCEDURES, static_cast<const SQLWCHAR*>(L"N"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoQuotedIdentifierCase) {
  Validate(this->conn, SQL_QUOTED_IDENTIFIER_CASE,
           static_cast<SQLUSMALLINT>(SQL_IC_MIXED));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSchemaUsage) {
  Validate(this->conn, SQL_SCHEMA_USAGE, static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSpecialCharacters) {
  Validate(this->conn, SQL_SPECIAL_CHARACTERS, static_cast<const SQLWCHAR*>(L""));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoSqlConformance) {
  Validate(this->conn, SQL_SQL_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSubqueries) {
  Validate(this->conn, SQL_SUBQUERIES,
           static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                    SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoUnion) {
  Validate(this->conn, SQL_UNION,
           static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL));
}

// SQL Limits

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxBinaryLiteralLen) {
  Validate(this->conn, SQL_MAX_BINARY_LITERAL_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxCatalogNameLen) {
  Validate(this->conn, SQL_MAX_CATALOG_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxCharLiteralLen) {
  Validate(this->conn, SQL_MAX_CHAR_LITERAL_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxColumnNameLen) {
  Validate(this->conn, SQL_MAX_COLUMN_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInGroupBy) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInIndex) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_INDEX, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInOrderBy) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInSelect) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_SELECT, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxColumnsInTable) {
  Validate(this->conn, SQL_MAX_COLUMNS_IN_TABLE, static_cast<SQLUSMALLINT>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxCursorNameLen) {
  Validate(this->conn, SQL_MAX_CURSOR_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxIdentifierLen) {
  Validate(this->conn, SQL_MAX_IDENTIFIER_LEN, static_cast<SQLUSMALLINT>(65535));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxIndexSize) {
  Validate(this->conn, SQL_MAX_INDEX_SIZE, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxProcedureNameLen) {
  Validate(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxRowSize) {
  Validate(this->conn, SQL_MAX_ROW_SIZE, static_cast<const SQLWCHAR*>(L""));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxRowSizeIncludesLong) {
  Validate(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG,
           static_cast<const SQLWCHAR*>(L"N"));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxSchemaNameLen) {
  Validate(this->conn, SQL_MAX_SCHEMA_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxStatementLen) {
  Validate(this->conn, SQL_MAX_STATEMENT_LEN, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxTableNameLen) {
  Validate(this->conn, SQL_MAX_TABLE_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoMaxTablesInSelect) {
  Validate(this->conn, SQL_MAX_TABLES_IN_SELECT, static_cast<SQLUSMALLINT>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoMaxUserNameLen) {
  Validate(this->conn, SQL_MAX_USER_NAME_LEN, static_cast<SQLUSMALLINT>(0));
}

// Scalar Function Information

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertFunctions) {
  Validate(this->conn, SQL_CONVERT_FUNCTIONS, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoNumericFunctions) {
  Validate(this->conn, SQL_NUMERIC_FUNCTIONS, static_cast<SQLUINTEGER>(4058942));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoStringFunctions) {
  Validate(this->conn, SQL_STRING_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                    SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoSystemFunctions) {
  Validate(this->conn, SQL_SYSTEM_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTimedateAddIntervals) {
  Validate(this->conn, SQL_TIMEDATE_ADD_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoTimedateDiffIntervals) {
  Validate(this->conn, SQL_TIMEDATE_DIFF_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoTimedateFunctions) {
  Validate(this->conn, SQL_TIMEDATE_FUNCTIONS,
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
  Validate(this->conn, SQL_CONVERT_BIGINT, static_cast<SQLUINTEGER>(8));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertBinary) {
  Validate(this->conn, SQL_CONVERT_BINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertBit) {
  Validate(this->conn, SQL_CONVERT_BIT, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertChar) {
  Validate(this->conn, SQL_CONVERT_CHAR, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertDate) {
  Validate(this->conn, SQL_CONVERT_DATE, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertDecimal) {
  Validate(this->conn, SQL_CONVERT_DECIMAL, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertDouble) {
  Validate(this->conn, SQL_CONVERT_DOUBLE, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertFloat) {
  Validate(this->conn, SQL_CONVERT_FLOAT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertInteger) {
  Validate(this->conn, SQL_CONVERT_INTEGER, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertIntervalDayTime) {
  Validate(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertIntervalYearMonth) {
  Validate(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertLongvarbinary) {
  Validate(this->conn, SQL_CONVERT_LONGVARBINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertLongvarchar) {
  Validate(this->conn, SQL_CONVERT_LONGVARCHAR, static_cast<SQLUINTEGER>(0));
}

TEST_F(ConnectionInfoMockTest, TestSQLGetInfoConvertNumeric) {
  Validate(this->conn, SQL_CONVERT_NUMERIC, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertReal) {
  Validate(this->conn, SQL_CONVERT_REAL, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertSmallint) {
  Validate(this->conn, SQL_CONVERT_SMALLINT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTime) {
  Validate(this->conn, SQL_CONVERT_TIME, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTimestamp) {
  Validate(this->conn, SQL_CONVERT_TIMESTAMP, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertTinyint) {
  Validate(this->conn, SQL_CONVERT_TINYINT, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertVarbinary) {
  Validate(this->conn, SQL_CONVERT_VARBINARY, static_cast<SQLUINTEGER>(0));
}

TYPED_TEST(ConnectionInfoTest, TestSQLGetInfoConvertVarchar) {
  Validate(this->conn, SQL_CONVERT_VARCHAR, static_cast<SQLUINTEGER>(0));
}

}  // namespace arrow::flight::sql::odbc
