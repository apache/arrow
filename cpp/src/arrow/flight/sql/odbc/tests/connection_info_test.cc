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

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ACTIVE_ENVIRONMENTS) {
  this->connect();

  validate(this->conn, SQL_ACTIVE_ENVIRONMENTS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_DBC_FUNCTIONS) {
  this->connect();

#ifdef SQL_ASYNC_DBC_FUNCTIONS
  validate(this->conn, SQL_ASYNC_DBC_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_ASYNC_DBC_NOT_CAPABLE));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_MODE) {
  this->connect();

  validate(this->conn, SQL_ASYNC_MODE, static_cast<SQLUINTEGER>(SQL_AM_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ASYNC_NOTIFICATION) {
  this->connect();

#ifdef SQL_ASYNC_NOTIFICATION
  validate(this->conn, SQL_ASYNC_NOTIFICATION,
           static_cast<SQLUINTEGER>(SQL_ASYNC_NOTIFICATION_NOT_CAPABLE));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_BATCH_ROW_COUNT) {
  this->connect();

  validate(this->conn, SQL_BATCH_ROW_COUNT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_BATCH_SUPPORT) {
  this->connect();

  validate(this->conn, SQL_BATCH_SUPPORT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DATA_SOURCE_NAME) {
  this->connect();

  validate(this->conn, SQL_DATA_SOURCE_NAME, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_AWARE_POOLING_SUPPORTED) {
  // A driver does not need to implement SQL_DRIVER_AWARE_POOLING_SUPPORTED and the
  // Driver Manager will not honor to the driver's return value.
  this->connect();

  validate(this->conn, SQL_DRIVER_AWARE_POOLING_SUPPORTED,
           static_cast<SQLUINTEGER>(SQL_DRIVER_AWARE_POOLING_NOT_CAPABLE));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HDBC) {
  this->connect();

  // Value returned from driver manager is the connection address
  validateGreaterThan(this->conn, SQL_DRIVER_HDBC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HDESC) {
  // TODO This is failing due to no descriptor being created
  GTEST_SKIP();
  this->connect();

  validate(this->conn, SQL_DRIVER_HDESC, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HENV) {
  this->connect();

  // Value returned from driver manager is the env address
  validateGreaterThan(this->conn, SQL_DRIVER_HENV, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HLIB) {
  this->connect();

  validateGreaterThan(this->conn, SQL_DRIVER_HLIB, static_cast<SQLULEN>(0));

  this->disconnect();
}

// These information types are implemented by the Driver Manager alone.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_HSTMT) {
  // TODO This is failing due to no statement being created
  // This should run after SQLGetStmtAttr is implemented
  GTEST_SKIP();
  this->connect();

  validate(this->conn, SQL_DRIVER_HSTMT, static_cast<SQLULEN>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_NAME) {
  this->connect();

  validate(this->conn, SQL_DRIVER_NAME, L"Arrow Flight ODBC Driver");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_ODBC_VER) {
  this->connect();

  validate(this->conn, SQL_DRIVER_ODBC_VER, L"03.80");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DRIVER_VER) {
  this->connect();

  validate(this->conn, SQL_DRIVER_VER, L"00.09.0000.0");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DYNAMIC_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DYNAMIC_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(this->conn, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1,
           static_cast<SQLUINTEGER>(SQL_CA1_NEXT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(this->conn, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2,
           static_cast<SQLUINTEGER>(SQL_CA2_READ_ONLY_CONCURRENCY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_FILE_USAGE) {
  this->connect();

  validate(this->conn, SQL_FILE_USAGE, static_cast<SQLUSMALLINT>(SQL_FILE_NOT_SUPPORTED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_GETDATA_EXTENSIONS) {
  this->connect();

  validate(this->conn, SQL_GETDATA_EXTENSIONS,
           static_cast<SQLUINTEGER>(SQL_GD_ANY_COLUMN | SQL_GD_ANY_ORDER));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_INFO_SCHEMA_VIEWS) {
  this->connect();

  validate(this->conn, SQL_INFO_SCHEMA_VIEWS,
           static_cast<SQLUINTEGER>(SQL_ISV_TABLES | SQL_ISV_COLUMNS | SQL_ISV_VIEWS));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_KEYSET_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_KEYSET_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(this->conn, SQL_KEYSET_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_ASYNC_CONCURRENT_STATEMENTS) {
  this->connect();

  validate(this->conn, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_CONCURRENT_ACTIVITIES) {
  this->connect();

  validate(this->conn, SQL_MAX_CONCURRENT_ACTIVITIES, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_DRIVER_CONNECTIONS) {
  this->connect();

  validate(this->conn, SQL_MAX_DRIVER_CONNECTIONS, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_INTERFACE_CONFORMANCE) {
  this->connect();

  validate(this->conn, SQL_ODBC_INTERFACE_CONFORMANCE,
           static_cast<SQLUINTEGER>(SQL_OIC_CORE));

  this->disconnect();
}

// case SQL_ODBC_STANDARD_CLI_CONFORMANCE: - mentioned in SQLGetInfo spec with no
// description and there is no constant for this.
TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_STANDARD_CLI_CONFORMANCE) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ODBC_STANDARD_CLI_CONFORMANCE,
  // static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ODBC_VER) {
  // This is implemented only in the Driver Manager.
  this->connect();

  validate(this->conn, SQL_ODBC_VER, L"03.80.0000");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_PARAM_ARRAY_ROW_COUNTS) {
  this->connect();

  validate(this->conn, SQL_PARAM_ARRAY_ROW_COUNTS,
           static_cast<SQLUINTEGER>(SQL_PARC_NO_BATCH));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_PARAM_ARRAY_SELECTS) {
  this->connect();

  validate(this->conn, SQL_PARAM_ARRAY_SELECTS,
           static_cast<SQLUINTEGER>(SQL_PAS_NO_SELECT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ROW_UPDATES) {
  this->connect();

  validate(this->conn, SQL_ROW_UPDATES, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SEARCH_PATTERN_ESCAPE) {
  this->connect();

  validate(this->conn, SQL_SEARCH_PATTERN_ESCAPE, L"\\");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SERVER_NAME) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_SERVER_NAME, false);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_STATIC_CURSOR_ATTRIBUTES1) {
  this->connect();

  validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES1, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_STATIC_CURSOR_ATTRIBUTES2) {
  this->connect();

  validate(this->conn, SQL_STATIC_CURSOR_ATTRIBUTES2, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

// DBMS Product Information

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DATABASE_NAME) {
  this->connect();

  validate(this->conn, SQL_DATABASE_NAME, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DBMS_NAME) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_DBMS_NAME, false);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DBMS_VER) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_DBMS_VER, false);

  this->disconnect();
}

// Data Source Information

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ACCESSIBLE_PROCEDURES) {
  this->connect();

  validate(this->conn, SQL_ACCESSIBLE_PROCEDURES, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ACCESSIBLE_TABLES) {
  this->connect();

  validate(this->conn, SQL_ACCESSIBLE_TABLES, L"Y");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_BOOKMARK_PERSISTENCE) {
  this->connect();

  validate(this->conn, SQL_BOOKMARK_PERSISTENCE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CATALOG_TERM) {
  this->connect();

  validate(this->conn, SQL_CATALOG_TERM, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_COLLATION_SEQ) {
  this->connect();

  validate(this->conn, SQL_COLLATION_SEQ, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONCAT_NULL_BEHAVIOR) {
  this->connect();

  validate(this->conn, SQL_CONCAT_NULL_BEHAVIOR, static_cast<SQLUSMALLINT>(SQL_CB_NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CURSOR_COMMIT_BEHAVIOR) {
  this->connect();

  validate(this->conn, SQL_CURSOR_COMMIT_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CURSOR_ROLLBACK_BEHAVIOR) {
  this->connect();

  validate(this->conn, SQL_CURSOR_ROLLBACK_BEHAVIOR,
           static_cast<SQLUSMALLINT>(SQL_CB_CLOSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CURSOR_SENSITIVITY) {
  this->connect();

  validate(this->conn, SQL_CURSOR_SENSITIVITY, static_cast<SQLUINTEGER>(SQL_UNSPECIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DATA_SOURCE_READ_ONLY) {
  this->connect();

  validate(this->conn, SQL_DATA_SOURCE_READ_ONLY, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DEFAULT_TXN_ISOLATION) {
  this->connect();

  validate(this->conn, SQL_DEFAULT_TXN_ISOLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DESCRIBE_PARAMETER) {
  this->connect();

  validate(this->conn, SQL_DESCRIBE_PARAMETER, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MULT_RESULT_SETS) {
  this->connect();

  validate(this->conn, SQL_MULT_RESULT_SETS, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MULTIPLE_ACTIVE_TXN) {
  this->connect();

  validate(this->conn, SQL_MULTIPLE_ACTIVE_TXN, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_NEED_LONG_DATA_LEN) {
  this->connect();

  validate(this->conn, SQL_NEED_LONG_DATA_LEN, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_NULL_COLLATION) {
  this->connect();

  validate(this->conn, SQL_NULL_COLLATION, static_cast<SQLUSMALLINT>(SQL_NC_START));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_PROCEDURE_TERM) {
  this->connect();

  validate(this->conn, SQL_PROCEDURE_TERM, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SCHEMA_TERM) {
  this->connect();

  validate(this->conn, SQL_SCHEMA_TERM, L"schema");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SCROLL_OPTIONS) {
  this->connect();

  validate(this->conn, SQL_SCROLL_OPTIONS, static_cast<SQLUINTEGER>(SQL_SO_FORWARD_ONLY));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_TABLE_TERM) {
  this->connect();

  validate(this->conn, SQL_TABLE_TERM, L"table");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_TXN_CAPABLE) {
  this->connect();

  validate(this->conn, SQL_TXN_CAPABLE, static_cast<SQLUSMALLINT>(SQL_TC_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_TXN_ISOLATION_OPTION) {
  this->connect();

  validate(this->conn, SQL_TXN_ISOLATION_OPTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_USER_NAME) {
  this->connect();

  validate(this->conn, SQL_USER_NAME, L"");

  this->disconnect();
}

// Supported SQL

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_AGGREGATE_FUNCTIONS) {
  this->connect();

  validate(
      this->conn, SQL_AGGREGATE_FUNCTIONS,
      static_cast<SQLUINTEGER>(SQL_AF_ALL | SQL_AF_AVG | SQL_AF_COUNT | SQL_AF_DISTINCT |
                               SQL_AF_MAX | SQL_AF_MIN | SQL_AF_SUM));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ALTER_DOMAIN) {
  this->connect();

  validate(this->conn, SQL_ALTER_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ALTER_SCHEMA) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ALTER_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ALTER_TABLE) {
  this->connect();

  validate(this->conn, SQL_ALTER_TABLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_ANSI_SQL_DATETIME_LITERALS) {
  // Type commented out in odbc_connection.cc
  GTEST_SKIP();
  this->connect();

  // Type does not exist in sql.h
  // validate(this->conn, SQL_ANSI_SQL_DATETIME_LITERALS, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CATALOG_LOCATION) {
  this->connect();

  validate(this->conn, SQL_CATALOG_LOCATION, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CATALOG_NAME) {
  this->connect();

  validate(this->conn, SQL_CATALOG_NAME, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CATALOG_NAME_SEPARATOR) {
  this->connect();

  validate(this->conn, SQL_CATALOG_NAME_SEPARATOR, L"");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CATALOG_USAGE) {
  this->connect();

  validate(this->conn, SQL_CATALOG_USAGE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_COLUMN_ALIAS) {
  this->connect();

  validate(this->conn, SQL_COLUMN_ALIAS, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CORRELATION_NAME) {
  this->connect();

  validate(this->conn, SQL_CORRELATION_NAME, static_cast<SQLUSMALLINT>(SQL_CN_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CREATE_ASSERTION) {
  this->connect();

  validate(this->conn, SQL_CREATE_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CREATE_CHARACTER_SET) {
  this->connect();

  validate(this->conn, SQL_CREATE_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CREATE_COLLATION) {
  this->connect();

  validate(this->conn, SQL_CREATE_COLLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CREATE_DOMAIN) {
  this->connect();

  validate(this->conn, SQL_CREATE_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CREATE_SCHEMA) {
  this->connect();

  validate(this->conn, SQL_CREATE_SCHEMA, static_cast<SQLUINTEGER>(1));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CREATE_TABLE) {
  this->connect();

  validate(this->conn, SQL_CREATE_TABLE, static_cast<SQLUINTEGER>(1));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CREATE_TRANSLATION) {
  this->connect();

  validate(this->conn, SQL_CREATE_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DDL_INDEX) {
  this->connect();

  validate(this->conn, SQL_DDL_INDEX, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_ASSERTION) {
  this->connect();

  validate(this->conn, SQL_DROP_ASSERTION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_CHARACTER_SET) {
  this->connect();

  validate(this->conn, SQL_DROP_CHARACTER_SET, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_COLLATION) {
  this->connect();

  validate(this->conn, SQL_DROP_COLLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_DOMAIN) {
  this->connect();

  validate(this->conn, SQL_DROP_DOMAIN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_SCHEMA) {
  this->connect();

  validate(this->conn, SQL_DROP_SCHEMA, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_TABLE) {
  this->connect();

  validate(this->conn, SQL_DROP_TABLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_TRANSLATION) {
  this->connect();

  validate(this->conn, SQL_DROP_TRANSLATION, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_DROP_VIEW) {
  this->connect();

  validate(this->conn, SQL_DROP_VIEW, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_EXPRESSIONS_IN_ORDERBY) {
  this->connect();

  validate(this->conn, SQL_EXPRESSIONS_IN_ORDERBY, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_GROUP_BY) {
  this->connect();

  validate(this->conn, SQL_GROUP_BY,
           static_cast<SQLUSMALLINT>(SQL_GB_GROUP_BY_CONTAINS_SELECT));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_IDENTIFIER_CASE) {
  this->connect();

  validate(this->conn, SQL_IDENTIFIER_CASE, static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_IDENTIFIER_QUOTE_CHAR) {
  this->connect();

  validate(this->conn, SQL_IDENTIFIER_QUOTE_CHAR, L"\"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_INDEX_KEYWORDS) {
  this->connect();

  validate(this->conn, SQL_INDEX_KEYWORDS, static_cast<SQLUINTEGER>(SQL_IK_NONE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_INSERT_STATEMENT) {
  this->connect();

  validate(this->conn, SQL_INSERT_STATEMENT,
           static_cast<SQLUINTEGER>(SQL_IS_INSERT_LITERALS | SQL_IS_INSERT_SEARCHED |
                                    SQL_IS_SELECT_INTO));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_INTEGRITY) {
  this->connect();

  validate(this->conn, SQL_INTEGRITY, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_KEYWORDS) {
  this->connect();

  validateNotEmptySQLWCHAR(this->conn, SQL_KEYWORDS, true);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_LIKE_ESCAPE_CLAUSE) {
  this->connect();

  validate(this->conn, SQL_LIKE_ESCAPE_CLAUSE, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_NON_NULLABLE_COLUMNS) {
  this->connect();

  validate(this->conn, SQL_NON_NULLABLE_COLUMNS, static_cast<SQLUSMALLINT>(SQL_NNC_NULL));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_OJ_CAPABILITIES) {
  this->connect();

  validate(this->conn, SQL_OJ_CAPABILITIES,
           static_cast<SQLUINTEGER>(SQL_OJ_LEFT | SQL_OJ_RIGHT | SQL_OJ_FULL));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_ORDER_BY_COLUMNS_IN_SELECT) {
  this->connect();

  validate(this->conn, SQL_ORDER_BY_COLUMNS_IN_SELECT, L"Y");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_OUTER_JOINS) {
  this->connect();

  validate(this->conn, SQL_OUTER_JOINS, L"N");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_PROCEDURES) {
  this->connect();

  validate(this->conn, SQL_PROCEDURES, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_QUOTED_IDENTIFIER_CASE) {
  this->connect();

  validate(this->conn, SQL_QUOTED_IDENTIFIER_CASE,
           static_cast<SQLUSMALLINT>(SQL_IC_MIXED));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_SCHEMA_USAGE) {
  this->connect();

  validate(this->conn, SQL_SCHEMA_USAGE, static_cast<SQLUINTEGER>(SQL_SU_DML_STATEMENTS));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SPECIAL_CHARACTERS) {
  this->connect();

  validate(this->conn, SQL_SPECIAL_CHARACTERS, L"");

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_SQL_CONFORMANCE) {
  this->connect();

  validate(this->conn, SQL_SQL_CONFORMANCE, static_cast<SQLUINTEGER>(SQL_SC_SQL92_ENTRY));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_SUBQUERIES) {
  this->connect();

  validate(this->conn, SQL_SUBQUERIES,
           static_cast<SQLUINTEGER>(SQL_SQ_CORRELATED_SUBQUERIES | SQL_SQ_COMPARISON |
                                    SQL_SQ_EXISTS | SQL_SQ_IN | SQL_SQ_QUANTIFIED));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_UNION) {
  this->connect();

  validate(this->conn, SQL_UNION,
           static_cast<SQLUINTEGER>(SQL_U_UNION | SQL_U_UNION_ALL));

  this->disconnect();
}

// SQL Limits

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_BINARY_LITERAL_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_BINARY_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_CATALOG_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_CATALOG_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_CHAR_LITERAL_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_CHAR_LITERAL_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_COLUMN_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMN_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_COLUMNS_IN_GROUP_BY) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_GROUP_BY, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_COLUMNS_IN_INDEX) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_INDEX, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_COLUMNS_IN_ORDER_BY) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_ORDER_BY, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_COLUMNS_IN_SELECT) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_COLUMNS_IN_TABLE) {
  this->connect();

  validate(this->conn, SQL_MAX_COLUMNS_IN_TABLE, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_CURSOR_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_CURSOR_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_IDENTIFIER_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_IDENTIFIER_LEN, static_cast<SQLUSMALLINT>(65535));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_INDEX_SIZE) {
  this->connect();

  validate(this->conn, SQL_MAX_INDEX_SIZE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_PROCEDURE_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_PROCEDURE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_ROW_SIZE) {
  this->connect();

  validate(this->conn, SQL_MAX_ROW_SIZE, L"");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_ROW_SIZE_INCLUDES_LONG) {
  this->connect();

  validate(this->conn, SQL_MAX_ROW_SIZE_INCLUDES_LONG, L"N");

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_SCHEMA_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_SCHEMA_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_STATEMENT_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_STATEMENT_LEN, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_TABLE_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_TABLE_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_MAX_TABLES_IN_SELECT) {
  this->connect();

  validate(this->conn, SQL_MAX_TABLES_IN_SELECT, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_MAX_USER_NAME_LEN) {
  this->connect();

  validate(this->conn, SQL_MAX_USER_NAME_LEN, static_cast<SQLUSMALLINT>(0));

  this->disconnect();
}

// Scalar Function Information

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_FUNCTIONS) {
  this->connect();

  validate(this->conn, SQL_CONVERT_FUNCTIONS, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_NUMERIC_FUNCTIONS) {
  this->connect();

  validate(this->conn, SQL_NUMERIC_FUNCTIONS, static_cast<SQLUINTEGER>(4058942));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_STRING_FUNCTIONS) {
  this->connect();

  validate(this->conn, SQL_STRING_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_STR_LTRIM | SQL_FN_STR_LENGTH |
                                    SQL_FN_STR_REPLACE | SQL_FN_STR_RTRIM));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_SYSTEM_FUNCTIONS) {
  this->connect();

  validate(this->conn, SQL_SYSTEM_FUNCTIONS,
           static_cast<SQLUINTEGER>(SQL_FN_SYS_IFNULL | SQL_FN_SYS_USERNAME));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_TIMEDATE_ADD_INTERVALS) {
  this->connect();

  validate(this->conn, SQL_TIMEDATE_ADD_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_TIMEDATE_DIFF_INTERVALS) {
  this->connect();

  validate(this->conn, SQL_TIMEDATE_DIFF_INTERVALS,
           static_cast<SQLUINTEGER>(SQL_FN_TSI_FRAC_SECOND | SQL_FN_TSI_SECOND |
                                    SQL_FN_TSI_MINUTE | SQL_FN_TSI_HOUR | SQL_FN_TSI_DAY |
                                    SQL_FN_TSI_WEEK | SQL_FN_TSI_MONTH |
                                    SQL_FN_TSI_QUARTER | SQL_FN_TSI_YEAR));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_TIMEDATE_FUNCTIONS) {
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

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_BIGINT) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BIGINT, static_cast<SQLUINTEGER>(8));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_BINARY) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_BIT) {
  this->connect();

  validate(this->conn, SQL_CONVERT_BIT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_CHAR) {
  this->connect();

  validate(this->conn, SQL_CONVERT_CHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_DATE) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DATE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_DECIMAL) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DECIMAL, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_DOUBLE) {
  this->connect();

  validate(this->conn, SQL_CONVERT_DOUBLE, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_FLOAT) {
  this->connect();

  validate(this->conn, SQL_CONVERT_FLOAT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_INTEGER) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTEGER, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_INTERVAL_DAY_TIME) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTERVAL_DAY_TIME, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_INTERVAL_YEAR_MONTH) {
  this->connect();

  validate(this->conn, SQL_CONVERT_INTERVAL_YEAR_MONTH, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_LONGVARBINARY) {
  this->connect();

  validate(this->conn, SQL_CONVERT_LONGVARBINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_LONGVARCHAR) {
  this->connect();

  validate(this->conn, SQL_CONVERT_LONGVARCHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, Test_SQL_CONVERT_NUMERIC) {
  this->connect();

  validate(this->conn, SQL_CONVERT_NUMERIC, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_REAL) {
  this->connect();

  validate(this->conn, SQL_CONVERT_REAL, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_SMALLINT) {
  this->connect();

  validate(this->conn, SQL_CONVERT_SMALLINT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_TIME) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TIME, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_TIMESTAMP) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TIMESTAMP, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_TINYINT) {
  this->connect();

  validate(this->conn, SQL_CONVERT_TINYINT, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_VARBINARY) {
  this->connect();

  validate(this->conn, SQL_CONVERT_VARBINARY, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, Test_SQL_CONVERT_VARCHAR) {
  this->connect();

  validate(this->conn, SQL_CONVERT_VARCHAR, static_cast<SQLUINTEGER>(0));

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
