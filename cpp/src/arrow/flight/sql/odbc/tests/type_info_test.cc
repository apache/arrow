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

// TODO: add tests with SQLDescribeCol to check metadata of SQLGetTypeInfo for ODBC 2 and
// ODBC 3.

namespace arrow::flight::sql::odbc {

using std::optional;

void checkSQLGetTypeInfo(
    SQLHSTMT stmt, const std::wstring& expectedTypeName,
    const SQLSMALLINT& expectedDataType, const SQLINTEGER& expectedColumnSize,
    const optional<std::wstring>& expectedLiteralPrefix,
    const optional<std::wstring>& expectedLiteralSuffix,
    const optional<std::wstring>& expectedCreateParams,
    const SQLSMALLINT& expectedNullable, const SQLSMALLINT& expectedCaseSensitive,
    const SQLSMALLINT& expectedSearchable, const SQLSMALLINT& expectedUnsignedAttr,
    const SQLSMALLINT& expectedFixedPrecScale, const SQLSMALLINT& expectedAutoUniqueValue,
    const std::wstring& expectedLocalTypeName, const SQLSMALLINT& expectedMinScale,
    const SQLSMALLINT& expectedMaxScale, const SQLSMALLINT& expectedSqlDataType,
    const SQLSMALLINT& expectedSqlDatetimeSub, const SQLINTEGER& expectedNumPrecRadix,
    const SQLINTEGER& expectedIntervalPrec) {
  CheckStringColumnW(stmt, 1, expectedTypeName);   // type name
  CheckSmallIntColumn(stmt, 2, expectedDataType);  // data type
  CheckIntColumn(stmt, 3, expectedColumnSize);     // column size

  if (expectedLiteralPrefix) {  // literal prefix
    CheckStringColumnW(stmt, 4, *expectedLiteralPrefix);
  } else {
    CheckNullColumnW(stmt, 4);
  }

  if (expectedLiteralSuffix) {  // literal suffix
    CheckStringColumnW(stmt, 5, *expectedLiteralSuffix);
  } else {
    CheckNullColumnW(stmt, 5);
  }

  if (expectedCreateParams) {  // create params
    CheckStringColumnW(stmt, 6, *expectedCreateParams);
  } else {
    CheckNullColumnW(stmt, 6);
  }

  CheckSmallIntColumn(stmt, 7, expectedNullable);          // nullable
  CheckSmallIntColumn(stmt, 8, expectedCaseSensitive);     // case sensitive
  CheckSmallIntColumn(stmt, 9, expectedSearchable);        // searchable
  CheckSmallIntColumn(stmt, 10, expectedUnsignedAttr);     // unsigned attr
  CheckSmallIntColumn(stmt, 11, expectedFixedPrecScale);   // fixed prec scale
  CheckSmallIntColumn(stmt, 12, expectedAutoUniqueValue);  // auto unique value
  CheckStringColumnW(stmt, 13, expectedLocalTypeName);     // local type name
  CheckSmallIntColumn(stmt, 14, expectedMinScale);         // min scale
  CheckSmallIntColumn(stmt, 15, expectedMaxScale);         // max scale
  CheckSmallIntColumn(stmt, 16, expectedSqlDataType);      // sql data type
  CheckSmallIntColumn(stmt, 17, expectedSqlDatetimeSub);   // sql datetime sub
  CheckIntColumn(stmt, 18, expectedNumPrecRadix);          // num prec radix
  CheckIntColumn(stmt, 19, expectedIntervalPrec);          // interval prec
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoAllTypes) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expectedTypeName
                      SQL_BIT,               // expectedDataType
                      1,                     // expectedColumnSize
                      std::nullopt,          // expectedLiteralPrefix
                      std::nullopt,          // expectedLiteralSuffix
                      std::nullopt,          // expectedCreateParams
                      SQL_NULLABLE,          // expectedNullable
                      SQL_FALSE,             // expectedCaseSensitive
                      SQL_SEARCHABLE,        // expectedSearchable
                      NULL,                  // expectedUnsignedAttr
                      SQL_FALSE,             // expectedFixedPrecScale
                      NULL,                  // expectedAutoUniqueValue
                      std::wstring(L"bit"),  // expectedLocalTypeName
                      NULL,                  // expectedMinScale
                      NULL,                  // expectedMaxScale
                      SQL_BIT,               // expectedSqlDataType
                      NULL,                  // expectedSqlDatetimeSub
                      NULL,                  // expectedNumPrecRadix
                      NULL);                 // expectedIntervalPrec

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expectedTypeName
                      SQL_TINYINT,               // expectedDataType
                      3,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"tinyint"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_TINYINT,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expectedTypeName
                      SQL_BIGINT,               // expectedDataType
                      19,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"bigint"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_BIGINT,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expectedTypeName
                      SQL_LONGVARBINARY,               // expectedDataType
                      65536,                           // expectedColumnSize
                      std::nullopt,                    // expectedLiteralPrefix
                      std::nullopt,                    // expectedLiteralSuffix
                      std::nullopt,                    // expectedCreateParams
                      SQL_NULLABLE,                    // expectedNullable
                      SQL_FALSE,                       // expectedCaseSensitive
                      SQL_SEARCHABLE,                  // expectedSearchable
                      NULL,                            // expectedUnsignedAttr
                      SQL_FALSE,                       // expectedFixedPrecScale
                      NULL,                            // expectedAutoUniqueValue
                      std::wstring(L"longvarbinary"),  // expectedLocalTypeName
                      NULL,                            // expectedMinScale
                      NULL,                            // expectedMaxScale
                      SQL_LONGVARBINARY,               // expectedSqlDataType
                      NULL,                            // expectedSqlDatetimeSub
                      NULL,                            // expectedNumPrecRadix
                      NULL);                           // expectedIntervalPrec

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expectedTypeName
                      SQL_VARBINARY,               // expectedDataType
                      255,                         // expectedColumnSize
                      std::nullopt,                // expectedLiteralPrefix
                      std::nullopt,                // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      NULL,                        // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"varbinary"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_VARBINARY,               // expectedSqlDataType
                      NULL,                        // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expectedTypeName
                      SQL_WLONGVARCHAR,         // expectedDataType
                      65536,                    // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"text"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WLONGVARCHAR,         // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expectedTypeName
                      SQL_WLONGVARCHAR,              // expectedDataType
                      65536,                         // expectedColumnSize
                      std::wstring(L"'"),            // expectedLiteralPrefix
                      std::wstring(L"'"),            // expectedLiteralSuffix
                      std::wstring(L"length"),       // expectedCreateParams
                      SQL_NULLABLE,                  // expectedNullable
                      SQL_FALSE,                     // expectedCaseSensitive
                      SQL_SEARCHABLE,                // expectedSearchable
                      NULL,                          // expectedUnsignedAttr
                      SQL_FALSE,                     // expectedFixedPrecScale
                      NULL,                          // expectedAutoUniqueValue
                      std::wstring(L"longvarchar"),  // expectedLocalTypeName
                      NULL,                          // expectedMinScale
                      NULL,                          // expectedMaxScale
                      SQL_WLONGVARCHAR,              // expectedSqlDataType
                      NULL,                          // expectedSqlDatetimeSub
                      NULL,                          // expectedNumPrecRadix
                      NULL);                         // expectedIntervalPrec

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expectedTypeName
                      SQL_WCHAR,                // expectedDataType
                      255,                      // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"char"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WCHAR,                // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expectedTypeName
                      SQL_INTEGER,               // expectedDataType
                      9,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"integer"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_INTEGER,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expectedTypeName
                      SQL_SMALLINT,               // expectedDataType
                      5,                          // expectedColumnSize
                      std::nullopt,               // expectedLiteralPrefix
                      std::nullopt,               // expectedLiteralSuffix
                      std::nullopt,               // expectedCreateParams
                      SQL_NULLABLE,               // expectedNullable
                      SQL_FALSE,                  // expectedCaseSensitive
                      SQL_SEARCHABLE,             // expectedSearchable
                      SQL_FALSE,                  // expectedUnsignedAttr
                      SQL_FALSE,                  // expectedFixedPrecScale
                      NULL,                       // expectedAutoUniqueValue
                      std::wstring(L"smallint"),  // expectedLocalTypeName
                      NULL,                       // expectedMinScale
                      NULL,                       // expectedMaxScale
                      SQL_SMALLINT,               // expectedSqlDataType
                      NULL,                       // expectedSqlDatetimeSub
                      NULL,                       // expectedNumPrecRadix
                      NULL);                      // expectedIntervalPrec

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expectedTypeName
                      SQL_FLOAT,               // expectedDataType
                      7,                       // expectedColumnSize
                      std::nullopt,            // expectedLiteralPrefix
                      std::nullopt,            // expectedLiteralSuffix
                      std::nullopt,            // expectedCreateParams
                      SQL_NULLABLE,            // expectedNullable
                      SQL_FALSE,               // expectedCaseSensitive
                      SQL_SEARCHABLE,          // expectedSearchable
                      SQL_FALSE,               // expectedUnsignedAttr
                      SQL_FALSE,               // expectedFixedPrecScale
                      NULL,                    // expectedAutoUniqueValue
                      std::wstring(L"float"),  // expectedLocalTypeName
                      NULL,                    // expectedMinScale
                      NULL,                    // expectedMaxScale
                      SQL_FLOAT,               // expectedSqlDataType
                      NULL,                    // expectedSqlDatetimeSub
                      NULL,                    // expectedNumPrecRadix
                      NULL);                   // expectedIntervalPrec

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expectedTypeName
                      SQL_DOUBLE,               // expectedDataType
                      15,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"double"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_DOUBLE,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expectedTypeName
                      SQL_DOUBLE,                // expectedDataType
                      15,                        // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"numeric"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_DOUBLE,                // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expectedTypeName
                      SQL_WVARCHAR,              // expectedDataType
                      255,                       // expectedColumnSize
                      std::wstring(L"'"),        // expectedLiteralPrefix
                      std::wstring(L"'"),        // expectedLiteralSuffix
                      std::wstring(L"length"),   // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"varchar"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_WVARCHAR,              // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expectedTypeName
                      SQL_TYPE_DATE,          // expectedDataType
                      10,                     // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"date"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_DATE,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expectedTypeName
                      SQL_TYPE_TIME,          // expectedDataType
                      8,                      // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"time"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_TIME,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expectedTypeName
                      SQL_TYPE_TIMESTAMP,          // expectedDataType
                      32,                          // expectedColumnSize
                      std::wstring(L"'"),          // expectedLiteralPrefix
                      std::wstring(L"'"),          // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      SQL_FALSE,                   // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"timestamp"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_DATETIME,                // expectedSqlDataType
                      SQL_CODE_TIMESTAMP,          // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoAllTypesODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expectedTypeName
                      SQL_BIT,               // expectedDataType
                      1,                     // expectedColumnSize
                      std::nullopt,          // expectedLiteralPrefix
                      std::nullopt,          // expectedLiteralSuffix
                      std::nullopt,          // expectedCreateParams
                      SQL_NULLABLE,          // expectedNullable
                      SQL_FALSE,             // expectedCaseSensitive
                      SQL_SEARCHABLE,        // expectedSearchable
                      NULL,                  // expectedUnsignedAttr
                      SQL_FALSE,             // expectedFixedPrecScale
                      NULL,                  // expectedAutoUniqueValue
                      std::wstring(L"bit"),  // expectedLocalTypeName
                      NULL,                  // expectedMinScale
                      NULL,                  // expectedMaxScale
                      SQL_BIT,               // expectedSqlDataType
                      NULL,                  // expectedSqlDatetimeSub
                      NULL,                  // expectedNumPrecRadix
                      NULL);                 // expectedIntervalPrec

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expectedTypeName
                      SQL_TINYINT,               // expectedDataType
                      3,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"tinyint"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_TINYINT,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expectedTypeName
                      SQL_BIGINT,               // expectedDataType
                      19,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"bigint"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_BIGINT,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expectedTypeName
                      SQL_LONGVARBINARY,               // expectedDataType
                      65536,                           // expectedColumnSize
                      std::nullopt,                    // expectedLiteralPrefix
                      std::nullopt,                    // expectedLiteralSuffix
                      std::nullopt,                    // expectedCreateParams
                      SQL_NULLABLE,                    // expectedNullable
                      SQL_FALSE,                       // expectedCaseSensitive
                      SQL_SEARCHABLE,                  // expectedSearchable
                      NULL,                            // expectedUnsignedAttr
                      SQL_FALSE,                       // expectedFixedPrecScale
                      NULL,                            // expectedAutoUniqueValue
                      std::wstring(L"longvarbinary"),  // expectedLocalTypeName
                      NULL,                            // expectedMinScale
                      NULL,                            // expectedMaxScale
                      SQL_LONGVARBINARY,               // expectedSqlDataType
                      NULL,                            // expectedSqlDatetimeSub
                      NULL,                            // expectedNumPrecRadix
                      NULL);                           // expectedIntervalPrec

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expectedTypeName
                      SQL_VARBINARY,               // expectedDataType
                      255,                         // expectedColumnSize
                      std::nullopt,                // expectedLiteralPrefix
                      std::nullopt,                // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      NULL,                        // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"varbinary"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_VARBINARY,               // expectedSqlDataType
                      NULL,                        // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expectedTypeName
                      SQL_WLONGVARCHAR,         // expectedDataType
                      65536,                    // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"text"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WLONGVARCHAR,         // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expectedTypeName
                      SQL_WLONGVARCHAR,              // expectedDataType
                      65536,                         // expectedColumnSize
                      std::wstring(L"'"),            // expectedLiteralPrefix
                      std::wstring(L"'"),            // expectedLiteralSuffix
                      std::wstring(L"length"),       // expectedCreateParams
                      SQL_NULLABLE,                  // expectedNullable
                      SQL_FALSE,                     // expectedCaseSensitive
                      SQL_SEARCHABLE,                // expectedSearchable
                      NULL,                          // expectedUnsignedAttr
                      SQL_FALSE,                     // expectedFixedPrecScale
                      NULL,                          // expectedAutoUniqueValue
                      std::wstring(L"longvarchar"),  // expectedLocalTypeName
                      NULL,                          // expectedMinScale
                      NULL,                          // expectedMaxScale
                      SQL_WLONGVARCHAR,              // expectedSqlDataType
                      NULL,                          // expectedSqlDatetimeSub
                      NULL,                          // expectedNumPrecRadix
                      NULL);                         // expectedIntervalPrec

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expectedTypeName
                      SQL_WCHAR,                // expectedDataType
                      255,                      // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"char"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WCHAR,                // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expectedTypeName
                      SQL_INTEGER,               // expectedDataType
                      9,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"integer"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_INTEGER,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expectedTypeName
                      SQL_SMALLINT,               // expectedDataType
                      5,                          // expectedColumnSize
                      std::nullopt,               // expectedLiteralPrefix
                      std::nullopt,               // expectedLiteralSuffix
                      std::nullopt,               // expectedCreateParams
                      SQL_NULLABLE,               // expectedNullable
                      SQL_FALSE,                  // expectedCaseSensitive
                      SQL_SEARCHABLE,             // expectedSearchable
                      SQL_FALSE,                  // expectedUnsignedAttr
                      SQL_FALSE,                  // expectedFixedPrecScale
                      NULL,                       // expectedAutoUniqueValue
                      std::wstring(L"smallint"),  // expectedLocalTypeName
                      NULL,                       // expectedMinScale
                      NULL,                       // expectedMaxScale
                      SQL_SMALLINT,               // expectedSqlDataType
                      NULL,                       // expectedSqlDatetimeSub
                      NULL,                       // expectedNumPrecRadix
                      NULL);                      // expectedIntervalPrec

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expectedTypeName
                      SQL_FLOAT,               // expectedDataType
                      7,                       // expectedColumnSize
                      std::nullopt,            // expectedLiteralPrefix
                      std::nullopt,            // expectedLiteralSuffix
                      std::nullopt,            // expectedCreateParams
                      SQL_NULLABLE,            // expectedNullable
                      SQL_FALSE,               // expectedCaseSensitive
                      SQL_SEARCHABLE,          // expectedSearchable
                      SQL_FALSE,               // expectedUnsignedAttr
                      SQL_FALSE,               // expectedFixedPrecScale
                      NULL,                    // expectedAutoUniqueValue
                      std::wstring(L"float"),  // expectedLocalTypeName
                      NULL,                    // expectedMinScale
                      NULL,                    // expectedMaxScale
                      SQL_FLOAT,               // expectedSqlDataType
                      NULL,                    // expectedSqlDatetimeSub
                      NULL,                    // expectedNumPrecRadix
                      NULL);                   // expectedIntervalPrec

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expectedTypeName
                      SQL_DOUBLE,               // expectedDataType
                      15,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"double"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_DOUBLE,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expectedTypeName
                      SQL_DOUBLE,                // expectedDataType
                      15,                        // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"numeric"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_DOUBLE,                // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expectedTypeName
                      SQL_WVARCHAR,              // expectedDataType
                      255,                       // expectedColumnSize
                      std::wstring(L"'"),        // expectedLiteralPrefix
                      std::wstring(L"'"),        // expectedLiteralSuffix
                      std::wstring(L"length"),   // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"varchar"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_WVARCHAR,              // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expectedTypeName
                      SQL_DATE,               // expectedDataType
                      10,                     // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"date"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expectedTypeName
                      SQL_TIME,               // expectedDataType
                      8,                      // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"time"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expectedTypeName
                      SQL_TIMESTAMP,               // expectedDataType
                      32,                          // expectedColumnSize
                      std::wstring(L"'"),          // expectedLiteralPrefix
                      std::wstring(L"'"),          // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      SQL_FALSE,                   // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"timestamp"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_DATETIME,                // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoBit) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_BIT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expectedTypeName
                      SQL_BIT,               // expectedDataType
                      1,                     // expectedColumnSize
                      std::nullopt,          // expectedLiteralPrefix
                      std::nullopt,          // expectedLiteralSuffix
                      std::nullopt,          // expectedCreateParams
                      SQL_NULLABLE,          // expectedNullable
                      SQL_FALSE,             // expectedCaseSensitive
                      SQL_SEARCHABLE,        // expectedSearchable
                      NULL,                  // expectedUnsignedAttr
                      SQL_FALSE,             // expectedFixedPrecScale
                      NULL,                  // expectedAutoUniqueValue
                      std::wstring(L"bit"),  // expectedLocalTypeName
                      NULL,                  // expectedMinScale
                      NULL,                  // expectedMaxScale
                      SQL_BIT,               // expectedSqlDataType
                      NULL,                  // expectedSqlDatetimeSub
                      NULL,                  // expectedNumPrecRadix
                      NULL);                 // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoTinyInt) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TINYINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expectedTypeName
                      SQL_TINYINT,               // expectedDataType
                      3,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"tinyint"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_TINYINT,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoBigInt) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_BIGINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expectedTypeName
                      SQL_BIGINT,               // expectedDataType
                      19,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"bigint"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_BIGINT,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoLongVarbinary) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_LONGVARBINARY);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expectedTypeName
                      SQL_LONGVARBINARY,               // expectedDataType
                      65536,                           // expectedColumnSize
                      std::nullopt,                    // expectedLiteralPrefix
                      std::nullopt,                    // expectedLiteralSuffix
                      std::nullopt,                    // expectedCreateParams
                      SQL_NULLABLE,                    // expectedNullable
                      SQL_FALSE,                       // expectedCaseSensitive
                      SQL_SEARCHABLE,                  // expectedSearchable
                      NULL,                            // expectedUnsignedAttr
                      SQL_FALSE,                       // expectedFixedPrecScale
                      NULL,                            // expectedAutoUniqueValue
                      std::wstring(L"longvarbinary"),  // expectedLocalTypeName
                      NULL,                            // expectedMinScale
                      NULL,                            // expectedMaxScale
                      SQL_LONGVARBINARY,               // expectedSqlDataType
                      NULL,                            // expectedSqlDatetimeSub
                      NULL,                            // expectedNumPrecRadix
                      NULL);                           // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoVarbinary) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_VARBINARY);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expectedTypeName
                      SQL_VARBINARY,               // expectedDataType
                      255,                         // expectedColumnSize
                      std::nullopt,                // expectedLiteralPrefix
                      std::nullopt,                // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      NULL,                        // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"varbinary"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_VARBINARY,               // expectedSqlDataType
                      NULL,                        // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoLongVarchar) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WLONGVARCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expectedTypeName
                      SQL_WLONGVARCHAR,         // expectedDataType
                      65536,                    // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"text"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WLONGVARCHAR,         // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expectedTypeName
                      SQL_WLONGVARCHAR,              // expectedDataType
                      65536,                         // expectedColumnSize
                      std::wstring(L"'"),            // expectedLiteralPrefix
                      std::wstring(L"'"),            // expectedLiteralSuffix
                      std::wstring(L"length"),       // expectedCreateParams
                      SQL_NULLABLE,                  // expectedNullable
                      SQL_FALSE,                     // expectedCaseSensitive
                      SQL_SEARCHABLE,                // expectedSearchable
                      NULL,                          // expectedUnsignedAttr
                      SQL_FALSE,                     // expectedFixedPrecScale
                      NULL,                          // expectedAutoUniqueValue
                      std::wstring(L"longvarchar"),  // expectedLocalTypeName
                      NULL,                          // expectedMinScale
                      NULL,                          // expectedMaxScale
                      SQL_WLONGVARCHAR,              // expectedSqlDataType
                      NULL,                          // expectedSqlDatetimeSub
                      NULL,                          // expectedNumPrecRadix
                      NULL);                         // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoChar) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expectedTypeName
                      SQL_WCHAR,                // expectedDataType
                      255,                      // expectedColumnSize
                      std::wstring(L"'"),       // expectedLiteralPrefix
                      std::wstring(L"'"),       // expectedLiteralSuffix
                      std::wstring(L"length"),  // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      NULL,                     // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"char"),    // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_WCHAR,                // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoInteger) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_INTEGER);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expectedTypeName
                      SQL_INTEGER,               // expectedDataType
                      9,                         // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"integer"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_INTEGER,               // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSmallInt) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_SMALLINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expectedTypeName
                      SQL_SMALLINT,               // expectedDataType
                      5,                          // expectedColumnSize
                      std::nullopt,               // expectedLiteralPrefix
                      std::nullopt,               // expectedLiteralSuffix
                      std::nullopt,               // expectedCreateParams
                      SQL_NULLABLE,               // expectedNullable
                      SQL_FALSE,                  // expectedCaseSensitive
                      SQL_SEARCHABLE,             // expectedSearchable
                      SQL_FALSE,                  // expectedUnsignedAttr
                      SQL_FALSE,                  // expectedFixedPrecScale
                      NULL,                       // expectedAutoUniqueValue
                      std::wstring(L"smallint"),  // expectedLocalTypeName
                      NULL,                       // expectedMinScale
                      NULL,                       // expectedMaxScale
                      SQL_SMALLINT,               // expectedSqlDataType
                      NULL,                       // expectedSqlDatetimeSub
                      NULL,                       // expectedNumPrecRadix
                      NULL);                      // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoFloat) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_FLOAT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expectedTypeName
                      SQL_FLOAT,               // expectedDataType
                      7,                       // expectedColumnSize
                      std::nullopt,            // expectedLiteralPrefix
                      std::nullopt,            // expectedLiteralSuffix
                      std::nullopt,            // expectedCreateParams
                      SQL_NULLABLE,            // expectedNullable
                      SQL_FALSE,               // expectedCaseSensitive
                      SQL_SEARCHABLE,          // expectedSearchable
                      SQL_FALSE,               // expectedUnsignedAttr
                      SQL_FALSE,               // expectedFixedPrecScale
                      NULL,                    // expectedAutoUniqueValue
                      std::wstring(L"float"),  // expectedLocalTypeName
                      NULL,                    // expectedMinScale
                      NULL,                    // expectedMaxScale
                      SQL_FLOAT,               // expectedSqlDataType
                      NULL,                    // expectedSqlDatetimeSub
                      NULL,                    // expectedNumPrecRadix
                      NULL);                   // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoDouble) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DOUBLE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expectedTypeName
                      SQL_DOUBLE,               // expectedDataType
                      15,                       // expectedColumnSize
                      std::nullopt,             // expectedLiteralPrefix
                      std::nullopt,             // expectedLiteralSuffix
                      std::nullopt,             // expectedCreateParams
                      SQL_NULLABLE,             // expectedNullable
                      SQL_FALSE,                // expectedCaseSensitive
                      SQL_SEARCHABLE,           // expectedSearchable
                      SQL_FALSE,                // expectedUnsignedAttr
                      SQL_FALSE,                // expectedFixedPrecScale
                      NULL,                     // expectedAutoUniqueValue
                      std::wstring(L"double"),  // expectedLocalTypeName
                      NULL,                     // expectedMinScale
                      NULL,                     // expectedMaxScale
                      SQL_DOUBLE,               // expectedSqlDataType
                      NULL,                     // expectedSqlDatetimeSub
                      NULL,                     // expectedNumPrecRadix
                      NULL);                    // expectedIntervalPrec

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expectedTypeName
                      SQL_DOUBLE,                // expectedDataType
                      15,                        // expectedColumnSize
                      std::nullopt,              // expectedLiteralPrefix
                      std::nullopt,              // expectedLiteralSuffix
                      std::nullopt,              // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"numeric"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_DOUBLE,                // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoVarchar) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WVARCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expectedTypeName
                      SQL_WVARCHAR,              // expectedDataType
                      255,                       // expectedColumnSize
                      std::wstring(L"'"),        // expectedLiteralPrefix
                      std::wstring(L"'"),        // expectedLiteralSuffix
                      std::wstring(L"length"),   // expectedCreateParams
                      SQL_NULLABLE,              // expectedNullable
                      SQL_FALSE,                 // expectedCaseSensitive
                      SQL_SEARCHABLE,            // expectedSearchable
                      SQL_FALSE,                 // expectedUnsignedAttr
                      SQL_FALSE,                 // expectedFixedPrecScale
                      NULL,                      // expectedAutoUniqueValue
                      std::wstring(L"varchar"),  // expectedLocalTypeName
                      NULL,                      // expectedMinScale
                      NULL,                      // expectedMaxScale
                      SQL_WVARCHAR,              // expectedSqlDataType
                      NULL,                      // expectedSqlDatetimeSub
                      NULL,                      // expectedNumPrecRadix
                      NULL);                     // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeDate) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expectedTypeName
                      SQL_TYPE_DATE,          // expectedDataType
                      10,                     // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"date"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_DATE,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLDate) {
  this->connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expectedTypeName
                      SQL_TYPE_DATE,          // expectedDataType
                      10,                     // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"date"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_DATE,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoDateODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expectedTypeName
                      SQL_DATE,               // expectedDataType
                      10,                     // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"date"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeDateODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTime) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expectedTypeName
                      SQL_TYPE_TIME,          // expectedDataType
                      8,                      // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"time"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_TIME,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTime) {
  this->connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expectedTypeName
                      SQL_TYPE_TIME,          // expectedDataType
                      8,                      // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"time"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      SQL_CODE_TIME,          // expectedSqlDatetimeSub
                      NULL,                   // expectedNumPrecRadix
                      NULL);                  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoTimeODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expectedTypeName
                      SQL_TIME,               // expectedDataType
                      8,                      // expectedColumnSize
                      std::wstring(L"'"),     // expectedLiteralPrefix
                      std::wstring(L"'"),     // expectedLiteralSuffix
                      std::nullopt,           // expectedCreateParams
                      SQL_NULLABLE,           // expectedNullable
                      SQL_FALSE,              // expectedCaseSensitive
                      SQL_SEARCHABLE,         // expectedSearchable
                      SQL_FALSE,              // expectedUnsignedAttr
                      SQL_FALSE,              // expectedFixedPrecScale
                      NULL,                   // expectedAutoUniqueValue
                      std::wstring(L"time"),  // expectedLocalTypeName
                      NULL,                   // expectedMinScale
                      NULL,                   // expectedMaxScale
                      SQL_DATETIME,           // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimeODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimestamp) {
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expectedTypeName
                      SQL_TYPE_TIMESTAMP,          // expectedDataType
                      32,                          // expectedColumnSize
                      std::wstring(L"'"),          // expectedLiteralPrefix
                      std::wstring(L"'"),          // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      SQL_FALSE,                   // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"timestamp"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_DATETIME,                // expectedSqlDataType
                      SQL_CODE_TIMESTAMP,          // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTimestamp) {
  this->connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expectedTypeName
                      SQL_TYPE_TIMESTAMP,          // expectedDataType
                      32,                          // expectedColumnSize
                      std::wstring(L"'"),          // expectedLiteralPrefix
                      std::wstring(L"'"),          // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      SQL_FALSE,                   // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"timestamp"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_DATETIME,                // expectedSqlDataType
                      SQL_CODE_TIMESTAMP,          // expectedSqlDatetimeSub
                      NULL,                        // expectedNumPrecRadix
                      NULL);                       // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTimestampODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expectedTypeName
                      SQL_TIMESTAMP,               // expectedDataType
                      32,                          // expectedColumnSize
                      std::wstring(L"'"),          // expectedLiteralPrefix
                      std::wstring(L"'"),          // expectedLiteralSuffix
                      std::nullopt,                // expectedCreateParams
                      SQL_NULLABLE,                // expectedNullable
                      SQL_FALSE,                   // expectedCaseSensitive
                      SQL_SEARCHABLE,              // expectedSearchable
                      SQL_FALSE,                   // expectedUnsignedAttr
                      SQL_FALSE,                   // expectedFixedPrecScale
                      NULL,                        // expectedAutoUniqueValue
                      std::wstring(L"timestamp"),  // expectedLocalTypeName
                      NULL,                        // expectedMinScale
                      NULL,                        // expectedMaxScale
                      SQL_DATETIME,                // expectedSqlDataType
                      NULL,   // expectedSqlDatetimeSub, driver returns NULL for Ver2
                      NULL,   // expectedNumPrecRadix
                      NULL);  // expectedIntervalPrec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimestampODBCVer2) {
  this->connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoInvalidDataType) {
  this->connect();

  SQLSMALLINT invalidDataType = -114;
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, invalidDataType);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY004);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetTypeInfoUnsupportedDataType) {
  // Assumes mock and remote server don't support GUID data type
  this->connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_GUID);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Result set is empty with valid data type that is unsupported by the server
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
