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
// Helper functions
void checkSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedTable, const std::wstring& expectedColumn,
    const SQLINTEGER& expectedDataType, const std::wstring& expectedTypeName,
    const SQLINTEGER& expectedColumnSize, const SQLINTEGER& expectedBufferLength,
    const SQLSMALLINT& expectedDecimalDigits, const SQLSMALLINT& expectedNumPrecRadix,
    const SQLSMALLINT& expectedNullable, const SQLSMALLINT& expectedSqlDataType,
    const SQLSMALLINT& expectedDateTimeSub, const SQLINTEGER& expectedOctetCharLength,
    const SQLINTEGER& expectedOrdinalPosition, const std::wstring& expectedIsNullable) {
  CheckStringColumnW(stmt, 3, expectedTable);   // table name
  CheckStringColumnW(stmt, 4, expectedColumn);  // column name

  CheckIntColumn(stmt, 5, expectedDataType);  // data type

  CheckStringColumnW(stmt, 6, expectedTypeName);  // type name

  CheckIntColumn(stmt, 7, expectedColumnSize);    // column size
  CheckIntColumn(stmt, 8, expectedBufferLength);  // buffer length

  CheckSmallIntColumn(stmt, 9, expectedDecimalDigits);  // decimal digits
  CheckSmallIntColumn(stmt, 10, expectedNumPrecRadix);  // num prec radix
  CheckSmallIntColumn(stmt, 11,
                      expectedNullable);  // nullable

  CheckNullColumnW(stmt, 12);  // remarks
  CheckNullColumnW(stmt, 13);  // column def

  CheckSmallIntColumn(stmt, 14, expectedSqlDataType);  // sql data type
  CheckSmallIntColumn(stmt, 15, expectedDateTimeSub);  // sql date type sub
  CheckIntColumn(stmt, 16, expectedOctetCharLength);   // char octet length
  CheckIntColumn(stmt, 17,
                 expectedOrdinalPosition);  // oridinal position

  CheckStringColumnW(stmt, 18, expectedIsNullable);  // is nullable
}

void checkMockSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedCatalog, const std::wstring& expectedTable,
    const std::wstring& expectedColumn, const SQLINTEGER& expectedDataType,
    const std::wstring& expectedTypeName, const SQLINTEGER& expectedColumnSize,
    const SQLINTEGER& expectedBufferLength, const SQLSMALLINT& expectedDecimalDigits,
    const SQLSMALLINT& expectedNumPrecRadix, const SQLSMALLINT& expectedNullable,
    const SQLSMALLINT& expectedSqlDataType, const SQLSMALLINT& expectedDateTimeSub,
    const SQLINTEGER& expectedOctetCharLength, const SQLINTEGER& expectedOrdinalPosition,
    const std::wstring& expectedIsNullable) {
  CheckStringColumnW(stmt, 1, expectedCatalog);  // catalog
  CheckNullColumnW(stmt, 2);                     // schema

  checkSQLColumns(stmt, expectedTable, expectedColumn, expectedDataType, expectedTypeName,
                  expectedColumnSize, expectedBufferLength, expectedDecimalDigits,
                  expectedNumPrecRadix, expectedNullable, expectedSqlDataType,
                  expectedDateTimeSub, expectedOctetCharLength, expectedOrdinalPosition,
                  expectedIsNullable);
}

void checkRemoteSQLColumns(
    SQLHSTMT stmt, const std::wstring& expectedSchema, const std::wstring& expectedTable,
    const std::wstring& expectedColumn, const SQLINTEGER& expectedDataType,
    const std::wstring& expectedTypeName, const SQLINTEGER& expectedColumnSize,
    const SQLINTEGER& expectedBufferLength, const SQLSMALLINT& expectedDecimalDigits,
    const SQLSMALLINT& expectedNumPrecRadix, const SQLSMALLINT& expectedNullable,
    const SQLSMALLINT& expectedSqlDataType, const SQLSMALLINT& expectedDateTimeSub,
    const SQLINTEGER& expectedOctetCharLength, const SQLINTEGER& expectedOrdinalPosition,
    const std::wstring& expectedIsNullable) {
  CheckNullColumnW(stmt, 1);                    // catalog
  CheckStringColumnW(stmt, 2, expectedSchema);  // schema
  checkSQLColumns(stmt, expectedTable, expectedColumn, expectedDataType, expectedTypeName,
                  expectedColumnSize, expectedBufferLength, expectedDecimalDigits,
                  expectedNumPrecRadix, expectedNullable, expectedSqlDataType,
                  expectedDateTimeSub, expectedOctetCharLength, expectedOrdinalPosition,
                  expectedIsNullable);
}

void checkSQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT idx,
                          const std::wstring& expectedColmnName, SQLLEN expectedDataType,
                          SQLLEN expectedConciseType, SQLLEN expectedDisplaySize,
                          SQLLEN expectedPrecScale, SQLLEN expectedLength,
                          const std::wstring& expectedLiteralPrefix,
                          const std::wstring& expectedLiteralSuffix,
                          SQLLEN expectedColumnSize, SQLLEN expectedColumnScale,
                          SQLLEN expectedColumnNullability, SQLLEN expectedNumPrecRadix,
                          SQLLEN expectedOctetLength, SQLLEN expectedSearchable,
                          SQLLEN expectedUnsignedColumn) {
  std::vector<SQLWCHAR> name(ODBC_BUFFER_SIZE);
  SQLSMALLINT nameLen = 0;
  std::vector<SQLWCHAR> baseColumnName(ODBC_BUFFER_SIZE);
  SQLSMALLINT columnNameLen = 0;
  std::vector<SQLWCHAR> label(ODBC_BUFFER_SIZE);
  SQLSMALLINT labelLen = 0;
  std::vector<SQLWCHAR> prefix(ODBC_BUFFER_SIZE);
  SQLSMALLINT prefixLen = 0;
  std::vector<SQLWCHAR> suffix(ODBC_BUFFER_SIZE);
  SQLSMALLINT suffixLen = 0;
  SQLLEN dataType = 0;
  SQLLEN conciseType = 0;
  SQLLEN displaySize = 0;
  SQLLEN precScale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN numPrecRadix = 0;
  SQLLEN octetLength = 0;
  SQLLEN searchable = 0;
  SQLLEN unsignedCol = 0;

  SQLRETURN ret = SQLColAttribute(stmt, idx, SQL_DESC_NAME, &name[0],
                                  (SQLSMALLINT)name.size(), &nameLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_BASE_COLUMN_NAME, &baseColumnName[0],
                        (SQLSMALLINT)baseColumnName.size(), &columnNameLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LABEL, &label[0], (SQLSMALLINT)label.size(),
                        &labelLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_TYPE, 0, 0, 0, &dataType);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_CONCISE_TYPE, 0, 0, 0, &conciseType);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_DISPLAY_SIZE, 0, 0, 0, &displaySize);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_FIXED_PREC_SCALE, 0, 0, 0, &precScale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LENGTH, 0, 0, 0, &length);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_PREFIX, &prefix[0],
                        (SQLSMALLINT)prefix.size(), &prefixLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_SUFFIX, &suffix[0],
                        (SQLSMALLINT)suffix.size(), &suffixLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_PRECISION, 0, 0, 0, &size);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_SCALE, 0, 0, 0, &scale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_NULLABLE, 0, 0, 0, &nullability);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_NUM_PREC_RADIX, 0, 0, 0, &numPrecRadix);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &octetLength);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_SEARCHABLE, 0, 0, 0, &searchable);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_DESC_UNSIGNED, 0, 0, 0, &unsignedCol);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring nameStr = ConvertToWString(name, nameLen);
  std::wstring baseColumnNameStr = ConvertToWString(baseColumnName, columnNameLen);
  std::wstring labelStr = ConvertToWString(label, labelLen);
  std::wstring prefixStr = ConvertToWString(prefix, prefixLen);

  // Assume column name, base column name, and label are equivalent in the result set
  EXPECT_EQ(nameStr, expectedColmnName);
  EXPECT_EQ(baseColumnNameStr, expectedColmnName);
  EXPECT_EQ(labelStr, expectedColmnName);
  EXPECT_EQ(dataType, expectedDataType);
  EXPECT_EQ(conciseType, expectedConciseType);
  EXPECT_EQ(displaySize, expectedDisplaySize);
  EXPECT_EQ(precScale, expectedPrecScale);
  EXPECT_EQ(length, expectedLength);
  EXPECT_EQ(prefixStr, expectedLiteralPrefix);
  EXPECT_EQ(size, expectedColumnSize);
  EXPECT_EQ(scale, expectedColumnScale);
  EXPECT_EQ(nullability, expectedColumnNullability);
  EXPECT_EQ(numPrecRadix, expectedNumPrecRadix);
  EXPECT_EQ(octetLength, expectedOctetLength);
  EXPECT_EQ(searchable, expectedSearchable);
  EXPECT_EQ(unsignedCol, expectedUnsignedColumn);
}

void checkSQLColAttributes(SQLHSTMT stmt, SQLUSMALLINT idx,
                           const std::wstring& expectedColmnName, SQLLEN expectedDataType,
                           SQLLEN expectedDisplaySize, SQLLEN expectedPrecScale,
                           SQLLEN expectedLength, SQLLEN expectedColumnSize,
                           SQLLEN expectedColumnScale, SQLLEN expectedColumnNullability,
                           SQLLEN expectedSearchable, SQLLEN expectedUnsignedColumn) {
  std::vector<SQLWCHAR> name(ODBC_BUFFER_SIZE);
  SQLSMALLINT nameLen = 0;
  std::vector<SQLWCHAR> label(ODBC_BUFFER_SIZE);
  SQLSMALLINT labelLen = 0;
  SQLLEN dataType = 0;
  SQLLEN displaySize = 0;
  SQLLEN precScale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN searchable = 0;
  SQLLEN unsignedCol = 0;

  SQLRETURN ret = SQLColAttributes(stmt, idx, SQL_COLUMN_NAME, &name[0],
                                   (SQLSMALLINT)name.size(), &nameLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_LABEL, &label[0],
                         (SQLSMALLINT)label.size(), &labelLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_TYPE, 0, 0, 0, &dataType);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_DISPLAY_SIZE, 0, 0, 0, &displaySize);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(stmt, idx, SQL_COLUMN_MONEY, 0, 0, 0, &precScale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_LENGTH, 0, 0, 0, &length);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_PRECISION, 0, 0, 0, &size);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_SCALE, 0, 0, 0, &scale);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_NULLABLE, 0, 0, 0, &nullability);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_SEARCHABLE, 0, 0, 0, &searchable);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttributes(stmt, idx, SQL_COLUMN_UNSIGNED, 0, 0, 0, &unsignedCol);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring nameStr = ConvertToWString(name, nameLen);
  std::wstring labelStr = ConvertToWString(label, labelLen);

  EXPECT_EQ(nameStr, expectedColmnName);
  EXPECT_EQ(labelStr, expectedColmnName);
  EXPECT_EQ(dataType, expectedDataType);
  EXPECT_EQ(displaySize, expectedDisplaySize);
  EXPECT_EQ(length, expectedLength);
  EXPECT_EQ(size, expectedColumnSize);
  EXPECT_EQ(scale, expectedColumnScale);
  EXPECT_EQ(nullability, expectedColumnNullability);
  EXPECT_EQ(searchable, expectedSearchable);
  EXPECT_EQ(unsignedCol, expectedUnsignedColumn);
}

void checkSQLColAttributeString(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                                SQLUSMALLINT fieldIdentifier,
                                const std::wstring& expectedAttrString) {
  // Execute query and check SQLColAttribute string attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::vector<SQLWCHAR> strVal(ODBC_BUFFER_SIZE);
  SQLSMALLINT strLen = 0;

  ret = SQLColAttribute(stmt, idx, fieldIdentifier, &strVal[0],
                        (SQLSMALLINT)strVal.size(), &strLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring attrStr = ConvertToWString(strVal, strLen);
  EXPECT_EQ(attrStr, expectedAttrString);
}

void checkSQLColAttributeNumeric(SQLHSTMT stmt, const std::wstring& wsql,
                                 SQLUSMALLINT idx, SQLUSMALLINT fieldIdentifier,
                                 SQLLEN expectedAttrNumeric) {
  // Execute query and check SQLColAttribute numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLLEN numVal = 0;
  ret = SQLColAttribute(stmt, idx, fieldIdentifier, 0, 0, 0, &numVal);
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(numVal, expectedAttrNumeric);
}

void checkSQLColAttributesString(SQLHSTMT stmt, const std::wstring& wsql,
                                 SQLUSMALLINT idx, SQLUSMALLINT fieldIdentifier,
                                 const std::wstring& expectedAttrString) {
  // Execute query and check ODBC 2.0 API SQLColAttributes string attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::vector<SQLWCHAR> strVal(ODBC_BUFFER_SIZE);
  SQLSMALLINT strLen = 0;

  ret = SQLColAttributes(stmt, idx, fieldIdentifier, &strVal[0],
                         (SQLSMALLINT)strVal.size(), &strLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring attrStr = ConvertToWString(strVal, strLen);
  EXPECT_EQ(attrStr, expectedAttrString);
}

void checkSQLColAttributesNumeric(SQLHSTMT stmt, const std::wstring& wsql,
                                  SQLUSMALLINT idx, SQLUSMALLINT fieldIdentifier,
                                  SQLLEN expectedAttrNumeric) {
  // Execute query and check ODBC 2.0 API SQLColAttributes numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  SQLRETURN ret = SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLLEN numVal = 0;
  ret = SQLColAttributes(stmt, idx, fieldIdentifier, 0, 0, 0, &numVal);
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(numVal, expectedAttrNumeric);
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColumnsTestInputData) {
  this->connect();

  SQLWCHAR catalogName[] = L"";
  SQLWCHAR schemaName[] = L"";
  SQLWCHAR tableName[] = L"";
  SQLWCHAR columnName[] = L"";

  // All values populated
  SQLRETURN ret = SQLColumns(this->stmt, catalogName, sizeof(catalogName), schemaName,
                             sizeof(schemaName), tableName, sizeof(tableName), columnName,
                             sizeof(columnName));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Sizes are nulls
  ret =
      SQLColumns(this->stmt, catalogName, 0, schemaName, 0, tableName, 0, columnName, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Values are nulls
  ret = SQLColumns(this->stmt, 0, sizeof(catalogName), 0, sizeof(schemaName), 0,
                   sizeof(tableName), 0, sizeof(columnName));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);
  // Close statement cursor to avoid leaving in an invalid state
  SQLFreeStmt(this->stmt, SQL_CLOSE);

  // All values and sizes are nulls
  ret = SQLColumns(this->stmt, 0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsAllColumns) {
  // Check table pattern and column pattern returns all columns
  this->connect();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"%";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // mock limitation: SQLite mock server returns 10 for bigint size when spec indicates
  // should be 19
  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"foreignName"),   // expectedColumn
                      SQL_WVARCHAR,                   // expectedDataType
                      std::wstring(L"WVARCHAR"),      // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expectedBufferLength
                      15,  // expectedDecimalDigits
                      0,   // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_WVARCHAR,           // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      0,                      // expectedOctetCharLength
                      2,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"value"),         // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      3,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"id"),        // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"keyName"),   // expectedColumn
                      SQL_WVARCHAR,               // expectedDataType
                      std::wstring(L"WVARCHAR"),  // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expectedBufferLength
                      15,  // expectedDecimalDigits
                      0,   // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_WVARCHAR,           // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      0,                      // expectedOctetCharLength
                      2,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"value"),     // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      3,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),       // expectedCatalog
                      std::wstring(L"intTable"),   // expectedTable
                      std::wstring(L"foreignId"),  // expectedColumn
                      SQL_BIGINT,                  // expectedDataType
                      std::wstring(L"BIGINT"),     // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      4,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsAllTypes) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.

  // Checks filtering table with table name pattern
  this->connect();
  this->CreateTableAllDataType();

  // Attempt to get all columns from AllTypesTable
  SQLWCHAR tablePattern[] = L"AllTypesTable";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch SQLColumn data for 1st column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expectedCatalog
                      std::wstring(L"AllTypesTable"),  // expectedTable
                      std::wstring(L"bigint_col"),     // expectedColumn
                      SQL_BIGINT,                      // expectedDataType
                      std::wstring(L"BIGINT"),         // expectedTypeName
                      10,  // expectedColumnSize (mock server limitation: returns 10,
                           // the ODBC spec expects 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock server limitation: returns 15,
                           // the ODBC spec expects 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 2nd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expectedCatalog
                      std::wstring(L"AllTypesTable"),  // expectedTable
                      std::wstring(L"char_col"),       // expectedColumn
                      SQL_WVARCHAR,                    // expectedDataType
                      std::wstring(L"WVARCHAR"),       // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expectedBufferLength
                      15,  // expectedDecimalDigits
                      0,   // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_WVARCHAR,           // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      0,                      // expectedOctetCharLength
                      2,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 3rd column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expectedCatalog
                      std::wstring(L"AllTypesTable"),  // expectedTable
                      std::wstring(L"varbinary_col"),  // expectedColumn
                      SQL_BINARY,                      // expectedDataType
                      std::wstring(L"BINARY"),         // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // BLOB column, spec expects binary data limit)
                      0,   // expectedBufferLength
                      15,  // expectedDecimalDigits
                      0,   // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BINARY,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      0,                      // expectedOctetCharLength
                      3,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check SQLColumn data for 4th column in AllTypesTable
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expectedCatalog
                      std::wstring(L"AllTypesTable"),  // expectedTable
                      std::wstring(L"double_col"),     // expectedColumn
                      SQL_DOUBLE,                      // expectedDataType
                      std::wstring(L"DOUBLE"),         // expectedTypeName
                      15,                              // expectedColumnSize
                      8,                               // expectedBufferLength
                      15,                              // expectedDecimalDigits
                      2,                               // expectedNumPrecRadix
                      SQL_NULLABLE,                    // expectedNullable
                      SQL_DOUBLE,                      // expectedSqlDataType
                      NULL,                            // expectedDateTimeSub
                      8,                               // expectedOctetCharLength
                      4,                               // expectedOrdinalPosition
                      std::wstring(L"YES"));           // expectedIsNullable

  // There should be no more column data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsUnicode) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.
  this->connect();
  this->CreateUnicodeTable();

  // Attempt to get all columns
  SQLWCHAR tablePattern[] = L"数据";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check SQLColumn data for 1st column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"数据"),      // expectedTable
                      std::wstring(L"资料"),      // expectedColumn
                      SQL_WVARCHAR,               // expectedDataType
                      std::wstring(L"WVARCHAR"),  // expectedTypeName
                      0,   // expectedColumnSize (mock server limitation: returns 0 for
                           // varchar(100), spec expects 100)
                      0,   // expectedBufferLength
                      15,  // expectedDecimalDigits
                      0,   // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_WVARCHAR,           // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      0,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // There should be no more column data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColumnsAllTypes) {
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits
  this->connect();

  SQLWCHAR tablePattern[] = L"ODBCTest";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),      // expectedSchema
                        std::wstring(L"ODBCTest"),      // expectedTable
                        std::wstring(L"sinteger_max"),  // expectedColumn
                        SQL_INTEGER,                    // expectedDataType
                        std::wstring(L"INTEGER"),       // expectedTypeName
                        32,  // expectedColumnSize (remote server returns number of bits)
                        4,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_INTEGER,            // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        4,                      // expectedOctetCharLength
                        1,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),     // expectedSchema
                        std::wstring(L"ODBCTest"),     // expectedTable
                        std::wstring(L"sbigint_max"),  // expectedColumn
                        SQL_BIGINT,                    // expectedDataType
                        std::wstring(L"BIGINT"),       // expectedTypeName
                        64,  // expectedColumnSize (remote server returns number of bits)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIGINT,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        2,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expectedSchema
                        std::wstring(L"ODBCTest"),          // expectedTable
                        std::wstring(L"decimal_positive"),  // expectedColumn
                        SQL_DECIMAL,                        // expectedDataType
                        std::wstring(L"DECIMAL"),           // expectedTypeName
                        38,                                 // expectedColumnSize
                        19,                                 // expectedBufferLength
                        0,                                  // expectedDecimalDigits
                        10,                                 // expectedNumPrecRadix
                        SQL_NULLABLE,                       // expectedNullable
                        SQL_DECIMAL,                        // expectedSqlDataType
                        NULL,                               // expectedDateTimeSub
                        2,                                  // expectedOctetCharLength
                        3,                                  // expectedOrdinalPosition
                        std::wstring(L"YES"));              // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expectedSchema
                        std::wstring(L"ODBCTest"),   // expectedTable
                        std::wstring(L"float_max"),  // expectedColumn
                        SQL_FLOAT,                   // expectedDataType
                        std::wstring(L"FLOAT"),      // expectedTypeName
                        24,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_FLOAT,              // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        4,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expectedSchema
                        std::wstring(L"ODBCTest"),    // expectedTable
                        std::wstring(L"double_max"),  // expectedColumn
                        SQL_DOUBLE,                   // expectedDataType
                        std::wstring(L"DOUBLE"),      // expectedTypeName
                        53,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_DOUBLE,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        5,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expectedSchema
                        std::wstring(L"ODBCTest"),  // expectedTable
                        std::wstring(L"bit_true"),  // expectedColumn
                        SQL_BIT,                    // expectedDataType
                        std::wstring(L"BOOLEAN"),   // expectedTypeName
                        0,  // expectedColumnSize (limitation: remote server remote server
                            // returns 0, should be 1)
                        1,  // expectedBufferLength
                        0,  // expectedDecimalDigits
                        0,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIT,                // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        1,                      // expectedOctetCharLength
                        6,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // ODBC ver 3 returns SQL_TYPE_DATE, SQL_TYPE_TIME, and SQL_TYPE_TIMESTAMP in the
  // DATA_TYPE field

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"date_max"),  // expectedColumn
      SQL_TYPE_DATE,              // expectedDataType
      std::wstring(L"DATE"),      // expectedTypeName
      0,   // expectedColumnSize (limitation: remote server returns 0, should be 10)
      10,  // expectedBufferLength
      0,   // expectedDecimalDigits
      0,   // expectedNumPrecRadix
      SQL_NULLABLE,           // expectedNullable
      SQL_DATETIME,           // expectedSqlDataType
      SQL_CODE_DATE,          // expectedDateTimeSub
      6,                      // expectedOctetCharLength
      7,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 8th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"time_max"),  // expectedColumn
      SQL_TYPE_TIME,              // expectedDataType
      std::wstring(L"TIME"),      // expectedTypeName
      3,              // expectedColumnSize (limitation: should be 9+fractional digits)
      12,             // expectedBufferLength
      0,              // expectedDecimalDigits
      0,              // expectedNumPrecRadix
      SQL_NULLABLE,   // expectedNullable
      SQL_DATETIME,   // expectedSqlDataType
      SQL_CODE_TIME,  // expectedDateTimeSub
      6,              // expectedOctetCharLength
      8,              // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 9th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expectedSchema
      std::wstring(L"ODBCTest"),       // expectedTable
      std::wstring(L"timestamp_max"),  // expectedColumn
      SQL_TYPE_TIMESTAMP,              // expectedDataType
      std::wstring(L"TIMESTAMP"),      // expectedTypeName
      3,             // expectedColumnSize (limitation: should be 20+fractional digits)
      23,            // expectedBufferLength
      0,             // expectedDecimalDigits
      0,             // expectedNumPrecRadix
      SQL_NULLABLE,  // expectedNullable
      SQL_DATETIME,  // expectedSqlDataType
      SQL_CODE_TIMESTAMP,     // expectedDateTimeSub
      16,                     // expectedOctetCharLength
      9,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColumnsAllTypesODBCVer2) {
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits
  this->connect(SQL_OV_ODBC2);

  SQLWCHAR tablePattern[] = L"ODBCTest";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),      // expectedSchema
                        std::wstring(L"ODBCTest"),      // expectedTable
                        std::wstring(L"sinteger_max"),  // expectedColumn
                        SQL_INTEGER,                    // expectedDataType
                        std::wstring(L"INTEGER"),       // expectedTypeName
                        32,  // expectedColumnSize (remote server returns number of bits)
                        4,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_INTEGER,            // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        4,                      // expectedOctetCharLength
                        1,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),     // expectedSchema
                        std::wstring(L"ODBCTest"),     // expectedTable
                        std::wstring(L"sbigint_max"),  // expectedColumn
                        SQL_BIGINT,                    // expectedDataType
                        std::wstring(L"BIGINT"),       // expectedTypeName
                        64,  // expectedColumnSize (remote server returns number of bits)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        10,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIGINT,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        2,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 3rd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expectedSchema
                        std::wstring(L"ODBCTest"),          // expectedTable
                        std::wstring(L"decimal_positive"),  // expectedColumn
                        SQL_DECIMAL,                        // expectedDataType
                        std::wstring(L"DECIMAL"),           // expectedTypeName
                        38,                                 // expectedColumnSize
                        19,                                 // expectedBufferLength
                        0,                                  // expectedDecimalDigits
                        10,                                 // expectedNumPrecRadix
                        SQL_NULLABLE,                       // expectedNullable
                        SQL_DECIMAL,                        // expectedSqlDataType
                        NULL,                               // expectedDateTimeSub
                        2,                                  // expectedOctetCharLength
                        3,                                  // expectedOrdinalPosition
                        std::wstring(L"YES"));              // expectedIsNullable

  // Check 4th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expectedSchema
                        std::wstring(L"ODBCTest"),   // expectedTable
                        std::wstring(L"float_max"),  // expectedColumn
                        SQL_FLOAT,                   // expectedDataType
                        std::wstring(L"FLOAT"),      // expectedTypeName
                        24,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_FLOAT,              // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        4,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 5th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expectedSchema
                        std::wstring(L"ODBCTest"),    // expectedTable
                        std::wstring(L"double_max"),  // expectedColumn
                        SQL_DOUBLE,                   // expectedDataType
                        std::wstring(L"DOUBLE"),      // expectedTypeName
                        53,  // expectedColumnSize (precision bits from IEEE 754)
                        8,   // expectedBufferLength
                        0,   // expectedDecimalDigits
                        2,   // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_DOUBLE,             // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        8,                      // expectedOctetCharLength
                        5,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // Check 6th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expectedSchema
                        std::wstring(L"ODBCTest"),  // expectedTable
                        std::wstring(L"bit_true"),  // expectedColumn
                        SQL_BIT,                    // expectedDataType
                        std::wstring(L"BOOLEAN"),   // expectedTypeName
                        0,  // expectedColumnSize (limitation: remote server remote server
                            // returns 0, should be 1)
                        1,  // expectedBufferLength
                        0,  // expectedDecimalDigits
                        0,  // expectedNumPrecRadix
                        SQL_NULLABLE,           // expectedNullable
                        SQL_BIT,                // expectedSqlDataType
                        NULL,                   // expectedDateTimeSub
                        1,                      // expectedOctetCharLength
                        6,                      // expectedOrdinalPosition
                        std::wstring(L"YES"));  // expectedIsNullable

  // ODBC ver 2 returns SQL_DATE, SQL_TIME, and SQL_TIMESTAMP in the DATA_TYPE field

  // Check 7th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"date_max"),  // expectedColumn
      SQL_DATE,                   // expectedDataType
      std::wstring(L"DATE"),      // expectedTypeName
      0,   // expectedColumnSize (limitation: remote server returns 0, should be 10)
      10,  // expectedBufferLength
      0,   // expectedDecimalDigits
      0,   // expectedNumPrecRadix
      SQL_NULLABLE,           // expectedNullable
      SQL_DATETIME,           // expectedSqlDataType
      SQL_CODE_DATE,          // expectedDateTimeSub
      6,                      // expectedOctetCharLength
      7,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 8th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expectedSchema
      std::wstring(L"ODBCTest"),  // expectedTable
      std::wstring(L"time_max"),  // expectedColumn
      SQL_TIME,                   // expectedDataType
      std::wstring(L"TIME"),      // expectedTypeName
      3,              // expectedColumnSize (limitation: should be 9+fractional digits)
      12,             // expectedBufferLength
      0,              // expectedDecimalDigits
      0,              // expectedNumPrecRadix
      SQL_NULLABLE,   // expectedNullable
      SQL_DATETIME,   // expectedSqlDataType
      SQL_CODE_TIME,  // expectedDateTimeSub
      6,              // expectedOctetCharLength
      8,              // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // Check 9th Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expectedSchema
      std::wstring(L"ODBCTest"),       // expectedTable
      std::wstring(L"timestamp_max"),  // expectedColumn
      SQL_TIMESTAMP,                   // expectedDataType
      std::wstring(L"TIMESTAMP"),      // expectedTypeName
      3,             // expectedColumnSize (limitation: should be 20+fractional digits)
      23,            // expectedBufferLength
      0,             // expectedDecimalDigits
      0,             // expectedNumPrecRadix
      SQL_NULLABLE,  // expectedNullable
      SQL_DATETIME,  // expectedSqlDataType
      SQL_CODE_TIMESTAMP,     // expectedDateTimeSub
      16,                     // expectedOctetCharLength
      9,                      // expectedOrdinalPosition
      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsColumnPattern) {
  // Checks filtering table with column name pattern.
  // Only check table and column name
  this->connect();

  SQLWCHAR tablePattern[] = L"%";
  SQLWCHAR columnPattern[] = L"id";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // Check 2nd Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expectedCatalog
                      std::wstring(L"intTable"),  // expectedTable
                      std::wstring(L"id"),        // expectedColumn
                      SQL_BIGINT,                 // expectedDataType
                      std::wstring(L"BIGINT"),    // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsTableColumnPattern) {
  // Checks filtering table with table and column name pattern.
  // Only check table and column name
  this->connect();

  SQLWCHAR tablePattern[] = L"foreignTable";
  SQLWCHAR columnPattern[] = L"id";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check 1st Column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expectedCatalog
                      std::wstring(L"foreignTable"),  // expectedTable
                      std::wstring(L"id"),            // expectedColumn
                      SQL_BIGINT,                     // expectedDataType
                      std::wstring(L"BIGINT"),        // expectedTypeName
                      10,  // expectedColumnSize (mock returns 10 instead of 19)
                      8,   // expectedBufferLength
                      15,  // expectedDecimalDigits (mock returns 15 instead of 0)
                      10,  // expectedNumPrecRadix
                      SQL_NULLABLE,           // expectedNullable
                      SQL_BIGINT,             // expectedSqlDataType
                      NULL,                   // expectedDateTimeSub
                      8,                      // expectedOctetCharLength
                      1,                      // expectedOrdinalPosition
                      std::wstring(L"YES"));  // expectedIsNullable

  // There is no more column
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColumnsInvalidTablePattern) {
  this->connect();

  SQLWCHAR tablePattern[] = L"non-existent-table";
  SQLWCHAR columnPattern[] = L"%";

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, tablePattern,
                             SQL_NTS, columnPattern, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // There is no column from filter
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColAttributeTestInputData) {
  this->connect();

  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> characterAttr(ODBC_BUFFER_SIZE);
  SQLSMALLINT characterAttrLen = 0;
  SQLLEN numericAttr = 0;

  // All character values populated
  ret = SQLColAttribute(this->stmt, idx, SQL_DESC_NAME, &characterAttr[0],
                        (SQLSMALLINT)characterAttr.size(), &characterAttrLen, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // All numeric values populated
  ret = SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, &numericAttr);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Pass null values, driver should not throw error
  ret = SQLColAttribute(this->stmt, idx, SQL_COLUMN_TABLE_NAME, 0, 0, 0, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColAttributeGetCharacterLen) {
  this->connect();

  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLSMALLINT characterAttrLen = 0;

  // Check length of character attribute
  ret = SQLColAttribute(this->stmt, 1, SQL_DESC_BASE_COLUMN_NAME, 0, 0, &characterAttrLen,
                        0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(characterAttrLen, 4 * ODBC::GetSqlWCharSize());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColAttributeInvalidFieldId) {
  this->connect();

  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLUSMALLINT invalidFieldId = -100;
  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> characterAttr(ODBC_BUFFER_SIZE);
  SQLSMALLINT characterAttrLen = 0;
  SQLLEN numericAttr = 0;

  ret = SQLColAttribute(this->stmt, idx, invalidFieldId, &characterAttr[0],
                        (SQLSMALLINT)characterAttr.size(), &characterAttrLen, 0);
  EXPECT_EQ(ret, SQL_ERROR);
  // Verify invalid descriptor field identifier error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY091);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColAttributeInvalidColId) {
  this->connect();

  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLUSMALLINT invalidColId = 2;
  std::vector<SQLWCHAR> characterAttr(ODBC_BUFFER_SIZE);
  SQLSMALLINT characterAttrLen = 0;
  SQLLEN numericAttr = 0;

  ret = SQLColAttribute(this->stmt, invalidColId, SQL_DESC_BASE_COLUMN_NAME,
                        &characterAttr[0], (SQLSMALLINT)characterAttr.size(),
                        &characterAttrLen, 0);
  EXPECT_EQ(ret, SQL_ERROR);
  // Verify invalid descriptor index error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeAllTypes) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"bigint_col"),  // expectedColmnName
                       SQL_BIGINT,                   // expectedDataType
                       SQL_BIGINT,                   // expectedConciseType
                       20,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       10,                           // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"char_col"),  // expectedColmnName
                       SQL_WVARCHAR,               // expectedDataType
                       SQL_WVARCHAR,               // expectedConciseType
                       0,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       0,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       0,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       0,                          // expectedOctetLength
                       SQL_PRED_NONE,              // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"varbinary_col"),  // expectedColmnName
                       SQL_BINARY,                      // expectedDataType
                       SQL_BINARY,                      // expectedConciseType
                       0,                               // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       0,                               // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       0,                               // expectedColumnSize
                       0,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       0,                               // expectedOctetLength
                       SQL_PRED_NONE,                   // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"double_col"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_PRED_NONE,                // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);
  checkSQLColAttributes(this->stmt, 1,
                        std::wstring(L"bigint_col"),  // expectedColmnName
                        SQL_BIGINT,                   // expectedDataType
                        20,                           // expectedDisplaySize
                        SQL_FALSE,                    // expectedPrecScale
                        8,                            // expectedLength
                        8,                            // expectedColumnSize
                        0,                            // expectedColumnScale
                        SQL_NULLABLE,                 // expectedColumnNullability
                        SQL_PRED_NONE,                // expectedSearchable
                        SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 2,
                        std::wstring(L"char_col"),  // expectedColmnName
                        SQL_WVARCHAR,               // expectedDataType
                        0,                          // expectedDisplaySize
                        SQL_FALSE,                  // expectedPrecScale
                        0,                          // expectedLength
                        0,                          // expectedColumnSize
                        0,                          // expectedColumnScale
                        SQL_NULLABLE,               // expectedColumnNullability
                        SQL_PRED_NONE,              // expectedSearchable
                        SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 3,
                        std::wstring(L"varbinary_col"),  // expectedColmnName
                        SQL_BINARY,                      // expectedDataType
                        0,                               // expectedDisplaySize
                        SQL_FALSE,                       // expectedPrecScale
                        0,                               // expectedLength
                        0,                               // expectedColumnSize
                        0,                               // expectedColumnScale
                        SQL_NULLABLE,                    // expectedColumnNullability
                        SQL_PRED_NONE,                   // expectedSearchable
                        SQL_TRUE);                       // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 4,
                        std::wstring(L"double_col"),  // expectedColmnName
                        SQL_DOUBLE,                   // expectedDataType
                        24,                           // expectedDisplaySize
                        SQL_FALSE,                    // expectedPrecScale
                        8,                            // expectedLength
                        8,                            // expectedColumnSize
                        0,                            // expectedColumnScale
                        SQL_NULLABLE,                 // expectedColumnNullability
                        SQL_PRED_NONE,                // expectedSearchable
                        SQL_FALSE);                   // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeAllTypes) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expectedColmnName
                       SQL_INTEGER,                    // expectedDataType
                       SQL_INTEGER,                    // expectedConciseType
                       11,                             // expectedDisplaySize
                       SQL_FALSE,                      // expectedPrecScale
                       4,                              // expectedLength
                       std::wstring(L""),              // expectedLiteralPrefix
                       std::wstring(L""),              // expectedLiteralSuffix
                       4,                              // expectedColumnSize
                       0,                              // expectedColumnScale
                       SQL_NULLABLE,                   // expectedColumnNullability
                       10,                             // expectedNumPrecRadix
                       4,                              // expectedOctetLength
                       SQL_SEARCHABLE,                 // expectedSearchable
                       SQL_FALSE);                     // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expectedColmnName
                       SQL_BIGINT,                    // expectedDataType
                       SQL_BIGINT,                    // expectedConciseType
                       20,                            // expectedDisplaySize
                       SQL_FALSE,                     // expectedPrecScale
                       8,                             // expectedLength
                       std::wstring(L""),             // expectedLiteralPrefix
                       std::wstring(L""),             // expectedLiteralSuffix
                       8,                             // expectedColumnSize
                       0,                             // expectedColumnScale
                       SQL_NULLABLE,                  // expectedColumnNullability
                       10,                            // expectedNumPrecRadix
                       8,                             // expectedOctetLength
                       SQL_SEARCHABLE,                // expectedSearchable
                       SQL_FALSE);                    // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expectedColmnName
                       SQL_DECIMAL,                        // expectedDataType
                       SQL_DECIMAL,                        // expectedConciseType
                       40,                                 // expectedDisplaySize
                       SQL_FALSE,                          // expectedPrecScale
                       19,                                 // expectedLength
                       std::wstring(L""),                  // expectedLiteralPrefix
                       std::wstring(L""),                  // expectedLiteralSuffix
                       19,                                 // expectedColumnSize
                       0,                                  // expectedColumnScale
                       SQL_NULLABLE,                       // expectedColumnNullability
                       10,                                 // expectedNumPrecRadix
                       40,                                 // expectedOctetLength
                       SQL_SEARCHABLE,                     // expectedSearchable
                       SQL_FALSE);                         // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expectedColmnName
                       SQL_FLOAT,                   // expectedDataType
                       SQL_FLOAT,                   // expectedConciseType
                       24,                          // expectedDisplaySize
                       SQL_FALSE,                   // expectedPrecScale
                       8,                           // expectedLength
                       std::wstring(L""),           // expectedLiteralPrefix
                       std::wstring(L""),           // expectedLiteralSuffix
                       8,                           // expectedColumnSize
                       0,                           // expectedColumnScale
                       SQL_NULLABLE,                // expectedColumnNullability
                       2,                           // expectedNumPrecRadix
                       8,                           // expectedOctetLength
                       SQL_SEARCHABLE,              // expectedSearchable
                       SQL_FALSE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_SEARCHABLE,               // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expectedColmnName
                       SQL_BIT,                    // expectedDataType
                       SQL_BIT,                    // expectedConciseType
                       1,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       1,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       1,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       1,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TYPE_DATE,              // expectedConciseType
                       10,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       10,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       10,                         // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TYPE_TIME,              // expectedConciseType
                       12,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       12,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       12,                         // expectedColumnSize
                       3,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expectedColmnName
                       SQL_DATETIME,                    // expectedDataType
                       SQL_TYPE_TIMESTAMP,              // expectedConciseType
                       23,                              // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       23,                              // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       23,                              // expectedColumnSize
                       3,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       16,                              // expectedOctetLength
                       SQL_SEARCHABLE,                  // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeAllTypesODBCVer2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expectedColmnName
                       SQL_INTEGER,                    // expectedDataType
                       SQL_INTEGER,                    // expectedConciseType
                       11,                             // expectedDisplaySize
                       SQL_FALSE,                      // expectedPrecScale
                       4,                              // expectedLength
                       std::wstring(L""),              // expectedLiteralPrefix
                       std::wstring(L""),              // expectedLiteralSuffix
                       4,                              // expectedColumnSize
                       0,                              // expectedColumnScale
                       SQL_NULLABLE,                   // expectedColumnNullability
                       10,                             // expectedNumPrecRadix
                       4,                              // expectedOctetLength
                       SQL_SEARCHABLE,                 // expectedSearchable
                       SQL_FALSE);                     // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expectedColmnName
                       SQL_BIGINT,                    // expectedDataType
                       SQL_BIGINT,                    // expectedConciseType
                       20,                            // expectedDisplaySize
                       SQL_FALSE,                     // expectedPrecScale
                       8,                             // expectedLength
                       std::wstring(L""),             // expectedLiteralPrefix
                       std::wstring(L""),             // expectedLiteralSuffix
                       8,                             // expectedColumnSize
                       0,                             // expectedColumnScale
                       SQL_NULLABLE,                  // expectedColumnNullability
                       10,                            // expectedNumPrecRadix
                       8,                             // expectedOctetLength
                       SQL_SEARCHABLE,                // expectedSearchable
                       SQL_FALSE);                    // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expectedColmnName
                       SQL_DECIMAL,                        // expectedDataType
                       SQL_DECIMAL,                        // expectedConciseType
                       40,                                 // expectedDisplaySize
                       SQL_FALSE,                          // expectedPrecScale
                       19,                                 // expectedLength
                       std::wstring(L""),                  // expectedLiteralPrefix
                       std::wstring(L""),                  // expectedLiteralSuffix
                       19,                                 // expectedColumnSize
                       0,                                  // expectedColumnScale
                       SQL_NULLABLE,                       // expectedColumnNullability
                       10,                                 // expectedNumPrecRadix
                       40,                                 // expectedOctetLength
                       SQL_SEARCHABLE,                     // expectedSearchable
                       SQL_FALSE);                         // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expectedColmnName
                       SQL_FLOAT,                   // expectedDataType
                       SQL_FLOAT,                   // expectedConciseType
                       24,                          // expectedDisplaySize
                       SQL_FALSE,                   // expectedPrecScale
                       8,                           // expectedLength
                       std::wstring(L""),           // expectedLiteralPrefix
                       std::wstring(L""),           // expectedLiteralSuffix
                       8,                           // expectedColumnSize
                       0,                           // expectedColumnScale
                       SQL_NULLABLE,                // expectedColumnNullability
                       2,                           // expectedNumPrecRadix
                       8,                           // expectedOctetLength
                       SQL_SEARCHABLE,              // expectedSearchable
                       SQL_FALSE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expectedColmnName
                       SQL_DOUBLE,                   // expectedDataType
                       SQL_DOUBLE,                   // expectedConciseType
                       24,                           // expectedDisplaySize
                       SQL_FALSE,                    // expectedPrecScale
                       8,                            // expectedLength
                       std::wstring(L""),            // expectedLiteralPrefix
                       std::wstring(L""),            // expectedLiteralSuffix
                       8,                            // expectedColumnSize
                       0,                            // expectedColumnScale
                       SQL_NULLABLE,                 // expectedColumnNullability
                       2,                            // expectedNumPrecRadix
                       8,                            // expectedOctetLength
                       SQL_SEARCHABLE,               // expectedSearchable
                       SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expectedColmnName
                       SQL_BIT,                    // expectedDataType
                       SQL_BIT,                    // expectedConciseType
                       1,                          // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       1,                          // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       1,                          // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       1,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_DATE,                   // expectedConciseType
                       10,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       10,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       10,                         // expectedColumnSize
                       0,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expectedColmnName
                       SQL_DATETIME,               // expectedDataType
                       SQL_TIME,                   // expectedConciseType
                       12,                         // expectedDisplaySize
                       SQL_FALSE,                  // expectedPrecScale
                       12,                         // expectedLength
                       std::wstring(L""),          // expectedLiteralPrefix
                       std::wstring(L""),          // expectedLiteralSuffix
                       12,                         // expectedColumnSize
                       3,                          // expectedColumnScale
                       SQL_NULLABLE,               // expectedColumnNullability
                       0,                          // expectedNumPrecRadix
                       6,                          // expectedOctetLength
                       SQL_SEARCHABLE,             // expectedSearchable
                       SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expectedColmnName
                       SQL_DATETIME,                    // expectedDataType
                       SQL_TIMESTAMP,                   // expectedConciseType
                       23,                              // expectedDisplaySize
                       SQL_FALSE,                       // expectedPrecScale
                       23,                              // expectedLength
                       std::wstring(L""),               // expectedLiteralPrefix
                       std::wstring(L""),               // expectedLiteralSuffix
                       23,                              // expectedColumnSize
                       3,                               // expectedColumnScale
                       SQL_NULLABLE,                    // expectedColumnNullability
                       0,                               // expectedNumPrecRadix
                       16,                              // expectedOctetLength
                       SQL_SEARCHABLE,                  // expectedSearchable
                       SQL_TRUE);                       // expectedUnsignedColumn

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  checkSQLColAttributes(this->stmt, 1,
                        std::wstring(L"sinteger_max"),  // expectedColmnName
                        SQL_INTEGER,                    // expectedDataType
                        11,                             // expectedDisplaySize
                        SQL_FALSE,                      // expectedPrecScale
                        4,                              // expectedLength
                        4,                              // expectedColumnSize
                        0,                              // expectedColumnScale
                        SQL_NULLABLE,                   // expectedColumnNullability
                        SQL_SEARCHABLE,                 // expectedSearchable
                        SQL_FALSE);                     // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 2,
                        std::wstring(L"sbigint_max"),  // expectedColmnName
                        SQL_BIGINT,                    // expectedDataType
                        20,                            // expectedDisplaySize
                        SQL_FALSE,                     // expectedPrecScale
                        8,                             // expectedLength
                        8,                             // expectedColumnSize
                        0,                             // expectedColumnScale
                        SQL_NULLABLE,                  // expectedColumnNullability
                        SQL_SEARCHABLE,                // expectedSearchable
                        SQL_FALSE);                    // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 3,
                        std::wstring(L"decimal_positive"),  // expectedColmnName
                        SQL_DECIMAL,                        // expectedDataType
                        40,                                 // expectedDisplaySize
                        SQL_FALSE,                          // expectedPrecScale
                        19,                                 // expectedLength
                        19,                                 // expectedColumnSize
                        0,                                  // expectedColumnScale
                        SQL_NULLABLE,                       // expectedColumnNullability
                        SQL_SEARCHABLE,                     // expectedSearchable
                        SQL_FALSE);                         // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 4,
                        std::wstring(L"float_max"),  // expectedColmnName
                        SQL_FLOAT,                   // expectedDataType
                        24,                          // expectedDisplaySize
                        SQL_FALSE,                   // expectedPrecScale
                        8,                           // expectedLength
                        8,                           // expectedColumnSize
                        0,                           // expectedColumnScale
                        SQL_NULLABLE,                // expectedColumnNullability
                        SQL_SEARCHABLE,              // expectedSearchable
                        SQL_FALSE);                  // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 5,
                        std::wstring(L"double_max"),  // expectedColmnName
                        SQL_DOUBLE,                   // expectedDataType
                        24,                           // expectedDisplaySize
                        SQL_FALSE,                    // expectedPrecScale
                        8,                            // expectedLength
                        8,                            // expectedColumnSize
                        0,                            // expectedColumnScale
                        SQL_NULLABLE,                 // expectedColumnNullability
                        SQL_SEARCHABLE,               // expectedSearchable
                        SQL_FALSE);                   // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 6,
                        std::wstring(L"bit_true"),  // expectedColmnName
                        SQL_BIT,                    // expectedDataType
                        1,                          // expectedDisplaySize
                        SQL_FALSE,                  // expectedPrecScale
                        1,                          // expectedLength
                        1,                          // expectedColumnSize
                        0,                          // expectedColumnScale
                        SQL_NULLABLE,               // expectedColumnNullability
                        SQL_SEARCHABLE,             // expectedSearchable
                        SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 7,
                        std::wstring(L"date_max"),  // expectedColmnName
                        SQL_DATE,                   // expectedDataType
                        10,                         // expectedDisplaySize
                        SQL_FALSE,                  // expectedPrecScale
                        10,                         // expectedLength
                        10,                         // expectedColumnSize
                        0,                          // expectedColumnScale
                        SQL_NULLABLE,               // expectedColumnNullability
                        SQL_SEARCHABLE,             // expectedSearchable
                        SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 8,
                        std::wstring(L"time_max"),  // expectedColmnName
                        SQL_TIME,                   // expectedDataType
                        12,                         // expectedDisplaySize
                        SQL_FALSE,                  // expectedPrecScale
                        12,                         // expectedLength
                        12,                         // expectedColumnSize
                        3,                          // expectedColumnScale
                        SQL_NULLABLE,               // expectedColumnNullability
                        SQL_SEARCHABLE,             // expectedSearchable
                        SQL_TRUE);                  // expectedUnsignedColumn

  checkSQLColAttributes(this->stmt, 9,
                        std::wstring(L"timestamp_max"),  // expectedColmnName
                        SQL_TIMESTAMP,                   // expectedDataType
                        23,                              // expectedDisplaySize
                        SQL_FALSE,                       // expectedPrecScale
                        23,                              // expectedLength
                        23,                              // expectedColumnSize
                        3,                               // expectedColumnScale
                        SQL_NULLABLE,                    // expectedColumnNullability
                        SQL_SEARCHABLE,                  // expectedSearchable
                        SQL_TRUE);                       // expectedUnsignedColumn

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributeCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  // Int column
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_CASE_SENSITIVE, SQL_FALSE);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  checkSQLColAttributeNumeric(this->stmt, wsql, 28, SQL_DESC_CASE_SENSITIVE, SQL_FALSE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributesCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = this->getQueryAllDataTypes();
  // Int column
  checkSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_CASE_SENSITIVE, SQL_FALSE);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  checkSQLColAttributesNumeric(this->stmt, wsql, 28, SQL_COLUMN_CASE_SENSITIVE,
                               SQL_FALSE);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeUniqueValue) {
  // Mock server limitation: returns false for auto-increment column
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_AUTO_UNIQUE_VALUE, SQL_FALSE);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesAutoIncrement) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Mock server limitation: returns false for auto-increment column
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_COLUMN_AUTO_INCREMENT, SQL_FALSE);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeBaseTableName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_BASE_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesTableName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TABLE_NAME,
                              std::wstring(L"AllTypesTable"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeCatalogName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeCatalogName) {
  // Remote server does not have catalogs
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesQualifierName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributesQualifierName) {
  // Remote server does not have catalogs
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributeCount) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  // Pass 0 as column number, driver should ignore it
  checkSQLColAttributeNumeric(this->stmt, wsql, 0, SQL_DESC_COUNT, 32);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeLocalTypeName) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  // Mock server doesn't have local type name
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeLocalTypeName) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME,
                             std::wstring(L"INTEGER"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeSchemaName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeSchemaName) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME,
                             std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesOwnerName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  checkSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME,
                              std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributesOwnerName) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  checkSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME,
                              std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeTableName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributeTypeName) {
  this->connect();
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't return data source-dependent data type name
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME, std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributeTypeName) {
  this->connect();

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  checkSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME,
                             std::wstring(L"INTEGER"));

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't return data source-dependent data type name
  checkSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L""));

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  checkSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"INTEGER"));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributeUnnamed) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UNNAMED, SQL_NAMED);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributeUpdatable) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  checkSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UPDATABLE,
                              SQL_ATTR_READWRITE_UNKNOWN);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLColAttributesUpdatable) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->connect(SQL_OV_ODBC2);

  std::wstring wsql = this->getQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  checkSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_UPDATABLE,
                               SQL_ATTR_READWRITE_UNKNOWN);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLDescribeColValidateInput) {
  this->connect();
  this->CreateTestTables();

  SQLSMALLINT columnCount = 0;
  SQLSMALLINT expectedValue = 3;
  SQLWCHAR sqlQuery[] = L"SELECT * FROM TestTable LIMIT 1;";
  SQLINTEGER queryLength = static_cast<SQLINTEGER>(wcslen(sqlQuery));

  SQLUSMALLINT bookmarkColumn = 0;
  SQLUSMALLINT validColumn = 1;
  SQLUSMALLINT outOfRangeColumn = 4;
  SQLUSMALLINT negativeColumn = -1;
  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT dataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;

  SQLRETURN ret = SQLExecDirect(this->stmt, sqlQuery, queryLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid descriptor index - Bookmarks are not supported
  ret = SQLDescribeCol(this->stmt, bookmarkColumn, columnName, bufCharLen, &nameLength,
                       &dataType, &columnSize, &decimalDigits, &nullable);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  // Invalid descriptor index - index out of range
  ret = SQLDescribeCol(this->stmt, outOfRangeColumn, columnName, bufCharLen, &nameLength,
                       &dataType, &columnSize, &decimalDigits, &nullable);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  // Invalid descriptor index - index out of range
  ret = SQLDescribeCol(this->stmt, negativeColumn, columnName, bufCharLen, &nameLength,
                       &dataType, &columnSize, &decimalDigits, &nullable);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLDescribeColQueryAllDataTypesMetadata) {
  // Mock server has a limitation where only SQL_WVARCHAR column type values are returned
  // from SELECT AS queries
  this->connect();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLWCHAR* columnNames[] = {
      (SQLWCHAR*)L"stiny_int_min",    (SQLWCHAR*)L"stiny_int_max",
      (SQLWCHAR*)L"utiny_int_min",    (SQLWCHAR*)L"utiny_int_max",
      (SQLWCHAR*)L"ssmall_int_min",   (SQLWCHAR*)L"ssmall_int_max",
      (SQLWCHAR*)L"usmall_int_min",   (SQLWCHAR*)L"usmall_int_max",
      (SQLWCHAR*)L"sinteger_min",     (SQLWCHAR*)L"sinteger_max",
      (SQLWCHAR*)L"uinteger_min",     (SQLWCHAR*)L"uinteger_max",
      (SQLWCHAR*)L"sbigint_min",      (SQLWCHAR*)L"sbigint_max",
      (SQLWCHAR*)L"ubigint_min",      (SQLWCHAR*)L"ubigint_max",
      (SQLWCHAR*)L"decimal_negative", (SQLWCHAR*)L"decimal_positive",
      (SQLWCHAR*)L"float_min",        (SQLWCHAR*)L"float_max",
      (SQLWCHAR*)L"double_min",       (SQLWCHAR*)L"double_max",
      (SQLWCHAR*)L"bit_false",        (SQLWCHAR*)L"bit_true",
      (SQLWCHAR*)L"c_char",           (SQLWCHAR*)L"c_wchar",
      (SQLWCHAR*)L"c_wvarchar",       (SQLWCHAR*)L"c_varchar",
      (SQLWCHAR*)L"date_min",         (SQLWCHAR*)L"date_max",
      (SQLWCHAR*)L"timestamp_min",    (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT columnDataTypes[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR};

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, 1024);
    EXPECT_EQ(decimalDigits, 0);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLDescribeColQueryAllDataTypesMetadata) {
  this->connect();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLWCHAR* columnNames[] = {
      (SQLWCHAR*)L"stiny_int_min",    (SQLWCHAR*)L"stiny_int_max",
      (SQLWCHAR*)L"utiny_int_min",    (SQLWCHAR*)L"utiny_int_max",
      (SQLWCHAR*)L"ssmall_int_min",   (SQLWCHAR*)L"ssmall_int_max",
      (SQLWCHAR*)L"usmall_int_min",   (SQLWCHAR*)L"usmall_int_max",
      (SQLWCHAR*)L"sinteger_min",     (SQLWCHAR*)L"sinteger_max",
      (SQLWCHAR*)L"uinteger_min",     (SQLWCHAR*)L"uinteger_max",
      (SQLWCHAR*)L"sbigint_min",      (SQLWCHAR*)L"sbigint_max",
      (SQLWCHAR*)L"ubigint_min",      (SQLWCHAR*)L"ubigint_max",
      (SQLWCHAR*)L"decimal_negative", (SQLWCHAR*)L"decimal_positive",
      (SQLWCHAR*)L"float_min",        (SQLWCHAR*)L"float_max",
      (SQLWCHAR*)L"double_min",       (SQLWCHAR*)L"double_max",
      (SQLWCHAR*)L"bit_false",        (SQLWCHAR*)L"bit_true",
      (SQLWCHAR*)L"c_char",           (SQLWCHAR*)L"c_wchar",
      (SQLWCHAR*)L"c_wvarchar",       (SQLWCHAR*)L"c_varchar",
      (SQLWCHAR*)L"date_min",         (SQLWCHAR*)L"date_max",
      (SQLWCHAR*)L"timestamp_min",    (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT columnDataTypes[] = {
      SQL_INTEGER,        SQL_INTEGER,       SQL_INTEGER,  SQL_INTEGER,   SQL_INTEGER,
      SQL_INTEGER,        SQL_INTEGER,       SQL_INTEGER,  SQL_INTEGER,   SQL_INTEGER,
      SQL_BIGINT,         SQL_BIGINT,        SQL_BIGINT,   SQL_BIGINT,    SQL_BIGINT,
      SQL_WVARCHAR,       SQL_DECIMAL,       SQL_DECIMAL,  SQL_FLOAT,     SQL_FLOAT,
      SQL_DOUBLE,         SQL_DOUBLE,        SQL_BIT,      SQL_BIT,       SQL_WVARCHAR,
      SQL_WVARCHAR,       SQL_WVARCHAR,      SQL_WVARCHAR, SQL_TYPE_DATE, SQL_TYPE_DATE,
      SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP};
  SQLULEN columnSizes[] = {4, 4, 4,     4,     4,     4,     4,  4,  4,  4, 8,
                           8, 8, 8,     8,     65536, 19,    19, 8,  8,  8, 8,
                           1, 1, 65536, 65536, 65536, 65536, 10, 10, 23, 23};
  SQLULEN columnDecimalDigits[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0,  0,  0,
                                   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 10, 23, 23};

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, columnDecimalDigits[i]);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLDescribeColODBCTestTableMetadata) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  SQLWCHAR sqlQuery[] = L"SELECT * from $scratch.ODBCTest LIMIT 1;";
  SQLINTEGER queryLength = static_cast<SQLINTEGER>(wcslen(sqlQuery));

  SQLWCHAR* columnNames[] = {(SQLWCHAR*)L"sinteger_max",     (SQLWCHAR*)L"sbigint_max",
                             (SQLWCHAR*)L"decimal_positive", (SQLWCHAR*)L"float_max",
                             (SQLWCHAR*)L"double_max",       (SQLWCHAR*)L"bit_true",
                             (SQLWCHAR*)L"date_max",         (SQLWCHAR*)L"time_max",
                             (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT columnDataTypes[] = {SQL_INTEGER,   SQL_BIGINT,    SQL_DECIMAL,
                                   SQL_FLOAT,     SQL_DOUBLE,    SQL_BIT,
                                   SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP};
  SQLULEN columnSizes[] = {4, 8, 19, 8, 8, 1, 10, 12, 23};
  SQLULEN columnDecimalDigits[] = {0, 0, 0, 0, 0, 0, 10, 12, 23};

  SQLRETURN ret = SQLExecDirect(this->stmt, sqlQuery, queryLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, columnDecimalDigits[i]);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLDescribeColODBCTestTableMetadataODBC2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  this->connect(SQL_OV_ODBC2);

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  SQLWCHAR sqlQuery[] = L"SELECT * from $scratch.ODBCTest LIMIT 1;";
  SQLINTEGER queryLength = static_cast<SQLINTEGER>(wcslen(sqlQuery));

  SQLWCHAR* columnNames[] = {(SQLWCHAR*)L"sinteger_max",     (SQLWCHAR*)L"sbigint_max",
                             (SQLWCHAR*)L"decimal_positive", (SQLWCHAR*)L"float_max",
                             (SQLWCHAR*)L"double_max",       (SQLWCHAR*)L"bit_true",
                             (SQLWCHAR*)L"date_max",         (SQLWCHAR*)L"time_max",
                             (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT columnDataTypes[] = {SQL_INTEGER, SQL_BIGINT, SQL_DECIMAL,
                                   SQL_FLOAT,   SQL_DOUBLE, SQL_BIT,
                                   SQL_DATE,    SQL_TIME,   SQL_TIMESTAMP};
  SQLULEN columnSizes[] = {4, 8, 19, 8, 8, 1, 10, 12, 23};
  SQLULEN columnDecimalDigits[] = {0, 0, 0, 0, 0, 0, 10, 12, 23};

  SQLRETURN ret = SQLExecDirect(this->stmt, sqlQuery, queryLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, columnDecimalDigits[i]);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLDescribeColAllTypesTableMetadata) {
  this->connect();
  this->CreateTableAllDataType();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  SQLWCHAR sqlQuery[] = L"SELECT * from AllTypesTable LIMIT 1;";
  SQLINTEGER queryLength = static_cast<SQLINTEGER>(wcslen(sqlQuery));

  SQLWCHAR* columnNames[] = {(SQLWCHAR*)L"bigint_col", (SQLWCHAR*)L"char_col",
                             (SQLWCHAR*)L"varbinary_col", (SQLWCHAR*)L"double_col"};
  SQLSMALLINT columnDataTypes[] = {SQL_BIGINT, SQL_WVARCHAR, SQL_BINARY, SQL_DOUBLE};
  SQLULEN columnSizes[] = {8, 0, 0, 8};

  SQLRETURN ret = SQLExecDirect(this->stmt, sqlQuery, queryLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, 0);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLDescribeColUnicodeTableMetadata) {
  this->connect();
  this->CreateUnicodeTable();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 1;

  SQLWCHAR sqlQuery[] = L"SELECT * from 数据 LIMIT 1;";
  SQLINTEGER queryLength = static_cast<SQLINTEGER>(wcslen(sqlQuery));

  SQLWCHAR expectedColumnName[] = L"资料";
  SQLSMALLINT expectedColumnDataType = SQL_WVARCHAR;
  SQLULEN expectedColumnSize = 0;

  SQLRETURN ret = SQLExecDirect(this->stmt, sqlQuery, queryLength);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                       &columnDataType, &columnSize, &decimalDigits, &nullable);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(nameLength, wcslen(expectedColumnName));

  std::wstring returned(columnName, columnName + nameLength);
  EXPECT_EQ(returned, expectedColumnName);
  EXPECT_EQ(columnDataType, expectedColumnDataType);
  EXPECT_EQ(columnSize, expectedColumnSize);
  EXPECT_EQ(decimalDigits, 0);
  EXPECT_EQ(nullable, SQL_NULLABLE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColumnsGetMetadataBySQLDescribeCol) {
  this->connect();

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  SQLWCHAR* columnNames[] = {
      (SQLWCHAR*)L"TABLE_CAT",        (SQLWCHAR*)L"TABLE_SCHEM",
      (SQLWCHAR*)L"TABLE_NAME",       (SQLWCHAR*)L"COLUMN_NAME",
      (SQLWCHAR*)L"DATA_TYPE",        (SQLWCHAR*)L"TYPE_NAME",
      (SQLWCHAR*)L"COLUMN_SIZE",      (SQLWCHAR*)L"BUFFER_LENGTH",
      (SQLWCHAR*)L"DECIMAL_DIGITS",   (SQLWCHAR*)L"NUM_PREC_RADIX",
      (SQLWCHAR*)L"NULLABLE",         (SQLWCHAR*)L"REMARKS",
      (SQLWCHAR*)L"COLUMN_DEF",       (SQLWCHAR*)L"SQL_DATA_TYPE",
      (SQLWCHAR*)L"SQL_DATETIME_SUB", (SQLWCHAR*)L"CHAR_OCTET_LENGTH",
      (SQLWCHAR*)L"ORDINAL_POSITION", (SQLWCHAR*)L"IS_NULLABLE"};
  SQLSMALLINT columnDataTypes[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_INTEGER,  SQL_INTEGER,  SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_INTEGER,  SQL_WVARCHAR};
  SQLULEN columnSizes[] = {1024, 1024, 1024, 1024, 2, 1024, 4, 4, 2,
                           2,    2,    1024, 1024, 2, 2,    4, 4, 1024};

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, 0);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, SQLColumnsGetMetadataBySQLDescribeColODBC2) {
  this->connect(SQL_OV_ODBC2);

  SQLWCHAR columnName[1024];
  SQLSMALLINT bufCharLen =
      static_cast<SQLSMALLINT>(sizeof(columnName) / ODBC::GetSqlWCharSize());
  SQLSMALLINT nameLength = 0;
  SQLSMALLINT columnDataType = 0;
  SQLULEN columnSize = 0;
  SQLSMALLINT decimalDigits = 0;
  SQLSMALLINT nullable = 0;
  size_t columnIndex = 0;

  SQLWCHAR* columnNames[] = {(SQLWCHAR*)L"TABLE_QUALIFIER",
                             (SQLWCHAR*)L"TABLE_OWNER",
                             (SQLWCHAR*)L"TABLE_NAME",
                             (SQLWCHAR*)L"COLUMN_NAME",
                             (SQLWCHAR*)L"DATA_TYPE",
                             (SQLWCHAR*)L"TYPE_NAME",
                             (SQLWCHAR*)L"PRECISION",
                             (SQLWCHAR*)L"LENGTH",
                             (SQLWCHAR*)L"SCALE",
                             (SQLWCHAR*)L"RADIX",
                             (SQLWCHAR*)L"NULLABLE",
                             (SQLWCHAR*)L"REMARKS",
                             (SQLWCHAR*)L"COLUMN_DEF",
                             (SQLWCHAR*)L"SQL_DATA_TYPE",
                             (SQLWCHAR*)L"SQL_DATETIME_SUB",
                             (SQLWCHAR*)L"CHAR_OCTET_LENGTH",
                             (SQLWCHAR*)L"ORDINAL_POSITION",
                             (SQLWCHAR*)L"IS_NULLABLE"};
  SQLSMALLINT columnDataTypes[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_INTEGER,  SQL_INTEGER,  SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_INTEGER,  SQL_WVARCHAR};
  SQLULEN columnSizes[] = {1024, 1024, 1024, 1024, 2, 1024, 4, 4, 2,
                           2,    2,    1024, 1024, 2, 2,    4, 4, 1024};

  SQLRETURN ret = SQLColumns(this->stmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(columnNames) / sizeof(*columnNames); ++i) {
    columnIndex = i + 1;

    ret = SQLDescribeCol(this->stmt, columnIndex, columnName, bufCharLen, &nameLength,
                         &columnDataType, &columnSize, &decimalDigits, &nullable);

    EXPECT_EQ(ret, SQL_SUCCESS);

    EXPECT_EQ(nameLength, wcslen(columnNames[i]));

    std::wstring returned(columnName, columnName + nameLength);
    EXPECT_EQ(returned, columnNames[i]);
    EXPECT_EQ(columnDataType, columnDataTypes[i]);
    EXPECT_EQ(columnSize, columnSizes[i]);
    EXPECT_EQ(decimalDigits, 0);
    EXPECT_EQ(nullable, SQL_NULLABLE);

    nameLength = 0;
    columnDataType = 0;
    columnSize = 0;
    decimalDigits = 0;
    nullable = 0;
  }

  this->disconnect();
}
}  // namespace arrow::flight::sql::odbc
