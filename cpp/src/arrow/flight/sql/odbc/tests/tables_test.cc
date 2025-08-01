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

// TODO: Add tests with SQLDescribeCol to check metadata of SQLColumns for ODBC 2 and
// ODBC 3.

// Helper Functions

std::wstring GetStringColumnW(SQLHSTMT stmt, int colId) {
  SQLWCHAR buf[1024];
  SQLLEN lenIndicator = 0;

  SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_WCHAR, buf, sizeof(buf), &lenIndicator);

  EXPECT_EQ(ret, SQL_SUCCESS);

  if (lenIndicator == SQL_NULL_DATA) {
    return L"";
  }

  // indicator is in bytes, so convert to character count
  size_t charCount = static_cast<size_t>(lenIndicator) / ODBC::GetSqlWCharSize();
  return std::wstring(buf, buf + charCount);
}

// Test Cases

TYPED_TEST(FlightSQLODBCTestBase, SQLTablesTestInputData) {
  this->connect();

  SQLWCHAR catalogName[] = L"";
  SQLWCHAR schemaName[] = L"";
  SQLWCHAR tableName[] = L"";
  SQLWCHAR tableType[] = L"";

  // All values populated
  SQLRETURN ret = SQLTables(this->stmt, catalogName, sizeof(catalogName), schemaName,
                            sizeof(schemaName), tableName, sizeof(tableName), tableType,
                            sizeof(tableType));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Sizes are nulls
  ret = SQLTables(this->stmt, catalogName, 0, schemaName, 0, tableName, 0, tableType, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Values are nulls
  ret = SQLTables(this->stmt, 0, sizeof(catalogName), 0, sizeof(schemaName), 0,
                  sizeof(tableName), 0, sizeof(tableType));

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);
  // Close statement cursor to avoid leaving in an invalid state
  SQLFreeStmt(this->stmt, SQL_CLOSE);

  // All values and sizes are nulls
  ret = SQLTables(this->stmt, 0, 0, 0, 0, 0, 0, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForAllCatalogs) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_CATALOGS_W[] = L"%";
  std::wstring expectedCatalogName = std::wstring(L"main");

  // Get Catalog metadata
  SQLRETURN ret = SQLTables(this->stmt, SQL_ALL_CATALOGS_W, SQL_NTS, empty, SQL_NTS,
                            empty, SQL_NTS, empty, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckStringColumnW(this->stmt, 1, expectedCatalogName);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckNullColumnW(this->stmt, 4);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForNamedCatalog) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR catalogName[] = L"main";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(catalogName);
  std::wstring expectedTableType = std::wstring(L"table");

  // Get named Catalog metadata - Mock server returns the system table sqlite_sequence as
  // type "table"
  SQLRETURN ret = SQLTables(this->stmt, catalogName, SQL_NTS, nullptr, SQL_NTS, nullptr,
                            SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetSchemaHasNoData) {
  this->connect();

  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";

  // Validate that no schema data is available for Mock server
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, SQL_ALL_SCHEMAS_W, SQL_NTS,
                            nullptr, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesTestGetMetadataForAllSchemas) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";
  std::set<std::wstring> actualSchemas;
  std::set<std::wstring> expectedSchemas = {L"$scratch", L"INFORMATION_SCHEMA", L"sys",
                                            L"sys.cache"};

  // Return is unordered and contains user specific schemas, so collect schema names for
  // comparison with a known list
  SQLRETURN ret = SQLTables(this->stmt, empty, SQL_NTS, SQL_ALL_SCHEMAS_W, SQL_NTS, empty,
                            SQL_NTS, empty, SQL_NTS);

  ASSERT_EQ(ret, SQL_SUCCESS);

  while (true) {
    ret = SQLFetch(this->stmt);
    if (ret == SQL_NO_DATA) break;
    ASSERT_EQ(ret, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    std::wstring schema = GetStringColumnW(this->stmt, 2);
    CheckNullColumnW(this->stmt, 3);
    CheckNullColumnW(this->stmt, 4);
    CheckNullColumnW(this->stmt, 5);

    // Skip user-specific schemas like "@UserName"
    if (!schema.empty() && schema[0] != L'@') {
      actualSchemas.insert(schema);
    }
  }

  EXPECT_EQ(actualSchemas, expectedSchemas);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesTestFilterByAllSchema) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  this->connect();

  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";
  SQLWCHAR* schemaNames[] = {(SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"INFORMATION_SCHEMA",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"sys.cache",
                             (SQLWCHAR*)L"$scratch"};
  std::wstring expectedSystemTableType = std::wstring(L"SYSTEM_TABLE");
  std::wstring expectedUserTableType = std::wstring(L"TABLE");

  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, SQL_ALL_SCHEMAS_W, SQL_NTS,
                            nullptr, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(schemaNames) / sizeof(*schemaNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    const std::wstring& expectedTableType =
        (std::wstring(schemaNames[i]).rfind(L"sys", 0) == 0 ||
         std::wstring(schemaNames[i]) == L"INFORMATION_SCHEMA")
            ? expectedSystemTableType
            : expectedUserTableType;

    CheckNullColumnW(this->stmt, 1);
    CheckStringColumnW(this->stmt, 2, schemaNames[i]);
    // Ignore table name
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetMetadataForNamedSchema) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  this->connect();

  SQLWCHAR schemaName[] = L"$scratch";
  std::wstring expectedSchemaName = std::wstring(schemaName);
  std::wstring expectedTableName = std::wstring(L"ODBCTest");
  std::wstring expectedTableType = std::wstring(L"TABLE");

  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, schemaName, SQL_NTS, nullptr,
                            SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckNullColumnW(this->stmt, 1);
  CheckStringColumnW(this->stmt, 2, expectedSchemaName);
  // Ignore table name
  CheckStringColumnW(this->stmt, 4, expectedTableType);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForAllTables) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR SQL_ALL_TABLES_W[] = L"%";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableType = std::wstring(L"table");

  // Get all Table metadata - Mock server returns the system table sqlite_sequence as type
  // "table"
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            SQL_ALL_TABLES_W, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForTableName) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableType = std::wstring(L"table");

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    //  Get specific Table metadata
    SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                              tableNames[i], SQL_NTS, nullptr, SQL_NTS);

    EXPECT_EQ(ret, SQL_SUCCESS);

    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForUnicodeTableByTableName) {
  this->connect();
  this->CreateUnicodeTable();

  SQLWCHAR unicodeTableName[] = L"数据";
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableName = std::wstring(unicodeTableName);
  std::wstring expectedTableType = std::wstring(L"table");

  //  Get specific Table metadata
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            unicodeTableName, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckStringColumnW(this->stmt, 1, expectedCatalogName);
  // Mock server does not support table schema
  CheckNullColumnW(this->stmt, 2);
  CheckStringColumnW(this->stmt, 3, expectedTableName);
  CheckStringColumnW(this->stmt, 4, expectedTableType);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesTestGetMetadataForInvalidTableNameNoData) {
  this->connect();
  this->CreateTestTables();

  SQLWCHAR invalidTableName[] = L"NonExistantTableName";

  //  Try to get metadata for a non-existant table name
  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                            invalidTableName, SQL_NTS, nullptr, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesGetMetadataForTableType) {
  // Mock server only supports table type "table" in lowercase
  this->connect();
  this->CreateTestTables();

  SQLWCHAR tableTypeTableLowercase[] = L"table";
  SQLWCHAR tableTypeTableUppercase[] = L"TABLE";
  SQLWCHAR tableTypeView[] = L"VIEW";
  SQLWCHAR tableTypeTableView[] = L"TABLE,VIEW";
  SQLWCHAR* tableNames[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                            (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expectedCatalogName = std::wstring(L"main");
  std::wstring expectedTableName = std::wstring(L"TestTable");
  std::wstring expectedTableType = std::wstring(tableTypeTableLowercase);
  SQLRETURN ret = SQL_SUCCESS;

  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                  tableTypeTableUppercase, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                  tableTypeView, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                  tableTypeTableView, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Returns user table as well as system tables, even though only type table requested
  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                  tableTypeTableLowercase, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(tableNames) / sizeof(*tableNames); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expectedCatalogName);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, tableNames[i]);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetMetadataForTableTypeTable) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  this->connect();

  SQLWCHAR* typeList[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"TABLE,VIEW"};
  std::wstring expectedSchemaName = std::wstring(L"$scratch");
  std::wstring expectedTableName = std::wstring(L"ODBCTest");
  std::wstring expectedTableType = std::wstring(L"TABLE");
  SQLRETURN ret = SQL_SUCCESS;

  for (size_t i = 0; i < sizeof(typeList) / sizeof(*typeList); ++i) {
    ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                    typeList[i], SQL_NTS);

    EXPECT_EQ(ret, SQL_SUCCESS);

    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckStringColumnW(this->stmt, 2, expectedSchemaName);
    CheckStringColumnW(this->stmt, 3, expectedTableName);
    CheckStringColumnW(this->stmt, 4, expectedTableType);
    CheckNullColumnW(this->stmt, 5);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetMetadataForTableTypeViewHasNoData) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR typeView[] = L"VIEW";

  SQLRETURN ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, empty,
                            SQL_NTS, typeView, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  ret = SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                  typeView, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, SQLTablesGetSupportedTableTypes) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  std::wstring expectedTableType = std::wstring(L"table");

  // Mock server returns lower case for supported type of "table"
  SQLRETURN ret = SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty, SQL_NTS,
                            SQL_ALL_TABLE_TYPES_W, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckNullColumnW(this->stmt, 1);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckStringColumnW(this->stmt, 4, expectedTableType);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, SQLTablesGetSupportedTableTypes) {
  this->connect();

  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  SQLWCHAR* typeLists[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"SYSTEM_TABLE",
                           (SQLWCHAR*)L"VIEW"};

  SQLRETURN ret = SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty, SQL_NTS,
                            SQL_ALL_TABLE_TYPES_W, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  for (size_t i = 0; i < sizeof(typeLists) / sizeof(*typeLists); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckNullColumnW(this->stmt, 2);
    CheckNullColumnW(this->stmt, 3);
    CheckStringColumnW(this->stmt, 4, typeLists[i]);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
