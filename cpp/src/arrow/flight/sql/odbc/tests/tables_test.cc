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
class TablesTest : public T {
 public:
  using List = std::list<T>;
};

class TablesMockTest : public FlightSQLODBCMockTestBase {};
class TablesRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<TablesMockTest, TablesRemoteTest>;
TYPED_TEST_SUITE(TablesTest, TestTypes);

template <typename T>
class TablesOdbcV2Test : public T {
 public:
  using List = std::list<T>;
};

using TestTypesOdbcV2 =
    ::testing::Types<FlightSQLOdbcV2MockTestBase, FlightSQLOdbcV2RemoteTestBase>;
TYPED_TEST_SUITE(TablesOdbcV2Test, TestTypesOdbcV2);

// Helper Functions

std::wstring GetStringColumnW(SQLHSTMT stmt, int colId) {
  SQLWCHAR buf[1024];
  SQLLEN len_indicator = 0;

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetData(stmt, colId, SQL_C_WCHAR, buf, sizeof(buf), &len_indicator));

  if (len_indicator == SQL_NULL_DATA) {
    return L"";
  }

  // indicator is in bytes, so convert to character count
  size_t char_count = static_cast<size_t>(len_indicator) / ODBC::GetSqlWCharSize();
  return std::wstring(buf, buf + char_count);
}

// Test Cases

TYPED_TEST(TablesTest, SQLTablesTestInputData) {
  SQLWCHAR catalog_name[] = L"";
  SQLWCHAR schema_name[] = L"";
  SQLWCHAR table_name[] = L"";
  SQLWCHAR table_type[] = L"";

  // All values populated
  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, catalog_name, sizeof(catalog_name),
                                   schema_name, sizeof(schema_name), table_name,
                                   sizeof(table_name), table_type, sizeof(table_type)));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Sizes are nulls
  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, catalog_name, 0, schema_name, 0,
                                   table_name, 0, table_type, 0));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Values are nulls
  EXPECT_EQ(SQL_SUCCESS,
            SQLTables(this->stmt, 0, sizeof(catalog_name), 0, sizeof(schema_name), 0,
                      sizeof(table_name), 0, sizeof(table_type)));

  ValidateFetch(this->stmt, SQL_SUCCESS);
  // Close statement cursor to avoid leaving in an invalid state
  SQLFreeStmt(this->stmt, SQL_CLOSE);

  // All values and sizes are nulls
  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, 0, 0, 0, 0, 0, 0, 0, 0));

  ValidateFetch(this->stmt, SQL_SUCCESS);
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForAllCatalogs) {
  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_CATALOGS_W[] = L"%";
  std::wstring expected_catalog_name = std::wstring(L"main");

  // Get Catalog metadata
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, SQL_ALL_CATALOGS_W, SQL_NTS, empty,
                                   SQL_NTS, empty, SQL_NTS, empty, SQL_NTS));

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckStringColumnW(this->stmt, 1, expected_catalog_name);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckNullColumnW(this->stmt, 4);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForNamedCatalog) {
  this->CreateTestTables();

  SQLWCHAR catalog_name[] = L"main";
  SQLWCHAR* table_names[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                             (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expected_catalog_name = std::wstring(catalog_name);
  std::wstring expected_table_type = std::wstring(L"table");

  // Get named Catalog metadata - Mock server returns the system table sqlite_sequence as
  // type "table"
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, catalog_name, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, nullptr, SQL_NTS));

  for (size_t i = 0; i < sizeof(table_names) / sizeof(*table_names); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expected_catalog_name);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, table_names[i]);
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesTestGetSchemaHasNoData) {
  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";

  // Validate that no schema data is available for Mock server
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, SQL_ALL_SCHEMAS_W,
                                   SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesRemoteTest, SQLTablesTestGetMetadataForAllSchemas) {
  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";
  std::set<std::wstring> actual_schemas;
  std::set<std::wstring> expected_schemas = {L"$scratch", L"INFORMATION_SCHEMA", L"sys",
                                             L"sys.cache"};

  // Return is unordered and contains user specific schemas, so collect schema names for
  // comparison with a known list
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, empty, SQL_NTS, SQL_ALL_SCHEMAS_W, SQL_NTS,
                                   empty, SQL_NTS, empty, SQL_NTS));

  while (true) {
    SQLRETURN ret = SQLFetch(this->stmt);
    if (ret == SQL_NO_DATA) break;
    ASSERT_EQ(SQL_SUCCESS, ret);

    CheckNullColumnW(this->stmt, 1);
    std::wstring schema = GetStringColumnW(this->stmt, 2);
    CheckNullColumnW(this->stmt, 3);
    CheckNullColumnW(this->stmt, 4);
    CheckNullColumnW(this->stmt, 5);

    // Skip user-specific schemas like "@UserName"
    if (!schema.empty() && schema[0] != L'@') {
      actual_schemas.insert(schema);
    }
  }

  EXPECT_EQ(actual_schemas, expected_schemas);
}

TEST_F(TablesRemoteTest, SQLTablesTestFilterByAllSchema) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  SQLWCHAR SQL_ALL_SCHEMAS_W[] = L"%";
  SQLWCHAR* schema_names[] = {(SQLWCHAR*)L"INFORMATION_SCHEMA",
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
  std::wstring expected_system_table_type = std::wstring(L"SYSTEM_TABLE");
  std::wstring expected_user_table_type = std::wstring(L"TABLE");

  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, SQL_ALL_SCHEMAS_W,
                                   SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS));

  for (size_t i = 0; i < sizeof(schema_names) / sizeof(*schema_names); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    const std::wstring& expected_table_type =
        (std::wstring(schema_names[i]).rfind(L"sys", 0) == 0 ||
         std::wstring(schema_names[i]) == L"INFORMATION_SCHEMA")
            ? expected_system_table_type
            : expected_user_table_type;

    CheckNullColumnW(this->stmt, 1);
    CheckStringColumnW(this->stmt, 2, schema_names[i]);
    // Ignore table name
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesRemoteTest, SQLTablesGetMetadataForNamedSchema) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  SQLWCHAR schema_name[] = L"$scratch";
  std::wstring expected_schema_name = std::wstring(schema_name);
  std::wstring expected_table_name = std::wstring(L"ODBCTest");
  std::wstring expected_table_type = std::wstring(L"TABLE");

  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, schema_name, SQL_NTS,
                                   nullptr, SQL_NTS, nullptr, SQL_NTS));

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckNullColumnW(this->stmt, 1);
  CheckStringColumnW(this->stmt, 2, expected_schema_name);
  // Ignore table name
  CheckStringColumnW(this->stmt, 4, expected_table_type);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForAllTables) {
  this->CreateTestTables();

  SQLWCHAR SQL_ALL_TABLES_W[] = L"%";
  SQLWCHAR* table_names[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                             (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expected_catalog_name = std::wstring(L"main");
  std::wstring expected_table_type = std::wstring(L"table");

  // Get all Table metadata - Mock server returns the system table sqlite_sequence as type
  // "table"
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   SQL_ALL_TABLES_W, SQL_NTS, nullptr, SQL_NTS));

  for (size_t i = 0; i < sizeof(table_names) / sizeof(*table_names); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expected_catalog_name);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, table_names[i]);
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForTableName) {
  this->CreateTestTables();

  SQLWCHAR* table_names[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                             (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expected_catalog_name = std::wstring(L"main");
  std::wstring expected_table_type = std::wstring(L"table");

  for (size_t i = 0; i < sizeof(table_names) / sizeof(*table_names); ++i) {
    //  Get specific Table metadata
    ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                     table_names[i], SQL_NTS, nullptr, SQL_NTS));

    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expected_catalog_name);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, table_names[i]);
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForUnicodeTableByTableName) {
  this->CreateUnicodeTable();

  SQLWCHAR unicodetable_name[] = L"数据";
  std::wstring expected_catalog_name = std::wstring(L"main");
  std::wstring expected_table_name = std::wstring(unicodetable_name);
  std::wstring expected_table_type = std::wstring(L"table");

  //  Get specific Table metadata
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   unicodetable_name, SQL_NTS, nullptr, SQL_NTS));

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckStringColumnW(this->stmt, 1, expected_catalog_name);
  // Mock server does not support table schema
  CheckNullColumnW(this->stmt, 2);
  CheckStringColumnW(this->stmt, 3, expected_table_name);
  CheckStringColumnW(this->stmt, 4, expected_table_type);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesTestGetMetadataForInvalidTableNameNoData) {
  this->CreateTestTables();

  SQLWCHAR invalid_table_name[] = L"NonExistanttable_name";

  //  Try to get metadata for a non-existant table name
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   invalid_table_name, SQL_NTS, nullptr, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesGetMetadataForTableType) {
  // Mock server only supports table type "table" in lowercase
  this->CreateTestTables();

  SQLWCHAR table_type_table_lowercase[] = L"table";
  SQLWCHAR table_type_table_uppercase[] = L"TABLE";
  SQLWCHAR table_type_view[] = L"VIEW";
  SQLWCHAR table_type_table_view[] = L"TABLE,VIEW";
  SQLWCHAR* table_names[] = {(SQLWCHAR*)L"TestTable", (SQLWCHAR*)L"foreignTable",
                             (SQLWCHAR*)L"intTable", (SQLWCHAR*)L"sqlite_sequence"};
  std::wstring expected_catalog_name = std::wstring(L"main");
  std::wstring expected_table_name = std::wstring(L"TestTable");
  std::wstring expected_table_type = std::wstring(table_type_table_lowercase);

  EXPECT_EQ(SQL_SUCCESS,
            SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                      table_type_table_uppercase, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, table_type_view, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, table_type_table_view, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Returns user table as well as system tables, even though only type table requested
  EXPECT_EQ(SQL_SUCCESS,
            SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, nullptr, SQL_NTS,
                      table_type_table_lowercase, SQL_NTS));

  for (size_t i = 0; i < sizeof(table_names) / sizeof(*table_names); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckStringColumnW(this->stmt, 1, expected_catalog_name);
    // Mock server does not support table schema
    CheckNullColumnW(this->stmt, 2);
    CheckStringColumnW(this->stmt, 3, table_names[i]);
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesRemoteTest, SQLTablesGetMetadataForTableTypeTable) {
  // Requires creation of user table named ODBCTest using schema $scratch in remote server
  SQLWCHAR* type_list[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"TABLE,VIEW"};
  std::wstring expected_schema_name = std::wstring(L"$scratch");
  std::wstring expected_table_name = std::wstring(L"ODBCTest");
  std::wstring expected_table_type = std::wstring(L"TABLE");

  for (size_t i = 0; i < sizeof(type_list) / sizeof(*type_list); ++i) {
    ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                     nullptr, SQL_NTS, type_list[i], SQL_NTS));

    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckStringColumnW(this->stmt, 2, expected_schema_name);
    CheckStringColumnW(this->stmt, 3, expected_table_name);
    CheckStringColumnW(this->stmt, 4, expected_table_type);
    CheckNullColumnW(this->stmt, 5);

    ValidateFetch(this->stmt, SQL_NO_DATA);
  }
}

TEST_F(TablesRemoteTest, SQLTablesGetMetadataForTableTypeViewHasNoData) {
  SQLWCHAR empty[] = L"";
  SQLWCHAR type_view[] = L"VIEW";

  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS, empty,
                                   SQL_NTS, type_view, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);

  EXPECT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, type_view, SQL_NTS));

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesMockTest, SQLTablesGetSupportedTableTypes) {
  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  std::wstring expected_table_type = std::wstring(L"table");

  // Mock server returns lower case for supported type of "table"
  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty,
                                   SQL_NTS, SQL_ALL_TABLE_TYPES_W, SQL_NTS));

  ValidateFetch(this->stmt, SQL_SUCCESS);

  CheckNullColumnW(this->stmt, 1);
  CheckNullColumnW(this->stmt, 2);
  CheckNullColumnW(this->stmt, 3);
  CheckStringColumnW(this->stmt, 4, expected_table_type);
  CheckNullColumnW(this->stmt, 5);

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TEST_F(TablesRemoteTest, SQLTablesGetSupportedTableTypes) {
  SQLWCHAR empty[] = L"";
  SQLWCHAR SQL_ALL_TABLE_TYPES_W[] = L"%";
  SQLWCHAR* type_lists[] = {(SQLWCHAR*)L"TABLE", (SQLWCHAR*)L"SYSTEM_TABLE",
                            (SQLWCHAR*)L"VIEW"};

  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, empty, SQL_NTS, empty, SQL_NTS, empty,
                                   SQL_NTS, SQL_ALL_TABLE_TYPES_W, SQL_NTS));

  for (size_t i = 0; i < sizeof(type_lists) / sizeof(*type_lists); ++i) {
    ValidateFetch(this->stmt, SQL_SUCCESS);

    CheckNullColumnW(this->stmt, 1);
    CheckNullColumnW(this->stmt, 2);
    CheckNullColumnW(this->stmt, 3);
    CheckStringColumnW(this->stmt, 4, type_lists[i]);
    CheckNullColumnW(this->stmt, 5);
  }

  ValidateFetch(this->stmt, SQL_NO_DATA);
}

TYPED_TEST(TablesTest, SQLTablesGetMetadataBySQLDescribeCol) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"TABLE_CAT", (SQLWCHAR*)L"TABLE_SCHEM",
                              (SQLWCHAR*)L"TABLE_NAME", (SQLWCHAR*)L"TABLE_TYPE",
                              (SQLWCHAR*)L"REMARKS"};
  SQLSMALLINT column_data_types[] = {SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
                                     SQL_WVARCHAR, SQL_WVARCHAR};
  SQLULEN column_sizes[] = {1024, 1024, 1024, 1024, 1024};

  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, nullptr, SQL_NTS));

  for (size_t i = 0; i < sizeof(column_names) / sizeof(*column_names); ++i) {
    column_index = i + 1;

    ASSERT_EQ(SQL_SUCCESS, SQLDescribeCol(this->stmt, column_index, column_name,
                                          buf_char_len, &name_length, &column_data_type,
                                          &column_size, &decimal_digits, &nullable));

    EXPECT_EQ(wcslen(column_names[i]), name_length);

    std::wstring returned(column_name, column_name + name_length);
    EXPECT_EQ(column_names[i], returned);
    EXPECT_EQ(column_data_types[i], column_data_type);
    EXPECT_EQ(column_sizes[i], column_size);
    EXPECT_EQ(0, decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}

TYPED_TEST(TablesOdbcV2Test, SQLTablesGetMetadataBySQLDescribeColODBC2) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"TABLE_QUALIFIER", (SQLWCHAR*)L"TABLE_OWNER",
                              (SQLWCHAR*)L"TABLE_NAME", (SQLWCHAR*)L"TABLE_TYPE",
                              (SQLWCHAR*)L"REMARKS"};
  SQLSMALLINT column_data_types[] = {SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
                                     SQL_WVARCHAR, SQL_WVARCHAR};
  SQLULEN column_sizes[] = {1024, 1024, 1024, 1024, 1024};

  ASSERT_EQ(SQL_SUCCESS, SQLTables(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                   nullptr, SQL_NTS, nullptr, SQL_NTS));

  for (size_t i = 0; i < sizeof(column_names) / sizeof(*column_names); ++i) {
    column_index = i + 1;

    ASSERT_EQ(SQL_SUCCESS, SQLDescribeCol(this->stmt, column_index, column_name,
                                          buf_char_len, &name_length, &column_data_type,
                                          &column_size, &decimal_digits, &nullable));

    EXPECT_EQ(wcslen(column_names[i]), name_length);

    std::wstring returned(column_name, column_name + name_length);
    EXPECT_EQ(column_names[i], returned);
    EXPECT_EQ(column_data_types[i], column_data_type);
    EXPECT_EQ(column_sizes[i], column_size);
    EXPECT_EQ(0, decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}
}  // namespace arrow::flight::sql::odbc
