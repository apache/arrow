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
class ColumnsTest : public T {};

class ColumnsMockTest : public FlightSQLODBCMockTestBase {};
class ColumnsRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<ColumnsMockTest, ColumnsRemoteTest>;
TYPED_TEST_SUITE(ColumnsTest, TestTypes);

template <typename T>
class ColumnsOdbcV2Test : public T {};

class ColumnsOdbcV2MockTest : public FlightSQLOdbcV2MockTestBase {};
class ColumnsOdbcV2RemoteTest : public FlightSQLOdbcV2RemoteTestBase {};
using TestTypesOdbcV2 = ::testing::Types<ColumnsOdbcV2MockTest, ColumnsOdbcV2RemoteTest>;
TYPED_TEST_SUITE(ColumnsOdbcV2Test, TestTypesOdbcV2);

namespace {
// Helper functions
void CheckSQLColumns(
    SQLHSTMT stmt, const std::wstring& expected_table,
    const std::wstring& expected_column, const SQLINTEGER& expected_data_type,
    const std::wstring& expected_type_name, const SQLINTEGER& expected_column_size,
    const SQLINTEGER& expected_buffer_length, const SQLSMALLINT& expected_decimal_digits,
    const SQLSMALLINT& expected_num_prec_radix, const SQLSMALLINT& expected_nullable,
    const SQLSMALLINT& expected_sql_data_type, const SQLSMALLINT& expected_date_time_sub,
    const SQLINTEGER& expected_octet_char_length,
    const SQLINTEGER& expected_ordinal_position,
    const std::wstring& expected_is_nullable) {
  CheckStringColumnW(stmt, 3, expected_table);   // table name
  CheckStringColumnW(stmt, 4, expected_column);  // column name

  CheckIntColumn(stmt, 5, expected_data_type);  // data type

  CheckStringColumnW(stmt, 6, expected_type_name);  // type name

  CheckIntColumn(stmt, 7, expected_column_size);    // column size
  CheckIntColumn(stmt, 8, expected_buffer_length);  // buffer length

  CheckSmallIntColumn(stmt, 9, expected_decimal_digits);   // decimal digits
  CheckSmallIntColumn(stmt, 10, expected_num_prec_radix);  // num prec radix
  CheckSmallIntColumn(stmt, 11,
                      expected_nullable);  // nullable

  CheckNullColumnW(stmt, 12);  // remarks
  CheckNullColumnW(stmt, 13);  // column def

  CheckSmallIntColumn(stmt, 14, expected_sql_data_type);  // sql data type
  CheckSmallIntColumn(stmt, 15, expected_date_time_sub);  // sql date type sub
  CheckIntColumn(stmt, 16, expected_octet_char_length);   // char octet length
  CheckIntColumn(stmt, 17,
                 expected_ordinal_position);  // oridinal position

  CheckStringColumnW(stmt, 18, expected_is_nullable);  // is nullable
}

void CheckMockSQLColumns(
    SQLHSTMT stmt, const std::wstring& expected_catalog,
    const std::wstring& expected_table, const std::wstring& expected_column,
    const SQLINTEGER& expected_data_type, const std::wstring& expected_type_name,
    const SQLINTEGER& expected_column_size, const SQLINTEGER& expected_buffer_length,
    const SQLSMALLINT& expected_decimal_digits,
    const SQLSMALLINT& expected_num_prec_radix, const SQLSMALLINT& expected_nullable,
    const SQLSMALLINT& expected_sql_data_type, const SQLSMALLINT& expected_date_time_sub,
    const SQLINTEGER& expected_octet_char_length,
    const SQLINTEGER& expected_ordinal_position,
    const std::wstring& expected_is_nullable) {
  CheckStringColumnW(stmt, 1, expected_catalog);  // catalog
  CheckNullColumnW(stmt, 2);                      // schema

  CheckSQLColumns(stmt, expected_table, expected_column, expected_data_type,
                  expected_type_name, expected_column_size, expected_buffer_length,
                  expected_decimal_digits, expected_num_prec_radix, expected_nullable,
                  expected_sql_data_type, expected_date_time_sub,
                  expected_octet_char_length, expected_ordinal_position,
                  expected_is_nullable);
}

void CheckRemoteSQLColumns(
    SQLHSTMT stmt, const std::wstring& expected_schema,
    const std::wstring& expected_table, const std::wstring& expected_column,
    const SQLINTEGER& expected_data_type, const std::wstring& expected_type_name,
    const SQLINTEGER& expected_column_size, const SQLINTEGER& expected_buffer_length,
    const SQLSMALLINT& expected_decimal_digits,
    const SQLSMALLINT& expected_num_prec_radix, const SQLSMALLINT& expected_nullable,
    const SQLSMALLINT& expected_sql_data_type, const SQLSMALLINT& expected_date_time_sub,
    const SQLINTEGER& expected_octet_char_length,
    const SQLINTEGER& expected_ordinal_position,
    const std::wstring& expected_is_nullable) {
  CheckNullColumnW(stmt, 1);                     // catalog
  CheckStringColumnW(stmt, 2, expected_schema);  // schema
  CheckSQLColumns(stmt, expected_table, expected_column, expected_data_type,
                  expected_type_name, expected_column_size, expected_buffer_length,
                  expected_decimal_digits, expected_num_prec_radix, expected_nullable,
                  expected_sql_data_type, expected_date_time_sub,
                  expected_octet_char_length, expected_ordinal_position,
                  expected_is_nullable);
}

}  // namespace

TYPED_TEST(ColumnsTest, SQLColumnsTestInputData) {
  SQLWCHAR catalog_name[] = L"";
  SQLWCHAR schema_name[] = L"";
  SQLWCHAR table_name[] = L"";
  SQLWCHAR column_name[] = L"";

  // All values populated
  EXPECT_EQ(SQL_SUCCESS,
            SQLColumns(this->stmt, catalog_name, sizeof(catalog_name), schema_name,
                       sizeof(schema_name), table_name, sizeof(table_name), column_name,
                       sizeof(column_name)));
  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Sizes are nulls
  EXPECT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, catalog_name, 0, schema_name, 0,
                                    table_name, 0, column_name, 0));
  ValidateFetch(this->stmt, SQL_NO_DATA);

  // Values are nulls
  EXPECT_EQ(SQL_SUCCESS,
            SQLColumns(this->stmt, 0, sizeof(catalog_name), 0, sizeof(schema_name), 0,
                       sizeof(table_name), 0, sizeof(column_name)));
  ValidateFetch(this->stmt, SQL_SUCCESS);
  // Close statement cursor to avoid leaving in an invalid state
  SQLFreeStmt(this->stmt, SQL_CLOSE);

  // All values and sizes are nulls
  EXPECT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, 0, 0, 0, 0, 0, 0, 0, 0));
  ValidateFetch(this->stmt, SQL_SUCCESS);
}

TEST_F(ColumnsMockTest, TestSQLColumnsAllColumns) {
  // Check table pattern and column pattern returns all columns

  // Attempt to get all columns
  SQLWCHAR table_pattern[] = L"%";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // mock limitation: SQLite mock server returns 10 for bigint size when spec indicates
  // should be 19
  // DECIMAL_DIGITS should be 0 for bigint type since it is exact
  // mock limitation: SQLite mock server returns 10 for bigint decimal digits when spec
  // indicates should be 0
  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expected_catalog
                      std::wstring(L"foreignTable"),  // expected_table
                      std::wstring(L"id"),            // expected_column
                      SQL_BIGINT,                     // expected_data_type
                      std::wstring(L"BIGINT"),        // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 2nd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expected_catalog
                      std::wstring(L"foreignTable"),  // expected_table
                      std::wstring(L"foreignName"),   // expected_column
                      SQL_WVARCHAR,                   // expected_data_type
                      std::wstring(L"WVARCHAR"),      // expected_type_name
                      0,   // expected_column_size (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expected_buffer_length
                      15,  // expected_decimal_digits
                      0,   // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_WVARCHAR,           // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      0,                      // expected_octet_char_length
                      2,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 3rd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expected_catalog
                      std::wstring(L"foreignTable"),  // expected_table
                      std::wstring(L"value"),         // expected_column
                      SQL_BIGINT,                     // expected_data_type
                      std::wstring(L"BIGINT"),        // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      3,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 4th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expected_catalog
                      std::wstring(L"intTable"),  // expected_table
                      std::wstring(L"id"),        // expected_column
                      SQL_BIGINT,                 // expected_data_type
                      std::wstring(L"BIGINT"),    // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 5th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expected_catalog
                      std::wstring(L"intTable"),  // expected_table
                      std::wstring(L"keyName"),   // expected_column
                      SQL_WVARCHAR,               // expected_data_type
                      std::wstring(L"WVARCHAR"),  // expected_type_name
                      0,   // expected_column_size (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expected_buffer_length
                      15,  // expected_decimal_digits
                      0,   // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_WVARCHAR,           // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      0,                      // expected_octet_char_length
                      2,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 6th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expected_catalog
                      std::wstring(L"intTable"),  // expected_table
                      std::wstring(L"value"),     // expected_column
                      SQL_BIGINT,                 // expected_data_type
                      std::wstring(L"BIGINT"),    // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      3,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 7th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),       // expected_catalog
                      std::wstring(L"intTable"),   // expected_table
                      std::wstring(L"foreignId"),  // expected_column
                      SQL_BIGINT,                  // expected_data_type
                      std::wstring(L"BIGINT"),     // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      4,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable
}

TEST_F(ColumnsMockTest, TestSQLColumnsAllTypes) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.

  // Checks filtering table with table name pattern
  this->CreateTableAllDataType();

  // Attempt to get all columns from AllTypesTable
  SQLWCHAR table_pattern[] = L"AllTypesTable";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Fetch SQLColumn data for 1st column in AllTypesTable
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expected_catalog
                      std::wstring(L"AllTypesTable"),  // expected_table
                      std::wstring(L"bigint_col"),     // expected_column
                      SQL_BIGINT,                      // expected_data_type
                      std::wstring(L"BIGINT"),         // expected_type_name
                      10,  // expected_column_size (mock server limitation: returns 10,
                           // the ODBC spec expects 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock server limitation: returns 15,
                           // the ODBC spec expects 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check SQLColumn data for 2nd column in AllTypesTable
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expected_catalog
                      std::wstring(L"AllTypesTable"),  // expected_table
                      std::wstring(L"char_col"),       // expected_column
                      SQL_WVARCHAR,                    // expected_data_type
                      std::wstring(L"WVARCHAR"),       // expected_type_name
                      0,   // expected_column_size (mock server limitation: returns 0 for
                           // varchar(100), the ODBC spec expects 100)
                      0,   // expected_buffer_length
                      15,  // expected_decimal_digits
                      0,   // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_WVARCHAR,           // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      0,                      // expected_octet_char_length
                      2,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check SQLColumn data for 3rd column in AllTypesTable
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expected_catalog
                      std::wstring(L"AllTypesTable"),  // expected_table
                      std::wstring(L"varbinary_col"),  // expected_column
                      SQL_BINARY,                      // expected_data_type
                      std::wstring(L"BINARY"),         // expected_type_name
                      0,   // expected_column_size (mock server limitation: returns 0 for
                           // BLOB column, spec expects binary data limit)
                      0,   // expected_buffer_length
                      15,  // expected_decimal_digits
                      0,   // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BINARY,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      0,                      // expected_octet_char_length
                      3,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check SQLColumn data for 4th column in AllTypesTable
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),           // expected_catalog
                      std::wstring(L"AllTypesTable"),  // expected_table
                      std::wstring(L"double_col"),     // expected_column
                      SQL_DOUBLE,                      // expected_data_type
                      std::wstring(L"DOUBLE"),         // expected_type_name
                      15,                              // expected_column_size
                      8,                               // expected_buffer_length
                      15,                              // expected_decimal_digits
                      2,                               // expected_num_prec_radix
                      SQL_NULLABLE,                    // expected_nullable
                      SQL_DOUBLE,                      // expected_sql_data_type
                      NULL,                            // expected_date_time_sub
                      8,                               // expected_octet_char_length
                      4,                               // expected_ordinal_position
                      std::wstring(L"YES"));           // expected_is_nullable

  // There should be no more column data
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsMockTest, TestSQLColumnsUnicode) {
  // Limitation: Mock server returns incorrect values for column size for some columns.
  // For character and binary type columns, the driver calculates buffer length and char
  // octet length from column size.
  this->CreateUnicodeTable();

  // Attempt to get all columns
  SQLWCHAR table_pattern[] = L"数据";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check SQLColumn data for 1st column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expected_catalog
                      std::wstring(L"数据"),      // expected_table
                      std::wstring(L"资料"),      // expected_column
                      SQL_WVARCHAR,               // expected_data_type
                      std::wstring(L"WVARCHAR"),  // expected_type_name
                      0,   // expected_column_size (mock server limitation: returns 0 for
                           // varchar(100), spec expects 100)
                      0,   // expected_buffer_length
                      15,  // expected_decimal_digits
                      0,   // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_WVARCHAR,           // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      0,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // There should be no more column data
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsRemoteTest, TestSQLColumnsAllTypes) {
  // GH-47159 TODO: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits

  SQLWCHAR table_pattern[] = L"ODBCTest";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),      // expected_schema
      std::wstring(L"ODBCTest"),      // expected_table
      std::wstring(L"sinteger_max"),  // expected_column
      SQL_INTEGER,                    // expected_data_type
      std::wstring(L"INTEGER"),       // expected_type_name
      32,            // expected_column_size (remote server returns number of bits)
      4,             // expected_buffer_length
      0,             // expected_decimal_digits
      10,            // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_INTEGER,   // expected_sql_data_type
      NULL,          // expected_date_time_sub
      4,             // expected_octet_char_length
      1,             // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 2nd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),     // expected_schema
      std::wstring(L"ODBCTest"),     // expected_table
      std::wstring(L"sbigint_max"),  // expected_column
      SQL_BIGINT,                    // expected_data_type
      std::wstring(L"BIGINT"),       // expected_type_name
      64,            // expected_column_size (remote server returns number of bits)
      8,             // expected_buffer_length
      0,             // expected_decimal_digits
      10,            // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_BIGINT,    // expected_sql_data_type
      NULL,          // expected_date_time_sub
      8,             // expected_octet_char_length
      2,             // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 3rd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expected_schema
                        std::wstring(L"ODBCTest"),          // expected_table
                        std::wstring(L"decimal_positive"),  // expected_column
                        SQL_DECIMAL,                        // expected_data_type
                        std::wstring(L"DECIMAL"),           // expected_type_name
                        38,                                 // expected_column_size
                        19,                                 // expected_buffer_length
                        0,                                  // expected_decimal_digits
                        10,                                 // expected_num_prec_radix
                        SQL_NULLABLE,                       // expected_nullable
                        SQL_DECIMAL,                        // expected_sql_data_type
                        NULL,                               // expected_date_time_sub
                        2,                                  // expected_octet_char_length
                        3,                                  // expected_ordinal_position
                        std::wstring(L"YES"));              // expected_is_nullable

  // Check 4th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expected_schema
                        std::wstring(L"ODBCTest"),   // expected_table
                        std::wstring(L"float_max"),  // expected_column
                        SQL_FLOAT,                   // expected_data_type
                        std::wstring(L"FLOAT"),      // expected_type_name
                        24,  // expected_column_size (precision bits from IEEE 754)
                        8,   // expected_buffer_length
                        0,   // expected_decimal_digits
                        2,   // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_FLOAT,              // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        8,                      // expected_octet_char_length
                        4,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // Check 5th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expected_schema
                        std::wstring(L"ODBCTest"),    // expected_table
                        std::wstring(L"double_max"),  // expected_column
                        SQL_DOUBLE,                   // expected_data_type
                        std::wstring(L"DOUBLE"),      // expected_type_name
                        53,  // expected_column_size (precision bits from IEEE 754)
                        8,   // expected_buffer_length
                        0,   // expected_decimal_digits
                        2,   // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_DOUBLE,             // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        8,                      // expected_octet_char_length
                        5,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // Check 6th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expected_schema
                        std::wstring(L"ODBCTest"),  // expected_table
                        std::wstring(L"bit_true"),  // expected_column
                        SQL_BIT,                    // expected_data_type
                        std::wstring(L"BOOLEAN"),   // expected_type_name
                        0,  // expected_column_size (limitation: remote server remote
                            // server returns 0, should be 1)
                        1,  // expected_buffer_length
                        0,  // expected_decimal_digits
                        0,  // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_BIT,                // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        1,                      // expected_octet_char_length
                        6,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // ODBC ver 3 returns SQL_TYPE_DATE, SQL_TYPE_TIME, and SQL_TYPE_TIMESTAMP in the
  // DATA_TYPE field

  // Check 7th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expected_schema
      std::wstring(L"ODBCTest"),  // expected_table
      std::wstring(L"date_max"),  // expected_column
      SQL_TYPE_DATE,              // expected_data_type
      std::wstring(L"DATE"),      // expected_type_name
      0,   // expected_column_size (limitation: remote server returns 0, should be 10)
      10,  // expected_buffer_length
      0,   // expected_decimal_digits
      0,   // expected_num_prec_radix
      SQL_NULLABLE,           // expected_nullable
      SQL_DATETIME,           // expected_sql_data_type
      SQL_CODE_DATE,          // expected_date_time_sub
      6,                      // expected_octet_char_length
      7,                      // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 8th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expected_schema
      std::wstring(L"ODBCTest"),  // expected_table
      std::wstring(L"time_max"),  // expected_column
      SQL_TYPE_TIME,              // expected_data_type
      std::wstring(L"TIME"),      // expected_type_name
      3,              // expected_column_size (limitation: should be 9+fractional digits)
      12,             // expected_buffer_length
      0,              // expected_decimal_digits
      0,              // expected_num_prec_radix
      SQL_NULLABLE,   // expected_nullable
      SQL_DATETIME,   // expected_sql_data_type
      SQL_CODE_TIME,  // expected_date_time_sub
      6,              // expected_octet_char_length
      8,              // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 9th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expected_schema
      std::wstring(L"ODBCTest"),       // expected_table
      std::wstring(L"timestamp_max"),  // expected_column
      SQL_TYPE_TIMESTAMP,              // expected_data_type
      std::wstring(L"TIMESTAMP"),      // expected_type_name
      3,             // expected_column_size (limitation: should be 20+fractional digits)
      23,            // expected_buffer_length
      0,             // expected_decimal_digits
      0,             // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_DATETIME,  // expected_sql_data_type
      SQL_CODE_TIMESTAMP,     // expected_date_time_sub
      16,                     // expected_octet_char_length
      9,                      // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // There is no more column
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColumnsAllTypesODBCVer2) {
  // GH-47159 TODO: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits

  SQLWCHAR table_pattern[] = L"ODBCTest";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),      // expected_schema
      std::wstring(L"ODBCTest"),      // expected_table
      std::wstring(L"sinteger_max"),  // expected_column
      SQL_INTEGER,                    // expected_data_type
      std::wstring(L"INTEGER"),       // expected_type_name
      32,            // expected_column_size (remote server returns number of bits)
      4,             // expected_buffer_length
      0,             // expected_decimal_digits
      10,            // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_INTEGER,   // expected_sql_data_type
      NULL,          // expected_date_time_sub
      4,             // expected_octet_char_length
      1,             // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 2nd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),     // expected_schema
      std::wstring(L"ODBCTest"),     // expected_table
      std::wstring(L"sbigint_max"),  // expected_column
      SQL_BIGINT,                    // expected_data_type
      std::wstring(L"BIGINT"),       // expected_type_name
      64,            // expected_column_size (remote server returns number of bits)
      8,             // expected_buffer_length
      0,             // expected_decimal_digits
      10,            // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_BIGINT,    // expected_sql_data_type
      NULL,          // expected_date_time_sub
      8,             // expected_octet_char_length
      2,             // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 3rd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),          // expected_schema
                        std::wstring(L"ODBCTest"),          // expected_table
                        std::wstring(L"decimal_positive"),  // expected_column
                        SQL_DECIMAL,                        // expected_data_type
                        std::wstring(L"DECIMAL"),           // expected_type_name
                        38,                                 // expected_column_size
                        19,                                 // expected_buffer_length
                        0,                                  // expected_decimal_digits
                        10,                                 // expected_num_prec_radix
                        SQL_NULLABLE,                       // expected_nullable
                        SQL_DECIMAL,                        // expected_sql_data_type
                        NULL,                               // expected_date_time_sub
                        2,                                  // expected_octet_char_length
                        3,                                  // expected_ordinal_position
                        std::wstring(L"YES"));              // expected_is_nullable

  // Check 4th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),   // expected_schema
                        std::wstring(L"ODBCTest"),   // expected_table
                        std::wstring(L"float_max"),  // expected_column
                        SQL_FLOAT,                   // expected_data_type
                        std::wstring(L"FLOAT"),      // expected_type_name
                        24,  // expected_column_size (precision bits from IEEE 754)
                        8,   // expected_buffer_length
                        0,   // expected_decimal_digits
                        2,   // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_FLOAT,              // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        8,                      // expected_octet_char_length
                        4,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // Check 5th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),    // expected_schema
                        std::wstring(L"ODBCTest"),    // expected_table
                        std::wstring(L"double_max"),  // expected_column
                        SQL_DOUBLE,                   // expected_data_type
                        std::wstring(L"DOUBLE"),      // expected_type_name
                        53,  // expected_column_size (precision bits from IEEE 754)
                        8,   // expected_buffer_length
                        0,   // expected_decimal_digits
                        2,   // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_DOUBLE,             // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        8,                      // expected_octet_char_length
                        5,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // Check 6th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(this->stmt,
                        std::wstring(L"$scratch"),  // expected_schema
                        std::wstring(L"ODBCTest"),  // expected_table
                        std::wstring(L"bit_true"),  // expected_column
                        SQL_BIT,                    // expected_data_type
                        std::wstring(L"BOOLEAN"),   // expected_type_name
                        0,  // expected_column_size (limitation: remote server remote
                            // server returns 0, should be 1)
                        1,  // expected_buffer_length
                        0,  // expected_decimal_digits
                        0,  // expected_num_prec_radix
                        SQL_NULLABLE,           // expected_nullable
                        SQL_BIT,                // expected_sql_data_type
                        NULL,                   // expected_date_time_sub
                        1,                      // expected_octet_char_length
                        6,                      // expected_ordinal_position
                        std::wstring(L"YES"));  // expected_is_nullable

  // ODBC ver 2 returns SQL_DATE, SQL_TIME, and SQL_TIMESTAMP in the DATA_TYPE field

  // Check 7th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expected_schema
      std::wstring(L"ODBCTest"),  // expected_table
      std::wstring(L"date_max"),  // expected_column
      SQL_DATE,                   // expected_data_type
      std::wstring(L"DATE"),      // expected_type_name
      0,   // expected_column_size (limitation: remote server returns 0, should be 10)
      10,  // expected_buffer_length
      0,   // expected_decimal_digits
      0,   // expected_num_prec_radix
      SQL_NULLABLE,           // expected_nullable
      SQL_DATETIME,           // expected_sql_data_type
      SQL_CODE_DATE,          // expected_date_time_sub
      6,                      // expected_octet_char_length
      7,                      // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 8th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),  // expected_schema
      std::wstring(L"ODBCTest"),  // expected_table
      std::wstring(L"time_max"),  // expected_column
      SQL_TIME,                   // expected_data_type
      std::wstring(L"TIME"),      // expected_type_name
      3,              // expected_column_size (limitation: should be 9+fractional digits)
      12,             // expected_buffer_length
      0,              // expected_decimal_digits
      0,              // expected_num_prec_radix
      SQL_NULLABLE,   // expected_nullable
      SQL_DATETIME,   // expected_sql_data_type
      SQL_CODE_TIME,  // expected_date_time_sub
      6,              // expected_octet_char_length
      8,              // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // Check 9th Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckRemoteSQLColumns(
      this->stmt,
      std::wstring(L"$scratch"),       // expected_schema
      std::wstring(L"ODBCTest"),       // expected_table
      std::wstring(L"timestamp_max"),  // expected_column
      SQL_TIMESTAMP,                   // expected_data_type
      std::wstring(L"TIMESTAMP"),      // expected_type_name
      3,             // expected_column_size (limitation: should be 20+fractional digits)
      23,            // expected_buffer_length
      0,             // expected_decimal_digits
      0,             // expected_num_prec_radix
      SQL_NULLABLE,  // expected_nullable
      SQL_DATETIME,  // expected_sql_data_type
      SQL_CODE_TIMESTAMP,     // expected_date_time_sub
      16,                     // expected_octet_char_length
      9,                      // expected_ordinal_position
      std::wstring(L"YES"));  // expected_is_nullable

  // There is no more column
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsMockTest, TestSQLColumnsColumnPattern) {
  // Checks filtering table with column name pattern.
  // Only check table and column name

  SQLWCHAR table_pattern[] = L"%";
  SQLWCHAR column_pattern[] = L"id";

  EXPECT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expected_catalog
                      std::wstring(L"foreignTable"),  // expected_table
                      std::wstring(L"id"),            // expected_column
                      SQL_BIGINT,                     // expected_data_type
                      std::wstring(L"BIGINT"),        // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // Check 2nd Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),      // expected_catalog
                      std::wstring(L"intTable"),  // expected_table
                      std::wstring(L"id"),        // expected_column
                      SQL_BIGINT,                 // expected_data_type
                      std::wstring(L"BIGINT"),    // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // There is no more column
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsMockTest, TestSQLColumnsTableColumnPattern) {
  // Checks filtering table with table and column name pattern.
  // Only check table and column name

  SQLWCHAR table_pattern[] = L"foreignTable";
  SQLWCHAR column_pattern[] = L"id";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckMockSQLColumns(this->stmt,
                      std::wstring(L"main"),          // expected_catalog
                      std::wstring(L"foreignTable"),  // expected_table
                      std::wstring(L"id"),            // expected_column
                      SQL_BIGINT,                     // expected_data_type
                      std::wstring(L"BIGINT"),        // expected_type_name
                      10,  // expected_column_size (mock returns 10 instead of 19)
                      8,   // expected_buffer_length
                      15,  // expected_decimal_digits (mock returns 15 instead of 0)
                      10,  // expected_num_prec_radix
                      SQL_NULLABLE,           // expected_nullable
                      SQL_BIGINT,             // expected_sql_data_type
                      NULL,                   // expected_date_time_sub
                      8,                      // expected_octet_char_length
                      1,                      // expected_ordinal_position
                      std::wstring(L"YES"));  // expected_is_nullable

  // There is no more column
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(ColumnsMockTest, TestSQLColumnsInvalidTablePattern) {
  SQLWCHAR table_pattern[] = L"non-existent-table";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // There is no column from filter
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

}  // namespace arrow::flight::sql::odbc
