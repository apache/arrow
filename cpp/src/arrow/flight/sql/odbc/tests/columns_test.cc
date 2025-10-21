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
class ColumnsTest : public T {
 public:
  using List = std::list<T>;
};

class ColumnsMockTest : public FlightSQLODBCMockTestBase {};
class ColumnsRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<ColumnsMockTest, ColumnsRemoteTest>;
TYPED_TEST_SUITE(ColumnsTest, TestTypes);

template <typename T>
class ColumnsOdbcV2Test : public T {
 public:
  using List = std::list<T>;
};

class ColumnsOdbcV2MockTest : public FlightSQLOdbcV2MockTestBase {};
class ColumnsOdbcV2RemoteTest : public FlightSQLOdbcV2RemoteTestBase {};
using TestTypesOdbcV2 = ::testing::Types<ColumnsOdbcV2MockTest, ColumnsOdbcV2RemoteTest>;
TYPED_TEST_SUITE(ColumnsOdbcV2Test, TestTypesOdbcV2);

// Helper functions
void checkSQLColumns(
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

  checkSQLColumns(stmt, expected_table, expected_column, expected_data_type,
                  expected_type_name, expected_column_size, expected_buffer_length,
                  expected_decimal_digits, expected_num_prec_radix, expected_nullable,
                  expected_sql_data_type, expected_date_time_sub,
                  expected_octet_char_length, expected_ordinal_position,
                  expected_is_nullable);
}

void checkRemoteSQLColumns(
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
  checkSQLColumns(stmt, expected_table, expected_column, expected_data_type,
                  expected_type_name, expected_column_size, expected_buffer_length,
                  expected_decimal_digits, expected_num_prec_radix, expected_nullable,
                  expected_sql_data_type, expected_date_time_sub,
                  expected_octet_char_length, expected_ordinal_position,
                  expected_is_nullable);
}

void CheckSQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT idx,
                          const std::wstring& expected_column_name,
                          SQLLEN expected_data_type, SQLLEN expected_concise_type,
                          SQLLEN expected_display_size, SQLLEN expected_prec_scale,
                          SQLLEN expected_length,
                          const std::wstring& expected_literal_prefix,
                          const std::wstring& expected_literal_suffix,
                          SQLLEN expected_column_size, SQLLEN expected_column_scale,
                          SQLLEN expected_column_nullability,
                          SQLLEN expected_num_prec_radix, SQLLEN expected_octet_length,
                          SQLLEN expected_searchable, SQLLEN expected_unsigned_column) {
  std::vector<SQLWCHAR> name(ODBC_BUFFER_SIZE);
  SQLSMALLINT name_len = 0;
  std::vector<SQLWCHAR> base_column_name(ODBC_BUFFER_SIZE);
  SQLSMALLINT column_name_len = 0;
  std::vector<SQLWCHAR> label(ODBC_BUFFER_SIZE);
  SQLSMALLINT label_len = 0;
  std::vector<SQLWCHAR> prefix(ODBC_BUFFER_SIZE);
  SQLSMALLINT prefix_len = 0;
  std::vector<SQLWCHAR> suffix(ODBC_BUFFER_SIZE);
  SQLSMALLINT suffix_len = 0;
  SQLLEN data_type = 0;
  SQLLEN concise_type = 0;
  SQLLEN display_size = 0;
  SQLLEN prec_scale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN num_prec_radix = 0;
  SQLLEN octet_length = 0;
  SQLLEN searchable = 0;
  SQLLEN unsigned_col = 0;

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_NAME, &name[0],
                                         (SQLSMALLINT)name.size(), &name_len, 0));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_BASE_COLUMN_NAME, &base_column_name[0],
                            (SQLSMALLINT)base_column_name.size(), &column_name_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LABEL, &label[0],
                                         (SQLSMALLINT)label.size(), &label_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_TYPE, 0, 0, 0, &data_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_CONCISE_TYPE, 0, 0, 0, &concise_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_DISPLAY_SIZE, 0, 0, 0, &display_size));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_FIXED_PREC_SCALE, 0, 0, 0, &prec_scale));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LENGTH, 0, 0, 0, &length));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_PREFIX, &prefix[0],
                                         (SQLSMALLINT)prefix.size(), &prefix_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_LITERAL_SUFFIX, &suffix[0],
                                         (SQLSMALLINT)suffix.size(), &suffix_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_PRECISION, 0, 0, 0, &size));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_SCALE, 0, 0, 0, &scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_NULLABLE, 0, 0, 0, &nullability));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, SQL_DESC_NUM_PREC_RADIX, 0, 0, 0,
                                         &num_prec_radix));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_OCTET_LENGTH, 0, 0, 0, &octet_length));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_SEARCHABLE, 0, 0, 0, &searchable));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_DESC_UNSIGNED, 0, 0, 0, &unsigned_col));

  std::wstring name_str = ConvertToWString(name, name_len);
  std::wstring base_column_name_str = ConvertToWString(base_column_name, column_name_len);
  std::wstring label_str = ConvertToWString(label, label_len);
  std::wstring prefixStr = ConvertToWString(prefix, prefix_len);

  // Assume column name, base column name, and label are equivalent in the result set
  EXPECT_EQ(expected_column_name, name_str);
  EXPECT_EQ(expected_column_name, base_column_name_str);
  EXPECT_EQ(expected_column_name, label_str);
  EXPECT_EQ(expected_data_type, data_type);
  EXPECT_EQ(expected_concise_type, concise_type);
  EXPECT_EQ(expected_display_size, display_size);
  EXPECT_EQ(expected_prec_scale, prec_scale);
  EXPECT_EQ(expected_length, length);
  EXPECT_EQ(expected_literal_prefix, prefixStr);
  EXPECT_EQ(expected_column_size, size);
  EXPECT_EQ(expected_column_scale, scale);
  EXPECT_EQ(expected_column_nullability, nullability);
  EXPECT_EQ(expected_num_prec_radix, num_prec_radix);
  EXPECT_EQ(expected_octet_length, octet_length);
  EXPECT_EQ(expected_searchable, searchable);
  EXPECT_EQ(expected_unsigned_column, unsigned_col);
}

void CheckSQLColAttributes(SQLHSTMT stmt, SQLUSMALLINT idx,
                           const std::wstring& expected_column_name,
                           SQLLEN expected_data_type, SQLLEN expected_display_size,
                           SQLLEN expected_prec_scale, SQLLEN expected_length,
                           SQLLEN expected_column_size, SQLLEN expected_column_scale,
                           SQLLEN expected_column_nullability, SQLLEN expected_searchable,
                           SQLLEN expected_unsigned_column) {
  std::vector<SQLWCHAR> name(ODBC_BUFFER_SIZE);
  SQLSMALLINT name_len = 0;
  std::vector<SQLWCHAR> label(ODBC_BUFFER_SIZE);
  SQLSMALLINT label_len = 0;
  SQLLEN data_type = 0;
  SQLLEN display_size = 0;
  SQLLEN prec_scale = 0;
  SQLLEN length = 0;
  SQLLEN size = 0;
  SQLLEN scale = 0;
  SQLLEN nullability = 0;
  SQLLEN searchable = 0;
  SQLLEN unsigned_col = 0;

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_NAME, &name[0],
                                          (SQLSMALLINT)name.size(), &name_len, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_LABEL, &label[0],
                                          (SQLSMALLINT)label.size(), &label_len, 0));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_TYPE, 0, 0, 0, &data_type));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_DISPLAY_SIZE, 0, 0, 0, &display_size));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(stmt, idx, SQL_COLUMN_MONEY, 0, 0, 0, &prec_scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_LENGTH, 0, 0, 0, &length));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_PRECISION, 0, 0, 0, &size));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, SQL_COLUMN_SCALE, 0, 0, 0, &scale));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_NULLABLE, 0, 0, 0, &nullability));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_SEARCHABLE, 0, 0, 0, &searchable));

  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, SQL_COLUMN_UNSIGNED, 0, 0, 0, &unsigned_col));

  std::wstring name_str = ConvertToWString(name, name_len);
  std::wstring label_str = ConvertToWString(label, label_len);

  EXPECT_EQ(expected_column_name, name_str);
  EXPECT_EQ(expected_column_name, label_str);
  EXPECT_EQ(expected_data_type, data_type);
  EXPECT_EQ(expected_display_size, display_size);
  EXPECT_EQ(expected_length, length);
  EXPECT_EQ(expected_column_size, size);
  EXPECT_EQ(expected_column_scale, scale);
  EXPECT_EQ(expected_column_nullability, nullability);
  EXPECT_EQ(expected_searchable, searchable);
  EXPECT_EQ(expected_unsigned_column, unsigned_col);
}

void CheckSQLColAttributeString(SQLHSTMT stmt, const std::wstring& wsql, SQLUSMALLINT idx,
                                SQLUSMALLINT field_identifier,
                                const std::wstring& expected_attr_string) {
  if (!wsql.empty()) {
    // Execute query
    std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
    ASSERT_EQ(SQL_SUCCESS,
              SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

    ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));
  }

  // check SQLColAttribute string attribute
  std::vector<SQLWCHAR> str_val(ODBC_BUFFER_SIZE);
  SQLSMALLINT str_len = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, field_identifier, &str_val[0],
                                         (SQLSMALLINT)str_val.size(), &str_len, 0));

  std::wstring attr_str = ConvertToWString(str_val, str_len);
  ASSERT_EQ(expected_attr_string, attr_str);
}

void CheckSQLColAttributeNumeric(SQLHSTMT stmt, const std::wstring& wsql,
                                 SQLUSMALLINT idx, SQLUSMALLINT field_identifier,
                                 SQLLEN expected_attr_numeric) {
  // Execute query and check SQLColAttribute numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));

  SQLLEN num_val = 0;
  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(stmt, idx, field_identifier, 0, 0, 0, &num_val));
  ASSERT_EQ(expected_attr_numeric, num_val);
}

void CheckSQLColAttributesString(SQLHSTMT stmt, const std::wstring& wsql,
                                 SQLUSMALLINT idx, SQLUSMALLINT field_identifier,
                                 const std::wstring& expected_attr_string) {
  if (!wsql.empty()) {
    // Execute query
    std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
    ASSERT_EQ(SQL_SUCCESS,
              SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

    ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));
  }

  // check ODBC 2.0 API SQLColAttributes string attribute
  std::vector<SQLWCHAR> str_val(ODBC_BUFFER_SIZE);
  SQLSMALLINT str_len = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLColAttributes(stmt, idx, field_identifier, &str_val[0],
                                          (SQLSMALLINT)str_val.size(), &str_len, 0));

  std::wstring attr_str = ConvertToWString(str_val, str_len);
  ASSERT_EQ(expected_attr_string, attr_str);
}

void CheckSQLColAttributesNumeric(SQLHSTMT stmt, const std::wstring& wsql,
                                  SQLUSMALLINT idx, SQLUSMALLINT field_identifier,
                                  SQLLEN expected_attr_numeric) {
  // Execute query and check ODBC 2.0 API SQLColAttributes numeric attribute
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());
  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(stmt));

  SQLLEN num_val = 0;
  ASSERT_EQ(SQL_SUCCESS,
            SQLColAttributes(stmt, idx, field_identifier, 0, 0, 0, &num_val));
  ASSERT_EQ(expected_attr_numeric, num_val);
}

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
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits

  SQLWCHAR table_pattern[] = L"ODBCTest";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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
  // GH-47159: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits

  SQLWCHAR table_pattern[] = L"ODBCTest";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // Check 1st Column
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(this->stmt,
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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

  checkRemoteSQLColumns(
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

TEST_F(ColumnsMockTest, TestSQLColumnscolumn_pattern) {
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

TEST_F(ColumnsMockTest, TestSQLColumnsTablecolumn_pattern) {
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

TEST_F(ColumnsMockTest, TestSQLColumnsInvalidtable_pattern) {
  SQLWCHAR table_pattern[] = L"non-existent-table";
  SQLWCHAR column_pattern[] = L"%";

  ASSERT_EQ(SQL_SUCCESS, SQLColumns(this->stmt, nullptr, SQL_NTS, nullptr, SQL_NTS,
                                    table_pattern, SQL_NTS, column_pattern, SQL_NTS));

  // There is no column from filter
  EXPECT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TYPED_TEST(ColumnsTest, SQLColAttributeTestInputData) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> character_attr(ODBC_BUFFER_SIZE);
  SQLSMALLINT character_attr_len = 0;
  SQLLEN numeric_attr = 0;

  // All character values populated
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_DESC_NAME, &character_attr[0],
                            (SQLSMALLINT)character_attr.size(), &character_attr_len, 0));

  // All numeric values populated
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, &numeric_attr));

  // Pass null values, driver should not throw error
  EXPECT_EQ(SQL_SUCCESS,
            SQLColAttribute(this->stmt, idx, SQL_COLUMN_TABLE_NAME, 0, 0, 0, 0));

  EXPECT_EQ(SQL_SUCCESS, SQLColAttribute(this->stmt, idx, SQL_DESC_COUNT, 0, 0, 0, 0));
}

TYPED_TEST(ColumnsTest, SQLColAttributeGetCharacterLen) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLSMALLINT character_attr_len = 0;

  // Check length of character attribute
  ASSERT_EQ(SQL_SUCCESS, SQLColAttribute(this->stmt, 1, SQL_DESC_BASE_COLUMN_NAME, 0, 0,
                                         &character_attr_len, 0));
  EXPECT_EQ(4 * ODBC::GetSqlWCharSize(), character_attr_len);
}

TYPED_TEST(ColumnsTest, SQLColAttributeInvalidFieldId) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT invalid_field_id = -100;
  SQLUSMALLINT idx = 1;
  std::vector<SQLWCHAR> character_attr(ODBC_BUFFER_SIZE);
  SQLSMALLINT character_attr_len = 0;
  SQLLEN numeric_attr = 0;

  ASSERT_EQ(SQL_ERROR,
            SQLColAttribute(this->stmt, idx, invalid_field_id, &character_attr[0],
                            (SQLSMALLINT)character_attr.size(), &character_attr_len, 0));
  // Verify invalid descriptor field identifier error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY091);
}

TYPED_TEST(ColumnsTest, SQLColAttributeInvalidColId) {
  std::wstring wsql = L"SELECT 1 as col1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLUSMALLINT invalid_col_id = 2;
  std::vector<SQLWCHAR> character_attr(ODBC_BUFFER_SIZE);
  SQLSMALLINT character_attr_len = 0;

  ASSERT_EQ(SQL_ERROR,
            SQLColAttribute(this->stmt, invalid_col_id, SQL_DESC_BASE_COLUMN_NAME,
                            &character_attr[0], (SQLSMALLINT)character_attr.size(),
                            &character_attr_len, 0));
  // Verify invalid descriptor index error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeAllTypes) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"bigint_col"),  // expected_column_name
                       SQL_BIGINT,                   // expected_data_type
                       SQL_BIGINT,                   // expected_concise_type
                       20,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       10,                           // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_PRED_NONE,                // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"char_col"),  // expected_column_name
                       SQL_WVARCHAR,               // expected_data_type
                       SQL_WVARCHAR,               // expected_concise_type
                       0,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       0,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       0,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       0,                          // expected_octet_length
                       SQL_PRED_NONE,              // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"varbinary_col"),  // expected_column_name
                       SQL_BINARY,                      // expected_data_type
                       SQL_BINARY,                      // expected_concise_type
                       0,                               // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       0,                               // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       0,                               // expected_column_size
                       0,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       0,                               // expected_octet_length
                       SQL_PRED_NONE,                   // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"double_col"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_PRED_NONE,                // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));
  CheckSQLColAttributes(this->stmt, 1,
                        std::wstring(L"bigint_col"),  // expected_column_name
                        SQL_BIGINT,                   // expected_data_type
                        20,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_PRED_NONE,                // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 2,
                        std::wstring(L"char_col"),  // expected_column_name
                        SQL_WVARCHAR,               // expected_data_type
                        0,                          // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        0,                          // expected_length
                        0,                          // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_PRED_NONE,              // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 3,
                        std::wstring(L"varbinary_col"),  // expected_column_name
                        SQL_BINARY,                      // expected_data_type
                        0,                               // expected_display_size
                        SQL_FALSE,                       // expected_prec_scale
                        0,                               // expected_length
                        0,                               // expected_column_size
                        0,                               // expected_column_scale
                        SQL_NULLABLE,                    // expected_column_nullability
                        SQL_PRED_NONE,                   // expected_searchable
                        SQL_TRUE);                       // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 4,
                        std::wstring(L"double_col"),  // expected_column_name
                        SQL_DOUBLE,                   // expected_data_type
                        24,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_PRED_NONE,                // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeAllTypes) {
  // Test assumes there is a table $scratch.ODBCTest in remote server

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expected_column_name
                       SQL_INTEGER,                    // expected_data_type
                       SQL_INTEGER,                    // expected_concise_type
                       11,                             // expected_display_size
                       SQL_FALSE,                      // expected_prec_scale
                       4,                              // expected_length
                       std::wstring(L""),              // expected_literal_prefix
                       std::wstring(L""),              // expected_literal_suffix
                       4,                              // expected_column_size
                       0,                              // expected_column_scale
                       SQL_NULLABLE,                   // expected_column_nullability
                       10,                             // expected_num_prec_radix
                       4,                              // expected_octet_length
                       SQL_SEARCHABLE,                 // expected_searchable
                       SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expected_column_name
                       SQL_BIGINT,                    // expected_data_type
                       SQL_BIGINT,                    // expected_concise_type
                       20,                            // expected_display_size
                       SQL_FALSE,                     // expected_prec_scale
                       8,                             // expected_length
                       std::wstring(L""),             // expected_literal_prefix
                       std::wstring(L""),             // expected_literal_suffix
                       8,                             // expected_column_size
                       0,                             // expected_column_scale
                       SQL_NULLABLE,                  // expected_column_nullability
                       10,                            // expected_num_prec_radix
                       8,                             // expected_octet_length
                       SQL_SEARCHABLE,                // expected_searchable
                       SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expected_column_name
                       SQL_DECIMAL,                        // expected_data_type
                       SQL_DECIMAL,                        // expected_concise_type
                       40,                                 // expected_display_size
                       SQL_FALSE,                          // expected_prec_scale
                       19,                                 // expected_length
                       std::wstring(L""),                  // expected_literal_prefix
                       std::wstring(L""),                  // expected_literal_suffix
                       19,                                 // expected_column_size
                       0,                                  // expected_column_scale
                       SQL_NULLABLE,                       // expected_column_nullability
                       10,                                 // expected_num_prec_radix
                       40,                                 // expected_octet_length
                       SQL_SEARCHABLE,                     // expected_searchable
                       SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expected_column_name
                       SQL_FLOAT,                   // expected_data_type
                       SQL_FLOAT,                   // expected_concise_type
                       24,                          // expected_display_size
                       SQL_FALSE,                   // expected_prec_scale
                       8,                           // expected_length
                       std::wstring(L""),           // expected_literal_prefix
                       std::wstring(L""),           // expected_literal_suffix
                       8,                           // expected_column_size
                       0,                           // expected_column_scale
                       SQL_NULLABLE,                // expected_column_nullability
                       2,                           // expected_num_prec_radix
                       8,                           // expected_octet_length
                       SQL_SEARCHABLE,              // expected_searchable
                       SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_SEARCHABLE,               // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expected_column_name
                       SQL_BIT,                    // expected_data_type
                       SQL_BIT,                    // expected_concise_type
                       1,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       1,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       1,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       1,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TYPE_DATE,              // expected_concise_type
                       10,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       10,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       10,                         // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TYPE_TIME,              // expected_concise_type
                       12,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       12,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       12,                         // expected_column_size
                       3,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expected_column_name
                       SQL_DATETIME,                    // expected_data_type
                       SQL_TYPE_TIMESTAMP,              // expected_concise_type
                       23,                              // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       23,                              // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       23,                              // expected_column_size
                       3,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       16,                              // expected_octet_length
                       SQL_SEARCHABLE,                  // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributeAllTypesODBCVer2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttribute(this->stmt, 1,
                       std::wstring(L"sinteger_max"),  // expected_column_name
                       SQL_INTEGER,                    // expected_data_type
                       SQL_INTEGER,                    // expected_concise_type
                       11,                             // expected_display_size
                       SQL_FALSE,                      // expected_prec_scale
                       4,                              // expected_length
                       std::wstring(L""),              // expected_literal_prefix
                       std::wstring(L""),              // expected_literal_suffix
                       4,                              // expected_column_size
                       0,                              // expected_column_scale
                       SQL_NULLABLE,                   // expected_column_nullability
                       10,                             // expected_num_prec_radix
                       4,                              // expected_octet_length
                       SQL_SEARCHABLE,                 // expected_searchable
                       SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 2,
                       std::wstring(L"sbigint_max"),  // expected_column_name
                       SQL_BIGINT,                    // expected_data_type
                       SQL_BIGINT,                    // expected_concise_type
                       20,                            // expected_display_size
                       SQL_FALSE,                     // expected_prec_scale
                       8,                             // expected_length
                       std::wstring(L""),             // expected_literal_prefix
                       std::wstring(L""),             // expected_literal_suffix
                       8,                             // expected_column_size
                       0,                             // expected_column_scale
                       SQL_NULLABLE,                  // expected_column_nullability
                       10,                            // expected_num_prec_radix
                       8,                             // expected_octet_length
                       SQL_SEARCHABLE,                // expected_searchable
                       SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 3,
                       std::wstring(L"decimal_positive"),  // expected_column_name
                       SQL_DECIMAL,                        // expected_data_type
                       SQL_DECIMAL,                        // expected_concise_type
                       40,                                 // expected_display_size
                       SQL_FALSE,                          // expected_prec_scale
                       19,                                 // expected_length
                       std::wstring(L""),                  // expected_literal_prefix
                       std::wstring(L""),                  // expected_literal_suffix
                       19,                                 // expected_column_size
                       0,                                  // expected_column_scale
                       SQL_NULLABLE,                       // expected_column_nullability
                       10,                                 // expected_num_prec_radix
                       40,                                 // expected_octet_length
                       SQL_SEARCHABLE,                     // expected_searchable
                       SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 4,
                       std::wstring(L"float_max"),  // expected_column_name
                       SQL_FLOAT,                   // expected_data_type
                       SQL_FLOAT,                   // expected_concise_type
                       24,                          // expected_display_size
                       SQL_FALSE,                   // expected_prec_scale
                       8,                           // expected_length
                       std::wstring(L""),           // expected_literal_prefix
                       std::wstring(L""),           // expected_literal_suffix
                       8,                           // expected_column_size
                       0,                           // expected_column_scale
                       SQL_NULLABLE,                // expected_column_nullability
                       2,                           // expected_num_prec_radix
                       8,                           // expected_octet_length
                       SQL_SEARCHABLE,              // expected_searchable
                       SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 5,
                       std::wstring(L"double_max"),  // expected_column_name
                       SQL_DOUBLE,                   // expected_data_type
                       SQL_DOUBLE,                   // expected_concise_type
                       24,                           // expected_display_size
                       SQL_FALSE,                    // expected_prec_scale
                       8,                            // expected_length
                       std::wstring(L""),            // expected_literal_prefix
                       std::wstring(L""),            // expected_literal_suffix
                       8,                            // expected_column_size
                       0,                            // expected_column_scale
                       SQL_NULLABLE,                 // expected_column_nullability
                       2,                            // expected_num_prec_radix
                       8,                            // expected_octet_length
                       SQL_SEARCHABLE,               // expected_searchable
                       SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 6,
                       std::wstring(L"bit_true"),  // expected_column_name
                       SQL_BIT,                    // expected_data_type
                       SQL_BIT,                    // expected_concise_type
                       1,                          // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       1,                          // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       1,                          // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       1,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 7,
                       std::wstring(L"date_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_DATE,                   // expected_concise_type
                       10,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       10,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       10,                         // expected_column_size
                       0,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 8,
                       std::wstring(L"time_max"),  // expected_column_name
                       SQL_DATETIME,               // expected_data_type
                       SQL_TIME,                   // expected_concise_type
                       12,                         // expected_display_size
                       SQL_FALSE,                  // expected_prec_scale
                       12,                         // expected_length
                       std::wstring(L""),          // expected_literal_prefix
                       std::wstring(L""),          // expected_literal_suffix
                       12,                         // expected_column_size
                       3,                          // expected_column_scale
                       SQL_NULLABLE,               // expected_column_nullability
                       0,                          // expected_num_prec_radix
                       6,                          // expected_octet_length
                       SQL_SEARCHABLE,             // expected_searchable
                       SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttribute(this->stmt, 9,
                       std::wstring(L"timestamp_max"),  // expected_column_name
                       SQL_DATETIME,                    // expected_data_type
                       SQL_TIMESTAMP,                   // expected_concise_type
                       23,                              // expected_display_size
                       SQL_FALSE,                       // expected_prec_scale
                       23,                              // expected_length
                       std::wstring(L""),               // expected_literal_prefix
                       std::wstring(L""),               // expected_literal_suffix
                       23,                              // expected_column_size
                       3,                               // expected_column_scale
                       SQL_NULLABLE,                    // expected_column_nullability
                       0,                               // expected_num_prec_radix
                       16,                              // expected_octet_length
                       SQL_SEARCHABLE,                  // expected_searchable
                       SQL_TRUE);                       // expected_unsigned_column
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesAllTypesODBCVer2) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Test assumes there is a table $scratch.ODBCTest in remote server
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckSQLColAttributes(this->stmt, 1,
                        std::wstring(L"sinteger_max"),  // expected_column_name
                        SQL_INTEGER,                    // expected_data_type
                        11,                             // expected_display_size
                        SQL_FALSE,                      // expected_prec_scale
                        4,                              // expected_length
                        4,                              // expected_column_size
                        0,                              // expected_column_scale
                        SQL_NULLABLE,                   // expected_column_nullability
                        SQL_SEARCHABLE,                 // expected_searchable
                        SQL_FALSE);                     // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 2,
                        std::wstring(L"sbigint_max"),  // expected_column_name
                        SQL_BIGINT,                    // expected_data_type
                        20,                            // expected_display_size
                        SQL_FALSE,                     // expected_prec_scale
                        8,                             // expected_length
                        8,                             // expected_column_size
                        0,                             // expected_column_scale
                        SQL_NULLABLE,                  // expected_column_nullability
                        SQL_SEARCHABLE,                // expected_searchable
                        SQL_FALSE);                    // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 3,
                        std::wstring(L"decimal_positive"),  // expected_column_name
                        SQL_DECIMAL,                        // expected_data_type
                        40,                                 // expected_display_size
                        SQL_FALSE,                          // expected_prec_scale
                        19,                                 // expected_length
                        19,                                 // expected_column_size
                        0,                                  // expected_column_scale
                        SQL_NULLABLE,                       // expected_column_nullability
                        SQL_SEARCHABLE,                     // expected_searchable
                        SQL_FALSE);                         // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 4,
                        std::wstring(L"float_max"),  // expected_column_name
                        SQL_FLOAT,                   // expected_data_type
                        24,                          // expected_display_size
                        SQL_FALSE,                   // expected_prec_scale
                        8,                           // expected_length
                        8,                           // expected_column_size
                        0,                           // expected_column_scale
                        SQL_NULLABLE,                // expected_column_nullability
                        SQL_SEARCHABLE,              // expected_searchable
                        SQL_FALSE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 5,
                        std::wstring(L"double_max"),  // expected_column_name
                        SQL_DOUBLE,                   // expected_data_type
                        24,                           // expected_display_size
                        SQL_FALSE,                    // expected_prec_scale
                        8,                            // expected_length
                        8,                            // expected_column_size
                        0,                            // expected_column_scale
                        SQL_NULLABLE,                 // expected_column_nullability
                        SQL_SEARCHABLE,               // expected_searchable
                        SQL_FALSE);                   // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 6,
                        std::wstring(L"bit_true"),  // expected_column_name
                        SQL_BIT,                    // expected_data_type
                        1,                          // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        1,                          // expected_length
                        1,                          // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 7,
                        std::wstring(L"date_max"),  // expected_column_name
                        SQL_DATE,                   // expected_data_type
                        10,                         // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        10,                         // expected_length
                        10,                         // expected_column_size
                        0,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 8,
                        std::wstring(L"time_max"),  // expected_column_name
                        SQL_TIME,                   // expected_data_type
                        12,                         // expected_display_size
                        SQL_FALSE,                  // expected_prec_scale
                        12,                         // expected_length
                        12,                         // expected_column_size
                        3,                          // expected_column_scale
                        SQL_NULLABLE,               // expected_column_nullability
                        SQL_SEARCHABLE,             // expected_searchable
                        SQL_TRUE);                  // expected_unsigned_column

  CheckSQLColAttributes(this->stmt, 9,
                        std::wstring(L"timestamp_max"),  // expected_column_name
                        SQL_TIMESTAMP,                   // expected_data_type
                        23,                              // expected_display_size
                        SQL_FALSE,                       // expected_prec_scale
                        23,                              // expected_length
                        23,                              // expected_column_size
                        3,                               // expected_column_scale
                        SQL_NULLABLE,                    // expected_column_nullability
                        SQL_SEARCHABLE,                  // expected_searchable
                        SQL_TRUE);                       // expected_unsigned_column
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column

  std::wstring wsql = this->GetQueryAllDataTypes();
  // Int column
  CheckSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_CASE_SENSITIVE, SQL_FALSE);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  CheckSQLColAttributeNumeric(this->stmt, wsql, 28, SQL_DESC_CASE_SENSITIVE, SQL_FALSE);
}

TYPED_TEST(ColumnsOdbcV2Test, TestSQLColAttributesCaseSensitive) {
  // Arrow limitation: returns SQL_FALSE for case sensitive column
  // Tests ODBC 2.0 API SQLColAttributes

  std::wstring wsql = this->GetQueryAllDataTypes();
  // Int column
  CheckSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_CASE_SENSITIVE, SQL_FALSE);
  SQLFreeStmt(this->stmt, SQL_CLOSE);
  // Varchar column
  CheckSQLColAttributesNumeric(this->stmt, wsql, 28, SQL_COLUMN_CASE_SENSITIVE,
                               SQL_FALSE);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeUniqueValue) {
  // Mock server limitation: returns false for auto-increment column
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_AUTO_UNIQUE_VALUE, SQL_FALSE);
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesAutoIncrement) {
  // Tests ODBC 2.0 API SQLColAttributes
  // Mock server limitation: returns false for auto-increment column
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_COLUMN_AUTO_INCREMENT, SQL_FALSE);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeBasetable_name) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_BASE_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributestable_name) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TABLE_NAME,
                              std::wstring(L"AllTypesTable"));
}

TEST_F(ColumnsMockTest, TestSQLColAttributecatalog_name) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributecatalog_name) {
  // Remote server does not have catalogs

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_CATALOG_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesQualifierName) {
  // Mock server limitattion: mock doesn't return catalog for result metadata,
  // and the defautl catalog should be 'main'
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesQualifierName) {
  // Remote server does not have catalogs
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_COLUMN_QUALIFIER_NAME,
                             std::wstring(L""));
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeCount) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Pass 0 as column number, driver should ignore it
  CheckSQLColAttributeNumeric(this->stmt, wsql, 0, SQL_DESC_COUNT, 32);
}

TEST_F(ColumnsMockTest, TestSQLColAttributeLocalTypeName) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server doesn't have local type name
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeLocalTypeName) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_LOCAL_TYPE_NAME,
                             std::wstring(L"INTEGER"));
}

TEST_F(ColumnsMockTest, TestSQLColAttributeschema_name) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeschema_name) {
  // Test assumes there is a table $scratch.ODBCTest in remote server

  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_SCHEMA_NAME,
                             std::wstring(L""));
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesOwnerName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't have schemas
  CheckSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME,
                              std::wstring(L""));
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesOwnerName) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  // Remote server limitation: doesn't return schema name, expected schema name is
  // $scratch
  CheckSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_OWNER_NAME,
                              std::wstring(L""));
}

TEST_F(ColumnsMockTest, TestSQLColAttributetable_name) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TABLE_NAME,
                             std::wstring(L"AllTypesTable"));
}

TEST_F(ColumnsMockTest, TestSQLColAttributeTypeName) {
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME,
                             std::wstring(L"BIGINT"));
  CheckSQLColAttributeString(this->stmt, L"", 2, SQL_DESC_TYPE_NAME,
                             std::wstring(L"WVARCHAR"));
  CheckSQLColAttributeString(this->stmt, L"", 3, SQL_DESC_TYPE_NAME,
                             std::wstring(L"BINARY"));
  CheckSQLColAttributeString(this->stmt, L"", 4, SQL_DESC_TYPE_NAME,
                             std::wstring(L"DOUBLE"));
}

TEST_F(ColumnsRemoteTest, TestSQLColAttributeTypeName) {
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  CheckSQLColAttributeString(this->stmt, wsql, 1, SQL_DESC_TYPE_NAME,
                             std::wstring(L"INTEGER"));
  CheckSQLColAttributeString(this->stmt, L"", 2, SQL_DESC_TYPE_NAME,
                             std::wstring(L"BIGINT"));
  CheckSQLColAttributeString(this->stmt, L"", 3, SQL_DESC_TYPE_NAME,
                             std::wstring(L"DECIMAL"));
  CheckSQLColAttributeString(this->stmt, L"", 4, SQL_DESC_TYPE_NAME,
                             std::wstring(L"FLOAT"));
  CheckSQLColAttributeString(this->stmt, L"", 5, SQL_DESC_TYPE_NAME,
                             std::wstring(L"DOUBLE"));
  CheckSQLColAttributeString(this->stmt, L"", 6, SQL_DESC_TYPE_NAME,
                             std::wstring(L"BOOLEAN"));
  CheckSQLColAttributeString(this->stmt, L"", 7, SQL_DESC_TYPE_NAME,
                             std::wstring(L"DATE"));
  CheckSQLColAttributeString(this->stmt, L"", 8, SQL_DESC_TYPE_NAME,
                             std::wstring(L"TIME"));
  CheckSQLColAttributeString(this->stmt, L"", 9, SQL_DESC_TYPE_NAME,
                             std::wstring(L"TIMESTAMP"));
}

TEST_F(ColumnsOdbcV2MockTest, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  this->CreateTableAllDataType();

  std::wstring wsql = L"SELECT * from AllTypesTable;";
  // Mock server doesn't return data source-dependent data type name
  CheckSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"BIGINT"));
  CheckSQLColAttributesString(this->stmt, L"", 2, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"WVARCHAR"));
  CheckSQLColAttributesString(this->stmt, L"", 3, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"BINARY"));
  CheckSQLColAttributesString(this->stmt, L"", 4, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"DOUBLE"));
}

TEST_F(ColumnsOdbcV2RemoteTest, TestSQLColAttributesTypeName) {
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = L"SELECT * from $scratch.ODBCTest;";
  CheckSQLColAttributesString(this->stmt, wsql, 1, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"INTEGER"));
  CheckSQLColAttributesString(this->stmt, L"", 2, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"BIGINT"));
  CheckSQLColAttributesString(this->stmt, L"", 3, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"DECIMAL"));
  CheckSQLColAttributesString(this->stmt, L"", 4, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"FLOAT"));
  CheckSQLColAttributesString(this->stmt, L"", 5, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"DOUBLE"));
  CheckSQLColAttributesString(this->stmt, L"", 6, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"BOOLEAN"));
  CheckSQLColAttributesString(this->stmt, L"", 7, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"DATE"));
  CheckSQLColAttributesString(this->stmt, L"", 8, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"TIME"));
  CheckSQLColAttributesString(this->stmt, L"", 9, SQL_COLUMN_TYPE_NAME,
                              std::wstring(L"TIMESTAMP"));
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeUnnamed) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  CheckSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UNNAMED, SQL_NAMED);
}

TYPED_TEST(ColumnsTest, TestSQLColAttributeUpdatable) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  CheckSQLColAttributeNumeric(this->stmt, wsql, 1, SQL_DESC_UPDATABLE,
                              SQL_ATTR_READWRITE_UNKNOWN);
}

TYPED_TEST(ColumnsOdbcV2Test, TestSQLColAttributesUpdatable) {
  // Tests ODBC 2.0 API SQLColAttributes
  std::wstring wsql = this->GetQueryAllDataTypes();
  // Mock server and remote server do not return updatable information
  CheckSQLColAttributesNumeric(this->stmt, wsql, 1, SQL_COLUMN_UPDATABLE,
                               SQL_ATTR_READWRITE_UNKNOWN);
}

TEST_F(ColumnsMockTest, SQLDescribeColValidateInput) {
  this->CreateTestTables();

  SQLWCHAR sql_query[] = L"SELECT * FROM TestTable LIMIT 1;";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  SQLUSMALLINT bookmark_column = 0;
  SQLUSMALLINT out_of_range_column = 4;
  SQLUSMALLINT negative_column = -1;
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Invalid descriptor index - Bookmarks are not supported
  EXPECT_EQ(SQL_ERROR, SQLDescribeCol(this->stmt, bookmark_column, column_name,
                                      buf_char_len, &name_length, &data_type,
                                      &column_size, &decimal_digits, &nullable));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  // Invalid descriptor index - index out of range
  EXPECT_EQ(SQL_ERROR, SQLDescribeCol(this->stmt, out_of_range_column, column_name,
                                      buf_char_len, &name_length, &data_type,
                                      &column_size, &decimal_digits, &nullable));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);

  // Invalid descriptor index - index out of range
  EXPECT_EQ(SQL_ERROR, SQLDescribeCol(this->stmt, negative_column, column_name,
                                      buf_char_len, &name_length, &data_type,
                                      &column_size, &decimal_digits, &nullable));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_07009);
}

TEST_F(ColumnsMockTest, SQLDescribeColQueryAllDataTypesMetadata) {
  // Mock server has a limitation where only SQL_WVARCHAR column type values are returned
  // from SELECT AS queries

  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLWCHAR* column_names[] = {
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
  SQLSMALLINT column_data_types[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_WVARCHAR};

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  for (size_t i = 0; i < sizeof(column_names) / sizeof(*column_names); ++i) {
    column_index = i + 1;

    ASSERT_EQ(SQL_SUCCESS, SQLDescribeCol(this->stmt, column_index, column_name,
                                          buf_char_len, &name_length, &column_data_type,
                                          &column_size, &decimal_digits, &nullable));

    EXPECT_EQ(wcslen(column_names[i]), name_length);

    std::wstring returned(column_name, column_name + name_length);
    EXPECT_EQ(column_names[i], returned);
    EXPECT_EQ(column_data_types[i], column_data_type);
    EXPECT_EQ(1024, column_size);
    EXPECT_EQ(0, decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}

TEST_F(ColumnsRemoteTest, SQLDescribeColQueryAllDataTypesMetadata) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLWCHAR* column_names[] = {
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
  SQLSMALLINT column_data_types[] = {
      SQL_INTEGER,        SQL_INTEGER,       SQL_INTEGER,  SQL_INTEGER,   SQL_INTEGER,
      SQL_INTEGER,        SQL_INTEGER,       SQL_INTEGER,  SQL_INTEGER,   SQL_INTEGER,
      SQL_BIGINT,         SQL_BIGINT,        SQL_BIGINT,   SQL_BIGINT,    SQL_BIGINT,
      SQL_WVARCHAR,       SQL_DECIMAL,       SQL_DECIMAL,  SQL_FLOAT,     SQL_FLOAT,
      SQL_DOUBLE,         SQL_DOUBLE,        SQL_BIT,      SQL_BIT,       SQL_WVARCHAR,
      SQL_WVARCHAR,       SQL_WVARCHAR,      SQL_WVARCHAR, SQL_TYPE_DATE, SQL_TYPE_DATE,
      SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP};
  SQLULEN column_sizes[] = {4, 4, 4,     4,     4,     4,     4,  4,  4,  4, 8,
                            8, 8, 8,     8,     65536, 19,    19, 8,  8,  8, 8,
                            1, 1, 65536, 65536, 65536, 65536, 10, 10, 23, 23};
  SQLULEN column_decimal_digits[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  0,  0,  0,
                                     0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 10, 23, 23};

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
    EXPECT_EQ(column_decimal_digits[i], decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}

TEST_F(ColumnsRemoteTest, SQLDescribeColODBCTestTableMetadata) {
  // Test assumes there is a table $scratch.ODBCTest in remote server

  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR sql_query[] = L"SELECT * from $scratch.ODBCTest LIMIT 1;";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"sinteger_max",     (SQLWCHAR*)L"sbigint_max",
                              (SQLWCHAR*)L"decimal_positive", (SQLWCHAR*)L"float_max",
                              (SQLWCHAR*)L"double_max",       (SQLWCHAR*)L"bit_true",
                              (SQLWCHAR*)L"date_max",         (SQLWCHAR*)L"time_max",
                              (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT column_data_types[] = {SQL_INTEGER,   SQL_BIGINT,    SQL_DECIMAL,
                                     SQL_FLOAT,     SQL_DOUBLE,    SQL_BIT,
                                     SQL_TYPE_DATE, SQL_TYPE_TIME, SQL_TYPE_TIMESTAMP};
  SQLULEN column_sizes[] = {4, 8, 19, 8, 8, 1, 10, 12, 23};
  SQLULEN columndecimal_digits[] = {0, 0, 0, 0, 0, 0, 10, 12, 23};

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
    EXPECT_EQ(columndecimal_digits[i], decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}

TEST_F(ColumnsOdbcV2RemoteTest, SQLDescribeColODBCTestTableMetadataODBC2) {
  // Test assumes there is a table $scratch.ODBCTest in remote server
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR sql_query[] = L"SELECT * from $scratch.ODBCTest LIMIT 1;";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"sinteger_max",     (SQLWCHAR*)L"sbigint_max",
                              (SQLWCHAR*)L"decimal_positive", (SQLWCHAR*)L"float_max",
                              (SQLWCHAR*)L"double_max",       (SQLWCHAR*)L"bit_true",
                              (SQLWCHAR*)L"date_max",         (SQLWCHAR*)L"time_max",
                              (SQLWCHAR*)L"timestamp_max"};
  SQLSMALLINT column_data_types[] = {SQL_INTEGER, SQL_BIGINT, SQL_DECIMAL,
                                     SQL_FLOAT,   SQL_DOUBLE, SQL_BIT,
                                     SQL_DATE,    SQL_TIME,   SQL_TIMESTAMP};
  SQLULEN column_sizes[] = {4, 8, 19, 8, 8, 1, 10, 12, 23};
  SQLULEN columndecimal_digits[] = {0, 0, 0, 0, 0, 0, 10, 12, 23};

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
    EXPECT_EQ(columndecimal_digits[i], decimal_digits);
    EXPECT_EQ(SQL_NULLABLE, nullable);

    name_length = 0;
    column_data_type = 0;
    column_size = 0;
    decimal_digits = 0;
    nullable = 0;
  }
}

TEST_F(ColumnsMockTest, SQLDescribeColAllTypesTableMetadata) {
  this->CreateTableAllDataType();

  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR sql_query[] = L"SELECT * from AllTypesTable LIMIT 1;";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"bigint_col", (SQLWCHAR*)L"char_col",
                              (SQLWCHAR*)L"varbinary_col", (SQLWCHAR*)L"double_col"};
  SQLSMALLINT column_data_types[] = {SQL_BIGINT, SQL_WVARCHAR, SQL_BINARY, SQL_DOUBLE};
  SQLULEN column_sizes[] = {8, 0, 0, 8};

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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

TEST_F(ColumnsMockTest, SQLDescribeColUnicodeTableMetadata) {
  this->CreateUnicodeTable();

  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 1;

  SQLWCHAR sql_query[] = L"SELECT * from 数据 LIMIT 1;";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  SQLWCHAR expected_column_name[] = L"资料";
  SQLSMALLINT expected_column_data_type = SQL_WVARCHAR;
  SQLULEN expected_column_size = 0;

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_SUCCESS, SQLDescribeCol(this->stmt, column_index, column_name,
                                        buf_char_len, &name_length, &column_data_type,
                                        &column_size, &decimal_digits, &nullable));

  EXPECT_EQ(name_length, wcslen(expected_column_name));

  std::wstring returned(column_name, column_name + name_length);
  EXPECT_EQ(returned, expected_column_name);
  EXPECT_EQ(column_data_type, expected_column_data_type);
  EXPECT_EQ(column_size, expected_column_size);
  EXPECT_EQ(0, decimal_digits);
  EXPECT_EQ(SQL_NULLABLE, nullable);
}

TYPED_TEST(ColumnsTest, SQLColumnsGetMetadataBySQLDescribeCol) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR* column_names[] = {
      (SQLWCHAR*)L"TABLE_CAT",        (SQLWCHAR*)L"TABLE_SCHEM",
      (SQLWCHAR*)L"TABLE_NAME",       (SQLWCHAR*)L"COLUMN_NAME",
      (SQLWCHAR*)L"DATA_TYPE",        (SQLWCHAR*)L"TYPE_NAME",
      (SQLWCHAR*)L"COLUMN_SIZE",      (SQLWCHAR*)L"BUFFER_LENGTH",
      (SQLWCHAR*)L"DECIMAL_DIGITS",   (SQLWCHAR*)L"NUM_PREC_RADIX",
      (SQLWCHAR*)L"NULLABLE",         (SQLWCHAR*)L"REMARKS",
      (SQLWCHAR*)L"COLUMN_DEF",       (SQLWCHAR*)L"SQL_DATA_TYPE",
      (SQLWCHAR*)L"SQL_DATETIME_SUB", (SQLWCHAR*)L"CHAR_OCTET_LENGTH",
      (SQLWCHAR*)L"ORDINAL_POSITION", (SQLWCHAR*)L"IS_NULLABLE"};
  SQLSMALLINT column_data_types[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_INTEGER,  SQL_INTEGER,  SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_INTEGER,  SQL_WVARCHAR};
  SQLULEN column_sizes[] = {1024, 1024, 1024, 1024, 2, 1024, 4, 4, 2,
                            2,    2,    1024, 1024, 2, 2,    4, 4, 1024};

  ASSERT_EQ(SQL_SUCCESS,
            SQLColumns(this->stmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0));

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

TYPED_TEST(ColumnsOdbcV2Test, SQLColumnsGetMetadataBySQLDescribeColODBC2) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;
  size_t column_index = 0;

  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"TABLE_QUALIFIER",
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
  SQLSMALLINT column_data_types[] = {
      SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_WVARCHAR, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_INTEGER,  SQL_INTEGER,  SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_INTEGER,  SQL_WVARCHAR};
  SQLULEN column_sizes[] = {1024, 1024, 1024, 1024, 2, 1024, 4, 4, 2,
                            2,    2,    1024, 1024, 2, 2,    4, 4, 1024};

  ASSERT_EQ(SQL_SUCCESS,
            SQLColumns(this->stmt, nullptr, 0, nullptr, 0, nullptr, 0, nullptr, 0));

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
