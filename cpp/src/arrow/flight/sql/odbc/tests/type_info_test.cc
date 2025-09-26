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

using std::optional;

void CheckSQLDescribeCol(SQLHSTMT stmt, const SQLUSMALLINT column_index,
                         const std::wstring& expected_name,
                         const SQLSMALLINT& expected_data_type,
                         const SQLULEN& expected_column_size,
                         const SQLSMALLINT& expected_decimal_digits,
                         const SQLSMALLINT& expected_nullable) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / ODBC::GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;

  SQLRETURN ret =
      SQLDescribeCol(stmt, column_index, column_name, buf_char_len, &name_length,
                     &column_data_type, &column_size, &decimal_digits, &nullable);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(name_length, expected_name.size());

  std::wstring returned(column_name, column_name + name_length);
  EXPECT_EQ(returned, expected_name);
  EXPECT_EQ(column_data_type, expected_data_type);
  EXPECT_EQ(column_size, expected_column_size);
  EXPECT_EQ(decimal_digits, expected_decimal_digits);
  EXPECT_EQ(nullable, expected_nullable);
}

void CheckSQLDescribeColODBC2(SQLHSTMT stmt) {
  SQLWCHAR* column_names[] = {(SQLWCHAR*)L"TYPE_NAME",
                              (SQLWCHAR*)L"DATA_TYPE",
                              (SQLWCHAR*)L"PRECISION",
                              (SQLWCHAR*)L"LITERAL_PREFIX",
                              (SQLWCHAR*)L"LITERAL_SUFFIX",
                              (SQLWCHAR*)L"CREATE_PARAMS",
                              (SQLWCHAR*)L"NULLABLE",
                              (SQLWCHAR*)L"CASE_SENSITIVE",
                              (SQLWCHAR*)L"SEARCHABLE",
                              (SQLWCHAR*)L"UNSIGNED_ATTRIBUTE",
                              (SQLWCHAR*)L"MONEY",
                              (SQLWCHAR*)L"AUTO_INCREMENT",
                              (SQLWCHAR*)L"LOCAL_TYPE_NAME",
                              (SQLWCHAR*)L"MINIMUM_SCALE",
                              (SQLWCHAR*)L"MAXIMUM_SCALE",
                              (SQLWCHAR*)L"SQL_DATA_TYPE",
                              (SQLWCHAR*)L"SQL_DATETIME_SUB",
                              (SQLWCHAR*)L"NUM_PREC_RADIX",
                              (SQLWCHAR*)L"INTERVAL_PRECISION"};
  SQLSMALLINT column_data_types[] = {
      SQL_WVARCHAR, SQL_SMALLINT, SQL_INTEGER,  SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT,
      SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT,
      SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_SMALLINT};
  SQLULEN column_sizes[] = {1024, 2, 4,    1024, 1024, 1024, 2, 2, 2, 2,
                            2,    2, 1024, 2,    2,    2,    2, 4, 2};
  SQLSMALLINT column_decimal_digits[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  SQLSMALLINT column_nullable[] = {SQL_NO_NULLS, SQL_NO_NULLS, SQL_NULLABLE, SQL_NULLABLE,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NO_NULLS, SQL_NO_NULLS,
                                   SQL_NO_NULLS, SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLABLE,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NULLABLE, SQL_NO_NULLS,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NULLABLE};

  for (size_t i = 0; i < sizeof(column_names) / sizeof(*column_names); ++i) {
    SQLUSMALLINT column_index = i + 1;
    CheckSQLDescribeCol(stmt, column_index, column_names[i], column_data_types[i],
                        column_sizes[i], column_decimal_digits[i], column_nullable[i]);
  }
}

void CheckSQLDescribeColODBC3(SQLHSTMT stmt) {
  SQLWCHAR* column_names[] = {
      (SQLWCHAR*)L"TYPE_NAME",         (SQLWCHAR*)L"DATA_TYPE",
      (SQLWCHAR*)L"COLUMN_SIZE",       (SQLWCHAR*)L"LITERAL_PREFIX",
      (SQLWCHAR*)L"LITERAL_SUFFIX",    (SQLWCHAR*)L"CREATE_PARAMS",
      (SQLWCHAR*)L"NULLABLE",          (SQLWCHAR*)L"CASE_SENSITIVE",
      (SQLWCHAR*)L"SEARCHABLE",        (SQLWCHAR*)L"UNSIGNED_ATTRIBUTE",
      (SQLWCHAR*)L"FIXED_PREC_SCALE",  (SQLWCHAR*)L"AUTO_UNIQUE_VALUE",
      (SQLWCHAR*)L"LOCAL_TYPE_NAME",   (SQLWCHAR*)L"MINIMUM_SCALE",
      (SQLWCHAR*)L"MAXIMUM_SCALE",     (SQLWCHAR*)L"SQL_DATA_TYPE",
      (SQLWCHAR*)L"SQL_DATETIME_SUB",  (SQLWCHAR*)L"NUM_PREC_RADIX",
      (SQLWCHAR*)L"INTERVAL_PRECISION"};
  SQLSMALLINT column_data_types[] = {
      SQL_WVARCHAR, SQL_SMALLINT, SQL_INTEGER,  SQL_WVARCHAR, SQL_WVARCHAR,
      SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT, SQL_SMALLINT,
      SQL_SMALLINT, SQL_SMALLINT, SQL_WVARCHAR, SQL_SMALLINT, SQL_SMALLINT,
      SQL_SMALLINT, SQL_SMALLINT, SQL_INTEGER,  SQL_SMALLINT};
  SQLULEN column_sizes[] = {1024, 2, 4,    1024, 1024, 1024, 2, 2, 2, 2,
                            2,    2, 1024, 2,    2,    2,    2, 4, 2};
  SQLSMALLINT column_decimal_digits[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                         0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  SQLSMALLINT column_nullable[] = {SQL_NO_NULLS, SQL_NO_NULLS, SQL_NULLABLE, SQL_NULLABLE,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NO_NULLS, SQL_NO_NULLS,
                                   SQL_NO_NULLS, SQL_NULLABLE, SQL_NO_NULLS, SQL_NULLABLE,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NULLABLE, SQL_NO_NULLS,
                                   SQL_NULLABLE, SQL_NULLABLE, SQL_NULLABLE};

  for (size_t i = 0; i < sizeof(column_names) / sizeof(*column_names); ++i) {
    SQLUSMALLINT column_index = i + 1;
    CheckSQLDescribeCol(stmt, column_index, column_names[i], column_data_types[i],
                        column_sizes[i], column_decimal_digits[i], column_nullable[i]);
  }
}

void CheckSQLGetTypeInfo(
    SQLHSTMT stmt, const std::wstring& expected_type_name,
    const SQLSMALLINT& expected_data_type, const SQLINTEGER& expected_column_size,
    const optional<std::wstring>& expected_literal_prefix,
    const optional<std::wstring>& expected_literal_suffix,
    const optional<std::wstring>& expected_create_params,
    const SQLSMALLINT& expected_nullable, const SQLSMALLINT& expected_case_sensitive,
    const SQLSMALLINT& expected_searchable, const SQLSMALLINT& expected_unsigned_attr,
    const SQLSMALLINT& expected_fixed_prec_scale,
    const SQLSMALLINT& expected_auto_unique_value,
    const std::wstring& expected_local_type_name, const SQLSMALLINT& expected_min_scale,
    const SQLSMALLINT& expected_max_scale, const SQLSMALLINT& expected_sql_data_type,
    const SQLSMALLINT& expected_sql_datetime_sub,
    const SQLINTEGER& expected_num_prec_radix, const SQLINTEGER& expected_interval_prec) {
  CheckStringColumnW(stmt, 1, expected_type_name);   // type name
  CheckSmallIntColumn(stmt, 2, expected_data_type);  // data type
  CheckIntColumn(stmt, 3, expected_column_size);     // column size

  if (expected_literal_prefix) {  // literal prefix
    CheckStringColumnW(stmt, 4, *expected_literal_prefix);
  } else {
    CheckNullColumnW(stmt, 4);
  }

  if (expected_literal_suffix) {  // literal suffix
    CheckStringColumnW(stmt, 5, *expected_literal_suffix);
  } else {
    CheckNullColumnW(stmt, 5);
  }

  if (expected_create_params) {  // create params
    CheckStringColumnW(stmt, 6, *expected_create_params);
  } else {
    CheckNullColumnW(stmt, 6);
  }

  CheckSmallIntColumn(stmt, 7, expected_nullable);            // nullable
  CheckSmallIntColumn(stmt, 8, expected_case_sensitive);      // case sensitive
  CheckSmallIntColumn(stmt, 9, expected_searchable);          // searchable
  CheckSmallIntColumn(stmt, 10, expected_unsigned_attr);      // unsigned attr
  CheckSmallIntColumn(stmt, 11, expected_fixed_prec_scale);   // fixed prec scale
  CheckSmallIntColumn(stmt, 12, expected_auto_unique_value);  // auto unique value
  CheckStringColumnW(stmt, 13, expected_local_type_name);     // local type name
  CheckSmallIntColumn(stmt, 14, expected_min_scale);          // min scale
  CheckSmallIntColumn(stmt, 15, expected_max_scale);          // max scale
  CheckSmallIntColumn(stmt, 16, expected_sql_data_type);      // sql data type
  CheckSmallIntColumn(stmt, 17, expected_sql_datetime_sub);   // sql datetime sub
  CheckIntColumn(stmt, 18, expected_num_prec_radix);          // num prec radix
  CheckIntColumn(stmt, 19, expected_interval_prec);           // interval prec
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoAllTypes) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expected_type_name
                      SQL_BIT,               // expected_data_type
                      1,                     // expected_column_size
                      std::nullopt,          // expected_literal_prefix
                      std::nullopt,          // expected_literal_suffix
                      std::nullopt,          // expected_create_params
                      SQL_NULLABLE,          // expected_nullable
                      SQL_FALSE,             // expected_case_sensitive
                      SQL_SEARCHABLE,        // expected_searchable
                      NULL,                  // expected_unsigned_attr
                      SQL_FALSE,             // expected_fixed_prec_scale
                      NULL,                  // expected_auto_unique_value
                      std::wstring(L"bit"),  // expected_local_type_name
                      NULL,                  // expected_min_scale
                      NULL,                  // expected_max_scale
                      SQL_BIT,               // expected_sql_data_type
                      NULL,                  // expected_sql_datetime_sub
                      NULL,                  // expected_num_prec_radix
                      NULL);                 // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expected_type_name
                      SQL_TINYINT,               // expected_data_type
                      3,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"tinyint"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_TINYINT,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expected_type_name
                      SQL_BIGINT,               // expected_data_type
                      19,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"bigint"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_BIGINT,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expected_type_name
                      SQL_LONGVARBINARY,               // expected_data_type
                      65536,                           // expected_column_size
                      std::nullopt,                    // expected_literal_prefix
                      std::nullopt,                    // expected_literal_suffix
                      std::nullopt,                    // expected_create_params
                      SQL_NULLABLE,                    // expected_nullable
                      SQL_FALSE,                       // expected_case_sensitive
                      SQL_SEARCHABLE,                  // expected_searchable
                      NULL,                            // expected_unsigned_attr
                      SQL_FALSE,                       // expected_fixed_prec_scale
                      NULL,                            // expected_auto_unique_value
                      std::wstring(L"longvarbinary"),  // expected_local_type_name
                      NULL,                            // expected_min_scale
                      NULL,                            // expected_max_scale
                      SQL_LONGVARBINARY,               // expected_sql_data_type
                      NULL,                            // expected_sql_datetime_sub
                      NULL,                            // expected_num_prec_radix
                      NULL);                           // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expected_type_name
                      SQL_VARBINARY,               // expected_data_type
                      255,                         // expected_column_size
                      std::nullopt,                // expected_literal_prefix
                      std::nullopt,                // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      NULL,                        // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"varbinary"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_VARBINARY,               // expected_sql_data_type
                      NULL,                        // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expected_type_name
                      SQL_WLONGVARCHAR,         // expected_data_type
                      65536,                    // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"text"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WLONGVARCHAR,         // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expected_type_name
                      SQL_WLONGVARCHAR,              // expected_data_type
                      65536,                         // expected_column_size
                      std::wstring(L"'"),            // expected_literal_prefix
                      std::wstring(L"'"),            // expected_literal_suffix
                      std::wstring(L"length"),       // expected_create_params
                      SQL_NULLABLE,                  // expected_nullable
                      SQL_FALSE,                     // expected_case_sensitive
                      SQL_SEARCHABLE,                // expected_searchable
                      NULL,                          // expected_unsigned_attr
                      SQL_FALSE,                     // expected_fixed_prec_scale
                      NULL,                          // expected_auto_unique_value
                      std::wstring(L"longvarchar"),  // expected_local_type_name
                      NULL,                          // expected_min_scale
                      NULL,                          // expected_max_scale
                      SQL_WLONGVARCHAR,              // expected_sql_data_type
                      NULL,                          // expected_sql_datetime_sub
                      NULL,                          // expected_num_prec_radix
                      NULL);                         // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expected_type_name
                      SQL_WCHAR,                // expected_data_type
                      255,                      // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"char"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WCHAR,                // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expected_type_name
                      SQL_INTEGER,               // expected_data_type
                      9,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"integer"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_INTEGER,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expected_type_name
                      SQL_SMALLINT,               // expected_data_type
                      5,                          // expected_column_size
                      std::nullopt,               // expected_literal_prefix
                      std::nullopt,               // expected_literal_suffix
                      std::nullopt,               // expected_create_params
                      SQL_NULLABLE,               // expected_nullable
                      SQL_FALSE,                  // expected_case_sensitive
                      SQL_SEARCHABLE,             // expected_searchable
                      SQL_FALSE,                  // expected_unsigned_attr
                      SQL_FALSE,                  // expected_fixed_prec_scale
                      NULL,                       // expected_auto_unique_value
                      std::wstring(L"smallint"),  // expected_local_type_name
                      NULL,                       // expected_min_scale
                      NULL,                       // expected_max_scale
                      SQL_SMALLINT,               // expected_sql_data_type
                      NULL,                       // expected_sql_datetime_sub
                      NULL,                       // expected_num_prec_radix
                      NULL);                      // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expected_type_name
                      SQL_FLOAT,               // expected_data_type
                      7,                       // expected_column_size
                      std::nullopt,            // expected_literal_prefix
                      std::nullopt,            // expected_literal_suffix
                      std::nullopt,            // expected_create_params
                      SQL_NULLABLE,            // expected_nullable
                      SQL_FALSE,               // expected_case_sensitive
                      SQL_SEARCHABLE,          // expected_searchable
                      SQL_FALSE,               // expected_unsigned_attr
                      SQL_FALSE,               // expected_fixed_prec_scale
                      NULL,                    // expected_auto_unique_value
                      std::wstring(L"float"),  // expected_local_type_name
                      NULL,                    // expected_min_scale
                      NULL,                    // expected_max_scale
                      SQL_FLOAT,               // expected_sql_data_type
                      NULL,                    // expected_sql_datetime_sub
                      NULL,                    // expected_num_prec_radix
                      NULL);                   // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expected_type_name
                      SQL_DOUBLE,               // expected_data_type
                      15,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"double"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_DOUBLE,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expected_type_name
                      SQL_DOUBLE,                // expected_data_type
                      15,                        // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"numeric"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_DOUBLE,                // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expected_type_name
                      SQL_WVARCHAR,              // expected_data_type
                      255,                       // expected_column_size
                      std::wstring(L"'"),        // expected_literal_prefix
                      std::wstring(L"'"),        // expected_literal_suffix
                      std::wstring(L"length"),   // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"varchar"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_WVARCHAR,              // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expected_type_name
                      SQL_TYPE_DATE,          // expected_data_type
                      10,                     // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"date"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_DATE,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expected_type_name
                      SQL_TYPE_TIME,          // expected_data_type
                      8,                      // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"time"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_TIME,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expected_type_name
                      SQL_TYPE_TIMESTAMP,          // expected_data_type
                      32,                          // expected_column_size
                      std::wstring(L"'"),          // expected_literal_prefix
                      std::wstring(L"'"),          // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      SQL_FALSE,                   // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"timestamp"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_DATETIME,                // expected_sql_data_type
                      SQL_CODE_TIMESTAMP,          // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoAllTypesODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expected_type_name
                      SQL_BIT,               // expected_data_type
                      1,                     // expected_column_size
                      std::nullopt,          // expected_literal_prefix
                      std::nullopt,          // expected_literal_suffix
                      std::nullopt,          // expected_create_params
                      SQL_NULLABLE,          // expected_nullable
                      SQL_FALSE,             // expected_case_sensitive
                      SQL_SEARCHABLE,        // expected_searchable
                      NULL,                  // expected_unsigned_attr
                      SQL_FALSE,             // expected_fixed_prec_scale
                      NULL,                  // expected_auto_unique_value
                      std::wstring(L"bit"),  // expected_local_type_name
                      NULL,                  // expected_min_scale
                      NULL,                  // expected_max_scale
                      SQL_BIT,               // expected_sql_data_type
                      NULL,                  // expected_sql_datetime_sub
                      NULL,                  // expected_num_prec_radix
                      NULL);                 // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expected_type_name
                      SQL_TINYINT,               // expected_data_type
                      3,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"tinyint"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_TINYINT,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expected_type_name
                      SQL_BIGINT,               // expected_data_type
                      19,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"bigint"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_BIGINT,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expected_type_name
                      SQL_LONGVARBINARY,               // expected_data_type
                      65536,                           // expected_column_size
                      std::nullopt,                    // expected_literal_prefix
                      std::nullopt,                    // expected_literal_suffix
                      std::nullopt,                    // expected_create_params
                      SQL_NULLABLE,                    // expected_nullable
                      SQL_FALSE,                       // expected_case_sensitive
                      SQL_SEARCHABLE,                  // expected_searchable
                      NULL,                            // expected_unsigned_attr
                      SQL_FALSE,                       // expected_fixed_prec_scale
                      NULL,                            // expected_auto_unique_value
                      std::wstring(L"longvarbinary"),  // expected_local_type_name
                      NULL,                            // expected_min_scale
                      NULL,                            // expected_max_scale
                      SQL_LONGVARBINARY,               // expected_sql_data_type
                      NULL,                            // expected_sql_datetime_sub
                      NULL,                            // expected_num_prec_radix
                      NULL);                           // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expected_type_name
                      SQL_VARBINARY,               // expected_data_type
                      255,                         // expected_column_size
                      std::nullopt,                // expected_literal_prefix
                      std::nullopt,                // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      NULL,                        // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"varbinary"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_VARBINARY,               // expected_sql_data_type
                      NULL,                        // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expected_type_name
                      SQL_WLONGVARCHAR,         // expected_data_type
                      65536,                    // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"text"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WLONGVARCHAR,         // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expected_type_name
                      SQL_WLONGVARCHAR,              // expected_data_type
                      65536,                         // expected_column_size
                      std::wstring(L"'"),            // expected_literal_prefix
                      std::wstring(L"'"),            // expected_literal_suffix
                      std::wstring(L"length"),       // expected_create_params
                      SQL_NULLABLE,                  // expected_nullable
                      SQL_FALSE,                     // expected_case_sensitive
                      SQL_SEARCHABLE,                // expected_searchable
                      NULL,                          // expected_unsigned_attr
                      SQL_FALSE,                     // expected_fixed_prec_scale
                      NULL,                          // expected_auto_unique_value
                      std::wstring(L"longvarchar"),  // expected_local_type_name
                      NULL,                          // expected_min_scale
                      NULL,                          // expected_max_scale
                      SQL_WLONGVARCHAR,              // expected_sql_data_type
                      NULL,                          // expected_sql_datetime_sub
                      NULL,                          // expected_num_prec_radix
                      NULL);                         // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expected_type_name
                      SQL_WCHAR,                // expected_data_type
                      255,                      // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"char"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WCHAR,                // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expected_type_name
                      SQL_INTEGER,               // expected_data_type
                      9,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"integer"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_INTEGER,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expected_type_name
                      SQL_SMALLINT,               // expected_data_type
                      5,                          // expected_column_size
                      std::nullopt,               // expected_literal_prefix
                      std::nullopt,               // expected_literal_suffix
                      std::nullopt,               // expected_create_params
                      SQL_NULLABLE,               // expected_nullable
                      SQL_FALSE,                  // expected_case_sensitive
                      SQL_SEARCHABLE,             // expected_searchable
                      SQL_FALSE,                  // expected_unsigned_attr
                      SQL_FALSE,                  // expected_fixed_prec_scale
                      NULL,                       // expected_auto_unique_value
                      std::wstring(L"smallint"),  // expected_local_type_name
                      NULL,                       // expected_min_scale
                      NULL,                       // expected_max_scale
                      SQL_SMALLINT,               // expected_sql_data_type
                      NULL,                       // expected_sql_datetime_sub
                      NULL,                       // expected_num_prec_radix
                      NULL);                      // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expected_type_name
                      SQL_FLOAT,               // expected_data_type
                      7,                       // expected_column_size
                      std::nullopt,            // expected_literal_prefix
                      std::nullopt,            // expected_literal_suffix
                      std::nullopt,            // expected_create_params
                      SQL_NULLABLE,            // expected_nullable
                      SQL_FALSE,               // expected_case_sensitive
                      SQL_SEARCHABLE,          // expected_searchable
                      SQL_FALSE,               // expected_unsigned_attr
                      SQL_FALSE,               // expected_fixed_prec_scale
                      NULL,                    // expected_auto_unique_value
                      std::wstring(L"float"),  // expected_local_type_name
                      NULL,                    // expected_min_scale
                      NULL,                    // expected_max_scale
                      SQL_FLOAT,               // expected_sql_data_type
                      NULL,                    // expected_sql_datetime_sub
                      NULL,                    // expected_num_prec_radix
                      NULL);                   // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expected_type_name
                      SQL_DOUBLE,               // expected_data_type
                      15,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"double"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_DOUBLE,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expected_type_name
                      SQL_DOUBLE,                // expected_data_type
                      15,                        // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"numeric"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_DOUBLE,                // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expected_type_name
                      SQL_WVARCHAR,              // expected_data_type
                      255,                       // expected_column_size
                      std::wstring(L"'"),        // expected_literal_prefix
                      std::wstring(L"'"),        // expected_literal_suffix
                      std::wstring(L"length"),   // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"varchar"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_WVARCHAR,              // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expected_type_name
                      SQL_DATE,               // expected_data_type
                      10,                     // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"date"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expected_type_name
                      SQL_TIME,               // expected_data_type
                      8,                      // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"time"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expected_type_name
                      SQL_TIMESTAMP,               // expected_data_type
                      32,                          // expected_column_size
                      std::wstring(L"'"),          // expected_literal_prefix
                      std::wstring(L"'"),          // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      SQL_FALSE,                   // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"timestamp"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_DATETIME,                // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoBit) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_BIT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bit data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bit"),  // expected_type_name
                      SQL_BIT,               // expected_data_type
                      1,                     // expected_column_size
                      std::nullopt,          // expected_literal_prefix
                      std::nullopt,          // expected_literal_suffix
                      std::nullopt,          // expected_create_params
                      SQL_NULLABLE,          // expected_nullable
                      SQL_FALSE,             // expected_case_sensitive
                      SQL_SEARCHABLE,        // expected_searchable
                      NULL,                  // expected_unsigned_attr
                      SQL_FALSE,             // expected_fixed_prec_scale
                      NULL,                  // expected_auto_unique_value
                      std::wstring(L"bit"),  // expected_local_type_name
                      NULL,                  // expected_min_scale
                      NULL,                  // expected_max_scale
                      SQL_BIT,               // expected_sql_data_type
                      NULL,                  // expected_sql_datetime_sub
                      NULL,                  // expected_num_prec_radix
                      NULL);                 // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoTinyInt) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TINYINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check tinyint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"tinyint"),  // expected_type_name
                      SQL_TINYINT,               // expected_data_type
                      3,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"tinyint"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_TINYINT,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoBigInt) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_BIGINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check bigint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"bigint"),  // expected_type_name
                      SQL_BIGINT,               // expected_data_type
                      19,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"bigint"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_BIGINT,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoLongVarbinary) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_LONGVARBINARY);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check longvarbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarbinary"),  // expected_type_name
                      SQL_LONGVARBINARY,               // expected_data_type
                      65536,                           // expected_column_size
                      std::nullopt,                    // expected_literal_prefix
                      std::nullopt,                    // expected_literal_suffix
                      std::nullopt,                    // expected_create_params
                      SQL_NULLABLE,                    // expected_nullable
                      SQL_FALSE,                       // expected_case_sensitive
                      SQL_SEARCHABLE,                  // expected_searchable
                      NULL,                            // expected_unsigned_attr
                      SQL_FALSE,                       // expected_fixed_prec_scale
                      NULL,                            // expected_auto_unique_value
                      std::wstring(L"longvarbinary"),  // expected_local_type_name
                      NULL,                            // expected_min_scale
                      NULL,                            // expected_max_scale
                      SQL_LONGVARBINARY,               // expected_sql_data_type
                      NULL,                            // expected_sql_datetime_sub
                      NULL,                            // expected_num_prec_radix
                      NULL);                           // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoVarbinary) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_VARBINARY);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check varbinary data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varbinary"),  // expected_type_name
                      SQL_VARBINARY,               // expected_data_type
                      255,                         // expected_column_size
                      std::nullopt,                // expected_literal_prefix
                      std::nullopt,                // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      NULL,                        // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"varbinary"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_VARBINARY,               // expected_sql_data_type
                      NULL,                        // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoLongVarchar) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WLONGVARCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check text data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WLONGVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"text"),    // expected_type_name
                      SQL_WLONGVARCHAR,         // expected_data_type
                      65536,                    // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"text"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WLONGVARCHAR,         // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check longvarchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"longvarchar"),  // expected_type_name
                      SQL_WLONGVARCHAR,              // expected_data_type
                      65536,                         // expected_column_size
                      std::wstring(L"'"),            // expected_literal_prefix
                      std::wstring(L"'"),            // expected_literal_suffix
                      std::wstring(L"length"),       // expected_create_params
                      SQL_NULLABLE,                  // expected_nullable
                      SQL_FALSE,                     // expected_case_sensitive
                      SQL_SEARCHABLE,                // expected_searchable
                      NULL,                          // expected_unsigned_attr
                      SQL_FALSE,                     // expected_fixed_prec_scale
                      NULL,                          // expected_auto_unique_value
                      std::wstring(L"longvarchar"),  // expected_local_type_name
                      NULL,                          // expected_min_scale
                      NULL,                          // expected_max_scale
                      SQL_WLONGVARCHAR,              // expected_sql_data_type
                      NULL,                          // expected_sql_datetime_sub
                      NULL,                          // expected_num_prec_radix
                      NULL);                         // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoChar) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check char data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"char"),    // expected_type_name
                      SQL_WCHAR,                // expected_data_type
                      255,                      // expected_column_size
                      std::wstring(L"'"),       // expected_literal_prefix
                      std::wstring(L"'"),       // expected_literal_suffix
                      std::wstring(L"length"),  // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      NULL,                     // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"char"),    // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_WCHAR,                // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoInteger) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_INTEGER);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check integer data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"integer"),  // expected_type_name
                      SQL_INTEGER,               // expected_data_type
                      9,                         // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"integer"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_INTEGER,               // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSmallInt) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_SMALLINT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check smallint data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"smallint"),  // expected_type_name
                      SQL_SMALLINT,               // expected_data_type
                      5,                          // expected_column_size
                      std::nullopt,               // expected_literal_prefix
                      std::nullopt,               // expected_literal_suffix
                      std::nullopt,               // expected_create_params
                      SQL_NULLABLE,               // expected_nullable
                      SQL_FALSE,                  // expected_case_sensitive
                      SQL_SEARCHABLE,             // expected_searchable
                      SQL_FALSE,                  // expected_unsigned_attr
                      SQL_FALSE,                  // expected_fixed_prec_scale
                      NULL,                       // expected_auto_unique_value
                      std::wstring(L"smallint"),  // expected_local_type_name
                      NULL,                       // expected_min_scale
                      NULL,                       // expected_max_scale
                      SQL_SMALLINT,               // expected_sql_data_type
                      NULL,                       // expected_sql_datetime_sub
                      NULL,                       // expected_num_prec_radix
                      NULL);                      // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoFloat) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_FLOAT);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check float data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"float"),  // expected_type_name
                      SQL_FLOAT,               // expected_data_type
                      7,                       // expected_column_size
                      std::nullopt,            // expected_literal_prefix
                      std::nullopt,            // expected_literal_suffix
                      std::nullopt,            // expected_create_params
                      SQL_NULLABLE,            // expected_nullable
                      SQL_FALSE,               // expected_case_sensitive
                      SQL_SEARCHABLE,          // expected_searchable
                      SQL_FALSE,               // expected_unsigned_attr
                      SQL_FALSE,               // expected_fixed_prec_scale
                      NULL,                    // expected_auto_unique_value
                      std::wstring(L"float"),  // expected_local_type_name
                      NULL,                    // expected_min_scale
                      NULL,                    // expected_max_scale
                      SQL_FLOAT,               // expected_sql_data_type
                      NULL,                    // expected_sql_datetime_sub
                      NULL,                    // expected_num_prec_radix
                      NULL);                   // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoDouble) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DOUBLE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check double data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"double"),  // expected_type_name
                      SQL_DOUBLE,               // expected_data_type
                      15,                       // expected_column_size
                      std::nullopt,             // expected_literal_prefix
                      std::nullopt,             // expected_literal_suffix
                      std::nullopt,             // expected_create_params
                      SQL_NULLABLE,             // expected_nullable
                      SQL_FALSE,                // expected_case_sensitive
                      SQL_SEARCHABLE,           // expected_searchable
                      SQL_FALSE,                // expected_unsigned_attr
                      SQL_FALSE,                // expected_fixed_prec_scale
                      NULL,                     // expected_auto_unique_value
                      std::wstring(L"double"),  // expected_local_type_name
                      NULL,                     // expected_min_scale
                      NULL,                     // expected_max_scale
                      SQL_DOUBLE,               // expected_sql_data_type
                      NULL,                     // expected_sql_datetime_sub
                      NULL,                     // expected_num_prec_radix
                      NULL);                    // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // Check numeric data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Mock server treats numeric data type as a double type
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"numeric"),  // expected_type_name
                      SQL_DOUBLE,                // expected_data_type
                      15,                        // expected_column_size
                      std::nullopt,              // expected_literal_prefix
                      std::nullopt,              // expected_literal_suffix
                      std::nullopt,              // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"numeric"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_DOUBLE,                // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoVarchar) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_WVARCHAR);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check varchar data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver returns SQL_WVARCHAR since unicode is enabled
  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"varchar"),  // expected_type_name
                      SQL_WVARCHAR,              // expected_data_type
                      255,                       // expected_column_size
                      std::wstring(L"'"),        // expected_literal_prefix
                      std::wstring(L"'"),        // expected_literal_suffix
                      std::wstring(L"length"),   // expected_create_params
                      SQL_NULLABLE,              // expected_nullable
                      SQL_FALSE,                 // expected_case_sensitive
                      SQL_SEARCHABLE,            // expected_searchable
                      SQL_FALSE,                 // expected_unsigned_attr
                      SQL_FALSE,                 // expected_fixed_prec_scale
                      NULL,                      // expected_auto_unique_value
                      std::wstring(L"varchar"),  // expected_local_type_name
                      NULL,                      // expected_min_scale
                      NULL,                      // expected_max_scale
                      SQL_WVARCHAR,              // expected_sql_data_type
                      NULL,                      // expected_sql_datetime_sub
                      NULL,                      // expected_num_prec_radix
                      NULL);                     // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeDate) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expected_type_name
                      SQL_TYPE_DATE,          // expected_data_type
                      10,                     // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"date"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_DATE,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLDate) {
  this->Connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expected_type_name
                      SQL_TYPE_DATE,          // expected_data_type
                      10,                     // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"date"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_DATE,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoDateODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_DATE);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check date data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"date"),  // expected_type_name
                      SQL_DATE,               // expected_data_type
                      10,                     // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"date"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeDateODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTime) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expected_type_name
                      SQL_TYPE_TIME,          // expected_data_type
                      8,                      // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"time"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_TIME,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTime) {
  this->Connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expected_type_name
                      SQL_TYPE_TIME,          // expected_data_type
                      8,                      // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"time"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      SQL_CODE_TIME,          // expected_sql_datetime_sub
                      NULL,                   // expected_num_prec_radix
                      NULL);                  // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoTimeODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIME);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check time data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"time"),  // expected_type_name
                      SQL_TIME,               // expected_data_type
                      8,                      // expected_column_size
                      std::wstring(L"'"),     // expected_literal_prefix
                      std::wstring(L"'"),     // expected_literal_suffix
                      std::nullopt,           // expected_create_params
                      SQL_NULLABLE,           // expected_nullable
                      SQL_FALSE,              // expected_case_sensitive
                      SQL_SEARCHABLE,         // expected_searchable
                      SQL_FALSE,              // expected_unsigned_attr
                      SQL_FALSE,              // expected_fixed_prec_scale
                      NULL,                   // expected_auto_unique_value
                      std::wstring(L"time"),  // expected_local_type_name
                      NULL,                   // expected_min_scale
                      NULL,                   // expected_max_scale
                      SQL_DATETIME,           // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimeODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimestamp) {
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expected_type_name
                      SQL_TYPE_TIMESTAMP,          // expected_data_type
                      32,                          // expected_column_size
                      std::wstring(L"'"),          // expected_literal_prefix
                      std::wstring(L"'"),          // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      SQL_FALSE,                   // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"timestamp"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_DATETIME,                // expected_sql_data_type
                      SQL_CODE_TIMESTAMP,          // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTimestamp) {
  this->Connect();

  // Pass ODBC Ver 2 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expected_type_name
                      SQL_TYPE_TIMESTAMP,          // expected_data_type
                      32,                          // expected_column_size
                      std::wstring(L"'"),          // expected_literal_prefix
                      std::wstring(L"'"),          // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      SQL_FALSE,                   // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"timestamp"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_DATETIME,                // expected_sql_data_type
                      SQL_CODE_TIMESTAMP,          // expected_sql_datetime_sub
                      NULL,                        // expected_num_prec_radix
                      NULL);                       // expected_interval_prec

  CheckSQLDescribeColODBC3(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTimestampODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check timestamp data type
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  CheckSQLGetTypeInfo(this->stmt,
                      std::wstring(L"timestamp"),  // expected_type_name
                      SQL_TIMESTAMP,               // expected_data_type
                      32,                          // expected_column_size
                      std::wstring(L"'"),          // expected_literal_prefix
                      std::wstring(L"'"),          // expected_literal_suffix
                      std::nullopt,                // expected_create_params
                      SQL_NULLABLE,                // expected_nullable
                      SQL_FALSE,                   // expected_case_sensitive
                      SQL_SEARCHABLE,              // expected_searchable
                      SQL_FALSE,                   // expected_unsigned_attr
                      SQL_FALSE,                   // expected_fixed_prec_scale
                      NULL,                        // expected_auto_unique_value
                      std::wstring(L"timestamp"),  // expected_local_type_name
                      NULL,                        // expected_min_scale
                      NULL,                        // expected_max_scale
                      SQL_DATETIME,                // expected_sql_data_type
                      NULL,   // expected_sql_datetime_sub, driver returns NULL for Ver2
                      NULL,   // expected_num_prec_radix
                      NULL);  // expected_interval_prec

  CheckSQLDescribeColODBC2(this->stmt);

  // No more data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoSQLTypeTimestampODBCVer2) {
  this->Connect(SQL_OV_ODBC2);

  // Pass ODBC Ver 3 data type
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP);

  EXPECT_EQ(ret, SQL_ERROR);

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_S1004);

  this->Disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLGetTypeInfoInvalidDataType) {
  this->Connect();

  SQLSMALLINT invalid_data_type = -114;
  SQLRETURN ret = SQLGetTypeInfo(this->stmt, invalid_data_type);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY004);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetTypeInfoUnsupportedDataType) {
  // Assumes mock and remote server don't support GUID data type
  this->Connect();

  SQLRETURN ret = SQLGetTypeInfo(this->stmt, SQL_GUID);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Result set is empty with valid data type that is unsupported by the server
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->Disconnect();
}

}  // namespace arrow::flight::sql::odbc
