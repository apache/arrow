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

using std::optional;

template <typename T>
class TypeInfoTest : public T {};

class TypeInfoMockTest : public FlightSQLODBCMockTestBase {};
using TestTypes = ::testing::Types<TypeInfoMockTest, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(TypeInfoTest, TestTypes);

class TypeInfoOdbcV2MockTest : public FlightSQLOdbcV2MockTestBase {};

namespace {
// Helper Functions

void CheckSQLDescribeCol(SQLHSTMT stmt, const SQLUSMALLINT column_index,
                         const std::wstring& expected_name,
                         const SQLSMALLINT& expected_data_type,
                         const SQLULEN& expected_column_size,
                         const SQLSMALLINT& expected_decimal_digits,
                         const SQLSMALLINT& expected_nullable) {
  SQLWCHAR column_name[1024];
  SQLSMALLINT buf_char_len =
      static_cast<SQLSMALLINT>(sizeof(column_name) / GetSqlWCharSize());
  SQLSMALLINT name_length = 0;
  SQLSMALLINT column_data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLDescribeCol(stmt, column_index, column_name, buf_char_len, &name_length,
                           &column_data_type, &column_size, &decimal_digits, &nullable));

  EXPECT_EQ(name_length, expected_name.size());

  std::wstring returned(column_name, column_name + name_length);
  EXPECT_EQ(expected_name, returned);
  EXPECT_EQ(expected_data_type, column_data_type);
  EXPECT_EQ(expected_column_size, column_size);
  EXPECT_EQ(expected_decimal_digits, decimal_digits);
  EXPECT_EQ(expected_nullable, nullable);
}

void CheckSQLDescribeColODBC2(SQLHSTMT stmt) {
  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"PRECISION"),
                                    static_cast<const SQLWCHAR*>(L"LITERAL_PREFIX"),
                                    static_cast<const SQLWCHAR*>(L"LITERAL_SUFFIX"),
                                    static_cast<const SQLWCHAR*>(L"CREATE_PARAMS"),
                                    static_cast<const SQLWCHAR*>(L"NULLABLE"),
                                    static_cast<const SQLWCHAR*>(L"CASE_SENSITIVE"),
                                    static_cast<const SQLWCHAR*>(L"SEARCHABLE"),
                                    static_cast<const SQLWCHAR*>(L"UNSIGNED_ATTRIBUTE"),
                                    static_cast<const SQLWCHAR*>(L"MONEY"),
                                    static_cast<const SQLWCHAR*>(L"AUTO_INCREMENT"),
                                    static_cast<const SQLWCHAR*>(L"LOCAL_TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"MINIMUM_SCALE"),
                                    static_cast<const SQLWCHAR*>(L"MAXIMUM_SCALE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATETIME_SUB"),
                                    static_cast<const SQLWCHAR*>(L"NUM_PREC_RADIX"),
                                    static_cast<const SQLWCHAR*>(L"INTERVAL_PRECISION")};
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
  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_SIZE"),
                                    static_cast<const SQLWCHAR*>(L"LITERAL_PREFIX"),
                                    static_cast<const SQLWCHAR*>(L"LITERAL_SUFFIX"),
                                    static_cast<const SQLWCHAR*>(L"CREATE_PARAMS"),
                                    static_cast<const SQLWCHAR*>(L"NULLABLE"),
                                    static_cast<const SQLWCHAR*>(L"CASE_SENSITIVE"),
                                    static_cast<const SQLWCHAR*>(L"SEARCHABLE"),
                                    static_cast<const SQLWCHAR*>(L"UNSIGNED_ATTRIBUTE"),
                                    static_cast<const SQLWCHAR*>(L"FIXED_PREC_SCALE"),
                                    static_cast<const SQLWCHAR*>(L"AUTO_UNIQUE_VALUE"),
                                    static_cast<const SQLWCHAR*>(L"LOCAL_TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"MINIMUM_SCALE"),
                                    static_cast<const SQLWCHAR*>(L"MAXIMUM_SCALE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATETIME_SUB"),
                                    static_cast<const SQLWCHAR*>(L"NUM_PREC_RADIX"),
                                    static_cast<const SQLWCHAR*>(L"INTERVAL_PRECISION")};
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
}  // namespace

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoAllTypes) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES));

  // Check bit data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoAllTypes) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_ALL_TYPES));

  // Check bit data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoBit) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_BIT));

  // Check bit data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoTinyInt) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TINYINT));

  // Check tinyint data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoBigInt) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_BIGINT));

  // Check bigint data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoLongVarbinary) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_LONGVARBINARY));

  // Check longvarbinary data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoVarbinary) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_VARBINARY));

  // Check varbinary data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoLongVarchar) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_WLONGVARCHAR));

  // Check text data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoChar) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_WCHAR));

  // Check char data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoInteger) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_INTEGER));

  // Check integer data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSmallInt) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_SMALLINT));

  // Check smallint data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoFloat) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_FLOAT));

  // Check float data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoDouble) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_DOUBLE));

  // Check double data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoVarchar) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_WVARCHAR));

  // Check varchar data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLTypeDate) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE));

  // Check date data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLDate) {
  // Pass ODBC Ver 2 data type
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_DATE));

  // Check date data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoDate) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_DATE));

  // Check date data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoSQLTypeDate) {
  // Pass ODBC Ver 3 data type
  ASSERT_EQ(SQL_ERROR, SQLGetTypeInfo(this->stmt, SQL_TYPE_DATE));

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1004);
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLTypeTime) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME));

  // Check time data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLTime) {
  // Pass ODBC Ver 2 data type
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TIME));

  // Check time data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoTime) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TIME));

  // Check time data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoSQLTypeTime) {
  // Pass ODBC Ver 3 data type
  ASSERT_EQ(SQL_ERROR, SQLGetTypeInfo(this->stmt, SQL_TYPE_TIME));

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1004);
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLTypeTimestamp) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP));

  // Check timestamp data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoSQLTimestamp) {
  // Pass ODBC Ver 2 data type
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP));

  // Check timestamp data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoSQLTimestamp) {
  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_TIMESTAMP));

  // Check timestamp data type
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

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
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TEST_F(TypeInfoOdbcV2MockTest, TestSQLGetTypeInfoSQLTypeTimestamp) {
  // Pass ODBC Ver 3 data type
  ASSERT_EQ(SQL_ERROR, SQLGetTypeInfo(this->stmt, SQL_TYPE_TIMESTAMP));

  // Driver manager returns SQL data type out of range error state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1004);
}

TEST_F(TypeInfoMockTest, TestSQLGetTypeInfoInvalidDataType) {
  SQLSMALLINT invalid_data_type = -114;
  ASSERT_EQ(SQL_ERROR, SQLGetTypeInfo(this->stmt, invalid_data_type));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY004);
}

TYPED_TEST(TypeInfoTest, TestSQLGetTypeInfoUnsupportedDataType) {
  // Assumes mock and remote server don't support GUID data type

  ASSERT_EQ(SQL_SUCCESS, SQLGetTypeInfo(this->stmt, SQL_GUID));

  // Result set is empty with valid data type that is unsupported by the server
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

}  // namespace arrow::flight::sql::odbc
