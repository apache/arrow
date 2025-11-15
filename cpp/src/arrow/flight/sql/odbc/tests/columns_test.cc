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
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState07009);

  // Invalid descriptor index - index out of range
  EXPECT_EQ(SQL_ERROR, SQLDescribeCol(this->stmt, out_of_range_column, column_name,
                                      buf_char_len, &name_length, &data_type,
                                      &column_size, &decimal_digits, &nullable));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState07009);

  // Invalid descriptor index - index out of range
  EXPECT_EQ(SQL_ERROR, SQLDescribeCol(this->stmt, negative_column, column_name,
                                      buf_char_len, &name_length, &data_type,
                                      &column_size, &decimal_digits, &nullable));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState07009);
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"stiny_int_min"),
                                    static_cast<const SQLWCHAR*>(L"stiny_int_max"),
                                    static_cast<const SQLWCHAR*>(L"utiny_int_min"),
                                    static_cast<const SQLWCHAR*>(L"utiny_int_max"),
                                    static_cast<const SQLWCHAR*>(L"ssmall_int_min"),
                                    static_cast<const SQLWCHAR*>(L"ssmall_int_max"),
                                    static_cast<const SQLWCHAR*>(L"usmall_int_min"),
                                    static_cast<const SQLWCHAR*>(L"usmall_int_max"),
                                    static_cast<const SQLWCHAR*>(L"sinteger_min"),
                                    static_cast<const SQLWCHAR*>(L"sinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"uinteger_min"),
                                    static_cast<const SQLWCHAR*>(L"uinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_min"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_max"),
                                    static_cast<const SQLWCHAR*>(L"ubigint_min"),
                                    static_cast<const SQLWCHAR*>(L"ubigint_max"),
                                    static_cast<const SQLWCHAR*>(L"decimal_negative"),
                                    static_cast<const SQLWCHAR*>(L"decimal_positive"),
                                    static_cast<const SQLWCHAR*>(L"float_min"),
                                    static_cast<const SQLWCHAR*>(L"float_max"),
                                    static_cast<const SQLWCHAR*>(L"double_min"),
                                    static_cast<const SQLWCHAR*>(L"double_max"),
                                    static_cast<const SQLWCHAR*>(L"bit_false"),
                                    static_cast<const SQLWCHAR*>(L"bit_true"),
                                    static_cast<const SQLWCHAR*>(L"c_char"),
                                    static_cast<const SQLWCHAR*>(L"c_wchar"),
                                    static_cast<const SQLWCHAR*>(L"c_wvarchar"),
                                    static_cast<const SQLWCHAR*>(L"c_varchar"),
                                    static_cast<const SQLWCHAR*>(L"date_min"),
                                    static_cast<const SQLWCHAR*>(L"date_max"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_min"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_max")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"stiny_int_min"),
                                    static_cast<const SQLWCHAR*>(L"stiny_int_max"),
                                    static_cast<const SQLWCHAR*>(L"utiny_int_min"),
                                    static_cast<const SQLWCHAR*>(L"utiny_int_max"),
                                    static_cast<const SQLWCHAR*>(L"ssmall_int_min"),
                                    static_cast<const SQLWCHAR*>(L"ssmall_int_max"),
                                    static_cast<const SQLWCHAR*>(L"usmall_int_min"),
                                    static_cast<const SQLWCHAR*>(L"usmall_int_max"),
                                    static_cast<const SQLWCHAR*>(L"sinteger_min"),
                                    static_cast<const SQLWCHAR*>(L"sinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"uinteger_min"),
                                    static_cast<const SQLWCHAR*>(L"uinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_min"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_max"),
                                    static_cast<const SQLWCHAR*>(L"ubigint_min"),
                                    static_cast<const SQLWCHAR*>(L"ubigint_max"),
                                    static_cast<const SQLWCHAR*>(L"decimal_negative"),
                                    static_cast<const SQLWCHAR*>(L"decimal_positive"),
                                    static_cast<const SQLWCHAR*>(L"float_min"),
                                    static_cast<const SQLWCHAR*>(L"float_max"),
                                    static_cast<const SQLWCHAR*>(L"double_min"),
                                    static_cast<const SQLWCHAR*>(L"double_max"),
                                    static_cast<const SQLWCHAR*>(L"bit_false"),
                                    static_cast<const SQLWCHAR*>(L"bit_true"),
                                    static_cast<const SQLWCHAR*>(L"c_char"),
                                    static_cast<const SQLWCHAR*>(L"c_wchar"),
                                    static_cast<const SQLWCHAR*>(L"c_wvarchar"),
                                    static_cast<const SQLWCHAR*>(L"c_varchar"),
                                    static_cast<const SQLWCHAR*>(L"date_min"),
                                    static_cast<const SQLWCHAR*>(L"date_max"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_min"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_max")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"sinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_max"),
                                    static_cast<const SQLWCHAR*>(L"decimal_positive"),
                                    static_cast<const SQLWCHAR*>(L"float_max"),
                                    static_cast<const SQLWCHAR*>(L"double_max"),
                                    static_cast<const SQLWCHAR*>(L"bit_true"),
                                    static_cast<const SQLWCHAR*>(L"date_max"),
                                    static_cast<const SQLWCHAR*>(L"time_max"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_max")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"sinteger_max"),
                                    static_cast<const SQLWCHAR*>(L"sbigint_max"),
                                    static_cast<const SQLWCHAR*>(L"decimal_positive"),
                                    static_cast<const SQLWCHAR*>(L"float_max"),
                                    static_cast<const SQLWCHAR*>(L"double_max"),
                                    static_cast<const SQLWCHAR*>(L"bit_true"),
                                    static_cast<const SQLWCHAR*>(L"date_max"),
                                    static_cast<const SQLWCHAR*>(L"time_max"),
                                    static_cast<const SQLWCHAR*>(L"timestamp_max")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"bigint_col"),
                                    static_cast<const SQLWCHAR*>(L"char_col"),
                                    static_cast<const SQLWCHAR*>(L"varbinary_col"),
                                    static_cast<const SQLWCHAR*>(L"double_col")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TABLE_CAT"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_SCHEM"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_NAME"),
                                    static_cast<const SQLWCHAR*>(L"DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_SIZE"),
                                    static_cast<const SQLWCHAR*>(L"BUFFER_LENGTH"),
                                    static_cast<const SQLWCHAR*>(L"DECIMAL_DIGITS"),
                                    static_cast<const SQLWCHAR*>(L"NUM_PREC_RADIX"),
                                    static_cast<const SQLWCHAR*>(L"NULLABLE"),
                                    static_cast<const SQLWCHAR*>(L"REMARKS"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_DEF"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATETIME_SUB"),
                                    static_cast<const SQLWCHAR*>(L"CHAR_OCTET_LENGTH"),
                                    static_cast<const SQLWCHAR*>(L"ORDINAL_POSITION"),
                                    static_cast<const SQLWCHAR*>(L"IS_NULLABLE")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TABLE_QUALIFIER"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_OWNER"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_NAME"),
                                    static_cast<const SQLWCHAR*>(L"DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"TYPE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"PRECISION"),
                                    static_cast<const SQLWCHAR*>(L"LENGTH"),
                                    static_cast<const SQLWCHAR*>(L"SCALE"),
                                    static_cast<const SQLWCHAR*>(L"RADIX"),
                                    static_cast<const SQLWCHAR*>(L"NULLABLE"),
                                    static_cast<const SQLWCHAR*>(L"REMARKS"),
                                    static_cast<const SQLWCHAR*>(L"COLUMN_DEF"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATA_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"SQL_DATETIME_SUB"),
                                    static_cast<const SQLWCHAR*>(L"CHAR_OCTET_LENGTH"),
                                    static_cast<const SQLWCHAR*>(L"ORDINAL_POSITION"),
                                    static_cast<const SQLWCHAR*>(L"IS_NULLABLE")};
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
