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
class TablesTest : public T {};

class TablesMockTest : public FlightSQLODBCMockTestBase {};
class TablesRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<TablesMockTest, TablesRemoteTest>;
TYPED_TEST_SUITE(TablesTest, TestTypes);

template <typename T>
class TablesOdbcV2Test : public T {};

using TestTypesOdbcV2 =
    ::testing::Types<FlightSQLOdbcV2MockTestBase, FlightSQLOdbcV2RemoteTestBase>;
TYPED_TEST_SUITE(TablesOdbcV2Test, TestTypesOdbcV2);

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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TABLE_CAT"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_SCHEM"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"REMARKS")};
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

  const SQLWCHAR* column_names[] = {static_cast<const SQLWCHAR*>(L"TABLE_QUALIFIER"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_OWNER"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_NAME"),
                                    static_cast<const SQLWCHAR*>(L"TABLE_TYPE"),
                                    static_cast<const SQLWCHAR*>(L"REMARKS")};
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
