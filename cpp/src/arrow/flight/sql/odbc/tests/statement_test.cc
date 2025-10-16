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

#include <limits>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

template <typename T>
class StatementTest : public T {};

class StatementMockTest : public FlightSQLODBCMockTestBase {};
class StatementRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<StatementMockTest, StatementRemoteTest>;
TYPED_TEST_SUITE(StatementTest, TestTypes);

TYPED_TEST(StatementTest, TestSQLBindColDataQuery) {
  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val_min;
  int8_t stiny_int_val_max;
  SQLLEN buf_len = 0;
  SQLLEN ind;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val_max, buf_len, &ind));

  // Unsigned Tiny Int
  uint8_t utiny_int_val_min;
  uint8_t utiny_int_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val_max, buf_len, &ind));

  // Signed Small Int
  int16_t ssmall_int_val_min;
  int16_t ssmall_int_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val_max, buf_len, &ind));

  // Unsigned Small Int
  uint16_t usmall_int_val_min;
  uint16_t usmall_int_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 7, SQL_C_USHORT, &usmall_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 8, SQL_C_USHORT, &usmall_int_val_max, buf_len, &ind));

  // Signed Integer
  SQLINTEGER slong_val_min;
  SQLINTEGER slong_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 9, SQL_C_SLONG, &slong_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 10, SQL_C_SLONG, &slong_val_max, buf_len, &ind));

  // Unsigned Integer
  SQLUINTEGER ulong_val_min;
  SQLUINTEGER ulong_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 11, SQL_C_ULONG, &ulong_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 12, SQL_C_ULONG, &ulong_val_max, buf_len, &ind));

  // Signed Big Int
  SQLBIGINT sbig_int_val_min;
  SQLBIGINT sbig_int_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val_max, buf_len, &ind));

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val_min;
  SQLUBIGINT ubig_int_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val_max, buf_len, &ind));

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val_neg;
  SQL_NUMERIC_STRUCT decimal_val_pos;
  memset(&decimal_val_neg, 0, sizeof(decimal_val_neg));
  memset(&decimal_val_pos, 0, sizeof(decimal_val_pos));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 17, SQL_C_NUMERIC, &decimal_val_neg, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 18, SQL_C_NUMERIC, &decimal_val_pos, buf_len, &ind));

  // Float
  float float_val_min;
  float float_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 19, SQL_C_FLOAT, &float_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 20, SQL_C_FLOAT, &float_val_max, buf_len, &ind));

  // Double
  SQLDOUBLE double_val_min;
  SQLDOUBLE double_val_max;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 21, SQL_C_DOUBLE, &double_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 22, SQL_C_DOUBLE, &double_val_max, buf_len, &ind));

  // Bit
  bool bit_val_false;
  bool bit_val_true;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 23, SQL_C_BIT, &bit_val_false, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 24, SQL_C_BIT, &bit_val_true, buf_len, &ind));

  // Characters
  SQLCHAR char_val[2];
  buf_len = sizeof(SQLCHAR) * 2;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &ind));

  SQLWCHAR wchar_val[2];
  size_t wchar_size = arrow::flight::sql::odbc::GetSqlWCharSize();
  buf_len = wchar_size * 2;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 26, SQL_C_WCHAR, &wchar_val, buf_len, &ind));

  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 27, SQL_C_WCHAR, &wvarchar_val, buf_len, &ind));

  SQLCHAR varchar_val[4];
  buf_len = sizeof(SQLCHAR) * 4;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 28, SQL_C_CHAR, &varchar_val, buf_len, &ind));

  // Date and Timestamp
  SQL_DATE_STRUCT date_val_min{}, date_val_max{};
  buf_len = 0;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 29, SQL_C_TYPE_DATE, &date_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 30, SQL_C_TYPE_DATE, &date_val_max, buf_len, &ind));

  SQL_TIMESTAMP_STRUCT timestamp_val_min{}, timestamp_val_max{};

  EXPECT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 31, SQL_C_TYPE_TIMESTAMP,
                                    &timestamp_val_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 32, SQL_C_TYPE_TIMESTAMP,
                                    &timestamp_val_max, buf_len, &ind));

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Data verification

  // Signed Tiny Int
  EXPECT_EQ(std::numeric_limits<int8_t>::min(), stiny_int_val_min);
  EXPECT_EQ(std::numeric_limits<int8_t>::max(), stiny_int_val_max);

  // Unsigned Tiny Int
  EXPECT_EQ(std::numeric_limits<uint8_t>::min(), utiny_int_val_min);
  EXPECT_EQ(std::numeric_limits<uint8_t>::max(), utiny_int_val_max);

  // Signed Small Int
  EXPECT_EQ(std::numeric_limits<int16_t>::min(), ssmall_int_val_min);
  EXPECT_EQ(std::numeric_limits<int16_t>::max(), ssmall_int_val_max);

  // Unsigned Small Int
  EXPECT_EQ(std::numeric_limits<uint16_t>::min(), usmall_int_val_min);
  EXPECT_EQ(std::numeric_limits<uint16_t>::max(), usmall_int_val_max);

  // Signed Long
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::min(), slong_val_min);
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::max(), slong_val_max);

  // Unsigned Long
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::min(), ulong_val_min);
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::max(), ulong_val_max);

  // Signed Big Int
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::min(), sbig_int_val_min);
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::max(), sbig_int_val_max);

  // Unsigned Big Int
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::min(), ubig_int_val_min);
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::max(), ubig_int_val_max);

  // Decimal
  EXPECT_EQ(0, decimal_val_neg.sign);
  EXPECT_EQ(0, decimal_val_neg.scale);
  EXPECT_EQ(38, decimal_val_neg.precision);
  EXPECT_THAT(decimal_val_neg.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0));

  EXPECT_EQ(1, decimal_val_pos.sign);
  EXPECT_EQ(0, decimal_val_pos.scale);
  EXPECT_EQ(38, decimal_val_pos.precision);
  EXPECT_THAT(decimal_val_pos.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  EXPECT_EQ(-std::numeric_limits<float>::max(), float_val_min);
  EXPECT_EQ(std::numeric_limits<float>::max(), float_val_max);

  // Double
  EXPECT_EQ(-std::numeric_limits<SQLDOUBLE>::max(), double_val_min);
  EXPECT_EQ(std::numeric_limits<SQLDOUBLE>::max(), double_val_max);

  // Bit
  EXPECT_EQ(false, bit_val_false);
  EXPECT_EQ(true, bit_val_true);

  // Characters
  EXPECT_EQ('Z', char_val[0]);
  EXPECT_EQ(L'你', wchar_val[0]);
  EXPECT_EQ(L'你', wvarchar_val[0]);
  EXPECT_EQ(L'好', wvarchar_val[1]);

  EXPECT_EQ('X', varchar_val[0]);
  EXPECT_EQ('Y', varchar_val[1]);
  EXPECT_EQ('Z', varchar_val[2]);

  // Date
  EXPECT_EQ(1, date_val_min.day);
  EXPECT_EQ(1, date_val_min.month);
  EXPECT_EQ(1400, date_val_min.year);

  EXPECT_EQ(31, date_val_max.day);
  EXPECT_EQ(12, date_val_max.month);
  EXPECT_EQ(9999, date_val_max.year);

  // Timestamp
  EXPECT_EQ(1, timestamp_val_min.day);
  EXPECT_EQ(1, timestamp_val_min.month);
  EXPECT_EQ(1400, timestamp_val_min.year);
  EXPECT_EQ(0, timestamp_val_min.hour);
  EXPECT_EQ(0, timestamp_val_min.minute);
  EXPECT_EQ(0, timestamp_val_min.second);
  EXPECT_EQ(0, timestamp_val_min.fraction);

  EXPECT_EQ(31, timestamp_val_max.day);
  EXPECT_EQ(12, timestamp_val_max.month);
  EXPECT_EQ(9999, timestamp_val_max.year);
  EXPECT_EQ(23, timestamp_val_max.hour);
  EXPECT_EQ(59, timestamp_val_max.minute);
  EXPECT_EQ(59, timestamp_val_max.second);
  EXPECT_EQ(0, timestamp_val_max.fraction);
}

TEST_F(StatementRemoteTest, TestSQLBindColTimeQuery) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction

  SQL_TIME_STRUCT time_var_min{};
  SQL_TIME_STRUCT time_var_max{};
  SQLLEN buf_len = sizeof(time_var_min);
  SQLLEN ind;

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 1, SQL_C_TYPE_TIME, &time_var_min, buf_len, &ind));

  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 2, SQL_C_TYPE_TIME, &time_var_max, buf_len, &ind));

  std::wstring wsql =
      LR"(
   SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
          CAST(TIME '23:59:59' AS TIME) AS time_max;
   )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Check min values for time.
  EXPECT_EQ(0, time_var_min.hour);
  EXPECT_EQ(0, time_var_min.minute);
  EXPECT_EQ(0, time_var_min.second);

  // Check max values for time.
  EXPECT_EQ(23, time_var_max.hour);
  EXPECT_EQ(59, time_var_max.minute);
  EXPECT_EQ(59, time_var_max.second);
}

TEST_F(StatementMockTest, TestSQLBindColVarbinaryQuery) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind));

  std::wstring wsql = L"SELECT X'ABCDEF' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Check varbinary values
  EXPECT_EQ('\xAB', varbinary_val[0]);
  EXPECT_EQ('\xCD', varbinary_val[1]);
  EXPECT_EQ('\xEF', varbinary_val[2]);
}

TEST_F(StatementRemoteTest, TestSQLBindColNullQuery) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.

  SQLINTEGER val;
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, &ind));

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify SQL_NULL_DATA is returned for indicator
  EXPECT_EQ(SQL_NULL_DATA, ind);
}

TEST_F(StatementRemoteTest, TestSQLBindColNullQueryNullIndicator) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.

  SQLINTEGER val;

  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, 0));

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_ERROR, SQLFetch(this->stmt));
  // Verify invalid null indicator is reported, as it is required
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState22002);
}

TYPED_TEST(StatementTest, TestSQLBindColRowFetching) {
  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  std::wstring wsql =
      LR"(
   SELECT 1 AS small_table
   UNION ALL
   SELECT 2
   UNION ALL
   SELECT 3;
 )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  // Fetch row 1
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify 1 is returned
  EXPECT_EQ(1, val);

  // Fetch row 2
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify 2 is returned
  EXPECT_EQ(2, val);

  // Fetch row 3
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify 3 is returned
  EXPECT_EQ(3, val);

  // Verify result set has no more data beyond row 3
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TYPED_TEST(StatementTest, TestSQLBindColRowArraySize) {
  // Set SQL_ATTR_ROW_ARRAY_SIZE to fetch 3 rows at once

  constexpr SQLULEN rows = 3;
  SQLINTEGER val[rows];
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind[rows];

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, val, buf_len, ind));

  SQLLEN rows_fetched;
  ASSERT_EQ(SQL_SUCCESS,
            SQLSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0));

  std::wstring wsql =
      LR"(
   SELECT 1 AS small_table
   UNION ALL
   SELECT 2
   UNION ALL
   SELECT 3;
 )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                                        reinterpret_cast<SQLPOINTER>(rows), 0));

  // Fetch 3 rows at once
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify 3 rows are fetched
  EXPECT_EQ(3, rows_fetched);

  // Verify 1 is returned
  EXPECT_EQ(1, val[0]);
  // Verify 2 is returned
  EXPECT_EQ(2, val[1]);
  // Verify 3 is returned
  EXPECT_EQ(3, val[2]);

  // Verify result set has no more data beyond row 3
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));
}

TYPED_TEST(StatementTest, DISABLED_TestSQLBindColIndicatorOnly) {
  // GH-47021: implement driver to return indicator value when data pointer is null

  // Verify driver supports null data pointer with valid indicator pointer

  // Numeric Types

  // Signed Tiny Int
  SQLLEN stiny_int_ind;
  EXPECT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_STINYINT, 0, 0, &stiny_int_ind));

  // Characters
  SQLLEN buf_len = sizeof(SQLCHAR) * 2;
  SQLLEN char_val_ind;
  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 25, SQL_C_CHAR, 0, buf_len, &char_val_ind));

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Verify values for indicator pointer
  // Signed Tiny Int
  EXPECT_EQ(1, stiny_int_ind);

  // Char array
  EXPECT_EQ(1, char_val_ind);
}

TYPED_TEST(StatementTest, TestSQLBindColIndicatorOnlySQLUnbind) {
  // Verify driver supports valid indicator pointer after unbinding all columns

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN stiny_int_ind;
  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, 0, &stiny_int_ind));

  // Characters
  SQLCHAR char_val[2];
  SQLLEN buf_len = sizeof(SQLCHAR) * 2;
  SQLLEN char_val_ind;
  EXPECT_EQ(SQL_SUCCESS,
            SQLBindCol(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &char_val_ind));

  // Driver should still be able to execute queries after unbinding columns
  EXPECT_EQ(SQL_SUCCESS, SQLFreeStmt(this->stmt, SQL_UNBIND));

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // GH-47021: implement driver to return indicator value when data pointer is null and
  // uncomment the checks Verify values for indicator pointer Signed Tiny Int
  // EXPECT_EQ(1, stiny_int_ind);

  // Char array
  // EXPECT_EQ(1, char_val_ind);
}

}  // namespace arrow::flight::sql::odbc
