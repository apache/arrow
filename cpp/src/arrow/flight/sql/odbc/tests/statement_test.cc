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

TYPED_TEST(StatementTest, TestSQLExecDirectSimpleQuery) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Verify 1 is returned
  EXPECT_EQ(1, val);

  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

TYPED_TEST(StatementTest, TestSQLExecDirectInvalidQuery) {
  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY000);
}

TYPED_TEST(StatementTest, TestSQLExecuteSimpleQuery) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLExecute(this->stmt));

  // Fetch data
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));

  // Verify 1 is returned
  EXPECT_EQ(1, val);

  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

TYPED_TEST(StatementTest, TestSQLPrepareInvalidQuery) {
  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_ERROR,
            SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY000);

  ASSERT_EQ(SQL_ERROR, SQLExecute(this->stmt));
  // Verify function sequence error state is returned
#ifdef __APPLE__
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1010);
#else
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);
#endif  // __APPLE__
}

TYPED_TEST(StatementTest, TestSQLExecDirectDataQuery) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN buf_len = sizeof(stiny_int_val);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int8_t>::min(), stiny_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int8_t>::max(), stiny_int_val);

  // Unsigned Tiny Int
  uint8_t utiny_int_val;
  buf_len = sizeof(utiny_int_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint8_t>::min(), utiny_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint8_t>::max(), utiny_int_val);

  // Signed Small Int
  int16_t ssmall_int_val;
  buf_len = sizeof(ssmall_int_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int16_t>::min(), ssmall_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int16_t>::max(), ssmall_int_val);

  // Unsigned Small Int
  uint16_t usmall_int_val;
  buf_len = sizeof(usmall_int_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 7, SQL_C_USHORT, &usmall_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint16_t>::min(), usmall_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 8, SQL_C_USHORT, &usmall_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint16_t>::max(), usmall_int_val);

  // Signed Integer
  SQLINTEGER slong_val;
  buf_len = sizeof(slong_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 9, SQL_C_SLONG, &slong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::min(), slong_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 10, SQL_C_SLONG, &slong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::max(), slong_val);

  // Unsigned Integer
  SQLUINTEGER ulong_val;
  buf_len = sizeof(ulong_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 11, SQL_C_ULONG, &ulong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::min(), ulong_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 12, SQL_C_ULONG, &ulong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::max(), ulong_val);

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  buf_len = sizeof(sbig_int_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::min(), sbig_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::max(), sbig_int_val);

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val;
  buf_len = sizeof(ubig_int_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::min(), ubig_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::max(), ubig_int_val);

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));
  buf_len = sizeof(SQL_NUMERIC_STRUCT);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 17, SQL_C_NUMERIC, &decimal_val, buf_len, &ind));
  // Check for negative decimal_val value
  EXPECT_EQ(0, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 18, SQL_C_NUMERIC, &decimal_val, buf_len, &ind));
  // Check for positive decimal_val value
  EXPECT_EQ(1, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;
  buf_len = sizeof(float_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 19, SQL_C_FLOAT, &float_val, buf_len, &ind));
  // Get minimum negative float value
  EXPECT_EQ(-std::numeric_limits<float>::max(), float_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 20, SQL_C_FLOAT, &float_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<float>::max(), float_val);

  // Double
  SQLDOUBLE double_val;
  buf_len = sizeof(double_val);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 21, SQL_C_DOUBLE, &double_val, buf_len, &ind));
  // Get minimum negative double value
  EXPECT_EQ(-std::numeric_limits<SQLDOUBLE>::max(), double_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 22, SQL_C_DOUBLE, &double_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLDOUBLE>::max(), double_val);

  // Bit
  bool bit_val;
  buf_len = sizeof(bit_val);
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 23, SQL_C_BIT, &bit_val, buf_len, &ind));
  EXPECT_EQ(false, bit_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 24, SQL_C_BIT, &bit_val, buf_len, &ind));
  EXPECT_EQ(true, bit_val);

  // Characters

  // Char
  SQLCHAR char_val[2];
  buf_len = sizeof(SQLCHAR) * 2;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &ind));
  EXPECT_EQ('Z', char_val[0]);

  // WChar
  SQLWCHAR wchar_val[2];
  size_t wchar_size = GetSqlWCharSize();
  buf_len = wchar_size * 2;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 26, SQL_C_WCHAR, &wchar_val, buf_len, &ind));
  EXPECT_EQ(L'你', wchar_val[0]);

  // WVarchar
  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 27, SQL_C_WCHAR, &wvarchar_val, buf_len, &ind));
  EXPECT_EQ(L'你', wvarchar_val[0]);
  EXPECT_EQ(L'好', wvarchar_val[1]);

  // varchar
  SQLCHAR varchar_val[4];
  buf_len = sizeof(SQLCHAR) * 4;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 28, SQL_C_CHAR, &varchar_val, buf_len, &ind));
  EXPECT_EQ('X', varchar_val[0]);
  EXPECT_EQ('Y', varchar_val[1]);
  EXPECT_EQ('Z', varchar_val[2]);

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};
  buf_len = sizeof(date_var);
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 29, SQL_C_TYPE_DATE, &date_var, buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, date_var.day);
  EXPECT_EQ(1, date_var.month);
  EXPECT_EQ(1400, date_var.year);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 30, SQL_C_TYPE_DATE, &date_var, buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, date_var.day);
  EXPECT_EQ(12, date_var.month);
  EXPECT_EQ(9999, date_var.year);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};
  buf_len = sizeof(timestamp_var);
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 31, SQL_C_TYPE_TIMESTAMP, &timestamp_var,
                                    buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, timestamp_var.day);
  EXPECT_EQ(1, timestamp_var.month);
  EXPECT_EQ(1400, timestamp_var.year);
  EXPECT_EQ(0, timestamp_var.hour);
  EXPECT_EQ(0, timestamp_var.minute);
  EXPECT_EQ(0, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 32, SQL_C_TYPE_TIMESTAMP, &timestamp_var,
                                    buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, timestamp_var.day);
  EXPECT_EQ(12, timestamp_var.month);
  EXPECT_EQ(9999, timestamp_var.year);
  EXPECT_EQ(23, timestamp_var.hour);
  EXPECT_EQ(59, timestamp_var.minute);
  EXPECT_EQ(59, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);
}

TEST_F(StatementRemoteTest, TestSQLExecDirectTimeQuery) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction

  std::wstring wsql =
      LR"(
    SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
           CAST(TIME '23:59:59' AS TIME) AS time_max;
    )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQL_TIME_STRUCT time_var{};
  SQLLEN buf_len = sizeof(time_var);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_TYPE_TIME, &time_var, buf_len, &ind));
  // Check min values for time.
  EXPECT_EQ(0, time_var.hour);
  EXPECT_EQ(0, time_var.minute);
  EXPECT_EQ(0, time_var.second);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 2, SQL_C_TYPE_TIME, &time_var, buf_len, &ind));
  // Check max values for time.
  EXPECT_EQ(23, time_var.hour);
  EXPECT_EQ(59, time_var.minute);
  EXPECT_EQ(59, time_var.second);
}

TEST_F(StatementMockTest, TestSQLExecDirectVarbinaryQuery) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data

  std::wstring wsql = L"SELECT X'ABCDEF' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind));
  EXPECT_EQ('\xAB', varbinary_val[0]);
  EXPECT_EQ('\xCD', varbinary_val[1]);
  EXPECT_EQ('\xEF', varbinary_val[2]);
}

// Tests with SQL_C_DEFAULT as the target type

TEST_F(StatementRemoteTest, TestSQLExecDirectDataQueryDefaultType) {
  // Test with default types. Only testing target types supported by server.

  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Numeric Types
  // Signed Integer
  SQLINTEGER slong_val;
  SQLLEN buf_len = sizeof(slong_val);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 9, SQL_C_DEFAULT, &slong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::min(), slong_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 10, SQL_C_DEFAULT, &slong_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::max(), slong_val);

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  buf_len = sizeof(sbig_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 13, SQL_C_DEFAULT, &sbig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::min(), sbig_int_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 14, SQL_C_DEFAULT, &sbig_int_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::max(), sbig_int_val);

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));
  buf_len = sizeof(SQL_NUMERIC_STRUCT);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 17, SQL_C_DEFAULT, &decimal_val, buf_len, &ind));
  // Check for negative decimal_val value
  EXPECT_EQ(0, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 18, SQL_C_DEFAULT, &decimal_val, buf_len, &ind));
  // Check for positive decimal_val value
  EXPECT_EQ(1, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;
  buf_len = sizeof(float_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 19, SQL_C_DEFAULT, &float_val, buf_len, &ind));
  // Get minimum negative float value
  EXPECT_EQ(-std::numeric_limits<float>::max(), float_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 20, SQL_C_DEFAULT, &float_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<float>::max(), float_val);

  // Double
  SQLDOUBLE double_val;
  buf_len = sizeof(double_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 21, SQL_C_DEFAULT, &double_val, buf_len, &ind));
  // Get minimum negative double value
  EXPECT_EQ(-std::numeric_limits<SQLDOUBLE>::max(), double_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 22, SQL_C_DEFAULT, &double_val, buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLDOUBLE>::max(), double_val);

  // Bit
  bool bit_val;
  buf_len = sizeof(bit_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 23, SQL_C_DEFAULT, &bit_val, buf_len, &ind));
  EXPECT_EQ(false, bit_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 24, SQL_C_DEFAULT, &bit_val, buf_len, &ind));
  EXPECT_EQ(true, bit_val);

  // Characters

  // Char will be fetched as wchar by default
  SQLWCHAR wchar_val[2];
  size_t wchar_size = GetSqlWCharSize();
  buf_len = wchar_size * 2;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 25, SQL_C_DEFAULT, &wchar_val, buf_len, &ind));
  EXPECT_EQ(L'Z', wchar_val[0]);

  // WChar
  SQLWCHAR wchar_val2[2];
  buf_len = wchar_size * 2;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 26, SQL_C_DEFAULT, &wchar_val2, buf_len, &ind));
  EXPECT_EQ(L'你', wchar_val2[0]);

  // WVarchar
  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 27, SQL_C_DEFAULT, &wvarchar_val, buf_len, &ind));
  EXPECT_EQ(L'你', wvarchar_val[0]);
  EXPECT_EQ(L'好', wvarchar_val[1]);

  // Varchar will be fetched as WVarchar by default
  SQLWCHAR wvarchar_val2[4];
  buf_len = wchar_size * 4;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 28, SQL_C_DEFAULT, &wvarchar_val2, buf_len, &ind));
  EXPECT_EQ(L'X', wvarchar_val2[0]);
  EXPECT_EQ(L'Y', wvarchar_val2[1]);
  EXPECT_EQ(L'Z', wvarchar_val2[2]);

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};
  buf_len = sizeof(date_var);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 29, SQL_C_DEFAULT, &date_var, buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, date_var.day);
  EXPECT_EQ(1, date_var.month);
  EXPECT_EQ(1400, date_var.year);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 30, SQL_C_DEFAULT, &date_var, buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, date_var.day);
  EXPECT_EQ(12, date_var.month);
  EXPECT_EQ(9999, date_var.year);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};
  buf_len = sizeof(timestamp_var);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 31, SQL_C_DEFAULT, &timestamp_var, buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, timestamp_var.day);
  EXPECT_EQ(1, timestamp_var.month);
  EXPECT_EQ(1400, timestamp_var.year);
  EXPECT_EQ(0, timestamp_var.hour);
  EXPECT_EQ(0, timestamp_var.minute);
  EXPECT_EQ(0, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 32, SQL_C_DEFAULT, &timestamp_var, buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, timestamp_var.day);
  EXPECT_EQ(12, timestamp_var.month);
  EXPECT_EQ(9999, timestamp_var.year);
  EXPECT_EQ(23, timestamp_var.hour);
  EXPECT_EQ(59, timestamp_var.minute);
  EXPECT_EQ(59, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);
}

TEST_F(StatementRemoteTest, TestSQLExecDirectTimeQueryDefaultType) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction

  std::wstring wsql =
      LR"(
   SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
          CAST(TIME '23:59:59' AS TIME) AS time_max;
   )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQL_TIME_STRUCT time_var{};
  SQLLEN buf_len = sizeof(time_var);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_DEFAULT, &time_var, buf_len, &ind));
  // Check min values for time.
  EXPECT_EQ(0, time_var.hour);
  EXPECT_EQ(0, time_var.minute);
  EXPECT_EQ(0, time_var.second);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 2, SQL_C_DEFAULT, &time_var, buf_len, &ind));
  // Check max values for time.
  EXPECT_EQ(23, time_var.hour);
  EXPECT_EQ(59, time_var.minute);
  EXPECT_EQ(59, time_var.second);
}

TEST_F(StatementRemoteTest, TestSQLExecDirectVarbinaryQueryDefaultType) {
  // Limitation on mock test server prevents SQL_C_DEFAULT from working properly.
  // Mock server has type `DENSE_UNION` for varbinary.
  // Note that not all remote servers support "from_hex" function

  std::wstring wsql = L"SELECT from_hex('ABCDEF') AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_DEFAULT, &varbinary_val[0], buf_len, &ind));
  EXPECT_EQ('\xAB', varbinary_val[0]);
  EXPECT_EQ('\xCD', varbinary_val[1]);
  EXPECT_EQ('\xEF', varbinary_val[2]);
}

TYPED_TEST(StatementTest, TestSQLExecDirectGuidQueryUnsupported) {
  // Query GUID as string as SQLite does not support GUID
  std::wstring wsql = L"SELECT 'C77313CF-4E08-47CE-B6DF-94DD2FCF3541' AS guid;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLGUID guid_var;
  SQLLEN buf_len = sizeof(guid_var);
  SQLLEN ind;
  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_GUID, &guid_var, buf_len, &ind));
  // GUID is not supported by ODBC
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY000);
}

TYPED_TEST(StatementTest, TestSQLExecDirectRowFetching) {
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

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 1 is returned
  EXPECT_EQ(1, val);

  // Fetch row 2
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 2 is returned
  EXPECT_EQ(2, val);

  // Fetch row 3
  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 3 is returned
  EXPECT_EQ(3, val);

  // Verify result set has no more data beyond row 3
  ASSERT_EQ(SQL_NO_DATA, SQLFetch(this->stmt));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind));

  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

TYPED_TEST(StatementTest, TestSQLFetchScrollRowFetching) {
  SQLLEN rows_fetched;
  SQLSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

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
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));
  // Verify 1 is returned
  EXPECT_EQ(1, val);
  // Verify 1 row is fetched
  EXPECT_EQ(1, rows_fetched);

  // Fetch row 2
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 2 is returned
  EXPECT_EQ(2, val);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(1, rows_fetched);

  // Fetch row 3
  ASSERT_EQ(SQL_SUCCESS, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind));

  // Verify 3 is returned
  EXPECT_EQ(3, val);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(1, rows_fetched);

  // Verify result set has no more data beyond row 3
  ASSERT_EQ(SQL_NO_DATA, SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0));

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind));
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

TYPED_TEST(StatementTest, TestSQLFetchScrollUnsupportedOrientation) {
  // SQL_FETCH_PRIOR is the only supported fetch orientation.

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_PRIOR, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  SQLLEN fetch_offset = 1;
  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_RELATIVE, fetch_offset));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_ABSOLUTE, fetch_offset));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_FIRST, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_LAST, 0));

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHYC00);

  ASSERT_EQ(SQL_ERROR, SQLFetchScroll(this->stmt, SQL_FETCH_BOOKMARK, fetch_offset));

  // DM returns state HY106 for SQL_FETCH_BOOKMARK
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY106);
}

TYPED_TEST(StatementTest, TestSQLExecDirectVarcharTruncation) {
  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  EXPECT_EQ(std::string("VERY LONG STRING"), ODBC::SqlStringToString(char_val));
  EXPECT_EQ(21, ind);

  // Fetch same column 2nd time
  const int len2 = 2;
  SQLCHAR char_val2[len2];
  buf_len = sizeof(SQLCHAR) * len2;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val2, buf_len, &ind));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  EXPECT_EQ(std::string(" "), ODBC::SqlStringToString(char_val2));
  EXPECT_EQ(5, ind);

  // Fetch same column 3rd time
  const int len3 = 5;
  SQLCHAR char_val3[len3];
  buf_len = sizeof(SQLCHAR) * len3;

  // Verify that there is no more truncation reports. The full string has been fetched.
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val3, buf_len, &ind));

  EXPECT_EQ(std::string("here"), ODBC::SqlStringToString(char_val3));
  EXPECT_EQ(4, ind);

  // Attempt to fetch data 4th time
  SQLCHAR char_val4[len];
  // Verify SQL_NO_DATA is returned
  ASSERT_EQ(SQL_NO_DATA, SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val4, 0, &ind));
}

TYPED_TEST(StatementTest, TestSQLExecDirectWVarcharTruncation) {
  std::wstring wsql = L"SELECT 'VERY LONG Unicode STRING 句子 here' AS wstring_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  const int len = 28;
  SQLWCHAR wchar_val[len];
  size_t wchar_size = GetSqlWCharSize();
  SQLLEN buf_len = wchar_size * len;
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val, buf_len, &ind));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  EXPECT_EQ(std::wstring(L"VERY LONG Unicode STRING 句子"), std::wstring(wchar_val));
  EXPECT_EQ(32 * wchar_size, ind);

  // Fetch same column 2nd time
  const int len2 = 2;
  SQLWCHAR wchar_val2[len2];
  buf_len = wchar_size * len2;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val2, buf_len, &ind));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  EXPECT_EQ(std::wstring(L" "), std::wstring(wchar_val2));
  EXPECT_EQ(5 * wchar_size, ind);

  // Fetch same column 3rd time
  const int len3 = 5;
  SQLWCHAR wchar_val3[len3];
  buf_len = wchar_size * len3;

  // Verify that there is no more truncation reports. The full string has been fetched.
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val3, buf_len, &ind));

  EXPECT_EQ(std::wstring(L"here"), std::wstring(wchar_val3));
  EXPECT_EQ(4 * wchar_size, ind);

  // Attempt to fetch data 4th time
  SQLWCHAR wchar_val4[len];
  // Verify SQL_NO_DATA is returned
  ASSERT_EQ(SQL_NO_DATA, SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val4, 0, &ind));
}

TEST_F(StatementMockTest, TestSQLExecDirectVarbinaryTruncation) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data

  std::wstring wsql = L"SELECT X'ABCDEFAB' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind));
  // Verify binary truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);
  EXPECT_EQ('\xAB', varbinary_val[0]);
  EXPECT_EQ('\xCD', varbinary_val[1]);
  EXPECT_EQ('\xEF', varbinary_val[2]);
  EXPECT_EQ(4, ind);

  // Fetch same column 2nd time
  std::vector<int8_t> varbinary_val2(1);
  buf_len = varbinary_val2.size();

  // Verify that there is no more truncation reports. The full binary has been fetched.
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val2[0], buf_len, &ind));

  EXPECT_EQ('\xAB', varbinary_val[0]);
  EXPECT_EQ(1, ind);

  // Attempt to fetch data 3rd time
  std::vector<int8_t> varbinary_val3(1);
  buf_len = varbinary_val3.size();
  // Verify SQL_NO_DATA is returned
  ASSERT_EQ(SQL_NO_DATA,
            SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val3[0], buf_len, &ind));
}

TYPED_TEST(StatementTest, DISABLED_TestSQLExecDirectFloatTruncation) {
  // Test is disabled until float truncation is supported.
  // GH-46985: return warning message instead of error on float truncation case
  std::wstring wsql;
  if constexpr (std::is_same_v<TypeParam, StatementMockTest>) {
    wsql = std::wstring(L"SELECT CAST(1.234 AS REAL) AS float_val");
  } else if constexpr (std::is_same_v<TypeParam, StatementRemoteTest>) {
    wsql = std::wstring(L"SELECT CAST(1.234 AS FLOAT) AS float_val");
  }
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  int16_t ssmall_int_val;

  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 1, SQL_C_SSHORT, &ssmall_int_val, 0, 0));
  // Verify float truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01S07);

  EXPECT_EQ(1, ssmall_int_val);
}

TEST_F(StatementRemoteTest, TestSQLExecDirectNullQuery) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind));

  // Verify SQL_NULL_DATA is returned for indicator
  EXPECT_EQ(SQL_NULL_DATA, ind);
}

TEST_F(StatementMockTest, TestSQLExecDirectTruncationQueryNullIndicator) {
  // Driver should not error out when indicator is null if the cell is non-null
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data

  std::wstring wsql =
      LR"(
       SELECT 1,
       'VERY LONG STRING here' AS string_col,
       'VERY LONG Unicode STRING 句子 here' AS wstring_col,
       X'ABCDEFAB' AS c_varbinary;
 )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Verify 1 is returned for non-truncation case.
  EXPECT_EQ(1, val);

  // Char
  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 2, SQL_C_CHAR, &char_val, buf_len, 0));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  // WChar
  const int len2 = 28;
  SQLWCHAR wchar_val[len2];
  size_t wchar_size = GetSqlWCharSize();
  buf_len = wchar_size * len2;
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 3, SQL_C_WCHAR, &wchar_val, buf_len, 0));
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  buf_len = varbinary_val.size();
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLGetData(this->stmt, 4, SQL_C_BINARY, &varbinary_val[0], buf_len, 0));
  // Verify binary truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState01004);
}

TEST_F(StatementRemoteTest, TestSQLExecDirectNullQueryNullIndicator) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  SQLINTEGER val;

  ASSERT_EQ(SQL_ERROR, SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0));
  // Verify invalid null indicator is reported, as it is required
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState22002);
}

TYPED_TEST(StatementTest, TestSQLExecDirectIgnoreInvalidBufLen) {
  // Verify the driver ignores invalid buffer length for fixed data types

  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN invalid_buf_len = -1;
  SQLLEN ind;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int8_t>::min(), stiny_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int8_t>::max(), stiny_int_val);

  // Unsigned Tiny Int
  uint8_t utiny_int_val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint8_t>::min(), utiny_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint8_t>::max(), utiny_int_val);

  // Signed Small Int
  int16_t ssmall_int_val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int16_t>::min(), ssmall_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<int16_t>::max(), ssmall_int_val);

  // Unsigned Small Int
  uint16_t usmall_int_val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 7, SQL_C_USHORT, &usmall_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint16_t>::min(), usmall_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 8, SQL_C_USHORT, &usmall_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<uint16_t>::max(), usmall_int_val);

  // Signed Integer
  SQLINTEGER slong_val;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 9, SQL_C_SLONG, &slong_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::min(), slong_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 10, SQL_C_SLONG, &slong_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLINTEGER>::max(), slong_val);

  // Unsigned Integer
  SQLUINTEGER ulong_val;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 11, SQL_C_ULONG, &ulong_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::min(), ulong_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 12, SQL_C_ULONG, &ulong_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUINTEGER>::max(), ulong_val);

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::min(), sbig_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLBIGINT>::max(), sbig_int_val);

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val;

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::min(), ubig_int_val);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val,
                                    invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLUBIGINT>::max(), ubig_int_val);

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 17, SQL_C_NUMERIC, &decimal_val,
                                    invalid_buf_len, &ind));
  // Check for negative decimal_val value
  EXPECT_EQ(0, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 18, SQL_C_NUMERIC, &decimal_val,
                                    invalid_buf_len, &ind));
  // Check for positive decimal_val value
  EXPECT_EQ(1, decimal_val.sign);
  EXPECT_EQ(0, decimal_val.scale);
  EXPECT_EQ(38, decimal_val.precision);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 19, SQL_C_FLOAT, &float_val, invalid_buf_len, &ind));
  // Get minimum negative float value
  EXPECT_EQ(-std::numeric_limits<float>::max(), float_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 20, SQL_C_FLOAT, &float_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<float>::max(), float_val);

  // Double
  SQLDOUBLE double_val;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 21, SQL_C_DOUBLE, &double_val, invalid_buf_len, &ind));
  // Get minimum negative double value
  EXPECT_EQ(-std::numeric_limits<SQLDOUBLE>::max(), double_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 22, SQL_C_DOUBLE, &double_val, invalid_buf_len, &ind));
  EXPECT_EQ(std::numeric_limits<SQLDOUBLE>::max(), double_val);

  // Bit
  bool bit_val;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 23, SQL_C_BIT, &bit_val, invalid_buf_len, &ind));
  EXPECT_EQ(false, bit_val);

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetData(this->stmt, 24, SQL_C_BIT, &bit_val, invalid_buf_len, &ind));
  EXPECT_EQ(true, bit_val);

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};
  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 29, SQL_C_TYPE_DATE, &date_var,
                                    invalid_buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, date_var.day);
  EXPECT_EQ(1, date_var.month);
  EXPECT_EQ(1400, date_var.year);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 30, SQL_C_TYPE_DATE, &date_var,
                                    invalid_buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, date_var.day);
  EXPECT_EQ(12, date_var.month);
  EXPECT_EQ(9999, date_var.year);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 31, SQL_C_TYPE_TIMESTAMP, &timestamp_var,
                                    invalid_buf_len, &ind));
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(1, timestamp_var.day);
  EXPECT_EQ(1, timestamp_var.month);
  EXPECT_EQ(1400, timestamp_var.year);
  EXPECT_EQ(0, timestamp_var.hour);
  EXPECT_EQ(0, timestamp_var.minute);
  EXPECT_EQ(0, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);

  ASSERT_EQ(SQL_SUCCESS, SQLGetData(this->stmt, 32, SQL_C_TYPE_TIMESTAMP, &timestamp_var,
                                    invalid_buf_len, &ind));
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(31, timestamp_var.day);
  EXPECT_EQ(12, timestamp_var.month);
  EXPECT_EQ(9999, timestamp_var.year);
  EXPECT_EQ(23, timestamp_var.hour);
  EXPECT_EQ(59, timestamp_var.minute);
  EXPECT_EQ(59, timestamp_var.second);
  EXPECT_EQ(0, timestamp_var.fraction);
}

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
  size_t wchar_size = GetSqlWCharSize();
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

TYPED_TEST(StatementTest, TestSQLExtendedFetchRowFetching) {
  // Set SQL_ROWSET_SIZE to fetch 3 rows at once

  constexpr SQLULEN rows = 3;
  SQLINTEGER val[rows];
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind[rows];

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, val, buf_len, ind));

  ASSERT_EQ(SQL_SUCCESS, SQLSetStmtAttr(this->stmt, SQL_ROWSET_SIZE,
                                        reinterpret_cast<SQLPOINTER>(rows), 0));

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

  // Fetch row 1-3.
  SQLULEN row_count;
  SQLUSMALLINT row_status[rows];

  ASSERT_EQ(SQL_SUCCESS,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count, row_status));
  EXPECT_EQ(3, row_count);

  for (int i = 0; i < rows; i++) {
    EXPECT_EQ(SQL_SUCCESS, row_status[i]);
  }

  // Verify 1 is returned for row 1
  EXPECT_EQ(1, val[0]);
  // Verify 2 is returned for row 2
  EXPECT_EQ(2, val[1]);
  // Verify 3 is returned for row 3
  EXPECT_EQ(3, val[2]);

  // Verify result set has no more data beyond row 3
  SQLULEN row_count2;
  SQLUSMALLINT row_status2[rows];
  EXPECT_EQ(SQL_NO_DATA,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count2, row_status2));
}

TEST_F(StatementRemoteTest, DISABLED_TestSQLExtendedFetchQueryNullIndicator) {
  // GH-47110: SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  SQLINTEGER val;

  ASSERT_EQ(SQL_SUCCESS, SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, 0));

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLULEN row_count1;
  SQLUSMALLINT row_status1[1];

  // SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002 state
  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count1, row_status1));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState22002);
}

TYPED_TEST(StatementTest, TestSQLMoreResultsNoData) {
  // Verify SQLMoreResults is stubbed to return SQL_NO_DATA

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_NO_DATA, SQLMoreResults(this->stmt));
}

TYPED_TEST(StatementTest, TestSQLMoreResultsInvalidFunctionSequence) {
  // Verify function sequence error state is reported when SQLMoreResults is called
  // without executing any queries
  ASSERT_EQ(SQL_ERROR, SQLMoreResults(this->stmt));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsInputString) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, input_char_len, buf,
                                      buf_char_len, &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  // returned length is in characters
  std::wstring returned_string(buf, buf + output_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsNTSInputString) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, SQL_NTS, buf, buf_char_len,
                                      &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  // returned length is in characters
  std::wstring returned_string(buf, buf + output_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsInputStringLength) {
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;
  std::wstring expected_string = std::wstring(input_str);

  ASSERT_EQ(SQL_SUCCESS, SQLNativeSql(this->conn, input_str, input_char_len, nullptr, 0,
                                      &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);

  ASSERT_EQ(SQL_SUCCESS,
            SQLNativeSql(this->conn, input_str, SQL_NTS, nullptr, 0, &output_char_len));

  EXPECT_EQ(input_char_len, output_char_len);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsTruncatedString) {
  const SQLINTEGER small_buf_size_in_char = 11;
  SQLWCHAR small_buf[small_buf_size_in_char];
  SQLINTEGER small_buf_char_len = sizeof(small_buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;

  // Create expected return string based on buf size
  SQLWCHAR expected_string_buf[small_buf_size_in_char];
  wcsncpy(expected_string_buf, input_str, 10);
  expected_string_buf[10] = L'\0';
  std::wstring expected_string(expected_string_buf,
                               expected_string_buf + small_buf_size_in_char);

  ASSERT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLNativeSql(this->conn, input_str, input_char_len, small_buf,
                         small_buf_char_len, &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorState01004);

  // Returned text length represents full string char length regardless of truncation
  EXPECT_EQ(input_char_len, output_char_len);

  std::wstring returned_string(small_buf, small_buf + small_buf_char_len);

  EXPECT_EQ(expected_string, returned_string);
}

TYPED_TEST(StatementTest, TestSQLNativeSqlReturnsErrorOnBadInputs) {
  SQLWCHAR buf[1024];
  SQLINTEGER buf_char_len = sizeof(buf) / GetSqlWCharSize();
  SQLWCHAR input_str[] = L"SELECT * FROM mytable WHERE id == 1";
  SQLINTEGER input_char_len = static_cast<SQLINTEGER>(wcslen(input_str));
  SQLINTEGER output_char_len = 0;

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, nullptr, input_char_len, buf,
                                    buf_char_len, &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY009);

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, nullptr, SQL_NTS, buf, buf_char_len,
                                    &output_char_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY009);

  ASSERT_EQ(SQL_ERROR, SQLNativeSql(this->conn, input_str, -100, buf, buf_char_len,
                                    &output_char_len));
#ifdef __APPLE__
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateS1090);
#else
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY090);
#endif  // __APPLE__
}

TYPED_TEST(StatementTest, SQLNumResultColsReturnsColumnsOnSelect) {
  SQLSMALLINT column_count = 0;
  SQLSMALLINT expected_value = 3;
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLNumResultCols(this->stmt, &column_count));

  EXPECT_EQ(expected_value, column_count);
}

TYPED_TEST(StatementTest, SQLNumResultColsReturnsSuccessOnNullptr) {
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLNumResultCols(this->stmt, nullptr));
}

TYPED_TEST(StatementTest, SQLNumResultColsFunctionSequenceErrorOnNoQuery) {
  SQLSMALLINT column_count = 0;
  SQLSMALLINT expected_value = 0;

  ASSERT_EQ(SQL_ERROR, SQLNumResultCols(this->stmt, &column_count));
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);

  ASSERT_EQ(SQL_ERROR, SQLNumResultCols(this->stmt, &column_count));
#ifdef __APPLE__
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1010);
#else
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);
#endif  // __APPLE__

  ASSERT_EQ(expected_value, column_count);
}

TYPED_TEST(StatementTest, SQLRowCountReturnsNegativeOneOnSelect) {
  SQLLEN row_count = 0;
  SQLLEN expected_value = -1;
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLRowCount(this->stmt, &row_count));

  EXPECT_EQ(expected_value, row_count);
}

TYPED_TEST(StatementTest, SQLRowCountReturnsSuccessOnNullptr) {
  SQLWCHAR sql_query[] = L"SELECT 1 AS col1, 'One' AS col2, 3 AS col3";
  SQLINTEGER query_length = static_cast<SQLINTEGER>(wcslen(sql_query));

  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(this->stmt, sql_query, query_length));

  ASSERT_EQ(SQL_SUCCESS, SQLFetch(this->stmt));

  CheckIntColumn(this->stmt, 1, 1);
  CheckStringColumnW(this->stmt, 2, L"One");
  CheckIntColumn(this->stmt, 3, 3);

  ASSERT_EQ(SQL_SUCCESS, SQLRowCount(this->stmt, 0));
}

TYPED_TEST(StatementTest, SQLRowCountFunctionSequenceErrorOnNoQuery) {
  SQLLEN row_count = 0;
  SQLLEN expected_value = 0;

  ASSERT_EQ(SQL_ERROR, SQLRowCount(this->stmt, &row_count));
#ifdef __APPLE__
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateS1010);
#else
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorStateHY010);
#endif  // __APPLE__

  EXPECT_EQ(expected_value, row_count);
}

TYPED_TEST(StatementTest, TestSQLFreeStmtSQLClose) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeStmt(this->stmt, SQL_CLOSE));
}

TYPED_TEST(StatementTest, TestSQLCloseCursor) {
  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  ASSERT_EQ(SQL_SUCCESS, SQLCloseCursor(this->stmt));
}

TYPED_TEST(StatementTest, TestSQLFreeStmtSQLCloseWithoutCursor) {
  // SQLFreeStmt(SQL_CLOSE) does not throw error with invalid cursor

  ASSERT_EQ(SQL_SUCCESS, SQLFreeStmt(this->stmt, SQL_CLOSE));
}

TYPED_TEST(StatementTest, TestSQLCloseCursorWithoutCursor) {
  ASSERT_EQ(SQL_ERROR, SQLCloseCursor(this->stmt));

  // Verify invalid cursor error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, kErrorState24000);
}

}  // namespace arrow::flight::sql::odbc
