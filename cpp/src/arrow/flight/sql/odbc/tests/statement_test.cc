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

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {
TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectSimpleQuery) {
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Verify 1 is returned
  EXPECT_EQ(val, 1);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_24000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectInvalidQuery) {
  this->connect();

  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecuteSimpleQuery) {
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret = SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLExecute(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch data
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Verify 1 is returned
  EXPECT_EQ(val, 1);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_24000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLPrepareInvalidQuery) {
  this->connect();

  std::wstring wsql = L"SELECT;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret = SQLPrepare(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));

  EXPECT_EQ(ret, SQL_ERROR);
  // ODBC provides generic error code HY000 to all statement errors
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY000);

  ret = SQLExecute(this->stmt);
  // Verify function sequence error state is returned
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY010);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectDataQuery) {
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN buf_len = sizeof(stiny_int_val);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::min());

  ret = SQLGetData(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::max());

  // Unsigned Tiny Int
  uint8_t utiny_int_val;
  buf_len = sizeof(utiny_int_val);

  ret = SQLGetData(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::min());

  ret = SQLGetData(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::max());

  // Signed Small Int
  int16_t ssmall_int_val;
  buf_len = sizeof(ssmall_int_val);

  ret = SQLGetData(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::min());

  ret = SQLGetData(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::max());

  // Unsigned Small Int
  uint16_t usmall_int_val;
  buf_len = sizeof(usmall_int_val);

  ret = SQLGetData(this->stmt, 7, SQL_C_USHORT, &usmall_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::min());

  ret = SQLGetData(this->stmt, 8, SQL_C_USHORT, &usmall_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::max());

  // Signed Integer
  SQLINTEGER slong_val;
  buf_len = sizeof(slong_val);

  ret = SQLGetData(this->stmt, 9, SQL_C_SLONG, &slong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::min());

  ret = SQLGetData(this->stmt, 10, SQL_C_SLONG, &slong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::max());

  // Unsigned Integer
  SQLUINTEGER ulong_val;
  buf_len = sizeof(ulong_val);

  ret = SQLGetData(this->stmt, 11, SQL_C_ULONG, &ulong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::min());

  ret = SQLGetData(this->stmt, 12, SQL_C_ULONG, &ulong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::max());

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  buf_len = sizeof(sbig_int_val);

  ret = SQLGetData(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::min());

  ret = SQLGetData(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::max());

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val;
  buf_len = sizeof(ubig_int_val);

  ret = SQLGetData(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::min());

  ret = SQLGetData(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::max());

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));
  buf_len = sizeof(SQL_NUMERIC_STRUCT);

  ret = SQLGetData(this->stmt, 17, SQL_C_NUMERIC, &decimal_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for negative decimal_val value
  EXPECT_EQ(decimal_val.sign, 0);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));
  ret = SQLGetData(this->stmt, 18, SQL_C_NUMERIC, &decimal_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for positive decimal_val value
  EXPECT_EQ(decimal_val.sign, 1);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;
  buf_len = sizeof(float_val);

  ret = SQLGetData(this->stmt, 19, SQL_C_FLOAT, &float_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative float value
  EXPECT_EQ(float_val, -std::numeric_limits<float>::max());

  ret = SQLGetData(this->stmt, 20, SQL_C_FLOAT, &float_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(float_val, std::numeric_limits<float>::max());

  // Double
  SQLDOUBLE double_val;
  buf_len = sizeof(double_val);

  ret = SQLGetData(this->stmt, 21, SQL_C_DOUBLE, &double_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative double value
  EXPECT_EQ(double_val, -std::numeric_limits<SQLDOUBLE>::max());

  ret = SQLGetData(this->stmt, 22, SQL_C_DOUBLE, &double_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(double_val, std::numeric_limits<SQLDOUBLE>::max());

  // Bit
  bool bit_val;
  buf_len = sizeof(bit_val);

  ret = SQLGetData(this->stmt, 23, SQL_C_BIT, &bit_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, false);

  ret = SQLGetData(this->stmt, 24, SQL_C_BIT, &bit_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, true);

  // Characters

  // Char
  SQLCHAR char_val[2];
  buf_len = sizeof(SQLCHAR) * 2;

  ret = SQLGetData(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(char_val[0], 'Z');

  // WChar
  SQLWCHAR wchar_val[2];
  constexpr size_t wchar_size = driver::odbcabstraction::GetSqlWCharSize();
  buf_len = wchar_size * 2;

  ret = SQLGetData(this->stmt, 26, SQL_C_WCHAR, &wchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wchar_val[0], L'你');

  // WVarchar
  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;

  ret = SQLGetData(this->stmt, 27, SQL_C_WCHAR, &wvarchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wvarchar_val[0], L'你');
  EXPECT_EQ(wvarchar_val[1], L'好');

  // varchar
  SQLCHAR varchar_val[4];
  buf_len = sizeof(SQLCHAR) * 4;

  ret = SQLGetData(this->stmt, 28, SQL_C_CHAR, &varchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(varchar_val[0], 'X');
  EXPECT_EQ(varchar_val[1], 'Y');
  EXPECT_EQ(varchar_val[2], 'Z');

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};
  buf_len = sizeof(date_var);

  ret = SQLGetData(this->stmt, 29, SQL_C_TYPE_DATE, &date_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(date_var.day, 1);
  EXPECT_EQ(date_var.month, 1);
  EXPECT_EQ(date_var.year, 1400);

  ret = SQLGetData(this->stmt, 30, SQL_C_TYPE_DATE, &date_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(date_var.day, 31);
  EXPECT_EQ(date_var.month, 12);
  EXPECT_EQ(date_var.year, 9999);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};
  buf_len = sizeof(timestamp_var);

  ret = SQLGetData(this->stmt, 31, SQL_C_TYPE_TIMESTAMP, &timestamp_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(timestamp_var.day, 1);
  EXPECT_EQ(timestamp_var.month, 1);
  EXPECT_EQ(timestamp_var.year, 1400);
  EXPECT_EQ(timestamp_var.hour, 0);
  EXPECT_EQ(timestamp_var.minute, 0);
  EXPECT_EQ(timestamp_var.second, 0);
  EXPECT_EQ(timestamp_var.fraction, 0);

  ret = SQLGetData(this->stmt, 32, SQL_C_TYPE_TIMESTAMP, &timestamp_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(timestamp_var.day, 31);
  EXPECT_EQ(timestamp_var.month, 12);
  EXPECT_EQ(timestamp_var.year, 9999);
  EXPECT_EQ(timestamp_var.hour, 23);
  EXPECT_EQ(timestamp_var.minute, 59);
  EXPECT_EQ(timestamp_var.second, 59);
  EXPECT_EQ(timestamp_var.fraction, 0);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectTimeQuery) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction
  this->connect();

  std::wstring wsql =
      LR"(
    SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
           CAST(TIME '23:59:59' AS TIME) AS time_max;
    )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQL_TIME_STRUCT time_var{};
  SQLLEN buf_len = sizeof(time_var);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_TYPE_TIME, &time_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for time.
  EXPECT_EQ(time_var.hour, 0);
  EXPECT_EQ(time_var.minute, 0);
  EXPECT_EQ(time_var.second, 0);

  ret = SQLGetData(this->stmt, 2, SQL_C_TYPE_TIME, &time_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for time.
  EXPECT_EQ(time_var.hour, 23);
  EXPECT_EQ(time_var.minute, 59);
  EXPECT_EQ(time_var.second, 59);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLExecDirectVarbinaryQuery) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data
  this->connect();

  std::wstring wsql = L"SELECT X'ABCDEF' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ret = SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind);
  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(varbinary_val[1], '\xCD');
  EXPECT_EQ(varbinary_val[2], '\xEF');

  this->disconnect();
}

// Tests with SQL_C_DEFAULT as the target type

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectDataQueryDefaultType) {
  // Test with default types. Only testing target types supported by server.
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Numeric Types
  // Signed Integer
  SQLINTEGER slong_val;
  SQLLEN buf_len = sizeof(slong_val);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 9, SQL_C_DEFAULT, &slong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::min());

  ret = SQLGetData(this->stmt, 10, SQL_C_DEFAULT, &slong_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::max());

  // Signed Big Int
  SQLBIGINT sbig_int_val;
  buf_len = sizeof(sbig_int_val);

  ret = SQLGetData(this->stmt, 13, SQL_C_DEFAULT, &sbig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::min());

  ret = SQLGetData(this->stmt, 14, SQL_C_DEFAULT, &sbig_int_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::max());

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));
  buf_len = sizeof(SQL_NUMERIC_STRUCT);

  ret = SQLGetData(this->stmt, 17, SQL_C_DEFAULT, &decimal_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for negative decimal_val value
  EXPECT_EQ(decimal_val.sign, 0);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));
  ret = SQLGetData(this->stmt, 18, SQL_C_DEFAULT, &decimal_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for positive decimal_val value
  EXPECT_EQ(decimal_val.sign, 1);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;
  buf_len = sizeof(float_val);

  ret = SQLGetData(this->stmt, 19, SQL_C_DEFAULT, &float_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative float value
  EXPECT_EQ(float_val, -std::numeric_limits<float>::max());

  ret = SQLGetData(this->stmt, 20, SQL_C_DEFAULT, &float_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(float_val, std::numeric_limits<float>::max());

  // Double
  SQLDOUBLE double_val;
  buf_len = sizeof(double_val);

  ret = SQLGetData(this->stmt, 21, SQL_C_DEFAULT, &double_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative double value
  EXPECT_EQ(double_val, -std::numeric_limits<SQLDOUBLE>::max());

  ret = SQLGetData(this->stmt, 22, SQL_C_DEFAULT, &double_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(double_val, std::numeric_limits<SQLDOUBLE>::max());

  // Bit
  bool bit_val;
  buf_len = sizeof(bit_val);

  ret = SQLGetData(this->stmt, 23, SQL_C_DEFAULT, &bit_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, false);

  ret = SQLGetData(this->stmt, 24, SQL_C_DEFAULT, &bit_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, true);

  // Characters

  // Char will be fetched as wchar by default
  SQLWCHAR wchar_val[2];
  constexpr size_t wchar_size = driver::odbcabstraction::GetSqlWCharSize();
  buf_len = wchar_size * 2;

  ret = SQLGetData(this->stmt, 25, SQL_C_DEFAULT, &wchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wchar_val[0], L'Z');

  // WChar
  SQLWCHAR wchar_val2[2];
  buf_len = wchar_size * 2;
  ret = SQLGetData(this->stmt, 26, SQL_C_DEFAULT, &wchar_val2, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wchar_val2[0], L'你');

  // WVarchar
  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;

  ret = SQLGetData(this->stmt, 27, SQL_C_DEFAULT, &wvarchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wvarchar_val[0], L'你');
  EXPECT_EQ(wvarchar_val[1], L'好');

  // Varchar will be fetched as WVarchar by default
  SQLWCHAR wvarchar_val2[4];
  buf_len = wchar_size * 4;

  ret = SQLGetData(this->stmt, 28, SQL_C_DEFAULT, &wvarchar_val2, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(wvarchar_val2[0], L'X');
  EXPECT_EQ(wvarchar_val2[1], L'Y');
  EXPECT_EQ(wvarchar_val2[2], L'Z');

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};
  buf_len = sizeof(date_var);

  ret = SQLGetData(this->stmt, 29, SQL_C_DEFAULT, &date_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(date_var.day, 1);
  EXPECT_EQ(date_var.month, 1);
  EXPECT_EQ(date_var.year, 1400);

  ret = SQLGetData(this->stmt, 30, SQL_C_DEFAULT, &date_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(date_var.day, 31);
  EXPECT_EQ(date_var.month, 12);
  EXPECT_EQ(date_var.year, 9999);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};
  buf_len = sizeof(timestamp_var);

  ret = SQLGetData(this->stmt, 31, SQL_C_DEFAULT, &timestamp_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(timestamp_var.day, 1);
  EXPECT_EQ(timestamp_var.month, 1);
  EXPECT_EQ(timestamp_var.year, 1400);
  EXPECT_EQ(timestamp_var.hour, 0);
  EXPECT_EQ(timestamp_var.minute, 0);
  EXPECT_EQ(timestamp_var.second, 0);
  EXPECT_EQ(timestamp_var.fraction, 0);

  ret = SQLGetData(this->stmt, 32, SQL_C_DEFAULT, &timestamp_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(timestamp_var.day, 31);
  EXPECT_EQ(timestamp_var.month, 12);
  EXPECT_EQ(timestamp_var.year, 9999);
  EXPECT_EQ(timestamp_var.hour, 23);
  EXPECT_EQ(timestamp_var.minute, 59);
  EXPECT_EQ(timestamp_var.second, 59);
  EXPECT_EQ(timestamp_var.fraction, 0);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectTimeQueryDefaultType) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction
  this->connect();

  std::wstring wsql =
      LR"(
    SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
           CAST(TIME '23:59:59' AS TIME) AS time_max;
    )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQL_TIME_STRUCT time_var{};
  SQLLEN buf_len = sizeof(time_var);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_DEFAULT, &time_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for time.
  EXPECT_EQ(time_var.hour, 0);
  EXPECT_EQ(time_var.minute, 0);
  EXPECT_EQ(time_var.second, 0);

  ret = SQLGetData(this->stmt, 2, SQL_C_DEFAULT, &time_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for time.
  EXPECT_EQ(time_var.hour, 23);
  EXPECT_EQ(time_var.minute, 59);
  EXPECT_EQ(time_var.second, 59);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectVarbinaryQueryDefaultType) {
  // Limitation on mock test server prevents SQL_C_DEFAULT from working properly.
  // Mock server has type `DENSE_UNION` for varbinary.
  // Note that not all remote servers support "from_hex" function
  this->connect();

  std::wstring wsql = L"SELECT from_hex('ABCDEF') AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ret = SQLGetData(this->stmt, 1, SQL_C_DEFAULT, &varbinary_val[0], buf_len, &ind);
  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(varbinary_val[1], '\xCD');
  EXPECT_EQ(varbinary_val[2], '\xEF');

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectGuidQueryUnsupported) {
  this->connect();

  // Query GUID as string as SQLite does not support GUID
  std::wstring wsql = L"SELECT 'C77313CF-4E08-47CE-B6DF-94DD2FCF3541' AS guid;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLGUID guid_var;
  SQLLEN buf_len = sizeof(guid_var);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_GUID, &guid_var, buf_len, &ind);

  EXPECT_EQ(ret, SQL_ERROR);
  // GUID is not supported by ODBC
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectRowFetching) {
  this->connect();

  std::wstring wsql =
      LR"(
    SELECT 1 AS small_table
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch row 1
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 1 is returned
  EXPECT_EQ(val, 1);

  // Fetch row 2
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 2 is returned
  EXPECT_EQ(val, 2);

  // Fetch row 3
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 3 is returned
  EXPECT_EQ(val, 3);

  // Verify result set has no more data beyond row 3
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind);
  EXPECT_EQ(ret, SQL_ERROR);

  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_24000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLFetchScrollRowFetching) {
  this->connect();

  SQLLEN rows_fetched;
  SQLRETURN ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

  std::wstring wsql =
      LR"(
    SELECT 1 AS small_table
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch row 1
  ret = SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Verify 1 is returned
  EXPECT_EQ(val, 1);
  // Verify 1 row is fetched
  EXPECT_EQ(rows_fetched, 1);

  // Fetch row 2
  ret = SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 2 is returned
  EXPECT_EQ(val, 2);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(rows_fetched, 1);

  // Fetch row 3
  ret = SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 3 is returned
  EXPECT_EQ(val, 3);
  // Verify 1 row is fetched in the last SQLFetchScroll call
  EXPECT_EQ(rows_fetched, 1);

  // Verify result set has no more data beyond row 3
  ret = SQLFetchScroll(this->stmt, SQL_FETCH_NEXT, 0);
  EXPECT_EQ(ret, SQL_NO_DATA);

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind);

  EXPECT_EQ(ret, SQL_ERROR);
  // Invalid cursor state
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_24000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLFetchScrollUnsupportedOrientation) {
  // SQL_FETCH_PRIOR is the only supported fetch orientation.
  this->connect();

  std::wstring wsql = L"SELECT 1;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_PRIOR, 0);
  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HYC00);

  SQLLEN fetch_offset = 1;

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_RELATIVE, fetch_offset);
  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HYC00);

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_ABSOLUTE, fetch_offset);
  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HYC00);

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_FIRST, 0);
  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HYC00);

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_LAST, 0);
  EXPECT_EQ(ret, SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HYC00);

  ret = SQLFetchScroll(this->stmt, SQL_FETCH_BOOKMARK, fetch_offset);
  EXPECT_EQ(ret, SQL_ERROR);

  // DM returns state HY106 for SQL_FETCH_BOOKMARK
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_HY106);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectVarcharTruncation) {
  this->connect();

  std::wstring wsql = L"SELECT 'VERY LONG STRING here' AS string_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  EXPECT_EQ(ODBC::SqlStringToString(char_val), std::string("VERY LONG STRING"));
  EXPECT_EQ(ind, 21);

  // Fetch same column 2nd time
  const int len2 = 2;
  SQLCHAR char_val2[len2];
  buf_len = sizeof(SQLCHAR) * len2;

  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val2, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  EXPECT_EQ(ODBC::SqlStringToString(char_val2), std::string(" "));
  EXPECT_EQ(ind, 5);

  // Fetch same column 3rd time
  const int len3 = 5;
  SQLCHAR char_val3[len3];
  buf_len = sizeof(SQLCHAR) * len3;

  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val3, buf_len, &ind);

  // Verify that there is no more truncation reports. The full string has been fetched.
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(ODBC::SqlStringToString(char_val3), std::string("here"));
  EXPECT_EQ(ind, 4);

  // Attempt to fetch data 4th time
  SQLCHAR char_val4[len];
  ret = SQLGetData(this->stmt, 1, SQL_C_CHAR, &char_val4, 0, &ind);
  // Verify SQL_NO_DATA is returned
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectWVarcharTruncation) {
  this->connect();

  std::wstring wsql = L"SELECT 'VERY LONG Unicode STRING 句子 here' AS wstring_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  const int len = 28;
  SQLWCHAR wchar_val[len];
  constexpr size_t wchar_size = driver::odbcabstraction::GetSqlWCharSize();
  SQLLEN buf_len = wchar_size * len;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  EXPECT_EQ(std::wstring(wchar_val), std::wstring(L"VERY LONG Unicode STRING 句子"));
  EXPECT_EQ(ind, 32 * wchar_size);

  // Fetch same column 2nd time
  const int len2 = 2;
  SQLWCHAR wchar_val2[len2];
  buf_len = wchar_size * len2;

  ret = SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val2, buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  EXPECT_EQ(std::wstring(wchar_val2), std::wstring(L" "));
  EXPECT_EQ(ind, 5 * wchar_size);

  // Fetch same column 3rd time
  const int len3 = 5;
  SQLWCHAR wchar_val3[len3];
  buf_len = wchar_size * len3;

  ret = SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val3, buf_len, &ind);

  // Verify that there is no more truncation reports. The full string has been fetched.
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(std::wstring(wchar_val3), std::wstring(L"here"));
  EXPECT_EQ(ind, 4 * wchar_size);

  // Attempt to fetch data 4th time
  SQLWCHAR wchar_val4[len];
  ret = SQLGetData(this->stmt, 1, SQL_C_WCHAR, &wchar_val4, 0, &ind);
  // Verify SQL_NO_DATA is returned
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLExecDirectVarbinaryTruncation) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data
  this->connect();

  std::wstring wsql = L"SELECT X'ABCDEFAB' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  ret = SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind);
  // Verify binary truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);
  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(varbinary_val[1], '\xCD');
  EXPECT_EQ(varbinary_val[2], '\xEF');
  EXPECT_EQ(ind, 4);

  // Fetch same column 2nd time
  std::vector<int8_t> varbinary_val2(1);
  buf_len = varbinary_val2.size();

  ret = SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val2[0], buf_len, &ind);

  // Verify that there is no more truncation reports. The full binary has been fetched.
  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(ind, 1);

  // Attempt to fetch data 3rd time
  std::vector<int8_t> varbinary_val3(1);
  buf_len = varbinary_val3.size();
  ret = SQLGetData(this->stmt, 1, SQL_C_BINARY, &varbinary_val3[0], buf_len, &ind);
  // Verify SQL_NO_DATA is returned
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectFloatTruncation) {
  // Test is disabled until float truncation is supported.
  // GH-46985: return warning message instead of error on float truncation case
  GTEST_SKIP();
  this->connect();

  std::wstring wsql;
  if constexpr (std::is_same_v<TypeParam, FlightSQLODBCMockTestBase>) {
    wsql = std::wstring(L"SELECT CAST(1.234 AS REAL) AS float_val");
  } else if constexpr (std::is_same_v<TypeParam, FlightSQLODBCRemoteTestBase>) {
    wsql = std::wstring(L"SELECT CAST(1.234 AS FLOAT) AS float_val");
  }
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  int16_t ssmall_int_val;

  ret = SQLGetData(this->stmt, 1, SQL_C_SSHORT, &ssmall_int_val, 0, 0);
  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify float truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01S07);

  EXPECT_EQ(ssmall_int_val, 1);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectNullQuery) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  this->connect();

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify SQL_NULL_DATA is returned for indicator
  EXPECT_EQ(ind, SQL_NULL_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLExecDirectTruncationQueryNullIndicator) {
  // Driver should not error out when indicator is null if the cell is non-null
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data
  this->connect();

  std::wstring wsql =
      LR"(
        SELECT 1,
        'VERY LONG STRING here' AS string_col,
        'VERY LONG Unicode STRING 句子 here' AS wstring_col,
        X'ABCDEFAB' AS c_varbinary;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;
  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Verify 1 is returned for non-truncation case.
  EXPECT_EQ(val, 1);

  // Char
  const int len = 17;
  SQLCHAR char_val[len];
  SQLLEN buf_len = sizeof(SQLCHAR) * len;

  ret = SQLGetData(this->stmt, 2, SQL_C_CHAR, &char_val, buf_len, 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  // WChar
  const int len2 = 28;
  SQLWCHAR wchar_val[len2];
  constexpr size_t wchar_size = driver::odbcabstraction::GetSqlWCharSize();
  buf_len = wchar_size * len2;

  ret = SQLGetData(this->stmt, 3, SQL_C_WCHAR, &wchar_val, buf_len, 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  // Verify string truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  buf_len = varbinary_val.size();
  ret = SQLGetData(this->stmt, 4, SQL_C_BINARY, &varbinary_val[0], buf_len, 0);
  // Verify binary truncation is reported
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_01004);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExecDirectNullQueryNullIndicator) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  this->connect();

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLINTEGER val;

  ret = SQLGetData(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  // Verify invalid null indicator is reported, as it is required
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_22002);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExecDirectIgnoreInvalidBufLen) {
  // Verify the driver ignores invalid buffer length for fixed data types
  this->connect();

  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  SQLRETURN ret =
      SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN invalid_buf_len = -1;
  SQLLEN ind;

  ret = SQLGetData(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::min());

  ret = SQLGetData(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(stiny_int_val, std::numeric_limits<int8_t>::max());

  // Unsigned Tiny Int
  uint8_t utiny_int_val;

  ret = SQLGetData(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::min());

  ret = SQLGetData(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(utiny_int_val, std::numeric_limits<uint8_t>::max());

  // Signed Small Int
  int16_t ssmall_int_val;

  ret = SQLGetData(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::min());

  ret = SQLGetData(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ssmall_int_val, std::numeric_limits<int16_t>::max());

  // Unsigned Small Int
  uint16_t usmall_int_val;

  ret = SQLGetData(this->stmt, 7, SQL_C_USHORT, &usmall_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::min());

  ret = SQLGetData(this->stmt, 8, SQL_C_USHORT, &usmall_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(usmall_int_val, std::numeric_limits<uint16_t>::max());

  // Signed Integer
  SQLINTEGER slong_val;

  ret = SQLGetData(this->stmt, 9, SQL_C_SLONG, &slong_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::min());

  ret = SQLGetData(this->stmt, 10, SQL_C_SLONG, &slong_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(slong_val, std::numeric_limits<SQLINTEGER>::max());

  // Unsigned Integer
  SQLUINTEGER ulong_val;

  ret = SQLGetData(this->stmt, 11, SQL_C_ULONG, &ulong_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::min());

  ret = SQLGetData(this->stmt, 12, SQL_C_ULONG, &ulong_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ulong_val, std::numeric_limits<SQLUINTEGER>::max());

  // Signed Big Int
  SQLBIGINT sbig_int_val;

  ret = SQLGetData(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::min());

  ret = SQLGetData(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(sbig_int_val, std::numeric_limits<SQLBIGINT>::max());

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val;

  ret = SQLGetData(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::min());

  ret = SQLGetData(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ubig_int_val, std::numeric_limits<SQLUBIGINT>::max());

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val;
  memset(&decimal_val, 0, sizeof(decimal_val));

  ret = SQLGetData(this->stmt, 17, SQL_C_NUMERIC, &decimal_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for negative decimal_val value
  EXPECT_EQ(decimal_val.sign, 0);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  memset(&decimal_val, 0, sizeof(decimal_val));
  ret = SQLGetData(this->stmt, 18, SQL_C_NUMERIC, &decimal_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check for positive decimal_val value
  EXPECT_EQ(decimal_val.sign, 1);
  EXPECT_EQ(decimal_val.scale, 0);
  EXPECT_EQ(decimal_val.precision, 38);
  EXPECT_THAT(decimal_val.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0, 0,
                                                      0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  float float_val;

  ret = SQLGetData(this->stmt, 19, SQL_C_FLOAT, &float_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative float value
  EXPECT_EQ(float_val, -std::numeric_limits<float>::max());

  ret = SQLGetData(this->stmt, 20, SQL_C_FLOAT, &float_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(float_val, std::numeric_limits<float>::max());

  // Double
  SQLDOUBLE double_val;

  ret = SQLGetData(this->stmt, 21, SQL_C_DOUBLE, &double_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Get minimum negative double value
  EXPECT_EQ(double_val, -std::numeric_limits<SQLDOUBLE>::max());

  ret = SQLGetData(this->stmt, 22, SQL_C_DOUBLE, &double_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(double_val, std::numeric_limits<SQLDOUBLE>::max());

  // Bit
  bool bit_val;

  ret = SQLGetData(this->stmt, 23, SQL_C_BIT, &bit_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, false);

  ret = SQLGetData(this->stmt, 24, SQL_C_BIT, &bit_val, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(bit_val, true);

  // Date and Timestamp

  // Date
  SQL_DATE_STRUCT date_var{};

  ret = SQLGetData(this->stmt, 29, SQL_C_TYPE_DATE, &date_var, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(date_var.day, 1);
  EXPECT_EQ(date_var.month, 1);
  EXPECT_EQ(date_var.year, 1400);

  ret = SQLGetData(this->stmt, 30, SQL_C_TYPE_DATE, &date_var, invalid_buf_len, &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(date_var.day, 31);
  EXPECT_EQ(date_var.month, 12);
  EXPECT_EQ(date_var.year, 9999);

  // Timestamp
  SQL_TIMESTAMP_STRUCT timestamp_var{};

  ret = SQLGetData(this->stmt, 31, SQL_C_TYPE_TIMESTAMP, &timestamp_var, invalid_buf_len,
                   &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check min values for date. Min valid year is 1400.
  EXPECT_EQ(timestamp_var.day, 1);
  EXPECT_EQ(timestamp_var.month, 1);
  EXPECT_EQ(timestamp_var.year, 1400);
  EXPECT_EQ(timestamp_var.hour, 0);
  EXPECT_EQ(timestamp_var.minute, 0);
  EXPECT_EQ(timestamp_var.second, 0);
  EXPECT_EQ(timestamp_var.fraction, 0);

  ret = SQLGetData(this->stmt, 32, SQL_C_TYPE_TIMESTAMP, &timestamp_var, invalid_buf_len,
                   &ind);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Check max values for date. Max valid year is 9999.
  EXPECT_EQ(timestamp_var.day, 31);
  EXPECT_EQ(timestamp_var.month, 12);
  EXPECT_EQ(timestamp_var.year, 9999);
  EXPECT_EQ(timestamp_var.hour, 23);
  EXPECT_EQ(timestamp_var.minute, 59);
  EXPECT_EQ(timestamp_var.second, 59);
  EXPECT_EQ(timestamp_var.fraction, 0);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLBindColDataQuery) {
  this->connect();

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val_min;
  int8_t stiny_int_val_max;
  SQLLEN buf_len = 0;
  SQLLEN ind;

  SQLRETURN ret =
      SQLBindCol(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 2, SQL_C_STINYINT, &stiny_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Unsigned Tiny Int
  uint8_t utiny_int_val_min;
  uint8_t utiny_int_val_max;

  ret = SQLBindCol(this->stmt, 3, SQL_C_UTINYINT, &utiny_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 4, SQL_C_UTINYINT, &utiny_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Signed Small Int
  int16_t ssmall_int_val_min;
  int16_t ssmall_int_val_max;

  ret = SQLBindCol(this->stmt, 5, SQL_C_SSHORT, &ssmall_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 6, SQL_C_SSHORT, &ssmall_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Unsigned Small Int
  uint16_t usmall_int_val_min;
  uint16_t usmall_int_val_max;

  ret = SQLBindCol(this->stmt, 7, SQL_C_USHORT, &usmall_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 8, SQL_C_USHORT, &usmall_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Signed Integer
  SQLINTEGER slong_val_min;
  SQLINTEGER slong_val_max;

  ret = SQLBindCol(this->stmt, 9, SQL_C_SLONG, &slong_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 10, SQL_C_SLONG, &slong_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Unsigned Integer
  SQLUINTEGER ulong_val_min;
  SQLUINTEGER ulong_val_max;

  ret = SQLBindCol(this->stmt, 11, SQL_C_ULONG, &ulong_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 12, SQL_C_ULONG, &ulong_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Signed Big Int
  SQLBIGINT sbig_int_val_min;
  SQLBIGINT sbig_int_val_max;

  ret = SQLBindCol(this->stmt, 13, SQL_C_SBIGINT, &sbig_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 14, SQL_C_SBIGINT, &sbig_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Unsigned Big Int
  SQLUBIGINT ubig_int_val_min;
  SQLUBIGINT ubig_int_val_max;

  ret = SQLBindCol(this->stmt, 15, SQL_C_UBIGINT, &ubig_int_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 16, SQL_C_UBIGINT, &ubig_int_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Decimal
  SQL_NUMERIC_STRUCT decimal_val_neg;
  SQL_NUMERIC_STRUCT decimal_val_pos;
  memset(&decimal_val_neg, 0, sizeof(decimal_val_neg));
  memset(&decimal_val_pos, 0, sizeof(decimal_val_pos));

  ret = SQLBindCol(this->stmt, 17, SQL_C_NUMERIC, &decimal_val_neg, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 18, SQL_C_NUMERIC, &decimal_val_pos, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Float
  float float_val_min;
  float float_val_max;

  ret = SQLBindCol(this->stmt, 19, SQL_C_FLOAT, &float_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 20, SQL_C_FLOAT, &float_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Double
  SQLDOUBLE double_val_min;
  SQLDOUBLE double_val_max;

  ret = SQLBindCol(this->stmt, 21, SQL_C_DOUBLE, &double_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 22, SQL_C_DOUBLE, &double_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Bit
  bool bit_val_false;
  bool bit_val_true;

  ret = SQLBindCol(this->stmt, 23, SQL_C_BIT, &bit_val_false, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 24, SQL_C_BIT, &bit_val_true, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Characters
  SQLCHAR char_val[2];
  buf_len = sizeof(SQLCHAR) * 2;

  ret = SQLBindCol(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLWCHAR wchar_val[2];
  constexpr size_t wchar_size = driver::odbcabstraction::GetSqlWCharSize();
  buf_len = wchar_size * 2;

  ret = SQLBindCol(this->stmt, 26, SQL_C_WCHAR, &wchar_val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLWCHAR wvarchar_val[3];
  buf_len = wchar_size * 3;

  ret = SQLBindCol(this->stmt, 27, SQL_C_WCHAR, &wvarchar_val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLCHAR varchar_val[4];
  buf_len = sizeof(SQLCHAR) * 4;

  ret = SQLBindCol(this->stmt, 28, SQL_C_CHAR, &varchar_val, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Date and Timestamp
  SQL_DATE_STRUCT date_val_min{}, date_val_max{};
  buf_len = 0;

  ret = SQLBindCol(this->stmt, 29, SQL_C_TYPE_DATE, &date_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 30, SQL_C_TYPE_DATE, &date_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQL_TIMESTAMP_STRUCT timestamp_val_min{}, timestamp_val_max{};

  ret =
      SQLBindCol(this->stmt, 31, SQL_C_TYPE_TIMESTAMP, &timestamp_val_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret =
      SQLBindCol(this->stmt, 32, SQL_C_TYPE_TIMESTAMP, &timestamp_val_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Data verification

  // Signed Tiny Int
  EXPECT_EQ(stiny_int_val_min, std::numeric_limits<int8_t>::min());
  EXPECT_EQ(stiny_int_val_max, std::numeric_limits<int8_t>::max());

  // Unsigned Tiny Int
  EXPECT_EQ(utiny_int_val_min, std::numeric_limits<uint8_t>::min());
  EXPECT_EQ(utiny_int_val_max, std::numeric_limits<uint8_t>::max());

  // Signed Small Int
  EXPECT_EQ(ssmall_int_val_min, std::numeric_limits<int16_t>::min());
  EXPECT_EQ(ssmall_int_val_max, std::numeric_limits<int16_t>::max());

  // Unsigned Small Int
  EXPECT_EQ(usmall_int_val_min, std::numeric_limits<uint16_t>::min());
  EXPECT_EQ(usmall_int_val_max, std::numeric_limits<uint16_t>::max());

  // Signed Long
  EXPECT_EQ(slong_val_min, std::numeric_limits<SQLINTEGER>::min());
  EXPECT_EQ(slong_val_max, std::numeric_limits<SQLINTEGER>::max());

  // Unsigned Long
  EXPECT_EQ(ulong_val_min, std::numeric_limits<SQLUINTEGER>::min());
  EXPECT_EQ(ulong_val_max, std::numeric_limits<SQLUINTEGER>::max());

  // Signed Big Int
  EXPECT_EQ(sbig_int_val_min, std::numeric_limits<SQLBIGINT>::min());
  EXPECT_EQ(sbig_int_val_max, std::numeric_limits<SQLBIGINT>::max());

  // Unsigned Big Int
  EXPECT_EQ(ubig_int_val_min, std::numeric_limits<SQLUBIGINT>::min());
  EXPECT_EQ(ubig_int_val_max, std::numeric_limits<SQLUBIGINT>::max());

  // Decimal
  EXPECT_EQ(decimal_val_neg.sign, 0);
  EXPECT_EQ(decimal_val_neg.scale, 0);
  EXPECT_EQ(decimal_val_neg.precision, 38);
  EXPECT_THAT(decimal_val_neg.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0));

  EXPECT_EQ(decimal_val_pos.sign, 1);
  EXPECT_EQ(decimal_val_pos.scale, 0);
  EXPECT_EQ(decimal_val_pos.precision, 38);
  EXPECT_THAT(decimal_val_pos.val, ::testing::ElementsAre(0xFF, 0xC9, 0x9A, 0x3B, 0, 0, 0,
                                                          0, 0, 0, 0, 0, 0, 0, 0, 0));

  // Float
  EXPECT_EQ(float_val_min, -std::numeric_limits<float>::max());
  EXPECT_EQ(float_val_max, std::numeric_limits<float>::max());

  // Double
  EXPECT_EQ(double_val_min, -std::numeric_limits<SQLDOUBLE>::max());
  EXPECT_EQ(double_val_max, std::numeric_limits<SQLDOUBLE>::max());

  // Bit
  EXPECT_EQ(bit_val_false, false);
  EXPECT_EQ(bit_val_true, true);

  // Characters
  EXPECT_EQ(char_val[0], 'Z');
  EXPECT_EQ(wchar_val[0], L'你');
  EXPECT_EQ(wvarchar_val[0], L'你');
  EXPECT_EQ(wvarchar_val[1], L'好');

  EXPECT_EQ(varchar_val[0], 'X');
  EXPECT_EQ(varchar_val[1], 'Y');
  EXPECT_EQ(varchar_val[2], 'Z');

  // Date
  EXPECT_EQ(date_val_min.day, 1);
  EXPECT_EQ(date_val_min.month, 1);
  EXPECT_EQ(date_val_min.year, 1400);

  EXPECT_EQ(date_val_max.day, 31);
  EXPECT_EQ(date_val_max.month, 12);
  EXPECT_EQ(date_val_max.year, 9999);

  // Timestamp
  EXPECT_EQ(timestamp_val_min.day, 1);
  EXPECT_EQ(timestamp_val_min.month, 1);
  EXPECT_EQ(timestamp_val_min.year, 1400);
  EXPECT_EQ(timestamp_val_min.hour, 0);
  EXPECT_EQ(timestamp_val_min.minute, 0);
  EXPECT_EQ(timestamp_val_min.second, 0);
  EXPECT_EQ(timestamp_val_min.fraction, 0);

  EXPECT_EQ(timestamp_val_max.day, 31);
  EXPECT_EQ(timestamp_val_max.month, 12);
  EXPECT_EQ(timestamp_val_max.year, 9999);
  EXPECT_EQ(timestamp_val_max.hour, 23);
  EXPECT_EQ(timestamp_val_max.minute, 59);
  EXPECT_EQ(timestamp_val_max.second, 59);
  EXPECT_EQ(timestamp_val_max.fraction, 0);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLBindColTimeQuery) {
  // Mock server test is skipped due to limitation on the mock server.
  // Time type from mock server does not include the fraction
  this->connect();

  SQL_TIME_STRUCT time_var_min{};
  SQL_TIME_STRUCT time_var_max{};
  SQLLEN buf_len = sizeof(time_var_min);
  SQLLEN ind;

  SQLRETURN ret =
      SQLBindCol(this->stmt, 1, SQL_C_TYPE_TIME, &time_var_min, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLBindCol(this->stmt, 2, SQL_C_TYPE_TIME, &time_var_max, buf_len, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring wsql =
      LR"(
    SELECT CAST(TIME '00:00:00' AS TIME) AS time_min,
           CAST(TIME '23:59:59' AS TIME) AS time_max;
    )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check min values for time.
  EXPECT_EQ(time_var_min.hour, 0);
  EXPECT_EQ(time_var_min.minute, 0);
  EXPECT_EQ(time_var_min.second, 0);

  // Check max values for time.
  EXPECT_EQ(time_var_max.hour, 23);
  EXPECT_EQ(time_var_max.minute, 59);
  EXPECT_EQ(time_var_max.second, 59);

  this->disconnect();
}

TEST_F(FlightSQLODBCMockTestBase, TestSQLBindColVarbinaryQuery) {
  // Have binary test on mock test base as remote test servers tend to have different
  // formats for binary data
  this->connect();

  // varbinary
  std::vector<int8_t> varbinary_val(3);
  SQLLEN buf_len = varbinary_val.size();
  SQLLEN ind;
  SQLRETURN ret =
      SQLBindCol(this->stmt, 1, SQL_C_BINARY, &varbinary_val[0], buf_len, &ind);

  std::wstring wsql = L"SELECT X'ABCDEF' AS c_varbinary;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check varbinary values
  EXPECT_EQ(varbinary_val[0], '\xAB');
  EXPECT_EQ(varbinary_val[1], '\xCD');
  EXPECT_EQ(varbinary_val[2], '\xEF');

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLBindColNullQuery) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  this->connect();

  SQLINTEGER val;
  SQLLEN ind;

  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, &ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify SQL_NULL_DATA is returned for indicator
  EXPECT_EQ(ind, SQL_NULL_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLBindColNullQueryNullIndicator) {
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  this->connect();

  SQLINTEGER val;

  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);

  EXPECT_EQ(ret, SQL_ERROR);
  // Verify invalid null indicator is reported, as it is required
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_22002);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLBindColRowFetching) {
  this->connect();

  SQLINTEGER val;
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind;

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, buf_len, &ind);

  std::wstring wsql =
      LR"(
    SELECT 1 AS small_table
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch row 1
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 1 is returned
  EXPECT_EQ(val, 1);

  // Fetch row 2
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 2 is returned
  EXPECT_EQ(val, 2);

  // Fetch row 3
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 3 is returned
  EXPECT_EQ(val, 3);

  // Verify result set has no more data beyond row 3
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLBindColRowArraySize) {
  // Set SQL_ATTR_ROW_ARRAY_SIZE to fetch 3 rows at once
  this->connect();

  constexpr SQLULEN rows = 3;
  SQLINTEGER val[rows];
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind[rows];

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, val, buf_len, ind);

  SQLLEN rows_fetched;
  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_ROWS_FETCHED_PTR, &rows_fetched, 0);

  std::wstring wsql =
      LR"(
    SELECT 1 AS small_table
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_ROW_ARRAY_SIZE,
                       reinterpret_cast<SQLPOINTER>(rows), 0);

  // Fetch 3 rows at once
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify 3 rows are fetched
  EXPECT_EQ(rows_fetched, 3);

  // Verify 1 is returned
  EXPECT_EQ(val[0], 1);
  // Verify 2 is returned
  EXPECT_EQ(val[1], 2);
  // Verify 3 is returned
  EXPECT_EQ(val[2], 3);

  // Verify result set has no more data beyond row 3
  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLBindColIndicatorOnly) {
  // GH-47021: implement driver to return indicator value when data pointer is null
  GTEST_SKIP();
  // Verify driver supports null data pointer with valid indicator pointer
  this->connect();

  // Numeric Types

  // Signed Tiny Int
  SQLLEN stiny_int_ind;

  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_STINYINT, 0, 0, &stiny_int_ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Characters
  SQLLEN buf_len = sizeof(SQLCHAR) * 2;
  SQLLEN char_val_ind;

  ret = SQLBindCol(this->stmt, 25, SQL_C_CHAR, 0, buf_len, &char_val_ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify values for indicator pointer
  // Signed Tiny Int
  EXPECT_EQ(stiny_int_ind, 1);

  // Char array
  EXPECT_EQ(char_val_ind, 1);
  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLBindColIndicatorOnlySQLUnbind) {
  // Verify driver supports valid indicator pointer after unbinding all columns
  this->connect();

  // Numeric Types

  // Signed Tiny Int
  int8_t stiny_int_val;
  SQLLEN stiny_int_ind;

  SQLRETURN ret =
      SQLBindCol(this->stmt, 1, SQL_C_STINYINT, &stiny_int_val, 0, &stiny_int_ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Characters
  SQLCHAR char_val[2];
  SQLLEN buf_len = sizeof(SQLCHAR) * 2;
  SQLLEN char_val_ind;

  ret = SQLBindCol(this->stmt, 25, SQL_C_CHAR, &char_val, buf_len, &char_val_ind);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Driver should still be able to execute queries after unbinding columns
  ret = SQLFreeStmt(this->stmt, SQL_UNBIND);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Execute query and fetch data once since there is only 1 row.
  std::wstring wsql = this->getQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFetch(this->stmt);
  EXPECT_EQ(ret, SQL_SUCCESS);

  // GH-47021: implement driver to return indicator value when data pointer is null and
  // uncomment the checks Verify values for indicator pointer Signed Tiny Int
  // EXPECT_EQ(stiny_int_ind, 1);

  // Char array
  // EXPECT_EQ(char_val_ind, 1);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLExtendedFetchRowFetching) {
  // Set SQL_ROWSET_SIZE to fetch 3 rows at once
  this->connect();

  constexpr SQLULEN rows = 3;
  SQLINTEGER val[rows];
  SQLLEN buf_len = sizeof(val);
  SQLLEN ind[rows];

  // Same variable will be used for column 1, the value of `val`
  // should be updated after every SQLFetch call.
  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, val, buf_len, ind);

  ret =
      SQLSetStmtAttr(this->stmt, SQL_ROWSET_SIZE, reinterpret_cast<SQLPOINTER>(rows), 0);

  std::wstring wsql =
      LR"(
    SELECT 1 AS small_table
    UNION ALL
    SELECT 2
    UNION ALL
    SELECT 3;
  )";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  // Fetch row 1-3.
  SQLULEN row_count;
  SQLUSMALLINT row_status[rows];

  ret = SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count, row_status);
  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(row_count, 3);

  for (int i = 0; i < rows; i++) {
    EXPECT_EQ(row_status[i], SQL_SUCCESS);
  }

  // Verify 1 is returned for row 1
  EXPECT_EQ(val[0], 1);
  // Verify 2 is returned for row 2
  EXPECT_EQ(val[1], 2);
  // Verify 3 is returned for row 3
  EXPECT_EQ(val[2], 3);

  // Verify result set has no more data beyond row 3
  SQLULEN row_count2;
  SQLUSMALLINT row_status2[rows];
  ret = SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count2, row_status2);
  EXPECT_EQ(ret, SQL_NO_DATA);

  this->disconnect();
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLExtendedFetchQueryNullIndicator) {
  // GH-47110: SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002
  // Limitation on mock test server prevents null from working properly, so use remote
  // server instead. Mock server has type `DENSE_UNION` for null column data.
  GTEST_SKIP();
  this->connect();

  SQLINTEGER val;

  SQLRETURN ret = SQLBindCol(this->stmt, 1, SQL_C_LONG, &val, 0, 0);

  std::wstring wsql = L"SELECT null as null_col;";
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ret = SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size()));
  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLULEN row_count1;
  SQLUSMALLINT row_status1[1];

  // SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for 22002 state
  ret = SQLExtendedFetch(this->stmt, SQL_FETCH_NEXT, 0, &row_count1, row_status1);
  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);
  VerifyOdbcErrorState(SQL_HANDLE_STMT, this->stmt, error_state_22002);

  this->disconnect();
}
}  // namespace arrow::flight::sql::odbc
