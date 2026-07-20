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

// These tests run against the remote server only. The mock SQLite server
// reports SQL_WVARCHAR metadata for `SELECT ... AS` result columns (see
// ColumnsMockTest in columns_test.cc), so it cannot exercise the numeric or
// datetime branches of SQLDescribeCol that this fix touches.
class DescribeColRemoteTest : public FlightSQLODBCRemoteTestBase {};

namespace {
// Sentinel used to pre-fill the SQLULEN column_size output. If SQLDescribeCol
// only writes the low bytes (the width bug this test guards against), the upper
// bytes remain set to this pattern and the value is far outside any legal
// column size.
constexpr SQLULEN kColumnSizeSentinel = static_cast<SQLULEN>(0xFFFFFFFFFFFFFFFFULL);

// Column ordinals in the all-data-types query (see
// ODBCTestBase::GetQueryAllDataTypes). Decimal and timestamp exercise the
// numeric-precision and datetime-precision code paths in SQLDescribeCol.
constexpr SQLUSMALLINT kDecimalColumn = 17;
constexpr SQLUSMALLINT kTimestampColumn = 31;
}  // namespace

// Verify that SQLDescribeCol fully initializes the SQLULEN column_size output
// for a DECIMAL column. Prior to GH-50560 the numeric path read
// SQL_DESC_PRECISION (a SQLSMALLINT) straight into the SQLULEN* output, writing
// only 2 of the 8 bytes and leaving the upper 6 bytes as uninitialized garbage.
TEST_F(DescribeColRemoteTest, TestSQLDescribeColDecimalColumnSizeIsFullyWritten) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR column_name[kOdbcBufferSize];
  SQLSMALLINT name_length = 0;
  SQLSMALLINT data_type = 0;
  SQLULEN column_size = kColumnSizeSentinel;
  SQLSMALLINT decimal_digits = -1;
  SQLSMALLINT nullable = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLDescribeCol(this->stmt, kDecimalColumn, column_name,
                           sizeof(column_name) / sizeof(SQLWCHAR), &name_length,
                           &data_type, &column_size, &decimal_digits, &nullable));

  EXPECT_EQ(SQL_DECIMAL, data_type);

  // The precision (column size) is a small positive integer. If only the low
  // bytes were written, the sentinel's upper bytes would remain set and the
  // full 8-byte value would be enormous. Checking the whole SQLULEN (not just a
  // truncated view of it) is what catches the short write.
  EXPECT_NE(kColumnSizeSentinel, column_size);
  EXPECT_GT(column_size, 0u);
  EXPECT_LT(column_size, 100u) << "column_size upper bytes look uninitialized: 0x"
                               << std::hex << column_size;

  // decimal_digits (scale) is a SQLSMALLINT output; it must be a sane scale.
  EXPECT_GE(decimal_digits, 0);
  EXPECT_LE(static_cast<SQLULEN>(decimal_digits), column_size);
}

// Verify SQLDescribeCol reports a fully-written column_size for a TIMESTAMP
// column. This exercises the datetime branch, which reports the display size
// via SQL_DESC_LENGTH (a SQLULEN) and the seconds precision via
// SQL_DESC_PRECISION into the SQLSMALLINT decimal_digits output.
TEST_F(DescribeColRemoteTest, TestSQLDescribeColTimestampColumnSizeIsFullyWritten) {
  std::wstring wsql = this->GetQueryAllDataTypes();
  std::vector<SQLWCHAR> sql0(wsql.begin(), wsql.end());

  ASSERT_EQ(SQL_SUCCESS,
            SQLExecDirect(this->stmt, &sql0[0], static_cast<SQLINTEGER>(sql0.size())));

  SQLWCHAR column_name[kOdbcBufferSize];
  SQLSMALLINT name_length = 0;
  SQLSMALLINT data_type = 0;
  SQLULEN column_size = kColumnSizeSentinel;
  SQLSMALLINT decimal_digits = -1;
  SQLSMALLINT nullable = 0;

  ASSERT_EQ(SQL_SUCCESS,
            SQLDescribeCol(this->stmt, kTimestampColumn, column_name,
                           sizeof(column_name) / sizeof(SQLWCHAR), &name_length,
                           &data_type, &column_size, &decimal_digits, &nullable));

  EXPECT_EQ(SQL_TYPE_TIMESTAMP, data_type);

  // The timestamp display size is a small integer (e.g. 19 or 23). A short
  // write would leave the sentinel's upper bytes intact.
  EXPECT_NE(kColumnSizeSentinel, column_size);
  EXPECT_GT(column_size, 0u);
  EXPECT_LT(column_size, 100u) << "column_size upper bytes look uninitialized: 0x"
                               << std::hex << column_size;

  // Seconds precision is a small non-negative scale.
  EXPECT_GE(decimal_digits, 0);
  EXPECT_LE(decimal_digits, 9);
}

}  // namespace arrow::flight::sql::odbc
