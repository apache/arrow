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
class ConnectionAttributeTest : public T {};

using TestTypes =
    ::testing::Types<FlightSQLODBCMockTestBase, FlightSQLODBCRemoteTestBase>;
TYPED_TEST_SUITE(ConnectionAttributeTest, TestTypes);

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAsyncDbcEventUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, 0, 0));
  // Driver Manager on Windows returns error code HY118
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY118);
}
#endif

#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAyncEnableUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAyncDbcPcCallbackUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAyncDbcPcContextUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}
#endif

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAutoIpdReadOnly) {
  // Verify read-only attribute cannot be set
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY092);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrConnectionDeadReadOnly) {
  // Verify read-only attribute cannot be set
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_DEAD, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY092);
}

#ifdef SQL_ATTR_DBC_INFO_TOKEN
TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrDbcInfoTokenUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}
#endif

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrEnlistInDtcUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrOdbcCursorsDMOnly) {
  this->AllocEnvConnHandles();

  // Verify DM-only attribute is settable via Driver Manager
  ASSERT_EQ(SQL_SUCCESS,
            SQLSetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS,
                              reinterpret_cast<SQLPOINTER>(SQL_CUR_USE_DRIVER), 0));

  std::string connect_str = this->GetConnectionString();
  this->ConnectWithString(connect_str);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrQuietModeReadOnly) {
  // Verify read-only attribute cannot be set
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY092);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrTraceDMOnly) {
  // Verify DM-only attribute is settable via Driver Manager
  ASSERT_EQ(SQL_SUCCESS,
            SQLSetConnectAttr(this->conn, SQL_ATTR_TRACE,
                              reinterpret_cast<SQLPOINTER>(SQL_OPT_TRACE_OFF), 0));
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrTracefileDMOnly) {
  // Verify DM-only attribute is handled by Driver Manager

  // Use placeholder value as we want the call to fail, or else
  // the driver manager will produce a trace file.
  std::wstring trace_file = L"invalid/file/path";
  std::vector<SQLWCHAR> trace_file0(trace_file.begin(), trace_file.end());
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, &trace_file0[0],
                                         static_cast<SQLINTEGER>(trace_file0.size())));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY000);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrTranslateLabDMOnly) {
  // Verify DM-only attribute is handled by Driver Manager
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, 0, 0));
  // Checks for invalid argument return error
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY024);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrTranslateOptionUnsupported) {
  ASSERT_EQ(SQL_ERROR, SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrTxnIsolationUnsupported) {
  ASSERT_EQ(SQL_ERROR,
            SQLSetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION,
                              reinterpret_cast<SQLPOINTER>(SQL_TXN_READ_UNCOMMITTED), 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

#ifdef SQL_ATTR_DBC_INFO_TOKEN
TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrDbcInfoTokenSetOnly) {
  // Verify that set-only attribute cannot be read
  SQLPOINTER ptr = NULL;
  ASSERT_EQ(SQL_ERROR, SQLGetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, ptr, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY092);
}
#endif

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrOdbcCursorsDMOnly) {
  // Verify that DM-only attribute is handled by driver manager
  SQLULEN cursor_attr;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS, &cursor_attr, 0, 0));
  EXPECT_EQ(SQL_CUR_USE_DRIVER, cursor_attr);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrTraceDMOnly) {
  // Verify that DM-only attribute is handled by driver manager
  SQLUINTEGER trace;
  ASSERT_EQ(SQL_SUCCESS, SQLGetConnectAttr(this->conn, SQL_ATTR_TRACE, &trace, 0, 0));
  EXPECT_EQ(SQL_OPT_TRACE_OFF, trace);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrTraceFileDMOnly) {
  // Verify that DM-only attribute is handled by driver manager
  SQLWCHAR out_str[kOdbcBufferSize];
  SQLINTEGER out_str_len;
  ASSERT_EQ(SQL_SUCCESS, SQLGetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, out_str,
                                           kOdbcBufferSize, &out_str_len));
  // Length is returned in bytes for SQLGetConnectAttr,
  // we want the number of characters
  out_str_len /= GetSqlWCharSize();
  std::string out_connection_string =
      ODBC::SqlWcharToString(out_str, static_cast<SQLSMALLINT>(out_str_len));
  EXPECT_FALSE(out_connection_string.empty());
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrTranslateLibUnsupported) {
  SQLWCHAR out_str[kOdbcBufferSize];
  SQLINTEGER out_str_len;
  ASSERT_EQ(SQL_ERROR, SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, out_str,
                                         kOdbcBufferSize, &out_str_len));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrTranslateOptionUnsupported) {
  SQLINTEGER option;
  ASSERT_EQ(SQL_ERROR,
            SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, &option, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrTxnIsolationUnsupported) {
  SQLINTEGER isolation;
  ASSERT_EQ(SQL_ERROR,
            SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, &isolation, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHYC00);
}

#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
TYPED_TEST(ConnectionAttributeTest,
           TestSQLGetConnectAttrAsyncDbcFunctionsEnableUnsupported) {
  // Verifies that the Windows driver manager returns HY114 for unsupported functionality
  SQLUINTEGER enable;
  ASSERT_EQ(SQL_ERROR, SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE,
                                         &enable, 0, 0));
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorStateHY114);
}
#endif

// Tests for supported attributes

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAsyncDbcEventDefault) {
  SQLPOINTER ptr = NULL;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, ptr, 0, 0));
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAsyncDbcPcallbackDefault) {
  SQLPOINTER ptr = NULL;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, ptr, 0, 0));
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAsyncDbcPcontextDefault) {
  SQLPOINTER ptr = NULL;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, ptr, 0, 0));
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);
}
#endif

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAsyncEnableDefault) {
  SQLULEN enable;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, &enable, 0, 0));
  EXPECT_EQ(SQL_ASYNC_ENABLE_OFF, enable);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAutoIpdDefault) {
  SQLUINTEGER ipd;
  ASSERT_EQ(SQL_SUCCESS, SQLGetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, &ipd, 0, 0));
  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_FALSE), ipd);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrAutocommitDefault) {
  SQLUINTEGER auto_commit;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_AUTOCOMMIT, &auto_commit, 0, 0));
  EXPECT_EQ(SQL_AUTOCOMMIT_ON, auto_commit);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrEnlistInDtcDefault) {
  SQLPOINTER ptr = NULL;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, ptr, 0, 0));
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLGetConnectAttrQuietModeDefault) {
  HWND ptr = NULL;
  ASSERT_EQ(SQL_SUCCESS, SQLGetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, ptr, 0, 0));
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrAccessModeValid) {
  // The driver always returns SQL_MODE_READ_WRITE

  // Check default value first
  SQLUINTEGER mode = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0));
  EXPECT_EQ(SQL_MODE_READ_WRITE, mode);

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                              reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0));

  mode = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0));
  EXPECT_EQ(SQL_MODE_READ_WRITE, mode);

  // Attempt to set to SQL_MODE_READ_ONLY, driver should return warning and not error
  EXPECT_EQ(SQL_SUCCESS_WITH_INFO,
            SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                              reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_ONLY), 0));

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorState01S02);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrConnectionTimeoutValid) {
  // Check default value first
  SQLUINTEGER timeout = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0));
  EXPECT_EQ(0, timeout);

  ASSERT_EQ(SQL_SUCCESS, SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT,
                                           reinterpret_cast<SQLPOINTER>(42), 0));

  timeout = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0));
  EXPECT_EQ(42, timeout);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrLoginTimeoutValid) {
  // Check default value first
  SQLUINTEGER timeout = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0));
  EXPECT_EQ(0, timeout);

  ASSERT_EQ(SQL_SUCCESS, SQLSetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT,
                                           reinterpret_cast<SQLPOINTER>(42), 0));

  timeout = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0));
  EXPECT_EQ(42, timeout);
}

TYPED_TEST(ConnectionAttributeTest, TestSQLSetConnectAttrPacketSizeValid) {
  // The driver always returns 0. PACKET_SIZE value is unused by the driver.

  // Check default value first
  SQLUINTEGER size = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0));
  EXPECT_EQ(0, size);

  ASSERT_EQ(SQL_SUCCESS, SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                                           reinterpret_cast<SQLPOINTER>(0), 0));

  size = -1;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0));
  EXPECT_EQ(0, size);

  // Attempt to set to non-zero value, driver should return warning and not error
  EXPECT_EQ(SQL_SUCCESS_WITH_INFO, SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                                                     reinterpret_cast<SQLPOINTER>(2), 0));

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, kErrorState01S02);
}

}  // namespace arrow::flight::sql::odbc
