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

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAsyncDbcEventUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  // Driver Manager on Windows returns error code HY118
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY118);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_ENABLE
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncEnableUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcCallbackUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcContextUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAutoIpdReadOnly) {
  this->Connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionDeadReadOnly) {
  this->Connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_DEAD, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->Disconnect();
}

#ifdef SQL_ATTR_DBC_INFO_TOKEN
TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrDbcInfoTokenUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrEnlistInDtcUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrOdbcCursorsDMOnly) {
  this->AllocEnvConnHandles();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS,
                                    reinterpret_cast<SQLPOINTER>(SQL_CUR_USE_DRIVER), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  std::string connect_str = this->GetConnectionString();
  this->ConnectWithString(connect_str);
  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrQuietModeReadOnly) {
  this->Connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTraceDMOnly) {
  this->Connect();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACE,
                                    reinterpret_cast<SQLPOINTER>(SQL_OPT_TRACE_OFF), 0);
  EXPECT_EQ(SQL_SUCCESS, ret);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTracefileDMOnly) {
  this->Connect();

  // Verify DM-only attribute is handled by Driver Manager

  // Use placeholder value as we want the call to fail, or else
  // the driver manager will produce a trace file.
  std::wstring trace_file = L"invalid/file/path";
  std::vector<SQLWCHAR> trace_file0(trace_file.begin(), trace_file.end());
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, &trace_file0[0],
                                    static_cast<SQLINTEGER>(trace_file0.size()));
  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY000);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateLabDMOnly) {
  this->Connect();

  // Verify DM-only attribute is handled by Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, 0, 0);
  EXPECT_EQ(SQL_ERROR, ret);
  // Checks for invalid argument return error
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY024);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateOptionUnsupported) {
  this->Connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTxnIsolationUnsupported) {
  this->Connect();

  SQLRETURN ret =
      SQLSetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION,
                        reinterpret_cast<SQLPOINTER>(SQL_TXN_READ_UNCOMMITTED), 0);
  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

#ifdef SQL_ATTR_DBC_INFO_TOKEN
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrDbcInfoTokenSetOnly) {
  this->Connect();

  // Verify that set-only attribute cannot be read
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, ptr, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrOdbcCursorsDMOnly) {
  this->Connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLULEN cursor_attr;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS, &cursor_attr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_CUR_USE_DRIVER, cursor_attr);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceDMOnly) {
  this->Connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLUINTEGER trace;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACE, &trace, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_OPT_TRACE_OFF, trace);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceFileDMOnly) {
  this->Connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLINTEGER out_str_len;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, out_str,
                                    ODBC_BUFFER_SIZE, &out_str_len);

  EXPECT_EQ(SQL_SUCCESS, ret);
  // Length is returned in bytes for SQLGetConnectAttr,
  // we want the number of characters
  out_str_len /= arrow::flight::sql::odbc::GetSqlWCharSize();
  std::string out_connection_string =
      ODBC::SqlWcharToString(out_str, static_cast<SQLSMALLINT>(out_str_len));
  EXPECT_TRUE(!out_connection_string.empty());

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateLibUnsupported) {
  this->Connect();

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLINTEGER out_str_len;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, out_str,
                                    ODBC_BUFFER_SIZE, &out_str_len);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateOptionUnsupported) {
  this->Connect();

  SQLINTEGER option;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, &option, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTxnIsolationUnsupported) {
  this->Connect();

  SQLINTEGER isolation;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, &isolation, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->Disconnect();
}

#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
TYPED_TEST(FlightSQLODBCTestBase,
           TestSQLGetConnectAttrAsyncDbcFunctionsEnableUnsupported) {
  this->Connect();

  // Verifies that the Windows driver manager returns HY114 for unsupported functionality
  SQLUINTEGER enable;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE, &enable, 0, 0);

  EXPECT_EQ(SQL_ERROR, ret);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY114);

  this->Disconnect();
}
#endif

// Tests for supported attributes

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcEventDefault) {
  this->Connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, ptr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcallbackDefault) {
  this->Connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, ptr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);

  this->Disconnect();
}
#endif

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcontextDefault) {
  this->Connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, ptr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);

  this->Disconnect();
}
#endif

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncEnableDefault) {
  this->Connect();

  SQLULEN enable;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, &enable, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_ASYNC_ENABLE_OFF, enable);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutoIpdDefault) {
  this->Connect();

  SQLUINTEGER ipd;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, &ipd, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(static_cast<SQLUINTEGER>(SQL_FALSE), ipd);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutocommitDefault) {
  this->Connect();

  SQLUINTEGER auto_commit;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTOCOMMIT, &auto_commit, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_AUTOCOMMIT_ON, auto_commit);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrEnlistInDtcDefault) {
  this->Connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, ptr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrQuietModeDefault) {
  this->Connect();

  HWND ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, ptr, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(reinterpret_cast<SQLPOINTER>(NULL), ptr);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAccessModeValid) {
  this->Connect();

  // The driver always returns SQL_MODE_READ_WRITE

  // Check default value first
  SQLUINTEGER mode = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_MODE_READ_WRITE, mode);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  mode = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(SQL_MODE_READ_WRITE, mode);

  // Attempt to set to SQL_MODE_READ_ONLY, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_ONLY), 0);

  EXPECT_EQ(SQL_SUCCESS_WITH_INFO, ret);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionTimeoutValid) {
  this->Connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(0, timeout);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(42, timeout);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrLoginTimeoutValid) {
  this->Connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(0, timeout);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(42, timeout);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrPacketSizeValid) {
  this->Connect();

  // The driver always returns 0. PACKET_SIZE value is unused by the driver.

  // Check default value first
  SQLUINTEGER size = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(0, size);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(0), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  size = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);
  EXPECT_EQ(0, size);

  // Attempt to set to non-zero value, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(2), 0);

  EXPECT_EQ(SQL_SUCCESS_WITH_INFO, ret);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->Disconnect();
}

}  // namespace arrow::flight::sql::odbc
