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

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAsyncDbcEventUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  // Driver Manager on Windows returns error code HY118
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY118);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncEnableUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_ENABLE
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcCallbackUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAyncDbcPcContextUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAutoIpdReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionDeadReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_DEAD, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrDbcInfoTokenUnsupported) {
  this->connect();

#ifdef SQL_ATTR_DBC_INFO_TOKEN
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrEnlistInDtcUnsupported) {
  this->connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrOdbcCursorsDMOnly) {
  this->allocEnvConnHandles();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS,
                                    reinterpret_cast<SQLPOINTER>(SQL_CUR_USE_DRIVER), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  std::string connect_str = this->getConnectionString();
  this->connectWithString(connect_str);
  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrQuietModeReadOnly) {
  this->connect();

  // Verify read-only attribute cannot be set
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTraceDMOnly) {
  this->connect();

  // Verify DM-only attribute is settable via Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACE,
                                    reinterpret_cast<SQLPOINTER>(SQL_OPT_TRACE_OFF), 0);
  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTracefileDMOnly) {
  this->connect();

  // Verify DM-only attribute is handled by Driver Manager

  // Use placeholder value as we want the call to fail, or else
  // the driver manager will produce a trace file.
  std::wstring trace_file = L"invalid/file/path";
  std::vector<SQLWCHAR> trace_file0(trace_file.begin(), trace_file.end());
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, &trace_file0[0],
                                    static_cast<SQLINTEGER>(trace_file0.size()));
  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY000);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateLabDMOnly) {
  this->connect();

  // Verify DM-only attribute is handled by Driver Manager
  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, 0, 0);
  EXPECT_EQ(ret, SQL_ERROR);
  // Checks for invalid argument return error
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY024);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTranslateOptionUnsupported) {
  this->connect();

  SQLRETURN ret = SQLSetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrTxnIsolationUnsupported) {
  this->connect();

  SQLRETURN ret =
      SQLSetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION,
                        reinterpret_cast<SQLPOINTER>(SQL_TXN_READ_UNCOMMITTED), 0);
  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrDbcInfoTokenSetOnly) {
  this->connect();

#ifdef SQL_ATTR_DBC_INFO_TOKEN
  // Verify that set-only attribute cannot be read
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_DBC_INFO_TOKEN, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY092);
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrOdbcCursorsDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLULEN cursor_attr;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ODBC_CURSORS, &cursor_attr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(cursor_attr, SQL_CUR_USE_DRIVER);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLUINTEGER trace;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACE, &trace, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(trace, SQL_OPT_TRACE_OFF);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTraceFileDMOnly) {
  this->connect();

  // Verify that DM-only attribute is handled by driver manager
  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLINTEGER outstrlen;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRACEFILE, outstr,
                                    ODBC_BUFFER_SIZE, &outstrlen);

  EXPECT_EQ(ret, SQL_SUCCESS);
  // Length is returned in bytes for SQLGetConnectAttr,
  // we want the number of characters
  outstrlen /= driver::odbcabstraction::GetSqlWCharSize();
  std::string out_connection_string =
      ODBC::SqlWcharToString(outstr, static_cast<SQLSMALLINT>(outstrlen));
  EXPECT_TRUE(!out_connection_string.empty());

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateLibUnsupported) {
  this->connect();

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLINTEGER outstrlen;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_LIB, outstr,
                                    ODBC_BUFFER_SIZE, &outstrlen);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTranslateOptionUnsupported) {
  this->connect();

  SQLINTEGER option;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TRANSLATE_OPTION, &option, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrTxnIsolationUnsupported) {
  this->connect();

  SQLINTEGER isolation;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_TXN_ISOLATION, &isolation, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HYC00);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase,
           TestSQLGetConnectAttrAsyncDbcFunctionsEnableUnsupported) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
  // Verifies that the Windows driver manager returns HY114 for unsupported functionality
  SQLUINTEGER enable;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE, &enable, 0, 0);

  EXPECT_EQ(ret, SQL_ERROR);
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_HY114);
#endif

  this->disconnect();
}

// Tests for supported attributes

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcEventDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_EVENT
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_EVENT, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcallbackDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCALLBACK, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncDbcPcontextDefault) {
  this->connect();

#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_DBC_PCONTEXT, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));
#endif

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAsyncEnableDefault) {
  this->connect();

  SQLULEN enable;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ASYNC_ENABLE, &enable, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(enable, SQL_ASYNC_ENABLE_OFF);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutoIpdDefault) {
  this->connect();

  SQLUINTEGER ipd;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTO_IPD, &ipd, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ipd, static_cast<SQLUINTEGER>(SQL_FALSE));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrAutocommitDefault) {
  this->connect();

  SQLUINTEGER auto_commit;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_AUTOCOMMIT, &auto_commit, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(auto_commit, SQL_AUTOCOMMIT_ON);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrEnlistInDtcDefault) {
  this->connect();

  SQLPOINTER ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ENLIST_IN_DTC, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetConnectAttrQuietModeDefault) {
  this->connect();

  HWND ptr = NULL;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_QUIET_MODE, ptr, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(ptr, reinterpret_cast<SQLPOINTER>(NULL));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrAccessModeValid) {
  this->connect();

  // The driver always returns SQL_MODE_READ_WRITE

  // Check default value first
  SQLUINTEGER mode = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(mode, SQL_MODE_READ_WRITE);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_WRITE), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  mode = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE, &mode, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(mode, SQL_MODE_READ_WRITE);

  // Attempt to set to SQL_MODE_READ_ONLY, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_ACCESS_MODE,
                          reinterpret_cast<SQLPOINTER>(SQL_MODE_READ_ONLY), 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrConnectionTimeoutValid) {
  this->connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret =
      SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 42);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrLoginTimeoutValid) {
  this->connect();

  // Check default value first
  SQLUINTEGER timeout = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT,
                          reinterpret_cast<SQLPOINTER>(42), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  timeout = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(timeout, 42);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetConnectAttrPacketSizeValid) {
  this->connect();

  // The driver always returns 0. PACKET_SIZE value is unused by the driver.

  // Check default value first
  SQLUINTEGER size = -1;
  SQLRETURN ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(size, 0);

  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(0), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  size = -1;

  ret = SQLGetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE, &size, 0, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);
  EXPECT_EQ(size, 0);

  // Attempt to set to non-zero value, driver should return warning and not error
  ret = SQLSetConnectAttr(this->conn, SQL_ATTR_PACKET_SIZE,
                          reinterpret_cast<SQLPOINTER>(2), 0);

  EXPECT_EQ(ret, SQL_SUCCESS_WITH_INFO);

  // Verify warning status
  VerifyOdbcErrorState(SQL_HANDLE_DBC, this->conn, error_state_01S02);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc
