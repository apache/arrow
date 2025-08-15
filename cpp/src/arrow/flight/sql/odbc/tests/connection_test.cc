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

#include "google/protobuf/message_lite.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  EXPECT_TRUE(env != NULL);
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_connect = SQLAllocConnect(env, &conn);

  EXPECT_TRUE(return_alloc_connect == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeEnv(env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_TRUE(return_value == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free handle
  SQLRETURN return_free_handle = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(return_free_handle == SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestFreeNullHandles) {
  // Verifies attempt to free invalid handle does not cause segfault
  // Attempt to free null statement handle
  SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, this->stmt);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);

  // Attempt to free null connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, this->conn);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);

  // Attempt to free null environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, this->env);

  EXPECT_EQ(ret, SQL_INVALID_HANDLE);
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(return_alloc_handle == SQL_SUCCESS);

  // Free the created connection using free connect
  SQLRETURN return_free_connect = SQLFreeConnect(conn);

  EXPECT_TRUE(return_free_connect == SQL_SUCCESS);
}

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  // ODBC Environment
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(version, SQL_OV_ODBC2);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_TRUE(return_set == SQL_SUCCESS);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrOutputNTS) {
  this->connect();

  SQLINTEGER output_nts;

  SQLRETURN return_get = SQLGetEnvAttr(this->env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(output_nts, SQL_TRUE);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();

  this->connect();

  SQLINTEGER length;

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(length, sizeof(SQLINTEGER));

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();
  this->connect();

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr);

  EXPECT_TRUE(return_get == SQL_ERROR);

  this->disconnect();
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to output nts to supported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_TRUE), 0);

  EXPECT_TRUE(return_set == SQL_SUCCESS);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set to output nts to unsupported false
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_FALSE), 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrNullValuePointer) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_TRUE(return_env == SQL_SUCCESS);

  // Attempt to set using bad data pointer
  SQLRETURN return_set = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0);

  EXPECT_TRUE(return_set == SQL_ERROR);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLDriverConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = this->getConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Check that outstr has same content as connect_str
  std::string out_connection_string = ODBC::SqlWcharToString(outstr, outstrlen);
  Connection::ConnPropertyMap out_properties;
  Connection::ConnPropertyMap in_properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(out_connection_string,
                                                    out_properties);
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, in_properties);
  EXPECT_TRUE(compareConnPropertyMap(out_properties, in_properties));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLDriverConnectDsn) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = this->getConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));

  // Update connection string to use DSN to connect
  connect_str = std::string("DSN=") + std::string(TEST_DSN) +
                std::string(";driver={Apache Arrow Flight SQL ODBC Driver};");
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLDriverConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Invalid connect string
  std::string connect_str = getInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE];
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_TRUE(ret == SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  std::string out_connection_string = ODBC::SqlWcharToString(outstr, outstrlen);
  EXPECT_TRUE(out_connection_string.empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = this->getConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing
  std::string uid(""), pwd("");
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server. Empty uid and pwd should be ignored.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInputUidPwd) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = getConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, properties);
  std::string uid_key("uid");
  std::string pwd_key("pwd");
  std::string uid = properties[uid_key];
  std::string pwd = properties[pwd_key];

  // Write connection string content without uid and pwd into a DSN,
  // must succeed before continuing
  properties.erase(uid_key);
  properties.erase(pwd_key);
  ASSERT_TRUE(writeDSN(properties));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = getConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::getPropertiesFromConnString(connect_str, properties);
  std::string uid = properties[std::string("uid")];
  std::string pwd = properties[std::string("pwd")];

  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

  // Write connection string content into a DSN,
  // must succeed before continuing
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  // UID specified in DSN will take precedence,
  // so connection still fails despite passing valid uid in SQLConnect call
  EXPECT_TRUE(ret == SQL_ERROR);

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectDSNPrecedence) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = getConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing

  // Pass incorrect uid and password to SQLConnect, they will be ignored.
  // Assumes TEST_CONNECT_STR contains uid and pwd
  std::string uid("non_existent_id"), pwd("non_existent_password");
  ASSERT_TRUE(writeDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));
  ASSERT_OK_AND_ASSIGN(std::wstring wuid, arrow::util::UTF8ToWideString(uid));
  ASSERT_OK_AND_ASSIGN(std::wstring wpwd, arrow::util::UTF8ToWideString(pwd));
  std::vector<SQLWCHAR> dsn0(wdsn.begin(), wdsn.end());
  std::vector<SQLWCHAR> uid0(wuid.begin(), wuid.end());
  std::vector<SQLWCHAR> pwd0(wpwd.begin(), wpwd.end());

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TEST(SQLDisconnect, TestSQLDisconnectWithoutConnection) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Attempt to disconnect without a connection, expect to fail
  ret = SQLDisconnect(conn);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Expect ODBC driver manager to return error state
  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_08003);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestConnect) {
  // Verifies connect and disconnect works on its own
  this->connect();
  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeStmt) {
  this->connect();
  SQLHSTMT statement;

  // Allocate a statement using alloc statement
  SQLRETURN ret = SQLAllocStmt(this->conn, &statement);

  EXPECT_EQ(ret, SQL_SUCCESS);

  SQLWCHAR sql_buffer[ODBC_BUFFER_SIZE] = L"SELECT 1";
  ret = SQLExecDirect(statement, sql_buffer, SQL_NTS);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Close statement handle
  ret = SQLFreeStmt(statement, SQL_CLOSE);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free statement handle
  ret = SQLFreeStmt(statement, SQL_DROP);

  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestCloseConnectionWithOpenStatement) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT statement;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Connect string
  std::string connect_str = this->getConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR outstr[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT outstrlen;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), outstr,
                         ODBC_BUFFER_SIZE, &outstrlen, SQL_DRIVER_NOPROMPT);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate a statement using alloc statement
  ret = SQLAllocStmt(conn, &statement);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Disconnect from ODBC without closing the statement first
  ret = SQLDisconnect(conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(ret, SQL_SUCCESS);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeDesc) {
  this->connect();
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Free descriptor handle
  ret = SQLFreeHandle(SQL_HANDLE_DESC, descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  this->disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrDescriptor) {
  this->connect();

  SQLHDESC apd_descriptor, ard_descriptor;

  // Allocate an APD descriptor using alloc handle
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &apd_descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Allocate an ARD descriptor using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &ard_descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Save implicitly allocated internal APD and ARD descriptor pointers
  SQLPOINTER internal_apd, internal_ard = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &internal_apd,
                       sizeof(internal_apd), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &internal_ard,
                       sizeof(internal_ard), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Set APD descriptor to explicitly allocated handle
  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                       reinterpret_cast<SQLPOINTER>(apd_descriptor), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Set ARD descriptor to explicitly allocated handle
  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                       reinterpret_cast<SQLPOINTER>(ard_descriptor), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify APD and ARD descriptors are set to explicitly allocated pointers
  SQLPOINTER value = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, apd_descriptor);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, ard_descriptor);

  // Free explicitly allocated APD and ARD descriptor handles
  ret = SQLFreeHandle(SQL_HANDLE_DESC, apd_descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  ret = SQLFreeHandle(SQL_HANDLE_DESC, ard_descriptor);

  EXPECT_EQ(ret, SQL_SUCCESS);

  // Verify APD and ARD descriptors has been reverted to implicit descriptors
  value = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, internal_apd);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(ret, SQL_SUCCESS);

  EXPECT_EQ(value, internal_ard);

  this->disconnect();
}

}  // namespace arrow::flight::sql::odbc

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
