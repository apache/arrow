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

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {
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

TEST_F(FlightSQLODBCTestBase, TestSQLGetEnvAttrOutputNTS) {
  connect();

  SQLINTEGER output_nts;

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(output_nts, SQL_TRUE);

  disconnect();
}

TEST_F(FlightSQLODBCTestBase, TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentionally used on macOS/Linux
  GTEST_SKIP();

  connect();

  SQLINTEGER length;

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length);

  EXPECT_TRUE(return_get == SQL_SUCCESS);

  EXPECT_EQ(length, sizeof(SQLINTEGER));

  disconnect();
}

TEST_F(FlightSQLODBCTestBase, TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentionally used on macOS/Linux
  GTEST_SKIP();
  connect();

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr);

  EXPECT_TRUE(return_get == SQL_ERROR);

  disconnect();
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

TEST(SQLDriverConnect, TestSQLDriverConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
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

  EXPECT_TRUE(ret == SQL_SUCCESS);

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

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLDriverConnect, TestSQLDriverConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, std::string("28000"));

  // TODO: Check that outstr remains empty after SqlWcharToString
  // is fixed to handle empty `outstr`
  // std::string out_connection_string = ODBC::SqlWcharToString(outstr, outstrlen);
  // EXPECT_TRUE(out_connection_string.empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLConnect, TestSQLConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));

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

  // Connecting to ODBC server.
  ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()), uid0.data(),
                   static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                   static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(dsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLConnect, TestSQLConnectInputUidPwd) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));

  // Retrieve valid uid and pwd
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

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(dsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLConnect, TestSQLConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));

  // Retrieve valid uid and pwd
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

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, std::string("28000"));

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(dsn));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLConnect, TestSQLConnectDSNPrecedence) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));

  // Write connection string content into a DSN,
  // must succeed before continuing

  // Pass incorrect uid and password to SQLConnect, they will be ignored
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

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(dsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLDisconnect, TestSQLDisconnectWithoutConnection) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Attempt to disconnect without a connection, expect to fail
  ret = SQLDisconnect(conn);

  EXPECT_TRUE(ret == SQL_ERROR);

  // Expect ODBC driver manager to return error state
  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, std::string("08003"));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLGetDiagFieldW, TestSQLGetDiagFieldWForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_NUMBER
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, HEADER_LEVEL, SQL_DIAG_NUMBER, &diag_number,
                        sizeof(SQLINTEGER), &diag_number_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  EXPECT_EQ(diag_number, 1);

  // SQL_DIAG_SERVER_NAME
  SQLWCHAR server_name[ODBC_BUFFER_SIZE];
  SQLSMALLINT server_name_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SERVER_NAME, server_name,
                        ODBC_BUFFER_SIZE, &server_name_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // SQL_DIAG_MESSAGE_TEXT
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, ODBC_BUFFER_SIZE, &message_text_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // SQL_DIAG_NATIVE
  SQLINTEGER diag_native;
  SQLSMALLINT diag_native_length;

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_NATIVE, &diag_native,
                        sizeof(diag_native), &diag_native_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  EXPECT_EQ(diag_native, 200);

  // SQL_DIAG_SQLSTATE
  const SQLSMALLINT sql_state_size = 6;
  SQLWCHAR sql_state[sql_state_size];
  SQLSMALLINT sql_state_length;
  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_SQLSTATE, sql_state,
                        sql_state_size * driver::odbcabstraction::GetSqlWCharSize(),
                        &sql_state_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // 28000
  EXPECT_EQ(sql_state[0], '2');
  EXPECT_EQ(sql_state[1], '8');
  EXPECT_EQ(sql_state[2], '0');
  EXPECT_EQ(sql_state[3], '0');
  EXPECT_EQ(sql_state[4], '0');

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLGetDiagFieldW, TestSQLGetDiagFieldWForConnectFailureNTS) {
  // Test is disabled because driver manager on Windows does not pass through SQL_NTS
  // This test case can be potentionally used on macOS/Linux
  GTEST_SKIP();
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

  // Retrieve all supported header level and record level data
  SQLSMALLINT HEADER_LEVEL = 0;
  SQLSMALLINT RECORD_1 = 1;

  // SQL_DIAG_MESSAGE_TEXT SQL_NTS
  SQLWCHAR message_text[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_text_length;

  message_text[ODBC_BUFFER_SIZE - 1] = '\0';

  ret = SQLGetDiagField(SQL_HANDLE_DBC, conn, RECORD_1, SQL_DIAG_MESSAGE_TEXT,
                        message_text, SQL_NTS, &message_text_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  EXPECT_GT(message_text_length, 100);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

TEST(SQLGetDiagRec, TestSQLGetDiagRecForConnectFailure) {
  //  ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Connect string
  ASSERT_OK_AND_ASSIGN(std::string connect_str,
                       arrow::internal::GetEnvVar(TEST_CONNECT_STR));
  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

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

  SQLWCHAR sql_state[6];
  SQLINTEGER native_error;
  SQLWCHAR message[ODBC_BUFFER_SIZE];
  SQLSMALLINT message_length;

  ret = SQLGetDiagRec(SQL_HANDLE_DBC, conn, 1, sql_state, &native_error, message,
                      ODBC_BUFFER_SIZE, &message_length);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  EXPECT_GT(message_length, 200);

  EXPECT_EQ(native_error, 200);

  // 28000
  EXPECT_EQ(sql_state[0], '2');
  EXPECT_EQ(sql_state[1], '8');
  EXPECT_EQ(sql_state[2], '0');
  EXPECT_EQ(sql_state[3], '0');
  EXPECT_EQ(sql_state[4], '0');

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_TRUE(ret == SQL_SUCCESS);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_TRUE(ret == SQL_SUCCESS);
}

}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
