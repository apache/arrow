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

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace arrow::flight::sql::odbc {

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  EXPECT_THAT(env, ::testing::NotNull());
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_connect = SQLAllocConnect(env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_connect);
}

TEST(SQLFreeHandle, TestSQLFreeHandleEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  SQLRETURN return_value = SQLFreeEnv(env);

  EXPECT_EQ(SQL_SUCCESS, return_value);
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_value = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_value);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);

  // Free the created connection using free handle
  SQLRETURN return_free_handle = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, return_free_handle);
}

TYPED_TEST(FlightSQLODBCTestBase, TestFreeNullHandles) {
  // Verifies attempt to free invalid handle does not cause segfault
  // Attempt to free null statement handle
  SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, this->stmt);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);

  // Attempt to free null connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, this->conn);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);

  // Attempt to free null environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, this->env);

  EXPECT_EQ(SQL_INVALID_HANDLE, ret);
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Allocate a connection using alloc handle
  SQLRETURN return_alloc_handle = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, return_alloc_handle);

  // Free the created connection using free connect
  SQLRETURN return_free_connect = SQLFreeConnect(conn);

  EXPECT_EQ(SQL_SUCCESS, return_free_connect);
}

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  // ODBC Environment
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  SQLRETURN return_get = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, return_get);

  EXPECT_EQ(SQL_OV_ODBC2, version);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC2), 0);

  EXPECT_EQ(SQL_SUCCESS, return_set);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to unsupported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0);

  EXPECT_EQ(SQL_ERROR, return_set);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrOutputNTS) {
  this->Connect();

  SQLINTEGER output_nts;

  SQLRETURN return_get = SQLGetEnvAttr(this->env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0);

  EXPECT_EQ(SQL_SUCCESS, return_get);

  EXPECT_EQ(SQL_TRUE, output_nts);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();

  this->Connect();

  SQLINTEGER length;

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length);

  EXPECT_EQ(SQL_SUCCESS, return_get);

  EXPECT_EQ(sizeof(SQLINTEGER), length);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  GTEST_SKIP();
  this->Connect();

  SQLRETURN return_get =
      SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr);

  EXPECT_EQ(SQL_ERROR, return_get);

  this->Disconnect();
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSValid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to output nts to supported version
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_TRUE), 0);

  EXPECT_EQ(SQL_SUCCESS, return_set);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSInvalid) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set to output nts to unsupported false
  SQLRETURN return_set =
      SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS, reinterpret_cast<void*>(SQL_FALSE), 0);

  EXPECT_EQ(SQL_ERROR, return_set);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrNullValuePointer) {
  // ODBC Environment
  SQLHENV env;

  // Allocate an environment handle
  SQLRETURN return_env = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, return_env);

  // Attempt to set using bad data pointer
  SQLRETURN return_set = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0);

  EXPECT_EQ(SQL_ERROR, return_set);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLDriverConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = this->GetConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                         ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Check that out_str has same content as connect_str
  std::string out_connection_string = ODBC::SqlWcharToString(out_str, out_str_len);
  Connection::ConnPropertyMap out_properties;
  Connection::ConnPropertyMap in_properties;
  ODBC::ODBCConnection::GetPropertiesFromConnString(out_connection_string,
                                                    out_properties);
  ODBC::ODBCConnection::GetPropertiesFromConnString(connect_str, in_properties);
  EXPECT_TRUE(CompareConnPropertyMap(out_properties, in_properties));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

#if defined _WIN32 || defined _WIN64
TYPED_TEST(FlightSQLODBCTestBase, TestSQLDriverConnectDsn) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = this->GetConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing
  ASSERT_TRUE(WriteDSN(connect_str));

  std::string dsn(TEST_DSN);
  ASSERT_OK_AND_ASSIGN(std::wstring wdsn, arrow::util::UTF8ToWideString(dsn));

  // Update connection string to use DSN to connect
  connect_str = std::string("DSN=") + std::string(TEST_DSN) +
                std::string(";driver={Apache Arrow Flight SQL ODBC Driver};");
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                         ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLConnect) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = this->GetConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing
  std::string uid(""), pwd("");
  ASSERT_TRUE(WriteDSN(connect_str));

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

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInputUidPwd) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = GetConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::GetPropertiesFromConnString(connect_str, properties);
  std::string uid_key("uid");
  std::string pwd_key("pwd");
  std::string uid = properties[uid_key];
  std::string pwd = properties[pwd_key];

  // Write connection string content without uid and pwd into a DSN,
  // must succeed before continuing
  properties.erase(uid_key);
  properties.erase(pwd_key);
  ASSERT_TRUE(WriteDSN(properties));

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

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = GetConnectionString();

  // Retrieve valid uid and pwd, assumes TEST_CONNECT_STR contains uid and pwd
  Connection::ConnPropertyMap properties;
  ODBC::ODBCConnection::GetPropertiesFromConnString(connect_str, properties);
  std::string uid = properties[std::string("uid")];
  std::string pwd = properties[std::string("pwd")];

  // Append invalid uid to connection string
  connect_str += std::string("uid=non_existent_id;");

  // Write connection string content into a DSN,
  // must succeed before continuing
  ASSERT_TRUE(WriteDSN(connect_str));

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
  EXPECT_EQ(SQL_ERROR, ret);

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLConnectDSNPrecedence) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = GetConnectionString();

  // Write connection string content into a DSN,
  // must succeed before continuing

  // Pass incorrect uid and password to SQLConnect, they will be ignored.
  // Assumes TEST_CONNECT_STR contains uid and pwd
  std::string uid("non_existent_id"), pwd("non_existent_password");
  ASSERT_TRUE(WriteDSN(connect_str));

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

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  EXPECT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

#endif

TEST_F(FlightSQLODBCRemoteTestBase, TestSQLDriverConnectInvalidUid) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Invalid connect string
  std::string connect_str = GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                         ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  EXPECT_EQ(SQL_ERROR, ret);

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  std::string out_connection_string = ODBC::SqlWcharToString(out_str, out_str_len);
  EXPECT_TRUE(out_connection_string.empty());

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TEST(SQLDisconnect, TestSQLDisconnectWithoutConnection) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Attempt to disconnect without a connection, expect to fail
  ret = SQLDisconnect(conn);

  EXPECT_EQ(SQL_ERROR, ret);

  // Expect ODBC driver manager to return error state
  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_08003);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TYPED_TEST(FlightSQLODBCTestBase, TestConnect) {
  // Verifies connect and disconnect works on its own
  this->Connect();
  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeStmt) {
  this->Connect();
  SQLHSTMT statement;

  // Allocate a statement using alloc statement
  SQLRETURN ret = SQLAllocStmt(this->conn, &statement);

  EXPECT_EQ(SQL_SUCCESS, ret);

  SQLWCHAR sql_buffer[ODBC_BUFFER_SIZE] = L"SELECT 1";
  ret = SQLExecDirect(statement, sql_buffer, SQL_NTS);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Close statement handle
  ret = SQLFreeStmt(statement, SQL_CLOSE);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free statement handle
  ret = SQLFreeStmt(statement, SQL_DROP);

  EXPECT_EQ(SQL_SUCCESS, ret);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestCloseConnectionWithOpenStatement) {
  // ODBC Environment
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT statement;

  // Allocate an environment handle
  SQLRETURN ret = SQLAllocEnv(&env);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a connection using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Connect string
  std::string connect_str = this->GetConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                         static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                         ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate a statement using alloc statement
  ret = SQLAllocStmt(conn, &statement);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Disconnect from ODBC without closing the statement first
  ret = SQLDisconnect(conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ret = SQLFreeHandle(SQL_HANDLE_DBC, conn);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free environment handle
  ret = SQLFreeHandle(SQL_HANDLE_ENV, env);

  EXPECT_EQ(SQL_SUCCESS, ret);
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLAllocFreeDesc) {
  this->Connect();
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Free descriptor handle
  ret = SQLFreeHandle(SQL_HANDLE_DESC, descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  this->Disconnect();
}

TYPED_TEST(FlightSQLODBCTestBase, TestSQLSetStmtAttrDescriptor) {
  this->Connect();

  SQLHDESC apd_descriptor, ard_descriptor;

  // Allocate an APD descriptor using alloc handle
  SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &apd_descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Allocate an ARD descriptor using alloc handle
  ret = SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &ard_descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Save implicitly allocated internal APD and ARD descriptor pointers
  SQLPOINTER internal_apd, internal_ard = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &internal_apd,
                       sizeof(internal_apd), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &internal_ard,
                       sizeof(internal_ard), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Set APD descriptor to explicitly allocated handle
  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                       reinterpret_cast<SQLPOINTER>(apd_descriptor), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Set ARD descriptor to explicitly allocated handle
  ret = SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                       reinterpret_cast<SQLPOINTER>(ard_descriptor), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Verify APD and ARD descriptors are set to explicitly allocated pointers
  SQLPOINTER value = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  EXPECT_EQ(apd_descriptor, value);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  EXPECT_EQ(ard_descriptor, value);

  // Free explicitly allocated APD and ARD descriptor handles
  ret = SQLFreeHandle(SQL_HANDLE_DESC, apd_descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  ret = SQLFreeHandle(SQL_HANDLE_DESC, ard_descriptor);

  EXPECT_EQ(SQL_SUCCESS, ret);

  // Verify APD and ARD descriptors has been reverted to implicit descriptors
  value = nullptr;

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  EXPECT_EQ(internal_apd, value);

  ret = SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0);

  EXPECT_EQ(SQL_SUCCESS, ret);

  EXPECT_EQ(internal_ard, value);

  this->Disconnect();
}

}  // namespace arrow::flight::sql::odbc
