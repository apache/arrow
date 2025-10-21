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
class ConnectionTest : public T {
 public:
  using List = std::list<T>;
};

class ConnectionRemoteTest : public FlightSQLODBCRemoteTestBase {};
using TestTypes = ::testing::Types<FlightSQLODBCMockTestBase, ConnectionRemoteTest>;
TYPED_TEST_SUITE(ConnectionTest, TestTypes);

TEST(SQLAllocHandle, TestSQLAllocHandleEnv) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env));

  ASSERT_NE(env, nullptr);

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST(SQLAllocEnv, TestSQLAllocEnv) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLAllocHandle, TestSQLAllocHandleConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));
}

TEST(SQLAllocConnect, TestSQLAllocHandleConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocConnect(env, &conn));
}

TEST(SQLFreeEnv, TestSQLFreeEnv) {
  SQLHENV env;

  // Allocate an environment handle
  SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

  // Free an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeEnv(env));
}

TEST(SQLFreeHandle, TestSQLFreeHandleConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Free the created connection using free handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));
}

TEST(SQLFreeHandle, TestFreeNullHandles) {
  SQLHENV env = NULL;
  SQLHDBC conn = NULL;
  SQLHSTMT stmt = NULL;

  // Verifies attempt to free invalid handle does not cause segfault
  // Attempt to free null statement handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_STMT, stmt));

  // Attempt to free null connection handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Attempt to free null environment handle
  ASSERT_EQ(SQL_INVALID_HANDLE, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST(SQLFreeConnect, TestSQLFreeConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Free the created connection using free connect
  ASSERT_EQ(SQL_SUCCESS, SQLFreeConnect(conn));
}

TEST(SQLGetEnvAttr, TestSQLGetEnvAttrODBCVersion) {
  SQLHENV env;

  SQLINTEGER version;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS, SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0));

  ASSERT_EQ(SQL_OV_ODBC2, version);
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionValid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to unsupported version
  ASSERT_EQ(SQL_SUCCESS, SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION,
                                       reinterpret_cast<void*>(SQL_OV_ODBC2), 0));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrODBCVersionInvalid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to unsupported version
  ASSERT_EQ(SQL_ERROR,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(1), 0));
}

TYPED_TEST(ConnectionTest, TestSQLGetEnvAttrOutputNTS) {
  SQLINTEGER output_nts;

  ASSERT_EQ(SQL_SUCCESS,
            SQLGetEnvAttr(this->env, SQL_ATTR_OUTPUT_NTS, &output_nts, 0, 0));

  ASSERT_EQ(SQL_TRUE, output_nts);
}

TYPED_TEST(ConnectionTest, DISABLED_TestSQLGetEnvAttrGetLength) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  SQLINTEGER length;
  ASSERT_EQ(SQL_SUCCESS,
            SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, &length));

  EXPECT_EQ(sizeof(SQLINTEGER), length);
}

TYPED_TEST(ConnectionTest, DISABLED_TestSQLGetEnvAttrNullValuePointer) {
  // Test is disabled because call to SQLGetEnvAttr is handled by the driver manager on
  // Windows. This test case can be potentially used on macOS/Linux
  ASSERT_EQ(SQL_ERROR,
            SQLGetEnvAttr(this->env, SQL_ATTR_ODBC_VERSION, nullptr, 0, nullptr));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSValid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to output nts to supported version
  ASSERT_EQ(SQL_SUCCESS, SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS,
                                       reinterpret_cast<void*>(SQL_TRUE), 0));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrOutputNTSInvalid) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set to output nts to unsupported false
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(env, SQL_ATTR_OUTPUT_NTS,
                                     reinterpret_cast<void*>(SQL_FALSE), 0));
}

TEST(SQLSetEnvAttr, TestSQLSetEnvAttrNullValuePointer) {
  SQLHENV env;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  // Attempt to set using bad data pointer
  ASSERT_EQ(SQL_ERROR, SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, nullptr, 0));
}

TYPED_TEST(ConnectionTest, TestSQLDriverConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Connect string
  std::string connect_str = this->GetConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  SQLRETURN ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                                   static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                                   ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Check that out_str has same content as connect_str
  std::string out_connection_string = ODBC::SqlWcharToString(out_str, out_str_len);
  Connection::ConnPropertyMap out_properties;
  Connection::ConnPropertyMap in_properties;
  ODBC::ODBCConnection::GetPropertiesFromConnString(out_connection_string,
                                                    out_properties);
  ODBC::ODBCConnection::GetPropertiesFromConnString(connect_str, in_properties);
  ASSERT_TRUE(CompareConnPropertyMap(out_properties, in_properties));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

#if defined _WIN32 || defined _WIN64
TYPED_TEST(ConnectionTest, TestSQLDriverConnectDsn) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

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
  SQLRETURN ret = SQLDriverConnect(conn, NULL, &connect_str0[0],
                                   static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                                   ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  ASSERT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(ConnectionTest, TestSQLConnect) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

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
  SQLRETURN ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()),
                             uid0.data(), static_cast<SQLSMALLINT>(uid0.size()),
                             pwd0.data(), static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  ASSERT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST_F(ConnectionRemoteTest, TestSQLConnectInputUidPwd) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

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
  SQLRETURN ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()),
                             uid0.data(), static_cast<SQLSMALLINT>(uid0.size()),
                             pwd0.data(), static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  ASSERT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST_F(ConnectionRemoteTest, TestSQLConnectInvalidUid) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

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
  // UID specified in DSN will take precedence,
  // so connection still fails despite passing valid uid in SQLConnect call
  ASSERT_EQ(SQL_ERROR,
            SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()),
                       uid0.data(), static_cast<SQLSMALLINT>(uid0.size()), pwd0.data(),
                       static_cast<SQLSMALLINT>(pwd0.size())));

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  // Remove DSN
  ASSERT_TRUE(UnregisterDsn(wdsn));

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST_F(ConnectionRemoteTest, TestSQLConnectDSNPrecedence) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

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
  SQLRETURN ret = SQLConnect(conn, dsn0.data(), static_cast<SQLSMALLINT>(dsn0.size()),
                             uid0.data(), static_cast<SQLSMALLINT>(uid0.size()),
                             pwd0.data(), static_cast<SQLSMALLINT>(pwd0.size()));

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Remove DSN
  ASSERT_TRUE(UnregisterDsn(wdsn));

  // Disconnect from ODBC
  ret = SQLDisconnect(conn);

  if (ret != SQL_SUCCESS) {
    std::cerr << GetOdbcErrorMessage(SQL_HANDLE_DBC, conn) << std::endl;
  }

  ASSERT_EQ(SQL_SUCCESS, ret);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

#endif

TEST_F(ConnectionRemoteTest, TestSQLDriverConnectInvalidUid) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Invalid connect string
  std::string connect_str = GetInvalidConnectionString();

  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE];
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_ERROR,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT));

  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_28000);

  std::string out_connection_string = ODBC::SqlWcharToString(out_str, out_str_len);
  ASSERT_TRUE(out_connection_string.empty());

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TEST(SQLDisconnect, TestSQLDisconnectWithoutConnection) {
  SQLHENV env;
  SQLHDBC conn;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Attempt to disconnect without a connection, expect to fail
  ASSERT_EQ(SQL_ERROR, SQLDisconnect(conn));

  // Expect ODBC driver manager to return error state
  VerifyOdbcErrorState(SQL_HANDLE_DBC, conn, error_state_08003);

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(ConnectionTest, TestConnect) {
  // Verifies connect and disconnect works on its own
}

TYPED_TEST(ConnectionTest, TestSQLAllocFreeStmt) {
  SQLHSTMT statement;

  // Allocate a statement using alloc statement
  ASSERT_EQ(SQL_SUCCESS, SQLAllocStmt(this->conn, &statement));

  SQLWCHAR sql_buffer[ODBC_BUFFER_SIZE] = L"SELECT 1";
  ASSERT_EQ(SQL_SUCCESS, SQLExecDirect(statement, sql_buffer, SQL_NTS));

  // Close statement handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeStmt(statement, SQL_CLOSE));

  // Free statement handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeStmt(statement, SQL_DROP));
}

TYPED_TEST(ConnectionTest, TestCloseConnectionWithOpenStatement) {
  SQLHENV env;
  SQLHDBC conn;
  SQLHSTMT statement;

  // Allocate an environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocEnv(&env));

  ASSERT_EQ(SQL_SUCCESS,
            SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0));

  // Allocate a connection using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

  // Connect string
  std::string connect_str = this->GetConnectionString();
  ASSERT_OK_AND_ASSIGN(std::wstring wconnect_str,
                       arrow::util::UTF8ToWideString(connect_str));
  std::vector<SQLWCHAR> connect_str0(wconnect_str.begin(), wconnect_str.end());

  SQLWCHAR out_str[ODBC_BUFFER_SIZE] = L"";
  SQLSMALLINT out_str_len;

  // Connecting to ODBC server.
  ASSERT_EQ(SQL_SUCCESS,
            SQLDriverConnect(conn, NULL, &connect_str0[0],
                             static_cast<SQLSMALLINT>(connect_str0.size()), out_str,
                             ODBC_BUFFER_SIZE, &out_str_len, SQL_DRIVER_NOPROMPT));

  // Allocate a statement using alloc statement
  ASSERT_EQ(SQL_SUCCESS, SQLAllocStmt(conn, &statement));

  // Disconnect from ODBC without closing the statement first
  ASSERT_EQ(SQL_SUCCESS, SQLDisconnect(conn));

  // Free connection handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DBC, conn));

  // Free environment handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_ENV, env));
}

TYPED_TEST(ConnectionTest, TestSQLAllocFreeDesc) {
  SQLHDESC descriptor;

  // Allocate a descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &descriptor));

  // Free descriptor handle
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, descriptor));
}

TYPED_TEST(ConnectionTest, TestSQLSetStmtAttrDescriptor) {
  SQLHDESC apd_descriptor, ard_descriptor;

  // Allocate an APD descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &apd_descriptor));

  // Allocate an ARD descriptor using alloc handle
  ASSERT_EQ(SQL_SUCCESS, SQLAllocHandle(SQL_HANDLE_DESC, this->conn, &ard_descriptor));

  // Save implicitly allocated internal APD and ARD descriptor pointers
  SQLPOINTER internal_apd, internal_ard = nullptr;

  EXPECT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                        &internal_apd, sizeof(internal_apd), 0));

  EXPECT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &internal_ard,
                                        sizeof(internal_ard), 0));

  // Set APD descriptor to explicitly allocated handle
  EXPECT_EQ(SQL_SUCCESS, SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC,
                                        reinterpret_cast<SQLPOINTER>(apd_descriptor), 0));

  // Set ARD descriptor to explicitly allocated handle
  EXPECT_EQ(SQL_SUCCESS, SQLSetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC,
                                        reinterpret_cast<SQLPOINTER>(ard_descriptor), 0));

  // Verify APD and ARD descriptors are set to explicitly allocated pointers
  SQLPOINTER value = nullptr;
  EXPECT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value,
                                        sizeof(value), 0));

  EXPECT_EQ(apd_descriptor, value);

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0));

  EXPECT_EQ(ard_descriptor, value);

  // Free explicitly allocated APD and ARD descriptor handles
  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, apd_descriptor));

  ASSERT_EQ(SQL_SUCCESS, SQLFreeHandle(SQL_HANDLE_DESC, ard_descriptor));

  // Verify APD and ARD descriptors has been reverted to implicit descriptors
  value = nullptr;

  EXPECT_EQ(SQL_SUCCESS, SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_PARAM_DESC, &value,
                                        sizeof(value), 0));

  EXPECT_EQ(internal_apd, value);

  EXPECT_EQ(SQL_SUCCESS,
            SQLGetStmtAttr(this->stmt, SQL_ATTR_APP_ROW_DESC, &value, sizeof(value), 0));

  EXPECT_EQ(internal_ard, value);
}

}  // namespace arrow::flight::sql::odbc
