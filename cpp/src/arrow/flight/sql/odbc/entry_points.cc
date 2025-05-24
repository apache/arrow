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

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "arrow/flight/sql/odbc/odbc_api.h"
#include "arrow/flight/sql/odbc/visibility.h"

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  return arrow::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env) {
  return arrow::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC* conn) {
  return arrow::SQLAllocHandle(SQL_HANDLE_DBC, env, conn);
}

SQLRETURN SQL_API SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
  return arrow::SQLFreeHandle(type, handle);
}

SQLRETURN SQL_API SQLFreeEnv(SQLHENV env) {
  return arrow::SQLFreeHandle(SQL_HANDLE_ENV, env);
}

SQLRETURN SQL_API SQLFreeConnect(SQLHDBC conn) {
  return arrow::SQLFreeHandle(SQL_HANDLE_DBC, conn);
}

SQLRETURN SQL_API SQLGetDiagFieldW(SQLSMALLINT handleType, SQLHANDLE handle,
                                   SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier,
                                   SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                                   SQLSMALLINT* stringLengthPtr) {
  return arrow::SQLGetDiagFieldW(handleType, handle, recNumber, diagIdentifier,
                                 diagInfoPtr, bufferLength, stringLengthPtr);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER bufferLen, SQLINTEGER* strLenPtr) {
  return arrow::SQLGetEnvAttr(env, attr, valuePtr, bufferLen, strLenPtr);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER strLen) {
  return arrow::SQLSetEnvAttr(env, attr, valuePtr, strLen);
}

SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                     SQLINTEGER valueLen) {
  // TODO implement SQLSetConnectAttr
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetInfoW(SQLHDBC conn, SQLUSMALLINT infoType,
                              SQLPOINTER infoValuePtr, SQLSMALLINT bufLen,
                              SQLSMALLINT* length) {
  return arrow::SQLGetInfoW(conn, infoType, infoValuePtr, bufLen, length);
}

SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT type, SQLHANDLE handle, SQLSMALLINT recNum,
                                 SQLWCHAR* sqlState, SQLINTEGER* nativeError,
                                 SQLWCHAR* msgBuffer, SQLSMALLINT msgBufferLen,
                                 SQLSMALLINT* msgLen) {
  // TODO implement SQLGetDiagRecW
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC conn, SQLHWND windowHandle,
                                    SQLWCHAR* inConnectionString,
                                    SQLSMALLINT inConnectionStringLen,
                                    SQLWCHAR* outConnectionString,
                                    SQLSMALLINT outConnectionStringBufferLen,
                                    SQLSMALLINT* outConnectionStringLen,
                                    SQLUSMALLINT driverCompletion) {
  return arrow::SQLDriverConnectW(
      conn, windowHandle, inConnectionString, inConnectionStringLen, outConnectionString,
      outConnectionStringBufferLen, outConnectionStringLen, driverCompletion);
}

SQLRETURN SQL_API SQLConnectW(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                              SQLWCHAR* userName, SQLSMALLINT userNameLen,
                              SQLWCHAR* password, SQLSMALLINT passwordLen) {
  return arrow::SQLConnectW(conn, dsnName, dsnNameLen, userName, userNameLen, password,
                            passwordLen);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn) { return arrow::SQLDisconnect(conn); }
