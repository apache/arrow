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

#pragma once

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

//  @file odbc_api.h
//
//  Define internal ODBC API function headers.
namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result);
SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle);
SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option);
SQLRETURN SQLGetDiagFieldW(SQLSMALLINT handleType, SQLHANDLE handle,
                           SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier,
                           SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                           SQLSMALLINT* stringLengthPtr);
SQLRETURN SQLGetDiagRecW(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                         SQLWCHAR* sqlState, SQLINTEGER* nativeErrorPtr,
                         SQLWCHAR* messageText, SQLSMALLINT bufferLength,
                         SQLSMALLINT* textLengthPtr);
SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER bufferLen, SQLINTEGER* strLenPtr);
SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER strLen);
SQLRETURN SQLDriverConnectW(SQLHDBC conn, SQLHWND windowHandle,
                            SQLWCHAR* inConnectionString,
                            SQLSMALLINT inConnectionStringLen,
                            SQLWCHAR* outConnectionString,
                            SQLSMALLINT outConnectionStringBufferLen,
                            SQLSMALLINT* outConnectionStringLen,
                            SQLUSMALLINT driverCompletion);
SQLRETURN SQLConnectW(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                      SQLWCHAR* userName, SQLSMALLINT userNameLen, SQLWCHAR* password,
                      SQLSMALLINT passwordLen);
SQLRETURN SQLDisconnect(SQLHDBC conn);
SQLRETURN SQLGetInfoW(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                      SQLSMALLINT bufLen, SQLSMALLINT* length);
}  // namespace arrow
