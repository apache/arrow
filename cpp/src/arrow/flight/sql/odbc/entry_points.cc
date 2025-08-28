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

// platform.h includes windows.h, so it needs to be included first
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sqlucode.h>

#include "arrow/flight/sql/odbc/odbc_api.h"
#include "arrow/flight/sql/odbc/visibility.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_descriptor.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"

#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/logger.h"

SQLRETURN SQL_API SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  return arrow::SQLAllocHandle(type, parent, result);
}

SQLRETURN SQL_API SQLAllocEnv(SQLHENV* env) {
  return arrow::SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, env);
}

SQLRETURN SQL_API SQLAllocConnect(SQLHENV env, SQLHDBC* conn) {
  return arrow::SQLAllocHandle(SQL_HANDLE_DBC, env, conn);
}

SQLRETURN SQL_API SQLAllocStmt(SQLHDBC conn, SQLHSTMT* stmt) {
  return arrow::SQLAllocHandle(SQL_HANDLE_STMT, conn, stmt);
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

SQLRETURN SQL_API SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option) {
  return arrow::SQLFreeStmt(stmt, option);
}
