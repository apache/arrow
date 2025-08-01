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

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle,
                                  SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier,
                                  SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                                  SQLSMALLINT* stringLengthPtr) {
  return arrow::SQLGetDiagField(handleType, handle, recNumber, diagIdentifier,
                                diagInfoPtr, bufferLength, stringLengthPtr);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handleType, SQLHANDLE handle,
                                SQLSMALLINT recNumber, SQLWCHAR* sqlState,
                                SQLINTEGER* nativeErrorPtr, SQLWCHAR* messageText,
                                SQLSMALLINT bufferLength, SQLSMALLINT* textLengthPtr) {
  return arrow::SQLGetDiagRec(handleType, handle, recNumber, sqlState, nativeErrorPtr,
                              messageText, bufferLength, textLengthPtr);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER bufferLen, SQLINTEGER* strLenPtr) {
  return arrow::SQLGetEnvAttr(env, attr, valuePtr, bufferLen, strLenPtr);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                                SQLINTEGER strLen) {
  return arrow::SQLSetEnvAttr(env, attr, valuePtr, strLen);
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute,
                                    SQLPOINTER valuePtr, SQLINTEGER bufferLength,
                                    SQLINTEGER* stringLengthPtr) {
  return arrow::SQLGetConnectAttr(conn, attribute, valuePtr, bufferLength,
                                  stringLengthPtr);
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                    SQLINTEGER valueLen) {
  return arrow::SQLSetConnectAttr(conn, attr, value, valueLen);
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                             SQLSMALLINT bufLen, SQLSMALLINT* length) {
  return arrow::SQLGetInfo(conn, infoType, infoValuePtr, bufLen, length);
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
                                   SQLWCHAR* inConnectionString,
                                   SQLSMALLINT inConnectionStringLen,
                                   SQLWCHAR* outConnectionString,
                                   SQLSMALLINT outConnectionStringBufferLen,
                                   SQLSMALLINT* outConnectionStringLen,
                                   SQLUSMALLINT driverCompletion) {
  return arrow::SQLDriverConnect(
      conn, windowHandle, inConnectionString, inConnectionStringLen, outConnectionString,
      outConnectionStringBufferLen, outConnectionStringLen, driverCompletion);
}

SQLRETURN SQL_API SQLConnect(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                             SQLWCHAR* userName, SQLSMALLINT userNameLen,
                             SQLWCHAR* password, SQLSMALLINT passwordLen) {
  return arrow::SQLConnect(conn, dsnName, dsnNameLen, userName, userNameLen, password,
                           passwordLen);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn) { return arrow::SQLDisconnect(conn); }

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                                 SQLINTEGER bufferLength, SQLINTEGER* stringLengthPtr) {
  return arrow::SQLGetStmtAttr(stmt, attribute, valuePtr, bufferLength, stringLengthPtr);
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* queryText,
                                SQLINTEGER textLength) {
  return arrow::SQLExecDirect(stmt, queryText, textLength);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) { return arrow::SQLFetch(stmt); }

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT fetchOrientation,
                                   SQLLEN fetchOffset, SQLULEN* rowCountPtr,
                                   SQLUSMALLINT* rowStatusArray) {
  return arrow::SQLExtendedFetch(stmt, fetchOrientation, fetchOffset, rowCountPtr,
                                 rowStatusArray);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetchOrientation,
                                 SQLLEN fetchOffset) {
  return arrow::SQLFetchScroll(stmt, fetchOrientation, fetchOffset);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                             SQLPOINTER dataPtr, SQLLEN bufferLength,
                             SQLLEN* indicatorPtr) {
  return arrow::SQLGetData(stmt, recordNumber, cType, dataPtr, bufferLength,
                           indicatorPtr);
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLWCHAR* queryText, SQLINTEGER textLength) {
  return arrow::SQLPrepare(stmt, queryText, textLength);
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt) { return arrow::SQLExecute(stmt); }

SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT recordNumber, SQLSMALLINT cType,
                             SQLPOINTER dataPtr, SQLLEN bufferLength,
                             SQLLEN* indicatorPtr) {
  return arrow::SQLBindCol(stmt, recordNumber, cType, dataPtr, bufferLength,
                           indicatorPtr);
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt) {
  LOG_DEBUG("SQLCancel called with stmt: {}", stmt);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLCancel is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt) { return arrow::SQLCloseCursor(stmt); }

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT recordNumber,
                                  SQLUSMALLINT fieldIdentifier,
                                  SQLPOINTER characterAttributePtr,
                                  SQLSMALLINT bufferLength, SQLSMALLINT* outputLength,
                                  SQLLEN* numericAttributePtr) {
  return arrow::SQLColAttribute(stmt, recordNumber, fieldIdentifier,
                                characterAttributePtr, bufferLength, outputLength,
                                numericAttributePtr);
}

SQLRETURN SQL_API SQLTables(SQLHSTMT stmt, SQLWCHAR* catalogName,
                            SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                            SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                            SQLSMALLINT tableNameLength, SQLWCHAR* tableType,
                            SQLSMALLINT tableTypeLength) {
  return arrow::SQLTables(stmt, catalogName, catalogNameLength, schemaName,
                          schemaNameLength, tableName, tableNameLength, tableType,
                          tableTypeLength);
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT stmt, SQLWCHAR* catalogName,
                             SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                             SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                             SQLSMALLINT tableNameLength, SQLWCHAR* columnName,
                             SQLSMALLINT columnNameLength) {
  return arrow::SQLColumns(stmt, catalogName, catalogNameLength, schemaName,
                           schemaNameLength, tableName, tableNameLength, columnName,
                           columnNameLength);
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT stmt, SQLWCHAR* pKCatalogName,
                                 SQLSMALLINT pKCatalogNameLength, SQLWCHAR* pKSchemaName,
                                 SQLSMALLINT pKSchemaNameLength, SQLWCHAR* pKTableName,
                                 SQLSMALLINT pKTableNameLength, SQLWCHAR* fKCatalogName,
                                 SQLSMALLINT fKCatalogNameLength, SQLWCHAR* fKSchemaName,
                                 SQLSMALLINT fKSchemaNameLength, SQLWCHAR* fKTableName,
                                 SQLSMALLINT fKTableNameLength) {
  LOG_DEBUG(
      "SQLForeignKeysW called with stmt: {}, pKCatalogName: {}, "
      "pKCatalogNameLength: "
      "{}, pKSchemaName: {}, pKSchemaNameLength: {}, pKTableName: {}, pKTableNameLength: "
      "{}, "
      "fKCatalogName: {}, fKCatalogNameLength: {}, fKSchemaName: {}, fKSchemaNameLength: "
      "{}, "
      "fKTableName: {}, fKTableNameLength : {}",
      stmt, fmt::ptr(pKCatalogName), pKCatalogNameLength, fmt::ptr(pKSchemaName),
      pKSchemaNameLength, fmt::ptr(pKTableName), pKTableNameLength,
      fmt::ptr(fKCatalogName), fKCatalogNameLength, fmt::ptr(fKSchemaName),
      fKSchemaNameLength, fmt::ptr(fKTableName), fKTableNameLength);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLForeignKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT dataType) {
  LOG_DEBUG("SQLGetTypeInfoW called with stmt: {} dataType: {}", stmt, dataType);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLGetTypeInfoW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt) { return arrow::SQLMoreResults(stmt); }

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connectionHandle, SQLWCHAR* inStatementText,
                               SQLINTEGER inStatementTextLength,
                               SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
                               SQLINTEGER* outStatementTextLength) {
  return arrow::SQLNativeSql(connectionHandle, inStatementText, inStatementTextLength,
                             outStatementText, bufferLength, outStatementTextLength);
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* columnCountPtr) {
  return arrow::SQLNumResultCols(stmt, columnCountPtr);
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN* rowCountPtr) {
  return arrow::SQLRowCount(stmt, rowCountPtr);
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT stmt, SQLWCHAR* catalogName,
                                 SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                                 SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                                 SQLSMALLINT tableNameLength) {
  LOG_DEBUG(
      "SQLPrimaryKeysW called with stmt: {}, catalogName: {}, "
      "catalogNameLength: "
      "{}, schemaName: {}, schemaNameLength: {}, tableName: {}, tableNameLength: {}",
      stmt, fmt::ptr(catalogName), catalogNameLength, fmt::ptr(schemaName),
      schemaNameLength, fmt::ptr(tableName), tableNameLength);
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLPrimaryKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER valuePtr,
                                 SQLINTEGER stringLength) {
  return arrow::SQLSetStmtAttr(stmt, attribute, valuePtr, stringLength);
}
