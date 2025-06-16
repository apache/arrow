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

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                    SQLINTEGER valueLen) {
  LOG_DEBUG("SQLSetConnectAttrW called with conn: {}, attr: {}, value: {}, valueLen: {}",
            conn, attr, value, valueLen);
  return SQL_ERROR;
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

SQLRETURN SQL_API SQLBindCol(SQLHSTMT statementHandle, SQLUSMALLINT columnNumber,
                             SQLSMALLINT targetType, SQLPOINTER targetValuePtr,
                             SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) {
  LOG_DEBUG(
      "SQLBindCol called with statementHandle: {}, columnNumber: {}, targetType: {}, "
      "targetValuePtr: {}, bufferLength: {}, strLen_or_IndPtr: {}",
      statementHandle, columnNumber, targetType, targetValuePtr, bufferLength,
      fmt::ptr(strLen_or_IndPtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT statementHandle) {
  LOG_DEBUG("SQLCancel called with statementHandle: {}", statementHandle);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT statementHandle) {
  LOG_DEBUG("SQLCloseCursor called with statementHandle: {}", statementHandle);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT statementHandle, SQLUSMALLINT columnNumber,
                                  SQLUSMALLINT fieldIdentifier,
                                  SQLPOINTER characterAttributePtr,
                                  SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr,
                                  SQLLEN* numericAttributePtr) {
  LOG_DEBUG(
      "SQLColAttributeW called with statementHandle: {}, columnNumber: {}, "
      "fieldIdentifier: {}, characterAttributePtr: {}, bufferLength: {}, "
      "stringLengthPtr: {}, numericAttributePtr: {}",
      statementHandle, columnNumber, fieldIdentifier, characterAttributePtr, bufferLength,
      fmt::ptr(stringLengthPtr), fmt::ptr(numericAttributePtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT statementHandle, SQLWCHAR* catalogName,
                             SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                             SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                             SQLSMALLINT tableNameLength, SQLWCHAR* columnName,
                             SQLSMALLINT columnNameLength) {
  LOG_DEBUG(
      "SQLColumnsW called with statementHandle: {}, catalogName: {}, catalogNameLength: "
      "{}, "
      "schemaName: {}, schemaNameLength: {}, tableName: {}, tableNameLength: {}, "
      "columnName: {}, "
      "columnNameLength: {}",
      statementHandle, fmt::ptr(catalogName), catalogNameLength, fmt::ptr(schemaName),
      schemaNameLength, fmt::ptr(tableName), tableNameLength, fmt::ptr(columnName),
      columnNameLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLError(SQLHENV handleType, SQLHDBC handle, SQLHSTMT hstmt,
                           SQLWCHAR FAR* szSqlState, SQLINTEGER FAR* pfNativeError,
                           SQLWCHAR FAR* szErrorMsg, SQLSMALLINT cbErrorMsgMax,
                           SQLSMALLINT FAR* pcbErrorMsg) {
  LOG_DEBUG(
      "SQLErrorW called with handleType: {}, handle: {}, hstmt: {}, szSqlState: {}, "
      "pfNativeError: {}, szErrorMsg: {}, cbErrorMsgMax: {}, pcbErrorMsg: {}",
      handleType, handle, hstmt, fmt::ptr(szSqlState), fmt::ptr(pfNativeError),
      fmt::ptr(szErrorMsg), cbErrorMsgMax, fmt::ptr(pcbErrorMsg));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT statementHandle, SQLWCHAR* statementText,
                                SQLINTEGER textLength) {
  LOG_DEBUG(
      "SQLExecDirectW called with statementHandle: {}, statementText: {}, textLength: {}",
      statementHandle, fmt::ptr(statementText), textLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT statementHandle) {
  LOG_DEBUG("SQLExecute called with statementHandle: {}", statementHandle);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT statementHandle) {
  LOG_DEBUG("SQLFetch called with statementHandle: {}", statementHandle);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLForeignKeys(SQLHSTMT statementHandle, SQLWCHAR* pKCatalogName,
                                 SQLSMALLINT pKCatalogNameLength, SQLWCHAR* pKSchemaName,
                                 SQLSMALLINT pKSchemaNameLength, SQLWCHAR* pKTableName,
                                 SQLSMALLINT pKTableNameLength, SQLWCHAR* fKCatalogName,
                                 SQLSMALLINT fKCatalogNameLength, SQLWCHAR* fKSchemaName,
                                 SQLSMALLINT fKSchemaNameLength, SQLWCHAR* fKTableName,
                                 SQLSMALLINT fKTableNameLength) {
  LOG_DEBUG(
      "SQLForeignKeysW called with statementHandle: {}, pKCatalogName: {}, "
      "pKCatalogNameLength: "
      "{}, pKSchemaName: {}, pKSchemaNameLength: {}, pKTableName: {}, pKTableNameLength: "
      "{}, "
      "fKCatalogName: {}, fKCatalogNameLength: {}, fKSchemaName: {}, fKSchemaNameLength: "
      "{}, "
      "fKTableName: {}, fKTableNameLength : {}",
      statementHandle, fmt::ptr(pKCatalogName), pKCatalogNameLength,
      fmt::ptr(pKSchemaName), pKSchemaNameLength, fmt::ptr(pKTableName),
      pKTableNameLength, fmt::ptr(fKCatalogName), fKCatalogNameLength,
      fmt::ptr(fKSchemaName), fKSchemaNameLength, fmt::ptr(fKTableName),
      fKTableNameLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC connectionHandle, SQLINTEGER attribute,
                                    SQLPOINTER valuePtr, SQLINTEGER bufferLength,
                                    SQLINTEGER* stringLengthPtr) {
  LOG_DEBUG(
      "SQLGetConnectAttrW called with connectionHandle: {}, attribute: {}, valuePtr: {}, "
      "bufferLength: {}, stringLengthPtr: {}",
      connectionHandle, attribute, valuePtr, bufferLength, fmt::ptr(stringLengthPtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT statementHandle, SQLUSMALLINT col_or_Param_Num,
                             SQLSMALLINT targetType, SQLPOINTER targetValuePtr,
                             SQLLEN bufferLength, SQLLEN* strLen_or_IndPtr) {
  LOG_DEBUG(
      "SQLGetData called with statementHandle: {}, col_or_Param_Num: {}, targetType: {}, "
      "targetValuePtr: {}, bufferLength: {}, strLen_or_IndPtr: {}",
      statementHandle, col_or_Param_Num, targetType, targetValuePtr, bufferLength,
      fmt::ptr(strLen_or_IndPtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT statementHandle, SQLINTEGER attribute,
                                 SQLPOINTER valuePtr, SQLINTEGER bufferLength,
                                 SQLINTEGER* stringLengthPtr) {
  LOG_DEBUG(
      "SQLGetStmtAttrW called with statementHandle: {}, attribute: {}, valuePtr: {}, "
      "bufferLength: {}, stringLengthPtr: {}",
      statementHandle, attribute, valuePtr, bufferLength, fmt::ptr(stringLengthPtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT statementHandle, SQLSMALLINT dataType) {
  LOG_DEBUG("SQLGetTypeInfoW called with statementHandle: {} dataType: {}",
            statementHandle, dataType);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT statementHandle) {
  LOG_DEBUG("SQLMoreResults called with statementHandle: {}", statementHandle);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLNativeSql(SQLHDBC connectionHandle, SQLWCHAR* inStatementText,
                               SQLINTEGER inStatementTextLength,
                               SQLWCHAR* outStatementText, SQLINTEGER bufferLength,
                               SQLINTEGER* outStatementTextLength) {
  LOG_DEBUG(
      "SQLNativeSqlW called with connectionHandle: {}, inStatementText: {}, "
      "inStatementTextLength: "
      "{}, outStatementText: {}, bufferLength: {}, outStatementTextLength: {}",
      connectionHandle, fmt::ptr(inStatementText), inStatementTextLength,
      fmt::ptr(outStatementText), bufferLength, fmt::ptr(outStatementTextLength));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT statementHandle,
                                   SQLSMALLINT* columnCountPtr) {
  LOG_DEBUG("SQLNumResultCols called with statementHandle: {}, columnCountPtr: {}",
            statementHandle, fmt::ptr(columnCountPtr));
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT statementHandle, SQLWCHAR* statementText,
                             SQLINTEGER textLength) {
  LOG_DEBUG(
      "SQLPrepareW called with statementHandle: {}, statementText: {}, textLength: {}",
      statementHandle, fmt::ptr(statementText), textLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT statementHandle, SQLWCHAR* catalogName,
                                 SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                                 SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                                 SQLSMALLINT tableNameLength) {
  LOG_DEBUG(
      "SQLPrimaryKeysW called with statementHandle: {}, catalogName: {}, "
      "catalogNameLength: "
      "{}, schemaName: {}, schemaNameLength: {}, tableName: {}, tableNameLength: {}",
      statementHandle, fmt::ptr(catalogName), catalogNameLength, fmt::ptr(schemaName),
      schemaNameLength, fmt::ptr(tableName), tableNameLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT statementHandle, SQLINTEGER attribute,
                                 SQLPOINTER valuePtr, SQLINTEGER stringLength) {
  LOG_DEBUG(
      "SQLSetStmtAttrW called with statementHandle: {}, attribute: {}, valuePtr: {}, "
      "stringLength: {}",
      statementHandle, attribute, valuePtr, stringLength);
  return SQL_ERROR;
}

SQLRETURN SQL_API SQLTables(SQLHSTMT statementHandle, SQLWCHAR* catalogName,
                            SQLSMALLINT catalogNameLength, SQLWCHAR* schemaName,
                            SQLSMALLINT schemaNameLength, SQLWCHAR* tableName,
                            SQLSMALLINT tableNameLength, SQLWCHAR* tableType,
                            SQLSMALLINT tableTypeLength) {
  LOG_DEBUG(
      "SQLTablesW called with statementHandle: {}, catalogName: {}, catalogNameLength: "
      "{}, "
      "schemaName: {}, schemaNameLength: {}, tableName: {}, tableNameLength: {}, "
      "tableType: {}, "
      "tableTypeLength: {}",
      statementHandle, fmt::ptr(catalogName), catalogNameLength, fmt::ptr(schemaName),
      schemaNameLength, fmt::ptr(tableName), tableNameLength, fmt::ptr(tableType),
      tableTypeLength);
  return SQL_ERROR;
}
