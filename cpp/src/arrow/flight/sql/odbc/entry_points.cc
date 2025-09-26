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

#include "arrow/util/logging.h"

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

SQLRETURN SQL_API SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle,
                                  SQLSMALLINT rec_number, SQLSMALLINT diag_identifier,
                                  SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                  SQLSMALLINT* string_length_ptr) {
  return arrow::SQLGetDiagField(handle_type, handle, rec_number, diag_identifier,
                                diag_info_ptr, buffer_length, string_length_ptr);
}

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle,
                                SQLSMALLINT rec_number, SQLWCHAR* sql_state,
                                SQLINTEGER* native_error_ptr, SQLWCHAR* message_text,
                                SQLSMALLINT buffer_length, SQLSMALLINT* text_length_ptr) {
  return arrow::SQLGetDiagRec(handle_type, handle, rec_number, sql_state,
                              native_error_ptr, message_text, buffer_length,
                              text_length_ptr);
}

SQLRETURN SQL_API SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                                SQLINTEGER buffer_len, SQLINTEGER* str_len_ptr) {
  return arrow::SQLGetEnvAttr(env, attr, value_ptr, buffer_len, str_len_ptr);
}

SQLRETURN SQL_API SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                                SQLINTEGER str_len) {
  return arrow::SQLSetEnvAttr(env, attr, value_ptr, str_len);
}

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute,
                                    SQLPOINTER value_ptr, SQLINTEGER buffer_length,
                                    SQLINTEGER* string_length_ptr) {
  return arrow::SQLGetConnectAttr(conn, attribute, value_ptr, buffer_length,
                                  string_length_ptr);
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                    SQLINTEGER value_len) {
  return arrow::SQLSetConnectAttr(conn, attr, value, value_len);
}

SQLRETURN SQL_API SQLGetInfo(SQLHDBC conn, SQLUSMALLINT info_type,
                             SQLPOINTER info_value_ptr, SQLSMALLINT buf_len,
                             SQLSMALLINT* length) {
  return arrow::SQLGetInfo(conn, info_type, info_value_ptr, buf_len, length);
}

SQLRETURN SQL_API SQLDriverConnect(SQLHDBC conn, SQLHWND window_handle,
                                   SQLWCHAR* in_connection_string,
                                   SQLSMALLINT in_connection_stringLen,
                                   SQLWCHAR* out_connection_string,
                                   SQLSMALLINT out_connection_string_buffer_len,
                                   SQLSMALLINT* out_connection_string_len,
                                   SQLUSMALLINT driver_completion) {
  return arrow::SQLDriverConnect(conn, window_handle, in_connection_string,
                                 in_connection_stringLen, out_connection_string,
                                 out_connection_string_buffer_len,
                                 out_connection_string_len, driver_completion);
}

SQLRETURN SQL_API SQLConnect(SQLHDBC conn, SQLWCHAR* dsn_name, SQLSMALLINT dsn_name_len,
                             SQLWCHAR* user_name, SQLSMALLINT user_name_len,
                             SQLWCHAR* password, SQLSMALLINT password_len) {
  return arrow::SQLConnect(conn, dsn_name, dsn_name_len, user_name, user_name_len,
                           password, password_len);
}

SQLRETURN SQL_API SQLDisconnect(SQLHDBC conn) { return arrow::SQLDisconnect(conn); }

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute,
                                 SQLPOINTER value_ptr, SQLINTEGER buffer_length,
                                 SQLINTEGER* string_length_ptr) {
  return arrow::SQLGetStmtAttr(stmt, attribute, value_ptr, buffer_length,
                               string_length_ptr);
}

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* query_text,
                                SQLINTEGER text_length) {
  return arrow::SQLExecDirect(stmt, query_text, text_length);
}

SQLRETURN SQL_API SQLFetch(SQLHSTMT stmt) { return arrow::SQLFetch(stmt); }

SQLRETURN SQL_API SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT fetch_orientation,
                                   SQLLEN fetch_offset, SQLULEN* row_count_ptr,
                                   SQLUSMALLINT* row_status_array) {
  return arrow::SQLExtendedFetch(stmt, fetch_orientation, fetch_offset, row_count_ptr,
                                 row_status_array);
}

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetch_orientation,
                                 SQLLEN fetch_offset) {
  return arrow::SQLFetchScroll(stmt, fetch_orientation, fetch_offset);
}

SQLRETURN SQL_API SQLGetData(SQLHSTMT stmt, SQLUSMALLINT record_number,
                             SQLSMALLINT c_type, SQLPOINTER data_ptr,
                             SQLLEN buffer_length, SQLLEN* indicator_ptr) {
  return arrow::SQLGetData(stmt, record_number, c_type, data_ptr, buffer_length,
                           indicator_ptr);
}

SQLRETURN SQL_API SQLPrepare(SQLHSTMT stmt, SQLWCHAR* query_text,
                             SQLINTEGER text_length) {
  return arrow::SQLPrepare(stmt, query_text, text_length);
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT stmt) { return arrow::SQLExecute(stmt); }

SQLRETURN SQL_API SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT record_number,
                             SQLSMALLINT c_type, SQLPOINTER data_ptr,
                             SQLLEN buffer_length, SQLLEN* indicator_ptr) {
  return arrow::SQLBindCol(stmt, record_number, c_type, data_ptr, buffer_length,
                           indicator_ptr);
}

SQLRETURN SQL_API SQLCancel(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLCancel called with stmt: " << stmt;
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLCancel is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLCloseCursor(SQLHSTMT stmt) { return arrow::SQLCloseCursor(stmt); }

SQLRETURN SQL_API SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT record_number,
                                  SQLUSMALLINT field_identifier,
                                  SQLPOINTER character_attribute_ptr,
                                  SQLSMALLINT buffer_length, SQLSMALLINT* output_length,
                                  SQLLEN* numeric_attribute_ptr) {
  return arrow::SQLColAttribute(stmt, record_number, field_identifier,
                                character_attribute_ptr, buffer_length, output_length,
                                numeric_attribute_ptr);
}

SQLRETURN SQL_API SQLTables(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                            SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                            SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                            SQLSMALLINT table_name_length, SQLWCHAR* table_type,
                            SQLSMALLINT table_type_length) {
  return arrow::SQLTables(stmt, catalog_name, catalog_name_length, schema_name,
                          schema_name_length, table_name, table_name_length, table_type,
                          table_type_length);
}

SQLRETURN SQL_API SQLColumns(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                             SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                             SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                             SQLSMALLINT table_name_length, SQLWCHAR* columnName,
                             SQLSMALLINT column_name_length) {
  return arrow::SQLColumns(stmt, catalog_name, catalog_name_length, schema_name,
                           schema_name_length, table_name, table_name_length, columnName,
                           column_name_length);
}

SQLRETURN SQL_API SQLForeignKeys(
    SQLHSTMT stmt, SQLWCHAR* pk_catalog_name, SQLSMALLINT pk_catalog_name_length,
    SQLWCHAR* pk_schema_name, SQLSMALLINT pk_schema_name_length, SQLWCHAR* pk_table_name,
    SQLSMALLINT pk_table_name_length, SQLWCHAR* fk_catalog_name,
    SQLSMALLINT fk_catalog_name_length, SQLWCHAR* fk_schema_name,
    SQLSMALLINT fk_schema_name_length, SQLWCHAR* fk_table_name,
    SQLSMALLINT fk_table_name_length) {
  ARROW_LOG(DEBUG) << "SQLForeignKeysW called with stmt: " << stmt
                   << ", pk_catalog_name: " << static_cast<const void*>(pk_catalog_name)
                   << ", pk_catalog_name_length: " << pk_catalog_name_length
                   << ", pk_schema_name: " << static_cast<const void*>(pk_schema_name)
                   << ", pk_schema_name_length: " << pk_schema_name_length
                   << ", pk_table_name: " << static_cast<const void*>(pk_table_name)
                   << ", pk_table_name_length: " << pk_table_name_length
                   << ", fk_catalog_name: " << static_cast<const void*>(fk_catalog_name)
                   << ", fk_catalog_name_length: " << fk_catalog_name_length
                   << ", fk_schema_name: " << static_cast<const void*>(fk_schema_name)
                   << ", fk_schema_name_length: " << fk_schema_name_length
                   << ", fk_table_name: " << static_cast<const void*>(fk_table_name)
                   << ", fk_table_name_length: " << fk_table_name_length;
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLForeignKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT data_type) {
  return arrow::SQLGetTypeInfo(stmt, data_type);
}

SQLRETURN SQL_API SQLMoreResults(SQLHSTMT stmt) { return arrow::SQLMoreResults(stmt); }

SQLRETURN SQL_API SQLNativeSql(SQLHDBC conn, SQLWCHAR* in_statement_text,
                               SQLINTEGER in_statement_text_length,
                               SQLWCHAR* out_statement_text, SQLINTEGER buffer_length,
                               SQLINTEGER* out_statement_text_length) {
  return arrow::SQLNativeSql(conn, in_statement_text, in_statement_text_length,
                             out_statement_text, buffer_length,
                             out_statement_text_length);
}

SQLRETURN SQL_API SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* column_count_ptr) {
  return arrow::SQLNumResultCols(stmt, column_count_ptr);
}

SQLRETURN SQL_API SQLRowCount(SQLHSTMT stmt, SQLLEN* row_count_ptr) {
  return arrow::SQLRowCount(stmt, row_count_ptr);
}

SQLRETURN SQL_API SQLPrimaryKeys(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                                 SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                                 SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                                 SQLSMALLINT table_name_length) {
  ARROW_LOG(DEBUG) << "SQLPrimaryKeysW called with stmt: " << stmt
                   << ", catalog_name: " << static_cast<const void*>(catalog_name)
                   << ", catalog_name_length: " << catalog_name_length
                   << ", schema_name: " << static_cast<const void*>(schema_name)
                   << ", schema_name_length: " << schema_name_length
                   << ", table_name: " << static_cast<const void*>(table_name)
                   << ", table_name_length: " << table_name_length;
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    throw driver::odbcabstraction::DriverException("SQLPrimaryKeysW is not implemented",
                                                   "IM001");
    return SQL_ERROR;
  });
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute,
                                 SQLPOINTER value_ptr, SQLINTEGER stringLength) {
  return arrow::SQLSetStmtAttr(stmt, attribute, value_ptr, stringLength);
}

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT column_number,
                                 SQLWCHAR* column_name, SQLSMALLINT buffer_length,
                                 SQLSMALLINT* name_length_ptr, SQLSMALLINT* data_type_ptr,
                                 SQLULEN* column_size_ptr,
                                 SQLSMALLINT* decimal_digits_ptr,
                                 SQLSMALLINT* nullable_ptr) {
  return arrow::SQLDescribeCol(stmt, column_number, column_name, buffer_length,
                               name_length_ptr, data_type_ptr, column_size_ptr,
                               decimal_digits_ptr, nullable_ptr);
}
