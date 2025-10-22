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

#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

//  \file odbc_api_internal.h
//
//  Define internal ODBC API function headers.
namespace arrow::flight::sql::odbc {
[[nodiscard]] SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent,
                                       SQLHANDLE* result);
[[nodiscard]] SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle);
[[nodiscard]] SQLRETURN SQLFreeStmt(SQLHSTMT stmt, SQLUSMALLINT option);
[[nodiscard]] SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle,
                                        SQLSMALLINT rec_number,
                                        SQLSMALLINT diag_identifier,
                                        SQLPOINTER diag_info_ptr,
                                        SQLSMALLINT buffer_length,
                                        SQLSMALLINT* string_length_ptr);
[[nodiscard]] SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle,
                                      SQLSMALLINT rec_number, SQLWCHAR* sql_state,
                                      SQLINTEGER* native_error_ptr,
                                      SQLWCHAR* message_text, SQLSMALLINT buffer_length,
                                      SQLSMALLINT* text_length_ptr);
[[nodiscard]] SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                                      SQLINTEGER buffer_len, SQLINTEGER* str_len_ptr);
[[nodiscard]] SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                                      SQLINTEGER str_len);
[[nodiscard]] SQLRETURN SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute,
                                          SQLPOINTER value_ptr, SQLINTEGER buffer_length,
                                          SQLINTEGER* string_length_ptr);
[[nodiscard]] SQLRETURN SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value,
                                          SQLINTEGER value_len);
[[nodiscard]] SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND window_handle,
                                         SQLWCHAR* in_connection_string,
                                         SQLSMALLINT in_connection_string_len,
                                         SQLWCHAR* out_connection_string,
                                         SQLSMALLINT out_connection_string_buffer_len,
                                         SQLSMALLINT* out_connection_string_len,
                                         SQLUSMALLINT driver_completion);
[[nodiscard]] SQLRETURN SQLConnect(SQLHDBC conn, SQLWCHAR* dsn_name,
                                   SQLSMALLINT dsn_name_len, SQLWCHAR* user_name,
                                   SQLSMALLINT user_name_len, SQLWCHAR* password,
                                   SQLSMALLINT password_len);
[[nodiscard]] SQLRETURN SQLDisconnect(SQLHDBC conn);
[[nodiscard]] SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT info_type,
                                   SQLPOINTER info_value_ptr, SQLSMALLINT buf_len,
                                   SQLSMALLINT* length);
[[nodiscard]] SQLRETURN SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute,
                                       SQLPOINTER value_ptr, SQLINTEGER buffer_length,
                                       SQLINTEGER* string_length_ptr);
[[nodiscard]] SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute,
                                       SQLPOINTER value_ptr, SQLINTEGER stringLength);
[[nodiscard]] SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* queryText,
                                      SQLINTEGER text_length);
[[nodiscard]] SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLWCHAR* queryText,
                                   SQLINTEGER text_length);
[[nodiscard]] SQLRETURN SQLExecute(SQLHSTMT stmt);
[[nodiscard]] SQLRETURN SQLFetch(SQLHSTMT stmt);
[[nodiscard]] SQLRETURN SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT fetch_orientation,
                                         SQLLEN fetch_offset, SQLULEN* row_count_ptr,
                                         SQLUSMALLINT* row_status_array);
[[nodiscard]] SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetch_orientation,
                                       SQLLEN fetch_offset);
[[nodiscard]] SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT record_number,
                                   SQLSMALLINT c_type, SQLPOINTER data_ptr,
                                   SQLLEN buffer_length, SQLLEN* indicator_ptr);
[[nodiscard]] SQLRETURN SQLCloseCursor(SQLHSTMT stmt);
[[nodiscard]] SQLRETURN SQLGetData(SQLHSTMT stmt, SQLUSMALLINT record_number,
                                   SQLSMALLINT c_type, SQLPOINTER data_ptr,
                                   SQLLEN buffer_length, SQLLEN* indicator_ptr);
[[nodiscard]] SQLRETURN SQLMoreResults(SQLHSTMT stmt);
[[nodiscard]] SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* column_count_ptr);
[[nodiscard]] SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* row_count_ptr);
[[nodiscard]] SQLRETURN SQLTables(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                                  SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                                  SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                                  SQLSMALLINT table_name_length, SQLWCHAR* table_type,
                                  SQLSMALLINT table_type_length);
[[nodiscard]] SQLRETURN SQLColumns(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                                   SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                                   SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                                   SQLSMALLINT table_name_length, SQLWCHAR* column_name,
                                   SQLSMALLINT column_name_length);
[[nodiscard]] SQLRETURN SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT record_number,
                                        SQLUSMALLINT field_identifier,
                                        SQLPOINTER character_attribute_ptr,
                                        SQLSMALLINT buffer_length,
                                        SQLSMALLINT* output_length,
                                        SQLLEN* numeric_attribute_ptr);
[[nodiscard]] SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT dataType);
[[nodiscard]] SQLRETURN SQLNativeSql(SQLHDBC conn, SQLWCHAR* in_statement_text,
                                     SQLINTEGER in_statement_text_length,
                                     SQLWCHAR* out_statement_text,
                                     SQLINTEGER buffer_length,
                                     SQLINTEGER* out_statement_text_length);
[[nodiscard]] SQLRETURN SQLDescribeCol(
    SQLHSTMT stmt, SQLUSMALLINT column_number, SQLWCHAR* column_name,
    SQLSMALLINT buffer_length, SQLSMALLINT* name_length_ptr, SQLSMALLINT* data_type_ptr,
    SQLULEN* column_size_ptr, SQLSMALLINT* decimal_digits_ptr, SQLSMALLINT* nullable_ptr);
}  // namespace arrow::flight::sql::odbc
