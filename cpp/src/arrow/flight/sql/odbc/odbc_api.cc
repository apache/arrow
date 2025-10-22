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

// flight_sql_connection.h needs to be included first due to conflicts with windows.h
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"

#include "arrow/flight/sql/odbc/odbc_api_internal.h"
#include "arrow/flight/sql/odbc/odbc_impl/attribute_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/config/configuration.h"
#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/flight/sql/odbc/odbc_impl/encoding_utils.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_driver.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_descriptor.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_environment.h"
#include "arrow/flight/sql/odbc/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/connection.h"
#include "arrow/util/logging.h"

namespace arrow::flight::sql::odbc {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  ARROW_LOG(DEBUG) << "SQLAllocHandle called with type: " << type
                   << ", parent: " << parent
                   << ", result: " << static_cast<const void*>(result);
  // GH-47706 TODO: Add tests for SQLAllocStmt, pre-requisite requires
  // SQLDriverConnect implementation

  // GH-47707 TODO: Add tests for SQL_HANDLE_DESC implementation for
  // descriptor handle, pre-requisite requires SQLAllocStmt

  *result = nullptr;

  switch (type) {
    case SQL_HANDLE_ENV: {
      using ODBC::ODBCEnvironment;

      *result = SQL_NULL_HENV;

      try {
        static std::shared_ptr<FlightSqlDriver> odbc_driver =
            std::make_shared<FlightSqlDriver>();
        *result = reinterpret_cast<SQLHENV>(new ODBCEnvironment(odbc_driver));

        return SQL_SUCCESS;
      } catch (const std::bad_alloc&) {
        // allocating environment failed so cannot log diagnostic error here
        return SQL_ERROR;
      }
    }

    case SQL_HANDLE_DBC: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCEnvironment;

      *result = SQL_NULL_HDBC;

      ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(parent);

      return ODBCEnvironment::ExecuteWithDiagnostics(environment, SQL_ERROR, [=]() {
        std::shared_ptr<ODBCConnection> conn = environment->CreateConnection();

        if (conn) {
          // Inside `CreateConnection`, the shared_ptr `conn` is kept
          // in a `std::vector` of connections inside the environment handle.
          // As long as the parent environment handle is alive, the connection shared_ptr
          // will be kept alive unless the user frees the connection.
          *result = reinterpret_cast<SQLHDBC>(conn.get());

          return SQL_SUCCESS;
        }

        return SQL_ERROR;
      });
    }

    case SQL_HANDLE_STMT: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCStatement;

      *result = SQL_NULL_HSTMT;

      ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(parent);

      return ODBCConnection::ExecuteWithDiagnostics(connection, SQL_ERROR, [=]() {
        std::shared_ptr<ODBCStatement> statement = connection->CreateStatement();

        if (statement) {
          *result = reinterpret_cast<SQLHSTMT>(statement.get());

          return SQL_SUCCESS;
        }

        return SQL_ERROR;
      });
    }

    case SQL_HANDLE_DESC: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCDescriptor;

      *result = SQL_NULL_HDESC;

      ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(parent);

      return ODBCConnection::ExecuteWithDiagnostics(connection, SQL_ERROR, [=]() {
        std::shared_ptr<ODBCDescriptor> descriptor = connection->CreateDescriptor();

        if (descriptor) {
          *result = reinterpret_cast<SQLHDESC>(descriptor.get());

          return SQL_SUCCESS;
        }

        return SQL_ERROR;
      });
    }

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
  ARROW_LOG(DEBUG) << "SQLFreeHandle called with type: " << type
                   << ", handle: " << handle;
  // GH-47706 TODO: Add tests for SQLFreeStmt, pre-requisite requires
  // SQLAllocStmt tests

  // GH-47707 TODO: Add tests for SQL_HANDLE_DESC implementation for
  // descriptor handle
  switch (type) {
    case SQL_HANDLE_ENV: {
      using ODBC::ODBCEnvironment;

      ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(handle);

      if (!environment) {
        return SQL_INVALID_HANDLE;
      }

      delete environment;

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_DBC: {
      using ODBC::ODBCConnection;

      ODBCConnection* conn = reinterpret_cast<ODBCConnection*>(handle);

      if (!conn) {
        return SQL_INVALID_HANDLE;
      }

      // `ReleaseConnection` does the equivalent of `delete`.
      // `ReleaseConnection` removes the connection `shared_ptr` from the `std::vector` of
      // connections, and the `shared_ptr` is automatically destructed afterwards.
      conn->ReleaseConnection();

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_STMT: {
      using ODBC::ODBCStatement;

      ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);

      if (!statement) {
        return SQL_INVALID_HANDLE;
      }

      statement->ReleaseStatement();

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_DESC: {
      using ODBC::ODBCDescriptor;

      ODBCDescriptor* descriptor = reinterpret_cast<ODBCDescriptor*>(handle);

      if (!descriptor) {
        return SQL_INVALID_HANDLE;
      }

      descriptor->ReleaseDescriptor();

      return SQL_SUCCESS;
    }

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLFreeStmt(SQLHSTMT handle, SQLUSMALLINT option) {
  ARROW_LOG(DEBUG) << "SQLFreeStmt called with handle: " << handle
                   << ", option: " << option;
  // GH-47706 TODO: Implement SQLFreeStmt
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetDiagField(SQLSMALLINT handle_type, SQLHANDLE handle,
                          SQLSMALLINT rec_number, SQLSMALLINT diag_identifier,
                          SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                          SQLSMALLINT* string_length_ptr) {
  // GH-46573 TODO: Implement additional fields types
  ARROW_LOG(DEBUG) << "SQLGetDiagFieldW called with handle_type: " << handle_type
                   << ", handle: " << handle << ", rec_number: " << rec_number
                   << ", diag_identifier: " << diag_identifier
                   << ", diag_info_ptr: " << diag_info_ptr
                   << ", buffer_length: " << buffer_length << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);
  // GH-46575 TODO: Implement SQLGetDiagField
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT handle_type, SQLHANDLE handle, SQLSMALLINT rec_number,
                        SQLWCHAR* sql_state, SQLINTEGER* native_error_ptr,
                        SQLWCHAR* message_text, SQLSMALLINT buffer_length,
                        SQLSMALLINT* text_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetDiagRecW called with handle_type: " << handle_type
                   << ", handle: " << handle << ", rec_number: " << rec_number
                   << ", sql_state: " << static_cast<const void*>(sql_state)
                   << ", native_error_ptr: " << static_cast<const void*>(native_error_ptr)
                   << ", message_text: " << static_cast<const void*>(message_text)
                   << ", buffer_length: " << buffer_length
                   << ", text_length_ptr: " << static_cast<const void*>(text_length_ptr);
  // GH-46575 TODO: Implement SQLGetDiagRec
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                        SQLINTEGER buffer_length, SQLINTEGER* str_len_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetEnvAttr called with env: " << env << ", attr: " << attr
                   << ", value_ptr: " << value_ptr << ", buffer_length: " << buffer_length
                   << ", str_len_ptr: " << static_cast<const void*>(str_len_ptr);

  using ODBC::ODBCEnvironment;

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  return ODBCEnvironment::ExecuteWithDiagnostics(environment, SQL_ERROR, [=]() {
    switch (attr) {
      case SQL_ATTR_ODBC_VERSION: {
        if (!value_ptr && !str_len_ptr) {
          throw DriverException("Invalid null pointer for attribute.", "HY000");
        }

        if (value_ptr) {
          SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(value_ptr);
          *value = static_cast<SQLSMALLINT>(environment->GetODBCVersion());
        }

        if (str_len_ptr) {
          *str_len_ptr = sizeof(SQLINTEGER);
        }

        return SQL_SUCCESS;
      }

      case SQL_ATTR_OUTPUT_NTS: {
        if (!value_ptr && !str_len_ptr) {
          throw DriverException("Invalid null pointer for attribute.", "HY000");
        }

        if (value_ptr) {
          // output nts always returns SQL_TRUE
          SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(value_ptr);
          *value = SQL_TRUE;
        }

        if (str_len_ptr) {
          *str_len_ptr = sizeof(SQLINTEGER);
        }

        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_POOLING: {
        throw DriverException("Optional feature not supported.", "HYC00");
      }

      default: {
        throw DriverException("Invalid attribute", "HYC00");
      }
    }
  });
}

SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER value_ptr,
                        SQLINTEGER str_len) {
  ARROW_LOG(DEBUG) << "SQLSetEnvAttr called with env: " << env << ", attr: " << attr
                   << ", value_ptr: " << value_ptr << ", str_len: " << str_len;

  using ODBC::ODBCEnvironment;

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  return ODBCEnvironment::ExecuteWithDiagnostics(environment, SQL_ERROR, [=]() {
    if (!value_ptr) {
      throw DriverException("Invalid null pointer for attribute.", "HY024");
    }

    switch (attr) {
      case SQL_ATTR_ODBC_VERSION: {
        SQLINTEGER version =
            static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value_ptr));
        if (version == SQL_OV_ODBC2 || version == SQL_OV_ODBC3) {
          environment->SetODBCVersion(version);

          return SQL_SUCCESS;
        } else {
          throw DriverException("Invalid value for attribute", "HY024");
        }
      }

      case SQL_ATTR_OUTPUT_NTS: {
        // output nts can not be set to SQL_FALSE, is always SQL_TRUE
        SQLINTEGER value = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(value_ptr));
        if (value == SQL_TRUE) {
          return SQL_SUCCESS;
        } else {
          throw DriverException("Invalid value for attribute", "HY024");
        }
      }

      case SQL_ATTR_CONNECTION_POOLING: {
        throw DriverException("Optional feature not supported.", "HYC00");
      }

      default: {
        throw DriverException("Invalid attribute", "HY092");
      }
    }
  });
}

SQLRETURN SQLGetConnectAttr(SQLHDBC conn, SQLINTEGER attribute, SQLPOINTER value_ptr,
                            SQLINTEGER buffer_length, SQLINTEGER* string_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetConnectAttrW called with conn: " << conn
                   << ", attribute: " << attribute << ", value_ptr: " << value_ptr
                   << ", buffer_length: " << buffer_length << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);
  // GH-47708 TODO: Implement SQLGetConnectAttr
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value_ptr,
                            SQLINTEGER value_len) {
  ARROW_LOG(DEBUG) << "SQLSetConnectAttrW called with conn: " << conn
                   << ", attr: " << attr << ", value_ptr: " << value_ptr
                   << ", value_len: " << value_len;
  // GH-47708 TODO: Implement SQLSetConnectAttr
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND window_handle,
                           SQLWCHAR* in_connection_string,
                           SQLSMALLINT in_connection_string_len,
                           SQLWCHAR* out_connection_string,
                           SQLSMALLINT out_connection_string_buffer_len,
                           SQLSMALLINT* out_connection_string_len,
                           SQLUSMALLINT driver_completion) {
  ARROW_LOG(DEBUG) << "SQLDriverConnectW called with conn: " << conn
                   << ", window_handle: " << static_cast<const void*>(window_handle)
                   << ", in_connection_string: "
                   << static_cast<const void*>(in_connection_string)
                   << ", in_connection_string_len: " << in_connection_string_len
                   << ", out_connection_string: "
                   << static_cast<const void*>(out_connection_string)
                   << ", out_connection_string_buffer_len: "
                   << out_connection_string_buffer_len << ", out_connection_string_len: "
                   << static_cast<const void*>(out_connection_string_len)
                   << ", driver_completion: " << driver_completion;
  // GH-46449 TODO: Implement FILEDSN and SAVEFILE keywords according to the spec

  // GH-46560 TODO: Copy connection string properly in SQLDriverConnect according to the
  // spec

  // GH-46574 TODO: Implement SQLDriverConnect
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLConnect(SQLHDBC conn, SQLWCHAR* dsn_name, SQLSMALLINT dsn_name_len,
                     SQLWCHAR* user_name, SQLSMALLINT user_name_len, SQLWCHAR* password,
                     SQLSMALLINT password_len) {
  ARROW_LOG(DEBUG) << "SQLConnectW called with conn: " << conn
                   << ", dsn_name: " << static_cast<const void*>(dsn_name)
                   << ", dsn_name_len: " << dsn_name_len
                   << ", user_name: " << static_cast<const void*>(user_name)
                   << ", user_name_len: " << user_name_len
                   << ", password: " << static_cast<const void*>(password)
                   << ", password_len: " << password_len;
  // GH-46574 TODO: Implement SQLConnect
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLDisconnect(SQLHDBC conn) {
  ARROW_LOG(DEBUG) << "SQLDisconnect called with conn: " << conn;
  // GH-46574 TODO: Implement SQLDisconnect
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                     SQLSMALLINT buf_len, SQLSMALLINT* string_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetInfoW called with conn: " << conn
                   << ", info_type: " << info_type
                   << ", info_value_ptr: " << info_value_ptr << ", buf_len: " << buf_len
                   << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);
  // GH-47709 TODO: Implement SQLGetInfo
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER buffer_length, SQLINTEGER* string_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetStmtAttrW called with stmt: " << stmt
                   << ", attribute: " << attribute << ", value_ptr: " << value_ptr
                   << ", buffer_length: " << buffer_length << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);
  // GH-47710 TODO: Implement SQLGetStmtAttr
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER string_length) {
  ARROW_LOG(DEBUG) << "SQLSetStmtAttrW called with stmt: " << stmt
                   << ", attribute: " << attribute << ", value_ptr: " << value_ptr
                   << ", string_length: " << string_length;
  // GH-47710 TODO: Implement SQLSetStmtAttr
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* query_text, SQLINTEGER text_length) {
  ARROW_LOG(DEBUG) << "SQLExecDirectW called with stmt: " << stmt
                   << ", query_text: " << static_cast<const void*>(query_text)
                   << ", text_length: " << text_length;
  // GH-47711 TODO: Implement SQLExecDirect
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLWCHAR* query_text, SQLINTEGER text_length) {
  ARROW_LOG(DEBUG) << "SQLPrepareW called with stmt: " << stmt
                   << ", query_text: " << static_cast<const void*>(query_text)
                   << ", text_length: " << text_length;
  // GH-47712 TODO: Implement SQLPrepare
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLExecute(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLExecute called with stmt: " << stmt;
  // GH-47712 TODO: Implement SQLExecute
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLFetch(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLFetch called with stmt: " << stmt;
  // GH-47713 TODO: Implement SQLFetch
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLExtendedFetch(SQLHSTMT stmt, SQLUSMALLINT fetch_orientation,
                           SQLLEN fetch_offset, SQLULEN* row_count_ptr,
                           SQLUSMALLINT* row_status_array) {
  // GH-47110 TODO: SQLExtendedFetch should return SQL_SUCCESS_WITH_INFO for certain diag
  // states
  ARROW_LOG(DEBUG) << "SQLExtendedFetch called with stmt: " << stmt
                   << ", fetch_orientation: " << fetch_orientation
                   << ", fetch_offset: " << fetch_offset
                   << ", row_count_ptr: " << static_cast<const void*>(row_count_ptr)
                   << ", row_status_array: "
                   << static_cast<const void*>(row_status_array);
  // GH-47714 TODO: Implement SQLExtendedFetch
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetch_orientation,
                         SQLLEN fetch_offset) {
  ARROW_LOG(DEBUG) << "SQLFetchScroll called with stmt: " << stmt
                   << ", fetch_orientation: " << fetch_orientation
                   << ", fetch_offset: " << fetch_offset;
  // GH-47715 TODO: Implement SQLFetchScroll
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT record_number, SQLSMALLINT c_type,
                     SQLPOINTER data_ptr, SQLLEN buffer_length, SQLLEN* indicator_ptr) {
  ARROW_LOG(DEBUG) << "SQLBindCol called with stmt: " << stmt
                   << ", record_number: " << record_number << ", c_type: " << c_type
                   << ", data_ptr: " << data_ptr << ", buffer_length: " << buffer_length
                   << ", indicator_ptr: " << static_cast<const void*>(indicator_ptr);
  // GH-47716 TODO: Implement SQLBindCol
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLCloseCursor(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLCloseCursor called with stmt: " << stmt;
  // GH-47717 TODO: Implement SQLCloseCursor
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetData(SQLHSTMT stmt, SQLUSMALLINT record_number, SQLSMALLINT c_type,
                     SQLPOINTER data_ptr, SQLLEN buffer_length, SQLLEN* indicator_ptr) {
  // GH-46979 TODO: support SQL_C_GUID data type
  // GH-46980 TODO: support Interval data types
  // GH-46985 TODO: return warning message instead of error on float truncation case
  ARROW_LOG(DEBUG) << "SQLGetData called with stmt: " << stmt
                   << ", record_number: " << record_number << ", c_type: " << c_type
                   << ", data_ptr: " << data_ptr << ", buffer_length: " << buffer_length
                   << ", indicator_ptr: " << static_cast<const void*>(indicator_ptr);
  // GH-47713 TODO: Implement SQLGetData
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLMoreResults(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLMoreResults called with stmt: " << stmt;
  // GH-47713 TODO: Implement SQLMoreResults
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* column_count_ptr) {
  ARROW_LOG(DEBUG) << "SQLNumResultCols called with stmt: " << stmt
                   << ", column_count_ptr: "
                   << static_cast<const void*>(column_count_ptr);
  // GH-47713 TODO: Implement SQLNumResultCols
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* row_count_ptr) {
  ARROW_LOG(DEBUG) << "SQLRowCount called with stmt: " << stmt
                   << ", column_count_ptr: " << static_cast<const void*>(row_count_ptr);
  // GH-47713 TODO: Implement SQLRowCount
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLTables(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                    SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                    SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                    SQLSMALLINT table_name_length, SQLWCHAR* table_type,
                    SQLSMALLINT table_type_length) {
  ARROW_LOG(DEBUG) << "SQLTablesW called with stmt: " << stmt
                   << ", catalog_name: " << static_cast<const void*>(catalog_name)
                   << ", catalog_name_length: " << catalog_name_length
                   << ", schema_name: " << static_cast<const void*>(schema_name)
                   << ", schema_name_length: " << schema_name_length
                   << ", table_name: " << static_cast<const void*>(table_name)
                   << ", table_name_length: " << table_name_length
                   << ", table_type: " << static_cast<const void*>(table_type)
                   << ", table_type_length: " << table_type_length;
  // GH-47719 TODO: Implement SQLTables
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLColumns(SQLHSTMT stmt, SQLWCHAR* catalog_name,
                     SQLSMALLINT catalog_name_length, SQLWCHAR* schema_name,
                     SQLSMALLINT schema_name_length, SQLWCHAR* table_name,
                     SQLSMALLINT table_name_length, SQLWCHAR* column_name,
                     SQLSMALLINT column_name_length) {
  // GH-47159 TODO: Return NUM_PREC_RADIX based on whether COLUMN_SIZE contains number of
  // digits or bits
  ARROW_LOG(DEBUG) << "SQLColumnsW called with stmt: " << stmt
                   << ", catalog_name: " << static_cast<const void*>(catalog_name)
                   << ", catalog_name_length: " << catalog_name_length
                   << ", schema_name: " << static_cast<const void*>(schema_name)
                   << ", schema_name_length: " << schema_name_length
                   << ", table_name: " << static_cast<const void*>(table_name)
                   << ", table_name_length: " << table_name_length
                   << ", column_name: " << static_cast<const void*>(column_name)
                   << ", column_name_length: " << column_name_length;
  // GH-47720 TODO: Implement SQLColumns
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLColAttribute(SQLHSTMT stmt, SQLUSMALLINT record_number,
                          SQLUSMALLINT field_identifier,
                          SQLPOINTER character_attribute_ptr, SQLSMALLINT buffer_length,
                          SQLSMALLINT* output_length, SQLLEN* numeric_attribute_ptr) {
  ARROW_LOG(DEBUG) << "SQLColAttributeW called with stmt: " << stmt
                   << ", record_number: " << record_number
                   << ", field_identifier: " << field_identifier
                   << ", character_attribute_ptr: " << character_attribute_ptr
                   << ", buffer_length: " << buffer_length
                   << ", output_length: " << static_cast<const void*>(output_length)
                   << ", numeric_attribute_ptr: "
                   << static_cast<const void*>(numeric_attribute_ptr);
  // GH-47721 TODO: Implement SQLColAttribute, pre-requisite requires SQLColumns
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT data_type) {
  // GH-47237 TODO: return SQL_PRED_CHAR and SQL_PRED_BASIC for
  // appropriate data types in `SEARCHABLE` field
  ARROW_LOG(DEBUG) << "SQLGetTypeInfoW called with stmt: " << stmt
                   << " data_type: " << data_type;
  // GH-47722 TODO: Implement SQLGetTypeInfo
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLNativeSql(SQLHDBC conn, SQLWCHAR* in_statement_text,
                       SQLINTEGER in_statement_text_length, SQLWCHAR* out_statement_text,
                       SQLINTEGER buffer_length, SQLINTEGER* out_statement_text_length) {
  ARROW_LOG(DEBUG) << "SQLNativeSqlW called with connection_handle: " << conn
                   << ", in_statement_text: "
                   << static_cast<const void*>(in_statement_text)
                   << ", in_statement_text_length: " << in_statement_text_length
                   << ", out_statement_text: "
                   << static_cast<const void*>(out_statement_text)
                   << ", buffer_length: " << buffer_length
                   << ", out_statement_text_length: "
                   << static_cast<const void*>(out_statement_text_length);
  // GH-47723 TODO: Implement SQLNativeSql
  return SQL_INVALID_HANDLE;
}

SQLRETURN SQLDescribeCol(SQLHSTMT stmt, SQLUSMALLINT column_number, SQLWCHAR* column_name,
                         SQLSMALLINT buffer_length, SQLSMALLINT* name_length_ptr,
                         SQLSMALLINT* data_type_ptr, SQLULEN* column_size_ptr,
                         SQLSMALLINT* decimal_digits_ptr, SQLSMALLINT* nullable_ptr) {
  ARROW_LOG(DEBUG) << "SQLDescribeColW called with stmt: " << stmt
                   << ", column_number: " << column_number
                   << ", column_name: " << static_cast<const void*>(column_name)
                   << ", buffer_length: " << buffer_length
                   << ", name_length_ptr: " << static_cast<const void*>(name_length_ptr)
                   << ", data_type_ptr: " << static_cast<const void*>(data_type_ptr)
                   << ", column_size_ptr: " << static_cast<const void*>(column_size_ptr)
                   << ", decimal_digits_ptr: "
                   << static_cast<const void*>(decimal_digits_ptr)
                   << ", nullable_ptr: " << static_cast<const void*>(nullable_ptr);
  // GH-47724 TODO: Implement SQLDescribeCol
  return SQL_INVALID_HANDLE;
}

}  // namespace arrow::flight::sql::odbc
