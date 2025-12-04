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

#if defined _WIN32
// For displaying DSN Window
#  include "arrow/flight/sql/odbc/odbc_impl/system_dsn.h"
#endif  // defined(_WIN32)

namespace arrow::flight::sql::odbc {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  ARROW_LOG(DEBUG) << "SQLAllocHandle called with type: " << type
                   << ", parent: " << parent
                   << ", result: " << static_cast<const void*>(result);
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

  switch (option) {
    case SQL_CLOSE: {
      using ODBC::ODBCStatement;

      return ODBCStatement::ExecuteWithDiagnostics(handle, SQL_ERROR, [=]() {
        ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);

        // Close cursor with suppressErrors set to true
        statement->CloseCursor(true);

        return SQL_SUCCESS;
      });
    }

    case SQL_DROP: {
      return SQLFreeHandle(SQL_HANDLE_STMT, handle);
    }

    case SQL_UNBIND: {
      using ODBC::ODBCDescriptor;
      using ODBC::ODBCStatement;
      return ODBCStatement::ExecuteWithDiagnostics(handle, SQL_ERROR, [=]() {
        ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);
        ODBCDescriptor* ard = statement->GetARD();
        // Unbind columns
        ard->SetHeaderField(SQL_DESC_COUNT, (void*)0, 0);
        return SQL_SUCCESS;
      });
    }

    // SQLBindParameter is not supported
    case SQL_RESET_PARAMS: {
      return SQL_SUCCESS;
    }
  }

  return SQL_ERROR;
}

#if defined(__APPLE__)
SQLRETURN SQLError(SQLHENV env, SQLHDBC conn, SQLHSTMT stmt, SQLWCHAR* sql_state,
                   SQLINTEGER* native_error_ptr, SQLWCHAR* message_text,
                   SQLSMALLINT buffer_length, SQLSMALLINT* text_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLError called with env: " << env << ", conn: " << conn
                   << ", stmt: " << stmt
                   << ", sql_state: " << static_cast<const void*>(sql_state)
                   << ", native_error_ptr: " << static_cast<const void*>(native_error_ptr)
                   << ", message_text: " << static_cast<const void*>(message_text)
                   << ", buffer_length: " << buffer_length
                   << ", text_length_ptr: " << static_cast<const void*>(text_length_ptr);

  SQLSMALLINT handle_type;
  SQLHANDLE handle;

  if (env) {
    handle_type = SQL_HANDLE_ENV;
    handle = static_cast<SQLHANDLE>(env);
  } else if (conn) {
    handle_type = SQL_HANDLE_DBC;
    handle = static_cast<SQLHANDLE>(conn);
  } else if (stmt) {
    handle_type = SQL_HANDLE_STMT;
    handle = static_cast<SQLHANDLE>(stmt);
  } else {
    return static_cast<SQLRETURN>(SQL_INVALID_HANDLE);
  }

  // Use the last record
  SQLINTEGER diag_number;
  SQLSMALLINT diag_number_length;

  SQLRETURN ret = arrow::flight::sql::odbc::SQLGetDiagField(
      handle_type, handle, 0, SQL_DIAG_NUMBER, &diag_number, sizeof(SQLINTEGER), 0);
  if (ret != SQL_SUCCESS) {
    return ret;
  }

  if (diag_number == 0) {
    return SQL_NO_DATA;
  }

  SQLSMALLINT rec_number = static_cast<SQLSMALLINT>(diag_number);

  return arrow::flight::sql::odbc::SQLGetDiagRec(
      handle_type, handle, rec_number, sql_state, native_error_ptr, message_text,
      buffer_length, text_length_ptr);
}
#endif  // __APPLE__

inline bool IsValidStringFieldArgs(SQLPOINTER diag_info_ptr, SQLSMALLINT buffer_length,
                                   SQLSMALLINT* string_length_ptr, bool is_unicode) {
  const SQLSMALLINT char_size = is_unicode ? GetSqlWCharSize() : sizeof(char);
  const bool has_valid_buffer =
      buffer_length == SQL_NTS || (buffer_length >= 0 && buffer_length % char_size == 0);

  // regardless of capacity return false if invalid
  if (diag_info_ptr && !has_valid_buffer) {
    return false;
  }

  return has_valid_buffer || string_length_ptr;
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
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCDescriptor;
  using ODBC::ODBCEnvironment;
  using ODBC::ODBCStatement;

  if (!handle) {
    return SQL_INVALID_HANDLE;
  }

  if (!diag_info_ptr && !string_length_ptr) {
    return SQL_ERROR;
  }

  // If buffer length derived from null terminated string
  if (diag_info_ptr && buffer_length == SQL_NTS) {
    const wchar_t* str = reinterpret_cast<wchar_t*>(diag_info_ptr);
    buffer_length = wcslen(str) * GetSqlWCharSize();
  }

  // Set character type to be Unicode by default
  const bool is_unicode = true;
  Diagnostics* diagnostics = nullptr;

  switch (handle_type) {
    case SQL_HANDLE_ENV: {
      ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(handle);
      diagnostics = &environment->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DBC: {
      ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(handle);
      diagnostics = &connection->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DESC: {
      ODBCDescriptor* descriptor = reinterpret_cast<ODBCDescriptor*>(handle);
      diagnostics = &descriptor->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_STMT: {
      ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);
      diagnostics = &statement->GetDiagnostics();
      break;
    }

    default:
      return SQL_ERROR;
  }

  if (!diagnostics) {
    return SQL_ERROR;
  }

  // Retrieve and return if header level diagnostics
  switch (diag_identifier) {
    case SQL_DIAG_NUMBER: {
      if (diag_info_ptr) {
        *static_cast<SQLINTEGER*>(diag_info_ptr) =
            static_cast<SQLINTEGER>(diagnostics->GetRecordCount());
      }

      if (string_length_ptr) {
        *string_length_ptr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    // Driver manager implements SQL_DIAG_RETURNCODE
    case SQL_DIAG_RETURNCODE: {
      return SQL_SUCCESS;
    }

    case SQL_DIAG_CURSOR_ROW_COUNT: {
      if (handle_type == SQL_HANDLE_STMT) {
        if (diag_info_ptr) {
          // Will always be 0 if only SELECT supported
          *static_cast<SQLLEN*>(diag_info_ptr) = 0;
        }

        if (string_length_ptr) {
          *string_length_ptr = sizeof(SQLLEN);
        }

        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }

    // Not supported
    case SQL_DIAG_DYNAMIC_FUNCTION:
    case SQL_DIAG_DYNAMIC_FUNCTION_CODE: {
      if (handle_type == SQL_HANDLE_STMT) {
        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_ROW_COUNT: {
      if (handle_type == SQL_HANDLE_STMT) {
        if (diag_info_ptr) {
          // Will always be 0 if only SELECT is supported
          *static_cast<SQLLEN*>(diag_info_ptr) = 0;
        }

        if (string_length_ptr) {
          *string_length_ptr = sizeof(SQLLEN);
        }

        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }
  }

  // If not a diagnostic header field then the record number must be 1 or greater
  if (rec_number < 1) {
    return SQL_ERROR;
  }

  // Retrieve record level diagnostics from specified 1 based record
  const uint32_t record_index = static_cast<uint32_t>(rec_number - 1);
  if (!diagnostics->HasRecord(record_index)) {
    return SQL_NO_DATA;
  }

  // Retrieve record field data
  switch (diag_identifier) {
    case SQL_DIAG_MESSAGE_TEXT: {
      if (IsValidStringFieldArgs(diag_info_ptr, buffer_length, string_length_ptr,
                                 is_unicode)) {
        const std::string& message = diagnostics->GetMessageText(record_index);
        return GetStringAttribute(is_unicode, message, true, diag_info_ptr, buffer_length,
                                  string_length_ptr, *diagnostics);
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_NATIVE: {
      if (diag_info_ptr) {
        *static_cast<SQLINTEGER*>(diag_info_ptr) =
            diagnostics->GetNativeError(record_index);
      }

      if (string_length_ptr) {
        *string_length_ptr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    case SQL_DIAG_SERVER_NAME: {
      if (IsValidStringFieldArgs(diag_info_ptr, buffer_length, string_length_ptr,
                                 is_unicode)) {
        switch (handle_type) {
          case SQL_HANDLE_DBC: {
            ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(handle);
            std::string dsn = connection->GetDSN();
            return GetStringAttribute(is_unicode, dsn, true, diag_info_ptr, buffer_length,
                                      string_length_ptr, *diagnostics);
          }

          case SQL_HANDLE_DESC: {
            ODBCDescriptor* descriptor = reinterpret_cast<ODBCDescriptor*>(handle);
            ODBCConnection* connection = &descriptor->GetConnection();
            std::string dsn = connection->GetDSN();
            return GetStringAttribute(is_unicode, dsn, true, diag_info_ptr, buffer_length,
                                      string_length_ptr, *diagnostics);
            break;
          }

          case SQL_HANDLE_STMT: {
            ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);
            ODBCConnection* connection = &statement->GetConnection();
            std::string dsn = connection->GetDSN();
            return GetStringAttribute(is_unicode, dsn, true, diag_info_ptr, buffer_length,
                                      string_length_ptr, *diagnostics);
          }

          default:
            return SQL_ERROR;
        }
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_SQLSTATE: {
      if (IsValidStringFieldArgs(diag_info_ptr, buffer_length, string_length_ptr,
                                 is_unicode)) {
        const std::string& state = diagnostics->GetSQLState(record_index);
        return GetStringAttribute(is_unicode, state, true, diag_info_ptr, buffer_length,
                                  string_length_ptr, *diagnostics);
      }

      return SQL_ERROR;
    }

    // Return valid dummy variable for unimplemented field
    case SQL_DIAG_COLUMN_NUMBER: {
      if (diag_info_ptr) {
        *static_cast<SQLINTEGER*>(diag_info_ptr) = SQL_NO_COLUMN_NUMBER;
      }

      if (string_length_ptr) {
        *string_length_ptr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    // Return empty string dummy variable for unimplemented fields
    case SQL_DIAG_CLASS_ORIGIN:
    case SQL_DIAG_CONNECTION_NAME:
    case SQL_DIAG_SUBCLASS_ORIGIN: {
      if (IsValidStringFieldArgs(diag_info_ptr, buffer_length, string_length_ptr,
                                 is_unicode)) {
        return GetStringAttribute(is_unicode, "", true, diag_info_ptr, buffer_length,
                                  string_length_ptr, *diagnostics);
      }

      return SQL_ERROR;
    }

    // Return valid dummy variable for unimplemented field
    case SQL_DIAG_ROW_NUMBER: {
      if (diag_info_ptr) {
        *static_cast<SQLLEN*>(diag_info_ptr) = SQL_NO_ROW_NUMBER;
      }

      if (string_length_ptr) {
        *string_length_ptr = sizeof(SQLLEN);
      }

      return SQL_SUCCESS;
    }

    default: {
      return SQL_ERROR;
    }
  }

  return SQL_ERROR;
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
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCDescriptor;
  using ODBC::ODBCEnvironment;
  using ODBC::ODBCStatement;

  if (!handle) {
    return SQL_INVALID_HANDLE;
  }

  // Record number must be greater or equal to 1
  if (rec_number < 1 || buffer_length < 0) {
    return SQL_ERROR;
  }

  // Set character type to be Unicode by default
  const bool is_unicode = true;
  Diagnostics* diagnostics = nullptr;

  switch (handle_type) {
    case SQL_HANDLE_ENV: {
      auto* environment = ODBCEnvironment::Of(handle);
      diagnostics = &environment->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DBC: {
      auto* connection = ODBCConnection::Of(handle);
      diagnostics = &connection->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DESC: {
      auto* descriptor = ODBCDescriptor::Of(handle);
      diagnostics = &descriptor->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_STMT: {
      auto* statement = ODBCStatement::Of(handle);
      diagnostics = &statement->GetDiagnostics();
      break;
    }

    default:
      return SQL_INVALID_HANDLE;
  }

  if (!diagnostics) {
    return SQL_ERROR;
  }

  // Convert from ODBC 1 based record number to internal diagnostics 0 indexed storage
  const size_t record_index = static_cast<size_t>(rec_number - 1);
  if (!diagnostics->HasRecord(record_index)) {
    return SQL_NO_DATA;
  }

  if (sql_state) {
    // The length of the sql state is always 5 characters plus null
    SQLSMALLINT size = 6;
    const std::string& state = diagnostics->GetSQLState(record_index);

    // Microsoft documentation does not mention
    // any SQLGetDiagRec return value that is associated with `sql_state` buffer, so
    // the return value for writing `sql_state` buffer is ignored by the driver.
    ARROW_UNUSED(GetStringAttribute(is_unicode, state, false, sql_state, size, &size,
                                    *diagnostics));
  }

  if (native_error_ptr) {
    *native_error_ptr = diagnostics->GetNativeError(record_index);
  }

  if (message_text || text_length_ptr) {
    const std::string& message = diagnostics->GetMessageText(record_index);

    // According to Microsoft documentation,
    // SQL_SUCCESS_WITH_INFO should be returned if `*message_text` buffer was too
    // small to hold the requested diagnostic message.
    return GetStringAttribute(is_unicode, message, false, message_text, buffer_length,
                              text_length_ptr, *diagnostics);
  }

  return SQL_SUCCESS;
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
  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    const bool is_unicode = true;
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    return connection->GetConnectAttr(attribute, value_ptr, buffer_length,
                                      string_length_ptr, is_unicode);
  });
}

SQLRETURN SQLSetConnectAttr(SQLHDBC conn, SQLINTEGER attr, SQLPOINTER value_ptr,
                            SQLINTEGER value_len) {
  ARROW_LOG(DEBUG) << "SQLSetConnectAttrW called with conn: " << conn
                   << ", attr: " << attr << ", value_ptr: " << value_ptr
                   << ", value_len: " << value_len;

  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    const bool is_unicode = true;
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    connection->SetConnectAttr(attr, value_ptr, value_len, is_unicode);
    return SQL_SUCCESS;
  });
}

// Load properties from the given DSN. The properties loaded do _not_ overwrite existing
// entries in the properties.
void LoadPropertiesFromDSN(const std::string& dsn,
                           Connection::ConnPropertyMap& properties) {
  config::Configuration config;
  config.LoadDsn(dsn);
  Connection::ConnPropertyMap dsn_properties = config.GetProperties();
  for (auto& [key, value] : dsn_properties) {
    auto prop_iter = properties.find(key);
    if (prop_iter == properties.end()) {
      properties.emplace(std::make_pair(std::move(key), std::move(value)));
    }
  }
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

  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string connection_string =
        ODBC::SqlWcharToString(in_connection_string, in_connection_string_len);
    Connection::ConnPropertyMap properties;
    std::string dsn_value = "";
    std::optional<std::string> dsn = ODBCConnection::GetDsnIfExists(connection_string);
    if (dsn.has_value()) {
      dsn_value = dsn.value();
      LoadPropertiesFromDSN(dsn_value, properties);
    }
    ODBCConnection::GetPropertiesFromConnString(connection_string, properties);

    std::vector<std::string_view> missing_properties;

    // GH-46448 TODO: Implement SQL_DRIVER_COMPLETE_REQUIRED in SQLDriverConnect according
    // to the spec
#if defined _WIN32
    // Load the DSN window according to driver_completion
    if (driver_completion == SQL_DRIVER_PROMPT) {
      // Load DSN window before first attempt to connect
      config::Configuration config;
      if (!DisplayConnectionWindow(window_handle, config, properties)) {
        return static_cast<SQLRETURN>(SQL_NO_DATA);
      }
      connection->Connect(dsn_value, properties, missing_properties);
    } else if (driver_completion == SQL_DRIVER_COMPLETE ||
               driver_completion == SQL_DRIVER_COMPLETE_REQUIRED) {
      try {
        connection->Connect(dsn_value, properties, missing_properties);
      } catch (const DriverException&) {
        // If first connection fails due to missing attributes, load
        // the DSN window and try to connect again
        if (!missing_properties.empty()) {
          config::Configuration config;
          missing_properties.clear();

          if (!DisplayConnectionWindow(window_handle, config, properties)) {
            return static_cast<SQLRETURN>(SQL_NO_DATA);
          }
          connection->Connect(dsn_value, properties, missing_properties);
        } else {
          throw;
        }
      }
    } else {
      // Default case: attempt connection without showing DSN window
      connection->Connect(dsn_value, properties, missing_properties);
    }
#else
    // Attempt connection without loading DSN window on macOS/Linux
    connection->Connect(dsn_value, properties, missing_properties);
#endif
    // Copy connection string to out_connection_string after connection attempt
    return ODBC::GetStringAttribute(true, connection_string, false, out_connection_string,
                                    out_connection_string_buffer_len,
                                    out_connection_string_len,
                                    connection->GetDiagnostics());
  });
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

  using ODBC::ODBCConnection;

  using ODBC::SqlWcharToString;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string dsn = SqlWcharToString(dsn_name, dsn_name_len);

    config::Configuration config;
    config.LoadDsn(dsn);

    if (user_name) {
      std::string uid = SqlWcharToString(user_name, user_name_len);
      config.Emplace(FlightSqlConnection::UID, std::move(uid));
    }

    if (password) {
      std::string pwd = SqlWcharToString(password, password_len);
      config.Emplace(FlightSqlConnection::PWD, std::move(pwd));
    }

    std::vector<std::string_view> missing_properties;

    connection->Connect(dsn, config.GetProperties(), missing_properties);

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLDisconnect(SQLHDBC conn) {
  ARROW_LOG(DEBUG) << "SQLDisconnect called with conn: " << conn;

  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    connection->Disconnect();

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT info_type, SQLPOINTER info_value_ptr,
                     SQLSMALLINT buf_len, SQLSMALLINT* string_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetInfoW called with conn: " << conn
                   << ", info_type: " << info_type
                   << ", info_value_ptr: " << info_value_ptr << ", buf_len: " << buf_len
                   << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);

  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    // Set character type to be Unicode by default
    const bool is_unicode = true;

    if (!info_value_ptr && !string_length_ptr) {
      return static_cast<SQLRETURN>(SQL_ERROR);
    }

    return connection->GetInfo(info_type, info_value_ptr, buf_len, string_length_ptr,
                               is_unicode);
  });
}

SQLRETURN SQLGetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER buffer_length, SQLINTEGER* string_length_ptr) {
  ARROW_LOG(DEBUG) << "SQLGetStmtAttrW called with stmt: " << stmt
                   << ", attribute: " << attribute << ", value_ptr: " << value_ptr
                   << ", buffer_length: " << buffer_length << ", string_length_ptr: "
                   << static_cast<const void*>(string_length_ptr);

  using ODBC::ODBCStatement;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    bool is_unicode = true;

    statement->GetStmtAttr(attribute, value_ptr, buffer_length, string_length_ptr,
                           is_unicode);

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLSetStmtAttr(SQLHSTMT stmt, SQLINTEGER attribute, SQLPOINTER value_ptr,
                         SQLINTEGER string_length) {
  ARROW_LOG(DEBUG) << "SQLSetStmtAttrW called with stmt: " << stmt
                   << ", attribute: " << attribute << ", value_ptr: " << value_ptr
                   << ", string_length: " << string_length;

  using ODBC::ODBCStatement;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    bool is_unicode = true;

    statement->SetStmtAttr(attribute, value_ptr, string_length, is_unicode);

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLExecDirect(SQLHSTMT stmt, SQLWCHAR* query_text, SQLINTEGER text_length) {
  ARROW_LOG(DEBUG) << "SQLExecDirectW called with stmt: " << stmt
                   << ", query_text: " << static_cast<const void*>(query_text)
                   << ", text_length: " << text_length;

  using ODBC::ODBCStatement;
  // The driver is built to handle SELECT statements only.
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    std::string query = ODBC::SqlWcharToString(query_text, text_length);

    statement->Prepare(query);
    statement->ExecutePrepared();

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLPrepare(SQLHSTMT stmt, SQLWCHAR* query_text, SQLINTEGER text_length) {
  ARROW_LOG(DEBUG) << "SQLPrepareW called with stmt: " << stmt
                   << ", query_text: " << static_cast<const void*>(query_text)
                   << ", text_length: " << text_length;

  using ODBC::ODBCStatement;
  // The driver is built to handle SELECT statements only.
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    std::string query = ODBC::SqlWcharToString(query_text, text_length);

    statement->Prepare(query);

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLExecute(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLExecute called with stmt: " << stmt;

  using ODBC::ODBCStatement;
  // The driver is built to handle SELECT statements only.
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    statement->ExecutePrepared();

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLFetch(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLFetch called with stmt: " << stmt;

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    // The SQL_ATTR_ROW_ARRAY_SIZE statement attribute specifies the number of rows in the
    // rowset. Retrieve it with GetArraySize.
    ODBCDescriptor* ard = statement->GetARD();
    size_t rows = static_cast<size_t>(ard->GetArraySize());

    if (statement->Fetch(rows)) {
      return SQL_SUCCESS;
    } else {
      // Reached the end of rowset
      return SQL_NO_DATA;
    }
  });
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

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    if (fetch_orientation != SQL_FETCH_NEXT) {
      throw DriverException("Optional feature not supported.", "HYC00");
    }
    // fetch_offset is ignored as only SQL_FETCH_NEXT is supported

    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    // The SQL_ROWSET_SIZE statement attribute specifies the number of rows in the
    // rowset.
    SQLULEN row_set_size = statement->GetRowsetSize();
    ARROW_LOG(DEBUG) << "SQL_ROWSET_SIZE value for SQLExtendedFetch: " << row_set_size;

    if (statement->Fetch(static_cast<size_t>(row_set_size), row_count_ptr,
                         row_status_array)) {
      return SQL_SUCCESS;
    } else {
      // Reached the end of rowset
      return SQL_NO_DATA;
    }
  });
}

SQLRETURN SQLFetchScroll(SQLHSTMT stmt, SQLSMALLINT fetch_orientation,
                         SQLLEN fetch_offset) {
  ARROW_LOG(DEBUG) << "SQLFetchScroll called with stmt: " << stmt
                   << ", fetch_orientation: " << fetch_orientation
                   << ", fetch_offset: " << fetch_offset;

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    // Only SQL_FETCH_NEXT forward-only fetching orientation is supported,
    // meaning the behavior of SQLExtendedFetch is same as SQLFetch.
    if (fetch_orientation != SQL_FETCH_NEXT) {
      throw DriverException("Optional feature not supported.", "HYC00");
    }
    // Ignore fetch_offset as it's not applicable to SQL_FETCH_NEXT
    ARROW_UNUSED(fetch_offset);

    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    // The SQL_ATTR_ROW_ARRAY_SIZE statement attribute specifies the number of rows in the
    // rowset.
    ODBCDescriptor* ard = statement->GetARD();
    size_t rows = static_cast<size_t>(ard->GetArraySize());
    if (statement->Fetch(rows)) {
      return SQL_SUCCESS;
    } else {
      // Reached the end of rowset
      return SQL_NO_DATA;
    }
  });
}

SQLRETURN SQLBindCol(SQLHSTMT stmt, SQLUSMALLINT record_number, SQLSMALLINT c_type,
                     SQLPOINTER data_ptr, SQLLEN buffer_length, SQLLEN* indicator_ptr) {
  ARROW_LOG(DEBUG) << "SQLBindCol called with stmt: " << stmt
                   << ", record_number: " << record_number << ", c_type: " << c_type
                   << ", data_ptr: " << data_ptr << ", buffer_length: " << buffer_length
                   << ", indicator_ptr: " << static_cast<const void*>(indicator_ptr);

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    // GH-47021 TODO: implement driver to return indicator value when data pointer is null
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    ODBCDescriptor* ard = statement->GetARD();
    ard->BindCol(record_number, c_type, data_ptr, buffer_length, indicator_ptr);
    return SQL_SUCCESS;
  });
}

SQLRETURN SQLCloseCursor(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLCloseCursor called with stmt: " << stmt;

  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    // Close cursor with suppressErrors set to false
    statement->CloseCursor(false);

    return SQL_SUCCESS;
  });
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

  using ODBC::ODBCStatement;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    return statement->GetData(record_number, c_type, data_ptr, buffer_length,
                              indicator_ptr);
  });
}

SQLRETURN SQLMoreResults(SQLHSTMT stmt) {
  ARROW_LOG(DEBUG) << "SQLMoreResults called with stmt: " << stmt;

  using ODBC::ODBCStatement;
  // Multiple result sets are not supported by Arrow protocol. Return SQL_NO_DATA by
  // default to indicate no data is available.
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    return statement->GetMoreResults();
  });
}

SQLRETURN SQLNumResultCols(SQLHSTMT stmt, SQLSMALLINT* column_count_ptr) {
  ARROW_LOG(DEBUG) << "SQLNumResultCols called with stmt: " << stmt
                   << ", column_count_ptr: "
                   << static_cast<const void*>(column_count_ptr);

  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    statement->GetColumnCount(column_count_ptr);
    return SQL_SUCCESS;
  });
}

SQLRETURN SQLRowCount(SQLHSTMT stmt, SQLLEN* row_count_ptr) {
  ARROW_LOG(DEBUG) << "SQLRowCount called with stmt: " << stmt
                   << ", column_count_ptr: " << static_cast<const void*>(row_count_ptr);

  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    statement->GetRowCount(row_count_ptr);
    return SQL_SUCCESS;
  });
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

  using ODBC::ODBCStatement;
  using ODBC::SqlWcharToString;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    std::string catalog = SqlWcharToString(catalog_name, catalog_name_length);
    std::string schema = SqlWcharToString(schema_name, schema_name_length);
    std::string table = SqlWcharToString(table_name, table_name_length);
    std::string type = SqlWcharToString(table_type, table_type_length);

    statement->GetTables(catalog_name ? &catalog : nullptr,
                         schema_name ? &schema : nullptr, table_name ? &table : nullptr,
                         table_type ? &type : nullptr);

    return SQL_SUCCESS;
  });
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

  using ODBC::ODBCStatement;
  using ODBC::SqlWcharToString;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    std::string catalog = SqlWcharToString(catalog_name, catalog_name_length);
    std::string schema = SqlWcharToString(schema_name, schema_name_length);
    std::string table = SqlWcharToString(table_name, table_name_length);
    std::string column = SqlWcharToString(column_name, column_name_length);

    statement->GetColumns(catalog_name ? &catalog : nullptr,
                          schema_name ? &schema : nullptr, table_name ? &table : nullptr,
                          column_name ? &column : nullptr);

    return SQL_SUCCESS;
  });
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

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;
  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    ODBCDescriptor* ird = statement->GetIRD();
    SQLINTEGER output_length_int;
    switch (field_identifier) {
      // Numeric attributes
      // internal is SQLLEN, no conversion is needed
      case SQL_DESC_DISPLAY_SIZE:
      case SQL_DESC_OCTET_LENGTH: {
        ird->GetField(record_number, field_identifier, numeric_attribute_ptr,
                      buffer_length, &output_length_int);
        break;
      }
      // internal is SQLULEN, conversion is needed.
      case SQL_COLUMN_LENGTH:  // ODBC 2.0
      case SQL_DESC_LENGTH: {
        SQLULEN temp;
        ird->GetField(record_number, field_identifier, &temp, buffer_length,
                      &output_length_int);
        if (numeric_attribute_ptr) {
          *numeric_attribute_ptr = static_cast<SQLLEN>(temp);
        }
        break;
      }
      // internal is SQLINTEGER, conversion is needed.
      case SQL_DESC_AUTO_UNIQUE_VALUE:
      case SQL_DESC_CASE_SENSITIVE:
      case SQL_DESC_NUM_PREC_RADIX: {
        SQLINTEGER temp;
        ird->GetField(record_number, field_identifier, &temp, buffer_length,
                      &output_length_int);
        if (numeric_attribute_ptr) {
          *numeric_attribute_ptr = static_cast<SQLLEN>(temp);
        }
        break;
      }
      // internal is SQLSMALLINT, conversion is needed.
      case SQL_DESC_CONCISE_TYPE:
      case SQL_DESC_COUNT:
      case SQL_DESC_FIXED_PREC_SCALE:
      case SQL_DESC_TYPE:
      case SQL_DESC_NULLABLE:
      case SQL_COLUMN_PRECISION:  // ODBC 2.0
      case SQL_DESC_PRECISION:
      case SQL_COLUMN_SCALE:  // ODBC 2.0
      case SQL_DESC_SCALE:
      case SQL_DESC_SEARCHABLE:
      case SQL_DESC_UNNAMED:
      case SQL_DESC_UNSIGNED:
      case SQL_DESC_UPDATABLE: {
        SQLSMALLINT temp;
        ird->GetField(record_number, field_identifier, &temp, buffer_length,
                      &output_length_int);
        if (numeric_attribute_ptr) {
          *numeric_attribute_ptr = static_cast<SQLLEN>(temp);
        }
        break;
      }
      // Character attributes
      case SQL_DESC_BASE_COLUMN_NAME:
      case SQL_DESC_BASE_TABLE_NAME:
      case SQL_DESC_CATALOG_NAME:
      case SQL_DESC_LABEL:
      case SQL_DESC_LITERAL_PREFIX:
      case SQL_DESC_LITERAL_SUFFIX:
      case SQL_DESC_LOCAL_TYPE_NAME:
      case SQL_DESC_NAME:
      case SQL_DESC_SCHEMA_NAME:
      case SQL_DESC_TABLE_NAME:
      case SQL_DESC_TYPE_NAME:
        ird->GetField(record_number, field_identifier, character_attribute_ptr,
                      buffer_length, &output_length_int);
        break;
      default:
        throw DriverException("Invalid descriptor field", "HY091");
    }
    if (output_length) {
      *output_length = static_cast<SQLSMALLINT>(output_length_int);
    }
    return SQL_SUCCESS;
  });
}

SQLRETURN SQLGetTypeInfo(SQLHSTMT stmt, SQLSMALLINT data_type) {
  // GH-47237 return SQL_PRED_CHAR and SQL_PRED_BASIC for
  // appropriate data types in `SEARCHABLE` field
  ARROW_LOG(DEBUG) << "SQLGetTypeInfoW called with stmt: " << stmt
                   << " data_type: " << data_type;

  using ODBC::ODBCStatement;
  return ODBC::ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);

    switch (data_type) {
      case SQL_ALL_TYPES:
      case SQL_CHAR:
      case SQL_VARCHAR:
      case SQL_LONGVARCHAR:
      case SQL_WCHAR:
      case SQL_WVARCHAR:
      case SQL_WLONGVARCHAR:
      case SQL_BIT:
      case SQL_BINARY:
      case SQL_VARBINARY:
      case SQL_LONGVARBINARY:
      case SQL_TINYINT:
      case SQL_SMALLINT:
      case SQL_INTEGER:
      case SQL_BIGINT:
      case SQL_NUMERIC:
      case SQL_DECIMAL:
      case SQL_FLOAT:
      case SQL_REAL:
      case SQL_DOUBLE:
      case SQL_GUID:
      case SQL_DATE:
      case SQL_TYPE_DATE:
      case SQL_TIME:
      case SQL_TYPE_TIME:
      case SQL_TIMESTAMP:
      case SQL_TYPE_TIMESTAMP:
      case SQL_INTERVAL_DAY:
      case SQL_INTERVAL_DAY_TO_HOUR:
      case SQL_INTERVAL_DAY_TO_MINUTE:
      case SQL_INTERVAL_DAY_TO_SECOND:
      case SQL_INTERVAL_HOUR:
      case SQL_INTERVAL_HOUR_TO_MINUTE:
      case SQL_INTERVAL_HOUR_TO_SECOND:
      case SQL_INTERVAL_MINUTE:
      case SQL_INTERVAL_MINUTE_TO_SECOND:
      case SQL_INTERVAL_SECOND:
      case SQL_INTERVAL_YEAR:
      case SQL_INTERVAL_YEAR_TO_MONTH:
      case SQL_INTERVAL_MONTH:
        statement->GetTypeInfo(data_type);
        break;
      default:
        throw DriverException("Invalid SQL data type", "HY004");
    }

    return SQL_SUCCESS;
  });
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

  using ODBC::GetAttributeSQLWCHAR;
  using ODBC::ODBCConnection;
  using ODBC::SqlWcharToString;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    const bool is_length_in_bytes = false;

    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    Diagnostics& diagnostics = connection->GetDiagnostics();

    std::string in_statement_str =
        SqlWcharToString(in_statement_text, in_statement_text_length);

    return GetAttributeSQLWCHAR(in_statement_str, is_length_in_bytes, out_statement_text,
                                buffer_length, out_statement_text_length, diagnostics);
  });
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

  using ODBC::ODBCDescriptor;
  using ODBC::ODBCStatement;

  return ODBCStatement::ExecuteWithDiagnostics(stmt, SQL_ERROR, [=]() {
    ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(stmt);
    ODBCDescriptor* ird = statement->GetIRD();
    SQLINTEGER output_length_int;
    SQLSMALLINT sql_type;

    // Column SQL Type
    ird->GetField(column_number, SQL_DESC_CONCISE_TYPE, &sql_type, sizeof(SQLSMALLINT),
                  nullptr);
    if (data_type_ptr) {
      *data_type_ptr = sql_type;
    }

    // Column Name
    if (column_name || name_length_ptr) {
      ird->GetField(column_number, SQL_DESC_NAME, column_name, buffer_length,
                    &output_length_int);
      if (name_length_ptr) {
        // returned length should be in characters
        *name_length_ptr =
            static_cast<SQLSMALLINT>(output_length_int / GetSqlWCharSize());
      }
    }

    // Column Size
    if (column_size_ptr) {
      switch (sql_type) {
        // All numeric types
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        case SQL_TINYINT:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIGINT:
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE: {
          ird->GetField(column_number, SQL_DESC_PRECISION, column_size_ptr,
                        sizeof(SQLULEN), nullptr);
          break;
        }

        default: {
          ird->GetField(column_number, SQL_DESC_LENGTH, column_size_ptr, sizeof(SQLULEN),
                        nullptr);
        }
      }
    }

    // Column Decimal Digits
    if (decimal_digits_ptr) {
      switch (sql_type) {
        // All exact numeric types
        case SQL_TINYINT:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIGINT:
        case SQL_DECIMAL:
        case SQL_NUMERIC: {
          ird->GetField(column_number, SQL_DESC_SCALE, decimal_digits_ptr,
                        sizeof(SQLULEN), nullptr);
          break;
        }

        // All datetime types (ODBC2)
        case SQL_DATE:
        case SQL_TIME:
        case SQL_TIMESTAMP:
        // All datetime types (ODBC3)
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
        // All interval types with a seconds component
        case SQL_INTERVAL_SECOND:
        case SQL_INTERVAL_MINUTE_TO_SECOND:
        case SQL_INTERVAL_HOUR_TO_SECOND:
        case SQL_INTERVAL_DAY_TO_SECOND: {
          ird->GetField(column_number, SQL_DESC_PRECISION, decimal_digits_ptr,
                        sizeof(SQLULEN), nullptr);
          break;
        }

        default: {
          // All character and binary types
          // SQL_BIT
          // All approximate numeric types
          // All interval types with no seconds component
          *decimal_digits_ptr = static_cast<SQLSMALLINT>(0);
        }
      }
    }

    // Column Nullable
    if (nullable_ptr) {
      ird->GetField(column_number, SQL_DESC_NULLABLE, nullable_ptr, sizeof(SQLSMALLINT),
                    nullptr);
    }

    return SQL_SUCCESS;
  });
}

}  // namespace arrow::flight::sql::odbc
