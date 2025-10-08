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

#if defined _WIN32 || defined _WIN64
// For displaying DSN Window
#  include "arrow/flight/sql/odbc/odbc_impl/system_dsn.h"
#endif

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
  // GH-46575 TODO: Add tests for SQLGetDiagField
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
  // GH-46575 TODO: Add tests for SQLGetDiagRec
  using arrow::flight::sql::odbc::Diagnostics;
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

// Load properties from the given DSN. The properties loaded do _not_ overwrite existing
// entries in the properties.
void LoadPropertiesFromDSN(const std::string& dsn,
                           Connection::ConnPropertyMap& properties) {
  arrow::flight::sql::odbc::config::Configuration config;
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

  using arrow::flight::sql::odbc::Connection;
  using arrow::flight::sql::odbc::DriverException;
  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string connection_string =
        ODBC::SqlWcharToString(in_connection_string, in_connection_string_len);
    Connection::ConnPropertyMap properties;
    std::string dsn = ODBCConnection::GetDsnIfExists(connection_string);
    if (!dsn.empty()) {
      LoadPropertiesFromDSN(dsn, properties);
    }
    ODBCConnection::GetPropertiesFromConnString(connection_string, properties);

    std::vector<std::string_view> missing_properties;

    // GH-46448 TODO: Implement SQL_DRIVER_COMPLETE_REQUIRED in SQLDriverConnect according
    // to the spec
#if defined _WIN32 || defined _WIN64
    // Load the DSN window according to driver_completion
    if (driver_completion == SQL_DRIVER_PROMPT) {
      // Load DSN window before first attempt to connect
      arrow::flight::sql::odbc::config::Configuration config;
      if (!DisplayConnectionWindow(window_handle, config, properties)) {
        return static_cast<SQLRETURN>(SQL_NO_DATA);
      }
      connection->Connect(dsn, properties, missing_properties);
    } else if (driver_completion == SQL_DRIVER_COMPLETE ||
               driver_completion == SQL_DRIVER_COMPLETE_REQUIRED) {
      try {
        connection->Connect(dsn, properties, missing_properties);
      } catch (const DriverException&) {
        // If first connection fails due to missing attributes, load
        // the DSN window and try to connect again
        if (!missing_properties.empty()) {
          arrow::flight::sql::odbc::config::Configuration config;
          missing_properties.clear();

          if (!DisplayConnectionWindow(window_handle, config, properties)) {
            return static_cast<SQLRETURN>(SQL_NO_DATA);
          }
          connection->Connect(dsn, properties, missing_properties);
        } else {
          throw;
        }
      }
    } else {
      // Default case: attempt connection without showing DSN window
      connection->Connect(dsn, properties, missing_properties);
    }
#else
    // Attempt connection without loading DSN window on macOS/Linux
    connection->Connect(dsn, properties, missing_properties);
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

  using arrow::flight::sql::odbc::FlightSqlConnection;
  using arrow::flight::sql::odbc::config::Configuration;
  using ODBC::ODBCConnection;

  using ODBC::SqlWcharToString;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string dsn = SqlWcharToString(dsn_name, dsn_name_len);

    Configuration config;
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

  // GH-47709 TODO: Update SQLGetInfo implementation and add tests for SQLGetInfo
  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    // Set character type to be Unicode by default
    const bool is_unicode = true;

    if (!info_value_ptr && !string_length_ptr) {
      return static_cast<SQLRETURN>(SQL_ERROR);
    }

    connection->GetInfo(info_type, info_value_ptr, buf_len, string_length_ptr,
                        is_unicode);
    return static_cast<SQLRETURN>(SQL_SUCCESS);
  });
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
