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
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/logger.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/attribute_utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h"

#if defined _WIN32 || defined _WIN64
// For displaying DSN Window
#  include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"
#endif

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include "arrow/flight/sql/odbc/odbc_api.h"

namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  LOG_DEBUG("SQLAllocHandle called with type: {}, parent: {}, result: {}", type, parent,
            fmt::ptr(result));

  *result = nullptr;

  switch (type) {
    case SQL_HANDLE_ENV: {
      using driver::flight_sql::FlightSqlDriver;
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
        std::shared_ptr<ODBCStatement> statement = connection->createStatement();

        if (statement) {
          *result = reinterpret_cast<SQLHSTMT>(statement.get());

          return SQL_SUCCESS;
        }

        return SQL_ERROR;
      });
    }

    // TODO Implement for case of descriptor
    case SQL_HANDLE_DESC:
      return SQL_INVALID_HANDLE;

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
  LOG_DEBUG("SQLFreeHandle called with type: {}, handle: {}", type, handle);

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

      conn->releaseConnection();

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_STMT: {
      using ODBC::ODBCStatement;

      ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);

      if (!statement) {
        return SQL_INVALID_HANDLE;
      }

      statement->releaseStatement();

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_DESC:
      return SQL_INVALID_HANDLE;

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLFreeStmt(SQLHSTMT handle, SQLUSMALLINT option) {
  switch (option) {
    case SQL_CLOSE: {
      using ODBC::ODBCStatement;

      ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);

      return ODBCStatement::ExecuteWithDiagnostics(statement, SQL_ERROR, [=]() {
        if (!statement) {
          return SQL_INVALID_HANDLE;
        }

        // Close cursor with suppressErrors set to true
        statement->closeCursor(true);

        return SQL_SUCCESS;
      });
    }

    case SQL_DROP: {
      return SQLFreeHandle(SQL_HANDLE_STMT, handle);
    }

    // TODO Implement SQLBindCol
    case SQL_UNBIND: {
      return SQL_SUCCESS;
    }

    // SQLBindParameter is not supported
    case SQL_RESET_PARAMS: {
      return SQL_SUCCESS;
    }
  }

  return SQL_ERROR;
}

inline bool IsValidStringFieldArgs(SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                                   SQLSMALLINT* stringLengthPtr, bool isUnicode) {
  const SQLSMALLINT charSize = isUnicode ? GetSqlWCharSize() : sizeof(char);
  const bool hasValidBuffer =
      diagInfoPtr && bufferLength >= 0 && bufferLength % charSize == 0;

  // regardless of capacity return false if invalid
  if (diagInfoPtr && !hasValidBuffer) {
    return false;
  }

  return hasValidBuffer || stringLengthPtr;
}

SQLRETURN SQLGetDiagField(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                          SQLSMALLINT diagIdentifier, SQLPOINTER diagInfoPtr,
                          SQLSMALLINT bufferLength, SQLSMALLINT* stringLengthPtr) {
  // TODO: Implement additional fields types
  // https://github.com/apache/arrow/issues/46573
  using driver::odbcabstraction::Diagnostics;
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCEnvironment;
  using ODBC::ODBCStatement;

  LOG_DEBUG(
      "SQLGetDiagFieldW called with handleType: {}, handle: {}, recNumber: {}, "
      "diagIdentifier: {}, diagInfoPtr: {}, bufferLength: {}, stringLengthPtr: {}",
      handleType, handle, recNumber, diagIdentifier, diagInfoPtr, bufferLength,
      fmt::ptr(stringLengthPtr));

  if (!handle) {
    return SQL_INVALID_HANDLE;
  }

  if (!diagInfoPtr && !stringLengthPtr) {
    return SQL_ERROR;
  }

  // If buffer length derived from null terminated string
  if (diagInfoPtr && bufferLength == SQL_NTS) {
    const wchar_t* str = reinterpret_cast<wchar_t*>(diagInfoPtr);
    bufferLength = wcslen(str) * driver::odbcabstraction::GetSqlWCharSize();
  }

  // Set character type to be Unicode by default
  const bool isUnicode = true;
  Diagnostics* diagnostics = nullptr;

  switch (handleType) {
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
      return SQL_ERROR;
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
  switch (diagIdentifier) {
    case SQL_DIAG_NUMBER: {
      if (diagInfoPtr) {
        *static_cast<SQLINTEGER*>(diagInfoPtr) =
            static_cast<SQLINTEGER>(diagnostics->GetRecordCount());
      }

      if (stringLengthPtr) {
        *stringLengthPtr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    // TODO implement return code function
    case SQL_DIAG_RETURNCODE: {
      return SQL_SUCCESS;
    }

    case SQL_DIAG_CURSOR_ROW_COUNT: {
      if (handleType == SQL_HANDLE_STMT) {
        if (diagInfoPtr) {
          // Will always be 0 if only select supported
          *static_cast<SQLLEN*>(diagInfoPtr) = 0;
        }

        if (stringLengthPtr) {
          *stringLengthPtr = sizeof(SQLLEN);
        }

        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }

    // Not supported
    case SQL_DIAG_DYNAMIC_FUNCTION:
    case SQL_DIAG_DYNAMIC_FUNCTION_CODE: {
      if (handleType == SQL_HANDLE_STMT) {
        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_ROW_COUNT: {
      if (handleType == SQL_HANDLE_STMT) {
        if (diagInfoPtr) {
          // Will always be 0 if only select supported
          *static_cast<SQLLEN*>(diagInfoPtr) = 0;
        }

        if (stringLengthPtr) {
          *stringLengthPtr = sizeof(SQLLEN);
        }

        return SQL_SUCCESS;
      }

      return SQL_ERROR;
    }
  }

  // If not a diagnostic header field then the record number must be 1 or greater
  if (recNumber < 1) {
    return SQL_ERROR;
  }

  // Retrieve record level diagnostics from specified 1 based record
  const uint32_t recordIndex = static_cast<uint32_t>(recNumber - 1);
  if (!diagnostics->HasRecord(recordIndex)) {
    return SQL_NO_DATA;
  }

  // Retrieve record field data
  switch (diagIdentifier) {
    case SQL_DIAG_MESSAGE_TEXT: {
      if (IsValidStringFieldArgs(diagInfoPtr, bufferLength, stringLengthPtr, isUnicode)) {
        const std::string& message = diagnostics->GetMessageText(recordIndex);
        return GetStringAttribute(isUnicode, message, true, diagInfoPtr, bufferLength,
                                  stringLengthPtr, *diagnostics);
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_NATIVE: {
      if (diagInfoPtr) {
        *static_cast<SQLINTEGER*>(diagInfoPtr) = diagnostics->GetNativeError(recordIndex);
      }

      if (stringLengthPtr) {
        *stringLengthPtr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    case SQL_DIAG_SERVER_NAME: {
      if (IsValidStringFieldArgs(diagInfoPtr, bufferLength, stringLengthPtr, isUnicode)) {
        switch (handleType) {
          case SQL_HANDLE_DBC: {
            ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(handle);
            std::string dsn = connection->GetDSN();
            return GetStringAttribute(isUnicode, dsn, true, diagInfoPtr, bufferLength,
                                      stringLengthPtr, *diagnostics);
          }

          case SQL_HANDLE_DESC: {
            // TODO Implement for case of descriptor
            return SQL_ERROR;
          }

          case SQL_HANDLE_STMT: {
            ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);
            ODBCConnection* connection = &statement->GetConnection();
            std::string dsn = connection->GetDSN();
            return GetStringAttribute(isUnicode, dsn, true, diagInfoPtr, bufferLength,
                                      stringLengthPtr, *diagnostics);
          }

          default:
            return SQL_ERROR;
        }
      }

      return SQL_ERROR;
    }

    case SQL_DIAG_SQLSTATE: {
      if (IsValidStringFieldArgs(diagInfoPtr, bufferLength, stringLengthPtr, isUnicode)) {
        const std::string& state = diagnostics->GetSQLState(recordIndex);
        return GetStringAttribute(isUnicode, state, true, diagInfoPtr, bufferLength,
                                  stringLengthPtr, *diagnostics);
      }

      return SQL_ERROR;
    }

    // Return valid dummy variable for unimplemented field
    case SQL_DIAG_COLUMN_NUMBER: {
      if (diagInfoPtr) {
        *static_cast<SQLINTEGER*>(diagInfoPtr) = SQL_NO_COLUMN_NUMBER;
      }

      if (stringLengthPtr) {
        *stringLengthPtr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    // Return empty string dummy variable for unimplemented fields
    case SQL_DIAG_CLASS_ORIGIN:
    case SQL_DIAG_CONNECTION_NAME:
    case SQL_DIAG_SUBCLASS_ORIGIN: {
      if (IsValidStringFieldArgs(diagInfoPtr, bufferLength, stringLengthPtr, isUnicode)) {
        return GetStringAttribute(isUnicode, "", true, diagInfoPtr, bufferLength,
                                  stringLengthPtr, *diagnostics);
      }

      return SQL_ERROR;
    }

    // Return valid dummy variable for unimplemented field
    case SQL_DIAG_ROW_NUMBER: {
      if (diagInfoPtr) {
        *static_cast<SQLLEN*>(diagInfoPtr) = SQL_NO_ROW_NUMBER;
      }

      if (stringLengthPtr) {
        *stringLengthPtr = sizeof(SQLLEN);
      }

      return SQL_SUCCESS;
    }

    default: {
      return SQL_ERROR;
    }
  }

  return SQL_ERROR;
}

SQLRETURN SQLGetDiagRec(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                        SQLWCHAR* sqlState, SQLINTEGER* nativeErrorPtr,
                        SQLWCHAR* messageText, SQLSMALLINT bufferLength,
                        SQLSMALLINT* textLengthPtr) {
  using driver::odbcabstraction::Diagnostics;
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCEnvironment;
  using ODBC::ODBCStatement;

  LOG_DEBUG(
      "SQLGetDiagRecW called with handleType: {}, handle: {}, recNumber: {}, "
      "sqlState: {}, nativeErrorPtr: {}, messageText: {}, bufferLength: {}, "
      "textLengthPtr: {}",
      handleType, handle, recNumber, fmt::ptr(sqlState), fmt::ptr(nativeErrorPtr),
      fmt::ptr(messageText), bufferLength, fmt::ptr(textLengthPtr));

  if (!handle) {
    return SQL_INVALID_HANDLE;
  }

  // Record number must be greater or equal to 1
  if (recNumber < 1 || bufferLength < 0) {
    return SQL_ERROR;
  }

  // Set character type to be Unicode by default
  const bool isUnicode = true;
  Diagnostics* diagnostics = nullptr;

  switch (handleType) {
    case SQL_HANDLE_ENV: {
      auto* environment = ODBCEnvironment::of(handle);
      diagnostics = &environment->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DBC: {
      auto* connection = ODBCConnection::of(handle);
      diagnostics = &connection->GetDiagnostics();
      break;
    }

    case SQL_HANDLE_DESC: {
      return SQL_ERROR;
    }

    case SQL_HANDLE_STMT: {
      auto* statement = ODBCStatement::of(handle);
      diagnostics = &statement->GetDiagnostics();
    }

    default:
      return SQL_INVALID_HANDLE;
  }

  if (!diagnostics) {
    return SQL_ERROR;
  }

  // Convert from ODBC 1 based record number to internal diagnostics 0 indexed storage
  const size_t recordIndex = static_cast<size_t>(recNumber - 1);
  if (!diagnostics->HasRecord(recordIndex)) {
    return SQL_NO_DATA;
  }

  if (sqlState) {
    // The length of the sql state is always 5 characters plus null
    SQLSMALLINT size = 6;
    const std::string& state = diagnostics->GetSQLState(recordIndex);
    GetStringAttribute(isUnicode, state, false, sqlState, size, &size, *diagnostics);
  }

  if (nativeErrorPtr) {
    *nativeErrorPtr = diagnostics->GetNativeError(recordIndex);
  }

  if (messageText || textLengthPtr) {
    const std::string& message = diagnostics->GetMessageText(recordIndex);
    return GetStringAttribute(isUnicode, message, false, messageText, bufferLength,
                              textLengthPtr, *diagnostics);
  }

  return SQL_SUCCESS;
}

SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER bufferLength, SQLINTEGER* strLenPtr) {
  using driver::odbcabstraction::DriverException;
  using ODBC::ODBCEnvironment;

  LOG_DEBUG(
      "SQLGetEnvAttr called with env: {}, attr: {}, valuePtr: {}, "
      "bufferLength: {}, strLenPtr: {}",
      env, attr, valuePtr, bufferLength, fmt::ptr(strLenPtr));

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  return ODBCEnvironment::ExecuteWithDiagnostics(environment, SQL_ERROR, [=]() {
    switch (attr) {
      case SQL_ATTR_ODBC_VERSION: {
        if (!valuePtr && !strLenPtr) {
          throw DriverException("Invalid null pointer for attribute.", "HY000");
        }

        if (valuePtr) {
          SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(valuePtr);
          *value = static_cast<SQLSMALLINT>(environment->getODBCVersion());
        }

        if (strLenPtr) {
          *strLenPtr = sizeof(SQLINTEGER);
        }

        return SQL_SUCCESS;
      }

      case SQL_ATTR_OUTPUT_NTS: {
        if (!valuePtr && !strLenPtr) {
          throw DriverException("Invalid null pointer for attribute.", "HY000");
        }

        if (valuePtr) {
          // output nts always returns SQL_TRUE
          SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(valuePtr);
          *value = SQL_TRUE;
        }

        if (strLenPtr) {
          *strLenPtr = sizeof(SQLINTEGER);
        }

        return SQL_SUCCESS;
      }

      case SQL_ATTR_CONNECTION_POOLING:
      case SQL_ATTR_APP_ROW_DESC: {
        throw DriverException("Optional feature not supported.", "HYC00");
      }

      default: {
        throw DriverException("Invalid attribute", "HYC00");
      }
    }
  });
}

SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER strLen) {
  using driver::odbcabstraction::DriverException;
  using ODBC::ODBCEnvironment;

  LOG_DEBUG(
      "SQLSetEnvAttr called with env: {}, attr: {}, valuePtr: {}, "
      "strLen: {}",
      env, attr, valuePtr, strLen);

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  return ODBCEnvironment::ExecuteWithDiagnostics(environment, SQL_ERROR, [=]() {
    if (!valuePtr) {
      throw DriverException("Invalid null pointer for attribute.", "HY024");
    }

    switch (attr) {
      case SQL_ATTR_ODBC_VERSION: {
        SQLINTEGER version =
            static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(valuePtr));
        if (version == SQL_OV_ODBC2 || version == SQL_OV_ODBC3) {
          environment->setODBCVersion(version);

          return SQL_SUCCESS;
        } else {
          throw DriverException("Invalid value for attribute", "HY024");
        }
      }

      case SQL_ATTR_OUTPUT_NTS: {
        // output nts can not be set to SQL_FALSE, is always SQL_TRUE
        SQLINTEGER value = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(valuePtr));
        if (value == SQL_TRUE) {
          return SQL_SUCCESS;
        } else {
          throw DriverException("Invalid value for attribute", "HY024");
        }
      }

      case SQL_ATTR_CONNECTION_POOLING:
      case SQL_ATTR_APP_ROW_DESC: {
        throw DriverException("Optional feature not supported.", "HYC00");
      }

      default: {
        throw DriverException("Invalid attribute", "HY092");
      }
    }
  });
}

SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
                           SQLWCHAR* inConnectionString,
                           SQLSMALLINT inConnectionStringLen,
                           SQLWCHAR* outConnectionString,
                           SQLSMALLINT outConnectionStringBufferLen,
                           SQLSMALLINT* outConnectionStringLen,
                           SQLUSMALLINT driverCompletion) {
  // TODO: Implement FILEDSN and SAVEFILE keywords according to the spec
  // https://github.com/apache/arrow/issues/46449

  // TODO: Copy connection string properly in SQLDriverConnect according to the
  // spec https://github.com/apache/arrow/issues/46560

  using driver::odbcabstraction::Connection;
  using driver::odbcabstraction::DriverException;
  using ODBC::ODBCConnection;

  LOG_DEBUG(
      "SQLDriverConnectW called with conn: {}, windowHandle: {}, inConnectionString: {}, "
      "inConnectionStringLen: {}, outConnectionString: {}, outConnectionStringBufferLen: "
      "{}, outConnectionStringLen: {}, driverCompletion: {}",
      conn, fmt::ptr(windowHandle), fmt::ptr(inConnectionString), inConnectionStringLen,
      fmt::ptr(outConnectionString), outConnectionStringBufferLen,
      fmt::ptr(outConnectionStringLen), driverCompletion);

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string connection_string =
        ODBC::SqlWcharToString(inConnectionString, inConnectionStringLen);
    Connection::ConnPropertyMap properties;
    std::string dsn =
        ODBCConnection::getPropertiesFromConnString(connection_string, properties);

    std::vector<std::string_view> missing_properties;

    // TODO: Implement SQL_DRIVER_COMPLETE_REQUIRED in SQLDriverConnect according to the
    // spec https://github.com/apache/arrow/issues/46448
#if defined _WIN32 || defined _WIN64
    // Load the DSN window according to driverCompletion
    if (driverCompletion == SQL_DRIVER_PROMPT) {
      // Load DSN window before first attempt to connect
      driver::flight_sql::config::Configuration config;
      if (!DisplayConnectionWindow(windowHandle, config, properties)) {
        return static_cast<SQLRETURN>(SQL_NO_DATA);
      }
      connection->connect(dsn, properties, missing_properties);
    } else if (driverCompletion == SQL_DRIVER_COMPLETE ||
               driverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) {
      try {
        connection->connect(dsn, properties, missing_properties);
      } catch (const DriverException&) {
        // If first connection fails due to missing attributes, load
        // the DSN window and try to connect again
        if (!missing_properties.empty()) {
          driver::flight_sql::config::Configuration config;
          missing_properties.clear();

          if (!DisplayConnectionWindow(windowHandle, config, properties)) {
            return static_cast<SQLRETURN>(SQL_NO_DATA);
          }
          connection->connect(dsn, properties, missing_properties);
        } else {
          throw;
        }
      }
    } else {
      // Default case: attempt connection without showing DSN window
      connection->connect(dsn, properties, missing_properties);
    }
#else
    // Attempt connection without loading DSN window on macOS/Linux
    connection->connect(dsn, properties, missing_properties);
#endif
    // Copy connection string to outConnectionString after connection attempt
    return ODBC::GetStringAttribute(true, connection_string, false, outConnectionString,
                                    outConnectionStringBufferLen, outConnectionStringLen,
                                    connection->GetDiagnostics());
  });
}

SQLRETURN SQLConnect(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                     SQLWCHAR* userName, SQLSMALLINT userNameLen, SQLWCHAR* password,
                     SQLSMALLINT passwordLen) {
  using driver::flight_sql::FlightSqlConnection;
  using driver::flight_sql::config::Configuration;
  using ODBC::ODBCConnection;

  using ODBC::SqlWcharToString;

  LOG_DEBUG(
      "SQLConnectW called with conn: {}, dsnName: {}, dsnNameLen: {}, userName: {}, "
      "userNameLen: {}, password: {}, passwordLen: {}",
      conn, fmt::ptr(dsnName), dsnNameLen, fmt::ptr(userName), userNameLen,
      fmt::ptr(password), passwordLen);

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string dsn = SqlWcharToString(dsnName, dsnNameLen);

    Configuration config;
    config.LoadDsn(dsn);

    if (userName) {
      std::string uid = SqlWcharToString(userName, userNameLen);
      config.Emplace(FlightSqlConnection::UID, std::move(uid));
    }

    if (password) {
      std::string pwd = SqlWcharToString(password, passwordLen);
      config.Emplace(FlightSqlConnection::PWD, std::move(pwd));
    }

    std::vector<std::string_view> missing_properties;

    connection->connect(dsn, config.GetProperties(), missing_properties);

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLDisconnect(SQLHDBC conn) {
  using ODBC::ODBCConnection;

  LOG_DEBUG("SQLDisconnect called with conn: {}", conn);

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    connection->disconnect();

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                     SQLSMALLINT bufLen, SQLSMALLINT* length) {
  // TODO: complete implementation of SQLGetInfoW and write tests
  using ODBC::ODBCConnection;

  LOG_DEBUG(
      "SQLGetInfoW called with conn: {}, infoType: {}, infoValuePtr: {}, bufLen: {}, "
      "length: {}",
      conn, infoType, infoValuePtr, bufLen, fmt::ptr(length));

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    // Partially stubbed implementation of SQLGetInfoW
    if (infoType == SQL_DRIVER_ODBC_VER) {
      std::string_view ver("03.80");

      return ODBC::GetStringAttribute(true, ver, true, infoValuePtr, bufLen, length,
                                      connection->GetDiagnostics());
    }

    return static_cast<SQLRETURN>(SQL_ERROR);
  });
}

}  // namespace arrow
