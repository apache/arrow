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

#include <arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/attribute_utils.h"

#if defined _WIN32 || defined _WIN64
// For displaying DSN Window
#  include "arrow/flight/sql/odbc/flight_sql/system_dsn.h"
#endif

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include <arrow/flight/sql/odbc/odbc_api.h>

namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
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
      return SQL_INVALID_HANDLE;
    }

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLFreeHandle(SQLSMALLINT type, SQLHANDLE handle) {
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

    case SQL_HANDLE_STMT:
      return SQL_INVALID_HANDLE;

    case SQL_HANDLE_DESC:
      return SQL_INVALID_HANDLE;

    default:
      break;
  }

  return SQL_ERROR;
}

SQLRETURN SQLGetDiagFieldW(SQLSMALLINT handleType, SQLHANDLE handle,
                           SQLSMALLINT recNumber, SQLSMALLINT diagIdentifier,
                           SQLPOINTER diagInfoPtr, SQLSMALLINT bufferLength,
                           SQLSMALLINT* stringLengthPtr) {
  using driver::odbcabstraction::Diagnostics;
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCEnvironment;

  if (!handle) {
    return SQL_INVALID_HANDLE;
  }

  if (!diagInfoPtr) {
    return SQL_ERROR;
  }

  // Set character type to be Unicode by defualt (not Ansi)
  bool isUnicode = true;
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

    default:
      return SQL_ERROR;
  }

  if (!diagnostics) {
    return SQL_ERROR;
  }

  // Retrieve header level diagnostics if Record 0 specified
  if (recNumber == 0) {
    switch (diagIdentifier) {
      case SQL_DIAG_NUMBER: {
        SQLINTEGER count = static_cast<SQLINTEGER>(diagnostics->GetRecordCount());
        *static_cast<SQLINTEGER*>(diagInfoPtr) = count;
        if (stringLengthPtr) {
          *stringLengthPtr = sizeof(SQLINTEGER);
        }

        return SQL_SUCCESS;
      }

      case SQL_DIAG_SERVER_NAME: {
        const std::string source = diagnostics->GetDataSourceComponent();
        return GetStringAttribute(isUnicode, source, false, diagInfoPtr, bufferLength,
                                  stringLengthPtr, *diagnostics);
      }

      default:
        return SQL_ERROR;
    }
  }

  // Retrieve record level diagnostics from specified 1 based record
  uint32_t recordIndex = static_cast<uint32_t>(recNumber - 1);
  if (!diagnostics->HasRecord(recordIndex)) {
    return SQL_NO_DATA;
  }

  // Retrieve record field data
  switch (diagIdentifier) {
    case SQL_DIAG_MESSAGE_TEXT: {
      const std::string message = diagnostics->GetMessageText(recordIndex);
      return GetStringAttribute(isUnicode, message, false, diagInfoPtr, bufferLength,
                                stringLengthPtr, *diagnostics);
    }

    case SQL_DIAG_NATIVE: {
      *static_cast<SQLINTEGER*>(diagInfoPtr) = diagnostics->GetNativeError(recordIndex);
      if (stringLengthPtr) {
        *stringLengthPtr = sizeof(SQLINTEGER);
      }

      return SQL_SUCCESS;
    }

    case SQL_DIAG_SQLSTATE: {
      const std::string state = diagnostics->GetSQLState(recordIndex);
      return GetStringAttribute(isUnicode, state, false, diagInfoPtr, bufferLength,
                                stringLengthPtr, *diagnostics);
    }

    default: {
      // TODO Return correct dummy values
      return SQL_SUCCESS;
    }
  }

  return SQL_ERROR;
}

SQLRETURN SQLGetDiagRecW(SQLSMALLINT handleType, SQLHANDLE handle, SQLSMALLINT recNumber,
                         SQLWCHAR* sqlState, SQLINTEGER* nativeErrorPtr,
                         SQLWCHAR* messageText, SQLSMALLINT bufferLength,
                         SQLSMALLINT* textLengthPtr) {
  using driver::odbcabstraction::Diagnostics;
  using ODBC::ConvertToSqlWChar;
  using ODBC::GetStringAttribute;
  using ODBC::ODBCConnection;
  using ODBC::ODBCEnvironment;

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
      return SQL_ERROR;
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
                        SQLINTEGER bufferLen, SQLINTEGER* strLenPtr) {
  using driver::odbcabstraction::DriverException;
  using ODBC::ODBCEnvironment;

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

SQLRETURN SQLDriverConnectW(SQLHDBC conn, SQLHWND windowHandle,
                            SQLWCHAR* inConnectionString,
                            SQLSMALLINT inConnectionStringLen,
                            SQLWCHAR* outConnectionString,
                            SQLSMALLINT outConnectionStringBufferLen,
                            SQLSMALLINT* outConnectionStringLen,
                            SQLUSMALLINT driverCompletion) {
  // TODO: Implement FILEDSN and SAVEFILE keywords according to the spec
  // https://github.com/apache/arrow/issues/46449

  // TODO: Copy connection string properly in SQLDriverConnectW according to the
  // spec https://github.com/apache/arrow/issues/46560

  using driver::odbcabstraction::Connection;
  using driver::odbcabstraction::DriverException;
  using ODBC::ODBCConnection;

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);
    std::string connection_string =
        ODBC::SqlWcharToString(inConnectionString, inConnectionStringLen);
    Connection::ConnPropertyMap properties;
    std::string dsn =
        ODBCConnection::getPropertiesFromConnString(connection_string, properties);

    std::vector<std::string_view> missing_properties;

    // TODO: Implement SQL_DRIVER_COMPLETE_REQUIRED in SQLDriverConnectW according to the
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
    return ODBC::GetStringAttribute(true, connection_string, true, outConnectionString,
                                    outConnectionStringBufferLen, outConnectionStringLen,
                                    connection->GetDiagnostics());
  });
}

SQLRETURN SQLConnectW(SQLHDBC conn, SQLWCHAR* dsnName, SQLSMALLINT dsnNameLen,
                      SQLWCHAR* userName, SQLSMALLINT userNameLen, SQLWCHAR* password,
                      SQLSMALLINT passwordLen) {
  using driver::flight_sql::FlightSqlConnection;
  using driver::flight_sql::config::Configuration;
  using ODBC::ODBCConnection;

  using ODBC::SqlWcharToString;

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

  return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
    ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

    connection->disconnect();

    return SQL_SUCCESS;
  });
}

SQLRETURN SQLGetInfoW(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                      SQLSMALLINT bufLen, SQLSMALLINT* length) {
  // TODO: complete implementation of SQLGetInfoW and write tests
  using ODBC::ODBCConnection;

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
