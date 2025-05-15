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

#include "arrow/flight/sql/odbc/flight_sql/system_dsn.cc"

#include <arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/encoding_utils.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/config/configuration.h"

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include <arrow/flight/sql/odbc/odbc_api.h>

#include <iostream>  //-AL- remove later

namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  std::cout << "-AL- SQLAllocHandle called\n";
  // TODO: implement SQLAllocHandle by linking to `odbc_impl`
  *result = 0;

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
        // Will be caught in odbc abstration layer if using execute with diagnostics
        // allocating environment failed so cannot log diagnostic error here
        return SQL_ERROR;
      }
    }

    case SQL_HANDLE_DBC: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCEnvironment;

      *result = SQL_NULL_HDBC;

      ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(parent);

      if (!environment) {
        return SQL_INVALID_HANDLE;
      }

      try {
        std::shared_ptr<ODBCConnection> conn = environment->CreateConnection();
        *result = reinterpret_cast<SQLHDBC>(conn.get());

        return SQL_SUCCESS;
      } catch (const std::bad_alloc&) {
        // Will be caught in odbc abstration layer if using execute with diagnostics
        environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
            "A memory allocation error occurred.", "HY001"));

        return SQL_ERROR;
      }
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
  std::cout << "-AL- SQLFreeHandle called\n";
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

SQLRETURN SQLGetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER bufferLen, SQLINTEGER* strLenPtr) {
  using ODBC::ODBCEnvironment;

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  if (!environment) {
    return SQL_INVALID_HANDLE;
  }

  switch (attr) {
    case SQL_ATTR_ODBC_VERSION: {
      if (valuePtr) {
        SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(valuePtr);
        *value = static_cast<SQLSMALLINT>(environment->getODBCVersion());
        return SQL_SUCCESS;
      } else if (strLenPtr) {
        *strLenPtr = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
      } else {
        environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
            "Invalid null pointer for attribute.", "HY000"));
        return SQL_ERROR;
      }
    }

    case SQL_ATTR_OUTPUT_NTS: {
      if (valuePtr) {
        // output nts always returns SQL_TRUE
        SQLINTEGER* value = reinterpret_cast<SQLINTEGER*>(valuePtr);
        *value = SQL_TRUE;
        return SQL_SUCCESS;
      } else if (strLenPtr) {
        *strLenPtr = sizeof(SQLINTEGER);
        return SQL_SUCCESS;
      } else {
        environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
            "Invalid null pointer for attribute.", "HY000"));
        return SQL_ERROR;
      }
    }

    case SQL_ATTR_CONNECTION_POOLING:
    case SQL_ATTR_APP_ROW_DESC: {
      environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
          "Optional feature not supported.", "HYC00"));
      return SQL_ERROR;
    }

    default: {
      environment->GetDiagnostics().AddError(
          driver::odbcabstraction::DriverException("Invalid attribute", "HYC00"));
      return SQL_ERROR;
    }
  }

  return SQL_ERROR;
}

SQLRETURN SQLSetEnvAttr(SQLHENV env, SQLINTEGER attr, SQLPOINTER valuePtr,
                        SQLINTEGER strLen) {
  std::cout << "-AL- SQLSetEnvAttr called\n";
  using ODBC::ODBCEnvironment;

  ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(env);

  if (!environment) {
    return SQL_INVALID_HANDLE;
  }

  if (!valuePtr) {
    environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
        "Invalid null pointer for attribute.", "HY024"));

    return SQL_ERROR;
  }

  switch (attr) {
    case SQL_ATTR_ODBC_VERSION: {
      SQLINTEGER version = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(valuePtr));
      if (version == SQL_OV_ODBC2 || version == SQL_OV_ODBC3) {
        environment->setODBCVersion(version);
        return SQL_SUCCESS;
      } else {
        environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
            "Invalid value for attribute", "HY024"));
        return SQL_ERROR;
      }
    }

    case SQL_ATTR_OUTPUT_NTS: {
      // output nts can not be set to SQL_FALSE, is always SQL_TRUE
      SQLINTEGER value = static_cast<SQLINTEGER>(reinterpret_cast<intptr_t>(valuePtr));
      if (value == SQL_TRUE) {
        return SQL_SUCCESS;
      } else {
        environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
            "Invalid value for attribute", "HY024"));
        return SQL_ERROR;
      }
    }

    case SQL_ATTR_CONNECTION_POOLING:
    case SQL_ATTR_APP_ROW_DESC: {
      environment->GetDiagnostics().AddError(driver::odbcabstraction::DriverException(
          "Optional feature not supported.", "HYC00"));
      return SQL_ERROR;
    }

    default: {
      environment->GetDiagnostics().AddError(
          driver::odbcabstraction::DriverException("Invalid attribute", "HY092"));
      return SQL_ERROR;
    }
  }

  return SQL_ERROR;
}

SQLRETURN SQLDriverConnect(SQLHDBC conn, SQLHWND windowHandle,
                           SQLCHAR* inConnectionString, SQLSMALLINT inConnectionStringLen,
                           SQLCHAR* outConnectionString,
                           SQLSMALLINT outConnectionStringBufferLen,
                           SQLSMALLINT* outConnectionStringLen,
                           SQLUSMALLINT driverCompletion) {
  // TODO: Implement FILEDSN and SAVEFILE keywords according to the spec
  // https://github.com/apache/arrow/issues/46449

  std::cout << "-AL- SQLDriverConnect called\n";
  // -AL- TODO implement
  using driver::odbcabstraction::Connection;
  using ODBC::ODBCConnection;

  ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

  if (!connection) {
    // Invalid connection pointer
    return SQL_INVALID_HANDLE;
  }

  // -AL- draft code for execute with diagnostics
  // return connection->ExecuteWithDiagnostics(conn, SQL_ERROR, []() {

  std::string connection_string =
      ODBC::SqlStringToString(inConnectionString, inConnectionStringLen);
  Connection::ConnPropertyMap properties;
  std::string dsn =
      ODBCConnection::getPropertiesFromConnString(connection_string, properties);

  std::vector<std::string> missing_properties;
  connection->connect(dsn, properties, missing_properties);

  // TODO: Implement SQL_DRIVER_COMPLETE_REQUIRED in SQLDriverConnect according to the
  // spec https://github.com/apache/arrow/issues/46448
  if (driverCompletion == SQL_DRIVER_PROMPT ||
      ((driverCompletion == SQL_DRIVER_COMPLETE ||
        driverCompletion == SQL_DRIVER_COMPLETE_REQUIRED) &&
       !missing_properties.empty())) {
    driver::flight_sql::config::Configuration config;
    if (DisplayConnectionWindow(windowHandle, config)) {
      properties = config.GetProperties();

      // Copy connection string to outConnectionString before connection attempt
      connection_string = config.ToConnectString();
      size_t reslen = ODBC::CopyStringToBuffer(
          connection_string, reinterpret_cast<char*>(outConnectionString),
          static_cast<size_t>(outConnectionStringBufferLen));
      if (outConnectionStringLen)
        *outConnectionStringLen = static_cast<SQLSMALLINT>(reslen);

      connection->connect(dsn, properties, missing_properties);
    } else {
      throw DriverException("Connection canceled by user", "HY008");
    }
  } else {
    // Copy connection string to outConnectionString after connection attempt
    size_t reslen = ODBC::CopyStringToBuffer(
        connection_string, reinterpret_cast<char*>(outConnectionString),
        static_cast<size_t>(outConnectionStringBufferLen));
    if (outConnectionStringLen)
      *outConnectionStringLen = static_cast<SQLSMALLINT>(reslen);
  }

  return SQL_SUCCESS;
  //});
}

SQLRETURN SQLDisconnect(SQLHDBC conn) {
  using ODBC::ODBCConnection;

  ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(conn);

  if (!connection) {
    // Invalid connection pointer
    return SQL_INVALID_HANDLE;
  }

  connection->disconnect();

  return SQL_SUCCESS;
}

SQLRETURN SQLGetInfo(SQLHDBC conn, SQLUSMALLINT infoType, SQLPOINTER infoValuePtr,
                     SQLSMALLINT bufLen, SQLSMALLINT* length) {
  std::cout << "-AL- SQLGetInfo called\n";
  // TODO: complete implementation of SQLGetInfo and write tests

  // Partially stubbed implementation of SQLGetInfo
  if (infoType == SQL_DRIVER_ODBC_VER) {
    std::string ver("03.80");
    size_t reslen = ODBC::CopyStringToBuffer(ver, reinterpret_cast<char*>(infoValuePtr),
                                             static_cast<size_t>(bufLen));
    if (length) *length = static_cast<SQLSMALLINT>(reslen);

    return SQL_SUCCESS;
  }

  return SQL_ERROR;
}

}  // namespace arrow
