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

#include <arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/attribute_utils.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include <arrow/flight/sql/odbc/odbc_api.h>

namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  // TODO: implement SQLAllocHandle by linking to `odbc_impl`
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

      return ODBCConnection::ExecuteWithDiagnostics(conn, SQL_ERROR, [=]() {
        conn->releaseConnection();

        return SQL_SUCCESS;
      });
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

    default:
      return SQL_ERROR;
  }

  return SQL_ERROR;
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
}  // namespace arrow
