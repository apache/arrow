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
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_descriptor.h"
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

    case SQL_HANDLE_DESC: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCDescriptor;

      *result = SQL_NULL_HDESC;

      ODBCConnection* connection = reinterpret_cast<ODBCConnection*>(parent);

      return ODBCConnection::ExecuteWithDiagnostics(connection, SQL_ERROR, [=]() {
        std::shared_ptr<ODBCDescriptor> descriptor = connection->createDescriptor();

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
  switch (option) {
    case SQL_CLOSE: {
      using ODBC::ODBCStatement;

      return ODBCStatement::ExecuteWithDiagnostics(handle, SQL_ERROR, [=]() {
        ODBCStatement* statement = reinterpret_cast<ODBCStatement*>(handle);

        // Close cursor with suppressErrors set to true
        statement->closeCursor(true);

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

}  // namespace arrow
