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
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_connection.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/odbc_impl/odbc_environment.h>
#include <arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/connection.h>

// odbc_api includes windows.h, which needs to be put behind winsock2.h.
// odbc_environment.h includes winsock2.h
#include <arrow/flight/sql/odbc/odbc_api.h>

namespace arrow {
SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
  // TODO: implement SQLAllocHandle by linking to `odbc_impl`
  *result = 0;

  switch (type) {
    case SQL_HANDLE_ENV: {
      using driver::flight_sql::FlightSqlDriver;
      using ODBC::ODBCEnvironment;

      static std::shared_ptr<FlightSqlDriver> odbc_driver =
          std::make_shared<FlightSqlDriver>();
      *result = reinterpret_cast<SQLHENV>(new ODBCEnvironment(odbc_driver));

      return SQL_SUCCESS;
    }

    case SQL_HANDLE_DBC: {
      using ODBC::ODBCConnection;
      using ODBC::ODBCEnvironment;

      *result = SQL_NULL_HDBC;

      ODBCEnvironment* environment = reinterpret_cast<ODBCEnvironment*>(parent);

      if (!environment) {
        return SQL_INVALID_HANDLE;
      }

      std::shared_ptr<ODBCConnection> conn = environment->CreateConnection();

      if (!conn) {
        return SQL_INVALID_HANDLE;
      }

      *result = reinterpret_cast<SQLHDBC>(conn.get());

      return SQL_SUCCESS;
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

}  // namespace arrow
