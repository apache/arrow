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

#include "arrow/flight/sql/odbc/odbc_api.h"

namespace arrow
{
  SQLRETURN SQLAllocHandle(SQLSMALLINT type, SQLHANDLE parent, SQLHANDLE* result) {
    // TODO: implement SQLAllocHandle by linking to `odbc_impl` //-AL- TODO: create GitHub issue for SQLAllocHandle implementation
    *result = 0;

    switch (type)
    {
        case SQL_HANDLE_ENV:
            return SQLAllocEnv(result);

        case SQL_HANDLE_DBC:
            return SQL_INVALID_HANDLE;

        case SQL_HANDLE_STMT:
            return SQL_INVALID_HANDLE;

        default:
            break;
    }

    return SQL_ERROR;
  }

  SQLRETURN SQLAllocEnv(SQLHENV* env) {
    // -AL- todo implement
    /*
    // DRAFT Code:

    using ODBCEnvironment;
    *env = reinterpret_cast< SQLHENV >(new ODBCEnvironment());

    return SQL_SUCCESS
    */
    return SQL_INVALID_HANDLE;
  }
}  // namespace arrow