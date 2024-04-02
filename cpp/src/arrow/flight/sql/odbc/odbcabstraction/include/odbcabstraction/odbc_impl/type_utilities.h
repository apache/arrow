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

#pragma once

#include <sql.h>
#include <sqlext.h>

namespace ODBC {
inline SQLSMALLINT GetSqlTypeForODBCVersion(SQLSMALLINT type, bool isOdbc2x) {
  switch (type) {
    case SQL_DATE:
    case SQL_TYPE_DATE:
      return isOdbc2x ? SQL_DATE : SQL_TYPE_DATE;

    case SQL_TIME:
    case SQL_TYPE_TIME:
      return isOdbc2x ? SQL_TIME : SQL_TYPE_TIME;

    case SQL_TIMESTAMP:
    case SQL_TYPE_TIMESTAMP:
      return isOdbc2x ? SQL_TIMESTAMP : SQL_TYPE_TIMESTAMP;

    default:
      return type;
  }
}
}  // namespace ODBC
