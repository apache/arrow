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

#ifdef _WIN32
#  include <windows.h>
#endif

#include <sql.h>
#include <sqltypes.h>
#include <sqlucode.h>

// For wchar conversion
#include <codecvt>

namespace arrow {
namespace flight {
namespace odbc {
namespace integration_tests {
/** ODBC read buffer size. */
enum { ODBC_BUFFER_SIZE = 1024 };

std::string GetOdbcErrorMessage(SQLSMALLINT handle_type, SQLHANDLE handle);

// -AL- todo potentially add `connectToFlightSql` here.
}  // namespace integration_tests
}  // namespace odbc
}  // namespace flight
}  // namespace arrow
