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

// platform.h includes windows.h, so it needs to be included first
#include "arrow/flight/sql/odbc/odbc_impl/platform.h"

#include "arrow/flight/sql/odbc/odbc_impl/config/configuration.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/status.h"

#include <odbcinst.h>

namespace arrow::flight::sql::odbc {

#if defined _WIN32
/**
 * Display connection window for user to configure connection parameters.
 *
 * @param window_parent Parent window handle.
 * @param config Output configuration.
 * @return True on success and false on fail.
 */
bool DisplayConnectionWindow(void* window_parent, config::Configuration& config);

/**
 * For SQLDriverConnect.
 * Display connection window for user to configure connection parameters.
 *
 * @param window_parent Parent window handle.
 * @param config Output configuration, presumed to be empty, it will be using values from
 * properties.
 * @param properties Output properties.
 * @return True on success and false on fail.
 */
bool DisplayConnectionWindow(void* window_parent, config::Configuration& config,
                             Connection::ConnPropertyMap& properties);
#endif

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const config::Configuration& config, LPCWSTR driver);

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const std::wstring& dsn);

void PostError(DWORD error_code, LPWSTR error_msg);

void PostArrowUtilError(arrow::Status error_status);
}  // namespace arrow::flight::sql::odbc
