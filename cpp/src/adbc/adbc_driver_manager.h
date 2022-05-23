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

#include "adbc/adbc.h"

#ifdef __cplusplus
extern "C" {
#endif

#ifndef ADBC_DRIVER_MANAGER_H
#define ADBC_DRIVER_MANAGER_H

/// \brief Common entry point for drivers via the driver manager.
///
/// The driver manager can fill in default implementations of some
/// ADBC functions for drivers. Drivers must implement a minimum level
/// of functionality for this to be possible, however, and some
/// functions must be implemented by the driver.
///
/// \param[in] connection The driver to initialize. Should be in
///   ODBC-style format ("Key1=Value1;Key2=Value2").
/// \param[in] count The number of entries to initialize. Provides
///   backwards compatibility if the struct definition is changed.
/// \param[out] driver The table of function pointers to initialize.
/// \param[out] initialized How much of the table was actually
///   initialized (can be less than count).
AdbcStatusCode AdbcLoadDriver(const char* connection, size_t count,
                              struct AdbcDriver* driver, size_t* initialized);

#endif  // ADBC_DRIVER_MANAGER_H

#ifdef __cplusplus
}
#endif
