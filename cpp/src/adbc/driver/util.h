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

#include <string>
#include <unordered_map>

#include "arrow/result.h"
#include "arrow/util/string_view.h"

// XXX: We want to export Adbc* functions, but inside
// AdbcSqliteDriverInit, we want to always point to the local
// function, not the global function (so we can cooperate with the
// driver manager). "protected" visibility gives us this.
// RTLD_DEEPBIND also works but is glibc-specific and does not work
// with AddressSanitizer.
// TODO: should this go in adbc.h instead?
#ifdef __linux__
#define ADBC_DRIVER_EXPORT __attribute__((visibility("protected")))
#else
#define ADBC_DRIVER_EXPORT
#endif  // ifdef __linux__

namespace adbc {

/// \brief Parse a connection string.
arrow::Result<std::unordered_map<std::string, std::string>> ParseConnectionString(
    arrow::util::string_view target);

}  // namespace adbc
