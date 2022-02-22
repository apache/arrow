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

#include "adbc/adbc.h"

#include <dlfcn.h>

#include <string>

#include "adbc/c/types.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace adbc {

arrow::Result<struct AdbcConnection> ConnectRaw(
    const std::string& driver, const struct AdbcConnectionOptions& options) {
  struct AdbcConnection out;
  std::memset(&out, 0, sizeof(out));

  // Platform specific
  void* handle = dlopen(driver.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    char* message = dlerror();
    return arrow::Status::Invalid("Could not load ", driver, ": ", message);
  }

  auto* connection_init = reinterpret_cast<decltype(&AdbcConnectionInit)>(
      dlsym(handle, "AdbcConnectionInit"));
  if (!connection_init) {
    return arrow::Status::Invalid("Driver ", driver,
                                  " does not implement AdbcConnectionInit");
  }

  struct AdbcError error;
  auto status = connection_init(&options, &out, &error);
  if (status != ADBC_STATUS_OK) {
    // TODO: mapping function
    auto result =
        arrow::Status::UnknownError("Could not create connection: ", error.message);
    error.release(&error);
    return result;
  }
  return out;
}

}  // namespace adbc
