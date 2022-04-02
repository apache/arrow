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

#include "adbc/client/driver.h"

#include <dlfcn.h>

#include <string>

#include "adbc/adbc.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"
#include "arrow/util/macros.h"

namespace adbc {

namespace {
enum AdbcStatusCode DefaultConnectionDeserializePartitionDesc(
    struct AdbcConnection* connection, const uint8_t* serialized_partition,
    size_t serialized_length, struct AdbcStatement* statement, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
enum AdbcStatusCode DefaultConnectionGetTableTypes(struct AdbcConnection* connection,
                                                   struct AdbcStatement* statement,
                                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
enum AdbcStatusCode DefaultConnectionSqlPrepare(struct AdbcConnection* connection,
                                                const char* query, size_t query_length,
                                                struct AdbcStatement* statement,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
}  // namespace

#define GET_FUNC(DRIVER, HANDLE, NAME)                                               \
  instance->NAME = reinterpret_cast<decltype(&ARROW_CONCAT(Adbc, NAME))>(            \
      dlsym(HANDLE, "Adbc" ARROW_STRINGIFY(NAME)));                                  \
  if (!instance->NAME) {                                                             \
    return arrow::Status::Invalid("Driver ", DRIVER, " does not implement ", "Adbc", \
                                  ARROW_STRINGIFY(NAME));                            \
  }

#define GET_OPTIONAL(DRIVER, HANDLE, NAME)                                \
  instance->NAME = reinterpret_cast<decltype(&ARROW_CONCAT(Adbc, NAME))>( \
      dlsym(HANDLE, "Adbc" ARROW_STRINGIFY(NAME)));                       \
  if (!instance->NAME) {                                                  \
    instance->NAME = ARROW_CONCAT(Default, NAME);                         \
  }

arrow::Result<std::unique_ptr<AdbcDriver>> AdbcDriver::Load(const std::string& driver) {
  std::unique_ptr<AdbcDriver> instance(new AdbcDriver());

  // Platform specific
  void* handle = dlopen(driver.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    char* message = dlerror();
    return arrow::Status::Invalid("Could not load ", driver, ": ", message);
  }

  GET_FUNC(driver, handle, ErrorRelease);

  GET_FUNC(driver, handle, ConnectionInit);
  GET_OPTIONAL(driver, handle, ConnectionDeserializePartitionDesc);
  GET_OPTIONAL(driver, handle, ConnectionGetTableTypes);
  GET_FUNC(driver, handle, ConnectionRelease);
  GET_FUNC(driver, handle, ConnectionSqlExecute);
  GET_OPTIONAL(driver, handle, ConnectionSqlPrepare);

  GET_FUNC(driver, handle, StatementGetPartitionDesc);
  GET_FUNC(driver, handle, StatementGetPartitionDescSize);
  GET_FUNC(driver, handle, StatementGetStream);
  GET_FUNC(driver, handle, StatementRelease);
  return instance;
}
#undef GET_FUNC
#undef GET_OPTIONAL

}  // namespace adbc
