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

#include "adbc/c/types.h"

#include <dlfcn.h>

#include <cstring>
#include <queue>
#include <string>
#include <unordered_map>

#include "adbc/c/driver.h"
#include "adbc/util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string_builder.h"

namespace {
/// \brief AdbcConnection implementation that only implements release
/// and get_error, to report errors to the caller.
class ConnectionError {
 public:
  template <typename... Args>
  void LogError(Args&&... args) {
    auto message = arrow::util::StringBuilder(std::forward<Args>(args)...);
    messages_.push(std::move(message));
  }

  static void ReleaseMethod(struct AdbcConnection* connection) {
    if (!connection->private_data) return;
    delete reinterpret_cast<std::shared_ptr<ConnectionError>*>(connection->private_data);
    connection->private_data = nullptr;
  }

  char* GetError() {
    if (messages_.empty()) return nullptr;
    char* result = new char[messages_.front().size()];
    messages_.front().copy(result, messages_.front().size());
    messages_.pop();
    return result;
  }

  static char* GetErrorMethod(struct AdbcConnection* connection) {
    auto* ptr =
        reinterpret_cast<std::shared_ptr<ConnectionError>*>(connection->private_data);
    return (*ptr)->GetError();
  }

 private:
  std::queue<std::string> messages_;
};
}  // namespace

enum AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                       struct AdbcConnection* out) {
  std::memset(out, 0, sizeof(*out));

  auto impl = std::make_shared<ConnectionError>();

  // Find and load the correct driver.

  // TODO: For ODBC, drivers are installed and configured system-wide
  // and that may be a bit heavy for us. How do we want to handle this
  // configuration?

  std::unordered_map<std::string, std::string> option_pairs;
  auto status = adbc::ParseConnectionString(options->target).Value(&option_pairs);
  if (!status.ok()) {
    impl->LogError("[ADBC] AdbcConnectionInit: invalid target string: ", status);
    out->release = &ConnectionError::ReleaseMethod;
    out->get_error = &ConnectionError::GetErrorMethod;
    out->private_data = new std::shared_ptr<ConnectionError>(impl);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  auto library_it = option_pairs.find("Library");
  if (library_it == option_pairs.end()) {
    impl->LogError("[ADBC] AdbcConnectionInit: must provide Library in target string");
    out->release = &ConnectionError::ReleaseMethod;
    out->get_error = &ConnectionError::GetErrorMethod;
    out->private_data = new std::shared_ptr<ConnectionError>(impl);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  const std::string& filename = library_it->second;

  // Platform specific
  void* handle = dlopen(filename.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    char* message = dlerror();
    impl->LogError("[ADBC] AdbcConnectionInit: could not load ", filename, ": ", message);
    out->release = &ConnectionError::ReleaseMethod;
    out->get_error = &ConnectionError::GetErrorMethod;
    out->private_data = new std::shared_ptr<ConnectionError>(impl);
    return ADBC_STATUS_UNKNOWN;
  }

  auto* driver_connection_init = reinterpret_cast<decltype(&AdbcDriverConnectionInit)>(
      dlsym(handle, "AdbcDriverConnectionInit"));
  if (!driver_connection_init) {
    impl->LogError("[ADBC] AdbcConnectionInit: driver ", filename,
                   " does not implement AdbcDriverConnectionInit");
    out->release = &ConnectionError::ReleaseMethod;
    out->get_error = &ConnectionError::GetErrorMethod;
    out->private_data = new std::shared_ptr<ConnectionError>(impl);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  // TODO: we probably want to wrap the driver and provide default
  // implementations, error/sanity checking, etc.
  return driver_connection_init(options, out);
}
