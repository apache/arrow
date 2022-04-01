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

namespace adbc {

class AdbcDriver::Impl {
 public:
  arrow::Result<struct AdbcConnection> ConnectRaw(
      const struct AdbcConnectionOptions& options) const {
    struct AdbcConnection out;
    std::memset(&out, 0, sizeof(out));
    struct AdbcError error;
    std::memset(&error, 0, sizeof(error));

    auto status = connection_init_(&options, &out, &error);
    if (status != ADBC_STATUS_OK) {
      // TODO: mapping function
      auto result =
          arrow::Status::UnknownError("Could not create connection: ", error.message);
      error_release_(&error);
      return result;
    }
    return out;
  }

 private:
  friend class AdbcDriver;

  // Function table
  decltype(&AdbcConnectionInit) connection_init_;
  decltype(&AdbcErrorRelease) error_release_;
};

AdbcDriver::AdbcDriver(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
AdbcDriver::~AdbcDriver() = default;

arrow::Result<std::unique_ptr<AdbcDriver>> AdbcDriver::Load(const std::string& driver) {
  // Platform specific
  void* handle = dlopen(driver.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    char* message = dlerror();
    return arrow::Status::Invalid("Could not load ", driver, ": ", message);
  }

  std::unique_ptr<AdbcDriver::Impl> impl(new AdbcDriver::Impl());

  impl->connection_init_ = reinterpret_cast<decltype(&AdbcConnectionInit)>(
      dlsym(handle, "AdbcConnectionInit"));
  if (!impl->connection_init_) {
    return arrow::Status::Invalid("Driver ", driver,
                                  " does not implement AdbcConnectionInit");
  }

  impl->error_release_ =
      reinterpret_cast<decltype(&AdbcErrorRelease)>(dlsym(handle, "AdbcErrorRelease"));
  if (!impl->error_release_) {
    return arrow::Status::Invalid("Driver ", driver,
                                  " does not implement AdbcErrorRelease");
  }

  return std::unique_ptr<AdbcDriver>(new AdbcDriver(std::move(impl)));
}

arrow::Result<struct AdbcConnection> AdbcDriver::ConnectRaw(
    const struct AdbcConnectionOptions& options) const {
  return impl_->ConnectRaw(options);
}

void AdbcDriver::ReleaseError(struct AdbcError* error) const {
  return impl_->error_release_(error);
}

}  // namespace adbc
