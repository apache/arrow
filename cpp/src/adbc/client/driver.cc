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
  decltype(&AdbcErrorRelease) error_release_;

  decltype(&AdbcConnectionInit) connection_init_;
  decltype(&AdbcConnectionDeserializePartitionDesc)
      connection_deserialize_partition_desc_;
  decltype(&AdbcConnectionGetTableTypes) connection_get_table_types_;
  decltype(&AdbcConnectionRelease) connection_release_;

  decltype(&AdbcStatementGetPartitionDesc) statement_get_partition_desc_;
  decltype(&AdbcStatementGetPartitionDescSize) statement_get_partition_desc_size_;
  decltype(&AdbcStatementGetStream) statement_get_stream_;
  decltype(&AdbcStatementRelease) statement_release_;
};

AdbcDriver::AdbcDriver(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
AdbcDriver::~AdbcDriver() = default;

#define GET_FUNC(DRIVER, HANDLE, FIELD, NAME)                                      \
  FIELD = reinterpret_cast<decltype(&NAME)>(dlsym(HANDLE, ARROW_STRINGIFY(NAME))); \
  if (!FIELD) {                                                                    \
    return arrow::Status::Invalid("Driver ", DRIVER, " does not implement ",       \
                                  ARROW_STRINGIFY(NAME));                          \
  }

#define GET_OPTIONAL(DRIVER, HANDLE, FIELD, NAME) \
  FIELD = reinterpret_cast<decltype(&NAME)>(dlsym(HANDLE, ARROW_STRINGIFY(NAME)));

arrow::Result<std::unique_ptr<AdbcDriver>> AdbcDriver::Load(const std::string& driver) {
  // Platform specific
  void* handle = dlopen(driver.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!handle) {
    char* message = dlerror();
    return arrow::Status::Invalid("Could not load ", driver, ": ", message);
  }

  std::unique_ptr<AdbcDriver::Impl> impl(new AdbcDriver::Impl());
  GET_FUNC(driver, handle, impl->connection_init_, AdbcConnectionInit);
  GET_OPTIONAL(driver, handle, impl->connection_deserialize_partition_desc_,
               AdbcConnectionDeserializePartitionDesc);
  GET_OPTIONAL(driver, handle, impl->connection_get_table_types_,
               AdbcConnectionGetTableTypes);
  GET_FUNC(driver, handle, impl->connection_release_, AdbcConnectionRelease);
  GET_FUNC(driver, handle, impl->error_release_, AdbcErrorRelease);
  GET_FUNC(driver, handle, impl->statement_get_partition_desc_,
           AdbcStatementGetPartitionDesc);
  GET_FUNC(driver, handle, impl->statement_get_partition_desc_size_,
           AdbcStatementGetPartitionDescSize);
  GET_FUNC(driver, handle, impl->statement_get_stream_, AdbcStatementGetStream);
  GET_FUNC(driver, handle, impl->statement_release_, AdbcStatementRelease);
  return std::unique_ptr<AdbcDriver>(new AdbcDriver(std::move(impl)));
}
#undef GET_FUNC
#undef GET_OPTIONAL

arrow::Result<struct AdbcConnection> AdbcDriver::ConnectRaw(
    const struct AdbcConnectionOptions& options) const {
  return impl_->ConnectRaw(options);
}

enum AdbcStatusCode AdbcDriver::ConnectionDeserializePartitionDesc(
    struct AdbcConnection* connection, const uint8_t* serialized_partition,
    size_t serialized_length, struct AdbcStatement* statement,
    struct AdbcError* error) const {
  if (impl_->connection_deserialize_partition_desc_) {
    return impl_->connection_deserialize_partition_desc_(
        connection, serialized_partition, serialized_length, statement, error);
  }
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

enum AdbcStatusCode AdbcDriver::ConnectionGetTableTypes(struct AdbcConnection* connection,
                                                        struct AdbcStatement* statement,
                                                        struct AdbcError* error) const {
  if (impl_->connection_get_table_types_) {
    return impl_->connection_get_table_types_(connection, statement, error);
  }
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

enum AdbcStatusCode AdbcDriver::ConnectionRelease(struct AdbcConnection* connection,
                                                  struct AdbcError* error) const {
  return impl_->connection_release_(connection, error);
}

void AdbcDriver::ErrorRelease(struct AdbcError* error) const {
  impl_->error_release_(error);
}

enum AdbcStatusCode AdbcDriver::StatementGetPartitionDesc(struct AdbcStatement* statement,
                                                          uint8_t* partition_desc,
                                                          struct AdbcError* error) {
  return impl_->statement_get_partition_desc_(statement, partition_desc, error);
}

enum AdbcStatusCode AdbcDriver::StatementGetPartitionDescSize(
    struct AdbcStatement* statement, size_t* length, struct AdbcError* error) {
  return impl_->statement_get_partition_desc_size_(statement, length, error);
}

enum AdbcStatusCode AdbcDriver::StatementGetStream(struct AdbcStatement* statement,
                                                   struct ArrowArrayStream* out,
                                                   struct AdbcError* error) {
  return impl_->statement_get_stream_(statement, out, error);
}

enum AdbcStatusCode AdbcDriver::StatementRelease(struct AdbcStatement* statement,
                                                 struct AdbcError* error) const {
  return impl_->statement_release_(statement, error);
}

}  // namespace adbc
