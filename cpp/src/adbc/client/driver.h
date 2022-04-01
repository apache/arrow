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

// Basic C++ bindings for the ADBC API.

#pragma once

#include <memory>
#include <string>

#include "adbc/adbc.h"
#include "arrow/result.h"

namespace adbc {

/// \brief Low-level C++ wrapper over the C API.
class AdbcDriver {
 public:
  ~AdbcDriver();

  /// \brief Load the given driver.
  ///
  /// \param[in] driver The driver (a library name,
  ///   e.g. libadbc_driver_sqlite.so).
  static arrow::Result<std::unique_ptr<AdbcDriver>> Load(const std::string& driver);

  /// \brief Connect to a database.
  ///
  /// \param[in] options Connection options.
  arrow::Result<struct AdbcConnection> ConnectRaw(
      const struct AdbcConnectionOptions& options) const;

  /// \brief Release the given error struct.
  void ReleaseError(struct AdbcError* error) const;

 private:
  class Impl;
  explicit AdbcDriver(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace adbc
