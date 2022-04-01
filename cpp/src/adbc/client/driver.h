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

  /// \name Connections
  ///@{

  /// \brief Connect to a database.
  ///
  /// \param[in] options Connection options.
  // TODO: this should return a higher level wrapper type
  arrow::Result<struct AdbcConnection> ConnectRaw(
      const struct AdbcConnectionOptions& options) const;

  enum AdbcStatusCode ConnectionDeserializePartitionDesc(
      struct AdbcConnection* connection, const uint8_t* serialized_partition,
      size_t serialized_length, struct AdbcStatement* statement,
      struct AdbcError* error) const;

  enum AdbcStatusCode ConnectionGetCatalogs(struct AdbcConnection* connection,
                                            struct AdbcStatement* statement,
                                            struct AdbcError* error) const;
  enum AdbcStatusCode ConnectionGetDbSchemas(struct AdbcConnection* connection,
                                             struct AdbcStatement* statement,
                                             struct AdbcError* error) const;
  enum AdbcStatusCode ConnectionGetTableTypes(struct AdbcConnection* connection,
                                              struct AdbcStatement* statement,
                                              struct AdbcError* error) const;
  enum AdbcStatusCode ConnectionGetTables(
      struct AdbcConnection* connection, const char* catalog, size_t catalog_length,
      const char* db_schema, size_t db_schema_length, const char* table_name,
      size_t table_name_length, const char** table_types, size_t table_types_length,
      struct AdbcStatement* statement, struct AdbcError* error) const;

  /// \brief Release the given error.
  enum AdbcStatusCode ConnectionRelease(struct AdbcConnection* connection,
                                        struct AdbcError* error) const;

  ///@}

  /// \brief Release the given error.
  void ErrorRelease(struct AdbcError* error) const;

  /// \name Statements
  ///@{

  enum AdbcStatusCode StatementGetPartitionDesc(struct AdbcStatement* statement,
                                                uint8_t* partition_desc,
                                                struct AdbcError* error);

  enum AdbcStatusCode StatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                    size_t* length,
                                                    struct AdbcError* error);

  enum AdbcStatusCode StatementGetStream(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         struct AdbcError* error);

  enum AdbcStatusCode StatementRelease(struct AdbcStatement* statement,
                                       struct AdbcError* error) const;

  ///@}

 private:
  // TODO: is it worth pImpling this vs just exposing a table of pointers?
  class Impl;
  explicit AdbcDriver(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

}  // namespace adbc
