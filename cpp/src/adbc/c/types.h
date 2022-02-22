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

#include <stddef.h>
#include <stdint.h>

#include <arrow/c/abi.h>

#ifdef __cplusplus
extern "C" {
#endif

/// \file ADBC: Arrow DataBase connectivity (client API)
///
/// Implemented by libadbc.so (provided by Arrow/C++), which in turn
/// dynamically loads the appropriate database driver.
///
/// EXPERIMENTAL. Interface subject to change.

/// \page object-model Object Model
///
/// Except where noted, objects are not thread-safe and clients should
/// take care to serialize accesses to methods.

/// \defgroup adbc-error-handling Error handling primitives.
/// ADBC uses integer error codes to signal errors. To provide more
/// detail about errors, functions may also return an AdbcError via an
/// optional out parameter, which can be inspected. If provided, it is
/// the responsibility of the caller to zero-initialize the AdbcError
/// value.
///
/// @{

/// Error codes for operations that may fail.
enum AdbcStatusCode {
  /// No error.
  ADBC_STATUS_OK = 0,
  /// An unknown error occurred.
  ADBC_STATUS_UNKNOWN = 1,
  /// The operation is not implemented.
  ADBC_STATUS_NOT_IMPLEMENTED = 2,
  /// An operation was attempted on an uninitialized object.
  ADBC_STATUS_UNINITIALIZED = 3,
  /// The arguments are invalid.
  ADBC_STATUS_INVALID_ARGUMENT = 4,
  /// An I/O error occurred.
  ADBC_STATUS_IO = 5,
  // TODO: more codes as appropriate
};

/// \brief A detailed error message for an operation.
struct AdbcError {
  /// \brief The error message.
  char* message;
  /// \brief Destroy this error message.
  void (*release)(struct AdbcError* error);
};

/// }@

struct AdbcStatement;

/// \defgroup adbc-connection Connection establishment.
/// @{

/// \brief A set of connection options.
struct AdbcConnectionOptions {
  /// \brief A driver-specific connection string.
  ///
  /// Should be in ODBC-style format ("Key1=Value1;Key2=Value2").
  const char* target;
};

// TODO: Do we prefer an API like this, which mimics Arrow C ABI/OOP
// more, or something more like ODBC which is more procedural?

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not thread-safe and clients should take care to
/// serialize accesses to a connection.
struct AdbcConnection {
  /// \name Common Functions
  /// Standard functions for memory management of ADBC types.
  ///@{

  /// \brief Destroy this connection.
  /// \param[in] connection The connection to release.
  /// \param[out] error An optional location to return an error
  ///   message if necessary.
  enum AdbcStatusCode (*release)(struct AdbcConnection* connection,
                                 struct AdbcError* error);

  ///@}

  /// \name SQL Semantics
  /// Functions for executing SQL queries, or querying SQL-related
  /// metadata. Drivers are not required to support both SQL and
  /// Substrait semantics. If they do, it may be via converting
  /// between representations internally.
  ///@{

  /// \brief Execute a one-shot query.
  ///
  /// For queries expected to be executed repeatedly, create a
  /// prepared statement.
  enum AdbcStatusCode (*sql_execute)(struct AdbcConnection* connection, const char* query,
                                     struct AdbcStatement* statement,
                                     struct AdbcError* error);

  /// \brief Prepare a query to be executed multiple times.
  ///
  /// TODO: this should return AdbcPreparedStatement to disaggregate
  /// preparation and execution
  enum AdbcStatusCode (*sql_prepare)(struct AdbcConnection* connection, const char* query,
                                     struct AdbcStatement* statement,
                                     struct AdbcError* error);

  ///@}

  /// \name Substrait Semantics
  /// Functions for executing Substrait plans, or querying
  /// Substrait-related metadata.  Drivers are not required to support
  /// both SQL and Substrait semantics. If they do, it may be via
  /// converting between representations internally.
  ///@{

  ///@}

  /// \name Partitioned Results
  /// Some databases may internally partition the results. These
  /// partitions are exposed to clients who may wish to integrate them
  /// with a threaded or distributed execution model, where partitions
  /// can be divided among threads or machines for processing.
  ///
  /// Drivers are not required to support partitioning.
  ///
  ///@{

  /// \brief Construct a statement for a partition of a query. The
  ///   statement can then be read independently.
  ///
  /// A partition can be retrieved from AdbcStatement::get_partition_desc.
  enum AdbcStatusCode (*deserialize_partition_desc)(struct AdbcConnection* connection,
                                                    const uint8_t* serialized_partition,
                                                    size_t serialized_length,
                                                    struct AdbcStatement* statement,
                                                    struct AdbcError* error);

  ///@}

  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// \brief Create a new connection to a database.
enum AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                       struct AdbcConnection* out,
                                       struct AdbcError* error);

/// }@

/// \defgroup adbc-statement Managing statements.
/// @{

// TODO: ODBC uses a single "handle" concept and allocation function (but
// originally had separate functions for each handle type); is there
// any benefit to doing this for us? what was the motivation?

/// \brief The result of executing a database query. Can be consumed
///   to produce a result.
///
/// Statements are not thread-safe and clients should take care to
/// serialize access.
struct AdbcStatement {
  /// \name Common Functions
  /// Standard functions for memory management of ADBC types.
  ///@{

  /// \brief Destroy this connection.
  /// \param[in] connection The connection to release.
  /// \param[out] error An optional location to return an error
  ///   message if necessary.
  enum AdbcStatusCode (*release)(struct AdbcStatement* connection,
                                 struct AdbcError* error);

  ///@}

  /// \brief Read the result of a statement.
  ///
  /// This method can be called only once.
  ///
  /// \return out A stream of Arrow data. The stream itself must be
  ///   released before the statement is released.
  enum AdbcStatusCode (*get_results)(struct AdbcStatement* statement,
                                     struct ArrowArrayStream* out,
                                     struct AdbcError* error);

  /// \name Partitioned Results
  /// Some backends may internally partition the results. These
  /// partitions are exposed to clients who may wish to integrate them
  /// with a threaded or distributed execution model, where partitions
  /// can be divided among threads or machines.
  ///
  /// Drivers are not required to support partitioning. In this case,
  /// num_partitions will return 0. They are required to support
  /// get_results.
  ///
  ///@{

  /// \brief Get the number of partitions in this statement.
  ///
  /// \param[in] statement The statement object.
  /// \param[out] partitions May be 0, if this statement cannot be
  ///   distributed. (For example, in the case of an in-memory
  ///   database.)
  enum AdbcStatusCode (*num_partitions)(struct AdbcStatement* statement,
                                        size_t* partitions, struct AdbcError* error);

  /// \brief Get the length of the serialized descriptor for a partition in this
  ///   statement.
  enum AdbcStatusCode (*get_partition_desc_size)(struct AdbcStatement* statement,
                                                 size_t index, size_t* length,
                                                 struct AdbcError* error);

  /// \brief Get a serialized descriptor for a partition in this statement.
  ///
  /// The partitions can be reconstructed via
  /// AdbcConnection::deserialize_partition. Effectively, this means
  /// AdbcStatement is similar to arrow::flight::FlightInfo in
  /// Flight/Flight SQL and get_partitions is similar to getting the
  /// arrow::flight::Ticket.
  ///
  /// \param[in] index The partition to get.
  /// \param[out] partition A caller-allocated buffer, to which the
  ///   serialized partition will be written. The length to allocate
  ///   can be queried with get_partition_desc_size.
  enum AdbcStatusCode (*get_partition_desc)(struct AdbcStatement* statement, size_t index,
                                            uint8_t* partition, struct AdbcError* error);

  ///@}

  // TODO: how would we want to handle long-running queries? Do we
  // want a way to provide progress feedback?

  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// }@

/// \page typical-usage Typical Usage Patterns
/// (TODO: describe request sequences)

/// \page decoder-ring Decoder Ring
///
/// ADBC - Flight SQL - JDBC - ODBC
///
/// AdbcConnection - FlightClient - Connection - Connection handle
///
/// AdbcStatement - FlightInfo - Statement - Statement handle
///
/// ArrowArrayStream - FlightStream (Java)/RecordBatchReader (C++) -
/// ResultSet - Statement handle

#ifdef __cplusplus
}
#endif
