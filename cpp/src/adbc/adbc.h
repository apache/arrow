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
typedef uint8_t AdbcStatusCode;

/// No error.
#define ADBC_STATUS_OK 0
/// An unknown error occurred.
#define ADBC_STATUS_UNKNOWN 1
/// The operation is not implemented.
#define ADBC_STATUS_NOT_IMPLEMENTED 2
/// An operation was attempted on an uninitialized object.
#define ADBC_STATUS_UNINITIALIZED 3
/// The arguments are invalid.
#define ADBC_STATUS_INVALID_ARGUMENT 4
/// The object is in an invalid state for the given operation.
#define ADBC_STATUS_INTERNAL 5
/// An I/O error occurred.
#define ADBC_STATUS_IO 6

/// \brief A detailed error message for an operation.
struct AdbcError {
  /// \brief The error message.
  char* message;
};

/// \brief Destroy an error message.
void AdbcErrorRelease(struct AdbcError* error);

/// \brief Get a human-readable description of a status code.
const char* AdbcStatusCodeMessage(AdbcStatusCode code);

/// }@

// Forward declarations
struct AdbcStatement;

/// \defgroup adbc-database Database initialization.
/// Clients first initialize a database, then connect to the database
/// (below). For client-server databases, one of these steps may be a
/// no-op; for in-memory or otherwise non-client-server databases,
/// this gives the implementation a place to initialize and own any
/// common connection state.
/// @{

/// \brief A set of database options.
struct AdbcDatabaseOptions {
  /// \brief A driver-specific database string.
  ///
  /// Should be in ODBC-style format ("Key1=Value1;Key2=Value2").
  const char* target;
  size_t target_length;
};

/// \brief An instance of a database.
///
/// Must be kept alive as long as any connections exist.
struct AdbcDatabase {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// \brief Initialize a new database.
AdbcStatusCode AdbcDatabaseInit(const struct AdbcDatabaseOptions* options,
                                struct AdbcDatabase* out, struct AdbcError* error);

/// \brief Destroy this database. No connections may exist.
/// \param[in] database The database to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error);

/// }@

/// \defgroup adbc-connection Connection establishment.
/// @{

/// \brief A set of connection options.
struct AdbcConnectionOptions {
  /// \brief The database to connect to.
  struct AdbcDatabase* database;

  /// \brief A driver-specific connection string.
  ///
  /// Should be in ODBC-style format ("Key1=Value1;Key2=Value2").
  const char* target;
  size_t target_length;
};

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not thread-safe and clients should take care to
/// serialize accesses to a connection.
struct AdbcConnection {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// \brief Create a new connection to a database.
AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                  struct AdbcConnection* out, struct AdbcError* error);

/// \brief Destroy this connection.
/// \param[in] connection The connection to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error);

/// \defgroup adbc-connection-sql SQL Semantics
/// Functions for executing SQL queries, or querying SQL-related
/// metadata. Drivers are not required to support both SQL and
/// Substrait semantics. If they do, it may be via converting
/// between representations internally.
/// @{

/// \brief Execute a one-shot query.
///
/// For queries expected to be executed repeatedly, create a
/// prepared statement.
///
/// \param[in] connection The database connection.
/// \param[in] query The query to execute.
/// \param[in] query_length The length of the query string.
/// \param[in,out] statement The result set. Allocate with AdbcStatementInit.
/// \param[out] error Error details, if an error occurs.
AdbcStatusCode AdbcConnectionSqlExecute(struct AdbcConnection* connection,
                                        const char* query, size_t query_length,
                                        struct AdbcStatement* statement,
                                        struct AdbcError* error);

/// \brief Prepare a query to be executed multiple times.
///
/// TODO: this should return AdbcPreparedStatement to disaggregate
/// preparation and execution
AdbcStatusCode AdbcConnectionSqlPrepare(struct AdbcConnection* connection,
                                        const char* query, size_t query_length,
                                        struct AdbcStatement* statement,
                                        struct AdbcError* error);

/// }@

/// \defgroup adbc-connection-substrait Substrait Semantics
/// Functions for executing Substrait plans, or querying
/// Substrait-related metadata.  Drivers are not required to support
/// both SQL and Substrait semantics.  If they do, it may be via
/// converting between representations internally.
/// @{

// TODO: not yet defined

/// }@

/// \defgroup adbc-connection-partition Partitioned Results
/// Some databases may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines for processing.
///
/// Drivers are not required to support partitioning.
///
/// Partitions are not ordered. If the result set is sorted,
/// implementations should return a single partition.
///
/// @{

/// \brief Construct a statement for a partition of a query. The
///   statement can then be read independently.
///
/// A partition can be retrieved from AdbcStatementGetPartitionDesc.
AdbcStatusCode AdbcConnectionDeserializePartitionDesc(struct AdbcConnection* connection,
                                                      const uint8_t* serialized_partition,
                                                      size_t serialized_length,
                                                      struct AdbcStatement* statement,
                                                      struct AdbcError* error);

/// }@

/// \defgroup adbc-connection-metadata Metadata
/// Functions for retrieving metadata about the database.
///
/// Generally, these functions return an AdbcStatement that can be evaluated to
/// get the metadata as Arrow data. The returned metadata has an expected
/// schema given in the function docstring. Schema fields are nullable unless
/// otherwise marked.
///
/// Some functions accept a "search pattern" argument, which is a string that
/// can contain the special character "%" to match zero or more characters, or
/// "_" to match exactly one character. (See the documentation of
/// DatabaseMetaData in JDBC or "Pattern Value Arguments" in the ODBC
/// documentation.)
///
/// TODO: escaping in search patterns?
///
/// @{

/// \brief Get a list of catalogs in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
AdbcStatusCode AdbcConnectionGetCatalogs(struct AdbcConnection* connection,
                                         struct AdbcStatement* statement,
                                         struct AdbcError* error);

/// \brief Get a list of schemas in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8
/// db_schema_name | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
AdbcStatusCode AdbcConnectionGetDbSchemas(struct AdbcConnection* connection,
                                          struct AdbcStatement* statement,
                                          struct AdbcError* error);

/// \brief Get a list of table types in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// table_type     | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct AdbcStatement* statement,
                                           struct AdbcError* error);

/// \brief Get a list of tables matching the given criteria.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8
/// db_schema_name | utf8
/// table_name     | utf8 not null
/// table_type     | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[in] catalog Only show tables in the given catalog. If NULL, do not
///   filter by catalog. If an empty string, only show tables without a
///   catalog.
/// \param[in] catalog_length The length of the catalog parameter (ignored if
///   catalog is NULL).
/// \param[in] db_schema Only show tables in the given database schema. If
///   NULL, do not filter by database schema. If an empty string, only show
///   tables without a database schema. May be a search pattern (see section
///   documentation).
/// \param[in] db_schema_length The length of the db_schema parameter (ignored
///   if db_schema is NULL).
/// \param[in] table_name Only show tables with the given name. If NULL, do not
///   filter by name. May be a search pattern (see section documentation).
/// \param[in] table_name_length The length of the table_name parameter
///   (ignored if table_name is NULL).
/// \param[in] table_types Only show tables matching one of the given table
///   types. If NULL, show tables of any type. Valid table types can be fetched
///   from get_table_types.
/// \param[in] table_types_length The size of the table_types array (ignored if
///   table_types is NULL).
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
AdbcStatusCode AdbcConnectionGetTables(
    struct AdbcConnection* connection, const char* catalog, size_t catalog_length,
    const char* db_schema, size_t db_schema_length, const char* table_name,
    size_t table_name_length, const char** table_types, size_t table_types_length,
    struct AdbcStatement* statement, struct AdbcError* error);
/// }@

/// }@

/// \defgroup adbc-statement Managing statements.
/// Applications should first initialize and configure a statement with
/// AdbcStatementInit and the AdbcStatementSetOption functions, then use the
/// statement with a function like AdbcConnectionSqlExecute.
/// @{

/// \brief An instance of a database query, from parameters set before
///   execution to the result of execution.
///
/// Statements are not thread-safe and clients should take care to
/// serialize access.
struct AdbcStatement {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// \brief Create a new statement for a given connection.
AdbcStatusCode AdbcStatementInit(struct AdbcConnection* connection,
                                 struct AdbcStatement* statement,
                                 struct AdbcError* error);

/// \brief Set an integer option on a statement.
AdbcStatusCode AdbcStatementSetOptionInt64(struct AdbcStatement* statement,
                                           struct AdbcError* error);

/// \brief Destroy a statement.
/// \param[in] statement The statement to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error);

/// \brief Read the result of a statement.
///
/// This method can be called only once. It may not be called if any
/// of the partitioning methods have been called (see below).
///
/// \return out A stream of Arrow data. The stream itself must be
///   released before the statement is released.
AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error);

/// \defgroup adbc-statement-partition Partitioned Results
/// Some backends may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines. Partitions are exposed
/// as an iterator.
///
/// Drivers are not required to support partitioning. In this case,
/// num_partitions will return 0. They are required to support get_results.
///
/// If any of the partitioning methods are called, get_results may not be
/// called, and vice versa.
///
/// @{

/// \brief Get the length of the serialized descriptor for the current
///   partition.
///
/// This method must be called first, before calling other partitioning
/// methods. This method may block and perform I/O.
/// \param[in] statement The statement.
/// \param[out] length The length of the serialized partition, or 0 if there
///   are no more partitions.
/// \param[out] error An optional location to return an error message if
///   necessary.
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length, struct AdbcError* error);

/// \brief Get the serialized descriptor for the current partition, and advance
///   the iterator.
///
/// This method may block and perform I/O.
///
/// A partition can be turned back into a statement via
/// AdbcConnectionDeserializePartitionDesc. Effectively, this means AdbcStatement
/// is similar to arrow::flight::FlightInfo in Flight/Flight SQL and
/// get_partitions is similar to getting the arrow::flight::Ticket.
///
/// \param[in] statement The statement.
/// \param[out] partition_desc A caller-allocated buffer, to which the
///   serialized partition will be written. The length to allocate can be
///   queried with AdbcStatementGetPartitionDescSize.
/// \param[out] error An optional location to return an error message if
///   necessary.
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error);

/// }@

/// }@

/// \defgroup adbc-driver Driver initialization.
/// @{

/// \brief A table of function pointers for ADBC functions.
///
/// This provides a common interface for implementation-specific
/// driver initialization routines.
struct AdbcDriver {
  // TODO: migrate drivers
  // Do not edit fields. New fields can only be appended to the end.
};

/// \brief Common entry point for drivers using dlopen(3).
///
/// \param[in] count The number of entries to initialize. Provides
///   backwards compatibility if the struct definition is changed.
/// \param[out] driver The table of function pointers to initialize.
/// \param[out] initialized How much of the table was actually
///   initialized (can be less than count).
/// \param[out] error An optional location to return an error message if
///   necessary.
AdbcStatusCode AdbcDriverInit(size_t count, struct AdbcDriver* driver,
                              size_t* initialized, struct AdbcError* error);

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
