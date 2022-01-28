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

#include <stdint.h>

#include <arrow/c/abi.h>

#ifdef __cplusplus
extern "C" {
#endif

/// \file ADBC: Arrow DataBase connectivity (client API)
///
/// Implemented by libadbc.so (provided by Arrow/C++), which in turn
/// dynamically loads the appropriate driver.
///
/// EXPERIMENTAL. Interface subject to change.

/// \page object-model Object Model
/// (describe void* private_data)

/// \defgroup adbc-error-handling Error handling primitives.
/// ADBC uses integer error codes to signal errors. To provide more
/// detail about errors, each ADBC object provides a `get_error`
/// method, which can be called repeatedly to get a log of error
/// messages. This is intended to separate error messages from
/// different contexts. The caller should free or delete the error
/// string after retrieving it.
///
/// If there is concurrent usage of an object and errors occur,
/// then there is no guarantee on the ordering of error messages.
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
  // TODO: more codes as appropriate
};

/// }@

struct AdbcStatement;

/// \defgroup adbc-connection Connection establishment.
/// @{

/// \brief A set of connection options.
struct AdbcConnectionOptions {
  /// \brief A driver-specific connection string.
  const char* target;
};

// TODO: Do we prefer an API like this, which mimics Arrow C ABI more,
// or something more like ODBC which basically assumes the presence of
// a driver manager and use of dlopen/LoadLibrary?

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// TODO: thread safety? Send+Sync?
struct AdbcConnection {
  /// \name Common Functions
  /// Standard functions for memory management and error handling of
  /// ADBC types.
  ///@{

  /// \brief Destroy this connection.
  void (*release)(struct AdbcConnection* connection);

  /// \brief Clean up this connection. Errors in closing can be
  ///   retrieved from get_error before calling release.
  enum AdbcStatusCode (*close)(struct AdbcConnection* connection);

  /// \brief Page through error details associated with this object.
  char* (*get_error)(struct AdbcConnection* connection);

  ///@}

  /// \name SQL Semantics
  /// TODO DESCRIPTION GOES HERE
  ///@{

  /// \brief Execute a one-shot query.
  ///
  /// For queries expected to be executed repeatedly, create a
  /// prepared statement.
  enum AdbcStatusCode (*sql_execute)(struct AdbcConnection* connection, const char* query,
                                     struct AdbcStatement* statement);

  ///@}

  /// \name Substrait Semantics
  /// TODO DESCRIPTION GOES HERE
  ///@{

  ///@}

  /// \name Prepared Statements
  /// TODO DESCRIPTION GOES HERE
  ///@{

  ///@}

  /// \name Transactions
  /// TODO DESCRIPTION GOES HERE
  ///@{

  ///@}

  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// \brief Create a new connection to a database.
///
/// On failure, *out may have valid values for `get_error` and
/// `release`, so error messages can be retrieved.
enum AdbcStatusCode AdbcConnectionInit(const struct AdbcConnectionOptions* options,
                                       struct AdbcConnection* out);

/// }@

/// \defgroup adbc-statement Managing statements.
/// @{

// TODO: ODBC uses a single "handle" concept and allocation function (but
// originally had separate functions for each handle type); is there
// any benefit to doing this for us? what was the motivation?

// TODO: take a look at what JDBC does for this
// TODO: take a closer look at what ODBC does for this
// TODO: PEP 249 seems to call this a "Cursor" though the concept is different
// TODO: ODBC has its own "cursor"
// TODO: relation between this and a prepared statement

/// \brief The results of executing a statement.
struct AdbcStatement {
  /// \name Common Functions
  /// Standard functions for memory management and error handling of
  /// ADBC types.
  ///@{

  /// \brief Destroy this statement.
  void (*release)(struct AdbcStatement* statement);

  /// \brief Clean up this statement. Errors in closing can be
  ///   retrieved from get_error before calling release.
  enum AdbcStatusCode (*close)(struct AdbcStatement* connection);

  /// \brief Page through error details associated with this object.
  char* (*get_error)(struct AdbcStatement* statement);

  ///@}

  /// \brief Read the result of a statement.
  ///
  /// This method can be called only once.
  ///
  /// \return out A stream of Arrow data. The stream itself must be
  ///   released before the statement is released.
  enum AdbcStatusCode (*get_results)(struct AdbcStatement* statement,
                                     struct ArrowArrayStream* out);

  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
};

/// }@

/// \page typical-usage Typical Usage Patterns
/// (describe request sequences)

#ifdef __cplusplus
}
#endif
