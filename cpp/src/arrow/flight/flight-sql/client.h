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

#ifndef ARROW_FLIGHT_SQL_CLIENT_H
#define ARROW_FLIGHT_SQL_CLIENT_H

#include <arrow/flight/Flight.pb.h>
#include <arrow/flight/client.h>
#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <google/protobuf/message.h>

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

template <class T = arrow::flight::FlightClient>
class ARROW_EXPORT PreparedStatementT {
  pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;
  std::shared_ptr<RecordBatch> parameter_binding;
  FlightCallOptions options;
  bool is_closed;
  T* client;

 public:
  /// \brief Constructor for the PreparedStatement class.
  /// \param[in] client   A raw pointer to FlightClient.
  /// \param[in] query      The query that will be executed.
  PreparedStatementT(
      T* client, const std::string& query,
      pb::sql::ActionCreatePreparedStatementResult& prepared_statement_result,
      const FlightCallOptions& options);

  /// \brief Default destructor for the PreparedStatement class.
  /// The destructor will call the Close method from the class in order,
  /// to send a request to close the PreparedStatement.
  ~PreparedStatementT();

  /// \brief Executes the prepared statement query on the server.
  /// \param[in] call_options RPC-layer hints for this call.
  /// \param[out] info        A FlightInfo object representing the stream(s) to fetch.
  /// \return Status.
  Status Execute(std::unique_ptr<FlightInfo>* info);

  /// \brief Retrieve the parameter schema from the query.
  /// \param schema   The parameter schema from the query.
  /// \return         Status.
  Status GetParameterSchema(std::shared_ptr<Schema>* schema);

  /// \brief Retrieve the ResultSet schema from the query.
  /// \param schema   The ResultSet schema from the query.
  /// \return         Status.
  Status GetResultSetSchema(std::shared_ptr<Schema>* schema);

  /// \brief Set a RecordBatch that contains the parameters that will be bind.
  /// \param parameter_binding_   The parameters that will be bind.
  /// \return                     Status.
  Status SetParameters(std::shared_ptr<RecordBatch> parameter_binding);

  /// \brief Closes the prepared statement.
  /// \param[in] options  RPC-layer hints for this call.
  /// \return Status.
  Status Close();

  /// \brief Checks if the prepared statement is closed.
  /// \return The state of the prepared statement.
  bool IsClosed() const;
};

/// \brief Flight client with Flight SQL semantics.
template <class T = arrow::flight::FlightClient>
class FlightSqlClientT {
 public:
  explicit FlightSqlClientT(std::unique_ptr<T>& client);

  ~FlightSqlClientT();

  /// \brief Execute a query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset
  /// \return Status.
  Status Execute(const FlightCallOptions& options, const std::string& query,
                 std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Execute an update query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \param[out] rows        The quantity of rows affected by the operation.
  /// \return Status.
  Status ExecuteUpdate(const FlightCallOptions& options, const std::string& query,
                       int64_t* rows) const;

  /// \brief Request a list of catalogs.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetCatalogs(const FlightCallOptions& options,
                     std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Request a list of schemas.
  /// \param[in] options                RPC-layer hints for this call.
  /// \param[in] catalog                The catalog.
  /// \param[in] schema_filter_pattern  The schema filter pattern.
  /// \param[out] flight_info           The FlightInfo describing where to access the
  ///                                   dataset.
  /// \return Status.
  Status GetSchemas(const FlightCallOptions& options, const std::string* catalog,
                    const std::string* schema_filter_pattern,
                    std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Given a flight ticket and schema, request to be sent the
  /// stream. Returns record batch stream reader
  /// \param[in] options Per-RPC options
  /// \param[in] ticket The flight ticket to use
  /// \param[out] stream the returned RecordBatchReader
  /// \return Status
  Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream) const;

  /// \brief Request a list of tables.
  /// \param[in] options                  RPC-layer hints for this call.
  /// \param[in] catalog                  The catalog.
  /// \param[in] schema_filter_pattern    The schema filter pattern.
  /// \param[in] table_filter_pattern     The table filter pattern.
  /// \param[in] include_schema           True to include the schema upon return,
  ///                                     false to not include the schema.
  /// \param[in] table_types              The table types to include.
  /// \param[out] flight_info             The FlightInfo describing where to access the
  ///                                     dataset.
  /// \return Status.
  Status GetTables(const FlightCallOptions& options, const std::string* catalog,
                   const std::string* schema_filter_pattern,
                   const std::string* table_filter_pattern, bool include_schema,
                   std::vector<std::string>& table_types,
                   std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Request the primary keys for a table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] catalog          The catalog.
  /// \param[in] schema           The schema.
  /// \param[in] table            The table.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetPrimaryKeys(const FlightCallOptions& options, const std::string* catalog,
                        const std::string* schema, const std::string& table,
                        std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Retrieves a description about the foreign key columns that reference the
  /// primary key columns of the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] catalog          The foreign key table catalog.
  /// \param[in] schema           The foreign key table schema.
  /// \param[in] table            The foreign key table. Cannot be null.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetExportedKeys(const FlightCallOptions& options, const std::string* catalog,
                         const std::string* schema, const std::string& table,
                         std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Retrieves the foreign key columns for the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] catalog          The primary key table catalog.
  /// \param[in] schema           The primary key table schema.
  /// \param[in] table            The primary key table. Cannot be null.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetImportedKeys(const FlightCallOptions& options, const std::string* catalog,
                         const std::string* schema, const std::string& table,
                         std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Request a list of table types.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetTableTypes(const FlightCallOptions& options,
                       std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Request a list of SQL information.
  /// \param[in] options RPC-layer hints for this call.
  /// \param[in] sql_info the SQL info required.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetSqlInfo(const FlightCallOptions& options,
                    const std::vector<int>& sql_info,
                    std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Request a list of SQL information.
  /// \param[in] options RPC-layer hints for this call.
  /// \param[in] sql_info the SQL info required.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetSqlInfo(const FlightCallOptions& options,
                    const std::vector<pb::sql::SqlInfo>& sql_info,
                    std::unique_ptr<FlightInfo>* flight_info) const;

  /// \brief Create a prepared statement object.
  /// \param[in] options              RPC-layer hints for this call.
  /// \param[in] query                The query that will be executed.
  /// \param[out] prepared_statement  The created prepared statement.
  /// \return Status.
  Status Prepare(const FlightCallOptions& options, const std::string& query,
                 std::shared_ptr<PreparedStatementT<T>>* prepared_statement);

 private:
  std::unique_ptr<T> client;
};

}  // namespace internal

using FlightSqlClient = internal::FlightSqlClientT<>;
using PreparedStatement = internal::PreparedStatementT<>;

}  // namespace sql
}  // namespace flight
}  // namespace arrow

#endif  // ARROW_FLIGHT_SQL_CLIENT_H

#include <arrow/flight/flight-sql/client_impl.h>
