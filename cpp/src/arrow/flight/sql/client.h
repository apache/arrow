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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/flight/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/visibility.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace sql {

class PreparedStatement;
class Transaction;
class Savepoint;

/// \brief A default transaction to use when the default behavior
///   (auto-commit) is desired.
ARROW_FLIGHT_SQL_EXPORT
const Transaction& no_transaction();

/// \brief Flight client with Flight SQL semantics.
///
/// Wraps a Flight client to provide the Flight SQL RPC calls.
class ARROW_FLIGHT_SQL_EXPORT FlightSqlClient {
  friend class PreparedStatement;

 private:
  std::shared_ptr<FlightClient> impl_;

 public:
  explicit FlightSqlClient(std::shared_ptr<FlightClient> client);

  virtual ~FlightSqlClient() = default;

  /// \brief Execute a SQL query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The UTF8-encoded SQL query to be executed.
  /// \param[in] transaction  A transaction to associate this query with.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> Execute(
      const FlightCallOptions& options, const std::string& query,
      const Transaction& transaction = no_transaction());

  /// \brief Execute a Substrait plan that returns a result set on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] plan         The plan to be executed.
  /// \param[in] transaction  A transaction to associate this query with.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> ExecuteSubstrait(
      const FlightCallOptions& options, const SubstraitPlan& plan,
      const Transaction& transaction = no_transaction());

  /// \brief Get the result set schema from the server.
  arrow::Result<std::unique_ptr<SchemaResult>> GetExecuteSchema(
      const FlightCallOptions& options, const std::string& query,
      const Transaction& transaction = no_transaction());

  /// \brief Get the result set schema from the server.
  arrow::Result<std::unique_ptr<SchemaResult>> GetExecuteSubstraitSchema(
      const FlightCallOptions& options, const SubstraitPlan& plan,
      const Transaction& transaction = no_transaction());

  /// \brief Execute an update query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The UTF8-encoded SQL query to be executed.
  /// \param[in] transaction  A transaction to associate this query with.
  /// \return The quantity of rows affected by the operation.
  arrow::Result<int64_t> ExecuteUpdate(const FlightCallOptions& options,
                                       const std::string& query,
                                       const Transaction& transaction = no_transaction());

  /// \brief Execute a Substrait plan that does not return a result set on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] plan         The plan to be executed.
  /// \param[in] transaction  A transaction to associate this query with.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<int64_t> ExecuteSubstraitUpdate(
      const FlightCallOptions& options, const SubstraitPlan& plan,
      const Transaction& transaction = no_transaction());

  /// \brief Request a list of catalogs.
  /// \param[in] options      RPC-layer hints for this call.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetCatalogs(
      const FlightCallOptions& options);

  /// \brief Get the catalogs schema from the server (should be
  ///   identical to SqlSchema::GetCatalogsSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetCatalogsSchema(
      const FlightCallOptions& options);

  /// \brief Request a list of database schemas.
  /// \param[in] options                   RPC-layer hints for this call.
  /// \param[in] catalog                   The catalog.
  /// \param[in] db_schema_filter_pattern  The schema filter pattern.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetDbSchemas(
      const FlightCallOptions& options, const std::string* catalog,
      const std::string* db_schema_filter_pattern);

  /// \brief Get the database schemas schema from the server (should be
  ///   identical to SqlSchema::GetDbSchemasSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetDbSchemasSchema(
      const FlightCallOptions& options);

  /// \brief Given a flight ticket and schema, request to be sent the
  /// stream. Returns record batch stream reader
  /// \param[in] options Per-RPC options
  /// \param[in] ticket The flight ticket to use
  /// \return The returned RecordBatchReader
  arrow::Result<std::unique_ptr<FlightStreamReader>> DoGet(
      const FlightCallOptions& options, const Ticket& ticket);

  /// \brief Request a list of tables.
  /// \param[in] options                   RPC-layer hints for this call.
  /// \param[in] catalog                   The catalog.
  /// \param[in] db_schema_filter_pattern  The schema filter pattern.
  /// \param[in] table_filter_pattern      The table filter pattern.
  /// \param[in] include_schema            True to include the schema upon return,
  ///                                      false to not include the schema.
  /// \param[in] table_types               The table types to include.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetTables(
      const FlightCallOptions& options, const std::string* catalog,
      const std::string* db_schema_filter_pattern,
      const std::string* table_filter_pattern, bool include_schema,
      const std::vector<std::string>* table_types);

  /// \brief Get the tables schema from the server (should be
  ///   identical to SqlSchema::GetTablesSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetTablesSchema(
      const FlightCallOptions& options, bool include_schema);

  /// \brief Request the primary keys for a table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetPrimaryKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

  /// \brief Get the primary keys schema from the server (should be
  ///   identical to SqlSchema::GetPrimaryKeysSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetPrimaryKeysSchema(
      const FlightCallOptions& options);

  /// \brief Retrieves a description about the foreign key columns that reference the
  /// primary key columns of the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetExportedKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

  /// \brief Get the exported keys schema from the server (should be
  ///   identical to SqlSchema::GetExportedKeysSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetExportedKeysSchema(
      const FlightCallOptions& options);

  /// \brief Retrieves the foreign key columns for the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetImportedKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

  /// \brief Get the imported keys schema from the server (should be
  ///   identical to SqlSchema::GetImportedKeysSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetImportedKeysSchema(
      const FlightCallOptions& options);

  /// \brief Retrieves a description of the foreign key columns in the given foreign key
  ///        table that reference the primary key or the columns representing a unique
  ///        constraint of the parent table (could be the same or a different table).
  /// \param[in] options        RPC-layer hints for this call.
  /// \param[in] pk_table_ref   The table reference that exports the key.
  /// \param[in] fk_table_ref   The table reference that imports the key.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetCrossReference(
      const FlightCallOptions& options, const TableRef& pk_table_ref,
      const TableRef& fk_table_ref);

  /// \brief Get the cross reference schema from the server (should be
  ///   identical to SqlSchema::GetCrossReferenceSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetCrossReferenceSchema(
      const FlightCallOptions& options);

  /// \brief Request a list of table types.
  /// \param[in] options          RPC-layer hints for this call.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetTableTypes(
      const FlightCallOptions& options);

  /// \brief Get the table types schema from the server (should be
  ///   identical to SqlSchema::GetTableTypesSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetTableTypesSchema(
      const FlightCallOptions& options);

  /// \brief Request the information about all the data types supported.
  /// \param[in] options          RPC-layer hints for this call.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetXdbcTypeInfo(
      const FlightCallOptions& options);

  /// \brief Request the information about all the data types supported.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] data_type        The data type to search for as filtering.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetXdbcTypeInfo(
      const FlightCallOptions& options, int data_type);

  /// \brief Get the type info schema from the server (should be
  ///   identical to SqlSchema::GetXdbcTypeInfoSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetXdbcTypeInfoSchema(
      const FlightCallOptions& options);

  /// \brief Request a list of SQL information.
  /// \param[in] options RPC-layer hints for this call.
  /// \param[in] sql_info the SQL info required.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetSqlInfo(const FlightCallOptions& options,
                                                        const std::vector<int>& sql_info);

  /// \brief Get the SQL information schema from the server (should be
  ///   identical to SqlSchema::GetSqlInfoSchema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetSqlInfoSchema(
      const FlightCallOptions& options);

  /// \brief Create a prepared statement object.
  /// \param[in] options              RPC-layer hints for this call.
  /// \param[in] query                The query that will be executed.
  /// \param[in] transaction          A transaction to associate this query with.
  /// \return The created prepared statement.
  arrow::Result<std::shared_ptr<PreparedStatement>> Prepare(
      const FlightCallOptions& options, const std::string& query,
      const Transaction& transaction = no_transaction());

  /// \brief Create a prepared statement object.
  /// \param[in] options              RPC-layer hints for this call.
  /// \param[in] plan                 The Substrait plan that will be executed.
  /// \param[in] transaction          A transaction to associate this query with.
  /// \return The created prepared statement.
  arrow::Result<std::shared_ptr<PreparedStatement>> PrepareSubstrait(
      const FlightCallOptions& options, const SubstraitPlan& plan,
      const Transaction& transaction = no_transaction());

  /// \brief Call the underlying Flight client's GetFlightInfo.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfo(
      const FlightCallOptions& options, const FlightDescriptor& descriptor) {
    return impl_->GetFlightInfo(options, descriptor);
  }

  /// \brief Call the underlying Flight client's GetSchema.
  virtual arrow::Result<std::unique_ptr<SchemaResult>> GetSchema(
      const FlightCallOptions& options, const FlightDescriptor& descriptor) {
    return impl_->GetSchema(options, descriptor);
  }

  /// \brief Begin a new transaction.
  ::arrow::Result<Transaction> BeginTransaction(const FlightCallOptions& options);

  /// \brief Create a new savepoint within a transaction.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] transaction  The parent transaction.
  /// \param[in] name         A friendly name for the savepoint.
  ::arrow::Result<Savepoint> BeginSavepoint(const FlightCallOptions& options,
                                            const Transaction& transaction,
                                            const std::string& name);

  /// \brief Commit a transaction.
  ///
  /// After this, the transaction and all associated savepoints will
  /// be invalidated.
  ///
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] transaction  The transaction.
  Status Commit(const FlightCallOptions& options, const Transaction& transaction);

  /// \brief Release a savepoint.
  ///
  /// After this, the savepoint (and all savepoints created after it) will be invalidated.
  ///
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] savepoint    The savepoint.
  Status Release(const FlightCallOptions& options, const Savepoint& savepoint);

  /// \brief Rollback a transaction.
  ///
  /// After this, the transaction and all associated savepoints will be invalidated.
  ///
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] transaction  The transaction.
  Status Rollback(const FlightCallOptions& options, const Transaction& transaction);

  /// \brief Rollback a savepoint.
  ///
  /// After this, the savepoint will still be valid, but all
  /// savepoints created after it will be invalidated.
  ///
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] savepoint    The savepoint.
  Status Rollback(const FlightCallOptions& options, const Savepoint& savepoint);

  /// \brief Explicitly cancel a query.
  ///
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] info         The FlightInfo of the query to cancel.
  ::arrow::Result<CancelResult> CancelQuery(const FlightCallOptions& options,
                                            const FlightInfo& info);

  /// \brief Explicitly shut down and clean up the client.
  Status Close();

 protected:
  virtual Status DoPut(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       const std::shared_ptr<Schema>& schema,
                       std::unique_ptr<FlightStreamWriter>* writer,
                       std::unique_ptr<FlightMetadataReader>* reader) {
    ARROW_ASSIGN_OR_RAISE(auto result, impl_->DoPut(options, descriptor, schema));
    *writer = std::move(result.writer);
    *reader = std::move(result.reader);
    return Status::OK();
  }

  virtual Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
                       std::unique_ptr<FlightStreamReader>* stream) {
    return impl_->DoGet(options, ticket).Value(stream);
  }

  virtual Status DoAction(const FlightCallOptions& options, const Action& action,
                          std::unique_ptr<ResultStream>* results) {
    return impl_->DoAction(options, action).Value(results);
  }
};

/// \brief A prepared statement that can be executed.
class ARROW_FLIGHT_SQL_EXPORT PreparedStatement {
 public:
  /// \brief Create a new prepared statement. However, applications
  /// should generally use FlightSqlClient::Prepare.
  ///
  /// \param[in] client                Client object used to make the RPC requests.
  /// \param[in] handle                Handle for this prepared statement.
  /// \param[in] dataset_schema        Schema of the resulting dataset.
  /// \param[in] parameter_schema      Schema of the parameters (if any).
  PreparedStatement(FlightSqlClient* client, std::string handle,
                    std::shared_ptr<Schema> dataset_schema,
                    std::shared_ptr<Schema> parameter_schema);

  /// \brief Default destructor for the PreparedStatement class.
  /// The destructor will call the Close method from the class in order,
  /// to send a request to close the PreparedStatement.
  /// NOTE: It is best to explicitly close the PreparedStatement, otherwise
  /// errors can't be caught.
  ~PreparedStatement();

  /// \brief Create a PreparedStatement by parsing the server response.
  static arrow::Result<std::shared_ptr<PreparedStatement>> ParseResponse(
      FlightSqlClient* client, std::unique_ptr<ResultStream> results);

  /// \brief Executes the prepared statement query on the server.
  /// \return A FlightInfo object representing the stream(s) to fetch.
  arrow::Result<std::unique_ptr<FlightInfo>> Execute(
      const FlightCallOptions& options = {});

  /// \brief Executes the prepared statement update query on the server.
  /// \return The number of rows affected.
  arrow::Result<int64_t> ExecuteUpdate(const FlightCallOptions& options = {});

  /// \brief Retrieve the parameter schema from the query.
  /// \return The parameter schema from the query.
  const std::shared_ptr<Schema>& parameter_schema() const;

  /// \brief Retrieve the ResultSet schema from the query.
  /// \return The ResultSet schema from the query.
  const std::shared_ptr<Schema>& dataset_schema() const;

  /// \brief Set a RecordBatch that contains the parameters that will be bound.
  Status SetParameters(std::shared_ptr<RecordBatch> parameter_binding);

  /// \brief Set a RecordBatchReader that contains the parameters that will be bound.
  Status SetParameters(std::shared_ptr<RecordBatchReader> parameter_binding);

  /// \brief Re-request the result set schema from the server (should
  ///   be identical to dataset_schema).
  arrow::Result<std::unique_ptr<SchemaResult>> GetSchema(
      const FlightCallOptions& options = {});

  /// \brief Close the prepared statement so the server can free up any resources.
  ///
  /// After this, the prepared statement may not be used anymore.
  Status Close(const FlightCallOptions& options = {});

  /// \brief Check if the prepared statement is closed.
  /// \return The state of the prepared statement.
  bool IsClosed() const;

 private:
  FlightSqlClient* client_;
  std::string handle_;
  std::shared_ptr<Schema> dataset_schema_;
  std::shared_ptr<Schema> parameter_schema_;
  std::shared_ptr<RecordBatchReader> parameter_binding_;
  bool is_closed_;
};

/// \brief A handle for a server-side savepoint.
class ARROW_FLIGHT_SQL_EXPORT Savepoint {
 public:
  explicit Savepoint(std::string savepoint_id) : savepoint_id_(std::move(savepoint_id)) {}
  const std::string& savepoint_id() const { return savepoint_id_; }
  bool is_valid() const { return !savepoint_id_.empty(); }

 private:
  std::string savepoint_id_;
};

/// \brief A handle for a server-side transaction.
class ARROW_FLIGHT_SQL_EXPORT Transaction {
 public:
  explicit Transaction(std::string transaction_id)
      : transaction_id_(std::move(transaction_id)) {}
  const std::string& transaction_id() const { return transaction_id_; }
  bool is_valid() const { return !transaction_id_.empty(); }

 private:
  std::string transaction_id_;
};

}  // namespace sql
}  // namespace flight
}  // namespace arrow
