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

// Interfaces to use for defining Flight RPC servers. API should be considered
// experimental for now

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "arrow/flight/server.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/visibility.h"
#include "arrow/flight/types.h"

namespace arrow {
namespace flight {
namespace sql {

/// \defgroup flight-sql-protocol-messages Flight SQL Protocol Messages
/// Simple struct wrappers for various protocol messages, used to
/// avoid exposing Protobuf types in the API.
/// @{

/// \brief A SQL query.
struct ARROW_FLIGHT_SQL_EXPORT StatementQuery {
  /// \brief The SQL query.
  std::string query;
  /// \brief The transaction ID, if specified (else a blank string).
  std::string transaction_id;
};

/// \brief A Substrait plan to execute.
struct ARROW_FLIGHT_SQL_EXPORT StatementSubstraitPlan {
  /// \brief The Substrait plan.
  SubstraitPlan plan;
  /// \brief The transaction ID, if specified (else a blank string).
  std::string transaction_id;
};

/// \brief A SQL update query.
struct ARROW_FLIGHT_SQL_EXPORT StatementUpdate {
  /// \brief The SQL query.
  std::string query;
  /// \brief The transaction ID, if specified (else a blank string).
  std::string transaction_id;
};

/// \brief A request to execute a query.
struct ARROW_FLIGHT_SQL_EXPORT StatementQueryTicket {
  /// \brief The server-generated opaque identifier for the query.
  std::string statement_handle;

  static arrow::Result<StatementQueryTicket> Deserialize(std::string_view serialized);
};

/// \brief A prepared query statement.
struct ARROW_FLIGHT_SQL_EXPORT PreparedStatementQuery {
  /// \brief The server-generated opaque identifier for the statement.
  std::string prepared_statement_handle;
};

/// \brief A prepared update statement.
struct ARROW_FLIGHT_SQL_EXPORT PreparedStatementUpdate {
  /// \brief The server-generated opaque identifier for the statement.
  std::string prepared_statement_handle;
};

/// \brief A request to fetch server metadata.
struct ARROW_FLIGHT_SQL_EXPORT GetSqlInfo {
  /// \brief A list of metadata IDs to fetch.
  std::vector<int32_t> info;
};

/// \brief A request to list database schemas.
struct ARROW_FLIGHT_SQL_EXPORT GetDbSchemas {
  /// \brief An optional database catalog to filter on.
  std::optional<std::string> catalog;
  /// \brief An optional database schema to filter on.
  std::optional<std::string> db_schema_filter_pattern;
};

/// \brief A request to list database tables.
struct ARROW_FLIGHT_SQL_EXPORT GetTables {
  /// \brief An optional database catalog to filter on.
  std::optional<std::string> catalog;
  /// \brief An optional database schema to filter on.
  std::optional<std::string> db_schema_filter_pattern;
  /// \brief An optional table name to filter on.
  std::optional<std::string> table_name_filter_pattern;
  /// \brief A list of table types to filter on.
  std::vector<std::string> table_types;
  /// \brief Whether to include the Arrow schema in the response.
  bool include_schema;
};

/// \brief A request to get SQL data type information.
struct ARROW_FLIGHT_SQL_EXPORT GetXdbcTypeInfo {
  /// \brief A specific SQL type ID to fetch information about.
  std::optional<int> data_type;
};

/// \brief A request to list primary keys of a table.
struct ARROW_FLIGHT_SQL_EXPORT GetPrimaryKeys {
  /// \brief The given table.
  TableRef table_ref;
};

/// \brief A request to list foreign key columns referencing primary key
///   columns of a table.
struct ARROW_FLIGHT_SQL_EXPORT GetExportedKeys {
  /// \brief The given table.
  TableRef table_ref;
};

/// \brief A request to list foreign keys of a table.
struct ARROW_FLIGHT_SQL_EXPORT GetImportedKeys {
  /// \brief The given table.
  TableRef table_ref;
};

/// \brief A request to list foreign key columns of a table that
///   reference columns in a given parent table.
struct ARROW_FLIGHT_SQL_EXPORT GetCrossReference {
  /// \brief The parent table (the one containing referenced columns).
  TableRef pk_table_ref;
  /// \brief The foreign table (for which foreign key columns will be listed).
  TableRef fk_table_ref;
};

/// \brief A request to start a new transaction.
struct ARROW_FLIGHT_SQL_EXPORT ActionBeginTransactionRequest {};

/// \brief A request to create a new savepoint.
struct ARROW_FLIGHT_SQL_EXPORT ActionBeginSavepointRequest {
  std::string transaction_id;
  std::string name;
};

/// \brief The result of starting a new savepoint.
struct ARROW_FLIGHT_SQL_EXPORT ActionBeginSavepointResult {
  std::string savepoint_id;
};

/// \brief The result of starting a new transaction.
struct ARROW_FLIGHT_SQL_EXPORT ActionBeginTransactionResult {
  std::string transaction_id;
};

/// \brief A request to end a savepoint.
struct ARROW_FLIGHT_SQL_EXPORT ActionEndSavepointRequest {
  enum EndSavepoint {
    kRelease,
    kRollback,
  };

  std::string savepoint_id;
  EndSavepoint action;
};

/// \brief A request to end a transaction.
struct ARROW_FLIGHT_SQL_EXPORT ActionEndTransactionRequest {
  enum EndTransaction {
    kCommit,
    kRollback,
  };

  std::string transaction_id;
  EndTransaction action;
};

/// \brief An explicit request to cancel a running query.
struct ARROW_FLIGHT_SQL_EXPORT ActionCancelQueryRequest {
  std::unique_ptr<FlightInfo> info;
};

/// \brief A request to create a new prepared statement.
struct ARROW_FLIGHT_SQL_EXPORT ActionCreatePreparedStatementRequest {
  /// \brief The SQL query.
  std::string query;
  /// \brief The transaction ID, if specified (else a blank string).
  std::string transaction_id;
};

/// \brief A request to create a new prepared statement with a Substrait plan.
struct ARROW_FLIGHT_SQL_EXPORT ActionCreatePreparedSubstraitPlanRequest {
  /// \brief The serialized Substrait plan.
  SubstraitPlan plan;
  /// \brief The transaction ID, if specified (else a blank string).
  std::string transaction_id;
};

/// \brief A request to close a prepared statement.
struct ARROW_FLIGHT_SQL_EXPORT ActionClosePreparedStatementRequest {
  /// \brief The server-generated opaque identifier for the statement.
  std::string prepared_statement_handle;
};

/// \brief The result of creating a new prepared statement.
struct ARROW_FLIGHT_SQL_EXPORT ActionCreatePreparedStatementResult {
  /// \brief The schema of the query results, if applicable.
  std::shared_ptr<Schema> dataset_schema;
  /// \brief The schema of the query parameters, if applicable.
  std::shared_ptr<Schema> parameter_schema;
  /// \brief The server-generated opaque identifier for the statement.
  std::string prepared_statement_handle;
};

/// @}

/// \brief A utility function to create a ticket (a opaque binary
/// token that the server uses to identify this query) for a statement
/// query. Intended for Flight SQL server implementations.
///
/// \param[in] statement_handle      The statement handle that will originate the ticket.
/// \return                          The parsed ticket as an string.
ARROW_FLIGHT_SQL_EXPORT
arrow::Result<std::string> CreateStatementQueryTicket(
    const std::string& statement_handle);

/// \brief The base class for Flight SQL servers.
///
/// Applications should subclass this class and override the virtual
/// methods declared on this class.
class ARROW_FLIGHT_SQL_EXPORT FlightSqlServerBase : public FlightServerBase {
 private:
  SqlInfoResultMap sql_info_id_to_result_;

 public:
  /// \name Flight SQL methods
  /// Applications should override these methods to implement the
  /// Flight SQL endpoints.
  /// @{

  /// \brief Get a FlightInfo for executing a SQL query.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The StatementQuery object containing the SQL statement.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightInfo for executing a Substrait plan.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The StatementSubstraitPlan object containing the plan.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the query results.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The StatementQueryTicket containing the statement handle.
  /// \return                 The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command);

  /// \brief Get a FlightInfo for executing an already created prepared statement.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The PreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the prepared statement query results.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The PreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \return                 The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command);

  /// \brief Get a FlightInfo for listing catalogs.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
      const ServerCallContext& context, const FlightDescriptor& descriptor);

  /// \brief Get the schema of the result set of a query.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The StatementQuery containing the SQL query.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The schema of the result set.
  virtual arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor);

  /// \brief Get the schema of the result set of a Substrait plan.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The StatementQuery containing the plan.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The schema of the result set.
  virtual arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor);

  /// \brief Get the schema of the result set of a prepared statement.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The PreparedStatementQuery containing the
  ///                         prepared statement handle.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The schema of the result set.
  virtual arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the list of catalogs.
  /// \param[in] context  Per-call context.
  /// \return             An interface for sending data back to the client.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
      const ServerCallContext& context);

  /// \brief Gets a FlightInfo for retrieving other information (See TypeInfo).
  /// \param[in] context      Per-call context.
  /// \param[in] command      An optional filter for on the data type.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 Status.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoXdbcTypeInfo(
      const ServerCallContext& context, const GetXdbcTypeInfo& command,
      const FlightDescriptor& descriptor);

  /// \brief Gets a FlightDataStream containing information about the data types
  ///        supported.
  /// \param[in] context  Per-call context.
  /// \param[in] command  The GetXdbcTypeInfo object which may contain filter for
  ///                     the date type to be search for.
  /// \return             Status.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetXdbcTypeInfo(
      const ServerCallContext& context, const GetXdbcTypeInfo& command);

  /// \brief Get a FlightInfo for retrieving other information (See SqlInfo).
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetSqlInfo object containing the list of SqlInfo
  ///                         to be returned.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSqlInfo(
      const ServerCallContext& context, const GetSqlInfo& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the list of SqlInfo results.
  /// \param[in] context    Per-call context.
  /// \param[in] command    The GetSqlInfo object containing the list of SqlInfo
  ///                       to be returned.
  /// \return               The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetSqlInfo(
      const ServerCallContext& context, const GetSqlInfo& command);

  /// \brief Get a FlightInfo for listing schemas.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetDbSchemas object which may contain filters for
  ///                         catalog and schema name.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext& context, const GetDbSchemas& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the list of schemas.
  /// \param[in] context   Per-call context.
  /// \param[in] command   The GetDbSchemas object which may contain filters for
  ///                      catalog and schema name.
  /// \return              The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const GetDbSchemas& command);

  ///\brief Get a FlightInfo for listing tables.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetTables object which may contain filters for
  ///                         catalog, schema and table names.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const GetTables& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the list of tables.
  /// \param[in] context   Per-call context.
  /// \param[in] command   The GetTables object which may contain filters for
  ///                      catalog, schema and table names.
  /// \return              The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const GetTables& command);

  /// \brief Get a FlightInfo to extract information about the table types.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
      const ServerCallContext& context, const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the data related to the table types.
  /// \param[in] context   Per-call context.
  /// \return              The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
      const ServerCallContext& context);

  /// \brief Get a FlightInfo to extract information about primary and foreign keys.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetPrimaryKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the data related to the primary and
  /// foreign
  ///        keys.
  /// \param[in] context  Per-call context.
  /// \param[in] command  The GetPrimaryKeys object with necessary information
  ///                     to execute the request.
  /// \return             The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command);

  /// \brief Get a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetExportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the data related to the foreign and
  /// primary
  ///        keys.
  /// \param[in] context  Per-call context.
  /// \param[in] command  The GetExportedKeys object with necessary information
  ///                     to execute the request.
  /// \return             The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command);

  /// \brief Get a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetImportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the data related to the foreign and
  ///        primary keys.
  /// \param[in] context  Per-call context.
  /// \param[in] command  The GetImportedKeys object with necessary information
  ///                     to execute the request.
  /// \return             The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command);

  /// \brief Get a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] context      Per-call context.
  /// \param[in] command      The GetCrossReference object with necessary
  /// information
  ///                         to execute the request.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \return                 The FlightInfo describing where to access the
  ///                         dataset.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context, const GetCrossReference& command,
      const FlightDescriptor& descriptor);

  /// \brief Get a FlightDataStream containing the data related to the foreign and
  ///        primary keys.
  /// \param[in] context  Per-call context.
  /// \param[in] command  The GetCrossReference object with necessary information
  ///                     to execute the request.
  /// \return             The FlightDataStream containing the results.
  virtual arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context, const GetCrossReference& command);

  /// \brief Execute an update SQL statement.
  /// \param[in] context  The call context.
  /// \param[in] command  The StatementUpdate object containing the SQL statement.
  /// \return             The changed record count.
  virtual arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const ServerCallContext& context, const StatementUpdate& command);

  /// \brief Execute an update Substrait plan.
  /// \param[in] context  The call context.
  /// \param[in] command  The StatementSubstraitPlan object containing the plan.
  /// \return             The changed record count.
  virtual arrow::Result<int64_t> DoPutCommandSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command);

  /// \brief Create a prepared statement from a given SQL statement.
  /// \param[in] context  The call context.
  /// \param[in] request  The ActionCreatePreparedStatementRequest object containing the
  ///                     SQL statement.
  /// \return             A ActionCreatePreparedStatementResult containing the dataset
  ///                     and parameter schemas and a handle for created statement.
  virtual arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ServerCallContext& context,
      const ActionCreatePreparedStatementRequest& request);

  /// \brief Create a prepared statement from a Substrait plan.
  /// \param[in] context  The call context.
  /// \param[in] request  The ActionCreatePreparedSubstraitPlanRequest object containing
  ///                     the Substrait plan.
  /// \return             A ActionCreatePreparedStatementResult containing the dataset
  ///                     and parameter schemas and a handle for created statement.
  virtual arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedSubstraitPlan(
      const ServerCallContext& context,
      const ActionCreatePreparedSubstraitPlanRequest& request);

  /// \brief Close a prepared statement.
  /// \param[in] context  The call context.
  /// \param[in] request  The ActionClosePreparedStatementRequest object containing the
  ///                     prepared statement handle.
  virtual Status ClosePreparedStatement(
      const ServerCallContext& context,
      const ActionClosePreparedStatementRequest& request);

  /// \brief Bind parameters to given prepared statement.
  /// \param[in] context  The call context.
  /// \param[in] command  The PreparedStatementQuery object containing the
  ///                     prepared statement handle.
  /// \param[in] reader   A sequence of uploaded record batches.
  /// \param[in] writer   Send metadata back to the client.
  virtual Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                             const PreparedStatementQuery& command,
                                             FlightMessageReader* reader,
                                             FlightMetadataWriter* writer);

  /// \brief Execute an update SQL prepared statement.
  /// \param[in] context  The call context.
  /// \param[in] command  The PreparedStatementUpdate object containing the
  ///                     prepared statement handle.
  /// \param[in] reader   a sequence of uploaded record batches.
  /// \return             The changed record count.
  virtual arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const PreparedStatementUpdate& command,
      FlightMessageReader* reader);

  /// \brief Begin a new transaction.
  /// \param[in] context  The call context.
  /// \param[in] request  Request parameters.
  /// \return             The transaction ID.
  virtual arrow::Result<ActionBeginTransactionResult> BeginTransaction(
      const ServerCallContext& context, const ActionBeginTransactionRequest& request);

  /// \brief Create a new savepoint.
  /// \param[in] context  The call context.
  /// \param[in] request  Request parameters.
  /// \return             The savepoint ID.
  virtual arrow::Result<ActionBeginSavepointResult> BeginSavepoint(
      const ServerCallContext& context, const ActionBeginSavepointRequest& request);

  /// \brief Release/rollback a savepoint.
  /// \param[in] context  The call context.
  /// \param[in] request  The savepoint.
  virtual Status EndSavepoint(const ServerCallContext& context,
                              const ActionEndSavepointRequest& request);

  /// \brief Commit/rollback a transaction.
  /// \param[in] context  The call context.
  /// \param[in] request  The transaction.
  virtual Status EndTransaction(const ServerCallContext& context,
                                const ActionEndTransactionRequest& request);

  /// \brief Attempt to explicitly cancel a FlightInfo.
  /// \param[in] context  The call context.
  /// \param[in] request  The CancelFlightInfoRequest.
  /// \return             The cancellation result.
  virtual arrow::Result<CancelFlightInfoResult> CancelFlightInfo(
      const ServerCallContext& context, const CancelFlightInfoRequest& request);

  /// \brief Set server session option(s).
  /// \param[in] context  The call context.
  /// \param[in] request  The session options to set.
  virtual arrow::Result<SetSessionOptionsResult> SetSessionOptions(
      const ServerCallContext& context, const SetSessionOptionsRequest& request);

  /// \brief Get server session option(s).
  /// \param[in] context  The call context.
  /// \param[in] request  Request object.
  virtual arrow::Result<GetSessionOptionsResult> GetSessionOptions(
      const ServerCallContext& context, const GetSessionOptionsRequest& request);

  /// \brief Close/invalidate the session.
  /// \param[in] context  The call context.
  /// \param[in] request  Request object.
  virtual arrow::Result<CloseSessionResult> CloseSession(
      const ServerCallContext& context, const CloseSessionRequest& request);

  /// \brief Attempt to explicitly cancel a query.
  ///
  /// \param[in] context  The call context.
  /// \param[in] request  The query to cancel.
  /// \return             The cancellation result.
  /// \deprecated Deprecated in 13.0.0. You just need to implement
  /// CancelFlightInfo() to support both the CancelFlightInfo action
  /// (for newer clients) and the CancelQuery action (for older
  /// clients).
  ARROW_DEPRECATED("Deprecated in 13.0.0. Implement CancelFlightInfo() instead.")
  virtual arrow::Result<CancelResult> CancelQuery(
      const ServerCallContext& context, const ActionCancelQueryRequest& request);

  /// \brief Attempt to explicitly renew a FlightEndpoint.
  /// \param[in] context  The call context.
  /// \param[in] request  The RenewFlightEndpointRequest.
  /// \return             The renew result.
  virtual arrow::Result<FlightEndpoint> RenewFlightEndpoint(
      const ServerCallContext& context, const RenewFlightEndpointRequest& request);

  /// @}

  /// \name Utility methods
  /// @{

  /// \brief Register a new SqlInfo result, making it available when calling GetSqlInfo.
  /// \param[in] id the SqlInfo identifier.
  /// \param[in] result the result.
  void RegisterSqlInfo(int32_t id, const SqlInfoResult& result);

  /// @}

  /// \name Flight RPC handlers
  /// Applications should not override these methods; they implement
  /// the Flight SQL protocol.
  /// @{

  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) final;

  Status GetSchema(const ServerCallContext& context, const FlightDescriptor& request,
                   std::unique_ptr<SchemaResult>* schema) override;

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) final;

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) final;

  const ActionType kBeginSavepointActionType =
      ActionType{"BeginSavepoint",
                 "Create a new savepoint.\n"
                 "Request Message: ActionBeginSavepointRequest\n"
                 "Response Message: ActionBeginSavepointResult"};
  const ActionType kBeginTransactionActionType =
      ActionType{"BeginTransaction",
                 "Start a new transaction.\n"
                 "Request Message: ActionBeginTransactionRequest\n"
                 "Response Message: ActionBeginTransactionResult"};
  const ActionType kCreatePreparedStatementActionType =
      ActionType{"CreatePreparedStatement",
                 "Creates a reusable prepared statement resource on the server.\n"
                 "Request Message: ActionCreatePreparedStatementRequest\n"
                 "Response Message: ActionCreatePreparedStatementResult"};
  const ActionType kCreatePreparedSubstraitPlanActionType =
      ActionType{"CreatePreparedSubstraitPlan",
                 "Creates a reusable prepared statement resource on the server.\n"
                 "Request Message: ActionCreatePreparedSubstraitPlanRequest\n"
                 "Response Message: ActionCreatePreparedStatementResult"};
  const ActionType kCancelQueryActionType =
      ActionType{"CancelQuery",
                 "Deprecated since 13.0.0. Use CancelFlightInfo instead.\n"
                 "Explicitly cancel a running query.\n"
                 "Request Message: ActionCancelQueryRequest\n"
                 "Response Message: ActionCancelQueryResult"};
  const ActionType kClosePreparedStatementActionType =
      ActionType{"ClosePreparedStatement",
                 "Closes a reusable prepared statement resource on the server.\n"
                 "Request Message: ActionClosePreparedStatementRequest\n"
                 "Response Message: N/A"};
  const ActionType kEndSavepointActionType =
      ActionType{"EndSavepoint",
                 "End a savepoint.\n"
                 "Request Message: ActionEndSavepointRequest\n"
                 "Response Message: N/A"};
  const ActionType kEndTransactionActionType =
      ActionType{"EndTransaction",
                 "End a savepoint.\n"
                 "Request Message: ActionEndTransactionRequest\n"
                 "Response Message: N/A"};

  Status ListActions(const ServerCallContext& context,
                     std::vector<ActionType>* actions) final;

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) final;

  /// @}
};

/// \brief Auxiliary class containing all Schemas used on Flight SQL.
class ARROW_FLIGHT_SQL_EXPORT SqlSchema {
 public:
  /// \brief Get the Schema used on GetCatalogs response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetCatalogsSchema();

  /// \brief Get the Schema used on GetDbSchemas response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetDbSchemasSchema();

  /// \brief Get the Schema used on GetTables response when included schema
  /// flags is set to false.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetTablesSchema();

  /// \brief Get the Schema used on GetTables response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetTablesSchemaWithIncludedSchema();

  /// \brief Get the Schema used on GetTableTypes response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetTableTypesSchema();

  /// \brief Get the Schema used on GetPrimaryKeys response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetPrimaryKeysSchema();

  /// \brief Get the Schema used on GetImportedKeys response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetExportedKeysSchema();

  /// \brief Get the Schema used on GetImportedKeys response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetImportedKeysSchema();

  /// \brief Get the Schema used on GetCrossReference response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetCrossReferenceSchema();

  /// \brief Get the Schema used on GetXdbcTypeInfo response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetXdbcTypeInfoSchema();

  /// \brief Get the Schema used on GetSqlInfo response.
  /// \return The default schema template.
  static const std::shared_ptr<Schema>& GetSqlInfoSchema();
};
}  // namespace sql
}  // namespace flight
}  // namespace arrow
