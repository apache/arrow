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

#include <arrow/flight/flight_sql/example/sqlite_statement.h>
#include <arrow/flight/flight_sql/example/sqlite_statement_batch_reader.h>
#include <arrow/flight/flight_sql/server.h>
#include <arrow/flight/server.h>

#include <memory>
#include <string>

namespace arrow {
namespace flight {
namespace sql {

struct StatementQuery {
  std::string query;
};

struct StatementUpdate {
  std::string query;
};

struct StatementQueryTicket {
  std::string statement_handle;
};

struct PreparedStatementQuery {
  std::string prepared_statement_handle;
};

struct PreparedStatementUpdate {
  std::string prepared_statement_handle;
};

struct GetSqlInfo {
  // TODO: To be implemented.
};

struct GetSchemas {
  bool has_catalog;
  std::string catalog;
  bool has_schema_filter_pattern;
  std::string schema_filter_pattern;
};

struct GetTables {
  bool has_catalog;
  std::string catalog;
  bool has_schema_filter_pattern;
  std::string schema_filter_pattern;
  bool has_table_name_filter_pattern;
  std::string table_name_filter_pattern;
  std::vector<std::string> table_types;
  bool include_schema;
};

struct GetPrimaryKeys {
  bool has_catalog;
  std::string catalog;
  bool has_schema;
  std::string schema;
  std::string table;
};

struct GetExportedKeys {
  bool has_catalog;
  std::string catalog;
  bool has_schema;
  std::string schema;
  std::string table;
};

struct GetImportedKeys {
  bool has_catalog;
  std::string catalog;
  bool has_schema;
  std::string schema;
  std::string table;
};

struct GetCrossReference {
  bool has_pk_catalog;
  std::string pk_catalog;
  bool has_pk_schema;
  std::string pk_schema;
  std::string pk_table;
  bool has_fk_catalog;
  std::string fk_catalog;
  bool has_fk_schema;
  std::string fk_schema;
  std::string fk_table;
};

struct ActionCreatePreparedStatementRequest {
  std::string query;
};

struct ActionClosePreparedStatementRequest {
  std::string prepared_statement_handle;
};

struct ActionCreatePreparedStatementResult {
  std::shared_ptr<Schema> dataset_schema;
  std::shared_ptr<Schema> parameter_schema;
  std::string prepared_statement_handle;
};

class ARROW_EXPORT FlightSqlServerBase : public FlightServerBase {
 public:
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override;

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) override;

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override;

  const ActionType kCreatePreparedStatementActionType =
      ActionType{"CreatePreparedStatement",
                 "Creates a reusable prepared statement resource on the server.\n"
                 "Request Message: ActionCreatePreparedStatementRequest\n"
                 "Response Message: ActionCreatePreparedStatementResult"};
  const ActionType kClosePreparedStatementActionType =
      ActionType{"ClosePreparedStatement",
                 "Closes a reusable prepared statement resource on the server.\n"
                 "Request Message: ActionClosePreparedStatementRequest\n"
                 "Response Message: N/A"};

  Status ListActions(const ServerCallContext& context,
                     std::vector<ActionType>* actions) override;

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override;

  /// \brief Gets a FlightInfo for executing a SQL query.
  /// \param[in] command      The StatementQuery object containing the SQL statement.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoStatement(const StatementQuery& command,
                                        const ServerCallContext& context,
                                        const FlightDescriptor& descriptor,
                                        std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the query results.
  /// \param[in] command      The StatementQueryTicket containing the statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] result      The FlightDataStream containing the results.
  /// \return                 Status.
  virtual Status DoGetStatement(const StatementQueryTicket& command,
                                const ServerCallContext& context,
                                std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for executing an already created prepared statement.
  /// \param[in] command      The PreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPreparedStatement(const PreparedStatementQuery& command,
                                                const ServerCallContext& context,
                                                const FlightDescriptor& descriptor,
                                                std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the prepared statement query results.
  /// \param[in] command      The PreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] result      The FlightDataStream containing the results.
  /// \return                 Status.
  virtual Status DoGetPreparedStatement(const PreparedStatementQuery& command,
                                        const ServerCallContext& context,
                                        std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for listing catalogs.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoCatalogs(const ServerCallContext& context,
                                       const FlightDescriptor& descriptor,
                                       std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of catalogs.
  /// \param[in] context  Per-call context.
  /// \param[out] result  An interface for sending data back to the client.
  /// \return             Status.
  virtual Status DoGetCatalogs(const ServerCallContext& context,
                               std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for retrieving other information (See SqlInfo).
  /// \param[in] command      The GetSqlInfo object containing the list of SqlInfo
  ///                         to be returned.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSqlInfo(const GetSqlInfo& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of SqlInfo results.
  /// \param[in] command    The GetSqlInfo object containing the list of SqlInfo
  ///                       to be returned.
  /// \param[in] context    Per-call context.
  /// \param[out] result    The FlightDataStream containing the results.
  /// \return               Status.
  virtual Status DoGetSqlInfo(const GetSqlInfo& command, const ServerCallContext& context,
                              std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for listing schemas.
  /// \param[in] command      The GetSchemas object which may contain filters for
  ///                         catalog and schema name.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSchemas(const GetSchemas& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of schemas.
  /// \param[in] command   The GetSchemas object which may contain filters for
  ///                      catalog and schema name.
  /// \param[in] context   Per-call context.
  /// \param[out] result   The FlightDataStream containing the results.
  /// \return              Status.
  virtual Status DoGetSchemas(const GetSchemas& command, const ServerCallContext& context,
                              std::unique_ptr<FlightDataStream>* result);

  ///\brief Gets a FlightInfo for listing tables.
  /// \param[in] command      The GetTables object which may contain filters for
  ///                         catalog, schema and table names.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoTables(const GetTables& command,
                                     const ServerCallContext& context,
                                     const FlightDescriptor& descriptor,
                                     std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of tables.
  /// \param[in] command   The GetTables object which may contain filters for
  ///                      catalog, schema and table names.
  /// \param[in] context   Per-call context.
  /// \param[out] result   The FlightDataStream containing the results.
  /// \return              Status.
  virtual Status DoGetTables(const GetTables& command, const ServerCallContext& context,
                             std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about the table types.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoTableTypes(const ServerCallContext& context,
                                         const FlightDescriptor& descriptor,
                                         std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the table types.
  /// \param[in] context   Per-call context.
  /// \param[out] result   The FlightDataStream containing the results.
  /// \return               Status.
  virtual Status DoGetTableTypes(const ServerCallContext& context,
                                 std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about primary and foreign keys.
  /// \param[in] command      The GetPrimaryKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPrimaryKeys(const GetPrimaryKeys& command,
                                          const ServerCallContext& context,
                                          const FlightDescriptor& descriptor,
                                          std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the primary and
  /// foreign
  ///        keys.
  /// \param[in] command  The GetPrimaryKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetPrimaryKeys(const GetPrimaryKeys& command,
                                  const ServerCallContext& context,
                                  std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] command      The GetExportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoExportedKeys(const GetExportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the foreign and
  /// primary
  ///        keys.
  /// \param[in] command  The GetExportedKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetExportedKeys(const GetExportedKeys& command,
                                   const ServerCallContext& context,
                                   std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] command      The GetImportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoImportedKeys(const GetImportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the foreign and
  ///        primary keys.
  /// \param[in] command  The GetImportedKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetImportedKeys(const GetImportedKeys& command,
                                   const ServerCallContext& context,
                                   std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] command      The GetCrossReference object with necessary
  /// information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoCrossReference(const GetCrossReference& command,
                                             const ServerCallContext& context,
                                             const FlightDescriptor& descriptor,
                                             std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the foreign and
  ///        primary keys.
  /// \param[in] command  The GetCrossReference object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetCrossReference(const GetCrossReference& command,
                                     const ServerCallContext& context,
                                     std::unique_ptr<FlightDataStream>* result);

  /// \brief Executes an update SQL statement.
  /// \param[in] command  The StatementUpdate object containing the SQL statement.
  /// \param[in] context  The call context.
  /// \param[in] reader   a sequence of uploaded record batches.
  /// \return             The changed record count.
  virtual arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const StatementUpdate& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader);

  /// \brief Create a prepared statement from given SQL statement.
  /// \param[in] request  The ActionCreatePreparedStatementRequest object containing the
  ///                     SQL statement.
  /// \param[in] context  The call context.
  /// \return             A ActionCreatePreparedStatementResult containing the dataset
  ///                     and parameter schemas and a handle for created statement.
  virtual arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ActionCreatePreparedStatementRequest& request,
      const ServerCallContext& context);

  /// \brief Closes a prepared statement.
  /// \param[in] request  The ActionClosePreparedStatementRequest object containing the
  ///                     prepared statement handle.
  /// \param[in] context  The call context.
  /// \param[out] result  Empty ResultStream.
  virtual Status ClosePreparedStatement(
      const ActionClosePreparedStatementRequest& request,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* result);

  /// \brief Binds parameters to given prepared statement.
  /// \param[in] command  The PreparedStatementQuery object containing the
  ///                     prepared statement handle.
  /// \param[in] context  The call context.
  /// \param[in] reader   A sequence of uploaded record batches.
  /// \param[in] writer   Send metadata back to the client.
  virtual Status DoPutPreparedStatementQuery(
      const PreparedStatementQuery& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader,
      std::unique_ptr<FlightMetadataWriter>& writer);

  /// \brief Executes an update SQL prepared statement.
  /// \param[in] command  The PreparedStatementUpdate object containing the
  ///                     prepared statement handle.
  /// \param[in] context  The call context.
  /// \param[in] reader   a sequence of uploaded record batches.
  /// \return             The changed record count.
  virtual arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const PreparedStatementUpdate& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader);

 protected:
  static std::string CreateStatementQueryTicket(const std::string& statement_handle);
};

/// \brief Auxiliary class containing all Schemas used on Flight SQL.
class ARROW_EXPORT SqlSchema {
 public:
  /// \brief Gets the Schema used on GetCatalogs response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetCatalogsSchema();

  /// \brief Gets the Schema used on GetSchemas response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetSchemasSchema();

  /// \brief Gets the Schema used on GetTables response when included schema
  /// flags is set to false.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTablesSchema();

  /// \brief Gets the Schema used on GetTables response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTablesSchemaWithIncludedSchema();

  /// \brief Gets the Schema used on GetTableTypes response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTableTypesSchema();

  /// \brief Gets the Schema used on GetPrimaryKeys response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetPrimaryKeysSchema();

  /// \brief Gets the Schema used on GetImportedKeys response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetExportedKeysSchema();

  /// \brief Gets the Schema used on GetImportedKeys response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetImportedKeysSchema();

  /// \brief Gets the Schema used on GetCrossReference response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetCrossReferenceSchema();
};
}  // namespace sql
}  // namespace flight
}  // namespace arrow
