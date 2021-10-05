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

#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <google/protobuf/any.pb.h>

#include "../server.h"

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {

class FlightSqlServerBase : public FlightServerBase {
 public:
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override;

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* stream) override;

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override;

  const ActionType FLIGHT_SQL_CREATE_PREPARED_STATEMENT =
      ActionType{.type = "CreatePreparedStatement",
                 .description =
                     "Creates a reusable prepared statement resource on the server.\n"
                     "Request Message: ActionCreatePreparedStatementRequest\n"
                     "Response Message: ActionCreatePreparedStatementResult"};
  const ActionType FLIGHT_SQL_CLOSE_PREPARED_STATEMENT = ActionType {
    .type = "ClosePreparedStatement",
    .description =
        "Closes a reusable prepared statement resource on the server.\n"
        "Request Message: ActionClosePreparedStatementRequest\n"
        "Response Message: N/A"};

  Status ListActions(const ServerCallContext &context,
                     std::vector<ActionType> *actions) override;

  Status DoAction(const ServerCallContext &context,
                  const Action &action,
                  std::unique_ptr<ResultStream> *result) override;

  /// \brief Gets a FlightInfo for executing a SQL query.
  /// \param[in] command      The CommandStatementQuery object containing the SQL
  ///                         statement.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoStatement(const pb::sql::CommandStatementQuery& command,
                                        const ServerCallContext& context,
                                        const FlightDescriptor& descriptor,
                                        std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the query results.
  /// \param[in] command      The TicketStatementQuery containing the statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] result      The FlightDataStream containing the results.
  /// \return                 Status.
  virtual Status DoGetStatement(const pb::sql::TicketStatementQuery& command,
                                const ServerCallContext& context,
                                std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for executing an already created prepared statement.
  /// \param[in] command      The CommandPreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPreparedStatement(
      const pb::sql::CommandPreparedStatementQuery& command,
      const ServerCallContext& context, const FlightDescriptor& descriptor,
      std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the prepared statement query results.
  /// \param[in] command      The CommandPreparedStatementQuery object containing the
  ///                         prepared statement handle.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] result      The FlightDataStream containing the results.
  /// \return                 Status.
  virtual Status DoGetPreparedStatement(
      const pb::sql::CommandPreparedStatementQuery& command,
      const ServerCallContext& context, std::unique_ptr<FlightDataStream>* result);

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
  /// \param[in] command      The CommandGetSqlInfo object containing the list of SqlInfo
  ///                         to be returned.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSqlInfo(const pb::sql::CommandGetSqlInfo& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of SqlInfo results.
  /// \param[in] command    The CommandGetSqlInfo object containing the list of SqlInfo
  ///                       to be returned.
  /// \param[in] context    Per-call context.
  /// \param[out] result    The FlightDataStream containing the results.
  /// \return               Status.
  virtual Status DoGetSqlInfo(const pb::sql::CommandGetSqlInfo& command,
                              const ServerCallContext& context,
                              std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo for listing schemas.
  /// \param[in] command      The CommandGetSchemas object which may contain filters for
  ///                         catalog and schema name.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoSchemas(const pb::sql::CommandGetSchemas& command,
                                      const ServerCallContext& context,
                                      const FlightDescriptor& descriptor,
                                      std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of schemas.
  /// \param[in] command   The CommandGetSchemas object which may contain filters for
  ///                      catalog and schema name.
  /// \param[in] context   Per-call context.
  /// \param[out] result   The FlightDataStream containing the results.
  /// \return              Status.
  virtual Status DoGetSchemas(const pb::sql::CommandGetSchemas& command,
                              const ServerCallContext& context,
                              std::unique_ptr<FlightDataStream>* result);

  ///\brief Gets a FlightInfo for listing tables.
  /// \param[in] command      The CommandGetTables object which may contain filters for
  ///                         catalog, schema and table names.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoTables(const pb::sql::CommandGetTables& command,
                                     const ServerCallContext& context,
                                     const FlightDescriptor& descriptor,
                                     std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the list of tables.
  /// \param[in] command   The CommandGetTables object which may contain filters for
  ///                      catalog, schema and table names.
  /// \param[in] context   Per-call context.
  /// \param[out] result   The FlightDataStream containing the results.
  /// \return              Status.
  virtual Status DoGetTables(const pb::sql::CommandGetTables& command,
                             const ServerCallContext& context,
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
  /// \param[in] command      The CommandGetPrimaryKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoPrimaryKeys(const pb::sql::CommandGetPrimaryKeys& command,
                                          const ServerCallContext& context,
                                          const FlightDescriptor& descriptor,
                                          std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the primary and
  /// foreign
  ///        keys.
  /// \param[in] command  The CommandGetPrimaryKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetPrimaryKeys(const pb::sql::CommandGetPrimaryKeys& command,
                                  const ServerCallContext& context,
                                  std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] command      The CommandGetExportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoExportedKeys(const pb::sql::CommandGetExportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the foreign and
  /// primary
  ///        keys.
  /// \param[in] command  The CommandGetExportedKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetExportedKeys(const pb::sql::CommandGetExportedKeys& command,
                                   const ServerCallContext& context,
                                   std::unique_ptr<FlightDataStream>* result);

  /// \brief Gets a FlightInfo to extract information about foreign and primary keys.
  /// \param[in] command      The CommandGetImportedKeys object with necessary information
  ///                         to execute the request.
  /// \param[in] context      Per-call context.
  /// \param[in] descriptor   The descriptor identifying the data stream.
  /// \param[out] info        The FlightInfo describing where to access the
  ///                         dataset.
  /// \return                 Status.
  virtual Status GetFlightInfoImportedKeys(const pb::sql::CommandGetImportedKeys& command,
                                           const ServerCallContext& context,
                                           const FlightDescriptor& descriptor,
                                           std::unique_ptr<FlightInfo>* info);

  /// \brief Gets a FlightDataStream containing the data related to the foreign and
  ///        primary keys.
  /// \param[in] command  The CommandGetImportedKeys object with necessary information
  ///                     to execute the request.
  /// \param[in] context  Per-call context.
  /// \param[out] result  The FlightDataStream containing the results.
  /// \return             Status.
  virtual Status DoGetImportedKeys(const pb::sql::CommandGetImportedKeys& command,
                                   const ServerCallContext& context,
                                   std::unique_ptr<FlightDataStream>* result);

  /// \brief Executes an update SQL statement.
  /// \param[in] command  The CommandStatementUpdate object containing the SQL statement.
  /// \param[in] context  The call context.
  /// \param[in] reader   a sequence of uploaded record batches.
  /// \param[in] writer   send metadata back to the client.
  /// \return             Status.
  virtual Status DoPutCommandStatementUpdate(
      const pb::sql::CommandStatementUpdate& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader,
      std::unique_ptr<FlightMetadataWriter>& writer);

  virtual Status CreatePreparedStatement(
      const pb::sql::ActionCreatePreparedStatementRequest& request,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* p_ptr);

  virtual Status ClosePreparedStatement(
      const pb::sql::ActionClosePreparedStatementRequest& request,
      const ServerCallContext& context, std::unique_ptr<ResultStream>* p_ptr);

};

/// \brief Auxiliary class containing all Schemas used on Flight SQL.
class SqlSchema {
 public:
  /// \brief Gets the Schema used on CommandGetCatalogs response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetCatalogsSchema();

  /// \brief Gets the Schema used on CommandGetSchemas response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetSchemasSchema();

  /// \brief Gets the Schema used on CommandGetTables response when included schema
  /// flags is set to false.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTablesSchema();

  /// \brief Gets the Schema used on CommandGetTables response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTablesSchemaWithIncludedSchema();

  /// \brief Gets the Schema used on CommandGetTableTypes response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetTableTypesSchema();

  /// \brief Gets the Schema used on CommandGetPrimaryKeys response when included schema
  /// flags is set to true.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetPrimaryKeysSchema();

  /// \brief Gets the Schema used on CommandGetImportedKeys and CommandGetExportedKeys
  /// response.
  /// \return The default schema template.
  static std::shared_ptr<Schema> GetImportedAndExportedKeysSchema();
};
}  // namespace sql
}  // namespace flight
}  // namespace arrow
