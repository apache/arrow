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

#include <memory>
#include <string>

#include "arrow/flight/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "arrow/status.h"

namespace arrow {
namespace flight {
namespace sql {

class PreparedStatement;

/// \brief Flight client with Flight SQL semantics.
class ARROW_EXPORT FlightSqlClient {
  friend class PreparedStatement;

 private:
  std::shared_ptr<FlightClient> impl_;

 public:
  explicit FlightSqlClient(std::shared_ptr<FlightClient> client);

  virtual ~FlightSqlClient() = default;

  /// \brief Execute a query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> Execute(const FlightCallOptions& options,
                                                     const std::string& query);

  /// \brief Execute an update query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \return The quantity of rows affected by the operation.
  arrow::Result<int64_t> ExecuteUpdate(const FlightCallOptions& options,
                                       const std::string& query);

  /// \brief Request a list of catalogs.
  /// \param[in] options      RPC-layer hints for this call.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetCatalogs(
      const FlightCallOptions& options);

  /// \brief Request a list of database schemas.
  /// \param[in] options                   RPC-layer hints for this call.
  /// \param[in] catalog                   The catalog.
  /// \param[in] db_schema_filter_pattern  The schema filter pattern.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetDbSchemas(
      const FlightCallOptions& options, const std::string* catalog,
      const std::string* db_schema_filter_pattern);

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

  /// \brief Request the primary keys for a table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetPrimaryKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

  /// \brief Retrieves a description about the foreign key columns that reference the
  /// primary key columns of the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetExportedKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

  /// \brief Retrieves the foreign key columns for the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[in] table_ref        The table reference.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetImportedKeys(
      const FlightCallOptions& options, const TableRef& table_ref);

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

  /// \brief Request a list of table types.
  /// \param[in] options          RPC-layer hints for this call.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetTableTypes(
      const FlightCallOptions& options);

  /// \brief Request a list of SQL information.
  /// \param[in] options RPC-layer hints for this call.
  /// \param[in] sql_info the SQL info required.
  /// \return The FlightInfo describing where to access the dataset.
  arrow::Result<std::unique_ptr<FlightInfo>> GetSqlInfo(const FlightCallOptions& options,
                                                        const std::vector<int>& sql_info);

  /// \brief Create a prepared statement object.
  /// \param[in] options              RPC-layer hints for this call.
  /// \param[in] query                The query that will be executed.
  /// \return The created prepared statement.
  arrow::Result<std::shared_ptr<PreparedStatement>> Prepare(
      const FlightCallOptions& options, const std::string& query);

  /// \brief Retrieve the FlightInfo.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[in] descriptor   The flight descriptor.
  /// \return The flight info with the metadata.
  // NOTE: This is public because it is been used by the anonymous
  // function GetFlightInfoForCommand.
  virtual arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfo(
      const FlightCallOptions& options, const FlightDescriptor& descriptor) {
    std::unique_ptr<FlightInfo> info;
    ARROW_RETURN_NOT_OK(impl_->GetFlightInfo(options, descriptor, &info));

    return info;
  }

 protected:
  virtual Status DoPut(const FlightCallOptions& options,
                       const FlightDescriptor& descriptor,
                       const std::shared_ptr<Schema>& schema,
                       std::unique_ptr<FlightStreamWriter>* stream,
                       std::unique_ptr<FlightMetadataReader>* reader) {
    return impl_->DoPut(options, descriptor, schema, stream, reader);
  }

  virtual Status DoGet(const FlightCallOptions& options, const Ticket& ticket,
                       std::unique_ptr<FlightStreamReader>* stream) {
    return impl_->DoGet(options, ticket, stream);
  }

  virtual Status DoAction(const FlightCallOptions& options, const Action& action,
                          std::unique_ptr<ResultStream>* results) {
    return impl_->DoAction(options, action, results);
  }
};

/// \brief PreparedStatement class from flight sql.
class ARROW_EXPORT PreparedStatement {
  FlightSqlClient* client_;
  FlightCallOptions options_;
  std::string handle_;
  std::shared_ptr<Schema> dataset_schema_;
  std::shared_ptr<Schema> parameter_schema_;
  std::shared_ptr<RecordBatch> parameter_binding_;
  bool is_closed_;

 public:
  /// \brief Constructor for the PreparedStatement class.
  /// \param[in] client                Client object used to make the RPC requests.
  /// \param[in] handle                Handle for this prepared statement.
  /// \param[in] dataset_schema        Schema of the resulting dataset.
  /// \param[in] parameter_schema      Schema of the parameters (if any).
  /// \param[in] options               RPC-layer hints for this call.
  PreparedStatement(FlightSqlClient* client, std::string handle,
                    std::shared_ptr<Schema> dataset_schema,
                    std::shared_ptr<Schema> parameter_schema, FlightCallOptions options);

  /// \brief Default destructor for the PreparedStatement class.
  /// The destructor will call the Close method from the class in order,
  /// to send a request to close the PreparedStatement.
  /// NOTE: It is best to explicitly close the PreparedStatement, otherwise
  /// errors can't be caught.
  ~PreparedStatement();

  /// \brief Executes the prepared statement query on the server.
  /// \return A FlightInfo object representing the stream(s) to fetch.
  arrow::Result<std::unique_ptr<FlightInfo>> Execute();

  /// \brief Executes the prepared statement update query on the server.
  /// \return The number of rows affected.
  arrow::Result<int64_t> ExecuteUpdate();

  /// \brief Retrieve the parameter schema from the query.
  /// \return The parameter schema from the query.
  std::shared_ptr<Schema> parameter_schema() const;

  /// \brief Retrieve the ResultSet schema from the query.
  /// \return The ResultSet schema from the query.
  std::shared_ptr<Schema> dataset_schema() const;

  /// \brief Set a RecordBatch that contains the parameters that will be bind.
  /// \param parameter_binding   The parameters that will be bind.
  /// \return                     Status.
  Status SetParameters(std::shared_ptr<RecordBatch> parameter_binding);

  /// \brief Close the prepared statement, so that this PreparedStatement can not used
  /// anymore and server can free up any resources.
  /// \return Status.
  Status Close();

  /// \brief Check if the prepared statement is closed.
  /// \return The state of the prepared statement.
  bool IsClosed() const;
};

}  // namespace sql
}  // namespace flight
}  // namespace arrow
