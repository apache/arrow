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

#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/status.h>
#include <google/protobuf/message.h>

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

/// \brief Flight client with Flight SQL semantics.
template <class T = arrow::flight::FlightClient>
class FlightSqlClientT {
 public:
  explicit FlightSqlClientT(T* client);

  ~FlightSqlClientT();

  /// \brief Execute a query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \return Status.
  Status Execute(const FlightCallOptions& options,
                 std::unique_ptr<FlightInfo>* flight_info,
                 const std::string& query);

  /// \brief Execute an update query on the server.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[out] rows        The quantity of rows affected by the operation.
  /// \param[in] query        The query to be executed in the UTF-8 format.
  /// \return Status.
  Status ExecuteUpdate(const FlightCallOptions& options,
                       int64_t* rows,
                       const std::string& query);

  /// \brief Request a list of catalogs.
  /// \param[in] options      RPC-layer hints for this call.
  /// \param[out] flight_info The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetCatalogs(const FlightCallOptions& options,
                     std::unique_ptr<FlightInfo>* flight_info);

  /// \brief Request a list of schemas.
  /// \param[in] options                RPC-layer hints for this call.
  /// \param[out] flight_info           The FlightInfo describing where to access the
  ///                                   dataset.
  /// \param[in] catalog                The catalog.
  /// \param[in] schema_filter_pattern  The schema filter pattern.
  /// \return Status.
  Status GetSchemas(const FlightCallOptions& options,
                    std::unique_ptr<FlightInfo>* flight_info,
                    std::string* catalog,
                    std::string* schema_filter_pattern);

  /// \brief Given a flight ticket and schema, request to be sent the
  /// stream. Returns record batch stream reader
  /// \param[in] options Per-RPC options
  /// \param[in] ticket The flight ticket to use
  /// \param[out] stream the returned RecordBatchReader
  /// \return Status
  Status DoGet(const FlightCallOptions& options,
               const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream);

  /// \brief Request a list of tables.
  /// \param[in] options                  RPC-layer hints for this call.
  /// \param[out] flight_info             The FlightInfo describing where to access the
  ///                                     dataset.
  /// \param[in] catalog                  The catalog.
  /// \param[in] schema_filter_pattern    The schema filter pattern.
  /// \param[in] table_filter_pattern     The table filter pattern.
  /// \param[in] include_schema           True to include the schema upon return,
  ///                                     false to not include the schema.
  /// \param[in] table_types              The table types to include.
  /// \return Status.
  Status GetTables(const FlightCallOptions& options,
                   std::unique_ptr<FlightInfo>* flight_info,
                   const std::string* catalog,
                   const std::string* schema_filter_pattern,
                   const std::string* table_filter_pattern,
                   bool include_schema,
                   std::vector<std::string>& table_types);

  /// \brief Request the primary keys for a table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \param[in] catalog          The catalog.
  /// \param[in] schema           The schema.
  /// \param[in] table            The table.
  /// \return Status.
  Status GetPrimaryKeys(const FlightCallOptions& options,
                        std::unique_ptr<FlightInfo>* flight_info,
                        const std::string* catalog,
                        const std::string* schema,
                        const std::string& table);

  /// \brief Retrieves a description about the foreign key columns that reference the
  /// primary key columns of the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \param[in] catalog          The foreign key table catalog.
  /// \param[in] schema           The foreign key table schema.
  /// \param[in] table            The foreign key table. Cannot be null.
  /// \return Status.
  Status GetExportedKeys(const FlightCallOptions& options,
                         std::unique_ptr<FlightInfo>* flight_info,
                         const std::string* catalog,
                         const std::string* schema,
                         const std::string& table);

  /// \brief Retrieves the foreign key columns for the given table.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \param[in] catalog          The primary key table catalog.
  /// \param[in] schema           The primary key table schema.
  /// \param[in] table            The primary key table. Cannot be null.
  /// \return Status.
  Status GetImportedKeys(const FlightCallOptions& options,
                         std::unique_ptr<FlightInfo>* flight_info,
                         const std::string* catalog,
                         const std::string* schema,
                         const std::string& table);

  /// \brief Request a list of table types.
  /// \param[in] options          RPC-layer hints for this call.
  /// \param[out] flight_info     The FlightInfo describing where to access the dataset.
  /// \return Status.
  Status GetTableTypes(const FlightCallOptions& options,
                       std::unique_ptr<FlightInfo>* flight_info);


 private:
  T* client;
};

}  // namespace internal

typedef internal::FlightSqlClientT<FlightClient> FlightSqlClient;

}  // namespace sql
}  // namespace flight
}  // namespace arrow

#endif  // ARROW_FLIGHT_SQL_CLIENT_H

#include <arrow/flight/flight-sql/client_impl.h>
