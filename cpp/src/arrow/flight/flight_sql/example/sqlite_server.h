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

#include <arrow/api.h>
#include <arrow/flight/flight_sql/example/sqlite_statement.h>
#include <arrow/flight/flight_sql/example/sqlite_statement_batch_reader.h>
#include <arrow/flight/flight_sql/server.h>
#include <sqlite3.h>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <map>
#include <memory>
#include <string>

namespace arrow {
namespace flight {
namespace sql {
namespace example {

/// \brief Convert a column type to a ArrowType.
/// \param sqlite_type the sqlite type.
/// \return            The equivalent ArrowType.
std::shared_ptr<DataType> GetArrowType(const char* sqlite_type);

/// \brief  Get the DataType used when parameter type is not known.
/// \return DataType used when parameter type is not known.
inline std::shared_ptr<DataType> GetUnknownColumnDataType() {
  return dense_union({
      field("string", utf8()),
      field("bytes", binary()),
      field("bigint", int64()),
      field("double", float64()),
  });
}

/// \brief Example implementation of FlightSqlServerBase backed by an in-memory SQLite3
///        database.
class SQLiteFlightSqlServer : public FlightSqlServerBase {
 public:
  SQLiteFlightSqlServer();

  ~SQLiteFlightSqlServer() override;

  /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
  ///        SQLite database.
  void ExecuteSql(const std::string& sql);

  Status GetFlightInfoStatement(const StatementQuery& command,
                                const ServerCallContext& context,
                                const FlightDescriptor& descriptor,
                                std::unique_ptr<FlightInfo>* info) override;

  Status DoGetStatement(const StatementQueryTicket& command,
                        const ServerCallContext& context,
                        std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoCatalogs(const ServerCallContext& context,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info) override;
  Status DoGetCatalogs(const ServerCallContext& context,
                       std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoSchemas(const GetSchemas& command, const ServerCallContext& context,
                              const FlightDescriptor& descriptor,
                              std::unique_ptr<FlightInfo>* info) override;
  Status DoGetSchemas(const GetSchemas& command, const ServerCallContext& context,
                      std::unique_ptr<FlightDataStream>* result) override;
  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const StatementUpdate& update, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader) override;
  arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ActionCreatePreparedStatementRequest& request,
      const ServerCallContext& context) override;
  Status ClosePreparedStatement(const ActionClosePreparedStatementRequest& request,
                                const ServerCallContext& context,
                                std::unique_ptr<ResultStream>* result) override;
  Status GetFlightInfoPreparedStatement(const PreparedStatementQuery& command,
                                        const ServerCallContext& context,
                                        const FlightDescriptor& descriptor,
                                        std::unique_ptr<FlightInfo>* info) override;
  Status DoGetPreparedStatement(const PreparedStatementQuery& command,
                                const ServerCallContext& context,
                                std::unique_ptr<FlightDataStream>* result) override;
  Status DoPutPreparedStatementQuery(
      const PreparedStatementQuery& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader,
      std::unique_ptr<FlightMetadataWriter>& writer) override;
  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const PreparedStatementUpdate& command, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader) override;

  Status GetFlightInfoTables(const GetTables& command, const ServerCallContext& context,
                             const FlightDescriptor& descriptor,
                             std::unique_ptr<FlightInfo>* info) override;

  Status DoGetTables(const GetTables& command, const ServerCallContext& context,
                     std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoTableTypes(const ServerCallContext& context,
                                 const FlightDescriptor& descriptor,
                                 std::unique_ptr<FlightInfo>* info) override;
  Status DoGetTableTypes(const ServerCallContext& context,
                         std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoImportedKeys(const GetImportedKeys& command,
                                   const ServerCallContext& context,
                                   const FlightDescriptor& descriptor,
                                   std::unique_ptr<FlightInfo>* info) override;
  Status DoGetImportedKeys(const GetImportedKeys& command,
                           const ServerCallContext& context,
                           std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoExportedKeys(const GetExportedKeys& command,
                                   const ServerCallContext& context,
                                   const FlightDescriptor& descriptor,
                                   std::unique_ptr<FlightInfo>* info) override;
  Status DoGetExportedKeys(const GetExportedKeys& command,
                           const ServerCallContext& context,
                           std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoCrossReference(const GetCrossReference& command,
                                     const ServerCallContext& context,
                                     const FlightDescriptor& descriptor,
                                     std::unique_ptr<FlightInfo>* info) override;
  Status DoGetCrossReference(const GetCrossReference& command,
                             const ServerCallContext& context,
                             std::unique_ptr<FlightDataStream>* result) override;

  Status GetFlightInfoPrimaryKeys(const GetPrimaryKeys& command,
                                  const ServerCallContext& context,
                                  const FlightDescriptor& descriptor,
                                  std::unique_ptr<FlightInfo>* info) override;

  Status DoGetPrimaryKeys(const GetPrimaryKeys& command, const ServerCallContext& context,
                          std::unique_ptr<FlightDataStream>* result) override;

 private:
  sqlite3* db_;
  boost::uuids::random_generator uuid_generator_;
  std::map<boost::uuids::uuid, std::shared_ptr<SqliteStatement>> prepared_statements_;

  Status GetStatementByHandle(const std::string& prepared_statement_handle,
                              std::shared_ptr<SqliteStatement>* result);
};

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
