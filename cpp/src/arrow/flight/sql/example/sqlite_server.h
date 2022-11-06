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

#include <sqlite3.h>

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/flight/sql/example/sqlite_statement.h"
#include "arrow/flight/sql/example/sqlite_statement_batch_reader.h"
#include "arrow/flight/sql/server.h"
#include "arrow/result.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

/// \brief Convert a column type to a ArrowType.
/// \param sqlite_type the sqlite type.
/// \return            The equivalent ArrowType.
arrow::Result<std::shared_ptr<DataType>> GetArrowType(const char* sqlite_type);

/// \brief Convert a column type name to SQLite type.
/// \param type_name the type name.
/// \return          The equivalent SQLite type.
int32_t GetSqlTypeFromTypeName(const char* type_name);

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
  ~SQLiteFlightSqlServer() override;

  static arrow::Result<std::shared_ptr<SQLiteFlightSqlServer>> Create();

  /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
  ///        SQLite database.
  Status ExecuteSql(const std::string& sql);

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
      const ServerCallContext& context, const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
      const ServerCallContext& context) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext& context, const GetDbSchemas& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const GetDbSchemas& command) override;
  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const ServerCallContext& context, const StatementUpdate& update) override;
  arrow::Result<ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ServerCallContext& context,
      const ActionCreatePreparedStatementRequest& request) override;
  Status ClosePreparedStatement(
      const ServerCallContext& context,
      const ActionClosePreparedStatementRequest& request) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context, const PreparedStatementQuery& command) override;
  Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                     const PreparedStatementQuery& command,
                                     FlightMessageReader* reader,
                                     FlightMetadataWriter* writer) override;
  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const PreparedStatementUpdate& command,
      FlightMessageReader* reader) override;

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const GetTables& command,
      const FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const GetTables& command) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoXdbcTypeInfo(
      const ServerCallContext& context,
      const arrow::flight::sql::GetXdbcTypeInfo& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetXdbcTypeInfo(
      const ServerCallContext& context,
      const arrow::flight::sql::GetXdbcTypeInfo& command) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
      const ServerCallContext& context, const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
      const ServerCallContext& context) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext& context, const GetImportedKeys& command) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext& context, const GetExportedKeys& command) override;
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context, const GetCrossReference& command,
      const FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context, const GetCrossReference& command) override;

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command,
      const FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const GetPrimaryKeys& command) override;

  arrow::Result<ActionBeginTransactionResult> BeginTransaction(
      const ServerCallContext& context,
      const ActionBeginTransactionRequest& request) override;
  Status EndTransaction(const ServerCallContext& context,
                        const ActionEndTransactionRequest& request) override;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;

  explicit SQLiteFlightSqlServer(std::shared_ptr<Impl> impl);
};

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
