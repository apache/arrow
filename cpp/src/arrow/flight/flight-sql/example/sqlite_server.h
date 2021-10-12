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

#include "arrow/api.h"
#include "arrow/flight/flight-sql/example/sqlite_statement.h"
#include "arrow/flight/flight-sql/example/sqlite_statement_batch_reader.h"
#include "arrow/flight/flight-sql/sql_server.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

/// \brief Example implementation of FlightSqlServerBase backed by an in-memory SQLite3
///        database.
class SQLiteFlightSqlServer : public FlightSqlServerBase {
 public:
  SQLiteFlightSqlServer();

  ~SQLiteFlightSqlServer() override;

  /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
  ///        SQLite database.
  void ExecuteSql(const std::string& sql);

  Status GetFlightInfoStatement(const pb::sql::CommandStatementQuery& command,
                                const ServerCallContext& context,
                                const FlightDescriptor& descriptor,
                                std::unique_ptr<FlightInfo>* info) override;

  Status DoGetStatement(const pb::sql::TicketStatementQuery& command,
                        const ServerCallContext& context,
                        std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoCatalogs(const ServerCallContext& context,
                               const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info) override;
  Status DoGetCatalogs(const ServerCallContext& context,
                       std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoSchemas(const pb::sql::CommandGetSchemas& command,
                              const ServerCallContext& context,
                              const FlightDescriptor& descriptor,
                              std::unique_ptr<FlightInfo>* info) override;
  Status DoGetSchemas(const pb::sql::CommandGetSchemas& command,
                      const ServerCallContext& context,
                      std::unique_ptr<FlightDataStream>* result) override;
  Status DoPutCommandStatementUpdate(
      const pb::sql::CommandStatementUpdate& update, const ServerCallContext& context,
      std::unique_ptr<FlightMessageReader>& reader,
      std::unique_ptr<FlightMetadataWriter>& writer) override;

  Status GetFlightInfoTables(const pb::sql::CommandGetTables& command,
                             const ServerCallContext& context,
                             const FlightDescriptor& descriptor,
                             std::unique_ptr<FlightInfo>* info) override;

  Status DoGetTables(const pb::sql::CommandGetTables& command,
                     const ServerCallContext& context,
                     std::unique_ptr<FlightDataStream>* result) override;
  Status GetFlightInfoTableTypes(const ServerCallContext &context,
                                 const FlightDescriptor &descriptor,
                                 std::unique_ptr<FlightInfo> *info) override;
  Status DoGetTableTypes(const ServerCallContext &context,
                         std::unique_ptr<FlightDataStream> *result) override;

  Status GetFlightInfoPrimaryKeys(const pb::sql::CommandGetPrimaryKeys &command,
                                  const ServerCallContext &context,
                                  const FlightDescriptor &descriptor,
                                  std::unique_ptr<FlightInfo> *info) override;

  Status DoGetPrimaryKeys(const pb::sql::CommandGetPrimaryKeys &command,
                          const ServerCallContext &context,
                          std::unique_ptr<FlightDataStream> *result) override;

private:
  sqlite3* db_;
};

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
