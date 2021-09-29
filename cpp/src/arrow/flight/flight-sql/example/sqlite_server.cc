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

#include "arrow/flight/flight-sql/example/sqlite_server.h"

#include <sqlite3.h>

#include "arrow/api.h"
#include "arrow/flight/flight-sql/example/sqlite_statement.h"
#include "arrow/flight/flight-sql/example/sqlite_statement_batch_reader.h"
#include "arrow/flight/flight-sql/sql_server.h"

namespace arrow {
namespace flight {
namespace sql {
namespace example {

SQLiteFlightSqlServer::SQLiteFlightSqlServer() {
  db_ = NULLPTR;
  if (sqlite3_open(":memory:", &db_)) {
    sqlite3_close(db_);
    throw std::runtime_error(std::string("Can't open database: ") + sqlite3_errmsg(db_));
  }

  ExecuteSql(R"(
CREATE TABLE foreignTable (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  foreignName varchar(100),
  value int);

CREATE TABLE intTable (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  keyName varchar(100),
  value int,
  foreignId int references foreignTable(id));

INSERT INTO foreignTable (foreignName, value) VALUES ('keyOne', 1);
INSERT INTO foreignTable (foreignName, value) VALUES ('keyTwo', 0);
INSERT INTO foreignTable (foreignName, value) VALUES ('keyThree', -1);
INSERT INTO intTable (keyName, value, foreignId) VALUES ('one', 1, 1);
INSERT INTO intTable (keyName, value, foreignId) VALUES ('zero', 0, 1);
INSERT INTO intTable (keyName, value, foreignId) VALUES ('negative one', -1, 1);
)");
}

SQLiteFlightSqlServer::~SQLiteFlightSqlServer() { sqlite3_close(db_); }

void SQLiteFlightSqlServer::ExecuteSql(const std::string& sql) {
  char* zErrMsg = NULLPTR;
  int rc = sqlite3_exec(db_, sql.c_str(), NULLPTR, NULLPTR, &zErrMsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
    sqlite3_free(zErrMsg);
  }
}

Status GetFlightInfoForCommand(const FlightDescriptor& descriptor,
                               std::unique_ptr<FlightInfo>* info,
                               google::protobuf::Message& command,
                               const std::shared_ptr<Schema>& schema) {
  google::protobuf::Any ticketParsed;
  ticketParsed.PackFrom(command);

  std::vector<FlightEndpoint> endpoints{
      FlightEndpoint{{ticketParsed.SerializeAsString()}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoStatement(
    const pb::sql::CommandStatementQuery& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  const std::string& query = command.query();

  std::shared_ptr<SqliteStatement> statement;
  ARROW_RETURN_NOT_OK(SqliteStatement::Create(db_, query, &statement));

  std::shared_ptr<Schema> schema;
  ARROW_RETURN_NOT_OK(statement->GetSchema(&schema));

  pb::sql::TicketStatementQuery ticket_statement_query;
  ticket_statement_query.set_statement_handle(query);

  google::protobuf::Any ticket;
  ticket.PackFrom(ticket_statement_query);

  const std::string& ticket_string = ticket.SerializeAsString();
  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{ticket_string}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

Status SQLiteFlightSqlServer::DoGetStatement(const pb::sql::TicketStatementQuery& command,
                                             const ServerCallContext& context,
                                             std::unique_ptr<FlightDataStream>* result) {
  const std::string& sql = command.statement_handle();

  std::shared_ptr<SqliteStatement> statement;
  ARROW_RETURN_NOT_OK(SqliteStatement::Create(db_, sql, &statement));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_RETURN_NOT_OK(SqliteStatementBatchReader::Create(statement, &reader));

  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));

  return Status::OK();
}

Status SQLiteFlightSqlServer::GetFlightInfoCatalogs(const ServerCallContext& context,
                                                    const FlightDescriptor& descriptor,
                                                    std::unique_ptr<FlightInfo>* info) {
  pb::sql::CommandGetCatalogs command;
  return GetFlightInfoForCommand(descriptor, info, command,
                                 SqlSchema::GetCatalogsSchema());
}

Status SQLiteFlightSqlServer::DoGetCatalogs(const ServerCallContext& context,
                                            std::unique_ptr<FlightDataStream>* result) {
  // For the purpose of demonstrating a full Flight SQL server implementation, this
  // returns a hard-coded catalog named "sqlite_master", although SQLite actually doesn't
  // have support for catalogs.

  const std::shared_ptr<Schema>& schema = SqlSchema::GetCatalogsSchema();
  StringBuilder catalog_name_builder;
  ARROW_RETURN_NOT_OK(catalog_name_builder.Append("sqlite_master"));

  std::shared_ptr<Array> catalog_name;
  ARROW_RETURN_NOT_OK(catalog_name_builder.Finish(&catalog_name));

  const std::shared_ptr<RecordBatch>& batch =
      RecordBatch::Make(schema, 1, {catalog_name});

  ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({batch}));
  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  return Status::OK();
}

}  // namespace example
}  // namespace sql
}  // namespace flight
}  // namespace arrow
