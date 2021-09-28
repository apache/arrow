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

#include "arrow/flight/flight-sql/sql_server.h"

namespace arrow {
namespace flight {
namespace sql {
Status FlightSqlServerBase::GetFlightInfo(const ServerCallContext& context,
                                          const FlightDescriptor& request,
                                          std::unique_ptr<FlightInfo>* info) {
  google::protobuf::Any any;
  any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()));

  if (any.Is<pb::sql::CommandStatementQuery>()) {
    pb::sql::CommandStatementQuery command;
    any.UnpackTo(&command);
    return GetFlightInfoStatement(command, context, request, info);
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    pb::sql::CommandPreparedStatementQuery command;
    any.UnpackTo(&command);
    return GetFlightInfoPreparedStatement(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    pb::sql::CommandGetCatalogs command;
    any.UnpackTo(&command);
    return GetFlightInfoCatalogs(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSchemas>()) {
    pb::sql::CommandGetSchemas command;
    any.UnpackTo(&command);
    return GetFlightInfoSchemas(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    pb::sql::CommandGetTables command;
    any.UnpackTo(&command);
    return GetFlightInfoTables(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    pb::sql::CommandGetTableTypes command;
    any.UnpackTo(&command);
    return GetFlightInfoTableTypes(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    pb::sql::CommandGetSqlInfo command;
    any.UnpackTo(&command);
    return GetFlightInfoSqlInfo(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    pb::sql::CommandGetPrimaryKeys command;
    any.UnpackTo(&command);
    return GetFlightInfoPrimaryKeys(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    pb::sql::CommandGetExportedKeys command;
    any.UnpackTo(&command);
    return GetFlightInfoExportedKeys(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    pb::sql::CommandGetImportedKeys command;
    any.UnpackTo(&command);
    return GetFlightInfoImportedKeys(command, context, request, info);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                                  std::unique_ptr<FlightDataStream>* stream) {
  google::protobuf::Any anyCommand;

  anyCommand.ParseFromArray(request.ticket.data(),
                            static_cast<int>(request.ticket.size()));

  if (anyCommand.Is<pb::sql::TicketStatementQuery>()) {
    pb::sql::TicketStatementQuery command;
    anyCommand.UnpackTo(&command);
    return DoGetStatement(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandPreparedStatementQuery>()) {
    pb::sql::CommandPreparedStatementQuery command;
    anyCommand.UnpackTo(&command);
    return DoGetPreparedStatement(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetCatalogs>()) {
    pb::sql::CommandGetCatalogs command;
    anyCommand.UnpackTo(&command);
    return DoGetCatalogs(context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetSchemas>()) {
    pb::sql::CommandGetSchemas command;
    anyCommand.UnpackTo(&command);
    return DoGetSchemas(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetTables>()) {
    pb::sql::CommandGetTables command;
    anyCommand.UnpackTo(&command);
    return DoGetTables(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetTableTypes>()) {
    pb::sql::CommandGetTableTypes command;
    anyCommand.UnpackTo(&command);
    return DoGetTableTypes(context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetSqlInfo>()) {
    pb::sql::CommandGetSqlInfo command;
    anyCommand.UnpackTo(&command);
    return DoGetSqlInfo(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetPrimaryKeys>()) {
    pb::sql::CommandGetPrimaryKeys command;
    anyCommand.UnpackTo(&command);
    return DoGetPrimaryKeys(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetExportedKeys>()) {
    pb::sql::CommandGetExportedKeys command;
    anyCommand.UnpackTo(&command);
    return DoGetExportedKeys(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetImportedKeys>()) {
    pb::sql::CommandGetImportedKeys command;
    anyCommand.UnpackTo(&command);
    return DoGetImportedKeys(command, context, stream);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::GetFlightInfoCatalogs(const ServerCallContext& context,
                                                  const FlightDescriptor& descriptor,
                                                  std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoCatalogs not Implemented");
}

Status FlightSqlServerBase::DoGetImportedKeys(
    const pb::sql::CommandGetImportedKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetImportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoStatement(
    const pb::sql::CommandStatementQuery& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetImportedKeys not implemented");
}

Status FlightSqlServerBase::DoGetStatement(const pb::sql::TicketStatementQuery& command,
                                           const ServerCallContext& context,
                                           std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoStatement not implemented");
}

Status FlightSqlServerBase::GetFlightInfoPreparedStatement(
    const pb::sql::CommandPreparedStatementQuery& command,
    const ServerCallContext& context, const FlightDescriptor& descriptor,
    std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetStatement not implemented");
}

Status FlightSqlServerBase::DoGetPreparedStatement(
    const pb::sql::CommandPreparedStatementQuery& command,
    const ServerCallContext& context, std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoPreparedStatement not implemented");
}

Status FlightSqlServerBase::DoGetCatalogs(const ServerCallContext& context,
                                          std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetCatalogs not implemented");
}

Status FlightSqlServerBase::GetFlightInfoSqlInfo(
    const pb::sql::CommandGetSqlInfo& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetPreparedStatement not implemented");
}

Status FlightSqlServerBase::DoGetSqlInfo(const pb::sql::CommandGetSqlInfo& command,
                                         const ServerCallContext& context,
                                         std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoSqlInfo not implemented");
}

Status FlightSqlServerBase::GetFlightInfoSchemas(
    const pb::sql::CommandGetSchemas& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetSqlInfo not implemented");
}

Status FlightSqlServerBase::DoGetSchemas(const pb::sql::CommandGetSchemas& command,
                                         const ServerCallContext& context,
                                         std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoSchemas not implemented");
}

Status FlightSqlServerBase::GetFlightInfoTables(const pb::sql::CommandGetTables& command,
                                                const ServerCallContext& context,
                                                const FlightDescriptor& descriptor,
                                                std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetSchemas not implemented");
}

Status FlightSqlServerBase::DoGetTables(const pb::sql::CommandGetTables& command,
                                        const ServerCallContext& context,
                                        std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoTables not implemented");
}

Status FlightSqlServerBase::GetFlightInfoTableTypes(const ServerCallContext& context,
                                                    const FlightDescriptor& descriptor,
                                                    std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetTables not implemented");
}

Status FlightSqlServerBase::DoGetTableTypes(const ServerCallContext& context,
                                            std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetTableTypes not implemented");
}

Status FlightSqlServerBase::GetFlightInfoPrimaryKeys(
    const pb::sql::CommandGetPrimaryKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoTableTypes not implemented");
}

Status FlightSqlServerBase::DoGetPrimaryKeys(
    const pb::sql::CommandGetPrimaryKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoPrimaryKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoExportedKeys(
    const pb::sql::CommandGetExportedKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetPrimaryKeys not implemented");
}

Status FlightSqlServerBase::DoGetExportedKeys(
    const pb::sql::CommandGetExportedKeys& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("GetFlightInfoExportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoImportedKeys(
    const pb::sql::CommandGetImportedKeys& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("DoGetExportedKeys not implemented");
}

std::shared_ptr<Schema> SqlSchema::GetCatalogsSchema() {
  return arrow::schema({field("catalog_name", utf8())});
}

std::shared_ptr<Schema> SqlSchema::GetSchemasSchema() {
  return arrow::schema(
      {field("catalog_name", utf8()), field("schema_name", utf8(), false)});
}

std::shared_ptr<Schema> SqlSchema::GetTablesSchema() {
  return arrow::schema({field("catalog_name", utf8()), field("schema_name", utf8()),
                        field("table_name", utf8()), field("table_type", utf8())});
}

std::shared_ptr<Schema> SqlSchema::GetTablesSchemaWithIncludedSchema() {
  return arrow::schema({field("catalog_name", utf8()), field("schema_name", utf8()),
                        field("table_name", utf8()), field("table_type", utf8()),
                        field("table_schema", binary())});
}

std::shared_ptr<Schema> SqlSchema::GetTablesSchema() {
  return arrow::schema({field("catalog_name", utf8()), field("schema_name", utf8()),
                        field("table_name", utf8()), field("table_type", utf8())});
}

std::shared_ptr<Schema> SqlSchema::GetTablesSchemaWithSchema() {
  return arrow::schema({field("catalog_name", utf8()), field("schema_name", utf8()),
                        field("table_name", utf8()), field("table_type", utf8()),
                        field("table_schema", binary())});
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
