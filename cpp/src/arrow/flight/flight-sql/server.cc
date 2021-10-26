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

#include "server.h"

#include <boost/algorithm/string.hpp>
#include <boost/uuid/uuid.hpp>
#include <sstream>

#include "arrow/api.h"
#include "arrow/buffer.h"

std::string arrow::flight::sql::FlightSqlServerBase::CreateStatementQueryTicket(
    const std::string& statement_handle) {
  protocol::sql::TicketStatementQuery ticket_statement_query;
  ticket_statement_query.set_statement_handle(statement_handle);

  google::protobuf::Any ticket;
  ticket.PackFrom(ticket_statement_query);

  const std::string& ticket_string = ticket.SerializeAsString();
  return ticket_string;
}

namespace arrow {
namespace flight {
namespace sql {

namespace pb = arrow::flight::protocol;

GetCrossReference ParseCommandGetCrossReference(const google::protobuf::Any& any) {
  pb::sql::CommandGetCrossReference command;
  any.UnpackTo(&command);

  return (GetCrossReference){.has_pk_catalog = command.has_pk_catalog(),
                             .pk_catalog = command.pk_catalog(),
                             .has_pk_schema = command.has_pk_schema(),
                             .pk_schema = command.pk_schema(),
                             .pk_table = command.pk_table(),
                             .has_fk_catalog = command.has_fk_catalog(),
                             .fk_catalog = command.fk_catalog(),
                             .has_fk_schema = command.has_fk_schema(),
                             .fk_schema = command.fk_schema(),
                             .fk_table = command.fk_table()};
}

GetImportedKeys ParseCommandGetImportedKeys(const google::protobuf::Any& any) {
  pb::sql::CommandGetImportedKeys command;
  any.UnpackTo(&command);

  return (GetImportedKeys){.has_catalog = command.has_catalog(),
                           .catalog = command.catalog(),
                           .has_schema = command.has_schema(),
                           .schema = command.schema(),
                           .table = command.table()};
}

GetExportedKeys ParseCommandGetExportedKeys(const google::protobuf::Any& any) {
  pb::sql::CommandGetExportedKeys command;
  any.UnpackTo(&command);

  return (GetExportedKeys){.has_catalog = command.has_catalog(),
                           .catalog = command.catalog(),
                           .has_schema = command.has_schema(),
                           .schema = command.schema(),
                           .table = command.table()};
}

GetPrimaryKeys ParseCommandGetPrimaryKeys(const google::protobuf::Any& any) {
  pb::sql::CommandGetPrimaryKeys command;
  any.UnpackTo(&command);

  return (GetPrimaryKeys){.has_catalog = command.has_catalog(),
                          .catalog = command.catalog(),
                          .has_schema = command.has_schema(),
                          .schema = command.schema(),
                          .table = command.table()};
}

GetSqlInfo ParseCommandGetSqlInfo(const google::protobuf::Any& any) {
  pb::sql::CommandGetSqlInfo command;
  any.UnpackTo(&command);

  return (GetSqlInfo){};
}

GetSchemas ParseCommandGetSchemas(const google::protobuf::Any& any) {
  pb::sql::CommandGetSchemas command;
  any.UnpackTo(&command);

  return (GetSchemas){
      .has_catalog = command.has_catalog(),
      .catalog = command.catalog(),
      .has_schema_filter_pattern = command.has_schema_filter_pattern(),
      .schema_filter_pattern = command.schema_filter_pattern(),
  };
}

PreparedStatementQuery ParseCommandPreparedStatementQuery(
    const google::protobuf::Any& any) {
  pb::sql::CommandPreparedStatementQuery command;
  any.UnpackTo(&command);

  return (PreparedStatementQuery){.prepared_statement_handle =
                                      command.prepared_statement_handle()};
}

StatementQuery ParseCommandStatementQuery(const google::protobuf::Any& any) {
  pb::sql::CommandStatementQuery command;
  any.UnpackTo(&command);

  return (StatementQuery){.query = command.query()};
}

GetTables ParseCommandGetTables(const google::protobuf::Any& anyCommand) {
  pb::sql::CommandGetTables command;
  anyCommand.UnpackTo(&command);

  std::vector<std::string> table_types;
  table_types.reserve(command.table_types_size());
  for (const auto& item : command.table_types()) {
    table_types.push_back(item);
  }
  return (GetTables){
      .has_catalog = command.has_catalog(),
      .catalog = command.catalog(),
      .has_schema_filter_pattern = command.has_schema_filter_pattern(),
      .schema_filter_pattern = command.schema_filter_pattern(),
      .has_table_name_filter_pattern = command.has_table_name_filter_pattern(),
      .table_name_filter_pattern = command.table_name_filter_pattern(),
      .table_types = table_types,
      .include_schema = command.include_schema()};
}

StatementQueryTicket ParseStatementQueryTicket(const google::protobuf::Any& anyCommand) {
  pb::sql::TicketStatementQuery command;
  anyCommand.UnpackTo(&command);

  return (StatementQueryTicket){.statement_handle = command.statement_handle()};
}

StatementUpdate ParseCommandStatementUpdate(const google::protobuf::Any& any) {
  pb::sql::CommandStatementUpdate command;
  any.UnpackTo(&command);

  return (StatementUpdate){.query = command.query()};
}

PreparedStatementUpdate ParseCommandPreparedStatementUpdate(
    const google::protobuf::Any& any) {
  pb::sql::CommandPreparedStatementUpdate command;
  any.UnpackTo(&command);

  return (PreparedStatementUpdate){.prepared_statement_handle =
                                       command.prepared_statement_handle()};
}

ActionCreatePreparedStatementRequest ParseActionCreatePreparedStatementRequest(
    const google::protobuf::Any& anyCommand) {
  pb::sql::ActionCreatePreparedStatementRequest command;
  anyCommand.UnpackTo(&command);

  return (ActionCreatePreparedStatementRequest){.query = command.query()};
}

ActionClosePreparedStatementRequest ParseActionClosePreparedStatementRequest(
    const google::protobuf::Any& anyCommand) {
  pb::sql::ActionClosePreparedStatementRequest command;
  anyCommand.UnpackTo(&command);

  return (ActionClosePreparedStatementRequest){.prepared_statement_handle =
                                                   command.prepared_statement_handle()};
}

Status FlightSqlServerBase::GetFlightInfo(const ServerCallContext& context,
                                          const FlightDescriptor& request,
                                          std::unique_ptr<FlightInfo>* info) {
  google::protobuf::Any any;
  any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()));

  if (any.Is<pb::sql::CommandStatementQuery>()) {
    StatementQuery internal_command = ParseCommandStatementQuery(any);
    return GetFlightInfoStatement(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    PreparedStatementQuery internal_command = ParseCommandPreparedStatementQuery(any);
    return GetFlightInfoPreparedStatement(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    return GetFlightInfoCatalogs(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSchemas>()) {
    GetSchemas internal_command = ParseCommandGetSchemas(any);
    return GetFlightInfoSchemas(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    GetTables command = ParseCommandGetTables(any);
    return GetFlightInfoTables(command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    return GetFlightInfoTableTypes(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    GetSqlInfo internal_command = ParseCommandGetSqlInfo(any);
    return GetFlightInfoSqlInfo(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    GetPrimaryKeys internal_command = ParseCommandGetPrimaryKeys(any);
    return GetFlightInfoPrimaryKeys(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    GetExportedKeys internal_command = ParseCommandGetExportedKeys(any);
    return GetFlightInfoExportedKeys(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    GetImportedKeys internal_command = ParseCommandGetImportedKeys(any);
    return GetFlightInfoImportedKeys(internal_command, context, request, info);
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    GetCrossReference internal_command = ParseCommandGetCrossReference(any);
    return GetFlightInfoCrossReference(internal_command, context, request, info);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                                  std::unique_ptr<FlightDataStream>* stream) {
  google::protobuf::Any anyCommand;

  anyCommand.ParseFromArray(request.ticket.data(),
                            static_cast<int>(request.ticket.size()));

  if (anyCommand.Is<pb::sql::TicketStatementQuery>()) {
    StatementQueryTicket command = ParseStatementQueryTicket(anyCommand);
    return DoGetStatement(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandPreparedStatementQuery>()) {
    PreparedStatementQuery internal_command =
        ParseCommandPreparedStatementQuery(anyCommand);
    return DoGetPreparedStatement(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetCatalogs>()) {
    return DoGetCatalogs(context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetSchemas>()) {
    GetSchemas internal_command = ParseCommandGetSchemas(anyCommand);
    return DoGetSchemas(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetTables>()) {
    GetTables command = ParseCommandGetTables(anyCommand);
    return DoGetTables(command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetTableTypes>()) {
    return DoGetTableTypes(context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetSqlInfo>()) {
    GetSqlInfo internal_command = ParseCommandGetSqlInfo(anyCommand);
    return DoGetSqlInfo(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetPrimaryKeys>()) {
    GetPrimaryKeys internal_command = ParseCommandGetPrimaryKeys(anyCommand);
    return DoGetPrimaryKeys(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetExportedKeys>()) {
    GetExportedKeys internal_command = ParseCommandGetExportedKeys(anyCommand);
    return DoGetExportedKeys(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetImportedKeys>()) {
    GetImportedKeys internal_command = ParseCommandGetImportedKeys(anyCommand);
    return DoGetImportedKeys(internal_command, context, stream);
  } else if (anyCommand.Is<pb::sql::CommandGetCrossReference>()) {
    GetCrossReference internal_command = ParseCommandGetCrossReference(anyCommand);
    return DoGetCrossReference(internal_command, context, stream);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoPut(const ServerCallContext& context,
                                  std::unique_ptr<FlightMessageReader> reader,
                                  std::unique_ptr<FlightMetadataWriter> writer) {
  const FlightDescriptor& request = reader->descriptor();

  google::protobuf::Any any;
  any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()));

  if (any.Is<pb::sql::CommandStatementUpdate>()) {
    StatementUpdate internal_command = ParseCommandStatementUpdate(any);
    ARROW_ASSIGN_OR_RAISE(auto record_count,
                          DoPutCommandStatementUpdate(internal_command, context, reader))

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const std::shared_ptr<Buffer>& buffer =
        Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

    return Status::OK();
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    PreparedStatementQuery internal_command = ParseCommandPreparedStatementQuery(any);
    return DoPutPreparedStatementQuery(internal_command, context, reader, writer);
  } else if (any.Is<pb::sql::CommandPreparedStatementUpdate>()) {
    PreparedStatementUpdate internal_command = ParseCommandPreparedStatementUpdate(any);
    ARROW_ASSIGN_OR_RAISE(auto record_count,
                          DoPutPreparedStatementUpdate(internal_command, context, reader))

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const std::shared_ptr<Buffer>& buffer =
        Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

    return Status::OK();
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::ListActions(const ServerCallContext& context,
                                        std::vector<ActionType>* actions) {
  *actions = {FlightSqlServerBase::FLIGHT_SQL_CREATE_PREPARED_STATEMENT,
              FlightSqlServerBase::FLIGHT_SQL_CLOSE_PREPARED_STATEMENT};
  return Status::OK();
}

Status FlightSqlServerBase::DoAction(const ServerCallContext& context,
                                     const Action& action,
                                     std::unique_ptr<ResultStream>* result_stream) {
  if (action.type == FlightSqlServerBase::FLIGHT_SQL_CREATE_PREPARED_STATEMENT.type) {
    google::protobuf::Any anyCommand;
    anyCommand.ParseFromArray(action.body->data(), static_cast<int>(action.body->size()));

    ActionCreatePreparedStatementRequest internal_command =
        ParseActionCreatePreparedStatementRequest(anyCommand);
    ARROW_ASSIGN_OR_RAISE(auto result, CreatePreparedStatement(internal_command, context))

    pb::sql::ActionCreatePreparedStatementResult action_result;
    action_result.set_prepared_statement_handle(result.prepared_statement_handle);
    if (result.dataset_schema != nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto serialized_dataset_schema,
                            ipc::SerializeSchema(*result.dataset_schema))
      action_result.set_dataset_schema(serialized_dataset_schema->ToString());
    }
    if (result.parameter_schema != nullptr) {
      ARROW_ASSIGN_OR_RAISE(auto serialized_parameter_schema,
                            ipc::SerializeSchema(*result.parameter_schema))
      action_result.set_parameter_schema(serialized_parameter_schema->ToString());
    }

    google::protobuf::Any any;
    any.PackFrom(action_result);

    auto buf = Buffer::FromString(any.SerializeAsString());
    *result_stream = std::unique_ptr<ResultStream>(new SimpleResultStream({Result{buf}}));

    return Status::OK();
  } else if (action.type ==
             FlightSqlServerBase::FLIGHT_SQL_CLOSE_PREPARED_STATEMENT.type) {
    google::protobuf::Any anyCommand;
    anyCommand.ParseFromArray(action.body->data(), static_cast<int>(action.body->size()));

    ActionClosePreparedStatementRequest internal_command =
        ParseActionClosePreparedStatementRequest(anyCommand);

    return ClosePreparedStatement(internal_command, context, result_stream);
  }
  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::GetFlightInfoCatalogs(const ServerCallContext& context,
                                                  const FlightDescriptor& descriptor,
                                                  std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoCatalogs not implemented");
}

Status FlightSqlServerBase::DoGetCatalogs(const ServerCallContext& context,
                                          std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetCatalogs not implemented");
}

Status FlightSqlServerBase::GetFlightInfoStatement(const StatementQuery& command,
                                                   const ServerCallContext& context,
                                                   const FlightDescriptor& descriptor,
                                                   std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoStatement not implemented");
}

Status FlightSqlServerBase::DoGetStatement(const StatementQueryTicket& command,
                                           const ServerCallContext& context,
                                           std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetStatement not implemented");
}

Status FlightSqlServerBase::GetFlightInfoPreparedStatement(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoPreparedStatement not implemented");
}

Status FlightSqlServerBase::DoGetPreparedStatement(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetPreparedStatement not implemented");
}

Status FlightSqlServerBase::GetFlightInfoSqlInfo(const GetSqlInfo& command,
                                                 const ServerCallContext& context,
                                                 const FlightDescriptor& descriptor,
                                                 std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoSqlInfo not implemented");
}

Status FlightSqlServerBase::DoGetSqlInfo(const GetSqlInfo& command,
                                         const ServerCallContext& context,
                                         std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetSqlInfo not implemented");
}

Status FlightSqlServerBase::GetFlightInfoSchemas(const GetSchemas& command,
                                                 const ServerCallContext& context,
                                                 const FlightDescriptor& descriptor,
                                                 std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoSchemas not implemented");
}

Status FlightSqlServerBase::DoGetSchemas(const GetSchemas& command,
                                         const ServerCallContext& context,
                                         std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetSchemas not implemented");
}

Status FlightSqlServerBase::GetFlightInfoTables(const GetTables& command,
                                                const ServerCallContext& context,
                                                const FlightDescriptor& descriptor,
                                                std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoTables not implemented");
}

Status FlightSqlServerBase::DoGetTables(const GetTables& command,
                                        const ServerCallContext& context,
                                        std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetTables not implemented");
}

Status FlightSqlServerBase::GetFlightInfoTableTypes(const ServerCallContext& context,
                                                    const FlightDescriptor& descriptor,
                                                    std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoTableTypes not implemented");
}

Status FlightSqlServerBase::DoGetTableTypes(const ServerCallContext& context,
                                            std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetTableTypes not implemented");
}

Status FlightSqlServerBase::GetFlightInfoPrimaryKeys(const GetPrimaryKeys& command,
                                                     const ServerCallContext& context,
                                                     const FlightDescriptor& descriptor,
                                                     std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoPrimaryKeys not implemented");
}

Status FlightSqlServerBase::DoGetPrimaryKeys(const GetPrimaryKeys& command,
                                             const ServerCallContext& context,
                                             std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetPrimaryKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoExportedKeys(const GetExportedKeys& command,
                                                      const ServerCallContext& context,
                                                      const FlightDescriptor& descriptor,
                                                      std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoExportedKeys not implemented");
}

Status FlightSqlServerBase::DoGetExportedKeys(const GetExportedKeys& command,
                                              const ServerCallContext& context,
                                              std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetExportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoImportedKeys(const GetImportedKeys& command,
                                                      const ServerCallContext& context,
                                                      const FlightDescriptor& descriptor,
                                                      std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoImportedKeys not implemented");
}

Status FlightSqlServerBase::DoGetImportedKeys(const GetImportedKeys& command,
                                              const ServerCallContext& context,
                                              std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetImportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoCrossReference(
    const GetCrossReference& command, const ServerCallContext& context,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoCrossReference not implemented");
}

Status FlightSqlServerBase::DoGetCrossReference(
    const GetCrossReference& command, const ServerCallContext& context,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetCrossReference not implemented");
}

arrow::Result<ActionCreatePreparedStatementResult>
FlightSqlServerBase::CreatePreparedStatement(
    const ActionCreatePreparedStatementRequest& request,
    const ServerCallContext& context) {
  return Status::NotImplemented("CreatePreparedStatement not implemented");
}

Status FlightSqlServerBase::ClosePreparedStatement(
    const ActionClosePreparedStatementRequest& request, const ServerCallContext& context,
    std::unique_ptr<ResultStream>* p_ptr) {
  return Status::NotImplemented("ClosePreparedStatement not implemented");
}

Status FlightSqlServerBase::DoPutPreparedStatementQuery(
    const PreparedStatementQuery& command, const ServerCallContext& context,
    std::unique_ptr<FlightMessageReader>& reader,
    std::unique_ptr<FlightMetadataWriter>& writer) {
  return Status::NotImplemented("DoPutPreparedStatementQuery not implemented");
}

arrow::Result<int64_t> FlightSqlServerBase::DoPutPreparedStatementUpdate(
    const PreparedStatementUpdate& command, const ServerCallContext& context,
    std::unique_ptr<FlightMessageReader>& reader) {
  return Status::NotImplemented("DoPutPreparedStatementUpdate not implemented");
}

arrow::Result<int64_t> FlightSqlServerBase::DoPutCommandStatementUpdate(
    const StatementUpdate& command, const ServerCallContext& context,
    std::unique_ptr<FlightMessageReader>& reader) {
  return Status::NotImplemented("DoPutCommandStatementUpdate not implemented");
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

std::shared_ptr<Schema> SqlSchema::GetTableTypesSchema() {
  return arrow::schema({field("table_type", utf8())});
}

std::shared_ptr<Schema> SqlSchema::GetPrimaryKeysSchema() {
  return arrow::schema({field("catalog_name", utf8()), field("schema_name", utf8()),
                        field("table_name", utf8()), field("column_name", utf8()),
                        field("key_sequence", int64()), field("key_name", utf8())});
}

std::shared_ptr<Schema> GetImportedExportedKeysAndCrossReferenceSchema() {
  return arrow::schema(
      {field("pk_catalog_name", utf8(), true), field("pk_schema_name", utf8(), true),
       field("pk_table_name", utf8(), false), field("pk_column_name", utf8(), false),
       field("fk_catalog_name", utf8(), true), field("fk_schema_name", utf8(), true),
       field("fk_table_name", utf8(), false), field("fk_column_name", utf8(), false),
       field("key_sequence", int32(), false), field("fk_key_name", utf8(), true),
       field("pk_key_name", utf8(), true), field("update_rule", uint8(), false),
       field("delete_rule", uint8(), false)});
}

std::shared_ptr<Schema> SqlSchema::GetImportedKeysSchema() {
  return GetImportedExportedKeysAndCrossReferenceSchema();
}

std::shared_ptr<Schema> SqlSchema::GetExportedKeysSchema() {
  return GetImportedExportedKeysAndCrossReferenceSchema();
}

std::shared_ptr<Schema> SqlSchema::GetCrossReferenceSchema() {
  return GetImportedExportedKeysAndCrossReferenceSchema();
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
