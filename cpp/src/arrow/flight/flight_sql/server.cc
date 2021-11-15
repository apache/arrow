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

#include "arrow/flight/flight_sql/server.h"

#include <google/protobuf/any.pb.h>

#include "arrow/api.h"
#include "arrow/buffer.h"
#include "arrow/flight/flight_sql/FlightSql.pb.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

#define PROPERTY_TO_OPTIONAL(COMMAND, PROPERTY) \
  COMMAND.has_##PROPERTY() ? util::make_optional(COMMAND.PROPERTY()) : util::nullopt;

namespace arrow {
namespace flight {
namespace sql {

namespace pb = arrow::flight::protocol;

arrow::Result<std::string> CreateStatementQueryTicket(
    const std::string& statement_handle) {
  protocol::sql::TicketStatementQuery ticket_statement_query;
  ticket_statement_query.set_statement_handle(statement_handle);

  google::protobuf::Any ticket;
  ticket.PackFrom(ticket_statement_query);

  std::string ticket_string;

  if (!ticket.SerializeToString(&ticket_string)) {
    return Status::IOError("Invalid ticket.");
  }
  return ticket_string;
}

arrow::Result<GetCrossReference> ParseCommandGetCrossReference(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetCrossReference command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetCrossReference.");
  }

  GetCrossReference result;
  result.pk_catalog = PROPERTY_TO_OPTIONAL(command, pk_catalog);
  result.pk_schema = PROPERTY_TO_OPTIONAL(command, pk_schema);
  result.pk_table = command.pk_table();
  result.fk_catalog = PROPERTY_TO_OPTIONAL(command, fk_catalog);
  result.fk_schema = PROPERTY_TO_OPTIONAL(command, fk_schema);
  result.fk_table = command.fk_table();
  return result;
}

arrow::Result<GetImportedKeys> ParseCommandGetImportedKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetImportedKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetImportedKeys.");
  }

  GetImportedKeys result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.schema = PROPERTY_TO_OPTIONAL(command, schema);
  result.table = command.table();
  return result;
}

arrow::Result<GetExportedKeys> ParseCommandGetExportedKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetExportedKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetExportedKeys.");
  }

  GetExportedKeys result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.schema = PROPERTY_TO_OPTIONAL(command, schema);
  result.table = command.table();
  return result;
}

arrow::Result<GetPrimaryKeys> ParseCommandGetPrimaryKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetPrimaryKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetPrimaryKeys.");
  }

  GetPrimaryKeys result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.schema = PROPERTY_TO_OPTIONAL(command, schema);
  result.table = command.table();
  return result;
}

arrow::Result<GetSqlInfo> ParseCommandGetSqlInfo(
    const google::protobuf::Any& any, const SqlInfoResultMap& sql_info_id_to_result) {
  pb::sql::CommandGetSqlInfo command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetSqlInfo.");
  }

  GetSqlInfo result;
  if (command.info_size() > 0) {
    result.info.reserve(command.info_size());
    result.info.assign(command.info().begin(), command.info().end());
  } else {
    result.info.reserve(sql_info_id_to_result.size());
    for (const auto& it : sql_info_id_to_result) {
      result.info.push_back(it.first);
    }
  }
  return result;
}

arrow::Result<GetSchemas> ParseCommandGetSchemas(const google::protobuf::Any& any) {
  pb::sql::CommandGetSchemas command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetSchemas.");
  }

  GetSchemas result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.schema_filter_pattern = PROPERTY_TO_OPTIONAL(command, schema_filter_pattern);
  return result;
}

arrow::Result<PreparedStatementQuery> ParseCommandPreparedStatementQuery(
    const google::protobuf::Any& any) {
  pb::sql::CommandPreparedStatementQuery command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandPreparedStatementQuery.");
  }

  PreparedStatementQuery result;
  result.prepared_statement_handle = command.prepared_statement_handle();
  return result;
}

arrow::Result<StatementQuery> ParseCommandStatementQuery(
    const google::protobuf::Any& any) {
  pb::sql::CommandStatementQuery command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandStatementQuery.");
  }

  StatementQuery result;
  result.query = command.query();
  return result;
}

arrow::Result<GetTables> ParseCommandGetTables(const google::protobuf::Any& any) {
  pb::sql::CommandGetTables command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetTables.");
  }

  std::vector<std::string> table_types(command.table_types_size());
  std::copy(command.table_types().begin(), command.table_types().end(),
            table_types.begin());

  GetTables result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.schema_filter_pattern = PROPERTY_TO_OPTIONAL(command, schema_filter_pattern);
  result.table_name_filter_pattern =
      PROPERTY_TO_OPTIONAL(command, table_name_filter_pattern);
  result.table_types = table_types;
  result.include_schema = command.include_schema();
  return result;
}

arrow::Result<StatementQueryTicket> ParseStatementQueryTicket(
    const google::protobuf::Any& any) {
  pb::sql::TicketStatementQuery command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack TicketStatementQuery.");
  }

  StatementQueryTicket result;
  result.statement_handle = command.statement_handle();
  return result;
}

arrow::Result<StatementUpdate> ParseCommandStatementUpdate(
    const google::protobuf::Any& any) {
  pb::sql::CommandStatementUpdate command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandStatementUpdate.");
  }

  StatementUpdate result;
  result.query = command.query();
  return result;
}

arrow::Result<PreparedStatementUpdate> ParseCommandPreparedStatementUpdate(
    const google::protobuf::Any& any) {
  pb::sql::CommandPreparedStatementUpdate command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandPreparedStatementUpdate.");
  }

  PreparedStatementUpdate result;
  result.prepared_statement_handle = command.prepared_statement_handle();
  return result;
}

arrow::Result<ActionCreatePreparedStatementRequest>
ParseActionCreatePreparedStatementRequest(const google::protobuf::Any& any) {
  pb::sql::ActionCreatePreparedStatementRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionCreatePreparedStatementRequest.");
  }

  ActionCreatePreparedStatementRequest result;
  result.query = command.query();
  return result;
}

arrow::Result<ActionClosePreparedStatementRequest>
ParseActionClosePreparedStatementRequest(const google::protobuf::Any& any) {
  pb::sql::ActionClosePreparedStatementRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionClosePreparedStatementRequest.");
  }

  ActionClosePreparedStatementRequest result;
  result.prepared_statement_handle = command.prepared_statement_handle();
  return result;
}

Status FlightSqlServerBase::GetFlightInfo(const ServerCallContext& context,
                                          const FlightDescriptor& request,
                                          std::unique_ptr<FlightInfo>* info) {
  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command");
  }

  if (any.Is<pb::sql::CommandStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(StatementQuery internal_command,
                          ParseCommandStatementQuery(any));
    return GetFlightInfoStatement(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    return GetFlightInfoPreparedStatement(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    return GetFlightInfoCatalogs(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSchemas>()) {
    ARROW_ASSIGN_OR_RAISE(GetSchemas internal_command, ParseCommandGetSchemas(any));
    return GetFlightInfoSchemas(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    ARROW_ASSIGN_OR_RAISE(GetTables command, ParseCommandGetTables(any));
    return GetFlightInfoTables(context, command, request, info);
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    return GetFlightInfoTableTypes(context, request, info);
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetSqlInfo internal_command,
                          ParseCommandGetSqlInfo(any, sql_info_id_to_result_));
    return GetFlightInfoSqlInfo(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetPrimaryKeys internal_command,
                          ParseCommandGetPrimaryKeys(any));
    return GetFlightInfoPrimaryKeys(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetExportedKeys internal_command,
                          ParseCommandGetExportedKeys(any));
    return GetFlightInfoExportedKeys(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetImportedKeys internal_command,
                          ParseCommandGetImportedKeys(any));
    return GetFlightInfoImportedKeys(context, internal_command, request, info);
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    ARROW_ASSIGN_OR_RAISE(GetCrossReference internal_command,
                          ParseCommandGetCrossReference(any));
    return GetFlightInfoCrossReference(context, internal_command, request, info);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                                  std::unique_ptr<FlightDataStream>* stream) {
  google::protobuf::Any any;

  if (!any.ParseFromArray(request.ticket.data(),
                          static_cast<int>(request.ticket.size()))) {
    return Status::Invalid("Unable to parse ticket.");
  }

  if (any.Is<pb::sql::TicketStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(StatementQueryTicket command, ParseStatementQueryTicket(any));
    return DoGetStatement(context, command, stream);
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    return DoGetPreparedStatement(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    return DoGetCatalogs(context, stream);
  } else if (any.Is<pb::sql::CommandGetSchemas>()) {
    ARROW_ASSIGN_OR_RAISE(GetSchemas internal_command, ParseCommandGetSchemas(any));
    return DoGetSchemas(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    ARROW_ASSIGN_OR_RAISE(GetTables command, ParseCommandGetTables(any));
    return DoGetTables(context, command, stream);
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    return DoGetTableTypes(context, stream);
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetSqlInfo internal_command,
                          ParseCommandGetSqlInfo(any, sql_info_id_to_result_));
    return DoGetSqlInfo(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetPrimaryKeys internal_command,
                          ParseCommandGetPrimaryKeys(any));
    return DoGetPrimaryKeys(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetExportedKeys internal_command,
                          ParseCommandGetExportedKeys(any));
    return DoGetExportedKeys(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetImportedKeys internal_command,
                          ParseCommandGetImportedKeys(any));
    return DoGetImportedKeys(context, internal_command, stream);
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    ARROW_ASSIGN_OR_RAISE(GetCrossReference internal_command,
                          ParseCommandGetCrossReference(any));
    return DoGetCrossReference(context, internal_command, stream);
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoPut(const ServerCallContext& context,
                                  std::unique_ptr<FlightMessageReader> reader,
                                  std::unique_ptr<FlightMetadataWriter> writer) {
  const FlightDescriptor& request = reader->descriptor();

  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command.");
  }

  if (any.Is<pb::sql::CommandStatementUpdate>()) {
    ARROW_ASSIGN_OR_RAISE(StatementUpdate internal_command,
                          ParseCommandStatementUpdate(any));
    ARROW_ASSIGN_OR_RAISE(auto record_count,
                          DoPutCommandStatementUpdate(context, internal_command, reader))

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const std::shared_ptr<Buffer>& buffer =
        Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

    return Status::OK();
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    return DoPutPreparedStatementQuery(context, internal_command, reader.get(),
                                       writer.get());
  } else if (any.Is<pb::sql::CommandPreparedStatementUpdate>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementUpdate internal_command,
                          ParseCommandPreparedStatementUpdate(any));
    ARROW_ASSIGN_OR_RAISE(auto record_count, DoPutPreparedStatementUpdate(
                                                 context, internal_command, reader.get()))

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
  *actions = {FlightSqlServerBase::kCreatePreparedStatementActionType,
              FlightSqlServerBase::kClosePreparedStatementActionType};
  return Status::OK();
}

Status FlightSqlServerBase::DoAction(const ServerCallContext& context,
                                     const Action& action,
                                     std::unique_ptr<ResultStream>* result_stream) {
  if (action.type == FlightSqlServerBase::kCreatePreparedStatementActionType.type) {
    google::protobuf::Any any_command;
    if (!any_command.ParseFromArray(action.body->data(),
                                    static_cast<int>(action.body->size()))) {
      return Status::Invalid("Unable to parse action.");
    }

    ARROW_ASSIGN_OR_RAISE(ActionCreatePreparedStatementRequest internal_command,
                          ParseActionCreatePreparedStatementRequest(any_command));
    ARROW_ASSIGN_OR_RAISE(auto result, CreatePreparedStatement(context, internal_command))

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
  } else if (action.type == FlightSqlServerBase::kClosePreparedStatementActionType.type) {
    google::protobuf::Any any;
    if (!any.ParseFromArray(action.body->data(), static_cast<int>(action.body->size()))) {
      return Status::Invalid("Unable to parse action.");
    }

    ARROW_ASSIGN_OR_RAISE(ActionClosePreparedStatementRequest internal_command,
                          ParseActionClosePreparedStatementRequest(any));

    return ClosePreparedStatement(context, internal_command, result_stream);
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

Status FlightSqlServerBase::GetFlightInfoStatement(const ServerCallContext& context,
                                                   const StatementQuery& command,
                                                   const FlightDescriptor& descriptor,
                                                   std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoStatement not implemented");
}

Status FlightSqlServerBase::DoGetStatement(const ServerCallContext& context,
                                           const StatementQueryTicket& command,
                                           std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetStatement not implemented");
}

Status FlightSqlServerBase::GetFlightInfoPreparedStatement(
    const ServerCallContext& context, const PreparedStatementQuery& command,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoPreparedStatement not implemented");
}

Status FlightSqlServerBase::DoGetPreparedStatement(
    const ServerCallContext& context, const PreparedStatementQuery& command,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetPreparedStatement not implemented");
}

Status FlightSqlServerBase::GetFlightInfoSqlInfo(const ServerCallContext& context,
                                                 const GetSqlInfo& command,
                                                 const FlightDescriptor& descriptor,
                                                 std::unique_ptr<FlightInfo>* info) {
  assert(info != nullptr);

  if (sql_info_id_to_result_.empty()) {
    return Status::NotImplemented("GetFlightInfoSqlInfo not implemented");
  }

  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result, FlightInfo::Make(*SqlSchema::GetSqlInfoSchema(),
                                                      descriptor, endpoints, -1, -1))

  *info = std::unique_ptr<FlightInfo>(new FlightInfo(result));

  return Status::OK();
}

void FlightSqlServerBase::RegisterSqlInfo(int32_t id, const SqlInfoResult& result) {
  sql_info_id_to_result_[id] = result;
}

Status FlightSqlServerBase::DoGetSqlInfo(const ServerCallContext& context,
                                         const GetSqlInfo& command,
                                         std::unique_ptr<FlightDataStream>* result) {
  DCHECK(result);

  MemoryPool* memory_pool = default_memory_pool();
  UInt32Builder name_field_builder(memory_pool);
  std::unique_ptr<ArrayBuilder> value_field_builder;
  const auto& value_field_type = std::static_pointer_cast<DenseUnionType>(
      SqlSchema::GetSqlInfoSchema()->fields()[1]->type());
  ARROW_RETURN_NOT_OK(MakeBuilder(memory_pool, value_field_type, &value_field_builder));

  internal::SqlInfoResultAppender sql_info_result_appender(
      reinterpret_cast<DenseUnionBuilder&>(*value_field_builder));

  // Populate both name_field_builder and value_field_builder for each element
  // on command.info.
  // value_field_builder is populated differently depending on the data type (as it is
  // a DenseUnionBuilder). The population for each data type is implemented on
  // internal::SqlInfoResultAppender.
  for (const auto& info : command.info) {
    if (!sql_info_id_to_result_.count(info)) {
      return Status::KeyError(
          "Attempt to fetch unregistered/unsupported SqlInfo option: " +
          std::to_string(info));
    }
    ARROW_RETURN_NOT_OK(name_field_builder.Append(info));
    ARROW_RETURN_NOT_OK(
        arrow::util::visit(sql_info_result_appender, sql_info_id_to_result_.at(info)));
  }

  std::shared_ptr<Array> name;
  ARROW_RETURN_NOT_OK(name_field_builder.Finish(&name));
  std::shared_ptr<Array> value;
  ARROW_RETURN_NOT_OK(value_field_builder->Finish(&value));

  auto row_count = static_cast<int64_t>(command.info.size());
  const std::shared_ptr<RecordBatch>& batch =
      RecordBatch::Make(SqlSchema::GetSqlInfoSchema(), row_count, {name, value});
  ARROW_ASSIGN_OR_RAISE(const auto reader, RecordBatchReader::Make({batch}));
  *result = std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  return Status::OK();
}

Status FlightSqlServerBase::GetFlightInfoSchemas(const ServerCallContext& context,
                                                 const GetSchemas& command,
                                                 const FlightDescriptor& descriptor,
                                                 std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoSchemas not implemented");
}

Status FlightSqlServerBase::DoGetSchemas(const ServerCallContext& context,
                                         const GetSchemas& command,
                                         std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetSchemas not implemented");
}

Status FlightSqlServerBase::GetFlightInfoTables(const ServerCallContext& context,
                                                const GetTables& command,
                                                const FlightDescriptor& descriptor,
                                                std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoTables not implemented");
}

Status FlightSqlServerBase::DoGetTables(const ServerCallContext& context,
                                        const GetTables& command,
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

Status FlightSqlServerBase::GetFlightInfoPrimaryKeys(const ServerCallContext& context,
                                                     const GetPrimaryKeys& command,
                                                     const FlightDescriptor& descriptor,
                                                     std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoPrimaryKeys not implemented");
}

Status FlightSqlServerBase::DoGetPrimaryKeys(const ServerCallContext& context,
                                             const GetPrimaryKeys& command,
                                             std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetPrimaryKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoExportedKeys(const ServerCallContext& context,
                                                      const GetExportedKeys& command,
                                                      const FlightDescriptor& descriptor,
                                                      std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoExportedKeys not implemented");
}

Status FlightSqlServerBase::DoGetExportedKeys(const ServerCallContext& context,
                                              const GetExportedKeys& command,
                                              std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetExportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoImportedKeys(const ServerCallContext& context,
                                                      const GetImportedKeys& command,
                                                      const FlightDescriptor& descriptor,
                                                      std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoImportedKeys not implemented");
}

Status FlightSqlServerBase::DoGetImportedKeys(const ServerCallContext& context,
                                              const GetImportedKeys& command,
                                              std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetImportedKeys not implemented");
}

Status FlightSqlServerBase::GetFlightInfoCrossReference(
    const ServerCallContext& context, const GetCrossReference& command,
    const FlightDescriptor& descriptor, std::unique_ptr<FlightInfo>* info) {
  return Status::NotImplemented("GetFlightInfoCrossReference not implemented");
}

Status FlightSqlServerBase::DoGetCrossReference(
    const ServerCallContext& context, const GetCrossReference& command,
    std::unique_ptr<FlightDataStream>* result) {
  return Status::NotImplemented("DoGetCrossReference not implemented");
}

arrow::Result<ActionCreatePreparedStatementResult>
FlightSqlServerBase::CreatePreparedStatement(
    const ServerCallContext& context,
    const ActionCreatePreparedStatementRequest& request) {
  return Status::NotImplemented("CreatePreparedStatement not implemented");
}

Status FlightSqlServerBase::ClosePreparedStatement(
    const ServerCallContext& context, const ActionClosePreparedStatementRequest& request,
    std::unique_ptr<ResultStream>* result) {
  return Status::NotImplemented("ClosePreparedStatement not implemented");
}

Status FlightSqlServerBase::DoPutPreparedStatementQuery(
    const ServerCallContext& context, const PreparedStatementQuery& command,
    FlightMessageReader* reader, FlightMetadataWriter* writer) {
  return Status::NotImplemented("DoPutPreparedStatementQuery not implemented");
}

arrow::Result<int64_t> FlightSqlServerBase::DoPutPreparedStatementUpdate(
    const ServerCallContext& context, const PreparedStatementUpdate& command,
    FlightMessageReader* reader) {
  return Status::NotImplemented("DoPutPreparedStatementUpdate not implemented");
}

arrow::Result<int64_t> FlightSqlServerBase::DoPutCommandStatementUpdate(
    const ServerCallContext& context, const StatementUpdate& command,
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

std::shared_ptr<Schema> SqlSchema::GetSqlInfoSchema() {
  return arrow::schema({field("name", uint32(), false),
                        field("value",
                              dense_union({field("string_value", utf8(), false),
                                           field("bool_value", boolean(), false),
                                           field("bigint_value", int64(), false),
                                           field("int32_bitmask", int32(), false),
                                           field("string_list", list(utf8()), false),
                                           field("int32_to_int32_list_map",
                                                 map(int32(), list(int32())), false)}),
                              false)});
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
