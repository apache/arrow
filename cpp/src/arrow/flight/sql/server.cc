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

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/sql/server.h"

#include <google/protobuf/any.pb.h>

#include "arrow/buffer.h"
#include "arrow/builder.h"
#include "arrow/flight/sql/protocol_internal.h"
#include "arrow/flight/sql/sql_info_internal.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"

#define PROPERTY_TO_OPTIONAL(COMMAND, PROPERTY) \
  COMMAND.has_##PROPERTY() ? std::make_optional(COMMAND.PROPERTY()) : std::nullopt

namespace arrow {
namespace flight {
namespace sql {

namespace pb = arrow::flight::protocol;

using arrow::internal::checked_cast;
using arrow::internal::checked_pointer_cast;

namespace {

arrow::Result<GetCrossReference> ParseCommandGetCrossReference(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetCrossReference command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetCrossReference.");
  }

  GetCrossReference result;
  result.pk_table_ref = {PROPERTY_TO_OPTIONAL(command, pk_catalog),
                         PROPERTY_TO_OPTIONAL(command, pk_db_schema), command.pk_table()};
  result.fk_table_ref = {PROPERTY_TO_OPTIONAL(command, fk_catalog),
                         PROPERTY_TO_OPTIONAL(command, fk_db_schema), command.fk_table()};
  return result;
}

arrow::Result<GetImportedKeys> ParseCommandGetImportedKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetImportedKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetImportedKeys.");
  }

  GetImportedKeys result;
  result.table_ref = {PROPERTY_TO_OPTIONAL(command, catalog),
                      PROPERTY_TO_OPTIONAL(command, db_schema), command.table()};
  return result;
}

arrow::Result<GetExportedKeys> ParseCommandGetExportedKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetExportedKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetExportedKeys.");
  }

  GetExportedKeys result;
  result.table_ref = {PROPERTY_TO_OPTIONAL(command, catalog),
                      PROPERTY_TO_OPTIONAL(command, db_schema), command.table()};
  return result;
}

arrow::Result<GetPrimaryKeys> ParseCommandGetPrimaryKeys(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetPrimaryKeys command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetPrimaryKeys.");
  }

  GetPrimaryKeys result;
  result.table_ref = {PROPERTY_TO_OPTIONAL(command, catalog),
                      PROPERTY_TO_OPTIONAL(command, db_schema), command.table()};
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

arrow::Result<GetDbSchemas> ParseCommandGetDbSchemas(const google::protobuf::Any& any) {
  pb::sql::CommandGetDbSchemas command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetDbSchemas.");
  }

  GetDbSchemas result;
  result.catalog = PROPERTY_TO_OPTIONAL(command, catalog);
  result.db_schema_filter_pattern =
      PROPERTY_TO_OPTIONAL(command, db_schema_filter_pattern);
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
    return Status::Invalid("Unable to unpack CommandStatementQuery");
  }

  StatementQuery result;
  result.query = command.query();
  result.transaction_id = command.transaction_id();
  return result;
}

SubstraitPlan ParseStatementSubstraitPlan(const pb::sql::SubstraitPlan& pb_plan) {
  return {pb_plan.plan(), pb_plan.version()};
}

arrow::Result<StatementSubstraitPlan> ParseCommandStatementSubstraitPlan(
    const google::protobuf::Any& any) {
  pb::sql::CommandStatementSubstraitPlan command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandStatementSubstraitPlan");
  }

  StatementSubstraitPlan result;
  result.plan = ParseStatementSubstraitPlan(command.plan());
  result.transaction_id = command.transaction_id();
  return result;
}

arrow::Result<GetXdbcTypeInfo> ParseCommandGetXdbcTypeInfo(
    const google::protobuf::Any& any) {
  pb::sql::CommandGetXdbcTypeInfo command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack CommandGetXdbcTypeInfo.");
  }

  GetXdbcTypeInfo result;
  result.data_type = PROPERTY_TO_OPTIONAL(command, data_type);
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
  result.db_schema_filter_pattern =
      PROPERTY_TO_OPTIONAL(command, db_schema_filter_pattern);
  result.table_name_filter_pattern =
      PROPERTY_TO_OPTIONAL(command, table_name_filter_pattern);
  result.table_types = table_types;
  result.include_schema = command.include_schema();
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
  result.transaction_id = command.transaction_id();
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

arrow::Result<ActionBeginSavepointRequest> ParseActionBeginSavepointRequest(
    const google::protobuf::Any& any) {
  pb::sql::ActionBeginSavepointRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionBeginSavepointRequest");
  }

  ActionBeginSavepointRequest result;
  result.transaction_id = command.transaction_id();
  result.name = command.name();
  return result;
}

arrow::Result<ActionBeginTransactionRequest> ParseActionBeginTransactionRequest(
    const google::protobuf::Any& any) {
  pb::sql::ActionBeginTransactionRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionBeginTransactionRequest");
  }

  ActionBeginTransactionRequest result;
  return result;
}

arrow::Result<ActionCancelQueryRequest> ParseActionCancelQueryRequest(
    const google::protobuf::Any& any) {
  pb::sql::ActionCancelQueryRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionCancelQueryRequest");
  }

  ActionCancelQueryRequest result;
  ARROW_ASSIGN_OR_RAISE(result.info, FlightInfo::Deserialize(command.info()));
  return result;
}

arrow::Result<ActionCreatePreparedStatementRequest>
ParseActionCreatePreparedStatementRequest(const google::protobuf::Any& any) {
  pb::sql::ActionCreatePreparedStatementRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionCreatePreparedStatementRequest");
  }

  ActionCreatePreparedStatementRequest result;
  result.query = command.query();
  result.transaction_id = command.transaction_id();
  return result;
}

arrow::Result<ActionCreatePreparedSubstraitPlanRequest>
ParseActionCreatePreparedSubstraitPlanRequest(const google::protobuf::Any& any) {
  pb::sql::ActionCreatePreparedSubstraitPlanRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionCreatePreparedSubstraitPlanRequest");
  }

  ActionCreatePreparedSubstraitPlanRequest result;
  result.plan = ParseStatementSubstraitPlan(command.plan());
  result.transaction_id = command.transaction_id();
  return result;
}

arrow::Result<ActionClosePreparedStatementRequest>
ParseActionClosePreparedStatementRequest(const google::protobuf::Any& any) {
  pb::sql::ActionClosePreparedStatementRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionClosePreparedStatementRequest");
  }

  ActionClosePreparedStatementRequest result;
  result.prepared_statement_handle = command.prepared_statement_handle();
  return result;
}

arrow::Result<ActionEndSavepointRequest> ParseActionEndSavepointRequest(
    const google::protobuf::Any& any) {
  pb::sql::ActionEndSavepointRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionEndSavepointRequest");
  }

  ActionEndSavepointRequest result;
  result.savepoint_id = command.savepoint_id();
  switch (command.action()) {
    case pb::sql::ActionEndSavepointRequest::END_SAVEPOINT_UNSPECIFIED:
      return Status::Invalid(
          "ActionEndSavepointRequest.action was END_SAVEPOINT_UNSPECIFIED");
    case pb::sql::ActionEndSavepointRequest::END_SAVEPOINT_RELEASE:
      result.action = ActionEndSavepointRequest::kRelease;
      break;
    case pb::sql::ActionEndSavepointRequest::END_SAVEPOINT_ROLLBACK:
      result.action = ActionEndSavepointRequest::kRollback;
      break;
    default:
      return Status::Invalid("Unknown value for ActionEndSavepointRequest.action: ",
                             command.action());
  }
  return result;
}

arrow::Result<ActionEndTransactionRequest> ParseActionEndTransactionRequest(
    const google::protobuf::Any& any) {
  pb::sql::ActionEndTransactionRequest command;
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack ActionEndTransactionRequest");
  }

  ActionEndTransactionRequest result;
  result.transaction_id = command.transaction_id();
  switch (command.action()) {
    case pb::sql::ActionEndTransactionRequest::END_TRANSACTION_UNSPECIFIED:
      return Status::Invalid(
          "ActionEndTransactionRequest.action was END_TRANSACTION_UNSPECIFIED");
    case pb::sql::ActionEndTransactionRequest::END_TRANSACTION_COMMIT:
      result.action = ActionEndTransactionRequest::kCommit;
      break;
    case pb::sql::ActionEndTransactionRequest::END_TRANSACTION_ROLLBACK:
      result.action = ActionEndTransactionRequest::kRollback;
      break;
    default:
      return Status::Invalid("Unknown value for ActionEndTransactionRequest.action: ",
                             command.action());
  }
  return result;
}

arrow::Result<Result> PackActionResult(const google::protobuf::Message& message) {
  google::protobuf::Any any;
  if (!any.PackFrom(message)) {
    return Status::IOError("Failed to pack ", message.GetTypeName());
  }

  std::string buffer;
  if (!any.SerializeToString(&buffer)) {
    return Status::IOError("Failed to serialize packed ", message.GetTypeName());
  }
  return Result{Buffer::FromString(std::move(buffer))};
}

arrow::Result<Result> PackActionResult(ActionBeginSavepointResult result) {
  pb::sql::ActionBeginSavepointResult pb_result;
  pb_result.set_savepoint_id(std::move(result.savepoint_id));
  return PackActionResult(pb_result);
}

arrow::Result<Result> PackActionResult(ActionBeginTransactionResult result) {
  pb::sql::ActionBeginTransactionResult pb_result;
  pb_result.set_transaction_id(std::move(result.transaction_id));
  return PackActionResult(pb_result);
}

arrow::Result<Result> PackActionResult(CancelResult result) {
  pb::sql::ActionCancelQueryResult pb_result;
  switch (result) {
    case CancelResult::kUnspecified:
      pb_result.set_result(pb::sql::ActionCancelQueryResult::CANCEL_RESULT_UNSPECIFIED);
      break;
    case CancelResult::kCancelled:
      pb_result.set_result(pb::sql::ActionCancelQueryResult::CANCEL_RESULT_CANCELLED);
      break;
    case CancelResult::kCancelling:
      pb_result.set_result(pb::sql::ActionCancelQueryResult::CANCEL_RESULT_CANCELLING);
      break;
    case CancelResult::kNotCancellable:
      pb_result.set_result(
          pb::sql::ActionCancelQueryResult::CANCEL_RESULT_NOT_CANCELLABLE);
      break;
  }
  return PackActionResult(pb_result);
}

arrow::Result<Result> PackActionResult(ActionCreatePreparedStatementResult result) {
  pb::sql::ActionCreatePreparedStatementResult pb_result;
  pb_result.set_prepared_statement_handle(std::move(result.prepared_statement_handle));
  if (result.dataset_schema != nullptr) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> serialized,
                          ipc::SerializeSchema(*result.dataset_schema));
    pb_result.set_dataset_schema(reinterpret_cast<const char*>(serialized->data()),
                                 serialized->size());
  }
  if (result.parameter_schema != nullptr) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Buffer> serialized,
                          ipc::SerializeSchema(*result.parameter_schema));
    pb_result.set_parameter_schema(reinterpret_cast<const char*>(serialized->data()),
                                   serialized->size());
  }

  return PackActionResult(pb_result);
}

}  // namespace

arrow::Result<StatementQueryTicket> StatementQueryTicket::Deserialize(
    std::string_view serialized) {
  pb::sql::TicketStatementQuery command;
  google::protobuf::Any any;
  if (!any.ParseFromArray(serialized.data(), static_cast<int>(serialized.size()))) {
    return Status::Invalid("Unable to parse ticket");
  }
  if (!any.UnpackTo(&command)) {
    return Status::Invalid("Unable to unpack TicketStatementQuery");
  }
  StatementQueryTicket result;
  result.statement_handle = command.statement_handle();
  return result;
}

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
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoStatement(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandStatementSubstraitPlan>()) {
    ARROW_ASSIGN_OR_RAISE(StatementSubstraitPlan internal_command,
                          ParseCommandStatementSubstraitPlan(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoSubstraitPlan(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    ARROW_ASSIGN_OR_RAISE(
        *info, GetFlightInfoPreparedStatement(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    ARROW_ASSIGN_OR_RAISE(*info, GetFlightInfoCatalogs(context, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetDbSchemas>()) {
    ARROW_ASSIGN_OR_RAISE(GetDbSchemas internal_command, ParseCommandGetDbSchemas(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoSchemas(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    ARROW_ASSIGN_OR_RAISE(GetTables command, ParseCommandGetTables(any));
    ARROW_ASSIGN_OR_RAISE(*info, GetFlightInfoTables(context, command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    ARROW_ASSIGN_OR_RAISE(*info, GetFlightInfoTableTypes(context, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetXdbcTypeInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetXdbcTypeInfo internal_command,
                          ParseCommandGetXdbcTypeInfo(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoXdbcTypeInfo(context, internal_command, request))
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetSqlInfo internal_command,
                          ParseCommandGetSqlInfo(any, sql_info_id_to_result_));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoSqlInfo(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetPrimaryKeys internal_command,
                          ParseCommandGetPrimaryKeys(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoPrimaryKeys(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetExportedKeys internal_command,
                          ParseCommandGetExportedKeys(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoExportedKeys(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetImportedKeys internal_command,
                          ParseCommandGetImportedKeys(any));
    ARROW_ASSIGN_OR_RAISE(*info,
                          GetFlightInfoImportedKeys(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    ARROW_ASSIGN_OR_RAISE(GetCrossReference internal_command,
                          ParseCommandGetCrossReference(any));
    ARROW_ASSIGN_OR_RAISE(
        *info, GetFlightInfoCrossReference(context, internal_command, request));
    return Status::OK();
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::GetSchema(const ServerCallContext& context,
                                      const FlightDescriptor& request,
                                      std::unique_ptr<SchemaResult>* schema) {
  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command");
  }

  if (any.Is<pb::sql::CommandStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(StatementQuery internal_command,
                          ParseCommandStatementQuery(any));
    ARROW_ASSIGN_OR_RAISE(*schema,
                          GetSchemaStatement(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandStatementSubstraitPlan>()) {
    ARROW_ASSIGN_OR_RAISE(StatementSubstraitPlan internal_command,
                          ParseCommandStatementSubstraitPlan(any));
    ARROW_ASSIGN_OR_RAISE(*schema,
                          GetSchemaSubstraitPlan(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    ARROW_ASSIGN_OR_RAISE(*schema,
                          GetSchemaPreparedStatement(context, internal_command, request));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*SqlSchema::GetCatalogsSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    ARROW_ASSIGN_OR_RAISE(*schema,
                          SchemaResult::Make(*SqlSchema::GetCrossReferenceSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetDbSchemas>()) {
    ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*SqlSchema::GetDbSchemasSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(*schema,
                          SchemaResult::Make(*SqlSchema::GetExportedKeysSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(*schema,
                          SchemaResult::Make(*SqlSchema::GetImportedKeysSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    ARROW_ASSIGN_OR_RAISE(*schema,
                          SchemaResult::Make(*SqlSchema::GetPrimaryKeysSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*SqlSchema::GetSqlInfoSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    ARROW_ASSIGN_OR_RAISE(GetTables command, ParseCommandGetTables(any));
    if (command.include_schema) {
      ARROW_ASSIGN_OR_RAISE(
          *schema, SchemaResult::Make(*SqlSchema::GetTablesSchemaWithIncludedSchema()));
    } else {
      ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*SqlSchema::GetTablesSchema()));
    }
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    ARROW_ASSIGN_OR_RAISE(*schema, SchemaResult::Make(*SqlSchema::GetTableTypesSchema()));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetXdbcTypeInfo>()) {
    ARROW_ASSIGN_OR_RAISE(*schema,
                          SchemaResult::Make(*SqlSchema::GetXdbcTypeInfoSchema()));
    return Status::OK();
  }

  return Status::NotImplemented("Command not recognized: ", any.type_url());
}

Status FlightSqlServerBase::DoGet(const ServerCallContext& context, const Ticket& request,
                                  std::unique_ptr<FlightDataStream>* stream) {
  google::protobuf::Any any;
  if (!any.ParseFromArray(request.ticket.data(),
                          static_cast<int>(request.ticket.size()))) {
    return Status::Invalid("Unable to parse ticket");
  }

  if (any.Is<pb::sql::TicketStatementQuery>()) {
    pb::sql::TicketStatementQuery command;
    if (!any.UnpackTo(&command)) {
      return Status::Invalid("Unable to unpack TicketStatementQuery");
    }
    StatementQueryTicket result;
    result.statement_handle = command.statement_handle();
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetStatement(context, result));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandPreparedStatementQuery>()) {
    ARROW_ASSIGN_OR_RAISE(PreparedStatementQuery internal_command,
                          ParseCommandPreparedStatementQuery(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetPreparedStatement(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCatalogs>()) {
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetCatalogs(context));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetDbSchemas>()) {
    ARROW_ASSIGN_OR_RAISE(GetDbSchemas internal_command, ParseCommandGetDbSchemas(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetDbSchemas(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTables>()) {
    ARROW_ASSIGN_OR_RAISE(GetTables command, ParseCommandGetTables(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetTables(context, command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetTableTypes>()) {
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetTableTypes(context));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetXdbcTypeInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetXdbcTypeInfo command, ParseCommandGetXdbcTypeInfo(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetXdbcTypeInfo(context, command))
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetSqlInfo>()) {
    ARROW_ASSIGN_OR_RAISE(GetSqlInfo internal_command,
                          ParseCommandGetSqlInfo(any, sql_info_id_to_result_));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetSqlInfo(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetPrimaryKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetPrimaryKeys internal_command,
                          ParseCommandGetPrimaryKeys(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetPrimaryKeys(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetExportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetExportedKeys internal_command,
                          ParseCommandGetExportedKeys(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetExportedKeys(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetImportedKeys>()) {
    ARROW_ASSIGN_OR_RAISE(GetImportedKeys internal_command,
                          ParseCommandGetImportedKeys(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetImportedKeys(context, internal_command));
    return Status::OK();
  } else if (any.Is<pb::sql::CommandGetCrossReference>()) {
    ARROW_ASSIGN_OR_RAISE(GetCrossReference internal_command,
                          ParseCommandGetCrossReference(any));
    ARROW_ASSIGN_OR_RAISE(*stream, DoGetCrossReference(context, internal_command));
    return Status::OK();
  }

  return Status::Invalid("The defined request is invalid.");
}

Status FlightSqlServerBase::DoPut(const ServerCallContext& context,
                                  std::unique_ptr<FlightMessageReader> reader,
                                  std::unique_ptr<FlightMetadataWriter> writer) {
  const FlightDescriptor& request = reader->descriptor();

  google::protobuf::Any any;
  if (!any.ParseFromArray(request.cmd.data(), static_cast<int>(request.cmd.size()))) {
    return Status::Invalid("Unable to parse command");
  }

  if (any.Is<pb::sql::CommandStatementUpdate>()) {
    ARROW_ASSIGN_OR_RAISE(StatementUpdate internal_command,
                          ParseCommandStatementUpdate(any));
    ARROW_ASSIGN_OR_RAISE(auto record_count,
                          DoPutCommandStatementUpdate(context, internal_command))

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const auto buffer = Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));

    return Status::OK();
  } else if (any.Is<pb::sql::CommandStatementSubstraitPlan>()) {
    ARROW_ASSIGN_OR_RAISE(StatementSubstraitPlan internal_command,
                          ParseCommandStatementSubstraitPlan(any));
    ARROW_ASSIGN_OR_RAISE(auto record_count,
                          DoPutCommandSubstraitPlan(context, internal_command));

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const auto buffer = Buffer::FromString(result.SerializeAsString());
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
    ARROW_ASSIGN_OR_RAISE(
        auto record_count,
        DoPutPreparedStatementUpdate(context, internal_command, reader.get()));

    pb::sql::DoPutUpdateResult result;
    result.set_record_count(record_count);

    const auto buffer = Buffer::FromString(result.SerializeAsString());
    ARROW_RETURN_NOT_OK(writer->WriteMetadata(*buffer));
    return Status::OK();
  }

  return Status::NotImplemented("Command not recognized: ", any.type_url());
}

Status FlightSqlServerBase::ListActions(const ServerCallContext& context,
                                        std::vector<ActionType>* actions) {
  *actions = {
      FlightSqlServerBase::kBeginSavepointActionType,
      FlightSqlServerBase::kBeginTransactionActionType,
      FlightSqlServerBase::kCancelQueryActionType,
      FlightSqlServerBase::kCreatePreparedStatementActionType,
      FlightSqlServerBase::kCreatePreparedSubstraitPlanActionType,
      FlightSqlServerBase::kClosePreparedStatementActionType,
      FlightSqlServerBase::kEndSavepointActionType,
      FlightSqlServerBase::kEndTransactionActionType,
  };
  return Status::OK();
}

Status FlightSqlServerBase::DoAction(const ServerCallContext& context,
                                     const Action& action,
                                     std::unique_ptr<ResultStream>* result_stream) {
  google::protobuf::Any any;
  if (!any.ParseFromArray(action.body->data(), static_cast<int>(action.body->size()))) {
    return Status::Invalid("Unable to parse action");
  }

  std::vector<Result> results;
  if (action.type == FlightSqlServerBase::kBeginSavepointActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionBeginSavepointRequest internal_command,
                          ParseActionBeginSavepointRequest(any));
    ARROW_ASSIGN_OR_RAISE(ActionBeginSavepointResult result,
                          BeginSavepoint(context, internal_command));
    ARROW_ASSIGN_OR_RAISE(Result packed_result, PackActionResult(std::move(result)));

    results.push_back(std::move(packed_result));
  } else if (action.type == FlightSqlServerBase::kBeginTransactionActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionBeginTransactionRequest internal_command,
                          ParseActionBeginTransactionRequest(any));
    ARROW_ASSIGN_OR_RAISE(ActionBeginTransactionResult result,
                          BeginTransaction(context, internal_command));
    ARROW_ASSIGN_OR_RAISE(Result packed_result, PackActionResult(std::move(result)));

    results.push_back(std::move(packed_result));
  } else if (action.type == FlightSqlServerBase::kCancelQueryActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionCancelQueryRequest internal_command,
                          ParseActionCancelQueryRequest(any));
    ARROW_ASSIGN_OR_RAISE(CancelResult result, CancelQuery(context, internal_command));
    ARROW_ASSIGN_OR_RAISE(Result packed_result, PackActionResult(result));

    results.push_back(std::move(packed_result));
  } else if (action.type ==
             FlightSqlServerBase::kCreatePreparedStatementActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionCreatePreparedStatementRequest internal_command,
                          ParseActionCreatePreparedStatementRequest(any));
    ARROW_ASSIGN_OR_RAISE(ActionCreatePreparedStatementResult result,
                          CreatePreparedStatement(context, internal_command));
    ARROW_ASSIGN_OR_RAISE(Result packed_result, PackActionResult(std::move(result)));

    results.push_back(std::move(packed_result));
  } else if (action.type ==
             FlightSqlServerBase::kCreatePreparedSubstraitPlanActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionCreatePreparedSubstraitPlanRequest internal_command,
                          ParseActionCreatePreparedSubstraitPlanRequest(any));
    ARROW_ASSIGN_OR_RAISE(ActionCreatePreparedStatementResult result,
                          CreatePreparedSubstraitPlan(context, internal_command));
    ARROW_ASSIGN_OR_RAISE(Result packed_result, PackActionResult(std::move(result)));

    results.push_back(std::move(packed_result));
  } else if (action.type == FlightSqlServerBase::kClosePreparedStatementActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionClosePreparedStatementRequest internal_command,
                          ParseActionClosePreparedStatementRequest(any));
    ARROW_RETURN_NOT_OK(ClosePreparedStatement(context, internal_command));
  } else if (action.type == FlightSqlServerBase::kEndSavepointActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionEndSavepointRequest internal_command,
                          ParseActionEndSavepointRequest(any));
    ARROW_RETURN_NOT_OK(EndSavepoint(context, internal_command));
  } else if (action.type == FlightSqlServerBase::kEndTransactionActionType.type) {
    ARROW_ASSIGN_OR_RAISE(ActionEndTransactionRequest internal_command,
                          ParseActionEndTransactionRequest(any));
    ARROW_RETURN_NOT_OK(EndTransaction(context, internal_command));
  } else {
    return Status::NotImplemented("Action not implemented: ", action.type);
  }
  *result_stream = std::make_unique<SimpleResultStream>(std::move(results));
  return Status::OK();
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoCatalogs(
    const ServerCallContext& context, const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoCatalogs not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetCatalogs(
    const ServerCallContext& context) {
  return Status::NotImplemented("DoGetCatalogs not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoStatement(
    const ServerCallContext& context, const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoStatement not implemented");
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlServerBase::GetSchemaStatement(
    const ServerCallContext& context, const StatementQuery& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetSchemaStatement not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>>
FlightSqlServerBase::GetFlightInfoSubstraitPlan(const ServerCallContext& context,
                                                const StatementSubstraitPlan& command,
                                                const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoSubstraitPlan not implemented");
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlServerBase::GetSchemaSubstraitPlan(
    const ServerCallContext& context, const StatementSubstraitPlan& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetSchemaSubstraitPlan not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetStatement(
    const ServerCallContext& context, const StatementQueryTicket& command) {
  return Status::NotImplemented("DoGetStatement not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>>
FlightSqlServerBase::GetFlightInfoPreparedStatement(const ServerCallContext& context,
                                                    const PreparedStatementQuery& command,
                                                    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoPreparedStatement not implemented");
}

arrow::Result<std::unique_ptr<SchemaResult>>
FlightSqlServerBase::GetSchemaPreparedStatement(const ServerCallContext& context,
                                                const PreparedStatementQuery& command,
                                                const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetSchemaPreparedStatement not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>>
FlightSqlServerBase::DoGetPreparedStatement(const ServerCallContext& context,
                                            const PreparedStatementQuery& command) {
  return Status::NotImplemented("DoGetPreparedStatement not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoSqlInfo(
    const ServerCallContext& context, const GetSqlInfo& command,
    const FlightDescriptor& descriptor) {
  if (sql_info_id_to_result_.empty()) {
    return Status::KeyError("No SQL information available.");
  }

  std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
  ARROW_ASSIGN_OR_RAISE(
      auto result, FlightInfo::Make(*SqlSchema::GetSqlInfoSchema(), descriptor, endpoints,
                                    -1, -1, false))

  return std::make_unique<FlightInfo>(result);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoXdbcTypeInfo(
    const ServerCallContext& context, const GetXdbcTypeInfo& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoXdbcTypeInfo not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetXdbcTypeInfo(
    const ServerCallContext& context, const GetXdbcTypeInfo& command) {
  return Status::NotImplemented("DoGetXdbcTypeInfo not implemented");
}

void FlightSqlServerBase::RegisterSqlInfo(int32_t id, const SqlInfoResult& result) {
  sql_info_id_to_result_[id] = result;
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetSqlInfo(
    const ServerCallContext& context, const GetSqlInfo& command) {
  MemoryPool* memory_pool = default_memory_pool();
  UInt32Builder name_field_builder(memory_pool);
  std::unique_ptr<ArrayBuilder> value_field_builder;
  const auto& value_field_type = checked_pointer_cast<DenseUnionType>(
      SqlSchema::GetSqlInfoSchema()->fields()[1]->type());
  ARROW_RETURN_NOT_OK(MakeBuilder(memory_pool, value_field_type, &value_field_builder));

  internal::SqlInfoResultAppender sql_info_result_appender(
      checked_cast<DenseUnionBuilder*>(value_field_builder.get()));

  // Populate both name_field_builder and value_field_builder for each element
  // on command.info.
  // value_field_builder is populated differently depending on the data type (as it is
  // a DenseUnionBuilder). The population for each data type is implemented on
  // internal::SqlInfoResultAppender.
  for (const auto& info : command.info) {
    const auto it = sql_info_id_to_result_.find(info);
    if (it == sql_info_id_to_result_.end()) {
      return Status::KeyError("No information for SQL info number ", info);
    }
    ARROW_RETURN_NOT_OK(name_field_builder.Append(info));
    ARROW_RETURN_NOT_OK(std::visit(sql_info_result_appender, it->second));
  }

  std::shared_ptr<Array> name;
  ARROW_RETURN_NOT_OK(name_field_builder.Finish(&name));
  std::shared_ptr<Array> value;
  ARROW_RETURN_NOT_OK(value_field_builder->Finish(&value));

  auto row_count = static_cast<int64_t>(command.info.size());
  const std::shared_ptr<RecordBatch>& batch =
      RecordBatch::Make(SqlSchema::GetSqlInfoSchema(), row_count, {name, value});
  ARROW_ASSIGN_OR_RAISE(const auto reader, RecordBatchReader::Make({batch}));

  return std::make_unique<RecordBatchStream>(reader);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoSchemas(
    const ServerCallContext& context, const GetDbSchemas& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoSchemas not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetDbSchemas(
    const ServerCallContext& context, const GetDbSchemas& command) {
  return Status::NotImplemented("DoGetDbSchemas not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoTables(
    const ServerCallContext& context, const GetTables& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoTables not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetTables(
    const ServerCallContext& context, const GetTables& command) {
  return Status::NotImplemented("DoGetTables not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoTableTypes(
    const ServerCallContext& context, const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoTableTypes not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetTableTypes(
    const ServerCallContext& context) {
  return Status::NotImplemented("DoGetTableTypes not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoPrimaryKeys(
    const ServerCallContext& context, const GetPrimaryKeys& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoPrimaryKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetPrimaryKeys(
    const ServerCallContext& context, const GetPrimaryKeys& command) {
  return Status::NotImplemented("DoGetPrimaryKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoExportedKeys(
    const ServerCallContext& context, const GetExportedKeys& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoExportedKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetExportedKeys(
    const ServerCallContext& context, const GetExportedKeys& command) {
  return Status::NotImplemented("DoGetExportedKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlServerBase::GetFlightInfoImportedKeys(
    const ServerCallContext& context, const GetImportedKeys& command,
    const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoImportedKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetImportedKeys(
    const ServerCallContext& context, const GetImportedKeys& command) {
  return Status::NotImplemented("DoGetImportedKeys not implemented");
}

arrow::Result<std::unique_ptr<FlightInfo>>
FlightSqlServerBase::GetFlightInfoCrossReference(const ServerCallContext& context,
                                                 const GetCrossReference& command,
                                                 const FlightDescriptor& descriptor) {
  return Status::NotImplemented("GetFlightInfoCrossReference not implemented");
}

arrow::Result<std::unique_ptr<FlightDataStream>> FlightSqlServerBase::DoGetCrossReference(
    const ServerCallContext& context, const GetCrossReference& command) {
  return Status::NotImplemented("DoGetCrossReference not implemented");
}

arrow::Result<ActionBeginSavepointResult> FlightSqlServerBase::BeginSavepoint(
    const ServerCallContext& context, const ActionBeginSavepointRequest& request) {
  return Status::NotImplemented("BeginSavepoint not implemented");
}

arrow::Result<ActionBeginTransactionResult> FlightSqlServerBase::BeginTransaction(
    const ServerCallContext& context, const ActionBeginTransactionRequest& request) {
  return Status::NotImplemented("BeginTransaction not implemented");
}

arrow::Result<CancelResult> FlightSqlServerBase::CancelQuery(
    const ServerCallContext& context, const ActionCancelQueryRequest& request) {
  return Status::NotImplemented("CancelQuery not implemented");
}

arrow::Result<ActionCreatePreparedStatementResult>
FlightSqlServerBase::CreatePreparedStatement(
    const ServerCallContext& context,
    const ActionCreatePreparedStatementRequest& request) {
  return Status::NotImplemented("CreatePreparedStatement not implemented");
}

arrow::Result<ActionCreatePreparedStatementResult>
FlightSqlServerBase::CreatePreparedSubstraitPlan(
    const ServerCallContext& context,
    const ActionCreatePreparedSubstraitPlanRequest& request) {
  return Status::NotImplemented("CreatePreparedSubstraitPlan not implemented");
}

Status FlightSqlServerBase::ClosePreparedStatement(
    const ServerCallContext& context,
    const ActionClosePreparedStatementRequest& request) {
  return Status::NotImplemented("ClosePreparedStatement not implemented");
}

Status FlightSqlServerBase::EndSavepoint(const ServerCallContext& context,
                                         const ActionEndSavepointRequest& request) {
  return Status::NotImplemented("EndSavepoint not implemented");
}

Status FlightSqlServerBase::EndTransaction(const ServerCallContext& context,
                                           const ActionEndTransactionRequest& request) {
  return Status::NotImplemented("EndTransaction not implemented");
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
    const ServerCallContext& context, const StatementUpdate& command) {
  return Status::NotImplemented("DoPutCommandStatementUpdate not implemented");
}

arrow::Result<int64_t> FlightSqlServerBase::DoPutCommandSubstraitPlan(
    const ServerCallContext& context, const StatementSubstraitPlan& command) {
  return Status::NotImplemented("DoPutCommandSubstraitPlan not implemented");
}

const std::shared_ptr<Schema>& SqlSchema::GetCatalogsSchema() {
  static std::shared_ptr<Schema> kSchema =
      arrow::schema({field("catalog_name", utf8(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetDbSchemasSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema(
      {field("catalog_name", utf8()), field("db_schema_name", utf8(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetTablesSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema(
      {field("catalog_name", utf8()), field("db_schema_name", utf8()),
       field("table_name", utf8(), false), field("table_type", utf8(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetTablesSchemaWithIncludedSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema(
      {field("catalog_name", utf8()), field("db_schema_name", utf8()),
       field("table_name", utf8(), false), field("table_type", utf8(), false),
       field("table_schema", binary(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetTableTypesSchema() {
  static std::shared_ptr<Schema> kSchema =
      arrow::schema({field("table_type", utf8(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetPrimaryKeysSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema(
      {field("catalog_name", utf8()), field("db_schema_name", utf8()),
       field("table_name", utf8(), false), field("column_name", utf8(), false),
       field("key_sequence", int32(), false), field("key_name", utf8())});
  return kSchema;
}

const std::shared_ptr<Schema>& GetImportedExportedKeysAndCrossReferenceSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema(
      {field("pk_catalog_name", utf8(), true), field("pk_db_schema_name", utf8(), true),
       field("pk_table_name", utf8(), false), field("pk_column_name", utf8(), false),
       field("fk_catalog_name", utf8(), true), field("fk_db_schema_name", utf8(), true),
       field("fk_table_name", utf8(), false), field("fk_column_name", utf8(), false),
       field("key_sequence", int32(), false), field("fk_key_name", utf8(), true),
       field("pk_key_name", utf8(), true), field("update_rule", uint8(), false),
       field("delete_rule", uint8(), false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetImportedKeysSchema() {
  static std::shared_ptr<Schema> kSchema =
      GetImportedExportedKeysAndCrossReferenceSchema();
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetExportedKeysSchema() {
  static std::shared_ptr<Schema> kSchema =
      GetImportedExportedKeysAndCrossReferenceSchema();
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetCrossReferenceSchema() {
  static std::shared_ptr<Schema> kSchema =
      GetImportedExportedKeysAndCrossReferenceSchema();
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetSqlInfoSchema() {
  static std::shared_ptr<Schema> kSchema =
      arrow::schema({field("info_name", uint32(), false),
                     field("value",
                           dense_union({field("string_value", utf8(), false),
                                        field("bool_value", boolean(), false),
                                        field("bigint_value", int64(), false),
                                        field("int32_bitmask", int32(), false),
                                        field("string_list", list(utf8()), false),
                                        field("int32_to_int32_list_map",
                                              map(int32(), list(int32())), false)}),
                           false)});
  return kSchema;
}

const std::shared_ptr<Schema>& SqlSchema::GetXdbcTypeInfoSchema() {
  static std::shared_ptr<Schema> kSchema = arrow::schema({
      field("type_name", utf8(), false),
      field("data_type", int32(), false),
      field("column_size", int32()),
      field("literal_prefix", utf8()),
      field("literal_suffix", utf8()),
      field("create_params", list(field("item", utf8(), false))),
      field("nullable", int32(), false),
      field("case_sensitive", boolean(), false),
      field("searchable", int32(), false),
      field("unsigned_attribute", boolean()),
      field("fixed_prec_scale", boolean(), false),
      field("auto_increment", boolean()),
      field("local_type_name", utf8()),
      field("minimum_scale", int32()),
      field("maximum_scale", int32()),
      field("sql_data_type", int32(), false),
      field("datetime_subcode", int32()),
      field("num_prec_radix", int32()),
      field("interval_precision", int32()),
  });
  return kSchema;
}
}  // namespace sql
}  // namespace flight
}  // namespace arrow

#undef PROPERTY_TO_OPTIONAL
