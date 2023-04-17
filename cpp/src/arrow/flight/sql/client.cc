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

// Platform-specific defines
#include "arrow/flight/client.h"
#include "arrow/flight/platform.h"

#include "arrow/flight/sql/client.h"

#include <google/protobuf/any.pb.h>

#include "arrow/buffer.h"
#include "arrow/flight/sql/protocol_internal.h"
#include "arrow/flight/types.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/reader.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"

namespace flight_sql_pb = arrow::flight::protocol::sql;

namespace arrow {
namespace flight {
namespace sql {

namespace {
arrow::Result<FlightDescriptor> GetFlightDescriptorForCommand(
    const google::protobuf::Message& command) {
  google::protobuf::Any any;
  if (!any.PackFrom(command)) {
    return Status::SerializationError("Failed to pack ", command.GetTypeName());
  }

  std::string buf;
  if (!any.SerializeToString(&buf)) {
    return Status::SerializationError("Failed to serialize ", command.GetTypeName());
  }
  return FlightDescriptor::Command(buf);
}

arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    FlightSqlClient* client, const FlightCallOptions& options,
    const google::protobuf::Message& command) {
  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));
  return client->GetFlightInfo(options, descriptor);
}

arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaForCommand(
    FlightSqlClient* client, const FlightCallOptions& options,
    const google::protobuf::Message& command) {
  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));
  return client->GetSchema(options, descriptor);
}

::arrow::Result<Action> PackAction(const std::string& action_type,
                                   const google::protobuf::Message& message) {
  google::protobuf::Any any;
  if (!any.PackFrom(message)) {
    return Status::SerializationError("Could not pack ", message.GetTypeName(),
                                      " into Any");
  }

  std::string buffer;
  if (!any.SerializeToString(&buffer)) {
    return Status::SerializationError("Could not serialize packed ",
                                      message.GetTypeName());
  }

  Action action;
  action.type = action_type;
  action.body = Buffer::FromString(std::move(buffer));
  return action;
}

void SetPlan(const SubstraitPlan& plan, flight_sql_pb::SubstraitPlan* pb_plan) {
  pb_plan->set_plan(plan.plan);
  pb_plan->set_version(plan.version);
}

Status ReadResult(ResultStream* results, google::protobuf::Message* message) {
  ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
  if (!result) {
    return Status::IOError("Server did not return a result for ", message->GetTypeName());
  }

  google::protobuf::Any container;
  if (!container.ParseFromArray(result->body->data(),
                                static_cast<int>(result->body->size()))) {
    return Status::IOError("Unable to parse Any (expecting ", message->GetTypeName(),
                           ")");
  }
  if (!container.UnpackTo(message)) {
    return Status::IOError("Unable to unpack Any (expecting ", message->GetTypeName(),
                           ")");
  }
  return Status::OK();
}

Status DrainResultStream(ResultStream* results) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
    if (!result) break;
  }
  return Status::OK();
}
}  // namespace

const Transaction& no_transaction() {
  static Transaction kInvalidTransaction("");
  return kInvalidTransaction;
}

FlightSqlClient::FlightSqlClient(std::shared_ptr<FlightClient> client)
    : impl_(std::move(client)) {}

PreparedStatement::PreparedStatement(FlightSqlClient* client, std::string handle,
                                     std::shared_ptr<Schema> dataset_schema,
                                     std::shared_ptr<Schema> parameter_schema)
    : client_(client),
      handle_(std::move(handle)),
      dataset_schema_(std::move(dataset_schema)),
      parameter_schema_(std::move(parameter_schema)),
      is_closed_(false) {}

PreparedStatement::~PreparedStatement() {
  if (IsClosed()) return;

  const Status status = Close();
  if (!status.ok()) {
    ARROW_LOG(ERROR) << "Failed to delete PreparedStatement: " << status.ToString();
  }
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::Execute(
    const FlightCallOptions& options, const std::string& query,
    const Transaction& transaction) {
  flight_sql_pb::CommandStatementQuery command;
  command.set_query(query);
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetExecuteSchema(
    const FlightCallOptions& options, const std::string& query,
    const Transaction& transaction) {
  flight_sql_pb::CommandStatementQuery command;
  command.set_query(query);
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::ExecuteSubstrait(
    const FlightCallOptions& options, const SubstraitPlan& plan,
    const Transaction& transaction) {
  flight_sql_pb::CommandStatementSubstraitPlan command;
  SetPlan(plan, command.mutable_plan());
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetExecuteSubstraitSchema(
    const FlightCallOptions& options, const SubstraitPlan& plan,
    const Transaction& transaction) {
  flight_sql_pb::CommandStatementSubstraitPlan command;
  SetPlan(plan, command.mutable_plan());
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<int64_t> FlightSqlClient::ExecuteUpdate(const FlightCallOptions& options,
                                                      const std::string& query,
                                                      const Transaction& transaction) {
  flight_sql_pb::CommandStatementUpdate command;
  command.set_query(query);
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }

  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(DoPut(options, descriptor, arrow::schema({}), &writer, &reader));
  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(writer->Close());

  if (!metadata) return Status::IOError("Server did not send a response");

  flight_sql_pb::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult");
  }

  return result.record_count();
}

arrow::Result<int64_t> FlightSqlClient::ExecuteSubstraitUpdate(
    const FlightCallOptions& options, const SubstraitPlan& plan,
    const Transaction& transaction) {
  flight_sql_pb::CommandStatementSubstraitPlan command;
  SetPlan(plan, command.mutable_plan());
  if (transaction.is_valid()) {
    command.set_transaction_id(transaction.transaction_id());
  }

  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(DoPut(options, descriptor, arrow::schema({}), &writer, &reader));

  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(writer->Close());

  flight_sql_pb::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult");
  }

  return result.record_count();
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetCatalogs(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetCatalogs command;
  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetCatalogsSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetCatalogs command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetDbSchemas(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* db_schema_filter_pattern) {
  flight_sql_pb::CommandGetDbSchemas command;
  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }
  if (db_schema_filter_pattern != NULLPTR) {
    command.set_db_schema_filter_pattern(*db_schema_filter_pattern);
  }

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetDbSchemasSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetDbSchemas command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetTables(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* db_schema_filter_pattern, const std::string* table_filter_pattern,
    bool include_schema, const std::vector<std::string>* table_types) {
  flight_sql_pb::CommandGetTables command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (db_schema_filter_pattern != NULLPTR) {
    command.set_db_schema_filter_pattern(*db_schema_filter_pattern);
  }

  if (table_filter_pattern != NULLPTR) {
    command.set_table_name_filter_pattern(*table_filter_pattern);
  }

  command.set_include_schema(include_schema);

  if (table_types != NULLPTR) {
    for (const std::string& table_type : *table_types) {
      command.add_table_types(table_type);
    }
  }

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetTablesSchema(
    const FlightCallOptions& options, bool include_schema) {
  flight_sql_pb::CommandGetTables command;
  command.set_include_schema(include_schema);
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetPrimaryKeys(
    const FlightCallOptions& options, const TableRef& table_ref) {
  flight_sql_pb::CommandGetPrimaryKeys command;

  if (table_ref.catalog.has_value()) {
    command.set_catalog(table_ref.catalog.value());
  }

  if (table_ref.db_schema.has_value()) {
    command.set_db_schema(table_ref.db_schema.value());
  }

  command.set_table(table_ref.table);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetPrimaryKeysSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetPrimaryKeys command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetExportedKeys(
    const FlightCallOptions& options, const TableRef& table_ref) {
  flight_sql_pb::CommandGetExportedKeys command;

  if (table_ref.catalog.has_value()) {
    command.set_catalog(table_ref.catalog.value());
  }

  if (table_ref.db_schema.has_value()) {
    command.set_db_schema(table_ref.db_schema.value());
  }

  command.set_table(table_ref.table);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetExportedKeysSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetExportedKeys command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetImportedKeys(
    const FlightCallOptions& options, const TableRef& table_ref) {
  flight_sql_pb::CommandGetImportedKeys command;

  if (table_ref.catalog.has_value()) {
    command.set_catalog(table_ref.catalog.value());
  }

  if (table_ref.db_schema.has_value()) {
    command.set_db_schema(table_ref.db_schema.value());
  }

  command.set_table(table_ref.table);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetImportedKeysSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetImportedKeys command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetCrossReference(
    const FlightCallOptions& options, const TableRef& pk_table_ref,
    const TableRef& fk_table_ref) {
  flight_sql_pb::CommandGetCrossReference command;

  if (pk_table_ref.catalog.has_value()) {
    command.set_pk_catalog(pk_table_ref.catalog.value());
  }
  if (pk_table_ref.db_schema.has_value()) {
    command.set_pk_db_schema(pk_table_ref.db_schema.value());
  }
  command.set_pk_table(pk_table_ref.table);

  if (fk_table_ref.catalog.has_value()) {
    command.set_fk_catalog(fk_table_ref.catalog.value());
  }
  if (fk_table_ref.db_schema.has_value()) {
    command.set_fk_db_schema(fk_table_ref.db_schema.value());
  }
  command.set_fk_table(fk_table_ref.table);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetCrossReferenceSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetCrossReference command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetTableTypes(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetTableTypes command;

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetTableTypesSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetTableTypes command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetXdbcTypeInfo(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetXdbcTypeInfo command;

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetXdbcTypeInfo(
    const FlightCallOptions& options, int data_type) {
  flight_sql_pb::CommandGetXdbcTypeInfo command;

  command.set_data_type(data_type);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetXdbcTypeInfoSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetXdbcTypeInfo command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<int>& sql_info) {
  flight_sql_pb::CommandGetSqlInfo command;
  for (const int& info : sql_info) command.add_info(info);

  return GetFlightInfoForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<SchemaResult>> FlightSqlClient::GetSqlInfoSchema(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetSqlInfo command;
  return GetSchemaForCommand(this, options, command);
}

arrow::Result<std::unique_ptr<FlightStreamReader>> FlightSqlClient::DoGet(
    const FlightCallOptions& options, const Ticket& ticket) {
  std::unique_ptr<FlightStreamReader> stream;
  ARROW_RETURN_NOT_OK(DoGet(options, ticket, &stream));

  return std::move(stream);
}

arrow::Result<std::shared_ptr<PreparedStatement>> FlightSqlClient::Prepare(
    const FlightCallOptions& options, const std::string& query,
    const Transaction& transaction) {
  flight_sql_pb::ActionCreatePreparedStatementRequest request;
  request.set_query(query);
  if (transaction.is_valid()) {
    request.set_transaction_id(transaction.transaction_id());
  }

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("CreatePreparedStatement", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  return PreparedStatement::ParseResponse(this, std::move(results));
}

arrow::Result<std::shared_ptr<PreparedStatement>> FlightSqlClient::PrepareSubstrait(
    const FlightCallOptions& options, const SubstraitPlan& plan,
    const Transaction& transaction) {
  flight_sql_pb::ActionCreatePreparedSubstraitPlanRequest request;
  SetPlan(plan, request.mutable_plan());
  if (transaction.is_valid()) {
    request.set_transaction_id(transaction.transaction_id());
  }

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("CreatePreparedSubstraitPlan", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  return PreparedStatement::ParseResponse(this, std::move(results));
}

arrow::Result<std::shared_ptr<PreparedStatement>> PreparedStatement::ParseResponse(
    FlightSqlClient* client, std::unique_ptr<ResultStream> results) {
  flight_sql_pb::ActionCreatePreparedStatementResult prepared_statement_result;
  ARROW_RETURN_NOT_OK(ReadResult(results.get(), &prepared_statement_result));

  const std::string& serialized_dataset_schema =
      prepared_statement_result.dataset_schema();
  const std::string& serialized_parameter_schema =
      prepared_statement_result.parameter_schema();

  std::shared_ptr<Schema> dataset_schema;
  if (!serialized_dataset_schema.empty()) {
    io::BufferReader dataset_schema_reader(serialized_dataset_schema);
    ipc::DictionaryMemo in_memo;
    ARROW_ASSIGN_OR_RAISE(dataset_schema, ReadSchema(&dataset_schema_reader, &in_memo));
  }
  std::shared_ptr<Schema> parameter_schema;
  if (!serialized_parameter_schema.empty()) {
    io::BufferReader parameter_schema_reader(serialized_parameter_schema);
    ipc::DictionaryMemo in_memo;
    ARROW_ASSIGN_OR_RAISE(parameter_schema,
                          ReadSchema(&parameter_schema_reader, &in_memo));
  }
  auto handle = prepared_statement_result.prepared_statement_handle();

  return std::make_shared<PreparedStatement>(client, handle, dataset_schema,
                                             parameter_schema);
}

arrow::Result<std::shared_ptr<Buffer>> BindParameters(FlightClient* client,
                                                      const FlightCallOptions& options,
                                                      const FlightDescriptor& descriptor,
                                                      RecordBatchReader* params) {
  ARROW_ASSIGN_OR_RAISE(auto stream,
                        client->DoPut(options, descriptor, params->schema()));
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto batch, params->Next());
    if (!batch) break;
    ARROW_RETURN_NOT_OK(stream.writer->WriteRecordBatch(*batch));
  }
  ARROW_RETURN_NOT_OK(stream.writer->DoneWriting());
  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(stream.reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(stream.writer->Close());
  return metadata;
}

arrow::Result<std::unique_ptr<FlightInfo>> PreparedStatement::Execute(
    const FlightCallOptions& options) {
  if (is_closed_) {
    return Status::Invalid("Statement with handle '", handle_, "' already closed");
  }

  flight_sql_pb::CommandPreparedStatementQuery command;
  command.set_prepared_statement_handle(handle_);
  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));
  if (parameter_binding_) {
    ARROW_ASSIGN_OR_RAISE(auto metadata,
                          BindParameters(client_->impl_.get(), options, descriptor,
                                         parameter_binding_.get()));
  }
  ARROW_ASSIGN_OR_RAISE(auto flight_info, client_->GetFlightInfo(options, descriptor));
  return std::move(flight_info);
}

arrow::Result<int64_t> PreparedStatement::ExecuteUpdate(
    const FlightCallOptions& options) {
  if (is_closed_) {
    return Status::Invalid("Statement with handle '", handle_, "' already closed");
  }

  flight_sql_pb::CommandPreparedStatementUpdate command;
  command.set_prepared_statement_handle(handle_);
  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));
  std::shared_ptr<Buffer> metadata;
  if (parameter_binding_) {
    ARROW_ASSIGN_OR_RAISE(metadata, BindParameters(client_->impl_.get(), options,
                                                   descriptor, parameter_binding_.get()));
  } else {
    const std::shared_ptr<Schema> schema = arrow::schema({});
    ARROW_ASSIGN_OR_RAISE(auto params, RecordBatchReader::Make({}, schema));
    ARROW_ASSIGN_OR_RAISE(metadata, BindParameters(client_->impl_.get(), options,
                                                   descriptor, params.get()));
  }
  if (!metadata) {
    return Status::IOError("Server did not send a response to ", command.GetTypeName());
  }
  flight_sql_pb::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult object.");
  }

  return result.record_count();
}

Status PreparedStatement::SetParameters(std::shared_ptr<RecordBatch> parameter_binding) {
  ARROW_ASSIGN_OR_RAISE(parameter_binding_,
                        RecordBatchReader::Make({std::move(parameter_binding)}));
  return Status::OK();
}

Status PreparedStatement::SetParameters(
    std::shared_ptr<RecordBatchReader> parameter_binding) {
  parameter_binding_ = std::move(parameter_binding);

  return Status::OK();
}

bool PreparedStatement::IsClosed() const { return is_closed_; }

const std::shared_ptr<Schema>& PreparedStatement::dataset_schema() const {
  return dataset_schema_;
}

const std::shared_ptr<Schema>& PreparedStatement::parameter_schema() const {
  return parameter_schema_;
}

arrow::Result<std::unique_ptr<SchemaResult>> PreparedStatement::GetSchema(
    const FlightCallOptions& options) {
  if (is_closed_) {
    return Status::Invalid("Statement with handle '", handle_, "' already closed");
  }

  flight_sql_pb::CommandPreparedStatementQuery command;
  command.set_prepared_statement_handle(handle_);
  ARROW_ASSIGN_OR_RAISE(FlightDescriptor descriptor,
                        GetFlightDescriptorForCommand(command));
  return client_->GetSchema(options, descriptor);
}

Status PreparedStatement::Close(const FlightCallOptions& options) {
  if (is_closed_) {
    return Status::Invalid("Statement with handle '", handle_, "' already closed");
  }

  flight_sql_pb::ActionClosePreparedStatementRequest request;
  request.set_prepared_statement_handle(handle_);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("ClosePreparedStatement", request));
  ARROW_RETURN_NOT_OK(client_->DoAction(options, action, &results));
  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));

  is_closed_ = true;
  return Status::OK();
}

::arrow::Result<Transaction> FlightSqlClient::BeginTransaction(
    const FlightCallOptions& options) {
  flight_sql_pb::ActionBeginTransactionRequest request;

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("BeginTransaction", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  flight_sql_pb::ActionBeginTransactionResult transaction;
  ARROW_RETURN_NOT_OK(ReadResult(results.get(), &transaction));
  if (transaction.transaction_id().empty()) {
    return Status::Invalid("Server returned an empty transaction ID");
  }

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Transaction(transaction.transaction_id());
}

::arrow::Result<Savepoint> FlightSqlClient::BeginSavepoint(
    const FlightCallOptions& options, const Transaction& transaction,
    const std::string& name) {
  flight_sql_pb::ActionBeginSavepointRequest request;

  if (!transaction.is_valid()) {
    return Status::Invalid("Must provide an active transaction");
  }
  request.set_transaction_id(transaction.transaction_id());
  request.set_name(name);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("BeginSavepoint", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  flight_sql_pb::ActionBeginSavepointResult savepoint;
  ARROW_RETURN_NOT_OK(ReadResult(results.get(), &savepoint));
  if (savepoint.savepoint_id().empty()) {
    return Status::Invalid("Server returned an empty savepoint ID");
  }

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Savepoint(savepoint.savepoint_id());
}

Status FlightSqlClient::Commit(const FlightCallOptions& options,
                               const Transaction& transaction) {
  flight_sql_pb::ActionEndTransactionRequest request;

  if (!transaction.is_valid()) {
    return Status::Invalid("Must provide an active transaction");
  }
  request.set_transaction_id(transaction.transaction_id());
  request.set_action(flight_sql_pb::ActionEndTransactionRequest::END_TRANSACTION_COMMIT);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("EndTransaction", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Status::OK();
}

Status FlightSqlClient::Release(const FlightCallOptions& options,
                                const Savepoint& savepoint) {
  flight_sql_pb::ActionEndSavepointRequest request;

  if (!savepoint.is_valid()) {
    return Status::Invalid("Must provide an active savepoint");
  }
  request.set_savepoint_id(savepoint.savepoint_id());
  request.set_action(flight_sql_pb::ActionEndSavepointRequest::END_SAVEPOINT_RELEASE);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("EndSavepoint", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Status::OK();
}

Status FlightSqlClient::Rollback(const FlightCallOptions& options,
                                 const Transaction& transaction) {
  flight_sql_pb::ActionEndTransactionRequest request;

  if (!transaction.is_valid()) {
    return Status::Invalid("Must provide an active transaction");
  }
  request.set_transaction_id(transaction.transaction_id());
  request.set_action(
      flight_sql_pb::ActionEndTransactionRequest::END_TRANSACTION_ROLLBACK);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("EndTransaction", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Status::OK();
}

Status FlightSqlClient::Rollback(const FlightCallOptions& options,
                                 const Savepoint& savepoint) {
  flight_sql_pb::ActionEndSavepointRequest request;

  if (!savepoint.is_valid()) {
    return Status::Invalid("Must provide an active savepoint");
  }
  request.set_savepoint_id(savepoint.savepoint_id());
  request.set_action(flight_sql_pb::ActionEndSavepointRequest::END_SAVEPOINT_ROLLBACK);

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("EndSavepoint", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  return Status::OK();
}

::arrow::Result<CancelResult> FlightSqlClient::CancelQuery(
    const FlightCallOptions& options, const FlightInfo& info) {
  flight_sql_pb::ActionCancelQueryRequest request;
  ARROW_ASSIGN_OR_RAISE(auto serialized_info, info.SerializeToString());
  request.set_info(std::move(serialized_info));

  std::unique_ptr<ResultStream> results;
  ARROW_ASSIGN_OR_RAISE(auto action, PackAction("CancelQuery", request));
  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  flight_sql_pb::ActionCancelQueryResult result;
  ARROW_RETURN_NOT_OK(ReadResult(results.get(), &result));
  ARROW_RETURN_NOT_OK(DrainResultStream(results.get()));
  switch (result.result()) {
    case flight_sql_pb::ActionCancelQueryResult::CANCEL_RESULT_UNSPECIFIED:
      return CancelResult::kUnspecified;
    case flight_sql_pb::ActionCancelQueryResult::CANCEL_RESULT_CANCELLED:
      return CancelResult::kCancelled;
    case flight_sql_pb::ActionCancelQueryResult::CANCEL_RESULT_CANCELLING:
      return CancelResult::kCancelling;
    case flight_sql_pb::ActionCancelQueryResult::CANCEL_RESULT_NOT_CANCELLABLE:
      return CancelResult::kNotCancellable;
    default:
      break;
  }
  return Status::IOError("Server returned unknown result ", result.result());
}

Status FlightSqlClient::Close() { return impl_->Close(); }

std::ostream& operator<<(std::ostream& os, CancelResult result) {
  switch (result) {
    case CancelResult::kUnspecified:
      os << "CancelResult::kUnspecified";
      break;
    case CancelResult::kCancelled:
      os << "CancelResult::kCancelled";
      break;
    case CancelResult::kCancelling:
      os << "CancelResult::kCancelling";
      break;
    case CancelResult::kNotCancellable:
      os << "CancelResult::kNotCancellable";
      break;
  }
  return os;
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
