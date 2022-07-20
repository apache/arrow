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

FlightSqlClient::FlightSqlClient(std::shared_ptr<FlightClient> client)
    : impl_(std::move(client)) {}

PreparedStatement::PreparedStatement(FlightSqlClient* client, std::string handle,
                                     std::shared_ptr<Schema> dataset_schema,
                                     std::shared_ptr<Schema> parameter_schema,
                                     FlightCallOptions options)
    : client_(client),
      options_(std::move(options)),
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

inline FlightDescriptor GetFlightDescriptorForCommand(
    const google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    FlightSqlClient& client, const FlightCallOptions& options,
    const google::protobuf::Message& command) {
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  ARROW_ASSIGN_OR_RAISE(auto flight_info, client.GetFlightInfo(options, descriptor));
  return std::move(flight_info);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::Execute(
    const FlightCallOptions& options, const std::string& query) {
  flight_sql_pb::CommandStatementQuery command;
  command.set_query(query);

  return GetFlightInfoForCommand(*this, options, command);
}

arrow::Result<int64_t> FlightSqlClient::ExecuteUpdate(const FlightCallOptions& options,
                                                      const std::string& query) {
  flight_sql_pb::CommandStatementUpdate command;
  command.set_query(query);

  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(DoPut(options, descriptor, arrow::schema({}), &writer, &reader));

  std::shared_ptr<Buffer> metadata;

  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));

  flight_sql_pb::DoPutUpdateResult doPutUpdateResult;

  flight_sql_pb::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult object.");
  }

  return result.record_count();
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetCatalogs(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetCatalogs command;

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
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

  return GetFlightInfoForCommand(*this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetTableTypes(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetTableTypes command;

  return GetFlightInfoForCommand(*this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetXdbcTypeInfo(
    const FlightCallOptions& options) {
  flight_sql_pb::CommandGetXdbcTypeInfo command;

  return GetFlightInfoForCommand(*this, options, command);
}

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetXdbcTypeInfo(
    const FlightCallOptions& options, int data_type) {
  flight_sql_pb::CommandGetXdbcTypeInfo command;

  command.set_data_type(data_type);

  return GetFlightInfoForCommand(*this, options, command);
}

arrow::Result<std::unique_ptr<FlightStreamReader>> FlightSqlClient::DoGet(
    const FlightCallOptions& options, const Ticket& ticket) {
  std::unique_ptr<FlightStreamReader> stream;
  ARROW_RETURN_NOT_OK(DoGet(options, ticket, &stream));

  return std::move(stream);
}

arrow::Result<std::shared_ptr<PreparedStatement>> FlightSqlClient::Prepare(
    const FlightCallOptions& options, const std::string& query) {
  google::protobuf::Any command;
  flight_sql_pb::ActionCreatePreparedStatementRequest request;
  request.set_query(query);
  command.PackFrom(request);

  Action action;
  action.type = "CreatePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(DoAction(options, action, &results));

  ARROW_ASSIGN_OR_RAISE(std::unique_ptr<Result> result, results->Next());

  google::protobuf::Any prepared_result;

  std::shared_ptr<Buffer> message = std::move(result->body);
  if (!prepared_result.ParseFromArray(message->data(),
                                      static_cast<int>(message->size()))) {
    return Status::Invalid("Unable to parse packed ActionCreatePreparedStatementResult");
  }

  flight_sql_pb::ActionCreatePreparedStatementResult prepared_statement_result;

  if (!prepared_result.UnpackTo(&prepared_statement_result)) {
    return Status::Invalid("Unable to unpack ActionCreatePreparedStatementResult");
  }

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

  return std::make_shared<PreparedStatement>(this, handle, dataset_schema,
                                             parameter_schema, options);
}

arrow::Result<std::unique_ptr<FlightInfo>> PreparedStatement::Execute() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }

  flight_sql_pb::CommandPreparedStatementQuery execute_query_command;

  execute_query_command.set_prepared_statement_handle(handle_);

  google::protobuf::Any any;
  any.PackFrom(execute_query_command);

  const std::string& string = any.SerializeAsString();
  const FlightDescriptor descriptor = FlightDescriptor::Command(string);

  if (parameter_binding_ && parameter_binding_->num_rows() > 0) {
    std::unique_ptr<FlightStreamWriter> writer;
    std::unique_ptr<FlightMetadataReader> reader;
    ARROW_RETURN_NOT_OK(client_->DoPut(options_, descriptor, parameter_binding_->schema(),
                                       &writer, &reader));

    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding_));
    ARROW_RETURN_NOT_OK(writer->DoneWriting());
    // Wait for the server to ack the result
    std::shared_ptr<Buffer> buffer;
    ARROW_RETURN_NOT_OK(reader->ReadMetadata(&buffer));
  }

  ARROW_ASSIGN_OR_RAISE(auto flight_info, client_->GetFlightInfo(options_, descriptor));
  return std::move(flight_info);
}

arrow::Result<int64_t> PreparedStatement::ExecuteUpdate() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }

  flight_sql_pb::CommandPreparedStatementUpdate command;
  command.set_prepared_statement_handle(handle_);
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  if (parameter_binding_ && parameter_binding_->num_rows() > 0) {
    ARROW_RETURN_NOT_OK(client_->DoPut(options_, descriptor, parameter_binding_->schema(),
                                       &writer, &reader));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding_));
  } else {
    const std::shared_ptr<Schema> schema = arrow::schema({});
    ARROW_RETURN_NOT_OK(client_->DoPut(options_, descriptor, schema, &writer, &reader));
    const ArrayVector columns;
    const auto& record_batch = arrow::RecordBatch::Make(schema, 0, columns);
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  }

  ARROW_RETURN_NOT_OK(writer->DoneWriting());
  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(writer->Close());

  flight_sql_pb::DoPutUpdateResult result;
  if (!result.ParseFromArray(metadata->data(), static_cast<int>(metadata->size()))) {
    return Status::Invalid("Unable to parse DoPutUpdateResult object.");
  }

  return result.record_count();
}

Status PreparedStatement::SetParameters(std::shared_ptr<RecordBatch> parameter_binding) {
  parameter_binding_ = std::move(parameter_binding);

  return Status::OK();
}

bool PreparedStatement::IsClosed() const { return is_closed_; }

std::shared_ptr<Schema> PreparedStatement::dataset_schema() const {
  return dataset_schema_;
}

std::shared_ptr<Schema> PreparedStatement::parameter_schema() const {
  return parameter_schema_;
}

Status PreparedStatement::Close() {
  if (is_closed_) {
    return Status::Invalid("Statement already closed.");
  }
  google::protobuf::Any command;
  flight_sql_pb::ActionClosePreparedStatementRequest request;
  request.set_prepared_statement_handle(handle_);

  command.PackFrom(request);

  Action action;
  action.type = "ClosePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(client_->DoAction(options_, action, &results));

  is_closed_ = true;

  return Status::OK();
}

Status FlightSqlClient::Close() { return impl_->Close(); }

arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClient::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<int>& sql_info) {
  flight_sql_pb::CommandGetSqlInfo command;
  for (const int& info : sql_info) command.add_info(info);

  return GetFlightInfoForCommand(*this, options, command);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
