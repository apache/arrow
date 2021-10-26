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

#include <arrow/buffer.h>
#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <arrow/flight/types.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>
#include <google/protobuf/any.pb.h>
#include <google/protobuf/message_lite.h>

#include <utility>

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

template <class T>
FlightSqlClientT<T>::FlightSqlClientT(std::unique_ptr<T>& client) {
  this->client = std::move(client);
}

template <class T>
PreparedStatementT<T>::PreparedStatementT(
    T* client_, const std::string& query,
    pb::sql::ActionCreatePreparedStatementResult& prepared_statement_result_,
    FlightCallOptions options_)
    : client(client_),
      options(std::move(options_)),
      prepared_statement_result(std::move(prepared_statement_result_)),
      is_closed(false) {}

template <class T>
FlightSqlClientT<T>::~FlightSqlClientT() = default;

template <class T>
PreparedStatementT<T>::~PreparedStatementT<T>() {
  ARROW_UNUSED(Close());
}

inline FlightDescriptor GetFlightDescriptorForCommand(
    const google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
    const std::unique_ptr<T>& client, const FlightCallOptions& options,
    const google::protobuf::Message& command) {
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightInfo> flight_info;
  ARROW_RETURN_NOT_OK(client->GetFlightInfo(options, descriptor, &flight_info));

  return std::move(flight_info);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::Execute(
    const FlightCallOptions& options, const std::string& query) const {
  pb::sql::CommandStatementQuery command;
  command.set_query(query);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<int64_t> FlightSqlClientT<T>::ExecuteUpdate(
    const FlightCallOptions& options, const std::string& query) const {
  pb::sql::CommandStatementUpdate command;
  command.set_query(query);

  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(client->DoPut(options, descriptor, NULLPTR, &writer, &reader));

  std::shared_ptr<Buffer> metadata;

  const Status& status = reader->ReadMetadata(&metadata);

  pb::sql::DoPutUpdateResult doPutUpdateResult;

  Buffer* pBuffer = metadata.get();

  const std::string& string = pBuffer->ToString();

  doPutUpdateResult.ParseFrom<google::protobuf::MessageLite::kParse>(string);
  return doPutUpdateResult.record_count();
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetCatalogs(
    const FlightCallOptions& options) const {
  pb::sql::CommandGetCatalogs command;

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetSchemas(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema_filter_pattern) const {
  pb::sql::CommandGetSchemas command;
  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }
  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetTables(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema_filter_pattern, const std::string* table_filter_pattern,
    bool include_schema, std::vector<std::string>& table_types) const {
  pb::sql::CommandGetTables command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  if (table_filter_pattern != NULLPTR) {
    command.set_table_name_filter_pattern(*table_filter_pattern);
  }

  command.set_include_schema(include_schema);

  for (const std::string& table_type : table_types) {
    command.add_table_types(table_type);
  }

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetPrimaryKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) const {
  pb::sql::CommandGetPrimaryKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetExportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) const {
  pb::sql::CommandGetExportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetImportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table) const {
  pb::sql::CommandGetImportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetCrossReference(
    const FlightCallOptions& options, const std::string* pk_catalog,
    const std::string* pk_schema, const std::string& pk_table,
    const std::string* fk_catalog, const std::string* fk_schema,
    const std::string& fk_table) const {
  pb::sql::CommandGetCrossReference command;

  if (pk_catalog != NULLPTR) {
    command.set_pk_catalog(*pk_catalog);
  }
  if (pk_schema != NULLPTR) {
    command.set_pk_schema(*pk_schema);
  }
  command.set_pk_table(pk_table);

  if (fk_catalog != NULLPTR) {
    command.set_fk_catalog(*fk_catalog);
  }
  if (fk_schema != NULLPTR) {
    command.set_fk_schema(*fk_schema);
  }
  command.set_fk_table(fk_table);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetTableTypes(
    const FlightCallOptions& options) const {
  pb::sql::CommandGetTableTypes command;

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightStreamReader>> FlightSqlClientT<T>::DoGet(
    const FlightCallOptions& options, const Ticket& ticket) const {
  std::unique_ptr<FlightStreamReader> stream;
  ARROW_RETURN_NOT_OK(client->DoGet(options, ticket, &stream));

  return std::move(stream);
}

template <class T>
arrow::Result<std::shared_ptr<PreparedStatementT<T>>> FlightSqlClientT<T>::Prepare(
    const FlightCallOptions& options, const std::string& query) {
  google::protobuf::Any command;
  pb::sql::ActionCreatePreparedStatementRequest request;
  request.set_query(query);
  command.PackFrom(request);

  Action action;
  action.type = "CreatePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(client->DoAction(options, action, &results));

  std::unique_ptr<Result> result;
  ARROW_RETURN_NOT_OK(results->Next(&result));

  google::protobuf::Any prepared_result;

  std::shared_ptr<Buffer> message = std::move(result->body);

  prepared_result.ParseFromArray(message->data(), static_cast<int>(message->size()));

  pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

  prepared_result.UnpackTo(&prepared_statement_result);

  return std::shared_ptr<PreparedStatementT<T>>(
      new PreparedStatementT<T>(client.get(), query, prepared_statement_result, options));
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> PreparedStatementT<T>::Execute() {
  if (is_closed) {
    return Status::Invalid("Statement already closed.");
  }

  pb::sql::CommandPreparedStatementQuery execute_query_command;

  execute_query_command.set_prepared_statement_handle(
      prepared_statement_result.prepared_statement_handle());

  google::protobuf::Any any;
  any.PackFrom(execute_query_command);

  const std::string& string = any.SerializeAsString();
  const FlightDescriptor descriptor = FlightDescriptor::Command(string);

  if (parameter_binding && parameter_binding->num_rows() > 0) {
    std::unique_ptr<FlightStreamWriter> writer;
    std::unique_ptr<FlightMetadataReader> reader;
    ARROW_RETURN_NOT_OK(client->DoPut(options, descriptor, parameter_binding->schema(),
                                      &writer, &reader));

    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding));
    ARROW_RETURN_NOT_OK(writer->DoneWriting());
    // Wait for the server to ack the result
    std::shared_ptr<Buffer> buffer;
    ARROW_RETURN_NOT_OK(reader->ReadMetadata(&buffer));
  }

  std::unique_ptr<FlightInfo> info;
  ARROW_RETURN_NOT_OK(client->GetFlightInfo(options, descriptor, &info));

  return std::move(info);
}

template <class T>
arrow::Result<int64_t> PreparedStatementT<T>::ExecuteUpdate() {
  if (is_closed) {
    return Status::Invalid("Statement already closed.");
  }

  pb::sql::CommandPreparedStatementUpdate command;
  command.set_prepared_statement_handle(
      prepared_statement_result.prepared_statement_handle());
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;

  if (parameter_binding && parameter_binding->num_rows() > 0) {
    ARROW_RETURN_NOT_OK(client->DoPut(options, descriptor, parameter_binding->schema(),
                                      &writer, &reader));
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*parameter_binding));
  } else {
    const std::shared_ptr<Schema> schema = arrow::schema({});
    ARROW_RETURN_NOT_OK(client->DoPut(options, descriptor, schema, &writer, &reader));
    const auto& record_batch =
        arrow::RecordBatch::Make(schema, 0, (std::vector<std::shared_ptr<Array>>){});
    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*record_batch));
  }

  ARROW_RETURN_NOT_OK(writer->DoneWriting());
  std::shared_ptr<Buffer> metadata;
  ARROW_RETURN_NOT_OK(reader->ReadMetadata(&metadata));
  ARROW_RETURN_NOT_OK(writer->Close());

  pb::sql::DoPutUpdateResult doPutUpdateResult;
  const std::string& metadataAsString = metadata->ToString();

  doPutUpdateResult.ParseFrom<google::protobuf::MessageLite::kParse>(metadataAsString);

  return doPutUpdateResult.record_count();
}

template <class T>
Status PreparedStatementT<T>::SetParameters(
    std::shared_ptr<RecordBatch> parameter_binding_) {
  parameter_binding = std::move(parameter_binding_);

  return Status::OK();
}

template <class T>
bool PreparedStatementT<T>::IsClosed() const {
  return is_closed;
}

template <class T>
arrow::Result<std::shared_ptr<Schema>> PreparedStatementT<T>::GetResultSetSchema() {
  auto& args = prepared_statement_result.dataset_schema();
  std::shared_ptr<Buffer> schema_buffer = std::make_shared<Buffer>(args);

  io::BufferReader reader(schema_buffer);

  ipc::DictionaryMemo in_memo;
  return ReadSchema(&reader, &in_memo);
}

template <class T>
arrow::Result<std::shared_ptr<Schema>> PreparedStatementT<T>::GetParameterSchema() {
  auto& args = prepared_statement_result.parameter_schema();
  std::shared_ptr<Buffer> schema_buffer = std::make_shared<Buffer>(args);

  io::BufferReader reader(schema_buffer);

  ipc::DictionaryMemo in_memo;
  return ReadSchema(&reader, &in_memo);
}

template <class T>
Status PreparedStatementT<T>::Close() {
  if (is_closed) {
    return Status::Invalid("Statement already closed.");
  }
  google::protobuf::Any command;
  pb::sql::ActionClosePreparedStatementRequest request;
  request.set_prepared_statement_handle(
      prepared_statement_result.prepared_statement_handle());

  command.PackFrom(request);

  Action action;
  action.type = "ClosePreparedStatement";
  action.body = Buffer::FromString(command.SerializeAsString());

  std::unique_ptr<ResultStream> results;

  ARROW_RETURN_NOT_OK(client->DoAction(options, action, &results));

  is_closed = true;

  return Status::OK();
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<int>& sql_info) const {
  pb::sql::CommandGetSqlInfo command;
  for (const int& info : sql_info) command.add_info(info);

  return GetFlightInfoForCommand(client, options, command);
}

template <class T>
arrow::Result<std::unique_ptr<FlightInfo>> FlightSqlClientT<T>::GetSqlInfo(
    const FlightCallOptions& options,
    const std::vector<pb::sql::SqlInfo>& sql_info) const {
  return GetSqlInfo(options, reinterpret_cast<const std::vector<int>&>(sql_info));
}

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
