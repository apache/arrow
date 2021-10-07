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
    const FlightCallOptions& options_)
    : client(client_),
      prepared_statement_result(std::move(prepared_statement_result_)),
      options(options_) {
  is_closed = false;
}

template <class T>
FlightSqlClientT<T>::~FlightSqlClientT() = default;

template <class T>
PreparedStatementT<T>::~PreparedStatementT<T>() {
  Close();
}

inline FlightDescriptor GetFlightDescriptorForCommand(
    const google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

template <class T>
Status GetFlightInfoForCommand(const std::unique_ptr<T>& client,
                               const FlightCallOptions& options,
                               std::unique_ptr<FlightInfo>* flight_info,
                               const google::protobuf::Message& command) {
  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  return client->GetFlightInfo(options, descriptor, flight_info);
}

template <class T>
Status FlightSqlClientT<T>::Execute(const FlightCallOptions& options,
                                    const std::string& query,
                                    std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandStatementQuery command;
  command.set_query(query);

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::ExecuteUpdate(const FlightCallOptions& options,
                                          const std::string& query, int64_t* rows) const {
  pb::sql::CommandStatementUpdate command;
  command.set_query(query);

  const FlightDescriptor& descriptor = GetFlightDescriptorForCommand(command);

  std::unique_ptr<FlightMetadataReader> reader;

  ARROW_RETURN_NOT_OK(client->DoPut(options, descriptor, NULLPTR, NULL, &reader));

  std::shared_ptr<Buffer> metadata;

  const Status& status = reader->ReadMetadata(&metadata);

  pb::sql::DoPutUpdateResult doPutUpdateResult;

  Buffer* pBuffer = metadata.get();

  const std::string& string = pBuffer->ToString();

  doPutUpdateResult.ParseFrom<google::protobuf::MessageLite::kParse>(string);
  *rows = doPutUpdateResult.record_count();

  return Status::OK();
}

template <class T>
Status FlightSqlClientT<T>::GetCatalogs(const FlightCallOptions& options,
                                        std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetCatalogs command;

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetSchemas(const FlightCallOptions& options,
                                       const std::string* catalog,
                                       const std::string* schema_filter_pattern,
                                       std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetSchemas command;
  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }
  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetTables(const FlightCallOptions& options,
                                      const std::string* catalog,
                                      const std::string* schema_filter_pattern,
                                      const std::string* table_filter_pattern,
                                      bool include_schema,
                                      std::vector<std::string>& table_types,
                                      std::unique_ptr<FlightInfo>* flight_info) const {
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

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetPrimaryKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table,
    std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetPrimaryKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetExportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table,
    std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetExportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetImportedKeys(
    const FlightCallOptions& options, const std::string* catalog,
    const std::string* schema, const std::string& table,
    std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetImportedKeys command;

  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }

  if (schema != NULLPTR) {
    command.set_schema(*schema);
  }

  command.set_table(table);

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetTableTypes(
    const FlightCallOptions& options, std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetTableTypes command;

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::DoGet(const FlightCallOptions& options, const Ticket& ticket,
                                  std::unique_ptr<FlightStreamReader>* stream) const {
  return client->DoGet(options, ticket, stream);
}

template <class T>
Status FlightSqlClientT<T>::Prepare(
    const FlightCallOptions& options, const std::string& query,
    std::shared_ptr<PreparedStatementT<T>>* prepared_statement) {
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

  prepared_statement->reset(new PreparedStatementT<T>(client.get(), query, prepared_statement_result, options));

  return Status::OK();
}

template <class T>
Status PreparedStatementT<T>::Execute(std::unique_ptr<FlightInfo>* info) {
  if (is_closed) {
    return Status::Invalid("Statement already closed.");
  }

  pb::sql::CommandPreparedStatementQuery execute_query_command;

  execute_query_command.set_prepared_statement_handle(
      prepared_statement_result.prepared_statement_handle());

  google::protobuf::Any any;
  any.PackFrom(execute_query_command);

  const std::string& string = any.SerializeAsString();
  const FlightDescriptor& descriptor = FlightDescriptor::Command(string);

  return client->GetFlightInfo(options, descriptor, info);
}

template <class T>
bool PreparedStatementT<T>::IsClosed() const {
  return is_closed;
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
Status FlightSqlClientT<T>::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<int>& sql_info,
    std::unique_ptr<FlightInfo>* flight_info) const {
  pb::sql::CommandGetSqlInfo command;
  for (const int& info : sql_info) command.add_info(info);
  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template <class T>
Status FlightSqlClientT<T>::GetSqlInfo(
    const FlightCallOptions& options, const std::vector<pb::sql::SqlInfo>& sql_info,
    std::unique_ptr<FlightInfo>* flight_info) const {
  return GetSqlInfo(options, reinterpret_cast<const std::vector<int>&>(sql_info), flight_info);
}

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
