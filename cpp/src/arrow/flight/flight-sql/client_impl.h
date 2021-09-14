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

#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <arrow/flight/types.h>
#include <google/protobuf/any.pb.h>

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {
namespace internal {

template<class T>
FlightSqlClientT<T>::FlightSqlClientT(T *client) {
  this->client = client;
}

template<class T>
FlightSqlClientT<T>::~FlightSqlClientT() = default;

template<class T>
Status GetFlightInfoForCommand(T* client,
                               const FlightCallOptions &options,
                               std::unique_ptr<FlightInfo> *flight_info,
                               const google::protobuf::Message &command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  const FlightDescriptor& descriptor = FlightDescriptor::Command(string);

  return client->GetFlightInfo(options, descriptor, flight_info);
}

template<class T>
Status FlightSqlClientT<T>::Execute(const FlightCallOptions& options,
                                   std::unique_ptr<FlightInfo>* flight_info,
                                   const std::string& query) {
  pb::sql::CommandStatementQuery command;
  command.set_query(query);

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template<class T>
Status FlightSqlClientT<T>::ExecuteUpdate(const FlightCallOptions& options,
                                         int64_t* rows,
                                         const std::string& query) {
  return Status::NotImplemented("ExecuteUpdate");
}

template<class T>
Status FlightSqlClientT<T>::GetCatalogs(const FlightCallOptions& options,
                                       std::unique_ptr<FlightInfo>* flight_info) {
  pb::sql::CommandGetCatalogs command;

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template<class T>
Status FlightSqlClientT<T>::GetSchemas(const FlightCallOptions& options,
                                      std::unique_ptr<FlightInfo>* flight_info,
                                      std::string* catalog,
                                      std::string* schema_filter_pattern) {
  pb::sql::CommandGetSchemas command;
  if (catalog != NULLPTR) {
    command.set_catalog(*catalog);
  }
  if (schema_filter_pattern != NULLPTR) {
    command.set_schema_filter_pattern(*schema_filter_pattern);
  }

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template<class T>
Status FlightSqlClientT<T>::GetTables(const FlightCallOptions& options,
                                     std::unique_ptr<FlightInfo>* flight_info,
                                     const std::string* catalog,
                                     const std::string* schema_filter_pattern,
                                     const std::string* table_filter_pattern,
                                     bool include_schema,
                                     std::vector<std::string>& table_types) {
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

  for (const std::string &table_type : table_types) {
    command.add_table_types(table_type);
  }

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template<class T>
Status FlightSqlClientT<T>::GetPrimaryKeys(const FlightCallOptions& options,
                                          std::unique_ptr<FlightInfo>* flight_info,
                                          const std::string* catalog,
                                          const std::string* schema,
                                          const std::string& table) {
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

template<class T>
Status FlightSqlClientT<T>::GetExportedKeys(const FlightCallOptions& options,
                                           std::unique_ptr<FlightInfo>* flight_info,
                                           const std::string* catalog,
                                           const std::string* schema,
                                           const std::string& table) {
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

template<class T>
Status FlightSqlClientT<T>::GetImportedKeys(const FlightCallOptions& options,
                                           std::unique_ptr<FlightInfo>* flight_info,
                                           const std::string* catalog,
                                           const std::string* schema,
                                           const std::string& table) {
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

template<class T>
Status FlightSqlClientT<T>::GetTableTypes(const FlightCallOptions& options,
                                         std::unique_ptr<FlightInfo>* flight_info) {
  pb::sql::CommandGetTableTypes command;

  return GetFlightInfoForCommand(client, options, flight_info, command);
}

template<class T>
Status FlightSqlClientT<T>::DoGet(const FlightCallOptions& options,
                                 const Ticket& ticket,
                                 std::unique_ptr<FlightStreamReader>* stream) {
  return client->DoGet(options, ticket, stream);
}

}  // namespace internal
}  // namespace sql
}  // namespace flight
}  // namespace arrow
