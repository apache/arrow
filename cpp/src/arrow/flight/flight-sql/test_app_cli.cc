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

#include <arrow/array/builder_binary.h>
#include <arrow/flight/api.h>
#include <arrow/flight/flight-sql/api.h>
#include <arrow/io/memory.h>
#include <arrow/pretty_print.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <gflags/gflags.h>

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>

using arrow::Result;
using arrow::Schema;
using arrow::Status;
using arrow::flight::ClientAuthHandler;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClient;
using arrow::flight::FlightDescriptor;
using arrow::flight::FlightEndpoint;
using arrow::flight::FlightInfo;
using arrow::flight::FlightStreamChunk;
using arrow::flight::FlightStreamReader;
using arrow::flight::Location;
using arrow::flight::Ticket;
using arrow::flight::sql::FlightSqlClient;

DEFINE_string(host, "localhost", "Host to connect to");
DEFINE_int32(port, 32010, "Port to connect to");
DEFINE_string(username, "", "Username");
DEFINE_string(password, "", "Password");

DEFINE_string(command, "", "Method to run");
DEFINE_string(query, "", "Query");
DEFINE_string(catalog, "", "Catalog");
DEFINE_string(schema, "", "Schema");
DEFINE_string(table, "", "Table");

Status PrintResultsForEndpoint(const FlightSqlClient& client,
                               const FlightCallOptions& call_options,
                               const FlightEndpoint& endpoint) {
  ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(call_options, endpoint.ticket));

  const arrow::Result<std::shared_ptr<Schema>>& schema = stream->GetSchema();
  ARROW_RETURN_NOT_OK(schema);

  std::cout << "Schema:" << std::endl;
  std::cout << schema->get()->ToString() << std::endl << std::endl;

  std::cout << "Results:" << std::endl;

  FlightStreamChunk chunk;
  int64_t num_rows = 0;

  while (true) {
    ARROW_RETURN_NOT_OK(stream->Next(&chunk));
    if (!(chunk.data != nullptr)) {
      break;
    }
    std::cout << chunk.data->ToString() << std::endl;
    num_rows += chunk.data->num_rows();
  }

  std::cout << "Total: " << num_rows << std::endl;

  return Status::OK();
}

Status PrintResults(FlightSqlClient& client, const FlightCallOptions& call_options,
                    const std::unique_ptr<FlightInfo>& info) {
  const std::vector<FlightEndpoint>& endpoints = info->endpoints();

  for (size_t i = 0; i < endpoints.size(); i++) {
    std::cout << "Results from endpoint " << i + 1 << " of " << endpoints.size()
              << std::endl;
    ARROW_RETURN_NOT_OK(PrintResultsForEndpoint(client, call_options, endpoints[i]));
  }

  return Status::OK();
}

Status RunMain() {
  std::unique_ptr<FlightClient> client;
  Location location;
  ARROW_RETURN_NOT_OK(Location::ForGrpcTcp(fLS::FLAGS_host, fLI::FLAGS_port, &location));
  ARROW_RETURN_NOT_OK(FlightClient::Connect(location, &client));

  FlightCallOptions call_options;

  if (!fLS::FLAGS_username.empty() || !fLS::FLAGS_password.empty()) {
    Result<std::pair<std::string, std::string>> bearer_result =
        client->AuthenticateBasicToken({}, fLS::FLAGS_username, fLS::FLAGS_password);
    ARROW_RETURN_NOT_OK(bearer_result);

    call_options.headers.push_back(bearer_result.ValueOrDie());
  }

  FlightSqlClient sqlClient(client);

  if (fLS::FLAGS_command == "ExecuteUpdate") {
    ARROW_ASSIGN_OR_RAISE(auto rows,
                          sqlClient.ExecuteUpdate(call_options, fLS::FLAGS_query));

    std::cout << "Result: " << rows << std::endl;

    return Status::OK();
  }

  std::unique_ptr<FlightInfo> info;

  if (fLS::FLAGS_command == "Execute") {
    ARROW_ASSIGN_OR_RAISE(info, sqlClient.Execute(call_options, fLS::FLAGS_query));
  } else if (fLS::FLAGS_command == "GetCatalogs") {
    ARROW_ASSIGN_OR_RAISE(info, sqlClient.GetCatalogs(call_options));
  } else if (fLS::FLAGS_command == "PreparedStatementExecute") {
    ARROW_ASSIGN_OR_RAISE(auto prepared_statement,
                          sqlClient.Prepare(call_options, fLS::FLAGS_query));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute());
  } else if (fLS::FLAGS_command == "PreparedStatementExecuteParameterBinding") {
    ARROW_ASSIGN_OR_RAISE(auto prepared_statement,
                          sqlClient.Prepare({}, fLS::FLAGS_query));
    ARROW_ASSIGN_OR_RAISE(auto parameter_schema,
                          prepared_statement->GetParameterSchema());
    ARROW_ASSIGN_OR_RAISE(auto result_set_schema,
                          prepared_statement->GetResultSetSchema());

    std::cout << result_set_schema->ToString(false) << std::endl;
    arrow::Int64Builder int_builder;
    ARROW_RETURN_NOT_OK(int_builder.Append(1));
    std::shared_ptr<arrow::Array> int_array;
    ARROW_RETURN_NOT_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::RecordBatch> result;
    result = arrow::RecordBatch::Make(parameter_schema, 1, {int_array});

    ARROW_RETURN_NOT_OK(prepared_statement->SetParameters(result));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute());
  } else if (fLS::FLAGS_command == "GetSchemas") {
    ARROW_ASSIGN_OR_RAISE(info, sqlClient.GetSchemas(call_options, &fLS::FLAGS_catalog,
                                                     &fLS::FLAGS_schema));
  } else if (fLS::FLAGS_command == "GetTableTypes") {
    ARROW_ASSIGN_OR_RAISE(info, sqlClient.GetTableTypes(call_options));
  } else if (fLS::FLAGS_command == "GetTables") {
    std::vector<std::string> table_types = {};
    bool include_schema = false;

    ARROW_ASSIGN_OR_RAISE(
        info, sqlClient.GetTables(call_options, &fLS::FLAGS_catalog, &fLS::FLAGS_schema,
                                  &fLS::FLAGS_table, include_schema, table_types));
  } else if (fLS::FLAGS_command == "GetExportedKeys") {
    ARROW_ASSIGN_OR_RAISE(
        info, sqlClient.GetExportedKeys(call_options, &fLS::FLAGS_catalog,
                                        &fLS::FLAGS_schema, fLS::FLAGS_table));
  } else if (fLS::FLAGS_command == "GetImportedKeys") {
    ARROW_ASSIGN_OR_RAISE(
        info, sqlClient.GetImportedKeys(call_options, &fLS::FLAGS_catalog,
                                        &fLS::FLAGS_schema, fLS::FLAGS_table));
  } else if (fLS::FLAGS_command == "GetPrimaryKeys") {
    ARROW_ASSIGN_OR_RAISE(info,
                          sqlClient.GetPrimaryKeys(call_options, &fLS::FLAGS_catalog,
                                                   &fLS::FLAGS_schema, fLS::FLAGS_table));
  }

  if (info != NULLPTR &&
      !boost::istarts_with(fLS::FLAGS_command, "PreparedStatementExecute")) {
    return PrintResults(sqlClient, call_options, info);
  }

  return Status::OK();
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}
