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

#include <gflags/gflags.h>
#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>
#include <optional>

#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"
#include "arrow/io/memory.h"
#include "arrow/pretty_print.h"
#include "arrow/status.h"
#include "arrow/table.h"

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
using arrow::flight::sql::TableRef;

DEFINE_string(host, "localhost", "Host to connect to");
DEFINE_int32(port, 32010, "Port to connect to");
DEFINE_string(username, "", "Username");
DEFINE_string(password, "", "Password");

DEFINE_string(command, "", "Method to run");
DEFINE_string(query, "", "Query");
DEFINE_string(catalog, "", "Catalog");
DEFINE_string(schema, "", "Schema");
DEFINE_string(table, "", "Table");

Status PrintResultsForEndpoint(FlightSqlClient& client,
                               const FlightCallOptions& call_options,
                               const FlightEndpoint& endpoint) {
  ARROW_ASSIGN_OR_RAISE(auto stream, client.DoGet(call_options, endpoint.ticket));

  const arrow::Result<std::shared_ptr<Schema>>& schema = stream->GetSchema();
  ARROW_RETURN_NOT_OK(schema);

  std::cout << "Schema:" << std::endl;
  std::cout << schema->get()->ToString() << std::endl << std::endl;

  std::cout << "Results:" << std::endl;

  int64_t num_rows = 0;

  while (true) {
    ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, stream->Next());
    if (chunk.data == nullptr) {
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
  ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp(FLAGS_host, FLAGS_port));
  ARROW_ASSIGN_OR_RAISE(auto client, FlightClient::Connect(location));

  FlightCallOptions call_options;

  if (!FLAGS_username.empty() || !FLAGS_password.empty()) {
    Result<std::pair<std::string, std::string>> bearer_result =
        client->AuthenticateBasicToken({}, FLAGS_username, FLAGS_password);
    ARROW_RETURN_NOT_OK(bearer_result);

    call_options.headers.push_back(bearer_result.ValueOrDie());
  }

  FlightSqlClient sql_client(std::move(client));

  if (FLAGS_command == "ExecuteUpdate") {
    ARROW_ASSIGN_OR_RAISE(auto rows, sql_client.ExecuteUpdate(call_options, FLAGS_query));

    std::cout << "Result: " << rows << std::endl;

    return Status::OK();
  }

  std::unique_ptr<FlightInfo> info;

  if (FLAGS_command == "Execute") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.Execute(call_options, FLAGS_query));
  } else if (FLAGS_command == "GetCatalogs") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetCatalogs(call_options));
  } else if (FLAGS_command == "PreparedStatementExecute") {
    ARROW_ASSIGN_OR_RAISE(auto prepared_statement,
                          sql_client.Prepare(call_options, FLAGS_query));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute());
  } else if (FLAGS_command == "PreparedStatementExecuteParameterBinding") {
    ARROW_ASSIGN_OR_RAISE(auto prepared_statement, sql_client.Prepare({}, FLAGS_query));
    auto parameter_schema = prepared_statement->parameter_schema();
    auto result_set_schema = prepared_statement->dataset_schema();

    std::cout << result_set_schema->ToString(false) << std::endl;
    arrow::Int64Builder int_builder;
    ARROW_RETURN_NOT_OK(int_builder.Append(1));
    std::shared_ptr<arrow::Array> int_array;
    ARROW_RETURN_NOT_OK(int_builder.Finish(&int_array));
    std::shared_ptr<arrow::RecordBatch> result;
    result = arrow::RecordBatch::Make(parameter_schema, 1, {int_array});

    ARROW_RETURN_NOT_OK(prepared_statement->SetParameters(result));
    ARROW_ASSIGN_OR_RAISE(info, prepared_statement->Execute());
  } else if (FLAGS_command == "GetDbSchemas") {
    ARROW_ASSIGN_OR_RAISE(
        info, sql_client.GetDbSchemas(call_options, &FLAGS_catalog, &FLAGS_schema));
  } else if (FLAGS_command == "GetTableTypes") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetTableTypes(call_options));
  } else if (FLAGS_command == "GetTables") {
    ARROW_ASSIGN_OR_RAISE(
        info, sql_client.GetTables(call_options, &FLAGS_catalog, &FLAGS_schema,
                                   &FLAGS_table, false, nullptr));
  } else if (FLAGS_command == "GetExportedKeys") {
    TableRef table_ref = {std::make_optional(FLAGS_catalog),
                          std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetExportedKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetImportedKeys") {
    TableRef table_ref = {std::make_optional(FLAGS_catalog),
                          std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetImportedKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetPrimaryKeys") {
    TableRef table_ref = {std::make_optional(FLAGS_catalog),
                          std::make_optional(FLAGS_schema), FLAGS_table};
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetPrimaryKeys(call_options, table_ref));
  } else if (FLAGS_command == "GetSqlInfo") {
    ARROW_ASSIGN_OR_RAISE(info, sql_client.GetSqlInfo(call_options, {}));
  }

  if (info != NULLPTR &&
      !boost::istarts_with(FLAGS_command, "PreparedStatementExecute")) {
    return PrintResults(sql_client, call_options, info);
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
