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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement_get_tables.h"
#include "arrow/flight/api.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set.h"
#include "arrow/flight/sql/odbc/flight_sql/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/flight_sql/util.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/types.h"
#include "arrow/util/string.h"

namespace arrow::flight::sql::odbc {

using arrow::Result;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;

static void AddTableType(std::string& table_type, std::vector<std::string>& table_types) {
  std::string trimmed_type = arrow::internal::TrimString(table_type);

  // Only put the string if the trimmed result is non-empty
  if (trimmed_type.length() > 0) {
    table_types.emplace_back(trimmed_type);
  }
}

void ParseTableTypes(const std::string& table_type,
                     std::vector<std::string>& table_types) {
  bool encountered = false;  // for checking if there is a single quote
  std::string curr_parse;    // the current string

  for (char temp : table_type) {  // while still in the string
    switch (temp) {               // switch depending on the character
      case '\'':                  // if the character is a single quote
        // track when we've encountered a single opening quote
        // and are still looking for the closing quote
        encountered = !encountered;
        break;
      case ',':                                   // if it is a comma
        if (!encountered) {                       // if we have not found a single quote
          AddTableType(curr_parse, table_types);  // put current string into vector
          curr_parse = "";                        // reset the current string
          break;
        }
        [[fallthrough]];
      default:                       // if it is a normal character
        curr_parse.push_back(temp);  // put the character into the current string
        break;                       // go to the next character
    }
  }
  AddTableType(curr_parse, table_types);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllCatalogs(
    const ColumnNames& names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetCatalogs(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  util::ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  util::ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .AddFieldOfNulls(names.schema_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_type_column, arrow::utf8())
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, client_options, call_options,
                                              flight_info, transformer, diagnostics,
                                              metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllDbSchemas(
    const ColumnNames& names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    const std::string* schema_name, Diagnostics& diagnostics,
    const MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetDbSchemas(call_options, nullptr, schema_name);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  util::ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  util::ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, arrow::utf8())
                         .RenameField("db_schema_name", names.schema_column)
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_type_column, arrow::utf8())
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, client_options, call_options,
                                              flight_info, transformer, diagnostics,
                                              metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllTableTypes(
    const ColumnNames& names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTableTypes(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  util::ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  util::ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, arrow::utf8())
                         .AddFieldOfNulls(names.schema_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, client_options, call_options,
                                              flight_info, transformer, diagnostics,
                                              metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForGenericUse(
    const ColumnNames& names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    const std::string* catalog_name, const std::string* schema_name,
    const std::string* table_name, const std::vector<std::string>& table_types,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTables(
      call_options, catalog_name, schema_name, table_name, false, &table_types);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  util::ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  util::ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .RenameField("db_schema_name", names.schema_column)
                         .RenameField("table_name", names.table_column)
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(sql_client, client_options, call_options,
                                              flight_info, transformer, diagnostics,
                                              metadata_settings);
}

}  // namespace arrow::flight::sql::odbc
