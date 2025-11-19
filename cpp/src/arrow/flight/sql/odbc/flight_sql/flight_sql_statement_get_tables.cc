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
#include "arrow/flight/sql/odbc/flight_sql/utils.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/types.h"

namespace driver {
namespace flight_sql {

using arrow::Result;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightInfo;
using arrow::flight::sql::FlightSqlClient;

void ParseTableTypes(const std::string& table_type,
                     std::vector<std::string>& table_types) {
  bool encountered = false;  // for checking if there is a single quote
  std::string curr_parse;    // the current string

  for (char temp : table_type) {  // while still in the string
    switch (temp) {               // switch depending on the character
      case '\'':                  // if the character is a single quote
        if (encountered) {
          encountered = false;  // if we already found a single quote, reset encountered
        } else {
          encountered =
              true;  // if we haven't found a single quote, set encountered to true
        }
        break;
      case ',':                               // if it is a comma
        if (!encountered) {                   // if we have not found a single quote
          table_types.push_back(curr_parse);  // put our current string into our vector
          curr_parse = "";                    // reset the current string
          break;
        }
      default:  // if it is a normal character
        if (encountered && isspace(temp)) {
          curr_parse.push_back(temp);  // if we have found a single quote put the
                                       // whitespace, we don't care
        } else if (temp == '\'' || temp == ' ') {
          break;  // if the current character is a single quote, trash it and go to
                  // the next character.
        } else {
          curr_parse.push_back(temp);  // if all of the above failed, put the
                                       // character into the current string
        }
        break;  // go to the next character
    }
  }
  table_types.emplace_back(
      curr_parse);  // if we have found a single quote put the whitespace,
  // we don't care
}

std::shared_ptr<ResultSet> GetTablesForSQLAllCatalogs(
    const ColumnNames& names, FlightCallOptions& call_options,
    FlightSqlClient& sql_client, odbcabstraction::Diagnostics& diagnostics,
    const odbcabstraction::MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetCatalogs(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .AddFieldOfNulls(names.schema_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_type_column, arrow::utf8())
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
      sql_client, call_options, flight_info, transformer, diagnostics, metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllDbSchemas(
    const ColumnNames& names, FlightCallOptions& call_options,
    FlightSqlClient& sql_client, const std::string* schema_name,
    odbcabstraction::Diagnostics& diagnostics,
    const odbcabstraction::MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result =
      sql_client.GetDbSchemas(call_options, nullptr, schema_name);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, arrow::utf8())
                         .RenameField("db_schema_name", names.schema_column)
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_type_column, arrow::utf8())
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
      sql_client, call_options, flight_info, transformer, diagnostics, metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForSQLAllTableTypes(
    const ColumnNames& names, FlightCallOptions& call_options,
    FlightSqlClient& sql_client, odbcabstraction::Diagnostics& diagnostics,
    const odbcabstraction::MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTableTypes(call_options);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .AddFieldOfNulls(names.catalog_column, arrow::utf8())
                         .AddFieldOfNulls(names.schema_column, arrow::utf8())
                         .AddFieldOfNulls(names.table_column, arrow::utf8())
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
      sql_client, call_options, flight_info, transformer, diagnostics, metadata_settings);
}

std::shared_ptr<ResultSet> GetTablesForGenericUse(
    const ColumnNames& names, FlightCallOptions& call_options,
    FlightSqlClient& sql_client, const std::string* catalog_name,
    const std::string* schema_name, const std::string* table_name,
    const std::vector<std::string>& table_types,
    odbcabstraction::Diagnostics& diagnostics,
    const odbcabstraction::MetadataSettings& metadata_settings) {
  Result<std::shared_ptr<FlightInfo>> result = sql_client.GetTables(
      call_options, catalog_name, schema_name, table_name, false, &table_types);

  std::shared_ptr<Schema> schema;
  std::shared_ptr<FlightInfo> flight_info;

  ThrowIfNotOK(result.status());
  flight_info = result.ValueOrDie();
  ThrowIfNotOK(flight_info->GetSchema(nullptr).Value(&schema));

  auto transformer = RecordBatchTransformerWithTasksBuilder(schema)
                         .RenameField("catalog_name", names.catalog_column)
                         .RenameField("db_schema_name", names.schema_column)
                         .RenameField("table_name", names.table_column)
                         .RenameField("table_type", names.table_type_column)
                         .AddFieldOfNulls(names.remarks_column, arrow::utf8())
                         .Build();

  return std::make_shared<FlightSqlResultSet>(
      sql_client, call_options, flight_info, transformer, diagnostics, metadata_settings);
}

}  // namespace flight_sql
}  // namespace driver
