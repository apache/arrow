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

#pragma once

#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/odbc/odbc_impl/diagnostics.h"
#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/odbc_impl/record_batch_transformer.h"
#include "arrow/flight/sql/odbc/odbc_impl/spi/result_set.h"
#include "arrow/flight/sql/odbc/odbc_impl/types.h"
#include "arrow/flight/types.h"
#include "arrow/type.h"

namespace arrow::flight::sql::odbc {

typedef struct {
  std::string catalog_column;
  std::string schema_column;
  std::string table_column;
  std::string table_type_column;
  std::string remarks_column;
} ColumnNames;

void ParseTableTypes(const std::string& table_type,
                     std::vector<std::string>& table_types);

std::shared_ptr<ResultSet> GetTablesForSQLAllCatalogs(
    const ColumnNames& column_names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings);

std::shared_ptr<ResultSet> GetTablesForSQLAllDbSchemas(
    const ColumnNames& column_names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    const std::string* schema_name, Diagnostics& diagnostics,
    const MetadataSettings& metadata_settings);

std::shared_ptr<ResultSet> GetTablesForSQLAllTableTypes(
    const ColumnNames& column_names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings);

std::shared_ptr<ResultSet> GetTablesForGenericUse(
    const ColumnNames& column_names, FlightClientOptions& client_options,
    FlightCallOptions& call_options, FlightSqlClient& sql_client,
    const std::string* catalog_name, const std::string* schema_name,
    const std::string* table_name, const std::vector<std::string>& table_types,
    Diagnostics& diagnostics, const MetadataSettings& metadata_settings);
}  // namespace arrow::flight::sql::odbc
