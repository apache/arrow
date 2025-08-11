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

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement_get_tables.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/diagnostics.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/spi/statement.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"
#include "arrow/flight/types.h"

namespace driver {
namespace flight_sql {

class FlightSqlStatement : public odbcabstraction::Statement {
 private:
  odbcabstraction::Diagnostics diagnostics_;
  std::map<StatementAttributeId, Attribute> attribute_;
  arrow::flight::FlightCallOptions call_options_;
  arrow::flight::sql::FlightSqlClient& sql_client_;
  std::shared_ptr<odbcabstraction::ResultSet> current_result_set_;
  std::shared_ptr<arrow::flight::sql::PreparedStatement> prepared_statement_;
  const odbcabstraction::MetadataSettings& metadata_settings_;

  std::shared_ptr<odbcabstraction::ResultSet> GetTables(const std::string* catalog_name,
                                                        const std::string* schema_name,
                                                        const std::string* table_name,
                                                        const std::string* table_type,
                                                        const ColumnNames& column_names);

 public:
  FlightSqlStatement(const odbcabstraction::Diagnostics& diagnostics,
                     arrow::flight::sql::FlightSqlClient& sql_client,
                     arrow::flight::FlightCallOptions call_options,
                     const odbcabstraction::MetadataSettings& metadata_settings);

  bool SetAttribute(StatementAttributeId attribute, const Attribute& value) override;

  boost::optional<Attribute> GetAttribute(StatementAttributeId attribute) override;

  boost::optional<std::shared_ptr<odbcabstraction::ResultSetMetadata>> Prepare(
      const std::string& query) override;

  bool ExecutePrepared() override;

  bool Execute(const std::string& query) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetResultSet() override;

  int64_t GetUpdateCount() override;

  std::shared_ptr<odbcabstraction::ResultSet> GetTables_V2(
      const std::string* catalog_name, const std::string* schema_name,
      const std::string* table_name, const std::string* table_type) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetTables_V3(
      const std::string* catalog_name, const std::string* schema_name,
      const std::string* table_name, const std::string* table_type) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetColumns_V2(
      const std::string* catalog_name, const std::string* schema_name,
      const std::string* table_name, const std::string* column_name) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetColumns_V3(
      const std::string* catalog_name, const std::string* schema_name,
      const std::string* table_name, const std::string* column_name) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetTypeInfo_V2(int16_t data_type) override;

  std::shared_ptr<odbcabstraction::ResultSet> GetTypeInfo_V3(int16_t data_type) override;

  odbcabstraction::Diagnostics& GetDiagnostics() override;

  void Cancel() override;
};
}  // namespace flight_sql
}  // namespace driver
