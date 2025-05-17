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

#include "arrow/flight/sql/odbc/flight_sql/include/flight_sql/flight_sql_driver.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/platform.h"
#include "arrow/flight/sql/odbc/odbcabstraction/include/odbcabstraction/types.h"

#include "arrow/flight/sql/odbc/flight_sql/flight_sql_connection.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_result_set_metadata.h"
#include "arrow/flight/sql/odbc/flight_sql/flight_sql_statement.h"

#include <iostream>
#include <memory>
#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"

using arrow::Status;
using arrow::flight::FlightClient;
using arrow::flight::Location;
using arrow::flight::sql::FlightSqlClient;

using driver::flight_sql::FlightSqlConnection;
using driver::flight_sql::FlightSqlDriver;
using driver::odbcabstraction::Connection;
using driver::odbcabstraction::ResultSet;
using driver::odbcabstraction::ResultSetMetadata;
using driver::odbcabstraction::Statement;

void TestBindColumn(const std::shared_ptr<Connection>& connection) {
  const std::shared_ptr<Statement>& statement = connection->CreateStatement();
  statement->Execute("SELECT IncidntNum, Category FROM \"@dremio\".Test LIMIT 10");

  const std::shared_ptr<ResultSet>& result_set = statement->GetResultSet();

  const int batch_size = 100;
  const int max_strlen = 1000;

  char IncidntNum[batch_size][max_strlen];
  ssize_t IncidntNum_length[batch_size];

  char Category[batch_size][max_strlen];
  ssize_t Category_length[batch_size];

  result_set->BindColumn(1, driver::odbcabstraction::CDataType_CHAR, 0, 0, IncidntNum,
                         max_strlen, IncidntNum_length);
  result_set->BindColumn(2, driver::odbcabstraction::CDataType_CHAR, 0, 0, Category,
                         max_strlen, Category_length);

  size_t total = 0;
  while (true) {
    size_t fetched_rows = result_set->Move(batch_size, 0, 0, nullptr);
    std::cout << "Fetched " << fetched_rows << " rows." << std::endl;

    total += fetched_rows;
    std::cout << "Total:" << total << std::endl;

    for (int i = 0; i < fetched_rows; ++i) {
      std::cout << "Row[" << i << "] IncidntNum: '" << IncidntNum[i] << "', Category: '"
                << Category[i] << "'" << std::endl;
    }

    if (fetched_rows < batch_size) break;
  }
}

void TestGetData(const std::shared_ptr<Connection>& connection) {
  const std::shared_ptr<Statement>& statement = connection->CreateStatement();
  statement->Execute(
      "SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL "
      "SELECT 5 UNION ALL SELECT 6");

  const std::shared_ptr<ResultSet>& result_set = statement->GetResultSet();
  const std::shared_ptr<ResultSetMetadata>& metadata = result_set->GetMetadata();

  while (result_set->Move(1, 0, 0, nullptr) == 1) {
    char result[128];
    ssize_t result_length;
    result_set->GetData(1, driver::odbcabstraction::CDataType_CHAR, 0, 0, &result,
                        sizeof(result), &result_length);
    std::cout << result << std::endl;
  }
}

void TestBindColumnBigInt(const std::shared_ptr<Connection>& connection) {
  const std::shared_ptr<Statement>& statement = connection->CreateStatement();
  statement->Execute(
      "SELECT IncidntNum, CAST(\"IncidntNum\" AS DOUBLE) / 100 AS "
      "double_field, Category\n"
      "FROM (\n"
      "  SELECT CONVERT_TO_INTEGER(IncidntNum, 1, 1, 0) AS IncidntNum, "
      "Category\n"
      "  FROM (\n"
      "    SELECT IncidntNum, Category FROM \"@dremio\".Test LIMIT 10\n"
      "  ) nested_0\n"
      ") nested_0");

  const std::shared_ptr<ResultSet>& result_set = statement->GetResultSet();

  const int batch_size = 100;
  const int max_strlen = 1000;

  char IncidntNum[batch_size][max_strlen];
  ssize_t IncidntNum_length[batch_size];

  double double_field[batch_size];
  ssize_t double_field_length[batch_size];

  char Category[batch_size][max_strlen];
  ssize_t Category_length[batch_size];

  result_set->BindColumn(1, driver::odbcabstraction::CDataType_CHAR, 0, 0, IncidntNum,
                         max_strlen, IncidntNum_length);
  result_set->BindColumn(2, driver::odbcabstraction::CDataType_DOUBLE, 0, 0, double_field,
                         max_strlen, double_field_length);
  result_set->BindColumn(3, driver::odbcabstraction::CDataType_CHAR, 0, 0, Category,
                         max_strlen, Category_length);

  size_t total = 0;
  while (true) {
    size_t fetched_rows = result_set->Move(batch_size, 0, 0, nullptr);
    std::cout << "Fetched " << fetched_rows << " rows." << std::endl;

    total += fetched_rows;
    std::cout << "Total:" << total << std::endl;

    for (int i = 0; i < fetched_rows; ++i) {
      std::cout << "Row[" << i << "] IncidntNum: '" << IncidntNum[i] << "', "
                << "double_field: '" << double_field[i] << "', "
                << "Category: '" << Category[i] << "'" << std::endl;
    }

    if (fetched_rows < batch_size) break;
  }
}

void TestGetTablesV2(const std::shared_ptr<Connection>& connection) {
  const std::shared_ptr<Statement>& statement = connection->CreateStatement();
  const std::shared_ptr<ResultSet>& result_set =
      statement->GetTables_V2(nullptr, nullptr, nullptr, nullptr);

  const std::shared_ptr<ResultSetMetadata>& metadata = result_set->GetMetadata();
  size_t column_count = metadata->GetColumnCount();

  while (result_set->Move(1, 0, 0, nullptr) == 1) {
    int buffer_length = 1024;
    std::vector<char> result(buffer_length);
    ssize_t result_length;
    result_set->GetData(1, driver::odbcabstraction::CDataType_CHAR, 0, 0, result.data(),
                        buffer_length, &result_length);
    std::cout << result.data() << std::endl;
  }

  std::cout << column_count << std::endl;
}

void TestGetColumnsV3(const std::shared_ptr<Connection>& connection) {
  const std::shared_ptr<Statement>& statement = connection->CreateStatement();
  std::string table_name = "test_numeric";
  std::string column_name = "%";
  const std::shared_ptr<ResultSet>& result_set =
      statement->GetColumns_V3(nullptr, nullptr, &table_name, &column_name);

  const std::shared_ptr<ResultSetMetadata>& metadata = result_set->GetMetadata();
  size_t column_count = metadata->GetColumnCount();

  int buffer_length = 1024;
  std::vector<char> result(buffer_length);
  ssize_t result_length;

  while (result_set->Move(1, 0, 0, nullptr) == 1) {
    for (int i = 0; i < column_count; ++i) {
      result_set->GetData(1 + i, driver::odbcabstraction::CDataType_CHAR, 0, 0,
                          result.data(), buffer_length, &result_length);
      std::cout << (result_length != -1 ? result.data() : "NULL") << '\t';
    }

    std::cout << std::endl;
  }

  std::cout << column_count << std::endl;
}

int main() {
  FlightSqlDriver driver;

  const std::shared_ptr<Connection>& connection =
      driver.CreateConnection(driver::odbcabstraction::V_3);

  Connection::ConnPropertyMap properties = {
      {FlightSqlConnection::HOST, std::string("automaster.drem.io")},
      {FlightSqlConnection::PORT, std::string("32010")},
      {FlightSqlConnection::USER, std::string("dremio")},
      {FlightSqlConnection::PASSWORD, std::string("dremio123")},
      {FlightSqlConnection::USE_ENCRYPTION, std::string("false")},
  };
  std::vector<std::string_view> missing_attr;
  connection->Connect(properties, missing_attr);

  //  TestBindColumnBigInt(connection);
  //    TestBindColumn(connection);
  TestGetData(connection);
  //  TestGetTablesV2(connection);
  //    TestGetColumnsV3(connection);

  connection->Close();
  return 0;
}
