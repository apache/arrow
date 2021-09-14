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

#include <arrow/flight/api.h>
#include <arrow/flight/flight-sql/FlightSql.pb.h>
#include <arrow/flight/flight-sql/api.h>
#include <arrow/flight/types.h>
#include <gmock/gmock.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

namespace pb = arrow::flight::protocol;
using ::testing::Ref;


namespace arrow {
namespace flight {
namespace sql {

using internal::FlightSqlClientT;

class FlightClientMock {
 public:
  MOCK_METHOD(Status, GetFlightInfo, (const FlightCallOptions&, const FlightDescriptor&,
          std::unique_ptr<FlightInfo>*));
};

FlightDescriptor getDescriptor(google::protobuf::Message &command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string &string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

TEST(TestFlightSql, TestGetCatalogs) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  pb::sql::CommandGetCatalogs command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetCatalogs(call_options, &flight_info);
}

TEST(TestFlightSql, TestGetSchemas) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string catalog = "catalog";

  pb::sql::CommandGetSchemas command;
  command.set_catalog(catalog);
  command.set_schema_filter_pattern(schema_filter_pattern);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetSchemas(call_options, &flight_info, &catalog,
                              &schema_filter_pattern);
}

TEST(TestFlightSql, TestGetTables) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string table_name_filter_pattern = "table_name_filter_pattern";
  bool include_schema = true;
  std::vector<std::string> table_types = {"type1", "type2"};

  pb::sql::CommandGetTables command;
  command.set_catalog(catalog);
  command.set_schema_filter_pattern(schema_filter_pattern);
  command.set_table_name_filter_pattern(table_name_filter_pattern);
  command.set_include_schema(include_schema);
  for (const std::string &table_type : table_types) {
    command.add_table_types(table_type);
  }
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetTables(call_options, &flight_info, &catalog, &schema_filter_pattern,
                             &table_name_filter_pattern, include_schema, table_types);
}

TEST(TestFlightSql, TestGetTableTypes) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  pb::sql::CommandGetTableTypes command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetTableTypes(call_options, &flight_info);
}

TEST(TestFlightSql, TestGetExported) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetExportedKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetExportedKeys(call_options, &flight_info, &catalog, &schema, table);
}

TEST(TestFlightSql, TestGetImported) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetImportedKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetImportedKeys(call_options, &flight_info, &catalog, &schema, table);
}

TEST(TestFlightSql, TestGetPrimary) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetPrimaryKeys command;
  command.set_catalog(catalog);
  command.set_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetPrimaryKeys(call_options, &flight_info, &catalog, &schema, table);
}

TEST(TestFlightSql, TestExecute) {
  FlightClientMock flightClientMock;
  FlightSqlClientT<FlightClientMock> sqlClient(&flightClientMock);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementQuery command;
  command.set_query(query);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(flightClientMock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.Execute(call_options, &flight_info, query);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
