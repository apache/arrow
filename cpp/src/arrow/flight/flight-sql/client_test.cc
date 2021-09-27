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
using ::testing::_;
using ::testing::Ref;

namespace arrow {
namespace flight {
namespace sql {

using internal::FlightSqlClientT;

class FlightClientMock {
 public:
  MOCK_METHOD(Status, GetFlightInfo,
              (const FlightCallOptions&, const FlightDescriptor&,
                  std::unique_ptr<FlightInfo>*));
  MOCK_METHOD(Status, DoPut,
              (const FlightCallOptions&, const FlightDescriptor&,
                  const std::shared_ptr<Schema>& schema,
                  std::unique_ptr<FlightStreamWriter>*,
                  std::unique_ptr<FlightMetadataReader>*));
};

class FlightMetadataReaderMock : public FlightMetadataReader {
 public:
  std::shared_ptr<Buffer>* buffer;

  explicit FlightMetadataReaderMock(std::shared_ptr<Buffer>* buffer) {
    this->buffer = buffer;
  }

  Status ReadMetadata(std::shared_ptr<Buffer>* out) override {
    *out = *buffer;
    return Status::OK();
  }
};

FlightDescriptor getDescriptor(google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

TEST(TestFlightSqlClient, TestGetCatalogs) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  pb::sql::CommandGetCatalogs command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetCatalogs(call_options, &flight_info);
}

TEST(TestFlightSqlClient, TestGetSchemas) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string catalog = "catalog";

  pb::sql::CommandGetSchemas command;
  command.set_catalog(catalog);
  command.set_schema_filter_pattern(schema_filter_pattern);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetSchemas(call_options, &catalog,
                              &schema_filter_pattern, &flight_info);
}

TEST(TestFlightSqlClient, TestGetTables) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
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
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetTables(call_options, &catalog, &schema_filter_pattern,
                             &table_name_filter_pattern, include_schema,
                             table_types, &flight_info);
}

TEST(TestFlightSqlClient, TestGetTableTypes) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  pb::sql::CommandGetTableTypes command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetTableTypes(call_options, &flight_info);
}

TEST(TestFlightSqlClient, TestGetExported) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
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
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetExportedKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestGetImported) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
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
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetImportedKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestGetPrimary) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
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
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.GetPrimaryKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestExecute) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementQuery command;
  command.set_query(query);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock,
              GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void) sqlClient.Execute(call_options, query, &flight_info);
}

TEST(TestFlightSqlClient, TestExecuteUpdate) {
  auto *client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementUpdate command;

  command.set_query(query);

  google::protobuf::Any any;
  any.PackFrom(command);

  const FlightDescriptor& descriptor = FlightDescriptor::Command(any.SerializeAsString());

  pb::sql::DoPutUpdateResult doPutUpdateResult;
  doPutUpdateResult.set_record_count(100);
  const std::string& string = doPutUpdateResult.SerializeAsString();

  auto buffer_ptr = std::make_shared<Buffer>(
      reinterpret_cast<const uint8_t*>(string.data()), doPutUpdateResult.ByteSizeLong());

  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));

        return Status::OK();
      });

  std::unique_ptr<FlightInfo> flight_info;
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  EXPECT_CALL(*client_mock, DoPut(Ref(call_options), descriptor, _, _, _));

  int64_t num_rows;

  (void) sqlClient.ExecuteUpdate(call_options, query, &num_rows);

  ASSERT_EQ(num_rows, 100);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
