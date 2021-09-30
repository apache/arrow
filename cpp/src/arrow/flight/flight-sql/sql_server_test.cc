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

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/flight-sql/api.h>
#include <arrow/flight/flight-sql/sql_server.h>
#include <arrow/flight/test_util.h>
#include <arrow/flight/types.h>
#include <arrow/testing/gtest_util.h>
#include <gmock/gmock.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>
#include <arrow/flight/flight-sql/sql_server.h>
#include <arrow/testing/gtest_util.h>

#define unparen(...) __VA_ARGS__
#define DECLARE_ARRAY(ARRAY_NAME, TYPE_CLASS, DATA)     \
  std::shared_ptr<arrow::TYPE_CLASS##Array> ARRAY_NAME; \
  {                                                     \
    arrow::TYPE_CLASS##Builder builder;                 \
    auto data = unparen DATA;                           \
    for (const auto& item : data) {                     \
      ASSERT_OK(builder.Append(item));                  \
    }                                                   \
    ASSERT_OK(builder.Finish(&(ARRAY_NAME)));           \
  }

#define DECLARE_BINARY_ARRAY(ARRAY_NAME, DATA, LENGTH) \
  std::shared_ptr<arrow::BinaryArray> ARRAY_NAME;    \
  {                                                    \
    arrow::Binary##Builder builder;                    \
    auto data = unparen DATA;                          \
    for (const auto& item : data) {                    \
      ASSERT_OK(builder.Append(item, LENGTH));         \
    }                                                  \
    ASSERT_OK(builder.Finish(&(ARRAY_NAME)));          \
  }

#define DECLARE_NULL_ARRAY(ARRAY_NAME, TYPE_CLASS, LENGTH) \
  std::shared_ptr<arrow::TYPE_CLASS##Array> ARRAY_NAME;    \
  {                                                        \
    arrow::TYPE_CLASS##Builder builder;                    \
    for (int i = 0; i < LENGTH; i++) {                     \
      ASSERT_OK(builder.AppendNull());                     \
    }                                                      \
    ASSERT_OK(builder.Finish(&(ARRAY_NAME)));              \
  }

using ::testing::_;
using ::testing::Ref;

namespace pb = arrow::flight::protocol;

namespace arrow {
namespace flight {
namespace sql {

TestServer* server;
FlightSqlClient* sql_client;

class TestFlightSqlServer : public ::testing::Environment {
 protected:
  void SetUp() override {
    server = new TestServer("flight_sql_test_server");
    server->Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    ASSERT_TRUE(server->IsRunning());

    std::stringstream ss;
    ss << "grpc://localhost:" << server->port();
    std::string uri = ss.str();

    std::unique_ptr<FlightClient> client;
    Location location;
    ASSERT_OK(Location::Parse(uri, &location));
    ASSERT_OK(FlightClient::Connect(location, &client));

    sql_client = new FlightSqlClient(client);
  }

  void TearDown() override {
    server->Stop();

    free(server);
    free(sql_client);
  }
};

TEST(TestFlightSqlServer, TestCommandStatementQuery) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(sql_client->Execute({}, "SELECT * FROM intTable", &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  DECLARE_ARRAY(id_array, Int64, ({1, 2, 3}));
  DECLARE_ARRAY(keyname_array, String, ({"one", "zero", "negative one"}));
  DECLARE_ARRAY(value_array, Int64, ({1, 0, -1}));
  DECLARE_ARRAY(foreignId_array, Int64, ({1, 1, 1}));

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetTables) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types;
  ASSERT_OK(sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types,
                                  &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(catalog_name, String,3);
  DECLARE_NULL_ARRAY(schema_name, String, 3);
  DECLARE_ARRAY(table_name, String, ({"foreignTable", "sqlite_sequence", "intTable"}));
  DECLARE_ARRAY(table_type, String, ({"table", "table", "table"}));

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithTableFilter) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types;

  std::string table_filter_pattern = "int%";
  ASSERT_OK(sql_client->GetTables({}, nullptr, nullptr, &table_filter_pattern, false,
                                  table_types, &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(catalog_name, String, 1);
  DECLARE_NULL_ARRAY(schema_name, String, 1);
  DECLARE_ARRAY(table_name, String, ({"intTable"}));
  DECLARE_ARRAY(table_type, String, ({"table"}));

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  ASSERT_TRUE(expected_table->Equals(*table));
}


TEST(TestFlightSqlServer, TestCommandGetTablesWithTableTypesFilter) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types{"index"};

  ASSERT_OK(sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types,
                                  &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_TRUE(table->schema()->Equals(SqlSchema::GetTablesSchema()));

  ASSERT_EQ(table->num_rows(), 0);
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithUnexistenceTableTypeFilter) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types{"table"};

  ASSERT_OK(sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types,
                                  &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(catalog_name, String,3);
  DECLARE_NULL_ARRAY(schema_name, String, 3);
  DECLARE_ARRAY(table_name, String, ({"foreignTable", "sqlite_sequence", "intTable"}));
  DECLARE_ARRAY(table_type, String, ({"table", "table", "table"}));

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithIncludedSchemas) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types;

  std::string table_filter_pattern = "int%";
  ASSERT_OK(sql_client->GetTables({}, nullptr, nullptr, &table_filter_pattern, true,
                                  table_types, &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(catalog_name, String, 1);
  DECLARE_NULL_ARRAY(schema_name, String, 1);
  DECLARE_ARRAY(table_name, String, ({"intTable"}));
  DECLARE_ARRAY(table_type, String, ({"table"}));

  const std::shared_ptr<Schema> schema_table =
      arrow::schema({arrow::field("id", int64(), true, NULL),
                     arrow::field("keyName", utf8(), true, NULL),
                     arrow::field("value", int64(), true, NULL),
                     arrow::field("foreignId", int64(), true, NULL)});

  const arrow::Result<std::shared_ptr<Buffer>>& value =
      ipc::SerializeSchema(*schema_table);
  value.ValueOrDie()->data(), value.ValueOrDie()->size();

  DECLARE_BINARY_ARRAY(table_schema, ({value.ValueOrDie()->data()}),
                       value.ValueOrDie()->size());

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTablesSchemaWithIncludedSchema(),
                  {catalog_name, schema_name, table_name, table_type, table_schema});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetCatalogs) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(sql_client->GetCatalogs({}, &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetCatalogsSchema();

  ASSERT_TRUE(table->schema()->Equals(*expected_schema));
  ASSERT_EQ(0, table->num_rows());
}

TEST(TestFlightSqlServer, TestCommandGetSchemas) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(sql_client->GetSchemas({}, NULLPTR, NULLPTR, &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetSchemasSchema();

  ASSERT_TRUE(table->schema()->Equals(*expected_schema));
  ASSERT_EQ(0, table->num_rows());
}

auto env =
    ::testing::AddGlobalTestEnvironment(new TestFlightSqlServer);

}  // namespace sql
}  // namespace flight
}  // namespace arrow
