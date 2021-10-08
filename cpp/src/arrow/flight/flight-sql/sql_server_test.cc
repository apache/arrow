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
#include <arrow/flight/flight-sql/example/sqlite_server.h>
#include <arrow/flight/test_util.h>
#include <arrow/flight/types.h>
#include <arrow/testing/gtest_util.h>
#include <gmock/gmock.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

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
  std::shared_ptr<arrow::BinaryArray> ARRAY_NAME;      \
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

  DECLARE_NULL_ARRAY(catalog_name, String, 3);
  DECLARE_NULL_ARRAY(schema_name, String, 3);
  DECLARE_ARRAY(table_name, String, ({"foreignTable", "intTable", "sqlite_sequence", }));
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

  DECLARE_NULL_ARRAY(catalog_name, String, 3);
  DECLARE_NULL_ARRAY(schema_name, String, 3);
  DECLARE_ARRAY(table_name, String, ({"foreignTable", "intTable", "sqlite_sequence", }));
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

TEST(TestFlightSqlServer, TestCommandGetTableTypes) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(sql_client->GetTableTypes({}, &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_ARRAY(table_type, String, ({"table"}));

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTableTypesSchema(), {table_type});
  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandStatementUpdate) {
  int64_t result;
  ASSERT_OK(sql_client->ExecuteUpdate(
      {},
      "INSERT INTO intTable (keyName, value) VALUES "
      "('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)",
      &result));
  ASSERT_EQ(3, result);

  ASSERT_OK(
      sql_client->ExecuteUpdate({},
                                "UPDATE intTable SET keyName = 'KEYNAME1' "
                                "WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'",
                                &result));
  ASSERT_EQ(2, result);

  ASSERT_OK(sql_client->ExecuteUpdate(
      {}, "DELETE FROM intTable WHERE keyName = 'KEYNAME1'", &result));
  ASSERT_EQ(3, result);
}

TEST(TestFlightSqlServer, TestCommandPreparedStatementQuery) {
  std::shared_ptr<PreparedStatement> prepared_statement;
  ASSERT_OK(sql_client->Prepare({}, "SELECT * FROM intTable", &prepared_statement));

  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(prepared_statement->Execute(&flight_info));

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

TEST(TestFlightSqlServer, TestCommandPreparedStatementQueryWithParameterBinding) {
  std::shared_ptr<PreparedStatement> prepared_statement;
  ASSERT_OK(sql_client->Prepare({}, "SELECT * FROM intTable WHERE keyName LIKE ?",
                                &prepared_statement));

  std::shared_ptr<Schema> parameter_schema;
  ASSERT_OK(prepared_statement->GetParameterSchema(&parameter_schema));

  const std::shared_ptr<Schema>& expected_parameter_schema = arrow::schema(
      {arrow::field("parameter_1", example::GetUnknownColumnDataType())});

  ASSERT_TRUE(expected_parameter_schema->Equals(*parameter_schema));

  std::shared_ptr<Array> type_ids;
  ArrayFromVector<Int8Type>({0}, &type_ids);
  std::shared_ptr<Array> offsets;
  ArrayFromVector<Int32Type>({0}, &offsets);

  std::shared_ptr<Array> string_array;
  ArrayFromVector<StringType, std::string>({"%one"}, &string_array);
  std::shared_ptr<Array> bytes_array;
  ArrayFromVector<BinaryType, std::string>({}, &bytes_array);
  std::shared_ptr<Array> bigint_array;
  ArrayFromVector<Int64Type>({}, &bigint_array);
  std::shared_ptr<Array> double_array;
  ArrayFromVector<FloatType>({}, &double_array);

  ASSERT_OK_AND_ASSIGN(
      auto parameter_1_array,
      DenseUnionArray::Make(*type_ids, *offsets,
                            {string_array, bytes_array, bigint_array, double_array},
                            {"string", "bytes", "bigint", "double"}, {0, 1, 2, 3}));

  const std::shared_ptr<RecordBatch>& record_batch =
      RecordBatch::Make(parameter_schema, 1, {parameter_1_array});

  ASSERT_OK(prepared_statement->SetParameters(record_batch));

  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(prepared_statement->Execute(&flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  DECLARE_ARRAY(id_array, Int64, ({1, 3}));
  DECLARE_ARRAY(keyname_array, String, ({"one", "negative one"}));
  DECLARE_ARRAY(value_array, Int64, ({1, -1}));
  DECLARE_ARRAY(foreignId_array, Int64, ({1, 1}));

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetPrimaryKeys) {
  std::unique_ptr<FlightInfo> flight_info;
  std::vector<std::string> table_types;
  ASSERT_OK(sql_client->GetPrimaryKeys({}, nullptr, nullptr, "int%",
                                  &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(catalog_name, String, 1);
  DECLARE_NULL_ARRAY(schema_name, String, 1);
  DECLARE_ARRAY(table_name, String, ({"intTable"}));
  DECLARE_ARRAY(column_name, String, ({"id"}));
  DECLARE_ARRAY(key_sequence, Int64, ({1}));
  DECLARE_NULL_ARRAY(key_name, String, 1);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetPrimaryKeysSchema(),
      {catalog_name, schema_name, table_name, column_name, key_sequence, key_name});

  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetImportedKeys) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(sql_client->GetImportedKeys({}, NULLPTR, NULLPTR, "intTable", &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(pk_catalog_name, String, 1);
  DECLARE_NULL_ARRAY(pk_schema_name, String, 1);
  DECLARE_ARRAY(pk_table_name, String, ({"foreignTable"}));
  DECLARE_ARRAY(pk_column_name, String, ({"id"}));
  DECLARE_NULL_ARRAY(fk_catalog_name, String, 1);
  DECLARE_NULL_ARRAY(fk_schema_name, String, 1);
  DECLARE_ARRAY(fk_table_name, String, ({"intTable"}));
  DECLARE_ARRAY(fk_column_name, String, ({"foreignId"}));
  DECLARE_ARRAY(key_sequence, Int32, ({0}));
  DECLARE_NULL_ARRAY(fk_key_name, String, 1);
  DECLARE_NULL_ARRAY(pk_key_name, String, 1);
  DECLARE_ARRAY(update_rule, UInt8, ({3}));
  DECLARE_ARRAY(delete_rule, UInt8, ({3}));

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetImportedAndExportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  ASSERT_TRUE(expected_table->Equals(*table));
}

TEST(TestFlightSqlServer, TestCommandGetExportedKeys) {
  std::unique_ptr<FlightInfo> flight_info;
  ASSERT_OK(
      sql_client->GetExportedKeys({}, NULLPTR, NULLPTR, "foreignTable", &flight_info));

  std::unique_ptr<FlightStreamReader> stream;
  ASSERT_OK(sql_client->DoGet({}, flight_info->endpoints()[0].ticket, &stream));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  DECLARE_NULL_ARRAY(pk_catalog_name, String, 1);
  DECLARE_NULL_ARRAY(pk_schema_name, String, 1);
  DECLARE_ARRAY(pk_table_name, String, ({"foreignTable"}));
  DECLARE_ARRAY(pk_column_name, String, ({"id"}));
  DECLARE_NULL_ARRAY(fk_catalog_name, String, 1);
  DECLARE_NULL_ARRAY(fk_schema_name, String, 1);
  DECLARE_ARRAY(fk_table_name, String, ({"intTable"}));
  DECLARE_ARRAY(fk_column_name, String, ({"foreignId"}));
  DECLARE_ARRAY(key_sequence, Int32, ({0}));
  DECLARE_NULL_ARRAY(fk_key_name, String, 1);
  DECLARE_NULL_ARRAY(pk_key_name, String, 1);
  DECLARE_ARRAY(update_rule, UInt8, ({3}));
  DECLARE_ARRAY(delete_rule, UInt8, ({3}));

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetImportedAndExportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  ASSERT_TRUE(expected_table->Equals(*table));
}

auto env =
    ::testing::AddGlobalTestEnvironment(new TestFlightSqlServer);

}  // namespace sql
}  // namespace flight
}  // namespace arrow
