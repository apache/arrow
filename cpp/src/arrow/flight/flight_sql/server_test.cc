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
#include <arrow/flight/flight_sql/FlightSql.pb.h>
#include <arrow/flight/flight_sql/api.h>
#include <arrow/flight/flight_sql/example/sqlite_server.h>
#include <arrow/flight/flight_sql/server.h>
#include <arrow/flight/test_util.h>
#include <arrow/flight/types.h>
#include <arrow/testing/gtest_util.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
    for (int i = 0; i < 100; i++) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (server->IsRunning()) {
        break;
      }
    }
    ASSERT_TRUE(server->IsRunning());

    std::stringstream ss;
    ss << "grpc://localhost:" << server->port();
    std::string uri = ss.str();

    std::unique_ptr<FlightClient> client;
    Location location;
    ASSERT_OK(Location::Parse(uri, &location));
    ASSERT_OK(FlightClient::Connect(location, &client));

    sql_client = new FlightSqlClient(std::move(client));
  }

  void TearDown() override {
    for (int i = 0; i < 100; i++) {
     std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (!(server->IsRunning())) {
        break;
      }
    }
    delete server;
    delete sql_client;
  }
};

TEST(TestFlightSqlServer, TestCommandStatementQuery) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->Execute({}, "SELECT * FROM intTable"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  std::shared_ptr<Array> id_array;
  std::shared_ptr<Array> keyname_array;
  std::shared_ptr<Array> value_array;
  std::shared_ptr<Array> foreignId_array;
  ArrayFromVector<Int64Type, std::int64_t>({1, 2, 3}, &id_array);
  ArrayFromVector<StringType, std::string>({"one", "zero", "negative one"},
                                           &keyname_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, 0, -1}, &value_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, 1, 1}, &foreignId_array);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetTables) {
  std::vector<std::string> table_types;
  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 3))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 3))
  std::shared_ptr<Array> table_name;
  ArrayFromVector<StringType, std::string>(
      {"foreignTable", "intTable", "sqlite_sequence"}, &table_name);
  std::shared_ptr<Array> table_type;
  ArrayFromVector<StringType, std::string>({"table", "table", "table"}, &table_type);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithTableFilter) {
  std::vector<std::string> table_types;

  std::string table_filter_pattern = "int%";
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetTables({}, nullptr, nullptr, &table_filter_pattern,
                                             false, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 1))
  std::shared_ptr<Array> table_name;
  std::shared_ptr<Array> table_type;
  ArrayFromVector<StringType, std::string>({"intTable"}, &table_name);
  ArrayFromVector<StringType, std::string>({"table"}, &table_type);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithTableTypesFilter) {
  std::vector<std::string> table_types{"index"};

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_TRUE(table->schema()->Equals(SqlSchema::GetTablesSchema()));

  ASSERT_EQ(table->num_rows(), 0);
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithUnexistenceTableTypeFilter) {
  std::vector<std::string> table_types{"table"};

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables({}, nullptr, nullptr, nullptr, false, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 3))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 3))
  std::shared_ptr<Array> table_name;
  ArrayFromVector<StringType, std::string>(
      {
          "foreignTable",
          "intTable",
          "sqlite_sequence",
      },
      &table_name);
  std::shared_ptr<Array> table_type;
  ArrayFromVector<StringType, std::string>({"table", "table", "table"}, &table_type);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetTablesWithIncludedSchemas) {
  std::vector<std::string> table_types;

  std::string table_filter_pattern = "int%";
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetTables({}, nullptr, nullptr, &table_filter_pattern,
                                             true, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 1))
  std::shared_ptr<Array> table_name;
  std::shared_ptr<Array> table_type;
  ArrayFromVector<StringType, std::string>({"intTable"}, &table_name);
  ArrayFromVector<StringType, std::string>({"table"}, &table_type);

  const std::shared_ptr<Schema> schema_table =
      arrow::schema({arrow::field("id", int64(), true, NULL),
                     arrow::field("keyName", utf8(), true, NULL),
                     arrow::field("value", int64(), true, NULL),
                     arrow::field("foreignId", int64(), true, NULL)});

  const arrow::Result<std::shared_ptr<Buffer>>& value =
      ipc::SerializeSchema(*schema_table);
  std::shared_ptr<Buffer> schema_buffer;

  ASSERT_OK_AND_ASSIGN(schema_buffer, value);
  std::shared_ptr<Array> table_schema;

  ArrayFromVector<BinaryType, std::string>({schema_buffer->ToString()}, &table_schema);

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTablesSchemaWithIncludedSchema(),
                  {catalog_name, schema_name, table_name, table_type, table_schema});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetCatalogs) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetCatalogs({}));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetCatalogsSchema();

  ASSERT_TRUE(table->schema()->Equals(*expected_schema));
  ASSERT_EQ(0, table->num_rows());
}

TEST(TestFlightSqlServer, TestCommandGetSchemas) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetSchemas({}, NULLPTR, NULLPTR));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetSchemasSchema();

  ASSERT_TRUE(table->schema()->Equals(*expected_schema));
  ASSERT_EQ(0, table->num_rows());
}

TEST(TestFlightSqlServer, TestCommandGetTableTypes) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetTableTypes({}));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  std::shared_ptr<Array> table_type;
  ArrayFromVector<StringType, std::string>({"table"}, &table_type);

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTableTypesSchema(), {table_type});
  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandStatementUpdate) {
  int64_t result;
  ASSERT_OK_AND_ASSIGN(result,
                       sql_client->ExecuteUpdate(
                           {},
                           "INSERT INTO intTable (keyName, value) VALUES "
                           "('KEYNAME1', 1001), ('KEYNAME2', 1002), ('KEYNAME3', 1003)"));
  ASSERT_EQ(3, result);

  ASSERT_OK_AND_ASSIGN(result, sql_client->ExecuteUpdate(
                                   {},
                                   "UPDATE intTable SET keyName = 'KEYNAME1' "
                                   "WHERE keyName = 'KEYNAME2' OR keyName = 'KEYNAME3'"));
  ASSERT_EQ(2, result);

  ASSERT_OK_AND_ASSIGN(
      result,
      sql_client->ExecuteUpdate({}, "DELETE FROM intTable WHERE keyName = 'KEYNAME1'"));
  ASSERT_EQ(3, result);
}

TEST(TestFlightSqlServer, TestCommandPreparedStatementQuery) {
  ASSERT_OK_AND_ASSIGN(auto prepared_statement,
                       sql_client->Prepare({}, "SELECT * FROM intTable"));

  ASSERT_OK_AND_ASSIGN(auto flight_info, prepared_statement->Execute());

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  std::shared_ptr<Array> id_array;
  std::shared_ptr<Array> keyname_array;
  std::shared_ptr<Array> value_array;
  std::shared_ptr<Array> foreignId_array;
  ArrayFromVector<Int64Type, std::int64_t>({1, 2, 3}, &id_array);
  ArrayFromVector<StringType, std::string>({"one", "zero", "negative one"},
                                           &keyname_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, 0, -1}, &value_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, 1, 1}, &foreignId_array);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandPreparedStatementQueryWithParameterBinding) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare({}, "SELECT * FROM intTable WHERE keyName LIKE ?"));

  auto parameter_schema = prepared_statement->parameter_schema();

  const std::shared_ptr<Schema>& expected_parameter_schema =
      arrow::schema({arrow::field("parameter_1", example::GetUnknownColumnDataType())});

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

  ASSERT_OK_AND_ASSIGN(auto flight_info, prepared_statement->Execute());

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  std::shared_ptr<Array> id_array;
  std::shared_ptr<Array> keyname_array;
  std::shared_ptr<Array> value_array;
  std::shared_ptr<Array> foreignId_array;
  ArrayFromVector<Int64Type, std::int64_t>({1, 3}, &id_array);
  ArrayFromVector<StringType, std::string>({"one", "negative one"}, &keyname_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, -1}, &value_array);
  ArrayFromVector<Int64Type, std::int64_t>({1, 1}, &foreignId_array);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

arrow::Result<int64_t> ExecuteCountQuery(const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto flight_info, sql_client->Execute({}, query));

  ARROW_ASSIGN_OR_RAISE(auto stream,
                        sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ARROW_RETURN_NOT_OK(stream->ReadAll(&table));

  const std::shared_ptr<Array>& result_array = table->column(0)->chunk(0);
  ARROW_ASSIGN_OR_RAISE(auto count_scalar, result_array->GetScalar(0));

  return reinterpret_cast<Int64Scalar&>(*count_scalar).value;
}

TEST(TestFlightSqlServer, TestCommandPreparedStatementUpdateWithParameterBinding) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare(
          {}, "INSERT INTO INTTABLE (keyName, value) VALUES ('new_value', ?)"));

  auto parameter_schema = prepared_statement->parameter_schema();

  const std::shared_ptr<Schema>& expected_parameter_schema =
      arrow::schema({arrow::field("parameter_1", example::GetUnknownColumnDataType())});

  ASSERT_TRUE(expected_parameter_schema->Equals(*parameter_schema));

  std::shared_ptr<Array> type_ids;
  ArrayFromVector<Int8Type>({2}, &type_ids);
  std::shared_ptr<Array> offsets;
  ArrayFromVector<Int32Type>({0}, &offsets);

  std::shared_ptr<Array> string_array;
  ArrayFromVector<StringType, std::string>({}, &string_array);
  std::shared_ptr<Array> bytes_array;
  ArrayFromVector<BinaryType, std::string>({}, &bytes_array);
  std::shared_ptr<Array> bigint_array;
  ArrayFromVector<Int64Type>({999}, &bigint_array);
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

  int64_t result;
  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(3, result);

  ASSERT_OK_AND_ASSIGN(result, prepared_statement->ExecuteUpdate());
  ASSERT_EQ(1, result);

  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(4, result);

  ASSERT_OK_AND_ASSIGN(
      result,
      sql_client->ExecuteUpdate({}, "DELETE FROM intTable WHERE keyName = 'new_value'"));
  ASSERT_EQ(1, result);

  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(3, result);
}

TEST(TestFlightSqlServer, TestCommandPreparedStatementUpdate) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare(
          {}, "INSERT INTO INTTABLE (keyName, value) VALUES ('new_value', 999)"));

  int64_t result;
  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(3, result);

  ASSERT_OK_AND_ASSIGN(result, prepared_statement->ExecuteUpdate());
  ASSERT_EQ(1, result);

  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(4, result);

  ASSERT_OK_AND_ASSIGN(
      result,
      sql_client->ExecuteUpdate({}, "DELETE FROM intTable WHERE keyName = 'new_value'"));
  ASSERT_EQ(1, result);

  ASSERT_OK_AND_ASSIGN(result, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
  ASSERT_EQ(3, result);
}

TEST(TestFlightSqlServer, TestCommandGetPrimaryKeys) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetPrimaryKeys({}, nullptr, nullptr, "int%"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto key_name, MakeArrayOfNull(utf8(), 1))

  std::shared_ptr<Array> table_name;
  std::shared_ptr<Array> column_name;
  std::shared_ptr<Array> key_sequence;
  ArrayFromVector<StringType, std::string>({"intTable"}, &table_name);
  ArrayFromVector<StringType, std::string>({"id"}, &column_name);
  ArrayFromVector<Int64Type, std::int64_t>({1}, &key_sequence);

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetPrimaryKeysSchema(),
      {catalog_name, schema_name, table_name, column_name, key_sequence, key_name});

  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetImportedKeys) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetImportedKeys({}, NULLPTR, NULLPTR, "intTable"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  std::shared_ptr<Array> pk_table_name;
  std::shared_ptr<Array> pk_column_name;
  std::shared_ptr<Array> fk_table_name;
  std::shared_ptr<Array> fk_column_name;
  std::shared_ptr<Array> key_sequence;
  std::shared_ptr<Array> update_rule;
  std::shared_ptr<Array> delete_rule;
  ArrayFromVector<StringType, std::string>({"foreignTable"}, &pk_table_name);
  ArrayFromVector<StringType, std::string>({"id"}, &pk_column_name);
  ArrayFromVector<StringType, std::string>({"intTable"}, &fk_table_name);
  ArrayFromVector<StringType, std::string>({"foreignId"}, &fk_column_name);
  ArrayFromVector<Int32Type, std::int32_t>({0}, &key_sequence);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &update_rule);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &delete_rule);

  ASSERT_OK_AND_ASSIGN(auto pk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_key_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_key_name, MakeArrayOfNull(utf8(), 1))

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetImportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetExportedKeys) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetExportedKeys({}, NULLPTR, NULLPTR, "foreignTable"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  std::shared_ptr<Array> pk_table_name;
  std::shared_ptr<Array> pk_column_name;
  std::shared_ptr<Array> fk_table_name;
  std::shared_ptr<Array> fk_column_name;
  std::shared_ptr<Array> key_sequence;
  std::shared_ptr<Array> update_rule;
  std::shared_ptr<Array> delete_rule;
  ArrayFromVector<StringType, std::string>({"foreignTable"}, &pk_table_name);
  ArrayFromVector<StringType, std::string>({"id"}, &pk_column_name);
  ArrayFromVector<StringType, std::string>({"intTable"}, &fk_table_name);
  ArrayFromVector<StringType, std::string>({"foreignId"}, &fk_column_name);
  ArrayFromVector<Int32Type, std::int32_t>({0}, &key_sequence);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &update_rule);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &delete_rule);

  ASSERT_OK_AND_ASSIGN(auto pk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_key_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_key_name, MakeArrayOfNull(utf8(), 1))

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetExportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

TEST(TestFlightSqlServer, TestCommandGetCrossReference) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetCrossReference({}, NULLPTR, NULLPTR, "foreignTable",
                                                     NULLPTR, NULLPTR, "intTable"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  std::shared_ptr<Array> pk_table_name;
  std::shared_ptr<Array> pk_column_name;
  std::shared_ptr<Array> fk_table_name;
  std::shared_ptr<Array> fk_column_name;
  std::shared_ptr<Array> key_sequence;
  std::shared_ptr<Array> update_rule;
  std::shared_ptr<Array> delete_rule;
  ArrayFromVector<StringType, std::string>({"foreignTable"}, &pk_table_name);
  ArrayFromVector<StringType, std::string>({"id"}, &pk_column_name);
  ArrayFromVector<StringType, std::string>({"intTable"}, &fk_table_name);
  ArrayFromVector<StringType, std::string>({"foreignId"}, &fk_column_name);
  ArrayFromVector<Int32Type, std::int32_t>({0}, &key_sequence);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &update_rule);
  ArrayFromVector<UInt8Type, std::uint8_t>({3}, &delete_rule);

  ASSERT_OK_AND_ASSIGN(auto pk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_catalog_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_schema_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto fk_key_name, MakeArrayOfNull(utf8(), 1))
  ASSERT_OK_AND_ASSIGN(auto pk_key_name, MakeArrayOfNull(utf8(), 1))
  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetCrossReferenceSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

auto env = ::testing::AddGlobalTestEnvironment(new TestFlightSqlServer);

}  // namespace sql
}  // namespace flight
}  // namespace arrow
