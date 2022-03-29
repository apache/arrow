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

#include "arrow/flight/sql/server.h"

#include <arrow/util/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <sqlite3.h>

#include <condition_variable>
#include <thread>

#include "arrow/flight/api.h"
#include "arrow/flight/sql/api.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/flight/sql/example/sqlite_sql_info.h"
#include "arrow/flight/sql/example/sqlite_type_info.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/testing/builder.h"
#include "arrow/testing/gtest_util.h"

using ::testing::_;
using ::testing::Ref;

using arrow::internal::checked_cast;

namespace arrow {
namespace flight {
namespace sql {

/// \brief Auxiliary variant visitor used to assert that GetSqlInfo's values are
/// correctly placed on its DenseUnionArray
class SqlInfoDenseUnionValidator {
 private:
  const DenseUnionScalar& data;

 public:
  /// \brief Asserts that the current DenseUnionScalar equals to given string value
  void operator()(const std::string& string_value) const {
    const auto& scalar = checked_cast<const StringScalar&>(*data.value);
    ASSERT_EQ(string_value, scalar.ToString());
  }

  /// \brief Asserts that the current DenseUnionScalar equals to given bool value
  void operator()(const bool bool_value) const {
    const auto& scalar = checked_cast<const BooleanScalar&>(*data.value);
    ASSERT_EQ(bool_value, scalar.value);
  }

  /// \brief Asserts that the current DenseUnionScalar equals to given int64_t value
  void operator()(const int64_t bigint_value) const {
    const auto& scalar = checked_cast<const Int64Scalar&>(*data.value);
    ASSERT_EQ(bigint_value, scalar.value);
  }

  /// \brief Asserts that the current DenseUnionScalar equals to given int32_t value
  void operator()(const int32_t int32_bitmask) const {
    const auto& scalar = checked_cast<const Int32Scalar&>(*data.value);
    ASSERT_EQ(int32_bitmask, scalar.value);
  }

  /// \brief Asserts that the current DenseUnionScalar equals to given string list
  void operator()(const std::vector<std::string>& string_list) const {
    const auto& array = checked_cast<const StringArray&>(
        *(checked_cast<const ListScalar&>(*data.value).value));

    ASSERT_EQ(string_list.size(), array.length());

    for (size_t index = 0; index < string_list.size(); index++) {
      ASSERT_EQ(string_list[index], array.GetString(index));
    }
  }

  /// \brief Asserts that the current DenseUnionScalar equals to given int32 to int32 list
  /// map.
  void operator()(const std::unordered_map<int32_t, std::vector<int32_t>>&
                      int32_to_int32_list) const {
    const auto& struct_array = checked_cast<const StructArray&>(
        *checked_cast<const MapScalar&>(*data.value).value);
    const auto& keys = checked_cast<const Int32Array&>(*struct_array.field(0));
    const auto& values = checked_cast<const ListArray&>(*struct_array.field(1));

    // Assert that the given map has the right size
    ASSERT_EQ(int32_to_int32_list.size(), keys.length());

    // For each element on given MapScalar, assert it matches the argument
    for (int i = 0; i < keys.length(); i++) {
      ASSERT_OK_AND_ASSIGN(const auto& key_scalar, keys.GetScalar(i));
      int32_t sql_info_id = checked_cast<const Int32Scalar&>(*key_scalar).value;

      // Assert the key (SqlInfo id) exists
      ASSERT_TRUE(int32_to_int32_list.count(sql_info_id));

      const std::vector<int32_t>& expected_int32_list =
          int32_to_int32_list.at(sql_info_id);

      // Assert the value (int32 list) has the correct size
      ASSERT_EQ(expected_int32_list.size(), values.value_length(i));

      // For each element on current ListScalar, assert it matches with the argument
      for (size_t j = 0; j < expected_int32_list.size(); j++) {
        ASSERT_OK_AND_ASSIGN(auto list_item_scalar,
                             values.values()->GetScalar(values.value_offset(i) + j));
        const auto& list_item = checked_cast<const Int32Scalar&>(*list_item_scalar).value;
        ASSERT_EQ(expected_int32_list[j], list_item);
      }
    }
  }

  explicit SqlInfoDenseUnionValidator(const DenseUnionScalar& data) : data(data) {}

  SqlInfoDenseUnionValidator(const SqlInfoDenseUnionValidator&) = delete;
  SqlInfoDenseUnionValidator(SqlInfoDenseUnionValidator&&) = delete;
  SqlInfoDenseUnionValidator& operator=(const SqlInfoDenseUnionValidator&) = delete;
};

class TestFlightSqlServer : public ::testing::Test {
 public:
  std::unique_ptr<FlightSqlClient> sql_client;

  arrow::Result<int64_t> ExecuteCountQuery(const std::string& query) {
    ARROW_ASSIGN_OR_RAISE(auto flight_info, sql_client->Execute({}, query));

    ARROW_ASSIGN_OR_RAISE(auto stream,
                          sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

    ARROW_ASSIGN_OR_RAISE(auto table, stream->ToTable());

    const std::shared_ptr<Array>& result_array = table->column(0)->chunk(0);
    ARROW_ASSIGN_OR_RAISE(auto count_scalar, result_array->GetScalar(0));

    return reinterpret_cast<Int64Scalar&>(*count_scalar).value;
  }

 protected:
  void SetUp() override {
    port = GetListenPort();
    server_thread.reset(new std::thread([&]() { RunServer(); }));

    std::unique_lock<std::mutex> lk(server_ready_m);
    server_ready_cv.wait(lk);

    std::stringstream ss;
    ss << "grpc://localhost:" << port;
    std::string uri = ss.str();

    std::unique_ptr<FlightClient> client;
    ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(uri));
    ASSERT_OK(FlightClient::Connect(location, &client));

    sql_client.reset(new FlightSqlClient(std::move(client)));
  }

  void TearDown() override {
    ASSERT_OK(sql_client->Close());
    sql_client.reset();

    ASSERT_OK(server->Shutdown());
    server_thread->join();
    server_thread.reset();
  }

 private:
  int port;
  std::shared_ptr<arrow::flight::sql::example::SQLiteFlightSqlServer> server;
  std::unique_ptr<std::thread> server_thread;
  std::condition_variable server_ready_cv;
  std::mutex server_ready_m;

  void RunServer() {
    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", port));
    arrow::flight::FlightServerOptions options(location);

    ARROW_CHECK_OK(example::SQLiteFlightSqlServer::Create().Value(&server));

    ARROW_CHECK_OK(server->Init(options));
    // Exit with a clean error code (0) on SIGTERM
    ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

    server_ready_cv.notify_all();
    ARROW_CHECK_OK(server->Serve());
  }
};

TEST_F(TestFlightSqlServer, TestCommandStatementQuery) {
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->Execute({}, "SELECT * FROM intTable"));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  const auto id_array = ArrayFromJSON(int64(), R"([1, 2, 3, 4])");
  const auto keyname_array =
      ArrayFromJSON(utf8(), R"(["one", "zero", "negative one", null])");
  const auto value_array = ArrayFromJSON(int64(), R"([1, 0, -1, null])");
  const auto foreignId_array = ArrayFromJSON(int64(), R"([1, 1, 1, null])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTables) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  std::string* table_filter_pattern = nullptr;
  bool include_schema = false;
  std::vector<std::string>* table_types = nullptr;

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables(options, catalog, schema_filter_pattern, table_filter_pattern,
                            include_schema, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  ASSERT_OK_AND_ASSIGN(auto catalog_name, MakeArrayOfNull(utf8(), 3))
  ASSERT_OK_AND_ASSIGN(auto schema_name, MakeArrayOfNull(utf8(), 3))

  const auto table_name =
      ArrayFromJSON(utf8(), R"(["foreignTable", "intTable", "sqlite_sequence"])");
  const auto table_type = ArrayFromJSON(utf8(), R"(["table", "table", "table"])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTablesWithTableFilter) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  std::string table_filter_pattern = "int%";
  bool include_schema = false;
  std::vector<std::string>* table_types = nullptr;

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables(options, catalog, schema_filter_pattern,
                            &table_filter_pattern, include_schema, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto table_type = ArrayFromJSON(utf8(), R"(["table"])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTablesWithTableTypesFilter) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  std::string* table_filter_pattern = nullptr;
  bool include_schema = false;
  std::vector<std::string> table_types{"index"};

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables(options, catalog, schema_filter_pattern, table_filter_pattern,
                            include_schema, &table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  AssertSchemaEqual(SqlSchema::GetTablesSchema(), table->schema());

  ASSERT_EQ(table->num_rows(), 0);
}

TEST_F(TestFlightSqlServer, TestCommandGetTablesWithUnexistenceTableTypeFilter) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  std::string* table_filter_pattern = nullptr;
  bool include_schema = false;
  std::vector<std::string> table_types{"table"};

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables(options, catalog, schema_filter_pattern, table_filter_pattern,
                            include_schema, &table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto catalog_name = ArrayFromJSON(utf8(), R"([null, null, null])");
  const auto schema_name = ArrayFromJSON(utf8(), R"([null, null, null])");
  const auto table_name =
      ArrayFromJSON(utf8(), R"(["foreignTable", "intTable", "sqlite_sequence"])");
  const auto table_type = ArrayFromJSON(utf8(), R"(["table", "table", "table"])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetTablesSchema(), {catalog_name, schema_name, table_name, table_type});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTablesWithIncludedSchemas) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  std::string table_filter_pattern = "int%";
  bool include_schema = true;
  std::vector<std::string>* table_types = nullptr;

  ASSERT_OK_AND_ASSIGN(
      auto flight_info,
      sql_client->GetTables(options, catalog, schema_filter_pattern,
                            &table_filter_pattern, include_schema, table_types));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const char* db_table_name = "intTable";

  const auto catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto table_type = ArrayFromJSON(utf8(), R"(["table"])");

  const std::shared_ptr<Schema> schema_table = arrow::schema(
      {arrow::field(
           "id", int64(), true,
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map()),
       arrow::field(
           "keyName", utf8(), true,
           example::GetColumnMetadata(SQLITE_TEXT, db_table_name).metadata_map()),
       arrow::field(
           "value", int64(), true,
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map()),
       arrow::field(
           "foreignId", int64(), true,
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map())});

  ASSERT_OK_AND_ASSIGN(auto schema_buffer, ipc::SerializeSchema(*schema_table));

  std::shared_ptr<Array> table_schema;
  ArrayFromVector<BinaryType, std::string>({schema_buffer->ToString()}, &table_schema);

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTablesSchemaWithIncludedSchema(),
                  {catalog_name, schema_name, table_name, table_type, table_schema});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTypeInfo) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetXdbcTypeInfo({}));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  auto batch = example::DoGetTypeInfoResult();

  ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches({batch}));
  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetTypeInfoWithFiltering) {
  int data_type = -4;
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetXdbcTypeInfo({}, data_type));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  auto batch = example::DoGetTypeInfoResult(data_type);

  ASSERT_OK_AND_ASSIGN(auto expected_table, Table::FromRecordBatches({batch}));
  std::shared_ptr<Table> table;
  ASSERT_OK(stream->ReadAll(&table));

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetCatalogs) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetCatalogs({}));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetCatalogsSchema();

  AssertSchemaEqual(expected_schema, table->schema());
  ASSERT_EQ(0, table->num_rows());
}

TEST_F(TestFlightSqlServer, TestCommandGetDbSchemas) {
  FlightCallOptions options = {};
  std::string* catalog = nullptr;
  std::string* schema_filter_pattern = nullptr;
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetDbSchemas(options, catalog, schema_filter_pattern));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const std::shared_ptr<Schema>& expected_schema = SqlSchema::GetDbSchemasSchema();

  AssertSchemaEqual(expected_schema, table->schema());
  ASSERT_EQ(0, table->num_rows());
}

TEST_F(TestFlightSqlServer, TestCommandGetTableTypes) {
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetTableTypes({}));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto table_type = ArrayFromJSON(utf8(), R"(["table"])");

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetTableTypesSchema(), {table_type});
  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandStatementUpdate) {
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

TEST_F(TestFlightSqlServer, TestCommandPreparedStatementQuery) {
  ASSERT_OK_AND_ASSIGN(auto prepared_statement,
                       sql_client->Prepare({}, "SELECT * FROM intTable"));

  ASSERT_OK_AND_ASSIGN(auto flight_info, prepared_statement->Execute());

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const char* db_table_name = "intTable";

  const std::shared_ptr<Schema>& expected_schema = arrow::schema(
      {arrow::field(
           "id", int64(),
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map()),
       arrow::field(
           "keyName", utf8(),
           example::GetColumnMetadata(SQLITE_TEXT, db_table_name).metadata_map()),
       arrow::field(
           "value", int64(),
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map()),
       arrow::field(
           "foreignId", int64(),
           example::GetColumnMetadata(SQLITE_INTEGER, db_table_name).metadata_map())});

  const auto id_array = ArrayFromJSON(int64(), R"([1, 2, 3, 4])");
  const auto keyname_array =
      ArrayFromJSON(utf8(), R"(["one", "zero", "negative one", null])");
  const auto value_array = ArrayFromJSON(int64(), R"([1, 0, -1, null])");
  const auto foreignId_array = ArrayFromJSON(int64(), R"([1, 1, 1, null])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandPreparedStatementQueryWithParameterBinding) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare({}, "SELECT * FROM intTable WHERE keyName LIKE ?"));

  auto parameter_schema = prepared_statement->parameter_schema();

  const std::shared_ptr<Schema>& expected_parameter_schema =
      arrow::schema({arrow::field("parameter_1", example::GetUnknownColumnDataType())});

  AssertSchemaEqual(expected_parameter_schema, parameter_schema);

  std::shared_ptr<Array> type_ids = ArrayFromJSON(int8(), R"([0])");
  std::shared_ptr<Array> offsets = ArrayFromJSON(int32(), R"([0])");
  std::shared_ptr<Array> string_array = ArrayFromJSON(utf8(), R"(["%one"])");
  std::shared_ptr<Array> bytes_array = ArrayFromJSON(binary(), R"([])");
  std::shared_ptr<Array> bigint_array = ArrayFromJSON(int64(), R"([])");
  std::shared_ptr<Array> double_array = ArrayFromJSON(float64(), R"([])");

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

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const std::shared_ptr<Schema>& expected_schema =
      arrow::schema({arrow::field("id", int64()), arrow::field("keyName", utf8()),
                     arrow::field("value", int64()), arrow::field("foreignId", int64())});

  const auto id_array = ArrayFromJSON(int64(), R"([1, 3])");
  const auto keyname_array = ArrayFromJSON(utf8(), R"(["one", "negative one"])");
  const auto value_array = ArrayFromJSON(int64(), R"([1, -1])");
  const auto foreignId_array = ArrayFromJSON(int64(), R"([1, 1])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      expected_schema, {id_array, keyname_array, value_array, foreignId_array});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandPreparedStatementUpdateWithParameterBinding) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare(
          {}, "INSERT INTO INTTABLE (keyName, value) VALUES ('new_value', ?)"));

  auto parameter_schema = prepared_statement->parameter_schema();

  const std::shared_ptr<Schema>& expected_parameter_schema =
      arrow::schema({arrow::field("parameter_1", example::GetUnknownColumnDataType())});

  AssertSchemaEqual(expected_parameter_schema, parameter_schema);

  std::shared_ptr<Array> type_ids = ArrayFromJSON(int8(), R"([2])");
  std::shared_ptr<Array> offsets = ArrayFromJSON(int32(), R"([0])");
  std::shared_ptr<Array> string_array = ArrayFromJSON(utf8(), R"([])");
  std::shared_ptr<Array> bytes_array = ArrayFromJSON(binary(), R"([])");
  std::shared_ptr<Array> bigint_array = ArrayFromJSON(int64(), R"([999])");
  std::shared_ptr<Array> double_array = ArrayFromJSON(float64(), R"([])");

  ASSERT_OK_AND_ASSIGN(
      auto parameter_1_array,
      DenseUnionArray::Make(*type_ids, *offsets,
                            {string_array, bytes_array, bigint_array, double_array},
                            {"string", "bytes", "bigint", "double"}, {0, 1, 2, 3}));

  const std::shared_ptr<RecordBatch>& record_batch =
      RecordBatch::Make(parameter_schema, 1, {parameter_1_array});

  ASSERT_OK(prepared_statement->SetParameters(record_batch));

  ASSERT_OK_AND_EQ(4, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));

  ASSERT_OK_AND_EQ(1, prepared_statement->ExecuteUpdate());

  ASSERT_OK_AND_EQ(5, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));

  ASSERT_OK_AND_EQ(1, sql_client->ExecuteUpdate(
                          {}, "DELETE FROM intTable WHERE keyName = 'new_value'"));

  ASSERT_OK_AND_EQ(4, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
}

TEST_F(TestFlightSqlServer, TestCommandPreparedStatementUpdate) {
  ASSERT_OK_AND_ASSIGN(
      auto prepared_statement,
      sql_client->Prepare(
          {}, "INSERT INTO INTTABLE (keyName, value) VALUES ('new_value', 999)"));

  ASSERT_OK_AND_EQ(4, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));

  ASSERT_OK_AND_EQ(1, prepared_statement->ExecuteUpdate());

  ASSERT_OK_AND_EQ(5, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));

  ASSERT_OK_AND_EQ(1, sql_client->ExecuteUpdate(
                          {}, "DELETE FROM intTable WHERE keyName = 'new_value'"));

  ASSERT_OK_AND_EQ(4, ExecuteCountQuery("SELECT COUNT(*) FROM intTable"));
}

TEST_F(TestFlightSqlServer, TestCommandGetPrimaryKeys) {
  FlightCallOptions options = {};
  TableRef table_ref = {util::nullopt, util::nullopt, "int%"};
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetPrimaryKeys(options, table_ref));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto column_name = ArrayFromJSON(utf8(), R"(["id"])");
  const auto key_sequence = ArrayFromJSON(int32(), R"([1])");

  const std::shared_ptr<Table>& expected_table = Table::Make(
      SqlSchema::GetPrimaryKeysSchema(),
      {catalog_name, schema_name, table_name, column_name, key_sequence, key_name});

  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetImportedKeys) {
  FlightCallOptions options = {};
  TableRef table_ref = {util::nullopt, util::nullopt, "intTable"};
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetImportedKeys(options, table_ref));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto pk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_table_name = ArrayFromJSON(utf8(), R"(["foreignTable"])");
  const auto pk_column_name = ArrayFromJSON(utf8(), R"(["id"])");
  const auto fk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto fk_column_name = ArrayFromJSON(utf8(), R"(["foreignId"])");
  const auto key_sequence = ArrayFromJSON(int32(), R"([0])");
  const auto fk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto update_rule = ArrayFromJSON(uint8(), R"([3])");
  const auto delete_rule = ArrayFromJSON(uint8(), R"([3])");

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetImportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetExportedKeys) {
  FlightCallOptions options = {};
  TableRef table_ref = {util::nullopt, util::nullopt, "foreignTable"};
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetExportedKeys(options, table_ref));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto pk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_table_name = ArrayFromJSON(utf8(), R"(["foreignTable"])");
  const auto pk_column_name = ArrayFromJSON(utf8(), R"(["id"])");
  const auto fk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto fk_column_name = ArrayFromJSON(utf8(), R"(["foreignId"])");
  const auto key_sequence = ArrayFromJSON(int32(), R"([0])");
  const auto fk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto update_rule = ArrayFromJSON(uint8(), R"([3])");
  const auto delete_rule = ArrayFromJSON(uint8(), R"([3])");

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetExportedKeysSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetCrossReference) {
  FlightCallOptions options = {};
  TableRef pk_table_ref = {util::nullopt, util::nullopt, "foreignTable"};
  TableRef fk_table_ref = {util::nullopt, util::nullopt, "intTable"};
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetCrossReference(
                                             options, pk_table_ref, fk_table_ref));

  ASSERT_OK_AND_ASSIGN(auto stream,
                       sql_client->DoGet({}, flight_info->endpoints()[0].ticket));

  ASSERT_OK_AND_ASSIGN(auto table, stream->ToTable());

  const auto pk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_table_name = ArrayFromJSON(utf8(), R"(["foreignTable"])");
  const auto pk_column_name = ArrayFromJSON(utf8(), R"(["id"])");
  const auto fk_catalog_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_schema_name = ArrayFromJSON(utf8(), R"([null])");
  const auto fk_table_name = ArrayFromJSON(utf8(), R"(["intTable"])");
  const auto fk_column_name = ArrayFromJSON(utf8(), R"(["foreignId"])");
  const auto key_sequence = ArrayFromJSON(int32(), R"([0])");
  const auto fk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto pk_key_name = ArrayFromJSON(utf8(), R"([null])");
  const auto update_rule = ArrayFromJSON(uint8(), R"([3])");
  const auto delete_rule = ArrayFromJSON(uint8(), R"([3])");

  const std::shared_ptr<Table>& expected_table =
      Table::Make(SqlSchema::GetCrossReferenceSchema(),
                  {pk_catalog_name, pk_schema_name, pk_table_name, pk_column_name,
                   fk_catalog_name, fk_schema_name, fk_table_name, fk_column_name,
                   key_sequence, fk_key_name, pk_key_name, update_rule, delete_rule});
  AssertTablesEqual(*expected_table, *table);
}

TEST_F(TestFlightSqlServer, TestCommandGetSqlInfo) {
  const auto& sql_info_expected_results = sql::example::GetSqlInfoResultMap();
  std::vector<int> sql_info_ids;
  sql_info_ids.reserve(sql_info_expected_results.size());
  for (const auto& sql_info_expected_result : sql_info_expected_results) {
    sql_info_ids.push_back(sql_info_expected_result.first);
  }

  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto flight_info,
                       sql_client->GetSqlInfo(call_options, sql_info_ids));
  ASSERT_OK_AND_ASSIGN(
      auto reader, sql_client->DoGet(call_options, flight_info->endpoints()[0].ticket));
  ASSERT_OK_AND_ASSIGN(auto results, reader->ToTable());
  ASSERT_EQ(2, results->num_columns());
  ASSERT_EQ(sql_info_ids.size(), results->num_rows());
  const auto& col_name = results->column(0);
  const auto& col_value = results->column(1);
  for (int32_t i = 0; i < col_name->num_chunks(); i++) {
    const auto* col_name_chunk_data =
        col_name->chunk(i)->data()->GetValuesSafe<int32_t>(1);
    const auto& col_value_chunk = col_value->chunk(i);
    for (int64_t row = 0; row < col_value->length(); row++) {
      ASSERT_OK_AND_ASSIGN(const auto& scalar, col_value_chunk->GetScalar(row));
      const SqlInfoDenseUnionValidator validator(
          reinterpret_cast<const DenseUnionScalar&>(*scalar));
      const auto& expected_result =
          sql_info_expected_results.at(col_name_chunk_data[row]);
      arrow::util::visit(validator, expected_result);
    }
  }
}

TEST_F(TestFlightSqlServer, TestCommandGetSqlInfoNoInfo) {
  FlightCallOptions call_options;
  ASSERT_OK_AND_ASSIGN(auto flight_info, sql_client->GetSqlInfo(call_options, {999999}));

  EXPECT_RAISES_WITH_MESSAGE_THAT(
      KeyError, ::testing::HasSubstr("No information for SQL info number 999999"),
      sql_client->DoGet(call_options, flight_info->endpoints()[0].ticket));
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
