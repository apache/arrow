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

#include <utility>

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
  MOCK_METHOD(Status, DoAction,
              (const FlightCallOptions& options, const Action& action,
               std::unique_ptr<ResultStream>* results));
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

class FlightStreamWriterMock : public FlightStreamWriter {
 public:
  FlightStreamWriterMock() = default;

  Status DoneWriting() override { return Status::OK(); }

  Status WriteMetadata(std::shared_ptr<Buffer> app_metadata) override {
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema,
               const ipc::IpcWriteOptions& options) override {
    return Status::OK();
  }

  Status Begin(const std::shared_ptr<Schema>& schema) override {
    return MetadataRecordBatchWriter::Begin(schema);
  }

  ipc::WriteStats stats() const override { return ipc::WriteStats(); }

  Status WriteWithMetadata(const RecordBatch& batch,
                           std::shared_ptr<Buffer> app_metadata) override {
    return Status::OK();
  }

  Status Close() override { return Status::OK(); }

  Status WriteRecordBatch(const RecordBatch& batch) override { return Status::OK(); }
};

FlightDescriptor getDescriptor(google::protobuf::Message& command) {
  google::protobuf::Any any;
  any.PackFrom(command);

  const std::string& string = any.SerializeAsString();
  return FlightDescriptor::Command(string);
}

TEST(TestFlightSqlClient, TestGetCatalogs) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  pb::sql::CommandGetCatalogs command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetCatalogs(call_options, &flight_info);
}

TEST(TestFlightSqlClient, TestGetSchemas) {
  auto* client_mock = new FlightClientMock();
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
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetSchemas(call_options, &catalog, &schema_filter_pattern,
                             &flight_info);
}

TEST(TestFlightSqlClient, TestGetTables) {
  auto* client_mock = new FlightClientMock();
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
  for (const std::string& table_type : table_types) {
    command.add_table_types(table_type);
  }
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetTables(call_options, &catalog, &schema_filter_pattern,
                            &table_name_filter_pattern, include_schema, table_types,
                            &flight_info);
}

TEST(TestFlightSqlClient, TestGetTableTypes) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  pb::sql::CommandGetTableTypes command;
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetTableTypes(call_options, &flight_info);
}

TEST(TestFlightSqlClient, TestGetExported) {
  auto* client_mock = new FlightClientMock();
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
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetExportedKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestGetImported) {
  auto* client_mock = new FlightClientMock();
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
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetImportedKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestGetPrimary) {
  auto* client_mock = new FlightClientMock();
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
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetPrimaryKeys(call_options, &catalog, &schema, table, &flight_info);
}

TEST(TestFlightSqlClient, TestGetCrossReference) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  std::string pk_catalog = "pk_catalog";
  std::string pk_schema = "pk_schema";
  std::string pk_table = "pk_table";
  std::string fk_catalog = "fk_catalog";
  std::string fk_schema = "fk_schema";
  std::string fk_table = "fk_table";

  pb::sql::CommandGetCrossReference command;
  command.set_pk_catalog(pk_catalog);
  command.set_pk_schema(pk_schema);
  command.set_pk_table(pk_table);
  command.set_fk_catalog(fk_catalog);
  command.set_fk_schema(fk_schema);
  command.set_fk_table(fk_table);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.GetCrossReference(call_options, &pk_catalog, &pk_schema, pk_table,
                                    &fk_catalog, &fk_schema, fk_table, &flight_info);
}

TEST(TestFlightSqlClient, TestExecute) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  std::string query = "query";

  pb::sql::CommandStatementQuery command;
  command.set_query(query);
  FlightDescriptor descriptor = getDescriptor(command);

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));

  (void)sqlClient.Execute(call_options, query, &flight_info);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecute) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  const std::string query = "query";

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([](const FlightCallOptions& options, const Action& action,
                        std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;

        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle("query");

        command.PackFrom(prepared_statement_result);

        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);

  std::shared_ptr<internal::PreparedStatementT<FlightClientMock>> preparedStatement;
  (void)sqlClient.Prepare(call_options, query, &preparedStatement);

  EXPECT_CALL(*client_mock, GetFlightInfo(_, _, &flight_info));

  (void)preparedStatement->Execute(&flight_info);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteParameterBinding) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sqlClient(client_mock_ptr);
  FlightCallOptions call_options;

  const std::string query = "query";

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([](const FlightCallOptions& options, const Action& action,
                        std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;

        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle("query");

        auto schema = arrow::schema({arrow::field("id", int64())});

        std::shared_ptr<Buffer> schema_buffer;
        const arrow::Result<std::shared_ptr<Buffer>>& result =
            arrow::ipc::SerializeSchema(*schema);

        ARROW_ASSIGN_OR_RAISE(schema_buffer, result);

        prepared_statement_result.set_parameter_schema(schema_buffer->ToString());

        command.PackFrom(prepared_statement_result);

        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });

  std::shared_ptr<Buffer> buffer_ptr;
  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        writer->reset(new FlightStreamWriterMock());
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));

        return Status::OK();
      });

  std::unique_ptr<FlightInfo> flight_info;
  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);
  EXPECT_CALL(*client_mock, DoPut(_, _, _, _, _));

  std::shared_ptr<internal::PreparedStatementT<FlightClientMock>> prepared_statement;
  ASSERT_OK(sqlClient.Prepare(call_options, query, &prepared_statement));

  std::shared_ptr<Schema> parameter_schema;
  ASSERT_OK(prepared_statement->GetParameterSchema(&parameter_schema));

  arrow::Int64Builder int_builder;
  ASSERT_OK(int_builder.Append(1));
  std::shared_ptr<arrow::Array> int_array;
  ASSERT_OK(int_builder.Finish(&int_array));
  std::shared_ptr<arrow::RecordBatch> result;
  result = arrow::RecordBatch::Make(parameter_schema, 1, {int_array});
  ASSERT_OK(prepared_statement->SetParameters(result));

  EXPECT_CALL(*client_mock, GetFlightInfo(_, _, &flight_info));

  ASSERT_OK(prepared_statement->Execute(&flight_info));
}

TEST(TestFlightSqlClient, TestExecuteUpdate) {
  auto* client_mock = new FlightClientMock();
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

  (void)sqlClient.ExecuteUpdate(call_options, query, &num_rows);

  ASSERT_EQ(num_rows, 100);
}

TEST(TestFlightSqlClient, TestGetSqlInfo) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sql_client(client_mock_ptr);

  std::vector<pb::sql::SqlInfo> sql_info{
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_NAME,
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
      pb::sql::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION};
  std::unique_ptr<FlightInfo> flight_info;
  pb::sql::CommandGetSqlInfo command;

  for (const auto& info : sql_info) command.add_info(info);
  google::protobuf::Any any;
  any.PackFrom(command);
  const FlightDescriptor& descriptor = FlightDescriptor::Command(any.SerializeAsString());

  FlightCallOptions call_options;
  EXPECT_CALL(*client_mock, GetFlightInfo(Ref(call_options), descriptor, &flight_info));
  (void)sql_client.GetSqlInfo(call_options, sql_info, &flight_info);
}

template <class Func>
inline void AssertTestPreparedStatementExecuteUpdateOk(
    Func func, const std::shared_ptr<Schema>* schema) {
  auto* client_mock = new FlightClientMock();
  std::unique_ptr<FlightClientMock> client_mock_ptr(client_mock);
  FlightSqlClientT<FlightClientMock> sql_client(client_mock_ptr);

  const std::string query = "SELECT * FROM IRRELEVANT";
  const FlightCallOptions call_options;
  int64_t expected_rows = 100L;
  pb::sql::DoPutUpdateResult result;
  result.set_record_count(expected_rows);

  ON_CALL(*client_mock, DoAction)
      .WillByDefault([&query, &schema](const FlightCallOptions& options,
                                       const Action& action,
                                       std::unique_ptr<ResultStream>* results) {
        google::protobuf::Any command;
        pb::sql::ActionCreatePreparedStatementResult prepared_statement_result;

        prepared_statement_result.set_prepared_statement_handle(query);

        if (schema != NULLPTR) {
          std::shared_ptr<Buffer> schema_buffer;
          const arrow::Result<std::shared_ptr<Buffer>>& result =
              arrow::ipc::SerializeSchema(**schema);

          ARROW_ASSIGN_OR_RAISE(schema_buffer, result);
          prepared_statement_result.set_parameter_schema(schema_buffer->ToString());
        }

        command.PackFrom(prepared_statement_result);
        *results = std::unique_ptr<ResultStream>(new SimpleResultStream(
            {Result{Buffer::FromString(command.SerializeAsString())}}));

        return Status::OK();
      });
  EXPECT_CALL(*client_mock, DoAction(_, _, _)).Times(2);

  auto buffer = Buffer::FromString(result.SerializeAsString());
  ON_CALL(*client_mock, DoPut)
      .WillByDefault([&buffer](const FlightCallOptions& options,
                               const FlightDescriptor& descriptor1,
                               const std::shared_ptr<Schema>& schema,
                               std::unique_ptr<FlightStreamWriter>* writer,
                               std::unique_ptr<FlightMetadataReader>* reader) {
        reader->reset(new FlightMetadataReaderMock(&buffer));
        writer->reset(new FlightStreamWriterMock());
        return Status::OK();
      });
  if (schema == NULLPTR) {
    EXPECT_CALL(*client_mock, DoPut(_, _, _, _, _));
  } else {
    EXPECT_CALL(*client_mock, DoPut(_, _, *schema, _, _));
  }

  int64_t rows;
  std::shared_ptr<internal::PreparedStatementT<FlightClientMock>> prepared_statement;
  ASSERT_OK(sql_client.Prepare(call_options, query, &prepared_statement));
  func(prepared_statement, *client_mock, schema, expected_rows);
  ASSERT_OK(prepared_statement->ExecuteUpdate(&rows));
  ASSERT_EQ(expected_rows, rows);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteUpdateNoParameterBinding) {
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<internal::PreparedStatementT<FlightClientMock>>&
             prepared_statement,
         FlightClientMock& client_mock, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {},
      NULLPTR);
}

TEST(TestFlightSqlClient, TestPreparedStatementExecuteUpdateWithParameterBinding) {
  const auto schema = arrow::schema(
      {arrow::field("field0", arrow::utf8()), arrow::field("field1", arrow::uint8())});
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<internal::PreparedStatementT<FlightClientMock>>&
             prepared_statement,
         FlightClientMock& client_mock, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {
        std::shared_ptr<Array> string_array;
        std::shared_ptr<Array> uint8_array;
        const std::vector<std::string> string_data{"Lorem", "Ipsum", "Foo", "Bar", "Baz"};
        const std::vector<uint8_t> uint8_data{0, 10, 15, 20, 25};
        ArrayFromVector<StringType, std::string>(string_data, &string_array);
        ArrayFromVector<UInt8Type, uint8_t>(uint8_data, &uint8_array);
        std::shared_ptr<RecordBatch> recordBatch =
            RecordBatch::Make(*schema, row_count, {string_array, uint8_array});
        ASSERT_OK(prepared_statement->SetParameters(recordBatch));
      },
      &schema);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
