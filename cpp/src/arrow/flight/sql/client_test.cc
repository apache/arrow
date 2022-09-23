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

// Platform-specific defines
#include "arrow/flight/platform.h"

#include "arrow/flight/client.h"

#include <gmock/gmock.h>
#include <google/protobuf/any.pb.h>
#include <gtest/gtest.h>

#include <utility>

#include "arrow/buffer.h"
#include "arrow/flight/sql/api.h"
#include "arrow/flight/sql/protocol_internal.h"
#include "arrow/testing/gtest_util.h"

namespace pb = arrow::flight::protocol;
using ::testing::_;
using ::testing::Ref;

namespace arrow {
namespace flight {
namespace sql {

class FlightSqlClientMock : public FlightSqlClient {
 public:
  FlightSqlClientMock() : FlightSqlClient(nullptr) {}

  ~FlightSqlClientMock() = default;

  MOCK_METHOD(arrow::Result<std::unique_ptr<FlightInfo>>, GetFlightInfo,
              (const FlightCallOptions&, const FlightDescriptor&));
  MOCK_METHOD(Status, DoGet,
              (const FlightCallOptions& options, const Ticket& ticket,
               std::unique_ptr<FlightStreamReader>* stream));
  MOCK_METHOD(Status, DoPut,
              (const FlightCallOptions&, const FlightDescriptor&,
               const std::shared_ptr<Schema>& schema,
               std::unique_ptr<FlightStreamWriter>*,
               std::unique_ptr<FlightMetadataReader>*));
  MOCK_METHOD(Status, DoAction,
              (const FlightCallOptions& options, const Action& action,
               std::unique_ptr<ResultStream>* results));
};

class TestFlightSqlClient : public ::testing::Test {
 protected:
  FlightSqlClientMock sql_client_;
  FlightCallOptions call_options_;

  void SetUp() override {}

  void TearDown() override {}
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

auto ReturnEmptyFlightInfo = [](const FlightCallOptions& options,
                                const FlightDescriptor& descriptor) {
  std::unique_ptr<FlightInfo> flight_info;
  return flight_info;
};

TEST_F(TestFlightSqlClient, TestGetCatalogs) {
  pb::sql::CommandGetCatalogs command;
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetCatalogs(call_options_));
}

TEST_F(TestFlightSqlClient, TestGetDbSchemas) {
  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string catalog = "catalog";

  pb::sql::CommandGetDbSchemas command;
  command.set_catalog(catalog);
  command.set_db_schema_filter_pattern(schema_filter_pattern);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetDbSchemas(call_options_, &catalog, &schema_filter_pattern));
}

TEST_F(TestFlightSqlClient, TestGetTables) {
  std::string catalog = "catalog";
  std::string schema_filter_pattern = "schema_filter_pattern";
  std::string table_name_filter_pattern = "table_name_filter_pattern";
  bool include_schema = true;
  std::vector<std::string> table_types = {"type1", "type2"};

  pb::sql::CommandGetTables command;
  command.set_catalog(catalog);
  command.set_db_schema_filter_pattern(schema_filter_pattern);
  command.set_table_name_filter_pattern(table_name_filter_pattern);
  command.set_include_schema(include_schema);
  for (const std::string& table_type : table_types) {
    command.add_table_types(table_type);
  }
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetTables(call_options_, &catalog, &schema_filter_pattern,
                                  &table_name_filter_pattern, include_schema,
                                  &table_types));
}

TEST_F(TestFlightSqlClient, TestGetTableTypes) {
  pb::sql::CommandGetTableTypes command;
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetTableTypes(call_options_));
}

TEST_F(TestFlightSqlClient, TestGetTypeInfo) {
  pb::sql::CommandGetXdbcTypeInfo command;
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetXdbcTypeInfo(call_options_));
}

TEST_F(TestFlightSqlClient, TestGetExported) {
  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetExportedKeys command;
  command.set_catalog(catalog);
  command.set_db_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  TableRef table_ref = {std::make_optional(catalog), std::make_optional(schema), table};
  ASSERT_OK(sql_client_.GetExportedKeys(call_options_, table_ref));
}

TEST_F(TestFlightSqlClient, TestGetImported) {
  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetImportedKeys command;
  command.set_catalog(catalog);
  command.set_db_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  TableRef table_ref = {std::make_optional(catalog), std::make_optional(schema), table};
  ASSERT_OK(sql_client_.GetImportedKeys(call_options_, table_ref));
}

TEST_F(TestFlightSqlClient, TestGetPrimary) {
  std::string catalog = "catalog";
  std::string schema = "schema";
  std::string table = "table";

  pb::sql::CommandGetPrimaryKeys command;
  command.set_catalog(catalog);
  command.set_db_schema(schema);
  command.set_table(table);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  TableRef table_ref = {std::make_optional(catalog), std::make_optional(schema), table};
  ASSERT_OK(sql_client_.GetPrimaryKeys(call_options_, table_ref));
}

TEST_F(TestFlightSqlClient, TestGetCrossReference) {
  std::string pk_catalog = "pk_catalog";
  std::string pk_schema = "pk_schema";
  std::string pk_table = "pk_table";
  std::string fk_catalog = "fk_catalog";
  std::string fk_schema = "fk_schema";
  std::string fk_table = "fk_table";

  pb::sql::CommandGetCrossReference command;
  command.set_pk_catalog(pk_catalog);
  command.set_pk_db_schema(pk_schema);
  command.set_pk_table(pk_table);
  command.set_fk_catalog(fk_catalog);
  command.set_fk_db_schema(fk_schema);
  command.set_fk_table(fk_table);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  TableRef pk_table_ref = {std::make_optional(pk_catalog), std::make_optional(pk_schema),
                           pk_table};
  TableRef fk_table_ref = {std::make_optional(fk_catalog), std::make_optional(fk_schema),
                           fk_table};
  ASSERT_OK(sql_client_.GetCrossReference(call_options_, pk_table_ref, fk_table_ref));
}

TEST_F(TestFlightSqlClient, TestExecute) {
  std::string query = "query";

  pb::sql::CommandStatementQuery command;
  command.set_query(query);
  FlightDescriptor descriptor = getDescriptor(command);

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.Execute(call_options_, query));
}

TEST_F(TestFlightSqlClient, TestPreparedStatementExecute) {
  const std::string query = "query";

  ON_CALL(sql_client_, DoAction)
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

  EXPECT_CALL(sql_client_, DoAction(_, _, _)).Times(2);

  ASSERT_OK_AND_ASSIGN(auto prepared_statement,
                       sql_client_.Prepare(call_options_, query));

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(_, _));

  ASSERT_OK(prepared_statement->Execute());
}

TEST_F(TestFlightSqlClient, TestPreparedStatementExecuteParameterBinding) {
  const std::string query = "query";

  ON_CALL(sql_client_, DoAction)
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
  ON_CALL(sql_client_, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        writer->reset(new FlightStreamWriterMock());
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));

        return Status::OK();
      });

  EXPECT_CALL(sql_client_, DoAction(_, _, _)).Times(2);
  EXPECT_CALL(sql_client_, DoPut(_, _, _, _, _));

  ASSERT_OK_AND_ASSIGN(auto prepared_statement,
                       sql_client_.Prepare(call_options_, query));

  auto parameter_schema = prepared_statement->parameter_schema();

  auto result = RecordBatchFromJSON(parameter_schema, "[[1]]");
  ASSERT_OK(prepared_statement->SetParameters(result));

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(_, _));

  ASSERT_OK(prepared_statement->Execute());
}

TEST_F(TestFlightSqlClient, TestExecuteUpdate) {
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

  ON_CALL(sql_client_, DoPut)
      .WillByDefault([&buffer_ptr](const FlightCallOptions& options,
                                   const FlightDescriptor& descriptor1,
                                   const std::shared_ptr<Schema>& schema,
                                   std::unique_ptr<FlightStreamWriter>* writer,
                                   std::unique_ptr<FlightMetadataReader>* reader) {
        reader->reset(new FlightMetadataReaderMock(&buffer_ptr));
        writer->reset(new FlightStreamWriterMock());

        return Status::OK();
      });

  std::unique_ptr<FlightInfo> flight_info;
  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  EXPECT_CALL(sql_client_, DoPut(Ref(call_options_), descriptor, _, _, _));

  ASSERT_OK_AND_ASSIGN(auto num_rows, sql_client_.ExecuteUpdate(call_options_, query));

  ASSERT_EQ(num_rows, 100);
}

TEST_F(TestFlightSqlClient, TestGetSqlInfo) {
  std::vector<int> sql_info{pb::sql::SqlInfo::FLIGHT_SQL_SERVER_NAME,
                            pb::sql::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
                            pb::sql::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION};
  pb::sql::CommandGetSqlInfo command;

  for (const auto& info : sql_info) command.add_info(info);
  google::protobuf::Any any;
  any.PackFrom(command);
  const FlightDescriptor& descriptor = FlightDescriptor::Command(any.SerializeAsString());

  ON_CALL(sql_client_, GetFlightInfo).WillByDefault(ReturnEmptyFlightInfo);
  EXPECT_CALL(sql_client_, GetFlightInfo(Ref(call_options_), descriptor));

  ASSERT_OK(sql_client_.GetSqlInfo(call_options_, sql_info));
}

template <class Func>
inline void AssertTestPreparedStatementExecuteUpdateOk(
    Func func, const std::shared_ptr<Schema>* schema, FlightSqlClientMock& sql_client_) {
  const std::string query = "SELECT * FROM IRRELEVANT";
  int64_t expected_rows = 100L;
  pb::sql::DoPutUpdateResult result;
  result.set_record_count(expected_rows);

  ON_CALL(sql_client_, DoAction)
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
  EXPECT_CALL(sql_client_, DoAction(_, _, _)).Times(2);

  auto buffer = Buffer::FromString(result.SerializeAsString());
  ON_CALL(sql_client_, DoPut)
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
    EXPECT_CALL(sql_client_, DoPut(_, _, _, _, _));
  } else {
    EXPECT_CALL(sql_client_, DoPut(_, _, *schema, _, _));
  }

  ASSERT_OK_AND_ASSIGN(auto prepared_statement, sql_client_.Prepare({}, query));
  func(prepared_statement, sql_client_, schema, expected_rows);
  ASSERT_OK_AND_ASSIGN(auto rows, prepared_statement->ExecuteUpdate());
  ASSERT_EQ(expected_rows, rows);
  ASSERT_OK(prepared_statement->Close());
}

TEST_F(TestFlightSqlClient, TestPreparedStatementExecuteUpdateNoParameterBinding) {
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<PreparedStatement>& prepared_statement,
         FlightSqlClient& sql_client_, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {},
      NULLPTR, sql_client_);
}

TEST_F(TestFlightSqlClient, TestPreparedStatementExecuteUpdateWithParameterBinding) {
  const auto schema = arrow::schema(
      {arrow::field("field0", arrow::utf8()), arrow::field("field1", arrow::uint8())});
  AssertTestPreparedStatementExecuteUpdateOk(
      [](const std::shared_ptr<PreparedStatement>& prepared_statement,
         FlightSqlClient& sql_client_, const std::shared_ptr<Schema>* schema,
         const int64_t& row_count) {
        auto string_array =
            ArrayFromJSON(utf8(), R"(["Lorem", "Ipsum", "Foo", "Bar", "Baz"])");
        auto uint8_array = ArrayFromJSON(uint8(), R"([0, 10, 15, 20, 25])");
        std::shared_ptr<RecordBatch> recordBatch =
            RecordBatch::Make(*schema, row_count, {string_array, uint8_array});
        ASSERT_OK(prepared_statement->SetParameters(recordBatch));
      },
      &schema, sql_client_);
}

}  // namespace sql
}  // namespace flight
}  // namespace arrow
