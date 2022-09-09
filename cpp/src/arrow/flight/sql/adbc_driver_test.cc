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

#include <chrono>
#include <thread>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arrow/c/bridge.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/adbc_driver_internal.h"
#include "arrow/flight/sql/example/sqlite_server.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/test_util.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/config.h"

#include "adbc_validation/adbc_validation.h"
#include "adbc_validation/adbc_validation_util.h"

using arrow::internal::checked_cast;

namespace arrow::flight::sql {

using adbc_validation::IsOkStatus;

#define ARROW_ADBC_RETURN_NOT_OK(STATUS, ERROR, EXPR)                              \
  do {                                                                             \
    if (AdbcStatusCode _s = (EXPR); _s != ADBC_STATUS_OK) {                        \
      return Status::STATUS(adbc_validation::StatusCodeToString(_s), ": ",         \
                            (ERROR)->message ? (ERROR)->message : "(no message)"); \
    }                                                                              \
  } while (false)

// ------------------------------------------------------------
// ADBC Test Suite

class SqliteFlightSqlQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    std::string location = location_.ToString();
    if (auto status = AdbcDatabaseSetOption(
            database, "arrow.flight.sql.quirks.ingest_type.int64", "INT", error);
        status != ADBC_STATUS_OK) {
      return status;
    }
    return AdbcDatabaseSetOption(database, "uri", location.c_str(), error);
  }

  [[nodiscard]] std::string BindParameter(int index) const override { return "?"; }
  [[nodiscard]] bool supports_concurrent_statements() const override { return true; }
  [[nodiscard]] bool supports_partitioned_data() const override { return true; }

  void StartServer() {
    ASSERT_OK_AND_ASSIGN(auto bind_location, Location::ForGrpcTcp("0.0.0.0", 0));
    arrow::flight::FlightServerOptions options(bind_location);
    ASSERT_OK_AND_ASSIGN(server_, example::SQLiteFlightSqlServer::Create());
    ASSERT_OK(server_->Init(options));

    ASSERT_OK_AND_ASSIGN(location_, Location::ForGrpcTcp("localhost", server_->port()));
  }

  void StopServer() { ASSERT_OK(server_->Shutdown()); }

 private:
  std::shared_ptr<FlightSqlServerBase> server_;
  Location location_;
};

class AdbcSqliteDatabaseTest : public ::testing::Test,
                               public adbc_validation::DatabaseTest {
 public:
  [[nodiscard]] const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(quirks_.StartServer());
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    ASSERT_NO_FATAL_FAILURE(TearDownTest());
    ASSERT_NO_FATAL_FAILURE(quirks_.StopServer());
  }

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_DATABASE(AdbcSqliteDatabaseTest)

class AdbcSqliteConnectionTest : public ::testing::Test,
                                 public adbc_validation::ConnectionTest {
 public:
  [[nodiscard]] const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(quirks_.StartServer());
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    ASSERT_NO_FATAL_FAILURE(TearDownTest());
    ASSERT_NO_FATAL_FAILURE(quirks_.StopServer());
  }

#if !defined(ARROW_COMPUTE)
  void TestMetadataGetObjectsColumns() {
    GTEST_SKIP() << "Test fails without ARROW_COMPUTE";
  }
#endif

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_CONNECTION(AdbcSqliteConnectionTest)

class AdbcSqliteStatementTest : public ::testing::Test,
                                public adbc_validation::StatementTest {
 public:
  [[nodiscard]] const adbc_validation::DriverQuirks* quirks() const override {
    return &quirks_;
  }
  void SetUp() override {
    ASSERT_NO_FATAL_FAILURE(quirks_.StartServer());
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    ASSERT_NO_FATAL_FAILURE(TearDownTest());
    ASSERT_NO_FATAL_FAILURE(quirks_.StopServer());
  }

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_STATEMENT(AdbcSqliteStatementTest)

// Ensure partitions can be introspected by clients who know they're
// using Flight SQL
TEST_F(AdbcSqliteStatementTest, IntrospectPartitions) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  struct ArrowSchema c_schema;
  struct AdbcPartitions partitions;
  ASSERT_THAT(AdbcStatementExecutePartitions(&statement, &c_schema, &partitions,
                                             /*rows_affected=*/nullptr, &error),
              IsOkStatus(&error));

  ASSERT_OK_AND_ASSIGN(auto schema, ImportSchema(&c_schema));

  ASSERT_GT(partitions.num_partitions, 0);
  for (size_t i = 0; i < partitions.num_partitions; i++) {
    ASSERT_OK_AND_ASSIGN(auto info,
                         FlightInfo::Deserialize(std::string_view(
                             reinterpret_cast<const char*>(partitions.partitions[i]),
                             partitions.partition_lengths[i])));
    ipc::DictionaryMemo memo;
    ASSERT_OK_AND_ASSIGN(auto info_schema, info->GetSchema(&memo));
    ASSERT_NO_FATAL_FAILURE(AssertSchemaEqual(*schema, *info_schema, /*verbose=*/true));
    ASSERT_EQ(1, info->endpoints().size());
  }

  partitions.release(&partitions);
}

constexpr std::initializer_list<const char*> kInvalidNames = {
    "???", "", "名前", "[quoted]", "9abc", "foo-bar"};

TEST_F(AdbcSqliteStatementTest, InvalidTableName) {
  for (const auto& name : kInvalidNames) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE, name, &error),
        IsOkStatus(&error));
    auto schema = arrow::schema({field("ints", int64())});
    auto batch = RecordBatchFromJSON(schema, R"([[1]])");
    struct ArrowSchema c_schema = {};
    struct ArrowArray c_batch = {};
    ASSERT_OK(ExportSchema(*schema, &c_schema));
    ASSERT_OK(ExportRecordBatch(*batch, &c_batch));
    ASSERT_THAT(AdbcStatementBind(&statement, &c_batch, &c_schema, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, /*out=*/nullptr,
                                          /*rows_affected=*/nullptr, &error),
                ::testing::Not(IsOkStatus(&error)));
    ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  }
}

TEST_F(AdbcSqliteStatementTest, InvalidColumnName) {
  for (const auto& name : kInvalidNames) {
    ARROW_SCOPED_TRACE(name);
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "foobar", &error),
                IsOkStatus(&error));
    auto schema = arrow::schema({field(name, int64())});
    auto batch = RecordBatchFromJSON(schema, R"([[1]])");
    struct ArrowSchema c_schema = {};
    struct ArrowArray c_batch = {};
    ASSERT_OK(ExportSchema(*schema, &c_schema));
    ASSERT_OK(ExportRecordBatch(*batch, &c_batch));
    ASSERT_THAT(AdbcStatementBind(&statement, &c_batch, &c_schema, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, /*out=*/nullptr,
                                          /*rows_affected=*/nullptr, &error),
                ::testing::Not(IsOkStatus(&error)));
    ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  }
}

TEST(FlightSqlInternals, NameSanitizer) {
  for (const auto& name : kInvalidNames) {
    ASSERT_FALSE(IsApproximatelyValidIdentifier(name)) << name;
  }
  for (const auto& name : {"_foo", "a9", "foo_bar", "ABCD"}) {
    ASSERT_TRUE(IsApproximatelyValidIdentifier(name)) << name;
  }
}

class AdbcTimeoutTestServer : public FlightSqlServerBase {
  arrow::Result<ActionBeginTransactionResult> BeginTransaction(
      const ServerCallContext& context,
      const ActionBeginTransactionRequest& request) override {
    while (!context.is_cancelled()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return Status::NotImplemented("NYI");
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override {
    while (!context.is_cancelled()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return Status::NotImplemented("NYI");
  }

  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const ServerCallContext& context, const StatementUpdate& command) override {
    if (command.query == "timeout") {
      while (!context.is_cancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }
    return Status::NotImplemented("NYI");
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    if (command.query == "timeout") {
      while (!context.is_cancelled()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    } else if (command.query == "fetch") {
      ARROW_ASSIGN_OR_RAISE(std::string ticket, CreateStatementQueryTicket("fetch"));
      ARROW_ASSIGN_OR_RAISE(
          auto info, FlightInfo::Make(Schema({}), descriptor,
                                      {
                                          FlightEndpoint{Ticket{std::move(ticket)}, {}},
                                      },
                                      /*total_records=*/-1, /*total_bytes=*/-1));
      return std::make_unique<FlightInfo>(std::move(info));
    }
    return Status::NotImplemented("NYI");
  }
};

template <typename ServerType>
class AdbcServerTest : public ::testing::Test {
 protected:
  virtual arrow::Result<Location> GetLocation(const std::string& host, int port) {
    return Location::ForGrpcTcp(host, port);
  }
  virtual void Configure(FlightServerOptions* options) {}
  void SetUp() override {
    ASSERT_OK_AND_ASSIGN(auto bind_location, GetLocation("0.0.0.0", 0));
    FlightServerOptions options(bind_location);
    Configure(&options);
    server_ = std::make_unique<ServerType>();
    ASSERT_OK(server_->Init(options));

    ASSERT_OK_AND_ASSIGN(auto connect_location,
                         GetLocation("localhost", server_->port()));
    std::string uri = connect_location.ToString();
    ASSERT_THAT(AdbcDatabaseNew(&database_, /*error=*/nullptr), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseSetOption(&database_, "uri", uri.c_str(),
                                      /*error=*/nullptr),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, /*error=*/nullptr), IsOkStatus(&error_));

    ASSERT_THAT(AdbcConnectionNew(&connection_, /*error=*/nullptr), IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, /*error=*/nullptr),
                IsOkStatus(&error_));
    error_ = {};
  }
  void TearDown() override {
    if (error_.release) error_.release(&error_);
    ASSERT_THAT(AdbcConnectionRelease(&connection_, /*error=*/nullptr),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseRelease(&database_, /*error=*/nullptr), IsOkStatus(&error_));
    ASSERT_OK(server_->Shutdown());
  }

  std::unique_ptr<FlightSqlServerBase> server_;
  AdbcDatabase database_;
  AdbcConnection connection_;
  AdbcError error_;
};

class AdbcTimeoutTest : public AdbcServerTest<AdbcTimeoutTestServer> {};

TEST_F(AdbcTimeoutTest, InvalidValues) {
  for (const auto& key : {
           "arrow.flight.sql.rpc.timeout_seconds.fetch",
           "arrow.flight.sql.rpc.timeout_seconds.query",
           "arrow.flight.sql.rpc.timeout_seconds.update",
       }) {
    for (const auto& value : {"1.1f", "asdf", "inf", "NaN", "-1"}) {
      ARROW_SCOPED_TRACE("key=", key, " value=", value);
      ASSERT_EQ(ADBC_STATUS_INVALID_ARGUMENT,
                AdbcConnectionSetOption(&connection_, key, value, &error_));
      ASSERT_THAT(error_.message, ::testing::HasSubstr("Invalid timeout option value"));
    }
  }
}

TEST_F(AdbcTimeoutTest, RemoveTimeout) {
  for (const auto& key : {
           "arrow.flight.sql.rpc.timeout_seconds.fetch",
           "arrow.flight.sql.rpc.timeout_seconds.query",
           "arrow.flight.sql.rpc.timeout_seconds.update",
       }) {
    ARROW_SCOPED_TRACE("key=", key);
    ASSERT_THAT(AdbcConnectionSetOption(&connection_, key, "1.0", &error_),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionSetOption(&connection_, key, "0", &error_),
                IsOkStatus(&error_));
  }
}

TEST_F(AdbcTimeoutTest, DoActionTimeout) {
  AdbcStatusCode status;

  status = AdbcConnectionSetOption(
      &connection_, "arrow.flight.sql.rpc.timeout_seconds.update", "0.1", &error_);
  ASSERT_THAT(status, IsOkStatus(&error_));

  ASSERT_THAT(AdbcConnectionSetOption(&connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(error_.message, ::testing::Not(::testing::HasSubstr("NYI")));
}

TEST_F(AdbcTimeoutTest, DoGetTimeout) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(
      AdbcConnectionSetOption(&connection_, "arrow.flight.sql.rpc.timeout_seconds.fetch",
                              "0.1", &error_),
      IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "fetch", &error_), IsOkStatus(&error_));
  Status st = ([&]() -> Status {
    auto status =
        AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_);
    if (status != ADBC_STATUS_OK) return Status::Invalid(error_.message);

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatchReader> reader,
                          arrow::ImportRecordBatchReader(&out));
    while (true) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<RecordBatch> rb, reader->Next());
      if (!rb) break;
    }
    return Status::OK();
  })();
  ASSERT_NOT_OK(st);
  ASSERT_THAT(st.ToString(), ::testing::Not(::testing::HasSubstr("NYI")));

  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcTimeoutTest, DoPutTimeout) {
  struct AdbcStatement stmt = {};

  ASSERT_THAT(
      AdbcConnectionSetOption(&connection_, "arrow.flight.sql.rpc.timeout_seconds.update",
                              "0.1", &error_),
      IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, /*out=*/nullptr, /*rows_affected=*/nullptr,
                                        &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(error_.message, ::testing::Not(::testing::HasSubstr("NYI")));

  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcTimeoutTest, GetFlightInfoTimeout) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(
      AdbcConnectionSetOption(&connection_, "arrow.flight.sql.rpc.timeout_seconds.query",
                              "0.1", &error_),
      IsOkStatus(&error_))
      << error_.message;

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(error_.message, ::testing::Not(::testing::HasSubstr("NYI")));

  if (out.release) out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

/// Ensure options to add custom headers make it through.
class HeaderServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    for (const auto& header : incoming_headers) {
      recorded_headers_.emplace_back(header.first, header.second);
    }
    return Status::OK();
  }

  std::vector<std::pair<std::string, std::string>> recorded_headers_;
};

class AdbcHeaderTest : public AdbcServerTest<FlightSqlServerBase> {
 protected:
  void Configure(FlightServerOptions* options) override {
    headers_ = std::make_shared<HeaderServerMiddlewareFactory>();
    options->middleware.emplace_back("headers", headers_);
  }
  std::shared_ptr<HeaderServerMiddlewareFactory> headers_;
};

TEST_F(AdbcHeaderTest, Connection) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));

  ASSERT_THAT(
      AdbcConnectionSetOption(&connection_, "arrow.flight.sql.rpc.call_header.x-span-id",
                              "my span id", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionSetOption(&connection_,
                                      "arrow.flight.sql.rpc.call_header.x-user-agent",
                                      "Flight SQL ADBC", &error_),
              IsOkStatus(&error_))
      << error_.message;

  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));

  ASSERT_THAT(AdbcConnectionSetOption(&connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-span-id", "my span id")));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-user-agent", "Flight SQL ADBC")));
  headers_->recorded_headers_.clear();

  if (out.release) out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcHeaderTest, Database) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));

  ASSERT_THAT(
      AdbcDatabaseSetOption(&database_, "arrow.flight.sql.rpc.call_header.x-span-id",
                            "my span id", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionSetOption(&connection_,
                                      "arrow.flight.sql.rpc.call_header.x-user-agent",
                                      "Flight SQL ADBC", &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));

  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-span-id", "my span id")));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-user-agent", "Flight SQL ADBC")));
  headers_->recorded_headers_.clear();

  if (out.release) out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcHeaderTest, Statement) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetOption(&stmt, "arrow.flight.sql.rpc.call_header.x-span-id",
                                     "my span id", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-span-id", "my span id")));
  headers_->recorded_headers_.clear();

  // Set header to NULL to erase it
  ASSERT_THAT(AdbcStatementSetOption(&stmt, "arrow.flight.sql.rpc.call_header.x-span-id",
                                     nullptr, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(
      headers_->recorded_headers_,
      ::testing::Not(::testing::Contains(std::make_pair("x-span-id", "my span id"))));
  headers_->recorded_headers_.clear();

  // Connection headers are inherited
  ASSERT_THAT(
      AdbcConnectionSetOption(&connection_, "arrow.flight.sql.rpc.call_header.x-span-id",
                              "my span id", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-span-id", "my span id")));
  headers_->recorded_headers_.clear();

  if (out.release) out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcHeaderTest, Combined) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(
      AdbcDatabaseSetOption(&database_, "arrow.flight.sql.rpc.call_header.x-header-one",
                            "value 1", &error_),
      IsOkStatus(&error_));

  ASSERT_THAT(AdbcConnectionSetOption(&connection_,
                                      "arrow.flight.sql.rpc.call_header.x-header-two",
                                      "value 2", &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetOption(&stmt, "arrow.flight.sql.rpc.call_header.x-header-three",
                             "value 3", &error_),
      IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "timeout", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));

  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-header-one", "value 1")));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-header-two", "value 2")));
  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("x-header-three", "value 3")));

  if (out.release) out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

class SubstraitTestServer : public FlightSqlServerBase {
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override {
    if (command.statement_handle != "expected plan") {
      return Status::Invalid("invalid plan");
    }
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({}, arrow::schema({})));
    return std::make_unique<RecordBatchStream>(reader);
  }

  arrow::Result<int64_t> DoPutCommandSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command) override {
    if (command.plan.plan != "expected plan") return Status::Invalid("invalid plan");
    return 42;
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSubstraitPlan(
      const ServerCallContext& context, const StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor) override {
    if (command.plan.plan != "expected plan") {
      return Status::Invalid("invalid plan");
    }
    ARROW_ASSIGN_OR_RAISE(std::string ticket,
                          CreateStatementQueryTicket(command.plan.plan));
    std::vector<FlightEndpoint> endpoints = {
        FlightEndpoint{Ticket{std::move(ticket)}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto info,
                          FlightInfo::Make(Schema({}), descriptor, std::move(endpoints),
                                           /*total_records=*/-1, /*total_bytes=*/-1));
    return std::make_unique<FlightInfo>(std::move(info));
  }
};

class AdbcSubstraitTest : public AdbcServerTest<SubstraitTestServer> {};

TEST_F(AdbcSubstraitTest, DoGet) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  std::string plan = "expected plan";

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSubstraitPlan(&stmt, reinterpret_cast<const uint8_t*>(plan.data()),
                                    plan.size(), &error_),
      IsOkStatus(&error_));
  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, &rows_affected, &error_),
              IsOkStatus(&error_));
  ASSERT_NE(nullptr, out.release);

  out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
}

TEST_F(AdbcSubstraitTest, DoPut) {
  struct AdbcStatement stmt = {};

  std::string plan = "expected plan";

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSubstraitPlan(&stmt, reinterpret_cast<const uint8_t*>(plan.data()),
                                    plan.size(), &error_),
      IsOkStatus(&error_));
  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, /*out=*/nullptr, &rows_affected, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));

  ASSERT_EQ(42, rows_affected);
}

class FallbackTestServer : public FlightSqlServerBase {
 public:
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override {
    if (command.statement_handle == "FailedGet" && counter_++ == 0) {
      // Fail the first time around
      return Status::IOError("First time fails");
    }
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({}, arrow::schema({})));
    return std::make_unique<RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_ASSIGN_OR_RAISE(std::string ticket, CreateStatementQueryTicket(command.query));
    std::vector<FlightEndpoint> endpoints;
    if (command.query == "NoLocation") {
      endpoints = {FlightEndpoint{Ticket{std::move(ticket)}, {}}};
    } else if (command.query == "FailedConnection") {
      ARROW_ASSIGN_OR_RAISE(auto location1,
                            Location::ForGrpcTcp("unreachable", this->port()));
      ARROW_ASSIGN_OR_RAISE(auto location2,
                            Location::ForGrpcTcp("localhost", this->port()));
      endpoints = {
          FlightEndpoint{Ticket{std::move(ticket)},
                         {
                             location1,
                             location2,
                         }},
      };
    } else if (command.query == "FailedGet") {
      ARROW_ASSIGN_OR_RAISE(auto location,
                            Location::ForGrpcTcp("localhost", this->port()));
      endpoints = {
          FlightEndpoint{Ticket{std::move(ticket)},
                         {
                             location,
                             location,
                         }},
      };
    } else if (command.query == "Failure") {
      ARROW_ASSIGN_OR_RAISE(auto location,
                            Location::ForGrpcTcp("unreachable", this->port()));
      endpoints = {
          FlightEndpoint{Ticket{std::move(ticket)},
                         {
                             location,
                             location,
                         }},
      };
    } else {
      return Status::Invalid(command.query);
    }
    ARROW_ASSIGN_OR_RAISE(auto info,
                          FlightInfo::Make(Schema({}), descriptor, std::move(endpoints),
                                           /*total_records=*/-1, /*total_bytes=*/-1));
    return std::make_unique<FlightInfo>(std::move(info));
  }

 private:
  int32_t counter_ = 0;
};

class AdbcFallbackTest : public AdbcServerTest<FallbackTestServer> {
 protected:
  Status DoQuery(const std::string& query) {
    struct AdbcStatement stmt = {};
    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_,
                             AdbcStatementNew(&connection_, &stmt, &error_));
    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_,
                             AdbcStatementSetSqlQuery(&stmt, query.data(), &error_));

    struct ArrowArrayStream out;
    AdbcStatusCode status =
        AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_);

    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_, AdbcStatementRelease(&stmt, &error_));
    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_, status);
    if (out.release) out.release(&out);
    return Status::OK();
  }

  arrow::Result<std::vector<std::unique_ptr<FlightInfo>>> GetPartitions(
      const std::string& query) {
    struct AdbcStatement stmt = {};
    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_,
                             AdbcStatementNew(&connection_, &stmt, &error_));
    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_,
                             AdbcStatementSetSqlQuery(&stmt, query.data(), &error_));

    struct ArrowSchema c_schema;
    struct AdbcPartitions partitions;
    ARROW_ADBC_RETURN_NOT_OK(
        UnknownError, &error_,
        AdbcStatementExecutePartitions(&stmt, &c_schema, &partitions,
                                       /*rows_affected=*/nullptr, &error_));
    EXPECT_GT(partitions.num_partitions, 0);
    c_schema.release(&c_schema);

    std::vector<std::unique_ptr<FlightInfo>> result;
    for (size_t i = 0; i < partitions.num_partitions; i++) {
      EXPECT_OK_AND_ASSIGN(auto info,
                           FlightInfo::Deserialize(std::string_view(
                               reinterpret_cast<const char*>(partitions.partitions[i]),
                               partitions.partition_lengths[i])));
      result.emplace_back(std::move(info));
    }

    if (c_schema.release) c_schema.release(&c_schema);
    partitions.release(&partitions);

    ARROW_ADBC_RETURN_NOT_OK(UnknownError, &error_, AdbcStatementRelease(&stmt, &error_));
    return result;
  }
};

TEST_F(AdbcFallbackTest, NoLocation) {
  ASSERT_OK(DoQuery("NoLocation"));
  ASSERT_OK_AND_ASSIGN(auto partitions, GetPartitions("NoLocation"));
  ASSERT_EQ(1, partitions.size());
  ASSERT_EQ(1, partitions[0]->endpoints().size());
  ASSERT_TRUE(partitions[0]->endpoints()[0].locations.empty());
}

TEST_F(AdbcFallbackTest, FailedConnection) {
  ASSERT_OK(DoQuery("FailedConnection"));
  ASSERT_OK_AND_ASSIGN(auto partitions, GetPartitions("FailedConnection"));
  ASSERT_EQ(1, partitions.size());
  ASSERT_EQ(1, partitions[0]->endpoints().size());
  ASSERT_EQ(2, partitions[0]->endpoints()[0].locations.size());
}

TEST_F(AdbcFallbackTest, FailedGet) {
  ASSERT_OK(DoQuery("FailedGet"));
  ASSERT_OK_AND_ASSIGN(auto partitions, GetPartitions("FailedGet"));
  ASSERT_EQ(1, partitions.size());
  ASSERT_EQ(1, partitions[0]->endpoints().size());
  ASSERT_EQ(2, partitions[0]->endpoints()[0].locations.size());
}

TEST_F(AdbcFallbackTest, Failure) {
  ASSERT_NOT_OK(DoQuery("Failure"));
  ASSERT_OK_AND_ASSIGN(auto partitions, GetPartitions("Failure"));
  ASSERT_EQ(1, partitions.size());
  ASSERT_EQ(1, partitions[0]->endpoints().size());
  ASSERT_EQ(2, partitions[0]->endpoints()[0].locations.size());
}

class NoOpTestServer : public FlightSqlServerBase {
 public:
  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context, const StatementQueryTicket& command) override {
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({}, arrow::schema({})));
    return std::make_unique<RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_ASSIGN_OR_RAISE(std::string ticket, CreateStatementQueryTicket(command.query));
    ARROW_ASSIGN_OR_RAISE(auto info,
                          FlightInfo::Make(Schema({}), descriptor, {},
                                           /*total_records=*/-1, /*total_bytes=*/-1));
    return std::make_unique<FlightInfo>(std::move(info));
  }
};

class AdbcTlsTest : public AdbcServerTest<NoOpTestServer> {
 protected:
  arrow::Result<Location> GetLocation(const std::string& host, int port) override {
    return Location::ForGrpcTls(host, port);
  }

  void Configure(FlightServerOptions* options) override {
    ASSERT_OK(ExampleTlsCertificates(&options->tls_certificates));
    headers_ = std::make_shared<HeaderServerMiddlewareFactory>();
    options->middleware.emplace_back("headers", headers_);
  }
  std::shared_ptr<HeaderServerMiddlewareFactory> headers_;
};

TEST_F(AdbcTlsTest, DisableVerification) {
  {
    // Default connection tries to verify certs, fails
    struct AdbcStatement stmt = {};
    struct ArrowArrayStream out = {};
    ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
    ASSERT_THAT(
        AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
        ::testing::Not(IsOkStatus(&error_)));
    ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
  }
  {
    // Disabling verification works
    ASSERT_THAT(AdbcConnectionRelease(&connection_, /*error=*/nullptr),
                IsOkStatus(&error_));
    ASSERT_THAT(
        AdbcDatabaseSetOption(
            &database_, "arrow.flight.sql.client_option.disable_server_verification",
            ADBC_OPTION_VALUE_ENABLED, &error_),
        IsOkStatus(&error_));

    ASSERT_THAT(AdbcConnectionNew(&connection_, /*error=*/nullptr), IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, /*error=*/nullptr),
                IsOkStatus(&error_));

    struct AdbcStatement stmt = {};
    struct ArrowArrayStream out = {};
    ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
    ASSERT_THAT(
        AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
        IsOkStatus(&error_));
    out.release(&out);
    ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
  }
}

TEST_F(AdbcTlsTest, GenericIntOption) {
  ASSERT_THAT(AdbcDatabaseSetOption(&database_,
                                    "arrow.flight.sql.client_option.generic_int_option."
                                    "grpc.max_send_message_length",
                                    "invalid", &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(AdbcDatabaseSetOption(&database_,
                                    "arrow.flight.sql.client_option.generic_int_option."
                                    "grpc.max_send_message_length",
                                    "0", &error_),
              IsOkStatus(&error_));
}

TEST_F(AdbcTlsTest, GenericStringOption) {
  ASSERT_THAT(AdbcConnectionRelease(&connection_, /*error=*/nullptr),
              IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database_,
                            "arrow.flight.sql.client_option.disable_server_verification",
                            ADBC_OPTION_VALUE_ENABLED, &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcDatabaseSetOption(
          &database_,
          "arrow.flight.sql.client_option.generic_string_option.grpc.primary_user_agent",
          "custom user agent", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionNew(&connection_, /*error=*/nullptr), IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, /*error=*/nullptr),
              IsOkStatus(&error_));

  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};
  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));
  out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
  std::string found_header;
  for (const auto& pair : headers_->recorded_headers_) {
    if (pair.first == "user-agent") {
      found_header = pair.second;
    }
  }
  ASSERT_THAT(found_header, ::testing::HasSubstr("custom user agent"));
}

class AdbcJdbcStyleAuthTest : public AdbcServerTest<NoOpTestServer> {
 protected:
  void Configure(FlightServerOptions* options) override {
    headers_ = std::make_shared<HeaderServerMiddlewareFactory>();
    options->middleware.emplace_back("headers", headers_);
    options->middleware.emplace_back("auth",
                                     std::make_shared<TestAuthServerMiddlewareFactory>());
  }

  void ReconnectWithToken(const std::string& token) {
    ASSERT_THAT(AdbcConnectionRelease(&connection_, /*error=*/nullptr),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseSetOption(&database_, "arrow.flight.sql.authorization_header",
                                      token.c_str(), &error_),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionNew(&connection_, /*error=*/nullptr), IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, /*error=*/nullptr),
                IsOkStatus(&error_));
  }

  class TestAuthServerMiddleware : public ServerMiddleware {
   public:
    std::string name() const override { return "auth"; }
    void SendingHeaders(AddCallHeaders* outgoing_headers) override {
      outgoing_headers->AddHeader("authorization", "response token");
    }
    void CallCompleted(const Status&) override {}
  };

  class TestAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
   public:
    Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                     std::shared_ptr<ServerMiddleware>* middleware) {
      if (info.method == FlightMethod::Handshake) return Status::OK();

      auto it = incoming_headers.find("authorization");
      if (it == incoming_headers.end()) {
        return MakeFlightError(FlightStatusCode::Unauthenticated, "Missing auth header");
      }
      if (it->second == "initial token" || it->second == "response token") {
        *middleware = std::make_shared<TestAuthServerMiddleware>();
        return Status::OK();
      }
      return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid auth header");
    }
  };

  std::shared_ptr<HeaderServerMiddlewareFactory> headers_;
};

TEST_F(AdbcJdbcStyleAuthTest, SuccessfulAuth) {
  ASSERT_NO_FATAL_FAILURE(ReconnectWithToken("initial token"));

  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));
  out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));

  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("authorization", "initial token")));
  headers_->recorded_headers_.clear();

  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));
  out.release(&out);
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));

  ASSERT_THAT(headers_->recorded_headers_,
              ::testing::Contains(std::make_pair("authorization", "response token")));
}

TEST_F(AdbcJdbcStyleAuthTest, FailedAuth) {
  ASSERT_NO_FATAL_FAILURE(ReconnectWithToken("wrong token"));

  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};
  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(error_.message, ::testing::HasSubstr("Invalid auth header"));
}

TEST_F(AdbcJdbcStyleAuthTest, NoAuth) {
  struct AdbcStatement stmt = {};
  struct ArrowArrayStream out = {};
  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&stmt, "query", &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&stmt, &out, /*rows_affected=*/nullptr, &error_),
              ::testing::Not(IsOkStatus(&error_)));
  ASSERT_THAT(AdbcStatementRelease(&stmt, &error_), IsOkStatus(&error_));
  ASSERT_THAT(error_.message, ::testing::HasSubstr("Missing auth header"));
}

class AdbcSqlInfoTest : public AdbcServerTest<FlightSqlServerBase> {};

TEST_F(AdbcSqlInfoTest, NoTransactionSupport) {
  ASSERT_EQ(ADBC_STATUS_NOT_IMPLEMENTED,
            AdbcConnectionSetOption(&connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error_));
  ASSERT_THAT(error_.message,
              ::testing::HasSubstr("Server does not report transaction support"));
}

}  // namespace arrow::flight::sql
