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

#include "arrow/flight/integration_tests/test_integration.h"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/checked_cast.h"

namespace arrow {
namespace flight {
namespace integration_tests {
namespace {

using arrow::internal::checked_cast;

/// \brief The server for the basic auth integration test.
class AuthBasicProtoServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // Respond with the authenticated username.
    auto buf = Buffer::FromString(context.peer_identity());
    *result = std::make_unique<SimpleResultStream>(std::vector<Result>{Result{buf}});
    return Status::OK();
  }
};

/// Validate the result of a DoAction.
Status CheckActionResults(FlightClient* client, const Action& action,
                          std::vector<std::string> results) {
  std::unique_ptr<ResultStream> stream;
  ARROW_ASSIGN_OR_RAISE(stream, client->DoAction(action));
  std::unique_ptr<Result> result;
  for (const std::string& expected : results) {
    ARROW_ASSIGN_OR_RAISE(result, stream->Next());
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    const auto actual = result->body->ToString();
    if (expected != actual) {
      return Status::Invalid("Got wrong result; expected", expected, "but got", actual);
    }
  }
  ARROW_ASSIGN_OR_RAISE(result, stream->Next());
  if (result) {
    return Status::Invalid("Action result stream had too many entries");
  }
  return Status::OK();
}

// The expected username for the basic auth integration test.
constexpr auto kAuthUsername = "arrow";
// The expected password for the basic auth integration test.
constexpr auto kAuthPassword = "flight";

/// \brief A scenario testing the basic auth protobuf.
class AuthBasicProtoScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new AuthBasicProtoServer());
    options->auth_handler =
        std::make_shared<TestServerBasicAuthHandler>(kAuthUsername, kAuthPassword);
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    Action action;
    ARROW_ASSIGN_OR_RAISE(auto stream, client->DoAction(action));
    const auto status = stream->Next().status();
    std::shared_ptr<FlightStatusDetail> detail = FlightStatusDetail::UnwrapStatus(status);
    // This client is unauthenticated and should fail.
    if (detail == nullptr) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", status.ToString());
    }
    if (detail->code() != FlightStatusCode::Unauthenticated) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", detail->ToString());
    }

    auto client_handler =
        std::make_unique<TestClientBasicAuthHandler>(kAuthUsername, kAuthPassword);
    RETURN_NOT_OK(client->Authenticate({}, std::move(client_handler)));
    return CheckActionResults(client.get(), action, {kAuthUsername});
  }
};

/// \brief Test middleware that echoes back the value of a particular
/// incoming header.
///
/// In Java, gRPC may consolidate this header with HTTP/2 trailers if
/// the call fails, but C++ generally doesn't do this. The integration
/// test confirms the presence of this header to ensure we can read it
/// regardless of what gRPC does.
class TestServerMiddleware : public ServerMiddleware {
 public:
  explicit TestServerMiddleware(std::string received) : received_(std::move(received)) {}

  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    outgoing_headers->AddHeader("x-middleware", received_);
  }

  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "GrpcTrailersMiddleware"; }

 private:
  std::string received_;
};

class TestServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-middleware");
    std::string received = "";
    if (iter_pair.first != iter_pair.second) {
      const std::string_view& value = (*iter_pair.first).second;
      received = std::string(value);
    }
    *middleware = std::make_shared<TestServerMiddleware>(received);
    return Status::OK();
  }
};

/// \brief Test middleware that adds a header on every outgoing call,
/// and gets the value of the expected header sent by the server.
class TestClientMiddleware : public ClientMiddleware {
 public:
  explicit TestClientMiddleware(std::string* received_header)
      : received_header_(received_header) {}

  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    outgoing_headers->AddHeader("x-middleware", "expected value");
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) override {
    // We expect the server to always send this header. gRPC/Java may
    // send it in trailers instead of headers, so we expect Flight to
    // account for this.
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-middleware");
    if (iter_pair.first != iter_pair.second) {
      const std::string_view& value = (*iter_pair.first).second;
      *received_header_ = std::string(value);
    }
  }

  void CallCompleted(const Status& status) override {}

 private:
  std::string* received_header_;
};

class TestClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  void StartCall(const CallInfo& info,
                 std::unique_ptr<ClientMiddleware>* middleware) override {
    *middleware = std::make_unique<TestClientMiddleware>(&received_header_);
  }

  std::string received_header_;
};

/// \brief The server used for testing middleware. Implements only one
/// endpoint, GetFlightInfo, in such a way that it either succeeds or
/// returns an error based on the input, in order to test both paths.
class MiddlewareServer : public FlightServerBase {
  Status GetFlightInfo(const ServerCallContext& context,
                       const FlightDescriptor& descriptor,
                       std::unique_ptr<FlightInfo>* result) override {
    if (descriptor.type == FlightDescriptor::DescriptorType::CMD &&
        descriptor.cmd == "success") {
      // Don't fail
      std::shared_ptr<Schema> schema = arrow::schema({});
      // Return a fake location - the test doesn't read it
      ARROW_ASSIGN_OR_RAISE(auto location, Location::ForGrpcTcp("localhost", 10010));
      std::vector<FlightEndpoint> endpoints{FlightEndpoint{{"foo"}, {location}}};
      ARROW_ASSIGN_OR_RAISE(auto info,
                            FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
      *result = std::make_unique<FlightInfo>(info);
      return Status::OK();
    }
    // Fail the call immediately. In some gRPC implementations, this
    // means that gRPC sends only HTTP/2 trailers and not headers. We want
    // Flight middleware to be agnostic to this difference.
    return Status::UnknownError("Unknown");
  }
};

/// \brief The middleware scenario.
///
/// This tests that the server and client get expected header values.
class MiddlewareScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    options->middleware.emplace_back("grpc_trailers",
                                     std::make_shared<TestServerMiddlewareFactory>());
    server->reset(new MiddlewareServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override {
    client_middleware_ = std::make_shared<TestClientMiddlewareFactory>();
    options->middleware.push_back(client_middleware_);
    return Status::OK();
  }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    // This call is expected to fail. In gRPC/Java, this causes the
    // server to combine headers and HTTP/2 trailers, so to read the
    // expected header, Flight must check for both headers and
    // trailers.
    if (client->GetFlightInfo(FlightDescriptor::Command("")).status().ok()) {
      return Status::Invalid("Expected call to fail");
    }
    if (client_middleware_->received_header_ != "expected value") {
      return Status::Invalid(
          "Expected to receive header 'x-middleware: expected value', but instead got: '",
          client_middleware_->received_header_, "'");
    }
    std::cerr << "Headers received successfully on failing call." << std::endl;

    // This call should succeed
    client_middleware_->received_header_ = "";
    ARROW_ASSIGN_OR_RAISE(auto info,
                          client->GetFlightInfo(FlightDescriptor::Command("success")));
    if (client_middleware_->received_header_ != "expected value") {
      return Status::Invalid(
          "Expected to receive header 'x-middleware: expected value', but instead got '",
          client_middleware_->received_header_, "'");
    }
    std::cerr << "Headers received successfully on passing call." << std::endl;
    return Status::OK();
  }

  std::shared_ptr<TestClientMiddlewareFactory> client_middleware_;
};

/// \brief Schema to be returned for mocking the statement/prepared statement results.
///
/// Must be the same across all languages.
const std::shared_ptr<Schema>& GetQuerySchema() {
  static std::shared_ptr<Schema> kSchema =
      schema({field("id", int64(), /*nullable=*/true,
                    arrow::flight::sql::ColumnMetadata::Builder()
                        .TableName("test")
                        .IsAutoIncrement(true)
                        .IsCaseSensitive(false)
                        .TypeName("type_test")
                        .SchemaName("schema_test")
                        .IsSearchable(true)
                        .CatalogName("catalog_test")
                        .Precision(100)
                        .Build()
                        .metadata_map())});
  return kSchema;
}

/// \brief Schema to be returned for queries with transactions.
///
/// Must be the same across all languages.
std::shared_ptr<Schema> GetQueryWithTransactionSchema() {
  static std::shared_ptr<Schema> kSchema =
      schema({field("pkey", int32(), /*nullable=*/true,
                    arrow::flight::sql::ColumnMetadata::Builder()
                        .TableName("test")
                        .IsAutoIncrement(true)
                        .IsCaseSensitive(false)
                        .TypeName("type_test")
                        .SchemaName("schema_test")
                        .IsSearchable(true)
                        .CatalogName("catalog_test")
                        .Precision(100)
                        .Build()
                        .metadata_map())});
  return kSchema;
}

constexpr int64_t kUpdateStatementExpectedRows = 10000L;
constexpr int64_t kUpdateStatementWithTransactionExpectedRows = 15000L;
constexpr int64_t kUpdatePreparedStatementExpectedRows = 20000L;
constexpr int64_t kUpdatePreparedStatementWithTransactionExpectedRows = 25000L;
constexpr char kSelectStatement[] = "SELECT STATEMENT";
constexpr char kSavepointId[] = "savepoint_id";
constexpr char kSavepointName[] = "savepoint_name";
constexpr char kSubstraitPlanText[] = "plan";
constexpr char kSubstraitVersion[] = "version";
static const sql::SubstraitPlan kSubstraitPlan{kSubstraitPlanText, kSubstraitVersion};
constexpr char kTransactionId[] = "transaction_id";

template <typename T>
arrow::Status AssertEq(const T& expected, const T& actual, const std::string& message) {
  if (expected != actual) {
    return Status::Invalid(message, ": expected \"", expected, "\", got \"", actual,
                           "\"");
  }
  return Status::OK();
}

template <typename T>
arrow::Status AssertUnprintableEq(const T& expected, const T& actual,
                                  const std::string& message) {
  if (expected != actual) {
    return Status::Invalid(message);
  }
  return Status::OK();
}

/// \brief The server used for testing Flight SQL, this implements a static Flight SQL
/// server which only asserts that commands called during integration tests are being
/// parsed correctly and returns the expected schemas to be validated on client.
class FlightSqlScenarioServer : public sql::FlightSqlServerBase {
 public:
  FlightSqlScenarioServer() : sql::FlightSqlServerBase() {
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SQL,
                    sql::SqlInfoResult(false));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT,
                    sql::SqlInfoResult(true));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION,
                    sql::SqlInfoResult(std::string("min_version")));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION,
                    sql::SqlInfoResult(std::string("max_version")));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
                    sql::SqlInfoResult(sql::SqlInfoOptions::SqlSupportedTransaction::
                                           SQL_SUPPORTED_TRANSACTION_SAVEPOINT));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_CANCEL,
                    sql::SqlInfoResult(true));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT,
                    sql::SqlInfoResult(int32_t(42)));
    RegisterSqlInfo(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT,
                    sql::SqlInfoResult(int32_t(7)));
  }
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const sql::StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSelectStatement, command.query,
                              "Unexpected statement in GetFlightInfoStatement"));
    std::string ticket;
    Schema* schema;
    if (command.transaction_id.empty()) {
      ticket = "SELECT STATEMENT HANDLE";
      schema = GetQuerySchema().get();
    } else {
      ticket = "SELECT STATEMENT WITH TXN HANDLE";
      schema = GetQueryWithTransactionSchema().get();
    }
    ARROW_ASSIGN_OR_RAISE(auto handle, sql::CreateStatementQueryTicket(ticket));
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{handle}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
    return std::make_unique<FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSubstraitPlan(
      const ServerCallContext& context, const sql::StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitPlanText, command.plan.plan,
                              "Unexpected plan in GetFlightInfoSubstraitPlan"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitVersion, command.plan.version,
                              "Unexpected version in GetFlightInfoSubstraitPlan"));
    std::string ticket;
    Schema* schema;
    if (command.transaction_id.empty()) {
      ticket = "PLAN HANDLE";
      schema = GetQuerySchema().get();
    } else {
      ticket = "PLAN WITH TXN HANDLE";
      schema = GetQueryWithTransactionSchema().get();
    }
    ARROW_ASSIGN_OR_RAISE(auto handle, sql::CreateStatementQueryTicket(ticket));
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{handle}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1));
    return std::make_unique<FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaStatement(
      const ServerCallContext& context, const sql::StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        kSelectStatement, command.query, "Unexpected statement in GetSchemaStatement"));
    if (command.transaction_id.empty()) {
      return SchemaResult::Make(*GetQuerySchema());
    } else {
      return SchemaResult::Make(*GetQueryWithTransactionSchema());
    }
  }

  arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaSubstraitPlan(
      const ServerCallContext& context, const sql::StatementSubstraitPlan& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitPlanText, command.plan.plan,
                              "Unexpected statement in GetSchemaSubstraitPlan"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitVersion, command.plan.version,
                              "Unexpected version in GetFlightInfoSubstraitPlan"));
    if (command.transaction_id.empty()) {
      return SchemaResult::Make(*GetQuerySchema());
    } else {
      return SchemaResult::Make(*GetQueryWithTransactionSchema());
    }
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context,
      const sql::StatementQueryTicket& command) override {
    if (command.statement_handle == "SELECT STATEMENT HANDLE" ||
        command.statement_handle == "PLAN HANDLE") {
      return DoGetForTestCase(GetQuerySchema());
    } else if (command.statement_handle == "SELECT STATEMENT WITH TXN HANDLE" ||
               command.statement_handle == "PLAN WITH TXN HANDLE") {
      return DoGetForTestCase(GetQueryWithTransactionSchema());
    }
    return Status::Invalid("Unknown handle: ", command.statement_handle);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const sql::PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) override {
    if (command.prepared_statement_handle == "SELECT PREPARED STATEMENT HANDLE" ||
        command.prepared_statement_handle == "PLAN HANDLE") {
      return GetFlightInfoForCommand(descriptor, GetQuerySchema());
    } else if (command.prepared_statement_handle ==
                   "SELECT PREPARED STATEMENT WITH TXN HANDLE" ||
               command.prepared_statement_handle == "PLAN WITH TXN HANDLE") {
      return GetFlightInfoForCommand(descriptor, GetQueryWithTransactionSchema());
    }
    return Status::Invalid("Invalid handle for GetFlightInfoForCommand: ",
                           command.prepared_statement_handle);
  }

  arrow::Result<std::unique_ptr<SchemaResult>> GetSchemaPreparedStatement(
      const ServerCallContext& context, const sql::PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) override {
    if (command.prepared_statement_handle == "SELECT PREPARED STATEMENT HANDLE" ||
        command.prepared_statement_handle == "PLAN HANDLE") {
      return SchemaResult::Make(*GetQuerySchema());
    } else if (command.prepared_statement_handle ==
                   "SELECT PREPARED STATEMENT WITH TXN HANDLE" ||
               command.prepared_statement_handle == "PLAN WITH TXN HANDLE") {
      return SchemaResult::Make(*GetQueryWithTransactionSchema());
    }
    return Status::Invalid("Invalid handle for GetSchemaPreparedStatement: ",
                           command.prepared_statement_handle);
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context,
      const sql::PreparedStatementQuery& command) override {
    if (command.prepared_statement_handle == "SELECT PREPARED STATEMENT HANDLE" ||
        command.prepared_statement_handle == "PLAN HANDLE") {
      return DoGetForTestCase(GetQuerySchema());
    } else if (command.prepared_statement_handle ==
                   "SELECT PREPARED STATEMENT WITH TXN HANDLE" ||
               command.prepared_statement_handle == "PLAN WITH TXN HANDLE") {
      return DoGetForTestCase(GetQueryWithTransactionSchema());
    }
    return Status::Invalid("Invalid handle: ", command.prepared_statement_handle);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCatalogs(
      const ServerCallContext& context, const FlightDescriptor& descriptor) override {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCatalogs(
      const ServerCallContext& context) override {
    return DoGetForTestCase(sql::SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoXdbcTypeInfo(
      const ServerCallContext& context, const sql::GetXdbcTypeInfo& command,
      const FlightDescriptor& descriptor) override {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetXdbcTypeInfoSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetXdbcTypeInfo(
      const ServerCallContext& context, const sql::GetXdbcTypeInfo& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetXdbcTypeInfoSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSqlInfo(
      const ServerCallContext& context, const sql::GetSqlInfo& command,
      const FlightDescriptor& descriptor) override {
    if (command.info.size() == 2) {
      // Integration test for the protocol messages
      ARROW_RETURN_NOT_OK(
          AssertEq<int32_t>(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME,
                            command.info[0], "Unexpected SqlInfo passed"));
      ARROW_RETURN_NOT_OK(
          AssertEq<int32_t>(sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY,
                            command.info[1], "Unexpected SqlInfo passed"));

      return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetSqlInfoSchema());
    }
    // Integration test for the values themselves
    return sql::FlightSqlServerBase::GetFlightInfoSqlInfo(context, command, descriptor);
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetSqlInfo(
      const ServerCallContext& context, const sql::GetSqlInfo& command) override {
    if (command.info.size() == 2) {
      // Integration test for the protocol messages
      return DoGetForTestCase(sql::SqlSchema::GetSqlInfoSchema());
    }
    // Integration test for the values themselves
    return sql::FlightSqlServerBase::DoGetSqlInfo(context, command);
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext& context, const sql::GetDbSchemas& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("catalog", command.catalog.value(),
                                              "Wrong catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("db_schema_filter_pattern",
                                              command.db_schema_filter_pattern.value(),
                                              "Wrong db_schema_filter_pattern passed"));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const sql::GetDbSchemas& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const sql::GetTables& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("catalog", command.catalog.value(),
                                              "Wrong catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("db_schema_filter_pattern",
                                              command.db_schema_filter_pattern.value(),
                                              "Wrong db_schema_filter_pattern passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table_filter_pattern",
                                              command.table_name_filter_pattern.value(),
                                              "Wrong table_filter_pattern passed"));
    ARROW_RETURN_NOT_OK(AssertEq<int64_t>(2, command.table_types.size(),
                                          "Wrong number of table types passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table", command.table_types[0],
                                              "Wrong table type passed"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("view", command.table_types[1], "Wrong table type passed"));
    ARROW_RETURN_NOT_OK(
        AssertEq<bool>(true, command.include_schema, "include_schema should be true"));

    return GetFlightInfoForCommand(descriptor,
                                   sql::SqlSchema::GetTablesSchemaWithIncludedSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTables(
      const ServerCallContext& context, const sql::GetTables& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetTablesSchemaWithIncludedSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTableTypes(
      const ServerCallContext& context, const FlightDescriptor& descriptor) override {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetTableTypes(
      const ServerCallContext& context) override {
    return DoGetForTestCase(sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPrimaryKeys(
      const ServerCallContext& context, const sql::GetPrimaryKeys& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "catalog", command.table_ref.catalog.value(), "Wrong catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "db_schema", command.table_ref.db_schema.value(), "Wrong db_schema passed"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("table", command.table_ref.table, "Wrong table passed"));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const sql::GetPrimaryKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext& context, const sql::GetExportedKeys& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "catalog", command.table_ref.catalog.value(), "Wrong catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "db_schema", command.table_ref.db_schema.value(), "Wrong db_schema passed"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("table", command.table_ref.table, "Wrong table passed"));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext& context, const sql::GetExportedKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext& context, const sql::GetImportedKeys& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "catalog", command.table_ref.catalog.value(), "Wrong catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "db_schema", command.table_ref.db_schema.value(), "Wrong db_schema passed"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("table", command.table_ref.table, "Wrong table passed"));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext& context, const sql::GetImportedKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context, const sql::GetCrossReference& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "pk_catalog", command.pk_table_ref.catalog.value(), "Wrong pk catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("pk_db_schema",
                                              command.pk_table_ref.db_schema.value(),
                                              "Wrong pk db_schema passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("pk_table", command.pk_table_ref.table,
                                              "Wrong pk table passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        "fk_catalog", command.fk_table_ref.catalog.value(), "Wrong fk catalog passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("fk_db_schema",
                                              command.fk_table_ref.db_schema.value(),
                                              "Wrong fk db_schema passed"));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("fk_table", command.fk_table_ref.table,
                                              "Wrong fk table passed"));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context, const sql::GetCrossReference& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const ServerCallContext& context, const sql::StatementUpdate& command) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("UPDATE STATEMENT", command.query,
                              "Wrong query for DoPutCommandStatementUpdate"));
    return command.transaction_id.empty() ? kUpdateStatementExpectedRows
                                          : kUpdateStatementWithTransactionExpectedRows;
  }

  arrow::Result<int64_t> DoPutCommandSubstraitPlan(
      const ServerCallContext& context,
      const sql::StatementSubstraitPlan& command) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitPlanText, command.plan.plan,
                              "Wrong plan for DoPutCommandSubstraitPlan"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitVersion, command.plan.version,
                              "Unexpected version in GetFlightInfoSubstraitPlan"));
    return command.transaction_id.empty() ? kUpdateStatementExpectedRows
                                          : kUpdateStatementWithTransactionExpectedRows;
  }

  arrow::Result<sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ServerCallContext& context,
      const sql::ActionCreatePreparedStatementRequest& request) override {
    if (request.query != "SELECT PREPARED STATEMENT" &&
        request.query != "UPDATE PREPARED STATEMENT") {
      return Status::Invalid("Unexpected query: ", request.query);
    }

    sql::ActionCreatePreparedStatementResult result;
    result.prepared_statement_handle = request.query;
    if (!request.transaction_id.empty()) {
      result.prepared_statement_handle += " WITH TXN";
    }
    result.prepared_statement_handle += " HANDLE";
    return result;
  }

  arrow::Result<sql::ActionCreatePreparedStatementResult> CreatePreparedSubstraitPlan(
      const ServerCallContext& context,
      const sql::ActionCreatePreparedSubstraitPlanRequest& request) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitPlanText, request.plan.plan,
                              "Wrong plan for CreatePreparedSubstraitPlan"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kSubstraitVersion, request.plan.version,
                              "Unexpected version in GetFlightInfoSubstraitPlan"));
    sql::ActionCreatePreparedStatementResult result;
    result.prepared_statement_handle =
        request.transaction_id.empty() ? "PLAN HANDLE" : "PLAN WITH TXN HANDLE";
    return result;
  }

  Status ClosePreparedStatement(
      const ServerCallContext& context,
      const sql::ActionClosePreparedStatementRequest& request) override {
    if (request.prepared_statement_handle != "SELECT PREPARED STATEMENT HANDLE" &&
        request.prepared_statement_handle != "UPDATE PREPARED STATEMENT HANDLE" &&
        request.prepared_statement_handle != "PLAN HANDLE" &&
        request.prepared_statement_handle !=
            "SELECT PREPARED STATEMENT WITH TXN HANDLE" &&
        request.prepared_statement_handle !=
            "UPDATE PREPARED STATEMENT WITH TXN HANDLE" &&
        request.prepared_statement_handle != "PLAN WITH TXN HANDLE") {
      return Status::Invalid("Invalid handle for ClosePreparedStatement: ",
                             request.prepared_statement_handle);
    }
    return Status::OK();
  }

  Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                     const sql::PreparedStatementQuery& command,
                                     FlightMessageReader* reader,
                                     FlightMetadataWriter* writer) override {
    if (command.prepared_statement_handle != "SELECT PREPARED STATEMENT HANDLE" &&
        command.prepared_statement_handle !=
            "SELECT PREPARED STATEMENT WITH TXN HANDLE" &&
        command.prepared_statement_handle != "PLAN HANDLE" &&
        command.prepared_statement_handle != "PLAN WITH TXN HANDLE") {
      return Status::Invalid("Invalid handle for DoPutPreparedStatementQuery: ",
                             command.prepared_statement_handle);
    }
    ARROW_ASSIGN_OR_RAISE(auto actual_schema, reader->GetSchema());
    ARROW_RETURN_NOT_OK(AssertEq<Schema>(*GetQuerySchema(), *actual_schema,
                                         "Wrong schema for DoPutPreparedStatementQuery"));
    return Status::OK();
  }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const sql::PreparedStatementUpdate& command,
      FlightMessageReader* reader) override {
    if (command.prepared_statement_handle == "UPDATE PREPARED STATEMENT HANDLE" ||
        command.prepared_statement_handle == "PLAN HANDLE") {
      return kUpdatePreparedStatementExpectedRows;
    } else if (command.prepared_statement_handle ==
                   "UPDATE PREPARED STATEMENT WITH TXN HANDLE" ||
               command.prepared_statement_handle == "PLAN WITH TXN HANDLE") {
      return kUpdatePreparedStatementWithTransactionExpectedRows;
    }
    return Status::Invalid("Invalid handle for DoPutPreparedStatementUpdate: ",
                           command.prepared_statement_handle);
  }

  arrow::Result<sql::ActionBeginSavepointResult> BeginSavepoint(
      const ServerCallContext& context,
      const sql::ActionBeginSavepointRequest& request) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        kSavepointName, request.name, "Unexpected savepoint name in BeginSavepoint"));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>(kTransactionId, request.transaction_id,
                              "Unexpected transaction ID in BeginSavepoint"));
    return sql::ActionBeginSavepointResult{kSavepointId};
  }

  arrow::Result<sql::ActionBeginTransactionResult> BeginTransaction(
      const ServerCallContext& context,
      const sql::ActionBeginTransactionRequest& request) override {
    return sql::ActionBeginTransactionResult{kTransactionId};
  }

  arrow::Result<sql::CancelResult> CancelQuery(
      const ServerCallContext& context,
      const sql::ActionCancelQueryRequest& request) override {
    ARROW_RETURN_NOT_OK(AssertEq<size_t>(1, request.info->endpoints().size(),
                                         "Expected 1 endpoint for CancelQuery"));
    const FlightEndpoint& endpoint = request.info->endpoints()[0];
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          sql::StatementQueryTicket::Deserialize(endpoint.ticket.ticket));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("PLAN HANDLE", ticket.statement_handle,
                                              "Unexpected ticket in CancelQuery"));
    return sql::CancelResult::kCancelled;
  }

  Status EndSavepoint(const ServerCallContext& context,
                      const sql::ActionEndSavepointRequest& request) override {
    switch (request.action) {
      case sql::ActionEndSavepointRequest::kRelease:
      case sql::ActionEndSavepointRequest::kRollback:
        ARROW_RETURN_NOT_OK(
            AssertEq<std::string>(kSavepointId, request.savepoint_id,
                                  "Unexpected savepoint ID in EndSavepoint"));
        break;
      default:
        return Status::Invalid("Unknown action ", static_cast<int>(request.action));
    }
    return Status::OK();
  }

  Status EndTransaction(const ServerCallContext& context,
                        const sql::ActionEndTransactionRequest& request) override {
    switch (request.action) {
      case sql::ActionEndTransactionRequest::kCommit:
      case sql::ActionEndTransactionRequest::kRollback:
        ARROW_RETURN_NOT_OK(
            AssertEq<std::string>(kTransactionId, request.transaction_id,
                                  "Unexpected transaction ID in EndTransaction"));
        break;
      default:
        return Status::Invalid("Unknown action ", static_cast<int>(request.action));
    }
    return Status::OK();
  }

 private:
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
      const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema) {
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

    return std::make_unique<FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetForTestCase(
      const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({}, schema));
    return std::make_unique<RecordBatchStream>(reader);
  }
};

/// \brief Integration test scenario for validating Flight SQL specs across multiple
/// implementations. This should ensure that RPC objects are being built and parsed
/// correctly for multiple languages and that the Arrow schemas are returned as expected.
class FlightSqlScenario : public Scenario {
 public:
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new FlightSqlScenarioServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status Validate(const std::shared_ptr<Schema>& expected_schema,
                  const FlightInfo& flight_info, sql::FlightSqlClient* sql_client) {
    FlightCallOptions call_options;
    ARROW_ASSIGN_OR_RAISE(
        std::unique_ptr<FlightStreamReader> reader,
        sql_client->DoGet(call_options, flight_info.endpoints()[0].ticket));
    ARROW_ASSIGN_OR_RAISE(auto actual_schema, reader->GetSchema());
    if (!expected_schema->Equals(*actual_schema, /*check_metadata=*/true)) {
      return Status::Invalid("Schemas did not match. Expected:\n", *expected_schema,
                             "\nActual:\n", *actual_schema);
    }
    ARROW_RETURN_NOT_OK(reader->ToTable());
    return Status::OK();
  }

  Status ValidateSchema(const std::shared_ptr<Schema>& expected_schema,
                        const SchemaResult& result) {
    ipc::DictionaryMemo memo;
    ARROW_ASSIGN_OR_RAISE(auto actual_schema, result.GetSchema(&memo));
    if (!expected_schema->Equals(*actual_schema, /*check_metadata=*/true)) {
      return Status::Invalid("Schemas did not match. Expected:\n", *expected_schema,
                             "\nActual:\n", *actual_schema);
    }
    return Status::OK();
  }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    sql::FlightSqlClient sql_client(std::move(client));
    ARROW_RETURN_NOT_OK(ValidateMetadataRetrieval(&sql_client));
    ARROW_RETURN_NOT_OK(ValidateStatementExecution(&sql_client));
    ARROW_RETURN_NOT_OK(ValidatePreparedStatementExecution(&sql_client));
    return Status::OK();
  }

  Status ValidateMetadataRetrieval(sql::FlightSqlClient* sql_client) {
    FlightCallOptions options;

    std::string catalog = "catalog";
    std::string db_schema_filter_pattern = "db_schema_filter_pattern";
    std::string table_filter_pattern = "table_filter_pattern";
    std::string table = "table";
    std::string db_schema = "db_schema";
    std::vector<std::string> table_types = {"table", "view"};

    sql::TableRef table_ref = {catalog, db_schema, table};
    sql::TableRef pk_table_ref = {"pk_catalog", "pk_db_schema", "pk_table"};
    sql::TableRef fk_table_ref = {"fk_catalog", "fk_db_schema", "fk_table"};

    std::unique_ptr<FlightInfo> info;
    std::unique_ptr<SchemaResult> schema;

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetCatalogs(options));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetCatalogsSchema(options));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetCatalogsSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetCatalogsSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(
        info, sql_client->GetDbSchemas(options, &catalog, &db_schema_filter_pattern));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetDbSchemasSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetDbSchemasSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetDbSchemasSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(
        info, sql_client->GetTables(options, &catalog, &db_schema_filter_pattern,
                                    &table_filter_pattern, true, &table_types));
    ARROW_ASSIGN_OR_RAISE(schema,
                          sql_client->GetTablesSchema(options, /*include_schema=*/true));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetTablesSchemaWithIncludedSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(
        ValidateSchema(sql::SqlSchema::GetTablesSchemaWithIncludedSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(schema,
                          sql_client->GetTablesSchema(options, /*include_schema=*/false));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetTablesSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetTableTypes(options));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetTableTypesSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetTableTypesSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetTableTypesSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetPrimaryKeys(options, table_ref));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetPrimaryKeysSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetPrimaryKeysSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetPrimaryKeysSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetExportedKeys(options, table_ref));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetExportedKeysSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetExportedKeysSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetExportedKeysSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetImportedKeys(options, table_ref));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetImportedKeysSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetImportedKeysSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetImportedKeysSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(
        info, sql_client->GetCrossReference(options, pk_table_ref, fk_table_ref));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetCrossReferenceSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetCrossReferenceSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(
        ValidateSchema(sql::SqlSchema::GetCrossReferenceSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetXdbcTypeInfo(options));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetXdbcTypeInfoSchema(options));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetXdbcTypeInfoSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetXdbcTypeInfoSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(
        info, sql_client->GetSqlInfo(
                  options, {sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME,
                            sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY}));
    ARROW_ASSIGN_OR_RAISE(schema, sql_client->GetSqlInfoSchema(options));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetSqlInfoSchema(), *info, sql_client));
    ARROW_RETURN_NOT_OK(ValidateSchema(sql::SqlSchema::GetSqlInfoSchema(), *schema));

    return Status::OK();
  }

  Status ValidateStatementExecution(sql::FlightSqlClient* sql_client) {
    ARROW_ASSIGN_OR_RAISE(auto info, sql_client->Execute({}, kSelectStatement));
    ARROW_RETURN_NOT_OK(Validate(GetQuerySchema(), *info, sql_client));

    ARROW_ASSIGN_OR_RAISE(auto schema,
                          sql_client->GetExecuteSchema({}, kSelectStatement));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQuerySchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(auto updated_rows,
                          sql_client->ExecuteUpdate({}, "UPDATE STATEMENT"));
    ARROW_RETURN_NOT_OK(AssertEq(kUpdateStatementExpectedRows, updated_rows,
                                 "Wrong number of updated rows for ExecuteUpdate"));

    return Status::OK();
  }

  Status ValidatePreparedStatementExecution(sql::FlightSqlClient* sql_client) {
    auto parameters =
        RecordBatch::Make(GetQuerySchema(), 1, {ArrayFromJSON(int64(), "[1]")});

    ARROW_ASSIGN_OR_RAISE(auto select_prepared_statement,
                          sql_client->Prepare({}, "SELECT PREPARED STATEMENT"));
    ARROW_RETURN_NOT_OK(select_prepared_statement->SetParameters(parameters));
    ARROW_ASSIGN_OR_RAISE(auto info, select_prepared_statement->Execute());
    ARROW_RETURN_NOT_OK(Validate(GetQuerySchema(), *info, sql_client));
    ARROW_ASSIGN_OR_RAISE(auto schema, select_prepared_statement->GetSchema({}));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQuerySchema(), *schema));
    ARROW_RETURN_NOT_OK(select_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(auto update_prepared_statement,
                          sql_client->Prepare({}, "UPDATE PREPARED STATEMENT"));
    ARROW_ASSIGN_OR_RAISE(auto updated_rows, update_prepared_statement->ExecuteUpdate());
    ARROW_RETURN_NOT_OK(
        AssertEq(kUpdatePreparedStatementExpectedRows, updated_rows,
                 "Wrong number of updated rows for prepared statement ExecuteUpdate"));
    ARROW_RETURN_NOT_OK(update_prepared_statement->Close());
    return Status::OK();
  }
};

/// \brief Integration test scenario for validating the Substrait and
///    transaction extensions to Flight SQL.
class FlightSqlExtensionScenario : public FlightSqlScenario {
 public:
  Status RunClient(std::unique_ptr<FlightClient> client) override {
    sql::FlightSqlClient sql_client(std::move(client));
    Status status;
    if (!(status = ValidateMetadataRetrieval(&sql_client)).ok()) {
      return status.WithMessage("MetadataRetrieval failed: ", status.message());
    }
    if (!(status = ValidateStatementExecution(&sql_client)).ok()) {
      return status.WithMessage("StatementExecution failed: ", status.message());
    }
    if (!(status = ValidatePreparedStatementExecution(&sql_client)).ok()) {
      return status.WithMessage("PreparedStatementExecution failed: ", status.message());
    }
    if (!(status = ValidateTransactions(&sql_client)).ok()) {
      return status.WithMessage("Transactions failed: ", status.message());
    }
    return Status::OK();
  }

  Status ValidateMetadataRetrieval(sql::FlightSqlClient* sql_client) {
    std::unique_ptr<FlightInfo> info;
    std::vector<int32_t> sql_info = {
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SQL,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_CANCEL,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT,
        sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT,
    };
    ARROW_ASSIGN_OR_RAISE(info, sql_client->GetSqlInfo({}, sql_info));
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          sql_client->DoGet({}, info->endpoints()[0].ticket));

    ARROW_ASSIGN_OR_RAISE(auto actual_schema, reader->GetSchema());
    if (!sql::SqlSchema::GetSqlInfoSchema()->Equals(*actual_schema,
                                                    /*check_metadata=*/true)) {
      return Status::Invalid("Schemas did not match. Expected:\n",
                             *sql::SqlSchema::GetSqlInfoSchema(), "\nActual:\n",
                             *actual_schema);
    }

    sql::SqlInfoResultMap info_values;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
      if (!chunk.data) break;

      const auto& info_name = checked_cast<const UInt32Array&>(*chunk.data->column(0));
      const auto& value = checked_cast<const DenseUnionArray&>(*chunk.data->column(1));

      for (int64_t i = 0; i < chunk.data->num_rows(); i++) {
        const uint32_t code = info_name.Value(i);
        if (info_values.find(code) != info_values.end()) {
          return Status::Invalid("Duplicate SqlInfo value ", code);
        }
        switch (value.type_code(i)) {
          case 0: {  // string
            std::string slot = checked_cast<const StringArray&>(*value.field(0))
                                   .GetString(value.value_offset(i));
            info_values[code] = sql::SqlInfoResult(std::move(slot));
            break;
          }
          case 1: {  // bool
            bool slot = checked_cast<const BooleanArray&>(*value.field(1))
                            .Value(value.value_offset(i));
            info_values[code] = sql::SqlInfoResult(slot);
            break;
          }
          case 2: {  // int64_t
            int64_t slot = checked_cast<const Int64Array&>(*value.field(2))
                               .Value(value.value_offset(i));
            info_values[code] = sql::SqlInfoResult(slot);
            break;
          }
          case 3: {  // int32_t
            int32_t slot = checked_cast<const Int32Array&>(*value.field(3))
                               .Value(value.value_offset(i));
            info_values[code] = sql::SqlInfoResult(slot);
            break;
          }
          default:
            return Status::NotImplemented("Decoding SqlInfoResult of type code ",
                                          value.type_code(i));
        }
      }
    }

    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SQL],
        sql::SqlInfoResult(false), "FLIGHT_SQL_SERVER_SQL did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT],
        sql::SqlInfoResult(true), "FLIGHT_SQL_SERVER_SUBSTRAIT did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION],
        sql::SqlInfoResult(std::string("min_version")),
        "FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION],
        sql::SqlInfoResult(std::string("max_version")),
        "FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION],
        sql::SqlInfoResult(sql::SqlInfoOptions::SqlSupportedTransaction::
                               SQL_SUPPORTED_TRANSACTION_SAVEPOINT),
        "FLIGHT_SQL_SERVER_TRANSACTION did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_CANCEL],
        sql::SqlInfoResult(true), "FLIGHT_SQL_SERVER_CANCEL did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT],
        sql::SqlInfoResult(int32_t(42)),
        "FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT did not match"));
    ARROW_RETURN_NOT_OK(AssertUnprintableEq(
        info_values[sql::SqlInfoOptions::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT],
        sql::SqlInfoResult(int32_t(7)),
        "FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT did not match"));

    return Status::OK();
  }

  Status ValidateStatementExecution(sql::FlightSqlClient* sql_client) {
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<FlightInfo> info,
                          sql_client->ExecuteSubstrait({}, kSubstraitPlan));
    ARROW_RETURN_NOT_OK(Validate(GetQuerySchema(), *info, sql_client));

    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<SchemaResult> schema,
                          sql_client->GetExecuteSubstraitSchema({}, kSubstraitPlan));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQuerySchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(info, sql_client->ExecuteSubstrait({}, kSubstraitPlan));
    ARROW_ASSIGN_OR_RAISE(sql::CancelResult cancel_result,
                          sql_client->CancelQuery({}, *info));
    ARROW_RETURN_NOT_OK(
        AssertEq(sql::CancelResult::kCancelled, cancel_result, "Wrong cancel result"));

    ARROW_ASSIGN_OR_RAISE(const int64_t updated_rows,
                          sql_client->ExecuteSubstraitUpdate({}, kSubstraitPlan));
    ARROW_RETURN_NOT_OK(
        AssertEq(kUpdateStatementExpectedRows, updated_rows,
                 "Wrong number of updated rows for ExecuteSubstraitUpdate"));

    return Status::OK();
  }

  Status ValidatePreparedStatementExecution(sql::FlightSqlClient* sql_client) {
    auto parameters =
        RecordBatch::Make(GetQuerySchema(), 1, {ArrayFromJSON(int64(), "[1]")});

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> substrait_prepared_statement,
        sql_client->PrepareSubstrait({}, kSubstraitPlan));
    ARROW_RETURN_NOT_OK(substrait_prepared_statement->SetParameters(parameters));
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<FlightInfo> info,
                          substrait_prepared_statement->Execute());
    ARROW_RETURN_NOT_OK(Validate(GetQuerySchema(), *info, sql_client));
    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<SchemaResult> schema,
                          substrait_prepared_statement->GetSchema({}));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQuerySchema(), *schema));
    ARROW_RETURN_NOT_OK(substrait_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> update_substrait_prepared_statement,
        sql_client->PrepareSubstrait({}, kSubstraitPlan));
    ARROW_ASSIGN_OR_RAISE(const int64_t updated_rows,
                          update_substrait_prepared_statement->ExecuteUpdate());
    ARROW_RETURN_NOT_OK(
        AssertEq(kUpdatePreparedStatementExpectedRows, updated_rows,
                 "Wrong number of updated rows for prepared statement ExecuteUpdate"));
    ARROW_RETURN_NOT_OK(update_substrait_prepared_statement->Close());

    return Status::OK();
  }

  Status ValidateTransactions(sql::FlightSqlClient* sql_client) {
    ARROW_ASSIGN_OR_RAISE(sql::Transaction transaction, sql_client->BeginTransaction({}));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        kTransactionId, transaction.transaction_id(), "Wrong transaction ID"));

    ARROW_ASSIGN_OR_RAISE(sql::Savepoint savepoint,
                          sql_client->BeginSavepoint({}, transaction, kSavepointName));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(kSavepointId, savepoint.savepoint_id(),
                                              "Wrong savepoint ID"));

    ARROW_ASSIGN_OR_RAISE(std::unique_ptr<FlightInfo> info,
                          sql_client->Execute({}, kSelectStatement, transaction));
    ARROW_RETURN_NOT_OK(Validate(GetQueryWithTransactionSchema(), *info, sql_client));

    ARROW_ASSIGN_OR_RAISE(info,
                          sql_client->ExecuteSubstrait({}, kSubstraitPlan, transaction));
    ARROW_RETURN_NOT_OK(Validate(GetQueryWithTransactionSchema(), *info, sql_client));

    ARROW_ASSIGN_OR_RAISE(
        std::unique_ptr<SchemaResult> schema,
        sql_client->GetExecuteSchema({}, kSelectStatement, transaction));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQueryWithTransactionSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(
        schema, sql_client->GetExecuteSubstraitSchema({}, kSubstraitPlan, transaction));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQueryWithTransactionSchema(), *schema));

    ARROW_ASSIGN_OR_RAISE(int64_t updated_rows,
                          sql_client->ExecuteUpdate({}, "UPDATE STATEMENT", transaction));
    ARROW_RETURN_NOT_OK(
        AssertEq(kUpdateStatementWithTransactionExpectedRows, updated_rows,
                 "Wrong number of updated rows for ExecuteUpdate with transaction"));
    ARROW_ASSIGN_OR_RAISE(updated_rows, sql_client->ExecuteSubstraitUpdate(
                                            {}, kSubstraitPlan, transaction));
    ARROW_RETURN_NOT_OK(AssertEq(
        kUpdateStatementWithTransactionExpectedRows, updated_rows,
        "Wrong number of updated rows for ExecuteSubstraitUpdate with transaction"));

    auto parameters =
        RecordBatch::Make(GetQuerySchema(), 1, {ArrayFromJSON(int64(), "[1]")});

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> select_prepared_statement,
        sql_client->Prepare({}, "SELECT PREPARED STATEMENT", transaction));
    ARROW_RETURN_NOT_OK(select_prepared_statement->SetParameters(parameters));
    ARROW_ASSIGN_OR_RAISE(info, select_prepared_statement->Execute());
    ARROW_RETURN_NOT_OK(Validate(GetQueryWithTransactionSchema(), *info, sql_client));
    ARROW_ASSIGN_OR_RAISE(schema, select_prepared_statement->GetSchema({}));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQueryWithTransactionSchema(), *schema));
    ARROW_RETURN_NOT_OK(select_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> substrait_prepared_statement,
        sql_client->PrepareSubstrait({}, kSubstraitPlan, transaction));
    ARROW_RETURN_NOT_OK(substrait_prepared_statement->SetParameters(parameters));
    ARROW_ASSIGN_OR_RAISE(info, substrait_prepared_statement->Execute());
    ARROW_RETURN_NOT_OK(Validate(GetQueryWithTransactionSchema(), *info, sql_client));
    ARROW_ASSIGN_OR_RAISE(schema, substrait_prepared_statement->GetSchema({}));
    ARROW_RETURN_NOT_OK(ValidateSchema(GetQueryWithTransactionSchema(), *schema));
    ARROW_RETURN_NOT_OK(substrait_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> update_prepared_statement,
        sql_client->Prepare({}, "UPDATE PREPARED STATEMENT", transaction));
    ARROW_ASSIGN_OR_RAISE(updated_rows, update_prepared_statement->ExecuteUpdate());
    ARROW_RETURN_NOT_OK(AssertEq(kUpdatePreparedStatementWithTransactionExpectedRows,
                                 updated_rows,
                                 "Wrong number of updated rows for prepared statement "
                                 "ExecuteUpdate with transaction"));
    ARROW_RETURN_NOT_OK(update_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(
        std::shared_ptr<sql::PreparedStatement> update_substrait_prepared_statement,
        sql_client->PrepareSubstrait({}, kSubstraitPlan, transaction));
    ARROW_ASSIGN_OR_RAISE(updated_rows,
                          update_substrait_prepared_statement->ExecuteUpdate());
    ARROW_RETURN_NOT_OK(AssertEq(kUpdatePreparedStatementWithTransactionExpectedRows,
                                 updated_rows,
                                 "Wrong number of updated rows for prepared statement "
                                 "ExecuteUpdate with transaction"));
    ARROW_RETURN_NOT_OK(update_substrait_prepared_statement->Close());

    ARROW_RETURN_NOT_OK(sql_client->Rollback({}, savepoint));

    ARROW_ASSIGN_OR_RAISE(sql::Savepoint savepoint2,
                          sql_client->BeginSavepoint({}, transaction, kSavepointName));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(kSavepointId, savepoint.savepoint_id(),
                                              "Wrong savepoint ID"));
    ARROW_RETURN_NOT_OK(sql_client->Release({}, savepoint));

    ARROW_RETURN_NOT_OK(sql_client->Commit({}, transaction));

    ARROW_ASSIGN_OR_RAISE(sql::Transaction transaction2,
                          sql_client->BeginTransaction({}));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>(
        kTransactionId, transaction.transaction_id(), "Wrong transaction ID"));
    ARROW_RETURN_NOT_OK(sql_client->Rollback({}, transaction2));

    return Status::OK();
  }
};
}  // namespace

Status GetScenario(const std::string& scenario_name, std::shared_ptr<Scenario>* out) {
  if (scenario_name == "auth:basic_proto") {
    *out = std::make_shared<AuthBasicProtoScenario>();
    return Status::OK();
  } else if (scenario_name == "middleware") {
    *out = std::make_shared<MiddlewareScenario>();
    return Status::OK();
  } else if (scenario_name == "flight_sql") {
    *out = std::make_shared<FlightSqlScenario>();
    return Status::OK();
  } else if (scenario_name == "flight_sql:extension") {
    *out = std::make_shared<FlightSqlExtensionScenario>();
    return Status::OK();
  }
  return Status::KeyError("Scenario not found: ", scenario_name);
}

}  // namespace integration_tests
}  // namespace flight
}  // namespace arrow
