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
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/column_metadata.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/testing/gtest_util.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace arrow {
namespace flight {
namespace integration_tests {

/// \brief The server for the basic auth integration test.
class AuthBasicProtoServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    // Respond with the authenticated username.
    auto buf = Buffer::FromString(context.peer_identity());
    *result = std::unique_ptr<ResultStream>(new SimpleResultStream({Result{buf}}));
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
    std::unique_ptr<ResultStream> stream;
    std::shared_ptr<FlightStatusDetail> detail;
    const auto& status = client->DoAction(action).Value(&stream);
    detail = FlightStatusDetail::UnwrapStatus(status);
    // This client is unauthenticated and should fail.
    if (detail == nullptr) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", status.ToString());
    }
    if (detail->code() != FlightStatusCode::Unauthenticated) {
      return Status::Invalid("Expected UNAUTHENTICATED but got ", detail->ToString());
    }

    auto client_handler = std::unique_ptr<ClientAuthHandler>(
        new TestClientBasicAuthHandler(kAuthUsername, kAuthPassword));
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
  explicit TestServerMiddleware(std::string received) : received_(received) {}

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
      const util::string_view& value = (*iter_pair.first).second;
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

  void SendingHeaders(AddCallHeaders* outgoing_headers) {
    outgoing_headers->AddHeader("x-middleware", "expected value");
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) {
    // We expect the server to always send this header. gRPC/Java may
    // send it in trailers instead of headers, so we expect Flight to
    // account for this.
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-middleware");
    if (iter_pair.first != iter_pair.second) {
      const util::string_view& value = (*iter_pair.first).second;
      *received_header_ = std::string(value);
    }
  }

  void CallCompleted(const Status& status) {}

 private:
  std::string* received_header_;
};

class TestClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    *middleware =
        std::unique_ptr<ClientMiddleware>(new TestClientMiddleware(&received_header_));
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
      *result = std::unique_ptr<FlightInfo>(new FlightInfo(info));
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
    options->middleware.push_back(
        {"grpc_trailers", std::make_shared<TestServerMiddlewareFactory>()});
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
/// Must be the same across all languages.
std::shared_ptr<Schema> GetQuerySchema() {
  std::string table_name = "test";
  std::string schema_name = "schema_test";
  std::string catalog_name = "catalog_test";
  std::string type_name = "type_test";
  return arrow::schema({arrow::field("id", int64(), true,
                                     arrow::flight::sql::ColumnMetadata::Builder()
                                         .TableName(table_name)
                                         .IsAutoIncrement(true)
                                         .IsCaseSensitive(false)
                                         .TypeName(type_name)
                                         .SchemaName(schema_name)
                                         .IsSearchable(true)
                                         .CatalogName(catalog_name)
                                         .Precision(100)
                                         .Build()
                                         .metadata_map())});
}

constexpr int64_t kUpdateStatementExpectedRows = 10000L;
constexpr int64_t kUpdatePreparedStatementExpectedRows = 20000L;

template <typename T>
arrow::Status AssertEq(const T& expected, const T& actual) {
  if (expected != actual) {
    return Status::Invalid("Expected \"", expected, "\", got \'", actual, "\"");
  }
  return Status::OK();
}

/// \brief The server used for testing Flight SQL, this implements a static Flight SQL
/// server which only asserts that commands called during integration tests are being
/// parsed correctly and returns the expected schemas to be validated on client.
class FlightSqlScenarioServer : public sql::FlightSqlServerBase {
 public:
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoStatement(
      const ServerCallContext& context, const sql::StatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("SELECT STATEMENT", command.query));

    ARROW_ASSIGN_OR_RAISE(auto handle,
                          sql::CreateStatementQueryTicket("SELECT STATEMENT HANDLE"));

    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{handle}, {}}};
    ARROW_ASSIGN_OR_RAISE(
        auto result, FlightInfo::Make(*GetQuerySchema(), descriptor, endpoints, -1, -1))

    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetStatement(
      const ServerCallContext& context,
      const sql::StatementQueryTicket& command) override {
    return DoGetForTestCase(GetQuerySchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoPreparedStatement(
      const ServerCallContext& context, const sql::PreparedStatementQuery& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("SELECT PREPARED STATEMENT HANDLE",
                                              command.prepared_statement_handle));

    return GetFlightInfoForCommand(descriptor, GetQuerySchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPreparedStatement(
      const ServerCallContext& context,
      const sql::PreparedStatementQuery& command) override {
    return DoGetForTestCase(GetQuerySchema());
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
    ARROW_RETURN_NOT_OK(AssertEq<int64_t>(2, command.info.size()));
    ARROW_RETURN_NOT_OK(AssertEq<int32_t>(
        sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME, command.info[0]));
    ARROW_RETURN_NOT_OK(AssertEq<int32_t>(
        sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, command.info[1]));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetSqlInfoSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetSqlInfo(
      const ServerCallContext& context, const sql::GetSqlInfo& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetSqlInfoSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoSchemas(
      const ServerCallContext& context, const sql::GetDbSchemas& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("catalog", command.catalog.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("db_schema_filter_pattern",
                                              command.db_schema_filter_pattern.value()));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetDbSchemas(
      const ServerCallContext& context, const sql::GetDbSchemas& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoTables(
      const ServerCallContext& context, const sql::GetTables& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("catalog", command.catalog.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("db_schema_filter_pattern",
                                              command.db_schema_filter_pattern.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table_filter_pattern",
                                              command.table_name_filter_pattern.value()));
    ARROW_RETURN_NOT_OK(AssertEq<int64_t>(2, command.table_types.size()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table", command.table_types[0]));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("view", command.table_types[1]));
    ARROW_RETURN_NOT_OK(AssertEq<bool>(true, command.include_schema));

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
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("catalog", command.table_ref.catalog.value()));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("db_schema", command.table_ref.db_schema.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table", command.table_ref.table));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetPrimaryKeys(
      const ServerCallContext& context, const sql::GetPrimaryKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoExportedKeys(
      const ServerCallContext& context, const sql::GetExportedKeys& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("catalog", command.table_ref.catalog.value()));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("db_schema", command.table_ref.db_schema.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table", command.table_ref.table));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetExportedKeys(
      const ServerCallContext& context, const sql::GetExportedKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoImportedKeys(
      const ServerCallContext& context, const sql::GetImportedKeys& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("catalog", command.table_ref.catalog.value()));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("db_schema", command.table_ref.db_schema.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("table", command.table_ref.table));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetImportedKeys(
      const ServerCallContext& context, const sql::GetImportedKeys& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoCrossReference(
      const ServerCallContext& context, const sql::GetCrossReference& command,
      const FlightDescriptor& descriptor) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("pk_catalog", command.pk_table_ref.catalog.value()));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("pk_db_schema", command.pk_table_ref.db_schema.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("pk_table", command.pk_table_ref.table));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("fk_catalog", command.fk_table_ref.catalog.value()));
    ARROW_RETURN_NOT_OK(
        AssertEq<std::string>("fk_db_schema", command.fk_table_ref.db_schema.value()));
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("fk_table", command.fk_table_ref.table));

    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetCrossReference(
      const ServerCallContext& context, const sql::GetCrossReference& command) override {
    return DoGetForTestCase(sql::SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const ServerCallContext& context, const sql::StatementUpdate& command) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("UPDATE STATEMENT", command.query));

    return kUpdateStatementExpectedRows;
  }

  arrow::Result<sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const ServerCallContext& context,
      const sql::ActionCreatePreparedStatementRequest& request) override {
    ARROW_RETURN_NOT_OK(
        AssertEq<bool>(true, request.query == "SELECT PREPARED STATEMENT" ||
                                 request.query == "UPDATE PREPARED STATEMENT"));

    sql::ActionCreatePreparedStatementResult result;
    result.prepared_statement_handle = request.query + " HANDLE";

    return result;
  }

  Status ClosePreparedStatement(
      const ServerCallContext& context,
      const sql::ActionClosePreparedStatementRequest& request) override {
    return Status::OK();
  }

  Status DoPutPreparedStatementQuery(const ServerCallContext& context,
                                     const sql::PreparedStatementQuery& command,
                                     FlightMessageReader* reader,
                                     FlightMetadataWriter* writer) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("SELECT PREPARED STATEMENT HANDLE",
                                              command.prepared_statement_handle));

    ARROW_ASSIGN_OR_RAISE(auto actual_schema, reader->GetSchema());
    ARROW_RETURN_NOT_OK(AssertEq<Schema>(*GetQuerySchema(), *actual_schema));

    return Status::OK();
  }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const ServerCallContext& context, const sql::PreparedStatementUpdate& command,
      FlightMessageReader* reader) override {
    ARROW_RETURN_NOT_OK(AssertEq<std::string>("UPDATE PREPARED STATEMENT HANDLE",
                                              command.prepared_statement_handle));

    return kUpdatePreparedStatementExpectedRows;
  }

 private:
  arrow::Result<std::unique_ptr<FlightInfo>> GetFlightInfoForCommand(
      const FlightDescriptor& descriptor, const std::shared_ptr<Schema>& schema) {
    std::vector<FlightEndpoint> endpoints{FlightEndpoint{{descriptor.cmd}, {}}};
    ARROW_ASSIGN_OR_RAISE(auto result,
                          FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

    return std::unique_ptr<FlightInfo>(new FlightInfo(result));
  }

  arrow::Result<std::unique_ptr<FlightDataStream>> DoGetForTestCase(
      const std::shared_ptr<Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto reader, RecordBatchReader::Make({}, schema));
    return std::unique_ptr<FlightDataStream>(new RecordBatchStream(reader));
  }
};

/// \brief Integration test scenario for validating Flight SQL specs across multiple
/// implementations. This should ensure that RPC objects are being built and parsed
/// correctly for multiple languages and that the Arrow schemas are returned as expected.
class FlightSqlScenario : public Scenario {
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new FlightSqlScenarioServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override { return Status::OK(); }

  Status Validate(std::shared_ptr<Schema> expected_schema,
                  arrow::Result<std::unique_ptr<FlightInfo>> flight_info_result,
                  sql::FlightSqlClient* sql_client) {
    FlightCallOptions call_options;

    ARROW_ASSIGN_OR_RAISE(auto flight_info, flight_info_result);
    ARROW_ASSIGN_OR_RAISE(
        auto reader, sql_client->DoGet(call_options, flight_info->endpoints()[0].ticket));

    ARROW_ASSIGN_OR_RAISE(auto actual_schema, reader->GetSchema());

    AssertSchemaEqual(expected_schema, actual_schema);

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

    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetCatalogsSchema(),
                                 sql_client->GetCatalogs(options), sql_client));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetDbSchemasSchema(),
                 sql_client->GetDbSchemas(options, &catalog, &db_schema_filter_pattern),
                 sql_client));
    ARROW_RETURN_NOT_OK(
        Validate(sql::SqlSchema::GetTablesSchemaWithIncludedSchema(),
                 sql_client->GetTables(options, &catalog, &db_schema_filter_pattern,
                                       &table_filter_pattern, true, &table_types),
                 sql_client));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetTableTypesSchema(),
                                 sql_client->GetTableTypes(options), sql_client));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetPrimaryKeysSchema(),
                                 sql_client->GetPrimaryKeys(options, table_ref),
                                 sql_client));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetExportedKeysSchema(),
                                 sql_client->GetExportedKeys(options, table_ref),
                                 sql_client));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetImportedKeysSchema(),
                                 sql_client->GetImportedKeys(options, table_ref),
                                 sql_client));
    ARROW_RETURN_NOT_OK(Validate(
        sql::SqlSchema::GetCrossReferenceSchema(),
        sql_client->GetCrossReference(options, pk_table_ref, fk_table_ref), sql_client));
    ARROW_RETURN_NOT_OK(Validate(sql::SqlSchema::GetXdbcTypeInfoSchema(),
                                 sql_client->GetXdbcTypeInfo(options), sql_client));
    ARROW_RETURN_NOT_OK(Validate(
        sql::SqlSchema::GetSqlInfoSchema(),
        sql_client->GetSqlInfo(
            options, {sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME,
                      sql::SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY}),
        sql_client));

    return Status::OK();
  }

  Status ValidateStatementExecution(sql::FlightSqlClient* sql_client) {
    FlightCallOptions options;

    ARROW_RETURN_NOT_OK(Validate(
        GetQuerySchema(), sql_client->Execute(options, "SELECT STATEMENT"), sql_client));
    ARROW_ASSIGN_OR_RAISE(auto update_statement_result,
                          sql_client->ExecuteUpdate(options, "UPDATE STATEMENT"));
    if (update_statement_result != kUpdateStatementExpectedRows) {
      return Status::Invalid("Expected 'UPDATE STATEMENT' return ",
                             kUpdateStatementExpectedRows, ", got ",
                             update_statement_result);
    }

    return Status::OK();
  }

  Status ValidatePreparedStatementExecution(sql::FlightSqlClient* sql_client) {
    FlightCallOptions options;

    ARROW_ASSIGN_OR_RAISE(auto select_prepared_statement,
                          sql_client->Prepare(options, "SELECT PREPARED STATEMENT"));

    auto parameters =
        RecordBatch::Make(GetQuerySchema(), 1, {ArrayFromJSON(int64(), "[1]")});
    ARROW_RETURN_NOT_OK(select_prepared_statement->SetParameters(parameters));

    ARROW_RETURN_NOT_OK(
        Validate(GetQuerySchema(), select_prepared_statement->Execute(), sql_client));
    ARROW_RETURN_NOT_OK(select_prepared_statement->Close());

    ARROW_ASSIGN_OR_RAISE(auto update_prepared_statement,
                          sql_client->Prepare(options, "UPDATE PREPARED STATEMENT"));
    ARROW_ASSIGN_OR_RAISE(auto update_prepared_statement_result,
                          update_prepared_statement->ExecuteUpdate());
    if (update_prepared_statement_result != kUpdatePreparedStatementExpectedRows) {
      return Status::Invalid("Expected 'UPDATE STATEMENT' return ",
                             kUpdatePreparedStatementExpectedRows, ", got ",
                             update_prepared_statement_result);
    }
    ARROW_RETURN_NOT_OK(update_prepared_statement->Close());

    return Status::OK();
  }
};

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
  }
  return Status::KeyError("Scenario not found: ", scenario_name);
}

}  // namespace integration_tests
}  // namespace flight
}  // namespace arrow
