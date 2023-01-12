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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "arrow/flight/api.h"
#include "arrow/flight/client_tracing_middleware.h"
#include "arrow/flight/server_tracing_middleware.h"
#include "arrow/ipc/test_common.h"
#include "arrow/status.h"
#include "arrow/testing/generator.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/util.h"
#include "arrow/util/base64.h"
#include "arrow/util/logging.h"

#ifdef GRPCPP_GRPCPP_H
#error "gRPC headers should not be in public API"
#endif

#ifdef GRPCPP_PP_INCLUDE
#include <grpcpp/grpcpp.h>
#else
#include <grpc++/grpc++.h>
#endif

// Include before test_util.h (boost), contains Windows fixes
#include "arrow/flight/platform.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/test_definitions.h"
#include "arrow/flight/test_util.h"
// OTel includes must come after any gRPC includes, and
// client_header_internal.h includes gRPC. See:
// https://github.com/open-telemetry/opentelemetry-cpp/blob/main/examples/otlp/README.md
//
// > gRPC internally uses a different version of Abseil than
// > OpenTelemetry C++ SDK.
// > ...
// > ...in case if you run into conflict between Abseil library and
// > OpenTelemetry C++ absl::variant implementation, please include
// > either grpcpp/grpcpp.h or
// > opentelemetry/exporters/otlp/otlp_grpc_exporter.h BEFORE any
// > other API headers. This approach efficiently avoids the conflict
// > between the two different versions of Abseil.
#include "arrow/util/tracing_internal.h"
#ifdef ARROW_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#endif

namespace arrow {
namespace flight {

namespace pb = arrow::flight::protocol;

const char kValidUsername[] = "flight_username";
const char kValidPassword[] = "flight_password";
const char kInvalidUsername[] = "invalid_flight_username";
const char kInvalidPassword[] = "invalid_flight_password";
const char kBearerToken[] = "bearertoken";
const char kBasicPrefix[] = "Basic ";
const char kBearerPrefix[] = "Bearer ";
const char kAuthHeader[] = "authorization";

//------------------------------------------------------------
// Common transport tests

class GrpcConnectivityTest : public ConnectivityTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_CONNECTIVITY(GrpcConnectivityTest);

class GrpcDataTest : public DataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_DATA(GrpcDataTest);

class GrpcDoPutTest : public DoPutTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_DO_PUT(GrpcDoPutTest);

class GrpcAppMetadataTest : public AppMetadataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_APP_METADATA(GrpcAppMetadataTest);

class GrpcIpcOptionsTest : public IpcOptionsTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_IPC_OPTIONS(GrpcIpcOptionsTest);

class GrpcCudaDataTest : public CudaDataTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_CUDA_DATA(GrpcCudaDataTest);

class GrpcErrorHandlingTest : public ErrorHandlingTest, public ::testing::Test {
 protected:
  std::string transport() const override { return "grpc"; }
  void SetUp() override { SetUpTest(); }
  void TearDown() override { TearDownTest(); }
};
ARROW_FLIGHT_TEST_ERROR_HANDLING(GrpcErrorHandlingTest);

//------------------------------------------------------------
// Ad-hoc gRPC-specific tests

TEST(TestFlight, ConnectUri) {
  TestServer server("flight-test-server");
  server.Start();
  ASSERT_TRUE(server.IsRunning());

  std::stringstream ss;
  ss << "grpc://localhost:" << server.port();
  std::string uri = ss.str();

  std::unique_ptr<FlightClient> client;
  ASSERT_OK_AND_ASSIGN(auto location1, Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(auto location2, Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location1));
  ASSERT_OK(client->Close());
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location2));
  ASSERT_OK(client->Close());
}

TEST(TestFlight, InvalidUriScheme) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("invalid://localhost:1234"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(
      KeyError, ::testing::HasSubstr("No client transport implementation for invalid"),
      FlightClient::Connect(location));
}

#ifndef _WIN32
TEST(TestFlight, ConnectUriUnix) {
  TestServer server("flight-test-server", "/tmp/flight-test.sock");
  server.Start();
  ASSERT_TRUE(server.IsRunning());

  std::stringstream ss;
  ss << "grpc+unix://" << server.unix_sock();
  std::string uri = ss.str();

  std::unique_ptr<FlightClient> client;
  ASSERT_OK_AND_ASSIGN(auto location1, Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(auto location2, Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location1));
  ASSERT_OK(client->Close());
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location2));
  ASSERT_OK(client->Close());
}
#endif

// CI environments don't have an IPv6 interface configured
TEST(TestFlight, DISABLED_IpV6Port) {
  std::unique_ptr<FlightServerBase> server = ExampleTestServer();

  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("[::1]", 0));
  FlightServerOptions options(location);
  ASSERT_OK(server->Init(options));
  ASSERT_GT(server->port(), 0);

  ASSERT_OK_AND_ASSIGN(auto location2, Location::ForGrpcTcp("[::1]", server->port()));
  std::unique_ptr<FlightClient> client;
  ASSERT_OK_AND_ASSIGN(client, FlightClient::Connect(location2));
  ASSERT_OK(client->ListFlights());
}

// ----------------------------------------------------------------------
// Client tests

class TestFlightClient : public ::testing::Test {
 public:
  void SetUp() {
    server_ = ExampleTestServer();

    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 0));
    FlightServerOptions options(location);
    ASSERT_OK(server_->Init(options));

    ASSERT_OK(ConnectClient());
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

  Status ConnectClient() {
    ARROW_ASSIGN_OR_RAISE(auto location,
                          Location::ForGrpcTcp("localhost", server_->port()));
    return FlightClient::Connect(location).Value(&client_);
  }

  template <typename EndpointCheckFunc>
  void CheckDoGet(const FlightDescriptor& descr,
                  const RecordBatchVector& expected_batches,
                  EndpointCheckFunc&& check_endpoints) {
    auto expected_schema = expected_batches[0]->schema();

    ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descr));
    check_endpoints(info->endpoints());

    ipc::DictionaryMemo dict_memo;
    ASSERT_OK_AND_ASSIGN(auto schema, info->GetSchema(&dict_memo));
    AssertSchemaEqual(*expected_schema, *schema);

    // By convention, fetch the first endpoint
    Ticket ticket = info->endpoints()[0].ticket;
    CheckDoGet(ticket, expected_batches);
  }

  void CheckDoGet(const Ticket& ticket, const RecordBatchVector& expected_batches) {
    auto num_batches = static_cast<int>(expected_batches.size());
    ASSERT_GE(num_batches, 2);

    ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(ticket));
    ASSERT_OK_AND_ASSIGN(auto stream2, client_->DoGet(ticket));
    ASSERT_OK_AND_ASSIGN(auto reader, MakeRecordBatchReader(std::move(stream2)));

    FlightStreamChunk chunk;
    std::shared_ptr<RecordBatch> batch;
    for (int i = 0; i < num_batches; ++i) {
      ASSERT_OK_AND_ASSIGN(chunk, stream->Next());
      ASSERT_OK(reader->ReadNext(&batch));
      ASSERT_NE(nullptr, chunk.data);
      ASSERT_NE(nullptr, batch);
#if !defined(__MINGW32__)
      ASSERT_BATCHES_EQUAL(*expected_batches[i], *chunk.data);
      ASSERT_BATCHES_EQUAL(*expected_batches[i], *batch);
#else
      // In MINGW32, the following code does not have the reproducibility at the LSB
      // even when this is called twice with the same seed.
      // As a workaround, use approxEqual
      //   /* from GenerateTypedData in random.cc */
      //   std::default_random_engine rng(seed);  // seed = 282475250
      //   std::uniform_real_distribution<double> dist;
      //   std::generate(data, data + n,          // n = 10
      //                 [&dist, &rng] { return static_cast<ValueType>(dist(rng)); });
      //   /* data[1] = 0x40852cdfe23d3976 or 0x40852cdfe23d3975 */
      ASSERT_BATCHES_APPROX_EQUAL(*expected_batches[i], *chunk.data);
      ASSERT_BATCHES_APPROX_EQUAL(*expected_batches[i], *batch);
#endif
    }

    // Stream exhausted
    ASSERT_OK_AND_ASSIGN(chunk, stream->Next());
    ASSERT_OK(reader->ReadNext(&batch));
    ASSERT_EQ(nullptr, chunk.data);
    ASSERT_EQ(nullptr, batch);
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class AuthTestServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    auto buf = Buffer::FromString(context.peer_identity());
    auto peer = Buffer::FromString(context.peer());
    *result = std::make_unique<SimpleResultStream>(
        std::vector<Result>{Result{buf}, Result{peer}});
    return Status::OK();
  }
};

class TlsTestServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    auto buf = Buffer::FromString("Hello, world!");
    *result = std::make_unique<SimpleResultStream>(std::vector<Result>{Result{buf}});
    return Status::OK();
  }
};

class HeaderAuthTestServer : public FlightServerBase {
 public:
  Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                     std::unique_ptr<FlightListing>* listings) override {
    return Status::OK();
  }
};

class TestAuthHandler : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(MakeServer<AuthTestServer>(
        &server_, &client_,
        [](FlightServerOptions* options) {
          options->auth_handler =
              std::make_unique<TestServerAuthHandler>("user", "p4ssw0rd");
          return Status::OK();
        },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class TestBasicAuthHandler : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(MakeServer<AuthTestServer>(
        &server_, &client_,
        [](FlightServerOptions* options) {
          options->auth_handler =
              std::make_unique<TestServerBasicAuthHandler>("user", "p4ssw0rd");
          return Status::OK();
        },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class TestTls : public ::testing::Test {
 public:
  void SetUp() {
    // Manually initialize gRPC to try to ensure some thread-locals
    // get initialized.
    // https://github.com/grpc/grpc/issues/13856
    // https://github.com/grpc/grpc/issues/20311
    // In general, gRPC on MacOS struggles with TLS (both in the sense
    // of thread-locals and encryption)
    grpc_init();

    server_.reset(new TlsTestServer);

    ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTls("localhost", 0));
    FlightServerOptions options(location);
    ASSERT_RAISES(UnknownError, server_->Init(options));
    ASSERT_OK(ExampleTlsCertificates(&options.tls_certificates));
    ASSERT_OK(server_->Init(options));

    ASSERT_OK_AND_ASSIGN(location_, Location::ForGrpcTls("localhost", server_->port()));
    ASSERT_OK(ConnectClient());
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
    grpc_shutdown();
  }

  Status ConnectClient() {
    auto options = FlightClientOptions::Defaults();
    CertKeyPair root_cert;
    RETURN_NOT_OK(ExampleTlsCertificateRoot(&root_cert));
    options.tls_root_certs = root_cert.pem_cert;
    return FlightClient::Connect(location_, options).Value(&client_);
  }

 protected:
  Location location_;
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

// A server middleware that rejects all calls.
class RejectServerMiddlewareFactory : public ServerMiddlewareFactory {
  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    return MakeFlightError(FlightStatusCode::Unauthenticated, "All calls are rejected");
  }
};

// A server middleware that counts the number of successful and failed
// calls.
class CountingServerMiddleware : public ServerMiddleware {
 public:
  CountingServerMiddleware(std::atomic<int>* successful, std::atomic<int>* failed)
      : successful_(successful), failed_(failed) {}
  void SendingHeaders(AddCallHeaders* outgoing_headers) override {}
  void CallCompleted(const Status& status) override {
    if (status.ok()) {
      ARROW_IGNORE_EXPR((*successful_)++);
    } else {
      ARROW_IGNORE_EXPR((*failed_)++);
    }
  }

  std::string name() const override { return "CountingServerMiddleware"; }

 private:
  std::atomic<int>* successful_;
  std::atomic<int>* failed_;
};

class CountingServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  CountingServerMiddlewareFactory() : successful_(0), failed_(0) {}

  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    *middleware = std::make_shared<CountingServerMiddleware>(&successful_, &failed_);
    return Status::OK();
  }

  std::atomic<int> successful_;
  std::atomic<int> failed_;
};

// The current span ID, used to emulate OpenTracing style distributed
// tracing. Only used for communication between application code and
// client middleware.
static thread_local std::string current_span_id = "";

// A server middleware that stores the current span ID, in an
// emulation of OpenTracing style distributed tracing.
class TracingTestServerMiddleware : public ServerMiddleware {
 public:
  explicit TracingTestServerMiddleware(const std::string& current_span_id)
      : span_id(current_span_id) {}
  void SendingHeaders(AddCallHeaders* outgoing_headers) override {}
  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "TracingTestServerMiddleware"; }

  std::string span_id;
};

class TracingTestServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  TracingTestServerMiddlewareFactory() {}

  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range("x-tracing-span-id");
    if (iter_pair.first != iter_pair.second) {
      const std::string_view& value = (*iter_pair.first).second;
      *middleware = std::make_shared<TracingTestServerMiddleware>(std::string(value));
    }
    return Status::OK();
  }
};

// Function to look in CallHeaders for a key that has a value starting with prefix and
// return the rest of the value after the prefix.
std::string FindKeyValPrefixInCallHeaders(const CallHeaders& incoming_headers,
                                          const std::string& key,
                                          const std::string& prefix) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char& char1, const char& char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  auto iter = incoming_headers.find(key);
  if (iter == incoming_headers.end()) {
    return "";
  }
  const std::string val(iter->second);
  if (val.size() > prefix.length()) {
    if (std::equal(val.begin(), val.begin() + prefix.length(), prefix.begin(),
                   char_compare)) {
      return val.substr(prefix.length());
    }
  }
  return "";
}

class HeaderAuthServerMiddleware : public ServerMiddleware {
 public:
  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + kBearerToken);
  }

  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "HeaderAuthServerMiddleware"; }
};

void ParseBasicHeader(const CallHeaders& incoming_headers, std::string& username,
                      std::string& password) {
  std::string encoded_credentials =
      FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
  std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
  std::getline(decoded_stream, username, ':');
  std::getline(decoded_stream, password, ':');
}

// Factory for base64 header authentication testing.
class HeaderAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  HeaderAuthServerMiddlewareFactory() {}

  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    std::string username, password;
    ParseBasicHeader(incoming_headers, username, password);
    if ((username == kValidUsername) && (password == kValidPassword)) {
      *middleware = std::make_shared<HeaderAuthServerMiddleware>();
    } else if ((username == kInvalidUsername) && (password == kInvalidPassword)) {
      return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid credentials");
    }
    return Status::OK();
  }
};

// A server middleware for validating incoming bearer header authentication.
class BearerAuthServerMiddleware : public ServerMiddleware {
 public:
  explicit BearerAuthServerMiddleware(const CallHeaders& incoming_headers, bool* isValid)
      : isValid_(isValid) {
    incoming_headers_ = incoming_headers;
  }

  void SendingHeaders(AddCallHeaders* outgoing_headers) override {
    std::string bearer_token =
        FindKeyValPrefixInCallHeaders(incoming_headers_, kAuthHeader, kBearerPrefix);
    *isValid_ = (bearer_token == std::string(kBearerToken));
  }

  void CallCompleted(const Status& status) override {}

  std::string name() const override { return "BearerAuthServerMiddleware"; }

 private:
  CallHeaders incoming_headers_;
  bool* isValid_;
};

// Factory for base64 header authentication testing.
class BearerAuthServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  BearerAuthServerMiddlewareFactory() : isValid_(false) {}

  Status StartCall(const CallInfo& info, const CallHeaders& incoming_headers,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    const std::pair<CallHeaders::const_iterator, CallHeaders::const_iterator>& iter_pair =
        incoming_headers.equal_range(kAuthHeader);
    if (iter_pair.first != iter_pair.second) {
      *middleware =
          std::make_shared<BearerAuthServerMiddleware>(incoming_headers, &isValid_);
    }
    return Status::OK();
  }

  bool GetIsValid() { return isValid_; }

 private:
  bool isValid_;
};

// A client middleware that adds a thread-local "request ID" to
// outgoing calls as a header, and keeps track of the status of
// completed calls. NOT thread-safe.
class PropagatingClientMiddleware : public ClientMiddleware {
 public:
  explicit PropagatingClientMiddleware(std::atomic<int>* received_headers,
                                       std::vector<Status>* recorded_status)
      : received_headers_(received_headers), recorded_status_(recorded_status) {}

  void SendingHeaders(AddCallHeaders* outgoing_headers) {
    // Pick up the span ID from thread locals. We have to use a
    // thread-local for communication, since we aren't even
    // instantiated until after the application code has already
    // started the call (and so there's no chance for application code
    // to pass us parameters directly).
    outgoing_headers->AddHeader("x-tracing-span-id", current_span_id);
  }

  void ReceivedHeaders(const CallHeaders& incoming_headers) { (*received_headers_)++; }

  void CallCompleted(const Status& status) { recorded_status_->push_back(status); }

 private:
  std::atomic<int>* received_headers_;
  std::vector<Status>* recorded_status_;
};

class PropagatingClientMiddlewareFactory : public ClientMiddlewareFactory {
 public:
  void StartCall(const CallInfo& info, std::unique_ptr<ClientMiddleware>* middleware) {
    recorded_calls_.push_back(info.method);
    *middleware = std::make_unique<PropagatingClientMiddleware>(&received_headers_,
                                                                &recorded_status_);
  }

  void Reset() {
    recorded_calls_.clear();
    recorded_status_.clear();
    received_headers_.fetch_and(0);
  }

  std::vector<FlightMethod> recorded_calls_;
  std::vector<Status> recorded_status_;
  std::atomic<int> received_headers_;
};

class ReportContextTestServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    std::shared_ptr<Buffer> buf;
    const ServerMiddleware* middleware = context.GetMiddleware("tracing");
    if (middleware == nullptr || middleware->name() != "TracingTestServerMiddleware") {
      buf = Buffer::FromString("");
    } else {
      buf = Buffer::FromString(((const TracingTestServerMiddleware*)middleware)->span_id);
    }
    *result = std::make_unique<SimpleResultStream>(std::vector<Result>{Result{buf}});
    return Status::OK();
  }
};

class ErrorMiddlewareServer : public FlightServerBase {
  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    std::string msg = "error_message";
    auto buf = Buffer::FromString("");

    std::shared_ptr<FlightStatusDetail> flightStatusDetail(
        new FlightStatusDetail(FlightStatusCode::Failed, msg));
    *result = std::make_unique<SimpleResultStream>(std::vector<Result>{Result{buf}});
    return Status(StatusCode::ExecutionError, "test failed", flightStatusDetail);
  }
};

class PropagatingTestServer : public FlightServerBase {
 public:
  explicit PropagatingTestServer(std::unique_ptr<FlightClient> client)
      : client_(std::move(client)) {}

  Status DoAction(const ServerCallContext& context, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    const ServerMiddleware* middleware = context.GetMiddleware("tracing");
    if (middleware == nullptr || middleware->name() != "TracingTestServerMiddleware") {
      current_span_id = "";
    } else {
      current_span_id = ((const TracingTestServerMiddleware*)middleware)->span_id;
    }

    return client_->DoAction(action).Value(result);
  }

 private:
  std::unique_ptr<FlightClient> client_;
};

class TestRejectServerMiddleware : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(MakeServer<AppMetadataTestServer>(
        &server_, &client_,
        [](FlightServerOptions* options) {
          options->middleware.push_back(
              {"reject", std::make_shared<RejectServerMiddlewareFactory>()});
          return Status::OK();
        },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class TestCountingServerMiddleware : public ::testing::Test {
 public:
  void SetUp() {
    request_counter_ = std::make_shared<CountingServerMiddlewareFactory>();
    ASSERT_OK(MakeServer<AppMetadataTestServer>(
        &server_, &client_,
        [&](FlightServerOptions* options) {
          options->middleware.push_back({"request_counter", request_counter_});
          return Status::OK();
        },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::shared_ptr<CountingServerMiddlewareFactory> request_counter_;
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

// Setup for this test is 2 servers
// 1. Client makes request to server A with a request ID set
// 2. server A extracts the request ID and makes a request to server B
//    with the same request ID set
// 3. server B extracts the request ID and sends it back
// 4. server A returns the response of server B
// 5. Client validates the response
class TestPropagatingMiddleware : public ::testing::Test {
 public:
  void SetUp() {
    server_middleware_ = std::make_shared<TracingTestServerMiddlewareFactory>();
    second_client_middleware_ = std::make_shared<PropagatingClientMiddlewareFactory>();
    client_middleware_ = std::make_shared<PropagatingClientMiddlewareFactory>();

    std::unique_ptr<FlightClient> server_client;
    ASSERT_OK(MakeServer<ReportContextTestServer>(
        &second_server_, &server_client,
        [&](FlightServerOptions* options) {
          options->middleware.push_back({"tracing", server_middleware_});
          return Status::OK();
        },
        [&](FlightClientOptions* options) {
          options->middleware.push_back(second_client_middleware_);
          return Status::OK();
        }));

    ASSERT_OK(MakeServer<PropagatingTestServer>(
        &first_server_, &client_,
        [&](FlightServerOptions* options) {
          options->middleware.push_back({"tracing", server_middleware_});
          return Status::OK();
        },
        [&](FlightClientOptions* options) {
          options->middleware.push_back(client_middleware_);
          return Status::OK();
        },
        std::move(server_client)));
  }

  void ValidateStatus(const Status& status, const FlightMethod& method) {
    ASSERT_EQ(1, client_middleware_->received_headers_);
    ASSERT_EQ(method, client_middleware_->recorded_calls_.at(0));
    ASSERT_EQ(status.code(), client_middleware_->recorded_status_.at(0).code());
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(first_server_->Shutdown());
    ASSERT_OK(second_server_->Shutdown());
  }

  void CheckHeader(const std::string& header, const std::string& value,
                   const CallHeaders::const_iterator& it) {
    // Construct a string_view before comparison to satisfy MSVC
    std::string_view header_view(header.data(), header.length());
    std::string_view value_view(value.data(), value.length());
    ASSERT_EQ(header_view, (*it).first);
    ASSERT_EQ(value_view, (*it).second);
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> first_server_;
  std::unique_ptr<FlightServerBase> second_server_;
  std::shared_ptr<TracingTestServerMiddlewareFactory> server_middleware_;
  std::shared_ptr<PropagatingClientMiddlewareFactory> second_client_middleware_;
  std::shared_ptr<PropagatingClientMiddlewareFactory> client_middleware_;
};

class TestErrorMiddleware : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(MakeServer<ErrorMiddlewareServer>(
        &server_, &client_, [](FlightServerOptions* options) { return Status::OK(); },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

class TestBasicHeaderAuthMiddleware : public ::testing::Test {
 public:
  void SetUp() {
    header_middleware_ = std::make_shared<HeaderAuthServerMiddlewareFactory>();
    bearer_middleware_ = std::make_shared<BearerAuthServerMiddlewareFactory>();
    std::pair<std::string, std::string> bearer = make_pair(
        kAuthHeader, std::string(kBearerPrefix) + " " + std::string(kBearerToken));
    ASSERT_OK(MakeServer<HeaderAuthTestServer>(
        &server_, &client_,
        [&](FlightServerOptions* options) {
          options->auth_handler = std::make_unique<NoOpAuthHandler>();
          options->middleware.push_back({"header-auth-server", header_middleware_});
          options->middleware.push_back({"bearer-auth-server", bearer_middleware_});
          return Status::OK();
        },
        [&](FlightClientOptions* options) { return Status::OK(); }));
  }

  void RunValidClientAuth() {
    arrow::Result<std::pair<std::string, std::string>> bearer_result =
        client_->AuthenticateBasicToken({}, kValidUsername, kValidPassword);
    ASSERT_OK(bearer_result.status());
    ASSERT_EQ(bearer_result.ValueOrDie().first, kAuthHeader);
    ASSERT_EQ(bearer_result.ValueOrDie().second,
              (std::string(kBearerPrefix) + kBearerToken));
    std::unique_ptr<FlightListing> listing;
    FlightCallOptions call_options;
    call_options.headers.push_back(bearer_result.ValueOrDie());
    ASSERT_OK_AND_ASSIGN(listing, client_->ListFlights(call_options, {}));
    ASSERT_TRUE(bearer_middleware_->GetIsValid());
  }

  void RunInvalidClientAuth() {
    arrow::Result<std::pair<std::string, std::string>> bearer_result =
        client_->AuthenticateBasicToken({}, kInvalidUsername, kInvalidPassword);
    ASSERT_RAISES(IOError, bearer_result.status());
    ASSERT_THAT(bearer_result.status().message(),
                ::testing::HasSubstr("Invalid credentials"));
  }

  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
  std::shared_ptr<HeaderAuthServerMiddlewareFactory> header_middleware_;
  std::shared_ptr<BearerAuthServerMiddlewareFactory> bearer_middleware_;
};

TEST_F(TestErrorMiddleware, TestMetadata) {
  Action action;

  // Run action1
  action.type = "action1";

  action.body = Buffer::FromString("action1-content");
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(action));
  Status s = stream->Next().status();
  ASSERT_FALSE(s.ok());
  std::shared_ptr<FlightStatusDetail> flightStatusDetail =
      FlightStatusDetail::UnwrapStatus(s);
  ASSERT_TRUE(flightStatusDetail);
  ASSERT_EQ(flightStatusDetail->extra_info(), "error_message");
}

TEST_F(TestFlightClient, ListFlights) {
  ASSERT_OK_AND_ASSIGN(auto listing, client_->ListFlights());
  ASSERT_TRUE(listing != nullptr);

  std::vector<FlightInfo> flights = ExampleFlightInfo();

  std::unique_ptr<FlightInfo> info;
  for (const FlightInfo& flight : flights) {
    ASSERT_OK_AND_ASSIGN(info, listing->Next());
    AssertEqual(flight, *info);
  }
  ASSERT_OK_AND_ASSIGN(info, listing->Next());
  ASSERT_TRUE(info == nullptr);

  ASSERT_OK_AND_ASSIGN(info, listing->Next());
  ASSERT_TRUE(info == nullptr);
}

TEST_F(TestFlightClient, ListFlightsWithCriteria) {
  ASSERT_OK_AND_ASSIGN(auto listing, client_->ListFlights(FlightCallOptions(), {"foo"}));
  std::unique_ptr<FlightInfo> info;
  ASSERT_OK_AND_ASSIGN(info, listing->Next());
  ASSERT_TRUE(info == nullptr);
}

TEST_F(TestFlightClient, GetFlightInfo) {
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  ASSERT_OK_AND_ASSIGN(auto info, client_->GetFlightInfo(descr));
  ASSERT_NE(info, nullptr);

  std::vector<FlightInfo> flights = ExampleFlightInfo();
  AssertEqual(flights[0], *info);
}

TEST_F(TestFlightClient, GetSchema) {
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  ipc::DictionaryMemo dict_memo;

  ASSERT_OK_AND_ASSIGN(auto schema_result, client_->GetSchema(descr));
  ASSERT_NE(schema_result, nullptr);
  ASSERT_OK(schema_result->GetSchema(&dict_memo));
}

TEST_F(TestFlightClient, GetFlightInfoNotFound) {
  auto descr = FlightDescriptor::Path({"examples", "things"});
  // XXX Ideally should be Invalid (or KeyError), but gRPC doesn't support
  // multiple error codes.
  auto st = client_->GetFlightInfo(descr).status();
  ASSERT_RAISES(Invalid, st);
  ASSERT_NE(st.message().find("Flight not found"), std::string::npos);
}

TEST_F(TestFlightClient, ListActions) {
  ASSERT_OK_AND_ASSIGN(std::vector<ActionType> actions, client_->ListActions());

  std::vector<ActionType> expected = ExampleActionTypes();
  EXPECT_THAT(actions, ::testing::ContainerEq(expected));
}

TEST_F(TestFlightClient, DoAction) {
  Action action;
  std::unique_ptr<Result> result;

  // Run action1
  action.type = "action1";

  const std::string action1_value = "action1-content";
  action.body = Buffer::FromString(action1_value);
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(action));

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK_AND_ASSIGN(result, stream->Next());
    std::string expected = action1_value + "-part" + std::to_string(i);
    ASSERT_EQ(expected, result->body->ToString());
  }

  // stream consumed
  ASSERT_OK_AND_ASSIGN(result, stream->Next());
  ASSERT_EQ(nullptr, result);

  // Run action2, no results
  action.type = "action2";
  ASSERT_OK_AND_ASSIGN(stream, client_->DoAction(action));

  ASSERT_OK_AND_ASSIGN(result, stream->Next());
  ASSERT_EQ(nullptr, result);
}

TEST_F(TestFlightClient, RoundTripStatus) {
  const auto descr = FlightDescriptor::Command("status-outofmemory");
  const auto status = client_->GetFlightInfo(descr).status();
  ASSERT_RAISES(OutOfMemory, status);
}

// Test setting generic transport options by configuring gRPC to fail
// all calls.
TEST_F(TestFlightClient, GenericOptions) {
  auto options = FlightClientOptions::Defaults();
  // Set a very low limit at the gRPC layer to fail all calls
  options.generic_options.emplace_back(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 4);
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", server_->port()));
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location, options));
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  std::shared_ptr<Schema> schema;
  ipc::DictionaryMemo dict_memo;
  auto status = client->GetSchema(descr).status();
  ASSERT_RAISES(Invalid, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("resource exhausted"));
}

TEST_F(TestFlightClient, TimeoutFires) {
  // Server does not exist on this port, so call should fail
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("localhost", 30001));
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location));
  FlightCallOptions options;
  options.timeout = TimeoutDuration{0.2};
  auto start = std::chrono::system_clock::now();
  Status status = client->GetFlightInfo(options, FlightDescriptor{}).status();
  auto end = std::chrono::system_clock::now();
#ifdef ARROW_WITH_TIMING_TESTS
  EXPECT_LE(end - start, std::chrono::milliseconds{400});
#else
  ARROW_UNUSED(end - start);
#endif
  ASSERT_RAISES(IOError, status);
}

TEST_F(TestFlightClient, NoTimeout) {
  // Call should complete quickly, so timeout should not fire
  FlightCallOptions options;
  options.timeout = TimeoutDuration{5.0};  // account for slow server process startup
  std::unique_ptr<FlightInfo> info;
  auto start = std::chrono::system_clock::now();
  auto descriptor = FlightDescriptor::Path({"examples", "ints"});
  Status status = client_->GetFlightInfo(options, descriptor).Value(&info);
  auto end = std::chrono::system_clock::now();
#ifdef ARROW_WITH_TIMING_TESTS
  EXPECT_LE(end - start, std::chrono::milliseconds{600});
#else
  ARROW_UNUSED(end - start);
#endif
  ASSERT_OK(status);
  ASSERT_NE(nullptr, info);
}

TEST_F(TestFlightClient, Close) {
  // For gRPC, this is always effectively a no-op
  ASSERT_OK(client_->Close());
  // Idempotent
  ASSERT_OK(client_->Close());

  EXPECT_RAISES_WITH_MESSAGE_THAT(Invalid, ::testing::HasSubstr("FlightClient is closed"),
                                  client_->ListFlights());
}

TEST_F(TestAuthHandler, PassAuthenticatedCalls) {
  ASSERT_OK(client_->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));

  Status status;
  status = client_->ListFlights().status();
  ASSERT_RAISES(NotImplemented, status);

  std::unique_ptr<ResultStream> results;
  Action action;
  action.type = "";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(results, client_->DoAction(action));

  status = client_->ListActions().status();
  ASSERT_RAISES(NotImplemented, status);

  status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(NotImplemented, status);

  status = client_->DoGet(Ticket{}).status();
  ASSERT_RAISES(NotImplemented, status);

  std::shared_ptr<Schema> schema = arrow::schema({});
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(FlightDescriptor{}, schema));
  status = do_put_result.writer->Close();
  ASSERT_RAISES(NotImplemented, status);
}

TEST_F(TestAuthHandler, FailUnauthenticatedCalls) {
  Status status;
  status = client_->ListFlights().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  Action action;
  action.type = "";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(action));
  status = stream->Next().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->ListActions().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->DoGet(Ticket{}).status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  std::unique_ptr<FlightStreamWriter> writer;
  std::unique_ptr<FlightMetadataReader> reader;
  std::shared_ptr<Schema> schema(
      (new arrow::Schema(std::vector<std::shared_ptr<Field>>())));
  FlightClient::DoPutResult do_put_result;
  status = client_->DoPut(FlightDescriptor{}, schema).Value(&do_put_result);
  // ARROW-16053: gRPC may or may not fail the call immediately
  if (status.ok()) status = do_put_result.writer->Close();
  ASSERT_RAISES(IOError, status);
  // ARROW-7583: don't check the error message here.
  // Because gRPC reports errors in some paths with booleans, instead
  // of statuses, we can fail the call without knowing why it fails,
  // instead reporting a generic error message. This is
  // nondeterministic, so don't assert any particular message here.
}

TEST_F(TestAuthHandler, CheckPeerIdentity) {
  ASSERT_OK(client_->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));

  Action action;
  action.type = "who-am-i";
  action.body = Buffer::FromString("");
  std::unique_ptr<ResultStream> results;
  ASSERT_OK_AND_ASSIGN(results, client_->DoAction(action));
  ASSERT_NE(results, nullptr);

  std::unique_ptr<Result> result;
  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  // Action returns the peer identity as the result.
  ASSERT_EQ(result->body->ToString(), "user");

  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  // Action returns the peer address as the result.
#ifndef _WIN32
  // On Windows gRPC sometimes returns a blank peer address, so don't
  // bother checking for it.
  ASSERT_NE(result->body->ToString(), "");
#endif
}

TEST_F(TestBasicAuthHandler, PassAuthenticatedCalls) {
  ASSERT_OK(client_->Authenticate(
      {}, std::make_unique<TestClientBasicAuthHandler>("user", "p4ssw0rd")));

  Status status;
  status = client_->ListFlights().status();
  ASSERT_RAISES(NotImplemented, status);

  Action action;
  action.type = "";
  action.body = Buffer::FromString("");
  status = client_->DoAction(action).status();
  ASSERT_OK(status);

  status = client_->ListActions().status();
  ASSERT_RAISES(NotImplemented, status);

  status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(NotImplemented, status);

  status = client_->DoGet(Ticket{}).status();
  ASSERT_RAISES(NotImplemented, status);

  std::shared_ptr<Schema> schema = arrow::schema({});
  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(FlightDescriptor{}, schema));
  status = do_put_result.writer->Close();
  ASSERT_RAISES(NotImplemented, status);
}

TEST_F(TestBasicAuthHandler, FailUnauthenticatedCalls) {
  Status status;
  status = client_->ListFlights().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  Action action;
  action.type = "";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(action));
  status = stream->Next().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->ListActions().status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  status = client_->DoGet(Ticket{}).status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));

  std::shared_ptr<Schema> schema(
      (new arrow::Schema(std::vector<std::shared_ptr<Field>>())));
  FlightClient::DoPutResult do_put_result;
  // May or may not succeed depending on if the transport buffers the write
  status = client_->DoPut(FlightDescriptor{}, schema).Value(&do_put_result);
  if (do_put_result.writer) {
    status = do_put_result.writer->Close();
  }
  // But this should definitely fail
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Invalid token"));
}

TEST_F(TestBasicAuthHandler, CheckPeerIdentity) {
  ASSERT_OK(client_->Authenticate(
      {}, std::make_unique<TestClientBasicAuthHandler>("user", "p4ssw0rd")));

  Action action;
  action.type = "who-am-i";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto results, client_->DoAction(action));
  ASSERT_NE(results, nullptr);

  std::unique_ptr<Result> result;
  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  // Action returns the peer identity as the result.
  ASSERT_EQ(result->body->ToString(), "user");
}

TEST_F(TestTls, DoAction) {
  FlightCallOptions options;
  options.timeout = TimeoutDuration{5.0};
  Action action;
  action.type = "test";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto results, client_->DoAction(options, action));
  ASSERT_NE(results, nullptr);

  std::unique_ptr<Result> result;
  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->body->ToString(), "Hello, world!");
}

#if defined(GRPC_NAMESPACE_FOR_TLS_CREDENTIALS_OPTIONS)
TEST_F(TestTls, DisableServerVerification) {
  auto client_options = FlightClientOptions::Defaults();
  // For security reasons, if encryption is being used,
  // the client should be configured to verify the server by default.
  ASSERT_EQ(client_options.disable_server_verification, false);
  client_options.disable_server_verification = true;
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location_, client_options));

  FlightCallOptions options;
  options.timeout = TimeoutDuration{5.0};
  Action action;
  action.type = "test";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(options, action));
  ASSERT_NE(results, nullptr);

  std::unique_ptr<Result> result;
  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->body->ToString(), "Hello, world!");
}
#endif

TEST_F(TestTls, OverrideHostname) {
  auto client_options = FlightClientOptions::Defaults();
  client_options.override_hostname = "fakehostname";
  CertKeyPair root_cert;
  ASSERT_OK(ExampleTlsCertificateRoot(&root_cert));
  client_options.tls_root_certs = root_cert.pem_cert;
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location_, client_options));

  FlightCallOptions options;
  options.timeout = TimeoutDuration{5.0};
  Action action;
  action.type = "test";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto stream, client->DoAction(options, action));
  ASSERT_RAISES(IOError, stream->Next());
}

// Test the facility for setting generic transport options.
TEST_F(TestTls, OverrideHostnameGeneric) {
  auto client_options = FlightClientOptions::Defaults();
  client_options.generic_options.emplace_back(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG,
                                              "fakehostname");
  CertKeyPair root_cert;
  ASSERT_OK(ExampleTlsCertificateRoot(&root_cert));
  client_options.tls_root_certs = root_cert.pem_cert;
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location_, client_options));

  FlightCallOptions options;
  options.timeout = TimeoutDuration{5.0};
  Action action;
  action.type = "test";
  action.body = Buffer::FromString("");
  ASSERT_OK_AND_ASSIGN(auto stream, client->DoAction(options, action));
  ASSERT_RAISES(IOError, stream->Next());
  // Could check error message for the gRPC error message but it isn't
  // necessarily stable
}

TEST_F(TestRejectServerMiddleware, Rejected) {
  const Status status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(IOError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("All calls are rejected"));
}

TEST_F(TestCountingServerMiddleware, Count) {
  const Status status = client_->GetFlightInfo(FlightDescriptor{}).status();
  ASSERT_RAISES(NotImplemented, status);

  Ticket ticket{""};
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(ticket));

  ASSERT_EQ(1, request_counter_->failed_);

  while (true) {
    ASSERT_OK_AND_ASSIGN(FlightStreamChunk chunk, stream->Next());
    if (chunk.data == nullptr) {
      break;
    }
  }

  ASSERT_EQ(1, request_counter_->successful_);
  ASSERT_EQ(1, request_counter_->failed_);
}

TEST_F(TestPropagatingMiddleware, Propagate) {
  Action action;
  std::unique_ptr<Result> result;

  current_span_id = "trace-id";
  client_middleware_->Reset();

  action.type = "action1";
  action.body = Buffer::FromString("action1-content");
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(action));

  ASSERT_OK_AND_ASSIGN(result, stream->Next());
  ASSERT_EQ("trace-id", result->body->ToString());
  ASSERT_OK_AND_ASSIGN(result, stream->Next());
  ASSERT_EQ(nullptr, result);
  ValidateStatus(Status::OK(), FlightMethod::DoAction);
}

// For each method, make sure that the client middleware received
// headers from the server and that the proper method enum value was
// passed to the interceptor
TEST_F(TestPropagatingMiddleware, ListFlights) {
  client_middleware_->Reset();
  const Status status = client_->ListFlights().status();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::ListFlights);
}

TEST_F(TestPropagatingMiddleware, GetFlightInfo) {
  client_middleware_->Reset();
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  const Status status = client_->GetFlightInfo(descr).status();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::GetFlightInfo);
}

TEST_F(TestPropagatingMiddleware, GetSchema) {
  client_middleware_->Reset();
  auto descr = FlightDescriptor::Path({"examples", "ints"});
  const Status status = client_->GetSchema(descr).status();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::GetSchema);
}

TEST_F(TestPropagatingMiddleware, ListActions) {
  client_middleware_->Reset();
  std::vector<ActionType> actions;
  const Status status = client_->ListActions().status();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::ListActions);
}

TEST_F(TestPropagatingMiddleware, DoGet) {
  client_middleware_->Reset();
  Ticket ticket1{"ARROW-5095-fail"};
  std::unique_ptr<FlightStreamReader> stream;
  Status status = client_->DoGet(ticket1).status();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::DoGet);
}

TEST_F(TestPropagatingMiddleware, DoPut) {
  client_middleware_->Reset();
  auto descr = FlightDescriptor::Path({"ints"});
  auto a1 = ArrayFromJSON(int32(), "[4, 5, 6, null]");
  auto schema = arrow::schema({field("f1", a1->type())});

  ASSERT_OK_AND_ASSIGN(auto do_put_result, client_->DoPut(descr, schema));
  const Status status = do_put_result.writer->Close();
  ASSERT_RAISES(NotImplemented, status);
  ValidateStatus(status, FlightMethod::DoPut);
}

TEST_F(TestBasicHeaderAuthMiddleware, ValidCredentials) { RunValidClientAuth(); }

TEST_F(TestBasicHeaderAuthMiddleware, InvalidCredentials) { RunInvalidClientAuth(); }

class ForeverFlightListing : public FlightListing {
  arrow::Result<std::unique_ptr<FlightInfo>> Next() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return std::make_unique<FlightInfo>(ExampleFlightInfo()[0]);
  }
};

class ForeverResultStream : public ResultStream {
  arrow::Result<std::unique_ptr<Result>> Next() override {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    auto result = std::make_unique<Result>();
    result->body = Buffer::FromString("foo");
    return result;
  }
};

class ForeverDataStream : public FlightDataStream {
 public:
  ForeverDataStream() : schema_(arrow::schema({})), mapper_(*schema_) {}
  std::shared_ptr<Schema> schema() override { return schema_; }

  arrow::Result<FlightPayload> GetSchemaPayload() override {
    FlightPayload payload;
    RETURN_NOT_OK(ipc::GetSchemaPayload(*schema_, ipc::IpcWriteOptions::Defaults(),
                                        mapper_, &payload.ipc_message));
    return payload;
  }

  arrow::Result<FlightPayload> Next() override {
    auto batch = RecordBatch::Make(schema_, 0, ArrayVector{});
    FlightPayload payload;
    RETURN_NOT_OK(ipc::GetRecordBatchPayload(*batch, ipc::IpcWriteOptions::Defaults(),
                                             &payload.ipc_message));
    return payload;
  }

 private:
  std::shared_ptr<Schema> schema_;
  ipc::DictionaryFieldMapper mapper_;
};

class CancelTestServer : public FlightServerBase {
 public:
  Status ListFlights(const ServerCallContext&, const Criteria*,
                     std::unique_ptr<FlightListing>* listings) override {
    *listings = std::make_unique<ForeverFlightListing>();
    return Status::OK();
  }
  Status DoAction(const ServerCallContext&, const Action& action,
                  std::unique_ptr<ResultStream>* result) override {
    if (action.type == "inc") {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      counter_++;
    }
    *result = std::make_unique<ForeverResultStream>();
    return Status::OK();
  }
  Status ListActions(const ServerCallContext&,
                     std::vector<ActionType>* actions) override {
    *actions = {};
    return Status::OK();
  }
  Status DoGet(const ServerCallContext&, const Ticket&,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    *data_stream = std::make_unique<ForeverDataStream>();
    return Status::OK();
  }

  int64_t CheckCounter() const { return counter_; }

 private:
  std::atomic<int64_t> counter_ = 0;
};

class TestCancel : public ::testing::Test {
 public:
  void SetUp() {
    ASSERT_OK(MakeServer<CancelTestServer>(
        &server_, &client_, [](FlightServerOptions* options) { return Status::OK(); },
        [](FlightClientOptions* options) { return Status::OK(); }));
  }
  void TearDown() {
    ASSERT_OK(client_->Close());
    ASSERT_OK(server_->Shutdown());
  }
  CancelTestServer* Server() const {
    return static_cast<CancelTestServer*>(server_.get());
  }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

TEST_F(TestCancel, ListFlights) {
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  stop_source.RequestStop(Status::Cancelled("StopSource"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  client_->ListFlights(options, {}));
}

TEST_F(TestCancel, DoAction) {
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(options, {}));
  ASSERT_OK_AND_ASSIGN(auto result, stream->Next());
  ASSERT_EQ("foo", result->body->ToString());
  stop_source.RequestStop(Status::Cancelled("StopSource"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  stream->Next());
}

TEST_F(TestCancel, DoActionSideEffect) {
  // GH-15150: DoAction should at least wait for the server to begin
  // the response, since existing code may be using DoAction solely
  // for the side effect.
  ASSERT_EQ(0, Server()->CheckCounter());
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  // Will block for a bit, but not forever
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoAction(options, {"inc", nullptr}));
  // Side effect should have happened
  ASSERT_EQ(1, Server()->CheckCounter());
  stop_source.RequestStop(Status::Cancelled("StopSource"));
}

TEST_F(TestCancel, ListActions) {
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  std::vector<ActionType> results;
  stop_source.RequestStop(Status::Cancelled("StopSource"));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  client_->ListActions(options));
}

TEST_F(TestCancel, DoGet) {
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  std::unique_ptr<ResultStream> results;
  stop_source.RequestStop(Status::Cancelled("StopSource"));
  ASSERT_OK_AND_ASSIGN(auto stream, client_->DoGet(options, {}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  stream->ToTable());

  ASSERT_OK_AND_ASSIGN(stream, client_->DoGet({}));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  stream->ToTable(options.stop_token));
}

TEST_F(TestCancel, DoExchange) {
  StopSource stop_source;
  FlightCallOptions options;
  options.stop_token = stop_source.token();
  std::unique_ptr<ResultStream> results;
  stop_source.RequestStop(Status::Cancelled("StopSource"));
  ASSERT_OK_AND_ASSIGN(auto do_exchange_result,
                       client_->DoExchange(options, FlightDescriptor::Command("")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  do_exchange_result.reader->ToTable());
  ARROW_UNUSED(do_exchange_result.writer->Close());

  ASSERT_OK_AND_ASSIGN(do_exchange_result,
                       client_->DoExchange(FlightDescriptor::Command("")));
  EXPECT_RAISES_WITH_MESSAGE_THAT(Cancelled, ::testing::HasSubstr("StopSource"),
                                  do_exchange_result.reader->ToTable(options.stop_token));
  ARROW_UNUSED(do_exchange_result.writer->Close());
}

class TracingTestServer : public FlightServerBase {
 public:
  Status DoAction(const ServerCallContext& call_context, const Action&,
                  std::unique_ptr<ResultStream>* result) override {
    std::vector<Result> results;
    auto* middleware =
        reinterpret_cast<TracingServerMiddleware*>(call_context.GetMiddleware("tracing"));
    if (!middleware) return Status::Invalid("Could not find middleware");
#ifdef ARROW_WITH_OPENTELEMETRY
    // Ensure the trace context is present (but the value is random so
    // we cannot assert any particular value)
    EXPECT_FALSE(middleware->GetTraceContext().empty());
    auto span = arrow::internal::tracing::GetTracer()->GetCurrentSpan();
    const auto context = span->GetContext();
    {
      const auto& span_id = context.span_id();
      ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(span_id.Id().size()));
      std::memcpy(buffer->mutable_data(), span_id.Id().data(), span_id.Id().size());
      results.push_back({std::move(buffer)});
    }
    {
      const auto& trace_id = context.trace_id();
      ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateBuffer(trace_id.Id().size()));
      std::memcpy(buffer->mutable_data(), trace_id.Id().data(), trace_id.Id().size());
      results.push_back({std::move(buffer)});
    }
#else
    // Ensure the trace context is not present (as OpenTelemetry is not enabled)
    EXPECT_TRUE(middleware->GetTraceContext().empty());
#endif
    *result = std::make_unique<SimpleResultStream>(std::move(results));
    return Status::OK();
  }
};

class TestTracing : public ::testing::Test {
 public:
  void SetUp() {
#ifdef ARROW_WITH_OPENTELEMETRY
    // The default tracer always generates no-op spans which have no
    // span/trace ID. Set up a different tracer. Note, this needs to
    // be run before Arrow uses OTel as GetTracer() gets a tracer once
    // and keeps it in a static.
    std::vector<std::unique_ptr<opentelemetry::sdk::trace::SpanProcessor>> processors;
    auto provider =
        opentelemetry::nostd::shared_ptr<opentelemetry::sdk::trace::TracerProvider>(
            new opentelemetry::sdk::trace::TracerProvider(std::move(processors)));
    opentelemetry::trace::Provider::SetTracerProvider(std::move(provider));

    opentelemetry::context::propagation::GlobalTextMapPropagator::SetGlobalPropagator(
        opentelemetry::nostd::shared_ptr<
            opentelemetry::context::propagation::TextMapPropagator>(
            new opentelemetry::trace::propagation::HttpTraceContext()));
#endif

    ASSERT_OK(MakeServer<TracingTestServer>(
        &server_, &client_,
        [](FlightServerOptions* options) {
          options->middleware.emplace_back("tracing",
                                           MakeTracingServerMiddlewareFactory());
          return Status::OK();
        },
        [](FlightClientOptions* options) {
          options->middleware.push_back(MakeTracingClientMiddlewareFactory());
          return Status::OK();
        }));
  }
  void TearDown() { ASSERT_OK(server_->Shutdown()); }

 protected:
  std::unique_ptr<FlightClient> client_;
  std::unique_ptr<FlightServerBase> server_;
};

#ifdef ARROW_WITH_OPENTELEMETRY
// Must define it ourselves to avoid a linker error
constexpr size_t kSpanIdSize = opentelemetry::trace::SpanId::kSize;
constexpr size_t kTraceIdSize = opentelemetry::trace::TraceId::kSize;

TEST_F(TestTracing, NoParentTrace) {
  ASSERT_OK_AND_ASSIGN(auto results, client_->DoAction(Action{}));

  ASSERT_OK_AND_ASSIGN(auto result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->body, nullptr);
  // Span ID should be a valid span ID, i.e. the server must have started a span
  ASSERT_EQ(result->body->size(), kSpanIdSize);
  opentelemetry::trace::SpanId span_id({result->body->data(), kSpanIdSize});
  ASSERT_TRUE(span_id.IsValid());

  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->body, nullptr);
  ASSERT_EQ(result->body->size(), kTraceIdSize);
  opentelemetry::trace::TraceId trace_id({result->body->data(), kTraceIdSize});
  ASSERT_TRUE(trace_id.IsValid());
}
TEST_F(TestTracing, WithParentTrace) {
  auto* tracer = arrow::internal::tracing::GetTracer();
  auto span = tracer->StartSpan("test");
  auto scope = tracer->WithActiveSpan(span);

  auto span_context = span->GetContext();
  auto current_trace_id = span_context.trace_id().Id();

  ASSERT_OK_AND_ASSIGN(auto results, client_->DoAction(Action{}));

  ASSERT_OK_AND_ASSIGN(auto result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->body, nullptr);
  ASSERT_EQ(result->body->size(), kSpanIdSize);
  opentelemetry::trace::SpanId span_id({result->body->data(), kSpanIdSize});
  ASSERT_TRUE(span_id.IsValid());

  ASSERT_OK_AND_ASSIGN(result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_NE(result->body, nullptr);
  ASSERT_EQ(result->body->size(), kTraceIdSize);
  opentelemetry::trace::TraceId trace_id({result->body->data(), kTraceIdSize});
  // The server span should have the same trace ID as the client span.
  ASSERT_EQ(std::string_view(reinterpret_cast<const char*>(trace_id.Id().data()),
                             trace_id.Id().size()),
            std::string_view(reinterpret_cast<const char*>(current_trace_id.data()),
                             current_trace_id.size()));
}
#else
TEST_F(TestTracing, NoOp) {
  // The middleware should not cause any trouble when OTel is not enabled.
  ASSERT_OK_AND_ASSIGN(auto results, client_->DoAction(Action{}));
  ASSERT_OK_AND_ASSIGN(auto result, results->Next());
  ASSERT_EQ(result, nullptr);
}
#endif

}  // namespace flight
}  // namespace arrow
