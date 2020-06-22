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

#include "arrow/flight/test_integration.h"
#include "arrow/flight/client_middleware.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/test_util.h"
#include "arrow/flight/types.h"
#include "arrow/ipc/dictionary.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace arrow {
namespace flight {

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
  RETURN_NOT_OK(client->DoAction(action, &stream));
  std::unique_ptr<Result> result;
  for (const std::string& expected : results) {
    RETURN_NOT_OK(stream->Next(&result));
    if (!result) {
      return Status::Invalid("Action result stream ended early");
    }
    const auto actual = result->body->ToString();
    if (expected != actual) {
      return Status::Invalid("Got wrong result; expected", expected, "but got", actual);
    }
  }
  RETURN_NOT_OK(stream->Next(&result));
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
    const auto& status = client->DoAction(action, &stream);
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
      Location location;
      // Return a fake location - the test doesn't read it
      RETURN_NOT_OK(Location::ForGrpcTcp("localhost", 10010, &location));
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
    std::unique_ptr<FlightInfo> info;
    // This call is expected to fail. In gRPC/Java, this causes the
    // server to combine headers and HTTP/2 trailers, so to read the
    // expected header, Flight must check for both headers and
    // trailers.
    if (client->GetFlightInfo(FlightDescriptor::Command(""), &info).ok()) {
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
    RETURN_NOT_OK(client->GetFlightInfo(FlightDescriptor::Command("success"), &info));
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

Status GetScenario(const std::string& scenario_name, std::shared_ptr<Scenario>* out) {
  if (scenario_name == "auth:basic_proto") {
    *out = std::make_shared<AuthBasicProtoScenario>();
    return Status::OK();
  } else if (scenario_name == "middleware") {
    *out = std::make_shared<MiddlewareScenario>();
    return Status::OK();
  }
  return Status::KeyError("Scenario not found: ", scenario_name);
}

}  // namespace flight
}  // namespace arrow
