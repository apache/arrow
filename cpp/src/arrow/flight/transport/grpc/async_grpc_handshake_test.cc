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

// Handshake and per-RPC token auth on the async generic service: the server
// class's Handshake()/ValidateToken() hooks, the token being required on data
// calls, payload fidelity (binary, empty), an abandoned or late handshake, and
// concurrency on fresh and reused channels.

#include <future>

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

// Counters shared between the server callbacks and the test thread.
struct AsyncAuthState {
  std::mutex mutex;
  int handshakes = 0;
  int validations = 0;
  std::string peer_identity;
};

// Async-transport twin of TestServerAuthHandler: the handshake answers the
// client's password with the username, later RPCs must carry that password as
// the token, which is what TestClientAuthHandler::GetToken() puts in the
// `auth-token-bin` header (the sync twin validates it the same way in
// TestServerAuthHandler::IsValid). It is a server class: the two hooks below
// are the only configuration the transport needs.
class AsyncAuthTestServer : public AsyncGenericFlightServerBase {
 public:
  AsyncAuthTestServer(std::string username, std::string password, std::string path)
      : username_(std::move(username)),
        password_(std::move(password)),
        path_(std::move(path)) {}

  const std::shared_ptr<AsyncAuthState>& state() const { return state_; }

  arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
      const ServerCallContext& context, const FlightDescriptor& request) override {
    {
      std::lock_guard<std::mutex> guard(state_->mutex);
      state_->peer_identity = context.peer_identity();
    }
    if (request.path != std::vector<std::string>{path_}) {
      return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
          Status::KeyError("No such flight: ", request.ToString()));
    }
    return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
        std::make_shared<FlightInfo>(MakeTestFlightInfo(request)));
  }

  // The hook: the transport read the client's password, answer with the
  // username, or reject the RPC the way the sync twin does.
  Status Handshake(const ServerCallContext& /*context*/, const std::string& password,
                   std::string* response) override {
    {
      std::lock_guard<std::mutex> guard(state_->mutex);
      state_->handshakes++;
    }
    if (password != password_) {
      return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
    }
    *response = username_;
    return Status::OK();
  }

  Status ValidateToken(const ServerCallContext& /*context*/, const std::string& token,
                       std::string* peer_identity) override {
    std::lock_guard<std::mutex> guard(state_->mutex);
    state_->validations++;
    if (token != password_) {
      return MakeFlightError(FlightStatusCode::Unauthenticated, "Invalid token");
    }
    *peer_identity = username_;
    return Status::OK();
  }

 private:
  std::string username_;
  std::string password_;
  std::string path_;
  std::shared_ptr<AsyncAuthState> state_ = std::make_shared<AsyncAuthState>();
};

// A server with the async auth hooks installed, and a client connected to it.
// `status` carries the setup result, construction never fails a test.
struct AuthenticatedHarness {
  AsyncAuthTestServer server{"user", "p4ssw0rd", "auth-path"};
  std::unique_ptr<FlightClient> client;
  Status status;

  AuthenticatedHarness() : status(Start()) {}

  ~AuthenticatedHarness() {
    if (client) {
      ARROW_WARN_NOT_OK(client->Close(), "Close()");
    }
    ARROW_WARN_NOT_OK(server.Shutdown(), "Shutdown()");
  }

 private:
  Status Start() {
    // The server class alone selects the async transport: deriving from it and
    // overriding its virtuals are the whole configuration.
    FlightServerOptions options(LocalhostLocation());
    RETURN_NOT_OK(server.Init(options));
    ARROW_ASSIGN_OR_RAISE(client, ConnectTo(server.port()));
    return Status::OK();
  }
};

TEST(AsyncGrpcTest, HandshakeAuthenticatesAndTokenIsRequired) {
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);

  // 1. Without a handshake the data call is rejected before it is served.
  ASSERT_OK_AND_ASSIGN(auto unauthenticated_client, ConnectTo(harness.server.port()));
  auto status =
      unauthenticated_client->GetFlightInfo(FlightDescriptor::Path({"auth-path"}))
          .status();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
  ASSERT_OK(unauthenticated_client->Close());

  // 2. After the handshake the same call succeeds and the server sees the
  //    validated identity.
  ASSERT_OK(harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
  ASSERT_OK_AND_ASSIGN(
      auto info, harness.client->GetFlightInfo(FlightDescriptor::Path({"auth-path"})));
  EXPECT_EQ(FlightDescriptor::Path({"auth-path"}), info->descriptor());
  {
    std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
    EXPECT_EQ(1, harness.server.state()->handshakes);
    EXPECT_EQ("user", harness.server.state()->peer_identity);
    EXPECT_GE(harness.server.state()->validations, 1);
  }
}

TEST(AsyncGrpcTest, BadPasswordIsRejectedAtHandshake) {
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  const auto status = harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "wrong"));
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
  auto detail = FlightStatusDetail::UnwrapStatus(status);
  ASSERT_NE(nullptr, detail) << "the rejection must carry its Flight status code";
  EXPECT_EQ(FlightStatusCode::Unauthenticated, detail->code());
}

TEST(AsyncGrpcTest, UnauthenticatedUnknownMethodIsRejectedBeforeUnimplemented) {
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  // A method the service does not have: it goes through the CheckAuth path and
  // the UNIMPLEMENTED fallback reactor, and auth runs first, so the client must
  // see the auth failure rather than UNIMPLEMENTED.
  ASSERT_OK_AND_ASSIGN(auto raw, CallRaw(harness.server.port(), kUnknownMethod, {}));
  ASSERT_FALSE(raw.status.ok());
  // UNAUTHENTICATED, not UNIMPLEMENTED: the auth check ran before dispatch.
  EXPECT_EQ(::grpc::StatusCode::UNAUTHENTICATED, raw.status.error_code())
      << "expected the auth failure, got: " << raw.status.error_message();
}

// ---------------------------------------------------------------------------
// The synchronous request/response Handshake hook: payload fidelity, edge
// cases, and reactor cleanup (the reactor finishes on the strength of "one
// terminal path per direction", so the concurrency test is its real check).
// ---------------------------------------------------------------------------

// Send `password`, require the server to echo `expect` back.
class EchoClientAuthHandler : public ClientAuthHandler {
 public:
  EchoClientAuthHandler(std::string password, std::string expect)
      : password_(std::move(password)), expect_(std::move(expect)) {}

  Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override {
    ARROW_RETURN_NOT_OK(outgoing->Write(password_));
    ARROW_RETURN_NOT_OK(incoming->Read(&response_));
    if (response_ != expect_) {
      return MakeFlightError(FlightStatusCode::Unauthenticated,
                             "unexpected handshake response");
    }
    return Status::OK();
  }

  Status GetToken(std::string* token) override {
    *token = password_;
    return Status::OK();
  }

  const std::string& response() const { return response_; }

 private:
  std::string password_;
  std::string expect_;
  std::string response_;
};

TEST(AsyncGrpcTest, HandshakePayloadTable) {
  // The hook's request is a std::string taken from the protobuf and its response
  // is echoed back through the client's reader: every byte must survive both
  // directions, including an embedded NUL and non-ASCII bytes, and an empty
  // request or response must be *a* message rather than a hang. A rejection on
  // either side must reach the client as an Unauthenticated Flight status.
  struct Case {
    std::string name;
    std::string server_password;
    std::string server_response;
    std::string request;
    std::string expect;
    /// Empty when the handshake succeeds; otherwise the expected message.
    std::string expected_error;
  };
  const std::string binary("p4ss\0w0rd\xC3\xA9", 11);
  const std::vector<Case> cases = {
      {"ASCII", "p4ssw0rd", "user", "p4ssw0rd", "user", ""},
      {"binary-safe request", binary, "user", binary, "user", ""},
      {"binary-safe response", "p4ssw0rd", binary, "p4ssw0rd", binary, ""},
      {"empty request", "", "user", "", "user", ""},
      {"empty response", "p4ssw0rd", "", "p4ssw0rd", "", ""},
      {"wrong password", "p4ssw0rd", "user", "nope", "user", "Invalid token"},
      {"unexpected response", "p4ssw0rd", "user", "p4ssw0rd", "other",
       "unexpected handshake response"},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    AsyncAuthTestServer server(test.server_response, test.server_password, "table-path");
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&server));

    auto handler = std::make_unique<EchoClientAuthHandler>(test.request, test.expect);
    const auto* observed = handler.get();
    const auto status = client->Authenticate({}, std::move(handler));
    if (!test.expected_error.empty()) {
      ASSERT_FALSE(status.ok()) << "the handshake must be rejected";
      EXPECT_THAT(status.ToString(), ::testing::HasSubstr(test.expected_error));
      auto detail = FlightStatusDetail::UnwrapStatus(status);
      ASSERT_NE(nullptr, detail) << "the rejection must carry its Flight status code";
      EXPECT_EQ(FlightStatusCode::Unauthenticated, detail->code());
      ASSERT_OK(StopClientAndServer(client.get(), &server));
      continue;
    }
    ASSERT_OK(status) << status;
    EXPECT_EQ(test.expect, observed->response());

    // An accepted handshake produces a usable token: the data call works.
    ASSERT_OK(client->GetFlightInfo(FlightDescriptor::Path({"table-path"})).status());
    std::lock_guard<std::mutex> guard(server.state()->mutex);
    EXPECT_EQ(1, server.state()->handshakes);
    ASSERT_OK(StopClientAndServer(client.get(), &server));
  }
}

TEST(AsyncGrpcTest, EmptyHandshakeResponseIsDelivered) {
  // An empty response is a message, not a missing one: the client's Read returns
  // an empty string rather than hanging.
  AsyncAuthTestServer server(/*username=*/"", /*password=*/"", "empty-response-path");
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&server));

  auto handler = std::make_unique<EchoClientAuthHandler>("", std::string());
  const auto* observed = handler.get();
  ASSERT_OK(client->Authenticate({}, std::move(handler)));
  EXPECT_EQ(std::string(), observed->response());

  std::lock_guard<std::mutex> guard(server.state()->mutex);
  EXPECT_EQ(1, server.state()->handshakes);
  ASSERT_OK(StopClientAndServer(client.get(), &server));
}

TEST(AsyncGrpcTest, EmptyHandshakeRequestIsRejectedNotHung) {
  // An empty HandshakeRequest is a valid protobuf message: the read succeeds,
  // the hook sees an empty string, and its rejection must reach the client with
  // the Flight status detail intact.
  class SilentClientAuthHandler : public ClientAuthHandler {
   public:
    Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader*) override {
      return outgoing->Write("");
    }
    Status GetToken(std::string*) override { return Status::OK(); }
  };

  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  const auto status =
      harness.client->Authenticate({}, std::make_unique<SilentClientAuthHandler>());
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
  EXPECT_EQ(FlightStatusCode::Unauthenticated,
            FlightStatusDetail::UnwrapStatus(status)->code());
  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(1, harness.server.state()->handshakes)
      << "the hook must run on an empty request, not fail the read";
}

TEST(AsyncGrpcTest, AbandonedHandshakeLeavesServerUsable) {
  // A client that never sends a message: the server's read completes with
  // ok=false, the reactor must still finish, and the server must keep serving.
  class AbandoningClientAuthHandler : public ClientAuthHandler {
   public:
    Status Authenticate(ClientAuthSender*, ClientAuthReader*) override {
      return Status::Invalid("client never speaks");
    }
    Status GetToken(std::string*) override { return Status::OK(); }
  };

  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  EXPECT_FALSE(
      harness.client->Authenticate({}, std::make_unique<AbandoningClientAuthHandler>())
          .ok());

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(harness.server.port()));
  ASSERT_OK(client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
  ASSERT_OK(client->GetFlightInfo(FlightDescriptor::Path({"auth-path"})).status());
  ASSERT_OK(client->Close());

  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(1, harness.server.state()->handshakes)
      << "the abandoned call must never have reached the hook";
}

TEST(AsyncGrpcTest, ConcurrentHandshakesAllComplete) {
  constexpr int kClients = 32;
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);

  std::vector<std::thread> threads;
  std::vector<Status> statuses(kClients);
  for (int i = 0; i < kClients; i++) {
    threads.emplace_back([&harness, &statuses, i] {
      auto client = ConnectTo(harness.server.port());
      if (!client.ok()) {
        statuses[i] = client.status();
        return;
      }
      auto authenticated = (*client)->Authenticate(
          {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd"));
      if (!authenticated.ok()) {
        statuses[i] = authenticated;
        return;
      }
      auto info = (*client)->GetFlightInfo(FlightDescriptor::Path({"auth-path"}));
      if (!info.ok()) {
        statuses[i] = info.status();
        return;
      }
      if ((*info)->descriptor() != FlightDescriptor::Path({"auth-path"})) {
        statuses[i] = Status::Invalid("client ", i, " got the wrong flight");
        return;
      }
      statuses[i] = (*client)->Close();
    });
  }
  for (auto& thread : threads) thread.join();
  for (int i = 0; i < kClients; i++) {
    EXPECT_TRUE(statuses[i].ok()) << "client " << i << ": " << statuses[i];
  }

  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(kClients, harness.server.state()->handshakes);
}

TEST(AsyncGrpcTest, RepeatedHandshakesOnFreshChannels) {
  constexpr int kRounds = 50;
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  for (int i = 0; i < kRounds; i++) {
    ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(harness.server.port()));
    ASSERT_OK(client->Authenticate(
        {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
    ASSERT_OK(client->Close());
  }
  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(kRounds, harness.server.state()->handshakes);
}

TEST(AsyncGrpcTest, LateHandshakeResponseLeavesServerUsable) {
  // The client gives up while the hook is still running, so the server's write
  // fails: the reactor's write-side terminal path must still Finish, and the
  // server must keep serving.
  //
  // The hook parks until the test releases it, rather than sleeping: the client's
  // deadline has to expire while the hook is provably running, not while the
  // connection is being set up, or the test fails on a loaded runner.
  class SlowAnsweringServer : public AsyncGenericFlightServerBase {
   public:
    Status Handshake(const ServerCallContext&, const std::string&,
                     std::string* response) override {
      handshakes.fetch_add(1);
      // Only the first call parks; the second one (on a fresh client, after the
      // deadline) must answer. A shared_future can be waited on repeatedly,
      // unlike a future, which may only be consumed once. The wait is bounded so
      // a test that fails before releasing cannot wedge a gRPC thread.
      if (handshakes.load() == 1) {
        release.wait_for(std::chrono::seconds(10));
      }
      *response = "user";
      return Status::OK();
    }
    std::atomic<int> handshakes{0};
    std::promise<void> release_promise;
    // Satisfied by the test once the client's deadline has expired. Blocking here
    // is what the deadline has to interrupt.
    std::shared_future<void> release = release_promise.get_future().share();
  };

  SlowAnsweringServer server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectToWarmed(server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  auto status = client->Authenticate(
      impatient, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd"));
  EXPECT_FALSE(status.ok()) << "the client's deadline must expire during the hook";
  EXPECT_NE(std::string::npos, status.ToString().find("Deadline"))
      << "expected a deadline error, got: " << status.ToString();
  EXPECT_EQ(1, server.handshakes.load())
      << "the first call must have reached the hook, so its write had to finish";
  // The deadline expired, so the hook is parked with the RPC already cancelled.
  server.release_promise.set_value();

  ASSERT_OK_AND_ASSIGN(auto second_client, ConnectTo(server.port()));
  ASSERT_OK(second_client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));

  ASSERT_OK(second_client->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &server));
}

TEST(AsyncGrpcTest, RehandshakeOnTheSameChannelWorks) {
  // The Handshake RPC is per-call, so authenticating twice on one channel must
  // work: the second handshake reaches the hook and the channel keeps serving.
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  ASSERT_OK(harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
  ASSERT_OK(harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
  ASSERT_OK(
      harness.client->GetFlightInfo(FlightDescriptor::Path({"auth-path"})).status());

  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(2, harness.server.state()->handshakes);
}

}  // namespace arrow::flight::transport::grpc
