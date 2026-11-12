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

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

// ---------------------------------------------------------------------------
// Handshake + per-RPC token auth on the async generic service.
// ---------------------------------------------------------------------------

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
  AsyncAuthTestServer(std::string username, std::string password, std::string ticket,
                      std::shared_ptr<AsyncAuthState> state = nullptr)
      : username_(std::move(username)),
        password_(std::move(password)),
        ticket_(std::move(ticket)),
        state_(state ? std::move(state) : std::make_shared<AsyncAuthState>()) {}

  const std::shared_ptr<AsyncAuthState>& state() const { return state_; }

  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext& context, const Ticket& request) override {
    {
      std::lock_guard<std::mutex> guard(state_->mutex);
      state_->peer_identity = context.peer_identity();
    }
    if (request.ticket != ticket_) {
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
          Status::KeyError("No such ticket: ", request.ticket));
    }
    auto schema = arrow::schema({arrow::field("a", arrow::int64())});
    auto batch = arrow::RecordBatch::Make(
        schema, 2, {arrow::ArrayFromJSON(arrow::int64(), "[1, 2]")});
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::make_shared<ResolvedStream>(std::move(reader)));
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
  std::string ticket_;
  std::shared_ptr<AsyncAuthState> state_;
};

// A server with the async auth hooks installed, and a client connected to it.
// `status` carries the setup result, construction never fails a test.
struct AuthenticatedHarness {
  AsyncAuthTestServer server{"user", "p4ssw0rd", "auth-ticket"};
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
    ARROW_ASSIGN_OR_RAISE(auto location, Location::Parse(kServerUri));
    // The server class alone selects the async transport: deriving from it and
    // overriding its virtuals are the whole configuration.
    FlightServerOptions options(location);
    RETURN_NOT_OK(server.Init(options));
    ARROW_ASSIGN_OR_RAISE(client, ConnectTo(server.port()));
    return Status::OK();
  }
};

// A minimal server that mixes in the async hooks: the server class alone
// selects the async generic transport.
class AsyncGenericTestServer : public AsyncGenericFlightServerBase {};

TEST(AsyncGrpcTest, HandshakeWithoutHookIsUnimplemented) {
  // A bare async server: Handshake is answered by the class's default virtual
  // with UNIMPLEMENTED, and the client's Authenticate() surfaces it.
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  AsyncGenericTestServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto status = client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd"));
  ASSERT_FALSE(status.ok()) << "handshake must not succeed without a hook";
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("authentication mechanism"));

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

TEST(AsyncGrpcTest, AsyncGenericFlightServerBaseImpliesAsyncTransport) {
  // Deriving from the async class is enough: no hooks and no option to set,
  // yet the RPC is served by the generic callback service. DoPut is the
  // discriminator: an async server with no listener factory answers with its own
  // message rather than "NYI".
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  AsyncGenericTestServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // DoPut is the discriminator: an async server with no listener factory
  // answers with its own message rather than "NYI". The refusal can surface on
  // either call: FlightClient::DoPut
  // writes the schema payload eagerly, so when the server's UNIMPLEMENTED
  // arrives first the DoPut call itself fails, otherwise the writer's Close()
  // reports it.
  auto put = client->DoPut(FlightDescriptor::Path({"x"}), arrow::schema({}));
  auto status = put.status();
  if (status.ok()) {
    status = put->writer->Close();
  }
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("no listener available"))
      << "the async generic service must have served this call, not the sync path";

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

TEST(AsyncGrpcTest, HandshakeAuthenticatesAndTokenIsRequired) {
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);

  // 1. Without a handshake the data call is rejected before it is served.
  ASSERT_OK_AND_ASSIGN(auto unauthenticated_client, ConnectTo(harness.server.port()));
  auto status = unauthenticated_client->DoGet(Ticket{"auth-ticket"}).status();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
  ASSERT_OK(unauthenticated_client->Close());

  // 2. After the handshake the same call succeeds and the server sees the
  //    validated identity.
  ASSERT_OK(harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));
  ASSERT_OK_AND_ASSIGN(auto reader, harness.client->DoGet(Ticket{"auth-ticket"}));
  int batches = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;
    batches++;
  }
  EXPECT_EQ(1, batches);
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
  auto status = harness.client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "wrong"));
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
}

TEST(AsyncGrpcTest, UnauthenticatedUnknownMethodIsRejectedBeforeUnimplemented) {
  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  // GetFlightInfo is not served by the async generic service, but auth runs
  // first: the client must see the auth failure, not UNIMPLEMENTED.
  auto status = harness.client->GetFlightInfo(FlightDescriptor::Path({"x"})).status();
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
  EXPECT_EQ(arrow::StatusCode::IOError, status.code());
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

TEST(AsyncGrpcTest, HandshakePayloadIsBinarySafe) {
  // The hook's request is a std::string taken from the protobuf and its
  // response is echoed back through the client's reader: an embedded NUL and
  // non-ASCII bytes must survive both directions unchanged.
  const std::string binary("p4ss\0w0rd\xC3\xA9", 11);
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  AsyncAuthTestServer server(binary, binary, "bin-ticket");
  FlightServerOptions options(location);
  ASSERT_OK(server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(server.port()));

  auto handler = std::make_unique<EchoClientAuthHandler>(binary, binary);
  const auto* observed = handler.get();
  ASSERT_OK(client->Authenticate({}, std::move(handler)));
  EXPECT_EQ(binary, observed->response());

  // The token is the password, so an authenticated call must also work.
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"bin-ticket"}));
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  EXPECT_NE(nullptr, chunk.data);

  ASSERT_OK(client->Close());
  ASSERT_OK(server.Shutdown());
  ASSERT_OK(server.Wait());
}

TEST(AsyncGrpcTest, EmptyHandshakeRequestIsRejectedNotHung) {
  // An empty HandshakeRequest is a valid protobuf message: the read succeeds,
  // the hook sees an empty string, and its rejection must reach the client.
  class SilentClientAuthHandler : public ClientAuthHandler {
   public:
    Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader*) override {
      return outgoing->Write("");
    }
    Status GetToken(std::string*) override { return Status::OK(); }
  };

  AuthenticatedHarness harness;
  ASSERT_OK(harness.status);
  auto status =
      harness.client->Authenticate({}, std::make_unique<SilentClientAuthHandler>());
  ASSERT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Invalid token"));
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
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"auth-ticket"}));
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  EXPECT_NE(nullptr, chunk.data);
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
      auto reader = (*client)->DoGet(Ticket{"auth-ticket"});
      if (!reader.ok()) {
        statuses[i] = reader.status();
        return;
      }
      auto chunk = (*reader)->Next();
      if (!chunk.ok()) {
        statuses[i] = chunk.status();
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
  class SlowAnsweringServer : public AsyncGenericFlightServerBase {
   public:
    Status Handshake(const ServerCallContext&, const std::string&,
                     std::string* response) override {
      handshakes.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::milliseconds(300));
      *response = "user";
      return Status::OK();
    }
    std::atomic<int> handshakes{0};
  };

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  SlowAnsweringServer server;
  FlightServerOptions options(location);
  ASSERT_OK(server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(50);
  auto status = client->Authenticate(
      impatient, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd"));
  EXPECT_FALSE(status.ok()) << "the client's deadline must expire during the hook";
  EXPECT_NE(std::string::npos, status.ToString().find("Deadline"))
      << "expected a deadline error, got: " << status.ToString();
  EXPECT_EQ(1, server.handshakes.load())
      << "the first call must have reached the hook, so its write had to finish";

  ASSERT_OK_AND_ASSIGN(auto second_client, ConnectTo(server.port()));
  ASSERT_OK(second_client->Authenticate(
      {}, std::make_unique<TestClientAuthHandler>("user", "p4ssw0rd")));

  ASSERT_OK(second_client->Close());
  ASSERT_OK(client->Close());
  ASSERT_OK(server.Shutdown());
  ASSERT_OK(server.Wait());
}

TEST(AsyncGrpcTest, EmptyHandshakeResponseIsDelivered) {
  // A hook that answers nothing still produces exactly one (empty) response
  // message: the client's Read must return an empty string rather than hang.
  class NoResponseServer : public AsyncGenericFlightServerBase {
   public:
    Status Handshake(const ServerCallContext&, const std::string&,
                     std::string*) override {
      return Status::OK();
    }
  };

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  NoResponseServer server;
  FlightServerOptions options(location);
  ASSERT_OK(server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(server.port()));

  auto handler = std::make_unique<EchoClientAuthHandler>("anything", std::string());
  const auto* observed = handler.get();
  ASSERT_OK(client->Authenticate({}, std::move(handler)));
  EXPECT_EQ(std::string(), observed->response());

  ASSERT_OK(client->Close());
  ASSERT_OK(server.Shutdown());
  ASSERT_OK(server.Wait());
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
  ASSERT_OK_AND_ASSIGN(auto reader, harness.client->DoGet(Ticket{"auth-ticket"}));
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  EXPECT_NE(nullptr, chunk.data);

  std::lock_guard<std::mutex> guard(harness.server.state()->mutex);
  EXPECT_EQ(2, harness.server.state()->handshakes);
}

}  // namespace arrow::flight::transport::grpc
