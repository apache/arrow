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

// Dispatch-level tests of the async generic gRPC Flight service: the
// UNIMPLEMENTED fallback reactor, middleware on every call (including the calls
// that never reach a handler), the options an async server accepts, and the two
// ways it can be started -- its own Init() or a hand-built grpc::ServerBuilder.
//
// Per-RPC behaviour lives in async_grpc_do_get_test.cc, async_grpc_do_put_test.cc,
// async_grpc_unary_test.cc, async_grpc_streaming_test.cc and
// async_grpc_exchange_test.cc.

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

TEST(AsyncGrpcTest, MiddlewareCallCompletedRunsOnUnknownMethod) {
  // Middleware that started for a call must also see CallCompleted when the
  // call is answered by the UNIMPLEMENTED fallback reactor: otherwise tracing
  // middleware leaks a span per unknown/unimplemented RPC.
  auto trace = std::make_shared<MiddlewareTrace>();
  TestFlightServer flight_server;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr,
          MiddlewareFactoryList{
              {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}});
  HandBuiltServer hand_built = StartHandBuilt(&flight_server, helper);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  ASSERT_OK_AND_ASSIGN(auto raw, CallRaw(hand_built.port, kUnknownMethod, {}));
  ASSERT_EQ(::grpc::StatusCode::UNIMPLEMENTED, raw.status.error_code())
      << raw.status.error_message();

  hand_built.server->Shutdown();
  hand_built.server->Wait();

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call) << "the middleware must have started for the call";
  ASSERT_EQ(1, trace->completed_statuses.size())
      << "CallCompleted must run for an UNIMPLEMENTED reply too";
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagRefusesBlockingAuthHandler) {
  // The callback path cannot run the blocking ServerAuthHandler (it would run
  // on callback threads), so a server configured with one must refuse to start
  // instead of silently serving unauthenticated requests. Middleware *is*
  // supported now - see UseAsyncGrpcFlagAllowsMiddleware.
  TestFlightServer auth_server;
  FlightServerOptions with_auth(LocalhostLocation());
  with_auth.auth_handler = std::make_shared<TestServerAuthHandler>("user", "pass");
  auto auth_status = auth_server.Init(with_auth);
  ASSERT_FALSE(auth_status.ok()) << "an auth handler must not be silently skipped";
  ASSERT_EQ(auth_status.code(), arrow::StatusCode::NotImplemented);
  ASSERT_NE(auth_status.message().find("AsyncGenericFlightServerBase"), std::string::npos)
      << auth_status;
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagAllowsMiddleware) {
  // Middleware configured through FlightServerOptions must run on the async
  // path: StartCall at call creation, SendingHeaders and CallCompleted at the
  // end - success or failure alike.
  auto trace = std::make_shared<MiddlewareTrace>();
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {
      {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
  ASSERT_OK(flight_server.Init(options));

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));
  ASSERT_OK(client->GetFlightInfo(FlightDescriptor::Path({"middleware"})).status());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call);
  EXPECT_EQ(1, trace->sending_headers);
  ASSERT_EQ(1, trace->completed_statuses.size());
  EXPECT_TRUE(trace->completed_statuses[0].ok()) << trace->completed_statuses[0];
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagMiddlewareCanRejectCall) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {
      {"rejecting", std::make_shared<RejectingServerMiddlewareFactory>()}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto status = client->GetFlightInfo(FlightDescriptor::Path({"rejected"})).status();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("rejected by middleware"));

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

/// A ServerCallContext no async server handed out, as a synchronous
/// FlightServerBase handler would hold.
class ForeignCallContext final : public ServerCallContext {
 public:
  const std::string& peer_identity() const override { return empty_; }
  const std::string& peer() const override { return empty_; }
  void AddHeader(const std::string&, const std::string&) const override {}
  void AddTrailer(const std::string&, const std::string&) const override {}
  ServerMiddleware* GetMiddleware(const std::string&) const override { return nullptr; }
  bool is_cancelled() const override { return false; }
  const CallHeaders& incoming_headers() const override { return headers_; }

 private:
  std::string empty_;
  CallHeaders headers_;
};

TEST(AsyncGrpcTest, AddCancelCallbackRefusesAContextItDidNotHandOut) {
  // Cancellation is only pushed for calls the async server serves: any other
  // context is refused, and its callback never runs.
  ForeignCallContext context;
  bool ran = false;
  const Status status =
      AsyncGenericFlightServerBase::AddCancelCallback(context, [&ran] { ran = true; });
  EXPECT_TRUE(status.IsInvalid()) << status;
  EXPECT_FALSE(ran) << "a refused callback must not run";
}

namespace {

/// One raw request message built from its protobuf bytes.
::grpc::ByteBuffer RawMessage(const std::string& bytes) {
  ::grpc::Slice slice(bytes);
  return ::grpc::ByteBuffer(&slice, 1);
}

/// One valid request message for `method`, from a real FlightClient-shaped
/// protobuf, for the malformed-payload and default-handler tables.
struct MethodRequest {
  std::string_view method;
  std::string message;
};

// A well-formed but incomplete FlightDescriptor protobuf: field 1 announces 127
// bytes and carries two.
const char kTruncatedDescriptor[] = "\x0a\x7f\x01\x02";

std::optional<MethodRequest> ValidRequest(std::string_view method) {
  if (method == "Handshake") {
    // An empty HandshakeRequest: every field is at its default, and the default
    // Handshake() answers NotImplemented whatever the payload.
    return MethodRequest{method, ""};
  }
  if (method == "GetFlightInfo" || method == "GetSchema" || method == "PollFlightInfo") {
    return MethodRequest{method, WireBytes(FlightDescriptor::Path({"path"}))};
  }
  return std::nullopt;
}

}  // namespace

TEST(AsyncGrpcTest, RawRequestFailures) {
  // Every RPC must reject a request it cannot read before dispatch: no handler
  // runs, nothing is answered, and the client gets a status that says which
  // check failed. A raw generic call is the only way to send these.
  struct Case {
    std::string name;
    std::string method;
    std::vector<::grpc::ByteBuffer> request;
    ::grpc::StatusCode code;
    std::string message;
    bool expect_handler;  // DoPut's listener factory runs before any message
  };

  const std::vector<std::string> no_message_methods = {"Handshake", "GetFlightInfo",
                                                       "GetSchema", "PollFlightInfo"};
  const std::vector<std::pair<std::string, std::string>> parse_names = {
      {"Handshake", "HandshakeRequest"},
      {"GetFlightInfo", "FlightDescriptor"},
      {"GetSchema", "FlightDescriptor"},
      {"PollFlightInfo", "FlightDescriptor"}};

  std::vector<Case> cases;
  // No message at all, then a half-close: nothing to parse.
  for (const auto& method : no_message_methods) {
    cases.push_back({method + " with no message",
                     method,
                     {},
                     ::grpc::StatusCode::INTERNAL,
                     "Failed to read request",
                     false});
  }
  // One truncated length-delimited protobuf.
  for (const auto& [method, what] : parse_names) {
    cases.push_back({method + " with a truncated request",
                     method,
                     {RawMessage(std::string(kTruncatedDescriptor, 4))},
                     ::grpc::StatusCode::INVALID_ARGUMENT,
                     std::string("Failed to parse ") + what,
                     false});
  }
  // A valid protobuf whose descriptor type is UNKNOWN: every field at its
  // default, and DescriptorType.UNKNOWN is 0, so an empty message is exactly
  // that. A path alongside it is not needed: the type is checked first.
  for (const auto& method : {"GetFlightInfo", "GetSchema", "PollFlightInfo"}) {
    cases.push_back({std::string(method) + " with an UNKNOWN descriptor",
                     method,
                     {RawMessage("")},
                     ::grpc::StatusCode::INVALID_ARGUMENT,
                     "UNKNOWN descriptor type",
                     false});
  }

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    ASSERT_OK_AND_ASSIGN(
        auto raw, CallRaw(flight_server.port(), MethodPath(test.method), test.request));
    EXPECT_EQ(test.code, raw.status.error_code()) << raw.status.error_message();
    EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr(test.message));
    EXPECT_TRUE(raw.responses.empty()) << "a rejected request must answer nothing";
    // DoPut asks the server for its listener before reading any message, so its
    // factory call is expected; no other handler override may run.
    if (test.method == "DoPut") {
      EXPECT_EQ(1, flight_server.handler_calls());
    } else {
      EXPECT_EQ(0, flight_server.handler_calls())
          << "a rejected request reached its handler";
    }
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
}

TEST(AsyncGrpcTest, DefaultHandlersAreUnimplemented) {
  // A bare async server implements no handler: every RPC must answer
  // UNIMPLEMENTED with the message of its own default, before anything is
  // written back.
  AsyncGenericFlightServerBase flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));

  struct Case {
    std::string method;
    std::string message;
  };
  const std::vector<Case> cases = {
      {"GetFlightInfo", "NYI"},
      {"PollFlightInfo", "NYI"},
      {"GetSchema", "NYI"},
      {"Handshake", "authentication mechanism"},
  };

  const int64_t before = detail::AsyncReactorBase::live_reactors();
  for (const auto& test : cases) {
    SCOPED_TRACE(test.method);
    const auto made = ValidRequest(test.method);
    ASSERT_TRUE(made.has_value());
    const std::vector<::grpc::ByteBuffer> request = {RawMessage(made->message)};
    ASSERT_OK_AND_ASSIGN(auto raw,
                         CallRaw(flight_server.port(), MethodPath(test.method), request));
    EXPECT_EQ(::grpc::StatusCode::UNIMPLEMENTED, raw.status.error_code())
        << raw.status.error_message();
    EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr(test.message));
    EXPECT_TRUE(raw.responses.empty()) << "an unimplemented method must answer nothing";
  }

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  ExpectNoReactorLeak(before);
}

TEST(AsyncGrpcTest, UnknownMethodsAreUnimplemented) {
  // A method the service does not have answers UNIMPLEMENTED naming it, and so
  // does a name under a different service prefix: neither may be dispatched to
  // any handler.
  TestFlightServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  const std::vector<std::string> methods = {
      "/arrow.flight.protocol.FlightService/NoSuchMethod",
      "/arrow.flight.protocol.OtherService/GetFlightInfo",
  };
  for (const auto& method : methods) {
    SCOPED_TRACE(method);
    ASSERT_OK_AND_ASSIGN(auto raw, CallRaw(flight_server.port(), method, {}));
    EXPECT_EQ(::grpc::StatusCode::UNIMPLEMENTED, raw.status.error_code());
    EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr("Unknown method"));
    EXPECT_TRUE(raw.responses.empty());
  }
  EXPECT_EQ(0, flight_server.handler_calls());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, MiddlewareCompletesOncePerCall) {
  // StartCall, SendingHeaders and CallCompleted run once per call, whatever its
  // outcome: a handler error or an unknown method. CallCompleted carries the
  // call's own status, which is the only Arrow-level oracle for a call that
  // never reached a handler.
  struct Case {
    std::string name;
    std::function<void(FlightClient*, const std::string&)> drive;
    bool reached_handler;
    std::function<void(const Status&)> check_status;
  };

  const std::vector<Case> cases = {
      {"handler error",
       [](FlightClient* client, const std::string&) {
         auto status = client->GetFlightInfo(FlightDescriptor::Command("fail")).status();
         EXPECT_FALSE(status.ok());
         EXPECT_EQ(StatusCode::KeyError, status.code());
       },
       true,
       [](const Status& status) { EXPECT_EQ(StatusCode::KeyError, status.code()); }},
      {"unknown method", nullptr, false,
       [](const Status& status) {
         EXPECT_EQ(StatusCode::NotImplemented, status.code());
       }},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    auto trace = std::make_shared<MiddlewareTrace>();
    TestFlightServer flight_server;
    FlightServerOptions options(LocalhostLocation());
    options.middleware = {
        {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server, options));
    if (test.drive) {
      test.drive(client.get(), test.name);
    } else {
      ASSERT_OK_AND_ASSIGN(auto raw, CallRaw(flight_server.port(), kUnknownMethod, {}));
      EXPECT_EQ(::grpc::StatusCode::UNIMPLEMENTED, raw.status.error_code());
    }
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
    SCOPED_TRACE("after the server stopped");
    std::lock_guard<std::mutex> guard(trace->mutex);
    EXPECT_EQ(1, trace->start_call);
    ASSERT_EQ(1, trace->completed_statuses.size())
        << "CallCompleted must run exactly once per call";
    test.check_status(trace->completed_statuses[0]);
  }
}

namespace {

/// A cancellable context the async service handed out, for the cancel-registry
/// cases: the callback set is the reactor's, driven without a network.
struct CancelRegistry {
  explicit CancelRegistry(int* runs) : runs(runs) {}

  /// Register a callback that counts itself; returns what AddCancelCallback said.
  Status Add() {
    return AsyncGenericFlightServerBase::AddCancelCallback(context, [this] { ++*runs; });
  }

  ::grpc::CallbackServerContext grpc_context;
  detail::AsyncCallContext context{&grpc_context};
  int* runs;
};

}  // namespace

TEST(AsyncGrpcTest, CancelCallbackRegistry) {
  // Registration before the cancellation does not run early; a normal completion
  // runs none; the cancellation runs each exactly once; a registration after the
  // cancellation runs inline; and a callback that registers another during the
  // cancellation runs both, without deadlock.
  {
    int runs = 0;
    CancelRegistry registry(&runs);
    ASSERT_OK(registry.Add());
    ARROW_EXPECT_OK(registry.Add());
    EXPECT_EQ(0, runs) << "a registered callback must not run early";
    registry.context.RunCancelCallbacks();
    EXPECT_EQ(2, runs) << "each registered callback runs exactly once";
    registry.context.RunCancelCallbacks();
    EXPECT_EQ(2, runs) << "the cancellation runs only once";
  }
  {
    int runs = 0;
    CancelRegistry registry(&runs);
    ASSERT_OK(registry.Add());
    // A foreign context is refused, and its callback never runs.
    ForeignCallContext foreign;
    bool ran = false;
    const Status status =
        AsyncGenericFlightServerBase::AddCancelCallback(foreign, [&ran] { ran = true; });
    EXPECT_TRUE(status.IsInvalid()) << status;
    EXPECT_FALSE(ran);
  }
  {
    int runs = 0;
    CancelRegistry registry(&runs);
    // A callback registered once the cancellation already ran fires immediately.
    registry.context.RunCancelCallbacks();
    ASSERT_OK(registry.Add());
    EXPECT_EQ(1, runs) << "a late registration must run inline";
  }
  {
    int runs = 0;
    CancelRegistry registry(&runs);
    // One callback registers another: both run, on the cancelling thread.
    auto* context = &registry.context;
    ASSERT_OK(AsyncGenericFlightServerBase::AddCancelCallback(
        registry.context, [context, &runs] {
          ++runs;
          ARROW_EXPECT_OK(AsyncGenericFlightServerBase::AddCancelCallback(
              *context, [&runs] { ++runs; }));
        }));
    context->RunCancelCallbacks();
    EXPECT_EQ(2, runs) << "the nested registration must run too";
  }
}

TEST(AsyncGrpcTest, AsyncServerStartupLifecycle) {
  // An unsupported location scheme is refused by name, and a served RPC ends on
  // Shutdown().
  {
    AsyncGenericFlightServerBase server;
    ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("unknown://127.0.0.1:0"));
    const auto status = server.Init(FlightServerOptions(location));
    ASSERT_FALSE(status.ok());
    EXPECT_THAT(status.ToString(), ::testing::HasSubstr("No async server transport"));
  }
  {
    TestFlightServer server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&server));
    ASSERT_GT(server.port(), 0);
    EXPECT_GT(server.location().port(), 0);
    EXPECT_EQ(server.port(), server.location().port());
    ASSERT_OK(client->Close());
    ARROW_EXPECT_OK(server.Shutdown());
    ARROW_EXPECT_OK(server.Wait());
  }
}

TEST(AsyncGrpcTest, AsyncServerInitFailsWhenThePortIsTaken) {
  // Two servers on the same loopback port with SO_REUSEPORT off: the second Init()
  // fails with gRPC's own error, and the first keeps serving.
  auto allow_reuseport_off = [](void* opaque_builder) {
    static_cast<::grpc::ServerBuilder*>(opaque_builder)
        ->AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  };
  TestFlightServer first;
  FlightServerOptions first_options(LocalhostLocation());
  first_options.builder_hook = allow_reuseport_off;
  ASSERT_OK(first.Init(first_options));
  ASSERT_GT(first.port(), 0);

  TestFlightServer second;
  FlightServerOptions second_options(LocalhostLocation());
  second_options.builder_hook = allow_reuseport_off;
  // The second server must try the first one's port, which a fixed location cannot
  // express: bind it explicitly.
  ASSERT_OK_AND_ASSIGN(auto location, Location::ForGrpcTcp("127.0.0.1", first.port()));
  second_options.location = location;
  const auto status = second.Init(second_options);
  ASSERT_FALSE(status.ok()) << "two servers must not share a fixed port";
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Server did not start properly"));

  // The first server is unaffected.
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(first.port()));
  ASSERT_OK(client->GetFlightInfo(FlightDescriptor::Path({"alive"})).status());
  ASSERT_OK(StopClientAndServer(client.get(), &first));
}

}  // namespace arrow::flight::transport::grpc
