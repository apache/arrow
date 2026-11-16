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

  const ::grpc::Status status = CallUnknownMethod(hand_built.port);
  ASSERT_EQ(::grpc::StatusCode::UNIMPLEMENTED, status.error_code())
      << status.error_message();

  hand_built.server->Shutdown();
  hand_built.server->Wait();

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call) << "the middleware must have started for the call";
  EXPECT_EQ(1, trace->call_completed)
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
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"middleware-ticket"}));
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;
  }

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call);
  EXPECT_EQ(1, trace->sending_headers);
  EXPECT_EQ(1, trace->call_completed);
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagMiddlewareCanRejectCall) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {
      {"rejecting", std::make_shared<RejectingServerMiddlewareFactory>()}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto status = client->DoGet(Ticket{"rejected"}).status();
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

}  // namespace arrow::flight::transport::grpc
