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
  AsyncGenericFlightService service(&flight_server, helper);

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:0", ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  const ::grpc::Status status = CallUnknownMethod(port);
  ASSERT_EQ(::grpc::StatusCode::UNIMPLEMENTED, status.error_code())
      << status.error_message();

  server->Shutdown();
  server->Wait();

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call) << "the middleware must have started for the call";
  EXPECT_EQ(1, trace->call_completed)
      << "CallCompleted must run for an UNIMPLEMENTED reply too";
}

TEST(AsyncGrpcTest, OtherMethodsAreUnimplemented) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:0", ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  std::string uri = "grpc://localhost:" + std::to_string(port);
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(auto client, FlightClient::Connect(location));

  // Every typed FlightService method is served now, the fallback reactor is
  // still reachable through a method name no stub knows (which maps to
  // FlightMethod::Invalid in MethodFromName), and that is the only pin left
  // that an unknown method cannot be answered by a typed path.
  const ::grpc::Status status = CallUnknownMethod(port);
  ASSERT_FALSE(status.ok()) << "an unknown method must not be served";
  ASSERT_EQ(::grpc::StatusCode::UNIMPLEMENTED, status.error_code())
      << status.error_message();

  ASSERT_OK(client->Close());
  server->Shutdown();
  server->Wait();
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlag) {
  ASSERT_OK_AND_ASSIGN(auto default_location, Location::Parse("grpc://localhost:0"));
  FlightServerOptions default_options(default_location);

  // Same adapter as the tests above, but now started through the async
  // server's own Init instead of a hand-built grpc::ServerBuilder.
  TestFlightServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // (i) DoGet went through the generic reactor: the ticket reached
  // TestFlightServer::DoGetAsync and the whole stream came back.
  Ticket ticket{"flag-path-do-get-ticket"};
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(ticket));
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  ASSERT_EQ(schema->num_fields(), 2);
  ASSERT_EQ(schema->field(0)->name(), "a");
  ASSERT_EQ(schema->field(1)->name(), "b");
  int batch_count = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;
    ASSERT_EQ(chunk.data->num_rows(), 5);
    ASSERT_OK_AND_ASSIGN(auto value, chunk.data->column(0)->GetScalar(0));
    ASSERT_TRUE(value->Equals(*arrow::MakeScalar(int64_t(1))));
    batch_count++;
  }
  ASSERT_EQ(batch_count, 3);

  // (ii) Every typed method is served on this path too, the fallback reactor
  // still answers a method name no stub knows, which is what pins "the typed
  // sync service is NOT registered alongside the generic one".
  const ::grpc::Status unknown_status = CallUnknownMethod(flight_server.port());
  ASSERT_FALSE(unknown_status.ok()) << "an unknown method must not be served";
  ASSERT_EQ(::grpc::StatusCode::UNIMPLEMENTED, unknown_status.error_code())
      << unknown_status.error_message();

  // (iii) The async server takes no listener factory here, so uploads are
  // refused the same way.
  FlightDescriptor descriptor = FlightDescriptor::Path({"not-served"});
  auto put_schema = arrow::schema({arrow::field("a", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(put_schema, 1,
                                        {arrow::ArrayFromJSON(arrow::int64(), "[1]")});
  auto [surface, status] = UploadOneBatch(client.get(), descriptor, put_schema, batch);
  ASSERT_FALSE(status.ok()) << "DoPut must be refused, surfaced on " << surface;
  ASSERT_EQ(status.code(), arrow::StatusCode::NotImplemented);

  // Cleanup. The ticket is asserted only after the server stopped, so the
  // DoGet callback cannot race with the read.
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  ASSERT_EQ(flight_server.ticket(), "flag-path-do-get-ticket");
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagServesDoPut) {
  // The async server serves uploads because the server class hands out the
  // per-RPC listener through CreateDoPutListener(), instead of the RPC falling
  // through to UNIMPLEMENTED.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));
  FlightServerOptions options(location);
  flight_server.set_listener(listener);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
  auto batch =
      arrow::RecordBatch::Make(schema, 3,
                               {arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3]"),
                                arrow::ArrayFromJSON(arrow::int64(), "[10, 20, 30]")});

  const FlightDescriptor descriptor = FlightDescriptor::Path({"wave", "h2"});
  auto [surface, status] = UploadOneBatch(client.get(), descriptor, schema, batch);
  ASSERT_TRUE(status.ok()) << "upload failed on " << surface << ": " << status;

  // Cleanup before looking at the listener: its callbacks run on a gRPC
  // thread, and nothing may read the recordings while that thread writes.
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The listener saw the descriptor the client sent, the schema, and the batch.
  ASSERT_EQ(listener->descriptor_count(), 1);
  ASSERT_EQ(listener->descriptors()[0], descriptor);
  ASSERT_EQ(listener->schema_count(), 1);
  ASSERT_EQ(listener->schema()->num_fields(), 2);
  ASSERT_EQ(listener->schema()->field(0)->name(), "a");
  ASSERT_EQ(listener->schema()->field(1)->name(), "b");
  ASSERT_EQ(listener->batches().size(), 1);
  const std::shared_ptr<arrow::RecordBatch> chunk = listener->batches()[0];
  ASSERT_EQ(chunk->num_rows(), 3);
  ASSERT_EQ(chunk->num_columns(), 2);
  ASSERT_OK_AND_ASSIGN(auto value, chunk->column(0)->GetScalar(0));
  ASSERT_TRUE(value->Equals(*arrow::MakeScalar(int64_t(1))));
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagRefusesBlockingAuthHandler) {
  // The callback path cannot run the blocking ServerAuthHandler (it would run
  // on callback threads), so a server configured with one must refuse to start
  // instead of silently serving unauthenticated requests. Middleware *is*
  // supported now - see UseAsyncGrpcFlagAllowsMiddleware.
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));

  TestFlightServer auth_server;
  FlightServerOptions with_auth(location);
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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));

  TestFlightServer flight_server;
  FlightServerOptions options(location);
  options.middleware = {
      {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
  ASSERT_OK(flight_server.Init(options));

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"middleware-ticket"}));
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;
  }

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->start_call);
  EXPECT_EQ(1, trace->sending_headers);
  EXPECT_EQ(1, trace->call_completed);
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagMiddlewareCanRejectCall) {
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse("grpc://localhost:0"));
  TestFlightServer flight_server;
  FlightServerOptions options(location);
  options.middleware = {
      {"rejecting", std::make_shared<RejectingServerMiddlewareFactory>()}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto status = client->DoGet(Ticket{"rejected"}).status();
  EXPECT_FALSE(status.ok());
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("rejected by middleware"));

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

}  // namespace arrow::flight::transport::grpc
