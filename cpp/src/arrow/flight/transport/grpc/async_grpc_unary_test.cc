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

// The unary RPCs served by UnaryReactor (one request in, one response out):
// GetFlightInfo, GetSchema and PollFlightInfo, plus the rules they share -- a
// null answer maps to the sync transport's "Flight not found", a server error
// propagates, an answer may complete on another thread, and a handler future
// that never resolves must not leak its reactor.

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

// ---------------------------------------------------------------------------
// The unary trio: GetFlightInfo, GetSchema and PollFlightInfo share the
// UnaryReactor (one request in, one response out), so the trio has one
// happy-path test each plus one error-path test that pins the base class.
// ---------------------------------------------------------------------------

TEST(AsyncGrpcTest, GetFlightInfoIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto info,
                       client->GetFlightInfo(FlightDescriptor::Command("ping")));
  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto schema, info->GetSchema(&dict_memo));
  ASSERT_EQ(1, schema->num_fields());
  ASSERT_EQ("value", schema->field(0)->name());

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  ASSERT_EQ("ping", flight_server.last_descriptor().cmd);
}

TEST(AsyncGrpcTest, GetFlightInfoWithNullResponseIsNotFound) {
  // A handler that answers OK without setting the response: the sync transport
  // answers NOT_FOUND with "Flight not found" (grpc_server.cc:282-286), and the
  // async reactor must answer the same, not INTERNAL.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const auto status =
      client->GetFlightInfo(FlightDescriptor::Command("null-info")).status();
  ASSERT_RAISES(KeyError, status);
  ASSERT_THAT(status.message(), ::testing::HasSubstr("Flight not found"));

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

TEST(AsyncGrpcTest, GetSchemaIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto schema_result,
                       client->GetSchema(FlightDescriptor::Command("ping")));
  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto schema, schema_result->GetSchema(&dict_memo));
  ASSERT_EQ(1, schema->num_fields());
  ASSERT_EQ("value", schema->field(0)->name());

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  ASSERT_EQ("ping", flight_server.last_descriptor().cmd);
}

TEST(AsyncGrpcTest, PollFlightInfoIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto poll_info,
                       client->PollFlightInfo(FlightDescriptor::Command("ping")));
  ASSERT_TRUE(poll_info->progress.has_value());
  ASSERT_DOUBLE_EQ(0.5, *poll_info->progress);
  ASSERT_TRUE(poll_info->descriptor.has_value());
  ASSERT_EQ("poll-next", poll_info->descriptor->cmd);

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  ASSERT_EQ("ping", flight_server.last_descriptor().cmd);
}

// A GetFlightInfo handler whose future never resolves: the deadline is the
// only thing that can end the RPC.
class HangingUnaryServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
      const ServerCallContext&, const FlightDescriptor&) override {
    return arrow::Future<std::shared_ptr<FlightInfo>>::Make();
  }
};

TEST(AsyncGrpcTest, GetFlightInfoDeadlineWithPendingHandlerFinishes) {
  // The handler's future never resolves and the client's deadline ends the
  // RPC: the reactor must be released even though the handler never completed,
  // or Shutdown() below would block.
  const int64_t before = detail::AsyncReactorBase::live_reactors();

  HangingUnaryServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(500);
  auto status =
      client->GetFlightInfo(impatient, FlightDescriptor::Command("hang")).status();
  EXPECT_FALSE(status.ok()) << "the deadline must expire while the handler hangs";

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  EXPECT_TRUE(WaitFor([&] {
    return detail::AsyncReactorBase::live_reactors() == before;
  })) << "a reactor leaked: still alive after the server stopped (before="
      << before << ", now=" << detail::AsyncReactorBase::live_reactors() << ")";
}

TEST(AsyncGrpcTest, UnaryReactorPropagatesServerError) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // The server's KeyError must reach the client as a status failure carrying
  // the server's message, not as a successful (empty) response. The message is
  // matched by substring because this build defines ARROW_EXTRA_ERROR_CONTEXT,
  // which appends the call-site trail to the server's message.
  auto status = client->GetFlightInfo(FlightDescriptor::Command("fail")).status();
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(arrow::StatusCode::KeyError, status.code());
  ASSERT_THAT(status.message(), ::testing::HasSubstr("no such flight"));

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

TEST(AsyncGrpcTest, GetFlightInfoAsyncCompletesOnAnotherThread) {
  // The handler completes its future from a helper thread after the dispatch
  // has returned: the answer must arrive on the client anyway, which is the
  // whole point of the async handler.
  class DeferredInfoServer : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
        const ServerCallContext&, const FlightDescriptor& request) override {
      auto out = arrow::Future<std::shared_ptr<FlightInfo>>::Make();
      std::thread([out, request]() mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        auto info =
            FlightInfo::Make(*arrow::schema({arrow::field("value", arrow::int64())}),
                             request, std::vector<FlightEndpoint>{}, -1, -1);
        if (!info.ok()) {
          out.MarkFinished(info.status());
          return;
        }
        out.MarkFinished(std::make_shared<FlightInfo>(std::move(*info)));
      }).detach();
      return out;
    }
  };

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  DeferredInfoServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto info,
                       client->GetFlightInfo(FlightDescriptor::Command("deferred")));
  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto schema, info->GetSchema(&dict_memo));
  ASSERT_EQ(1, schema->num_fields());
  ASSERT_EQ("value", schema->field(0)->name());

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

}  // namespace arrow::flight::transport::grpc
