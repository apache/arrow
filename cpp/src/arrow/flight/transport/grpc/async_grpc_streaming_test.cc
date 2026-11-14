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

// The streaming RPCs served by StreamingReactor (one request in, N responses
// out, driven by OnWriteDone): ListActions (a vector), DoAction (an
// AsyncResultStream) and ListFlights (an AsyncFlightListing). Covers the
// empty-stream case (no write is ever armed), the mid-stream error rule, a
// listing completed on another thread, and a handler deadline.

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

TEST(AsyncGrpcTest, ListActionsIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto actions, client->ListActions());
  ASSERT_EQ(actions.size(), 2);
  ASSERT_EQ(actions[0].type, "echo");
  ASSERT_EQ(actions[0].description, "echo back");
  ASSERT_EQ(actions[1].type, "fail");
  ASSERT_EQ(actions[1].description, "always fails");

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, ListActionsEmptyFinishesCleanly) {
  // Zero response messages: WriteNext() finds the vector exhausted before it
  // ever calls StartWrite(), so the RPC must be finished with OK rather than
  // left waiting for a write completion that will never arrive.
  TestFlightServer flight_server;
  flight_server.set_empty_actions();
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto actions, client->ListActions());
  ASSERT_TRUE(actions.empty());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoActionIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const Action action{"ping", Buffer::FromString("request-body")};
  ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(action));
  ASSERT_OK_AND_ASSIGN(auto result, results->Next());
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->body->ToString(), "one");
  // The stream ends with a null Result, not with an error.
  ASSERT_OK_AND_ASSIGN(auto end, results->Next());
  ASSERT_EQ(end, nullptr);

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ("ping", flight_server.last_action_type());
}

TEST(AsyncGrpcTest, DoActionStreamsMultipleResults) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const Action action{"multi", Buffer::FromString("")};
  ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(action));

  ASSERT_OK_AND_ASSIGN(auto bodies, DrainResultBodies(results.get()));
  ASSERT_EQ(bodies, (std::vector<std::string>{"one", "two", "three"}));

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoActionMidStreamErrorFinishesTheRpc) {
  // The stream yields one Result and then fails. The error must become the
  // RPC status -- the client must not see a truncated success.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const Action action{"fail-mid-stream", Buffer::FromString("")};
  ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(action));
  ASSERT_OK_AND_ASSIGN(auto first, results->Next());
  ASSERT_NE(first, nullptr);
  ASSERT_EQ(first->body->ToString(), "first");

  auto second = results->Next();
  ASSERT_FALSE(second.ok()) << "the mid-stream error must reach the client";
  ASSERT_EQ(second.status().code(), arrow::StatusCode::IOError);
  ASSERT_THAT(second.status().message(),
              ::testing::HasSubstr("mid-stream action failure"));

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoActionWithNullResultStreamIsCancelled) {
  // A handler that answers OK without a ResultStream answers CANCELLED on the
  // sync transport (grpc_server.cc:404-406), before any message is written, the
  // async reactor must answer the same. The client may see it on DoAction() or
  // on the first Next(), so both surfaces are accepted.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const Action action{"null-result-stream", Buffer::FromString("")};
  auto results = client->DoAction(action);
  if (results.ok()) {
    // The stream was handed out before the status arrived: CANCELLED is then
    // reported by the first read.
    ASSERT_RAISES(Cancelled, (*results)->Next());
  } else {
    ASSERT_RAISES(Cancelled, results.status());
  }

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, ListFlightsIsServedAsync) {
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  const Criteria criteria("filter-me");
  ASSERT_OK_AND_ASSIGN(auto listing, client->ListFlights({}, criteria));

  ASSERT_OK_AND_ASSIGN(auto paths, DrainListingPaths(listing.get()));
  ASSERT_EQ(paths, (std::vector<std::vector<std::string>>{{"first-listing"},
                                                          {"second-listing"}}));

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ("filter-me", flight_server.last_criteria().expression);
}

TEST(AsyncGrpcTest, ListFlightsEmptyFinishesCleanly) {
  // A listing with no flights: the server answers OK with zero messages, and
  // the client sees an exhausted listing rather than a hang.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto listing, client->ListFlights({}, Criteria("")));
  ASSERT_OK_AND_ASSIGN(auto info, listing->Next());
  ASSERT_EQ(info, nullptr);
  // The client sent no criteria, the server must still have received the
  // (empty) Criteria, exactly as the sync handler does.
  ASSERT_EQ("", flight_server.last_criteria().expression);

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, ListFlightsMidStreamErrorFinishesTheRpc) {
  // A listing that fails on its second Next(): the error must become the RPC
  // status, not a silently truncated success.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto listing = client->ListFlights({}, Criteria("fail-mid-stream"));
  ASSERT_FALSE(listing.ok()) << "the mid-stream error must reach the client";
  ASSERT_EQ(listing.status().code(), arrow::StatusCode::IOError);
  ASSERT_THAT(listing.status().message(),
              ::testing::HasSubstr("mid-stream listing failure"));

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, ListFlightsAsyncCompletesOnAnotherThread) {
  // The listing object itself arrives on a helper thread: the transport must
  // serve the response from wherever the future resolves.
  class DeferredListingServer : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
        const ServerCallContext&, const Criteria*) override {
      auto out = arrow::Future<std::shared_ptr<AsyncFlightListing>>::Make();
      std::thread([out]() mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        std::vector<FlightInfo> flights;
        flights.push_back(
            std::move(*MakeTestFlightInfo(FlightDescriptor::Path({"deferred"}))));
        out.MarkFinished(std::make_shared<SimpleListingAdapter>(std::move(flights)));
      }).detach();
      return out;
    }
  };

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  DeferredListingServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto listing, client->ListFlights());
  int seen = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto info, listing->Next());
    if (info == nullptr) break;
    ++seen;
  }
  ASSERT_EQ(1, seen);

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

// A ListFlights handler whose future never resolves.
class HangingListingServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
      const ServerCallContext&, const Criteria*) override {
    return arrow::Future<std::shared_ptr<AsyncFlightListing>>::Make();
  }
};

TEST(AsyncGrpcTest, ListFlightsDeadlineWithPendingHandlerFinishes) {
  // The handler's future never resolves and the client's deadline ends the RPC:
  // the reactor must be released even though the handler never completes.
  const int64_t before = detail::AsyncReactorBase::live_reactors();

  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  HangingListingServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(500);
  auto listing = client->ListFlights(impatient, Criteria("hang"));
  EXPECT_FALSE(listing.ok()) << "the deadline must expire while the handler hangs";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ExpectNoReactorLeak(before);
}

}  // namespace arrow::flight::transport::grpc
