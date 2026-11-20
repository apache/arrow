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

TEST(AsyncGrpcTest, DoActionStreamsMultipleResults) {
  // One RPC, several responses in order, then a clean null end: the request's
  // type must also reach the handler unchanged.
  TestFlightServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));

  const Action action{"multi", Buffer::FromString("request-body")};
  ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(action));

  ASSERT_OK_AND_ASSIGN(auto bodies, DrainResultBodies(results.get()));
  EXPECT_EQ(bodies, (std::vector<std::string>{"one", "two", "three"}));
  // The stream ends with a null Result, not with an error.
  ASSERT_OK_AND_ASSIGN(auto end, results->Next());
  EXPECT_EQ(end, nullptr);

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ("multi", flight_server.last_action_type());
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
  // The client sent no criteria, the server must still have received a
  // non-null (empty) Criteria, exactly as the sync handler does. Asserted
  // after the server stopped, so the handler has provably run.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_FALSE(flight_server.criteria_was_null())
      << "the handler was handed a null Criteria";
  EXPECT_EQ("", flight_server.last_criteria().expression);
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
  // The listing Future resolves on a thread the test joins after the handler has
  // been entered: the transport must serve the response from wherever the future
  // resolves, not only from the gRPC callback that dispatched it.
  class DeferredListingServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
        const ServerCallContext&, const Criteria*) override {
      reply = arrow::Future<std::shared_ptr<AsyncFlightListing>>::Make();
      worker = std::thread([this] {
        ARROW_UNUSED(entered.Wait(10));
        std::vector<FlightInfo> flights;
        flights.push_back(MakeTestFlightInfo(FlightDescriptor::Path({"deferred"})));
        reply.MarkFinished(std::make_shared<SimpleListingAdapter>(std::move(flights)));
      });
      entered.MarkFinished();
      return reply;
    }

    void Join() {
      if (worker.joinable()) worker.join();
    }

    Future<> entered = Future<>::Make();
    Future<std::shared_ptr<AsyncFlightListing>> reply;

   private:
    std::thread worker;
  };

  DeferredListingServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  FlightCallOptions bounded;
  bounded.timeout = std::chrono::seconds(10);
  auto listing = client->ListFlights(bounded, Criteria{"deferred"});
  flight_server.Join();
  ASSERT_OK(listing) << listing.status();
  ASSERT_OK_AND_ASSIGN(auto paths, DrainListingPaths(listing->get()));
  EXPECT_EQ(paths, (std::vector<std::vector<std::string>>{{"deferred"}}));

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

  HangingListingServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(500);
  auto listing = client->ListFlights(impatient, Criteria("hang"));
  EXPECT_FALSE(listing.ok()) << "the deadline must expire while the handler hangs";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ExpectNoReactorLeak(before);
}

// Serves DoAction and ListFlights with a stream that yields one item, then
// parks its second NextAsync() (called from OnWriteDone, on a gRPC thread)
// until the RPC is cancelled, and records whether the stream was destroyed
// meanwhile.
class CancelDuringNextServer final : public AsyncGenericFlightServerBase {
 public:
  struct State {
    /// Completed by the cancel callback: the RPC was cancelled.
    arrow::Future<> cancelled = arrow::Future<>::Make();
    /// Completed by the test once it saw the demand running: release the stream.
    arrow::Future<> release = arrow::Future<>::Make();
    /// Completed when the parked NextAsync() returns.
    arrow::Future<> parked_next_returned = arrow::Future<>::Make();
    /// Completed when the RPC's terminal status is settled.
    arrow::Future<> finished = arrow::Future<>::Make();
    std::atomic<bool> destroyed{false};
    std::atomic<bool> destroyed_during_next{false};
  };

  template <typename Stream, typename Item>
  class ParkingStream final : public Stream {
   public:
    ParkingStream(std::shared_ptr<State> state, Item first)
        : state_(std::move(state)), first_(std::move(first)) {}
    ~ParkingStream() override { state_->destroyed = true; }

    arrow::Future<std::shared_ptr<Item>> NextAsync() override {
      if (!served_) {
        served_ = true;
        return std::make_shared<Item>(first_);
      }
      // `this` may be freed while parked: only the local copy is used below. The
      // test releases the gate from its own thread, so no callback thread waits
      // on a deadline.
      auto state = state_;
      state->parked_next_returned.MarkFinished();
      state->release.Wait(10);
      state->destroyed_during_next = state->destroyed.load();
      return std::shared_ptr<Item>();
    }

   private:
    std::shared_ptr<State> state_;
    Item first_;
    bool served_ = false;
  };

  arrow::Future<std::shared_ptr<AsyncResultStream>> DoActionAsync(
      const ServerCallContext& context, const Action&) override {
    return Park<AsyncResultStream>(context, Result{Buffer::FromString("first")});
  }

  arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
      const ServerCallContext& context, const Criteria*) override {
    return Park<AsyncFlightListing>(
        context, MakeTestFlightInfo(FlightDescriptor::Path({"first"})));
  }

  std::shared_ptr<State> state_ = std::make_shared<State>();

 private:
  template <typename Stream, typename Item>
  arrow::Future<std::shared_ptr<Stream>> Park(const ServerCallContext& context,
                                              Item first) {
    ARROW_RETURN_NOT_OK(AddCancelCallback(
        context, [state = state_] { state->cancelled.MarkFinished(); }));
    return arrow::Future<std::shared_ptr<Stream>>::MakeFinished(
        std::make_shared<ParkingStream<Stream, Item>>(state_, std::move(first)));
  }
};

/// Which streaming RPC's reactor the deterministic cancel-during-NextAsync case
/// drives.
enum class CancelDuringNextKind { kDoAction, kListFlights };

/// Cancel a streaming RPC while its reactor is inside the application's
/// NextAsync(): the cancellation drops the reactor's reference to the stream, and
/// the stream must outlive the call it is in.
///
/// Driven by hand, on a real reactor and a ManualServerStream: a joined worker
/// makes the demand, the test cancels on this thread once the demand is provably
/// running, then releases it. No deadline, no gRPC callback thread blocked on a
/// gate, and the entry of the intended method is asserted rather than assumed.
void ExpectStreamOutlivesCancelDuringNext(CancelDuringNextKind kind) {
  auto state = std::make_shared<CancelDuringNextServer::State>();
  TestFlightServer base;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext flight_context(&stream.context());
  helper->MakeCallContext(kind == CancelDuringNextKind::kDoAction
                              ? FlightMethod::DoAction
                              : FlightMethod::ListFlights,
                          &stream.context(), &flight_context);
  CancelDuringNextServer server;
  server.state_ = state;

  ::grpc::ServerGenericBidiReactor* reactor =
      kind == CancelDuringNextKind::kDoAction
          ? detail::MakeDoActionReactor(std::move(flight_context), &server)
          : detail::MakeListFlightsReactor(std::move(flight_context), &server);
  stream.Bind(reactor);

  // The request: a DoAction needs its action, ListFlights passes no criteria.
  if (kind == CancelDuringNextKind::kDoAction) {
    ::grpc::Slice slice(WireBytes(Action{"park", nullptr}));
    stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  } else {
    ::grpc::Slice slice(WireBytes(Criteria{}));
    stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  }

  // The first item is written by the reactor; completing that write runs
  // OnWriteDone, which asks for the second item and parks in the stream. The write
  // completion runs on a joined worker so this thread is free to cancel while the
  // demand is provably in progress.
  ASSERT_TRUE(stream.write_pending()) << "the first item was never written";
  std::thread worker([&stream, state] {
    stream.CompleteWrite(true);
    // The reactor's continuation returns here, once the parked demand is released.
  });

  ASSERT_TRUE(state->parked_next_returned.Wait(10))
      << "the reactor never asked for its second item";

  // The cancellation runs on this thread, exactly as gRPC would deliver it.
  stream.Cancel();
  EXPECT_TRUE(state->cancelled.Wait(10)) << "the cancel callback never ran";
  state->release.MarkFinished();
  worker.join();
  stream.DrainWrites();

  EXPECT_FALSE(state->destroyed_during_next)
      << "the stream was destroyed while its own NextAsync() was running";
  ASSERT_TRUE(stream.finish_observed().Wait(10)) << "the RPC never finished";

  // Settle the stream and drop the local state: the reactor's last reference is
  // released once the callbacks are drained.
  if (stream.read_pending()) {
    stream.CloseRead();
  }
  stream.DrainWrites();
  if (!stream.done()) {
    stream.Done();
  }
}

TEST(AsyncGrpcTest, DoActionCancelDuringNextAsyncKeepsTheStreamAlive) {
  ExpectStreamOutlivesCancelDuringNext(CancelDuringNextKind::kDoAction);
}

TEST(AsyncGrpcTest, ListFlightsCancelDuringNextAsyncKeepsTheListingAlive) {
  ExpectStreamOutlivesCancelDuringNext(CancelDuringNextKind::kListFlights);
}

TEST(AsyncGrpcTest, ListActionsHandlerErrorFinishesTheRpc) {
  // The handler itself fails, before any action is written: the client must see
  // that error rather than an empty successful listing.
  TestFlightServer flight_server;
  flight_server.set_failing_actions();
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  const auto status = client->ListActions().status();
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("no such actions"));
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, StreamingFirstDemandFailureFinishesTheRpc) {
  // A stream whose very first NextAsync() fails: the RPC ends with that error
  // before anything is written, for both stream kinds.
  {
    SCOPED_TRACE("DoAction");
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    const auto result =
        client->DoAction(Action{"fail-first-next", Buffer::FromString("")});
    if (result.ok()) {
      const auto first = (*result)->Next();
      ASSERT_FALSE(first.ok());
      EXPECT_EQ(StatusCode::IOError, first.status().code()) << first.status();
      EXPECT_THAT(first.status().ToString(), ::testing::HasSubstr("first demand failed"));
    } else {
      EXPECT_THAT(result.status().ToString(), ::testing::HasSubstr("first demand failed"))
          << result.status();
    }
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
  {
    SCOPED_TRACE("ListFlights");
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    const auto listing = client->ListFlights({}, Criteria("fail-first-next"));
    ASSERT_FALSE(listing.ok());
    EXPECT_THAT(listing.status().ToString(), ::testing::HasSubstr("first demand failed"))
        << listing.status();
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
}

TEST(AsyncGrpcTest, StreamingEmptyStreamIsASuccess) {
  // An empty DoAction stream is a successful RPC with zero messages: distinct from a
  // null stream, which is Cancelled. An empty vector listing is likewise a success.
  {
    SCOPED_TRACE("empty DoAction stream");
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    ASSERT_OK_AND_ASSIGN(auto results, client->DoAction(Action{"empty-result-stream",
                                                               Buffer::FromString("")}));
    ASSERT_OK_AND_ASSIGN(auto end, results->Next());
    EXPECT_EQ(nullptr, end);
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
  {
    SCOPED_TRACE("empty ListFlights listing");
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    ASSERT_OK_AND_ASSIGN(auto listing, client->ListFlights({}, Criteria("")));
    ASSERT_OK_AND_ASSIGN(auto end, listing->Next());
    EXPECT_EQ(nullptr, end);
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
}

TEST(AsyncGrpcTest, StreamingWriteFailureFailsTheRpc) {
  // A response gRPC could not write ends the RPC with the transport's write error,
  // not a truncated success. Driven by hand so the failure is exact.
  struct Case {
    std::string name;
    FlightMethod method;
    std::function<::grpc::ServerGenericBidiReactor*(detail::AsyncCallContext,
                                                    TestFlightServer*)>
        make;
    std::function<void(::grpc::Slice*)> request;
  };
  const std::vector<Case> cases = {
      {"ListActions", FlightMethod::ListActions,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeListActionsReactor(std::move(context), server);
       },
       [](::grpc::Slice* slice) { *slice = ::grpc::Slice(""); }},
      {"DoAction", FlightMethod::DoAction,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeDoActionReactor(std::move(context), server);
       },
       [](::grpc::Slice* slice) {
         *slice = ::grpc::Slice(WireBytes(Action{"echo", nullptr}));
       }},
      {"ListFlights", FlightMethod::ListFlights,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeListFlightsReactor(std::move(context), server);
       },
       [](::grpc::Slice* slice) {
         // A non-empty expression, or the handler serves an empty listing and the
         // RPC would finish before writing anything.
         *slice = ::grpc::Slice(WireBytes(Criteria{"filter"}));
       }},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    TestFlightServer server;
    ManualServerStream stream;
    auto helper =
        std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
            /*auth_handler=*/nullptr, MiddlewareFactoryList{});
    detail::AsyncCallContext context(&stream.context());
    helper->MakeCallContext(test.method, &stream.context(), &context);
    stream.Bind(test.make(std::move(context), &server));

    ::grpc::Slice request;
    test.request(&request);
    stream.DeliverRead(::grpc::ByteBuffer(&request, 1));
    // The handler's continuation may run after OnReadDone returns, so wait on the
    // observable outcome: the first response on the wire.
    ASSERT_TRUE(WaitFor([&] { return stream.write_pending(); }))
        << "the first response was never written";
    stream.CompleteWrite(false);
    ASSERT_TRUE(stream.finish_observed().Wait(10));
    EXPECT_EQ(::grpc::StatusCode::INTERNAL, stream.finish_status()->error_code())
        << stream.finish_status()->error_message();
    EXPECT_THAT(stream.finish_status()->error_message(),
                ::testing::HasSubstr("Failed to write response"));
    EXPECT_FALSE(stream.write_pending()) << "a failed write must not be retried";
    if (stream.read_pending()) stream.CloseRead();
    stream.DrainWrites();
    if (!stream.done()) stream.Done();
  }
}

TEST(AsyncGrpcTest, ListActionsCancelUsesTheDefaultForgetStream) {
  // ListActions has no application iterator to drop, so its cancellation runs the
  // base class's default ForgetStream(). It must still settle the RPC as cancelled and
  // leave no reactor behind.
  const int64_t before = detail::AsyncReactorBase::live_reactors();
  TestFlightServer server;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext context(&stream.context());
  helper->MakeCallContext(FlightMethod::ListActions, &stream.context(), &context);
  stream.Bind(detail::MakeListActionsReactor(std::move(context), &server));

  ::grpc::Slice slice("");
  stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  // The reactor is inside its first write when the client goes away.
  ASSERT_TRUE(WaitFor([&] { return stream.write_pending(); }));
  stream.Cancel();
  ASSERT_TRUE(stream.finish_observed().Wait(10));
  EXPECT_EQ(::grpc::StatusCode::CANCELLED, stream.finish_status()->error_code())
      << stream.finish_status()->error_message();
  if (stream.read_pending()) stream.CloseRead();
  stream.DrainWrites();
  if (!stream.done()) stream.Done();
  ExpectNoReactorLeak(before);
}

TEST(AsyncGrpcTest, ListActionsCancelWithNoWritePendingSettles) {
  // The same default ForgetStream() path, reached when nothing is on the wire: the
  // cancellation must finish the RPC without starting a write or demanding anything.
  TestFlightServer server;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext context(&stream.context());
  helper->MakeCallContext(FlightMethod::ListActions, &stream.context(), &context);
  stream.Bind(detail::MakeListActionsReactor(std::move(context), &server));

  stream.Cancel();
  ASSERT_TRUE(stream.finish_observed().Wait(10));
  EXPECT_EQ(::grpc::StatusCode::CANCELLED, stream.finish_status()->error_code());
  EXPECT_FALSE(stream.write_pending()) << "a cancel must not start a write";
  if (stream.read_pending()) stream.CloseRead();
  if (!stream.done()) stream.Done();
}

}  // namespace arrow::flight::transport::grpc
