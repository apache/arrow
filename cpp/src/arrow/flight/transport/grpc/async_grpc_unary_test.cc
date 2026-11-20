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
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

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

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
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
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

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
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

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

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ExpectNoReactorLeak(before);
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

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, GetFlightInfoAsyncCompletesOnAnotherThread) {
  // The handler completes its future after the dispatch has returned, from a
  // thread the test joins: the answer must arrive on the client after the
  // dispatch, which is the whole point of the async handler.
  class DeferredInfoServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
        const ServerCallContext&, const FlightDescriptor& request) override {
      reply = arrow::Future<std::shared_ptr<FlightInfo>>::Make();
      auto info =
          FlightInfo::Make(*arrow::schema({arrow::field("value", arrow::int64())}),
                           request, std::vector<FlightEndpoint>{}, -1, -1);
      // Bounded on both sides: the client's deadline fails its call if the test
      // thread never gets here, and the test fails if the handler is never
      // entered.
      worker = std::thread([this, info = std::move(info)]() mutable {
        ARROW_UNUSED(entered.Wait(10));
        reply.MarkFinished(std::make_shared<FlightInfo>(std::move(info).ValueOrDie()));
      });
      entered.MarkFinished();
      return reply;
    }

    void Join() {
      if (worker.joinable()) worker.join();
    }

    Future<> entered = Future<>::Make();
    Future<std::shared_ptr<FlightInfo>> reply;

   private:
    std::thread worker;
  };

  DeferredInfoServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  FlightCallOptions bounded;
  bounded.timeout = std::chrono::seconds(10);
  auto info = client->GetFlightInfo(bounded, FlightDescriptor::Command("deferred"));
  flight_server.Join();
  ASSERT_OK(info) << info.status();
  ipc::DictionaryMemo dict_memo;
  ASSERT_OK_AND_ASSIGN(auto schema, (*info)->GetSchema(&dict_memo));
  ASSERT_EQ(1, schema->num_fields());
  EXPECT_EQ("value", schema->field(0)->name());
  EXPECT_EQ("deferred", (*info)->descriptor().cmd);

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, LateHandlerCompletion) {
  // A handler that completes its future after the RPC is over must neither
  // overwrite the cancellation nor start new I/O. The three unary RPCs have
  // different result types and handlers, so each gets its own row.
  struct Case {
    std::string name;
    std::string method;
    std::function<void(AsyncGenericFlightServerBase*, Future<>&)> park;
    std::function<void(const Future<>&, bool)> resolve;
  };

  // A small server per row: the handler parks the future the test holds.
  class LateUnaryServer final : public AsyncGenericFlightServerBase {
   public:
    explicit LateUnaryServer(std::function<void(LateUnaryServer*)> on_call)
        : on_call_(std::move(on_call)) {}

    /// A fresh latch for the call the test is about to make.
    void reset_entered() { entered = Future<>::Make(); }

    arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
        const ServerCallContext&, const FlightDescriptor&) override {
      info = Future<std::shared_ptr<FlightInfo>>::Make();
      on_call_(this);
      return info;
    }
    arrow::Future<std::shared_ptr<SchemaResult>> GetSchemaAsync(
        const ServerCallContext&, const FlightDescriptor&) override {
      schema = Future<std::shared_ptr<SchemaResult>>::Make();
      on_call_(this);
      return schema;
    }
    arrow::Future<std::shared_ptr<PollInfo>> PollFlightInfoAsync(
        const ServerCallContext&, const FlightDescriptor&) override {
      poll = Future<std::shared_ptr<PollInfo>>::Make();
      on_call_(this);
      return poll;
    }

    // Recreated per call: each row parks the future its own handler returned.
    Future<std::shared_ptr<FlightInfo>> info;
    Future<std::shared_ptr<SchemaResult>> schema;
    Future<std::shared_ptr<PollInfo>> poll;

    Future<> entered = Future<>::Make();

   private:
    std::function<void(LateUnaryServer*)> on_call_;
  };

  for (const bool succeeds : {true, false}) {
    SCOPED_TRACE(succeeds ? "late success" : "late error");
    const int64_t before = detail::AsyncReactorBase::live_reactors();
    LateUnaryServer flight_server(
        [](LateUnaryServer* server) { server->entered.MarkFinished(); });

    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    FlightCallOptions impatient;
    impatient.timeout = std::chrono::milliseconds(500);
    // All three RPCs against the same server: each parks, each is cancelled by its
    // deadline, then each completes late.
    struct Row {
      std::string name;
      std::function<Status(FlightClient*, const FlightCallOptions&)> call;
      std::function<void(LateUnaryServer*, bool)> finish;
    };
    const std::vector<Row> rows = {
        {"GetFlightInfo",
         [](FlightClient* c, const FlightCallOptions& o) {
           return c->GetFlightInfo(o, FlightDescriptor::Command("late")).status();
         },
         [](LateUnaryServer* s, bool ok) {
           if (ok) {
             s->info.MarkFinished(std::make_shared<FlightInfo>(
                 MakeTestFlightInfo(FlightDescriptor::Command("late"))));
           } else {
             s->info.MarkFinished(Status::Invalid("late failure"));
           }
         }},
        {"GetSchema",
         [](FlightClient* c, const FlightCallOptions& o) {
           return c->GetSchema(o, FlightDescriptor::Command("late")).status();
         },
         [](LateUnaryServer* s, bool ok) {
           if (ok) {
             s->schema.MarkFinished(std::shared_ptr<SchemaResult>(
                 SchemaResult::Make(*OneColumnSchema()).ValueOrDie().release()));
           } else {
             s->schema.MarkFinished(Status::Invalid("late failure"));
           }
         }},
        {"PollFlightInfo",
         [](FlightClient* c, const FlightCallOptions& o) {
           return c->PollFlightInfo(o, FlightDescriptor::Command("late")).status();
         },
         [](LateUnaryServer* s, bool ok) {
           if (ok) {
             auto poll = std::make_shared<PollInfo>();
             poll->descriptor = FlightDescriptor::Command("poll");
             s->poll.MarkFinished(std::move(poll));
           } else {
             s->poll.MarkFinished(Status::Invalid("late failure"));
           }
         }},
    };

    for (const auto& row : rows) {
      SCOPED_TRACE(row.name);
      flight_server.reset_entered();
      const auto status = row.call(client.get(), impatient);
      ASSERT_FALSE(status.ok()) << "the deadline must end the call";
      ASSERT_TRUE(flight_server.entered.Wait(10)) << "the handler never ran";
      row.finish(&flight_server, succeeds);
      // The late completion must not change what the client saw, and must release
      // its reference to the reactor.
      EXPECT_FALSE(status.ok());
    }

    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
    ExpectNoReactorLeak(before);
  }
}

TEST(AsyncGrpcTest, UnaryReplyOutcomes) {
  // A unary handler answers with an error, a null reply, or a reply whose write then
  // fails: each must reach the client as that failure, with no invented response.
  struct Case {
    std::string name;
    std::function<Status(FlightClient*)> call;
    std::function<void(const Status&)> check;
  };
  const std::vector<Case> cases = {
      {"GetSchema error",
       [](FlightClient* c) {
         return c->GetSchema(FlightDescriptor::Command("fail")).status();
       },
       [](const Status& status) {
         EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
         EXPECT_THAT(status.ToString(), ::testing::HasSubstr("no such schema"));
       }},
      {"GetSchema null reply",
       [](FlightClient* c) {
         return c->GetSchema(FlightDescriptor::Command("null-schema")).status();
       },
       [](const Status& status) {
         EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
         EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Flight not found"));
       }},
      {"PollFlightInfo error",
       [](FlightClient* c) {
         return c->PollFlightInfo(FlightDescriptor::Command("fail")).status();
       },
       [](const Status& status) {
         EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
         EXPECT_THAT(status.ToString(), ::testing::HasSubstr("no such poll"));
       }},
      {"PollFlightInfo null reply",
       [](FlightClient* c) {
         return c->PollFlightInfo(FlightDescriptor::Command("null-poll")).status();
       },
       [](const Status& status) {
         EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
         EXPECT_THAT(status.ToString(), ::testing::HasSubstr("Flight not found"));
       }},
      {"GetFlightInfo error",
       [](FlightClient* c) {
         return c->GetFlightInfo(FlightDescriptor::Command("fail")).status();
       },
       [](const Status& status) {
         EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
         EXPECT_THAT(status.ToString(), ::testing::HasSubstr("no such flight"));
       }},
  };

  const int64_t before = detail::AsyncReactorBase::live_reactors();
  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    TestFlightServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    const auto status = test.call(client.get());
    ASSERT_FALSE(status.ok());
    test.check(status);
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  }
  ExpectNoReactorLeak(before);
}

TEST(AsyncGrpcTest, UnaryReplyWriteFailureFailsTheRpc) {
  // A reply gRPC could not write: the RPC ends with the transport's own write
  // error, not a success. The reactor is driven by hand so the failure is exact
  // rather than a network race, and the same check runs for each unary method.
  struct Case {
    std::string name;
    FlightMethod method;
    std::function<::grpc::ServerGenericBidiReactor*(detail::AsyncCallContext,
                                                    TestFlightServer*)>
        make;
  };
  const std::vector<Case> cases = {
      {"GetFlightInfo", FlightMethod::GetFlightInfo,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeUnaryReactor(FlightMethod::GetFlightInfo, std::move(context),
                                         server);
       }},
      {"GetSchema", FlightMethod::GetSchema,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeUnaryReactor(FlightMethod::GetSchema, std::move(context),
                                         server);
       }},
      {"PollFlightInfo", FlightMethod::PollFlightInfo,
       [](detail::AsyncCallContext context, TestFlightServer* server) {
         return detail::MakeUnaryReactor(FlightMethod::PollFlightInfo, std::move(context),
                                         server);
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
    auto* reactor = test.make(std::move(context), &server);
    stream.Bind(reactor);

    ::grpc::Slice slice(WireBytes(FlightDescriptor::Path({"x"})));
    stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
    // The handler answers at once; its reply is on the wire.
    ASSERT_TRUE(stream.write_pending()) << "the reply was never written";
    stream.CompleteWrite(false);
    ASSERT_TRUE(stream.finish_observed().Wait(10));
    EXPECT_EQ(::grpc::StatusCode::INTERNAL, stream.finish_status()->error_code())
        << stream.finish_status()->error_message();
    EXPECT_THAT(stream.finish_status()->error_message(),
                ::testing::HasSubstr("Failed to write response"));
    // No second write and no pending callback after the failure.
    EXPECT_FALSE(stream.write_pending());
    if (stream.read_pending()) stream.CloseRead();
    stream.DrainWrites();
    if (!stream.done()) stream.Done();
  }
}

}  // namespace arrow::flight::transport::grpc
