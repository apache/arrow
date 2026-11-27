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

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

TEST(AsyncGrpcTest, BasicDoGet) {
  TestFlightServer flight_server;
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  // Connect the existing arrow::flight::FlightClient implementation.
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  // Call DoGet: the ticket must reach TestFlightServer::DoGetAsync.
  Ticket ticket{"basic-do-get-ticket"};
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(ticket));

  // Read schema.
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  ASSERT_EQ(schema->num_fields(), 2);
  ASSERT_EQ(schema->field(0)->name(), "a");
  ASSERT_EQ(schema->field(1)->name(), "b");

  // Read batches.
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

  // Cleanup. The ticket is asserted only after the server stopped, so the
  // DoGet callback cannot race with the read.
  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();

  ASSERT_EQ(flight_server.ticket(), "basic-do-get-ticket");
}

TEST(AsyncGrpcTest, SchemaOnlyDoGet) {
  TestFlightServer flight_server;
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  // This ticket is served as a stream with no batches at all.
  Ticket ticket{kSchemaOnlyTicket};
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(ticket));

  // The schema is served...
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  ASSERT_EQ(schema->num_fields(), 2);
  ASSERT_EQ(schema->field(0)->name(), "a");
  ASSERT_EQ(schema->field(1)->name(), "b");

  // ...and the very first read reports the end of stream instead of failing.
  ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
  ASSERT_EQ(chunk.data, nullptr);

  // Cleanup. The ticket is asserted only after the server stopped, so the
  // DoGet callback cannot race with the read.
  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();

  ASSERT_EQ(flight_server.ticket(), kSchemaOnlyTicket);
}

// A DoGet stream whose first payload never arrives: the producer is parked
// forever, so only a client cancel can ever end the RPC. `close_count` and
// `next_awaits` let the test observe, from the test thread, that the payload
// demand is live and that Close() was delivered exactly once.
class HangingDoGetStream final : public AsyncFlightDataStream {
 public:
  HangingDoGetStream(std::shared_ptr<ResolvedStream> resolved,
                     std::atomic<int>* next_awaits, std::atomic<int>* close_count)
      : resolved_(std::move(resolved)),
        next_awaits_(next_awaits),
        close_count_(close_count) {}

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    return resolved_->GetSchemaPayloadAsync();
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    next_awaits_->fetch_add(1);
    std::lock_guard<std::mutex> lock(mutex_);
    pending_ = arrow::Future<std::optional<FlightPayload>>::Make();
    return pending_;
  }

  // The transport's stop signal: resolve the pending payload so the producer
  // does not park forever (mirrors the example's TimedAsyncStream::Close).
  Status Close() override {
    close_count_->fetch_add(1);
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_.is_valid() && !pending_.is_finished()) {
      pending_.MarkFinished(arrow::Status::Cancelled());
    }
    return resolved_->Close();
  }

 private:
  std::shared_ptr<ResolvedStream> resolved_;
  std::atomic<int>* next_awaits_;
  std::atomic<int>* close_count_;
  std::mutex mutex_;
  arrow::Future<std::optional<FlightPayload>> pending_;
};

// DoGet server with a configurable hang: kPrepare leaves DoGetAsync() pending
// (no stream ever exists), kStream resolves the stream but leaves the first
// payload pending.
class HangingDoGetServer final : public AsyncGenericFlightServerBase {
 public:
  enum class HangAt { kPrepare, kStream };
  explicit HangingDoGetServer(HangAt hang) : hang_(hang) {}

  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext&, const Ticket&) override {
    if (hang_ == HangAt::kPrepare) {
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::Make();
    }
    auto reader = arrow::RecordBatchReader::Make(
                      {}, arrow::schema({arrow::field("value", arrow::int64())}))
                      .ValueOrDie();
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::make_shared<HangingDoGetStream>(
            std::make_shared<ResolvedStream>(std::move(reader)), &next_awaits,
            &stream_closes));
  }

  std::atomic<int> next_awaits{0};
  std::atomic<int> stream_closes{0};

 private:
  HangAt hang_;
};

TEST(AsyncGrpcTest, DoGetClientCancelWithPendingPayloadFinishes) {
  // The client gives up while the producer's NextAsync() is pending: the
  // transport must finish the RPC itself and deliver Close() to the producer,
  // instead of waiting for a payload that will not come.
  const int64_t before = detail::AsyncReactorBase::live_reactors();
  auto trace = std::make_shared<MiddlewareTrace>();
  HangingDoGetServer flight_server(HangingDoGetServer::HangAt::kStream);
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {
      {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // DoGet returns once the schema payload is written,the payload after it is
  // the one that never comes. Wait until the server has actually asked for
  // it, so the cancel lands on a live demand (not a race with the schema
  // write).
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"hang"}));
  ASSERT_TRUE(WaitFor([&flight_server] { return flight_server.next_awaits.load() >= 1; }))
      << "the server never asked for its first payload";

  reader->Cancel();
  reader.reset();
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  // The producer was told to stop, exactly once.
  EXPECT_EQ(1, flight_server.stream_closes.load());
  // And the reactor is gone even though its payload future never completed.
  ExpectNoReactorLeak(before);
  // And the call reported completion exactly once (the CAS in FinishOnce).
  std::lock_guard<std::mutex> guard(trace->mutex);
  ASSERT_EQ(1, trace->completed_statuses.size());
  EXPECT_TRUE(trace->completed_statuses[0].IsCancelled()) << trace->completed_statuses[0];
}

TEST(AsyncGrpcTest, DoGetClientDeadlineWhilePreparingFinishes) {
  // DoGetAsync() never resolves and the client's deadline expires: the RPC
  // must end anyway: the Shutdown()/Wait() below would otherwise block. No
  // stream exists here, so Close() has nothing to deliver and the producer
  // notices through ServerCallContext::is_cancelled().
  auto trace = std::make_shared<MiddlewareTrace>();
  HangingDoGetServer flight_server(HangingDoGetServer::HangAt::kPrepare);
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {
      {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  auto reader = client->DoGet(impatient, Ticket{"hang"});
  EXPECT_FALSE(reader.ok()) << "the deadline must expire while the server prepares";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  EXPECT_EQ(0, flight_server.stream_closes.load())
      << "no stream was ever prepared, so there is nothing to Close";
  std::lock_guard<std::mutex> guard(trace->mutex);
  ASSERT_EQ(1, trace->completed_statuses.size());
  EXPECT_FALSE(trace->completed_statuses[0].ok());
}

/// A DoGet server whose DoGetAsync() registers a cancel callback and parks its
/// future until the test resolves it: the test drives a cancellation, a
/// callback registered once the RPC is already cancelled, and a stream that
/// arrives after the RPC ended.
class CancelCallbackDoGetServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext& context, const Ticket&) override {
    auto pending = arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::Make();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      context_ = &context;
      pending_ = pending;
    }
    ARROW_RETURN_NOT_OK(
        AddCancelCallback(context, [this] { cancel_callbacks.fetch_add(1); }));
    return pending;
  }

  /// Register on the parked call's context from the test thread: the call is
  /// still live, since the transport awaits the parked future.
  Status AddCancelCallbackFromTest(std::function<void()> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    return AddCancelCallback(*context_, std::move(callback));
  }

  /// Resolve the parked DoGetAsync() future with a stream, on this thread.
  void ResolveWithStream() {
    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> pending;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      pending = pending_;
    }
    auto reader = arrow::RecordBatchReader::Make(
                      {}, arrow::schema({arrow::field("value", arrow::int64())}))
                      .ValueOrDie();
    std::shared_ptr<AsyncFlightDataStream> stream = std::make_shared<HangingDoGetStream>(
        std::make_shared<ResolvedStream>(std::move(reader)), &next_awaits,
        &stream_closes);
    pending.MarkFinished(std::move(stream));
  }

  std::atomic<int> cancel_callbacks{0};
  std::atomic<int> next_awaits{0};
  std::atomic<int> stream_closes{0};

 private:
  std::mutex mutex_;
  const ServerCallContext* context_ = nullptr;
  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> pending_;
};

TEST(AsyncGrpcTest, DoGetCancelCallbackReachesPendingHandler) {
  // The client's deadline expires while DoGetAsync() is pending: the callback
  // the handler registered runs once without any polling, a callback
  // registered once the RPC is cancelled runs at once, and the stream the
  // handler then resolves to is never read from but still receives Close().
  const int64_t before = detail::AsyncReactorBase::live_reactors();
  CancelCallbackDoGetServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  EXPECT_FALSE(client->DoGet(impatient, Ticket{"hang"}).ok())
      << "the deadline must expire while the handler is pending";
  ASSERT_TRUE(WaitFor([&] { return flight_server.cancel_callbacks.load() >= 1; }))
      << "the cancellation never reached the handler's callback";

  auto late_callbacks = std::make_shared<std::atomic<int>>(0);
  ASSERT_OK(flight_server.AddCancelCallbackFromTest(
      [late_callbacks] { late_callbacks->fetch_add(1); }));
  EXPECT_EQ(1, late_callbacks->load())
      << "a callback registered after the cancellation must run before "
         "AddCancelCallback() returns";

  // The transport's continuation runs inline, on this thread.
  flight_server.ResolveWithStream();
  EXPECT_EQ(1, flight_server.stream_closes.load())
      << "a stream that arrives after the RPC ended must still be closed";
  EXPECT_EQ(0, flight_server.next_awaits.load())
      << "a stream that arrives after the RPC ended must not be read from";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  EXPECT_EQ(1, flight_server.cancel_callbacks.load())
      << "the cancel callback must run exactly once";
  ExpectNoReactorLeak(before);
}

/// A DoGet stream asked for its schema while the RPC dies: GetSchemaPayloadAsync()
/// stays inside the call until the client's deadline cancelled the RPC, then
/// gives the cancellation's teardown time to deliver Close(). It records whether
/// Close() arrived while it was still being asked, or a demand after Close().
class SlowSchemaDoGetStream final : public AsyncFlightDataStream {
 public:
  SlowSchemaDoGetStream(const ServerCallContext* context,
                        std::shared_ptr<ResolvedStream> resolved,
                        std::atomic<int>* closes, std::atomic<bool>* closed_while_asked,
                        std::atomic<int>* asked_after_close)
      : context_(context),
        resolved_(std::move(resolved)),
        closes_(closes),
        closed_while_asked_(closed_while_asked),
        asked_after_close_(asked_after_close) {}

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    CountDemandAfterClose();
    WaitFor([this] { return context_->is_cancelled(); });
    if (WaitFor([this] { return closes_->load() > 0; }, std::chrono::milliseconds(300))) {
      closed_while_asked_->store(true);
    }
    return resolved_->GetSchemaPayloadAsync();
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    CountDemandAfterClose();
    return resolved_->NextAsync();
  }

  Status Close() override {
    closes_->fetch_add(1);
    return resolved_->Close();
  }

 private:
  void CountDemandAfterClose() {
    if (closes_->load() > 0) {
      asked_after_close_->fetch_add(1);
    }
  }

  const ServerCallContext* context_;
  std::shared_ptr<ResolvedStream> resolved_;
  std::atomic<int>* closes_;
  std::atomic<bool>* closed_while_asked_;
  std::atomic<int>* asked_after_close_;
};

/// Serves every DoGet with a SlowSchemaDoGetStream, resolved at once.
class SlowSchemaDoGetServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext& context, const Ticket&) override {
    auto reader = arrow::RecordBatchReader::Make(
                      {}, arrow::schema({arrow::field("value", arrow::int64())}))
                      .ValueOrDie();
    std::shared_ptr<AsyncFlightDataStream> stream =
        std::make_shared<SlowSchemaDoGetStream>(
            &context, std::make_shared<ResolvedStream>(std::move(reader)), &closes,
            &closed_while_asked, &asked_after_close);
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::move(stream));
  }

  std::atomic<int> closes{0};
  std::atomic<bool> closed_while_asked{false};
  std::atomic<int> asked_after_close{0};
};

TEST(AsyncGrpcTest, DoGetNeverClosesAStreamItIsAskingForAPayload) {
  // The client's deadline expires while the transport is inside the stream's
  // GetSchemaPayloadAsync(): the cancellation's Close() must wait for that
  // call to return, and the stream must not be asked for anything after it.
  const int64_t before = detail::AsyncReactorBase::live_reactors();
  SlowSchemaDoGetServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  EXPECT_FALSE(client->DoGet(impatient, Ticket{"slow-schema"}).ok())
      << "the deadline must expire while the stream is asked for its schema";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  EXPECT_FALSE(flight_server.closed_while_asked.load())
      << "Close() ran while the transport was inside GetSchemaPayloadAsync()";
  EXPECT_EQ(0, flight_server.asked_after_close.load())
      << "the stream was asked for a payload after Close()";
  EXPECT_EQ(1, flight_server.closes.load()) << "Close() must be delivered exactly once";
  ExpectNoReactorLeak(before);
}

/// A DoGet producer with one selectable failure, recording what the transport asked
/// of it. One stream type and one server cover the schema, payload and Close error
/// paths, so the assertions differ only where the transport's behavior does.
class CaseDoGetStream final : public AsyncFlightDataStream {
 public:
  enum class Case { kSchemaError, kNextError, kCloseError };

  CaseDoGetStream(Case c, std::shared_ptr<ResolvedStream> resolved,
                  std::atomic<int>* next_calls, std::atomic<int>* closes)
      : case_(c),
        resolved_(std::move(resolved)),
        next_calls_(next_calls),
        closes_(closes) {}

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    if (case_ == Case::kSchemaError) {
      return arrow::Status::IOError("the schema failed");
    }
    return resolved_->GetSchemaPayloadAsync();
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    next_calls_->fetch_add(1);
    if (case_ == Case::kNextError) {
      return arrow::Status::IOError("the payload failed");
    }
    return resolved_->NextAsync();
  }

  Status Close() override {
    closes_->fetch_add(1);
    const auto status = resolved_->Close();
    if (case_ == Case::kCloseError) {
      return Status::IOError("the close failed");
    }
    return status;
  }

 private:
  Case case_;
  std::shared_ptr<ResolvedStream> resolved_;
  std::atomic<int>* next_calls_;
  std::atomic<int>* closes_;
};

class CaseDoGetServer final : public AsyncGenericFlightServerBase {
 public:
  explicit CaseDoGetServer(CaseDoGetStream::Case c) : case_(c) {}

  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext&, const Ticket&) override {
    auto reader = arrow::RecordBatchReader::Make(
                      {}, arrow::schema({arrow::field("value", arrow::int64())}))
                      .ValueOrDie();
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::make_shared<CaseDoGetStream>(
            case_, std::make_shared<ResolvedStream>(std::move(reader)), &next_calls,
            &closes));
  }

  std::atomic<int> next_calls{0};
  std::atomic<int> closes{0};

 private:
  CaseDoGetStream::Case case_;
};

TEST(AsyncGrpcTest, DoGetSchemaErrorClosesTheStream) {
  // The schema payload fails: the RPC ends with that error, no data message is sent,
  // and the producer is told the RPC is over exactly once.
  CaseDoGetServer flight_server(CaseDoGetStream::Case::kSchemaError);
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  const auto reader = client->DoGet(Ticket{"schema-error"});
  ASSERT_FALSE(reader.ok());
  EXPECT_THAT(reader.status().ToString(), ::testing::HasSubstr("the schema failed"));
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, flight_server.closes.load());
  EXPECT_EQ(0, flight_server.next_calls.load())
      << "a stream whose schema failed must not be asked for data";
}

TEST(AsyncGrpcTest, DoGetNextErrorStillClosesTheStream) {
  // The producer's first NextAsync() fails: the RPC ends with that IOError, the
  // schema arrived first, and Close() is delivered exactly once.
  CaseDoGetServer flight_server(CaseDoGetStream::Case::kNextError);
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"next-error"}));
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  EXPECT_EQ(1, schema->num_fields());

  // The reader must end with the producer's error, not a clean end of stream.
  const auto chunk = reader->Next();
  ASSERT_FALSE(chunk.ok()) << "the producer error must reach the client";
  EXPECT_EQ(StatusCode::IOError, chunk.status().code()) << chunk.status();
  EXPECT_THAT(chunk.status().ToString(), ::testing::HasSubstr("the payload failed"));
  reader.reset();
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, flight_server.closes.load());
  EXPECT_EQ(1, flight_server.next_calls.load());
}

TEST(AsyncGrpcTest, DoGetCloseErrorIsTheTerminalStatus) {
  // A clean end of stream with a failing Close(): the RPC's status is that error, so
  // the client's last read may carry it instead of a silent end of stream. Close()
  // runs once either way.
  CaseDoGetServer flight_server(CaseDoGetStream::Case::kCloseError);
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"close-error"}));
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  EXPECT_EQ(1, schema->num_fields());

  // The reader is empty, so the first read is the end of the stream: either the
  // clean end, or the Close() error the RPC settled with. Never data.
  const auto chunk = reader->Next();
  if (chunk.ok()) {
    EXPECT_EQ(nullptr, chunk->data);
  } else {
    EXPECT_THAT(chunk.status().ToString(), ::testing::HasSubstr("the close failed"))
        << chunk.status();
  }
  reader.reset();
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, flight_server.closes.load());
  EXPECT_EQ(1, flight_server.next_calls.load());
}

TEST(AsyncGrpcTest, DoGetLatePayloadCompletionStartsNoIo) {
  // A payload Future the producer completes after the RPC is over: the continuation
  // must not write, re-demand, or Close() a second time, and the terminal status
  // stays the cancellation.
  const int64_t before = detail::AsyncReactorBase::live_reactors();

  class LatePayloadServer final : public AsyncGenericFlightServerBase {
   public:
    /// A stream that serves its schema, then parks its first payload until the
    /// test resolves it.
    class ParkedPayloadStream final : public AsyncFlightDataStream {
     public:
      ParkedPayloadStream(std::shared_ptr<RecordBatchReader> reader,
                          std::shared_ptr<Future<std::optional<FlightPayload>>> payload)
          : resolved_(std::move(reader)), payload_(std::move(payload)) {}

      arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
        return resolved_.GetSchemaPayloadAsync();
      }
      arrow::Future<std::optional<FlightPayload>> NextAsync() override {
        next_calls.fetch_add(1);
        *payload_ = Future<std::optional<FlightPayload>>::Make();
        return *payload_;
      }
      Status Close() override {
        closes.fetch_add(1);
        return Status::OK();
      }
      std::atomic<int> next_calls{0};
      std::atomic<int> closes{0};

     private:
      ResolvedStream resolved_;
      std::shared_ptr<Future<std::optional<FlightPayload>>> payload_;
    };

    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
        const ServerCallContext& context, const Ticket&) override {
      // The cancellation only signals the test: the test owns the parked demand and
      // completes it, so it can observe what the late completion does (and does not).
      ARROW_RETURN_NOT_OK(
          AddCancelCallback(context, [this] { cancelled.MarkFinished(); }));
      auto reader = arrow::RecordBatchReader::Make(
                        {}, arrow::schema({arrow::field("value", arrow::int64())}))
                        .ValueOrDie();
      stream = std::make_shared<ParkedPayloadStream>(std::move(reader), payload);
      entered.MarkFinished();
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(stream);
    }

    Future<> entered = Future<>::Make();
    Future<> cancelled = Future<>::Make();
    std::shared_ptr<Future<std::optional<FlightPayload>>> payload =
        std::make_shared<Future<std::optional<FlightPayload>>>();
    std::shared_ptr<ParkedPayloadStream> stream;
  };

  LatePayloadServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"late-payload"}));
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  EXPECT_EQ(1, schema->num_fields());
  ASSERT_TRUE(flight_server.entered.Wait(10)) << "the handler never ran";
  reader->Cancel();
  reader.reset();
  ASSERT_TRUE(flight_server.cancelled.Wait(10)) << "the cancellation never ran";

  const auto stream = flight_server.stream;
  ASSERT_NE(nullptr, stream);
  ASSERT_TRUE(WaitFor([&] { return stream->closes.load() == 1; }))
      << "the producer was not told the RPC is over";
  const int next_before = stream->next_calls.load();

  // Complete the abandoned demand late: nothing may be written, demanded or closed.
  flight_server.payload->MarkFinished(std::make_optional(FlightPayload{}));
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, stream->closes.load()) << "Close() must not run twice";
  EXPECT_EQ(next_before, stream->next_calls.load())
      << "a late payload must not make the transport demand more";
  ExpectNoReactorLeak(before);
}

TEST(AsyncGrpcTest, DoGetNullStreamIsNotFound) {
  // A handler that answers OK without a stream: the sync transport answers NOT_FOUND
  // with "No data in this flight", and the async reactor must answer the same.
  class NullStreamServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
        const ServerCallContext&, const Ticket&) override {
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
          std::shared_ptr<AsyncFlightDataStream>{});
    }
  };
  NullStreamServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  const auto status = client->DoGet(Ticket{"null"}).status();
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(StatusCode::KeyError, status.code()) << status;
  EXPECT_THAT(status.ToString(), ::testing::HasSubstr("No data in this flight"));
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoGetReadFailureEndsTheRpc) {
  // The client half-closes before sending its ticket: the reactor finishes with the
  // "Failed to read request" error rather than dispatching an empty ticket.
  TestFlightServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto raw, CallRaw(flight_server.port(), MethodPath("DoGet"), {}));
  EXPECT_EQ(::grpc::StatusCode::INTERNAL, raw.status.error_code());
  EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr("Failed to read request"));
  EXPECT_EQ(0, flight_server.handler_calls());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoGetPayloadWriteFailureClosesTheStream) {
  // A payload the reactor could not write ends the RPC with the write error and still
  // delivers Close() to the producer exactly once. Driven by hand so the failed write
  // is exact instead of a network race.
  class CountingStream final : public AsyncFlightDataStream {
   public:
    CountingStream(std::shared_ptr<RecordBatchReader> reader, std::atomic<int>* closes)
        : resolved_(std::move(reader)), closes_(closes) {}
    arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
      return resolved_.GetSchemaPayloadAsync();
    }
    arrow::Future<std::optional<FlightPayload>> NextAsync() override {
      return resolved_.NextAsync();
    }
    Status Close() override {
      closes_->fetch_add(1);
      return Status::OK();
    }

   private:
    ResolvedStream resolved_;
    std::atomic<int>* closes_;
  };
  class CountingServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
        const ServerCallContext&, const Ticket&) override {
      auto reader = arrow::RecordBatchReader::Make(
                        {}, arrow::schema({arrow::field("value", arrow::int64())}))
                        .ValueOrDie();
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
          std::make_shared<CountingStream>(std::move(reader), &closes));
    }
    std::atomic<int> closes{0};
  };

  CountingServer server;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext context(&stream.context());
  helper->MakeCallContext(FlightMethod::DoGet, &stream.context(), &context);
  stream.Bind(detail::MakeDoGetReactor(std::move(context), &server));

  ::grpc::Slice slice(WireBytes(Ticket{"write-failure"}));
  stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  // The schema payload is written first; fail that write.
  ASSERT_TRUE(WaitFor([&] { return stream.write_pending(); }))
      << "the schema payload was never written";
  stream.CompleteWrite(false);
  ASSERT_TRUE(stream.finish_observed().Wait(10));
  EXPECT_EQ(::grpc::StatusCode::INTERNAL, stream.finish_status()->error_code())
      << stream.finish_status()->error_message();
  EXPECT_THAT(stream.finish_status()->error_message(),
              ::testing::HasSubstr("Write failed"));
  EXPECT_EQ(1, server.closes.load())
      << "a failed write must still Close() the producer, exactly once";
  EXPECT_FALSE(stream.write_pending()) << "a failed write must not be retried";
  if (stream.read_pending()) stream.CloseRead();
  stream.DrainWrites();
  if (!stream.done()) stream.Done();
}

TEST(AsyncGrpcTest, DoGetLateWriteCallbackAfterFinishStartsNothing) {
  // A write completing after the RPC is already finished must not write again, demand
  // a payload, or Close() a second time: OnWriteDone() returns before doing any of it.
  // Driven by hand: the write is completed only after the cancellation settled the RPC.
  class CountingStream final : public AsyncFlightDataStream {
   public:
    CountingStream(std::shared_ptr<RecordBatchReader> reader, std::atomic<int>* closes,
                   std::atomic<int>* next_calls)
        : resolved_(std::move(reader)), closes_(closes), next_calls_(next_calls) {}
    arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
      return resolved_.GetSchemaPayloadAsync();
    }
    arrow::Future<std::optional<FlightPayload>> NextAsync() override {
      next_calls_->fetch_add(1);
      return resolved_.NextAsync();
    }
    Status Close() override {
      closes_->fetch_add(1);
      return Status::OK();
    }

   private:
    ResolvedStream resolved_;
    std::atomic<int>* closes_;
    std::atomic<int>* next_calls_;
  };
  class CountingServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
        const ServerCallContext&, const Ticket&) override {
      auto reader = arrow::RecordBatchReader::Make(
                        {}, arrow::schema({arrow::field("value", arrow::int64())}))
                        .ValueOrDie();
      stream = std::make_shared<CountingStream>(std::move(reader), &closes, &next_calls);
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(stream);
    }
    std::atomic<int> closes{0};
    std::atomic<int> next_calls{0};
    std::shared_ptr<CountingStream> stream;
  };

  CountingServer server;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext context(&stream.context());
  helper->MakeCallContext(FlightMethod::DoGet, &stream.context(), &context);
  stream.Bind(detail::MakeDoGetReactor(std::move(context), &server));

  ::grpc::Slice slice(WireBytes(Ticket{"late-write"}));
  stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  ASSERT_TRUE(WaitFor([&] { return stream.write_pending(); }))
      << "the schema payload was never written";
  // Cancel while the write is outstanding: the RPC finishes and the teardown closes
  // the stream. The write callback then arrives on a finished call.
  stream.Cancel();
  ASSERT_TRUE(stream.finish_observed().Wait(10));
  const int closes_after_cancel = server.closes.load();
  const int next_after_cancel = server.next_calls.load();
  ASSERT_EQ(1, closes_after_cancel) << "the teardown must close the producer";

  stream.CompleteWrite(true);
  EXPECT_EQ(closes_after_cancel, server.closes.load()) << "Close() must not run twice";
  EXPECT_EQ(next_after_cancel, server.next_calls.load())
      << "a late write callback must not demand another payload";
  EXPECT_FALSE(stream.write_pending()) << "a finished RPC must not write again";
  if (stream.read_pending()) stream.CloseRead();
  if (!stream.done()) stream.Done();
}

TEST(AsyncGrpcTest, DoGetSecondCloseIsANoOp) {
  // CloseStreamOnce() is reached from the write-failure path and again from the
  // cancellation: the second call must report OK without touching the stream twice.
  class CountingStream final : public AsyncFlightDataStream {
   public:
    CountingStream(std::shared_ptr<RecordBatchReader> reader, std::atomic<int>* closes)
        : resolved_(std::move(reader)), closes_(closes) {}
    arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
      return resolved_.GetSchemaPayloadAsync();
    }
    arrow::Future<std::optional<FlightPayload>> NextAsync() override {
      return resolved_.NextAsync();
    }
    Status Close() override {
      closes_->fetch_add(1);
      return Status::OK();
    }

   private:
    ResolvedStream resolved_;
    std::atomic<int>* closes_;
  };
  class CountingServer final : public AsyncGenericFlightServerBase {
   public:
    arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
        const ServerCallContext&, const Ticket&) override {
      auto reader = arrow::RecordBatchReader::Make(
                        {}, arrow::schema({arrow::field("value", arrow::int64())}))
                        .ValueOrDie();
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
          std::make_shared<CountingStream>(std::move(reader), &closes));
    }
    std::atomic<int> closes{0};
  };

  CountingServer server;
  ManualServerStream stream;
  auto helper =
      std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
          /*auth_handler=*/nullptr, MiddlewareFactoryList{});
  detail::AsyncCallContext context(&stream.context());
  helper->MakeCallContext(FlightMethod::DoGet, &stream.context(), &context);
  stream.Bind(detail::MakeDoGetReactor(std::move(context), &server));

  ::grpc::Slice slice(WireBytes(Ticket{"double-close"}));
  stream.DeliverRead(::grpc::ByteBuffer(&slice, 1));
  ASSERT_TRUE(WaitFor([&] { return stream.write_pending(); }));
  // The write fails: that path finishes the RPC and closes the stream.
  stream.CompleteWrite(false);
  ASSERT_TRUE(stream.finish_observed().Wait(10));
  ASSERT_EQ(1, server.closes.load());
  // A cancellation after the failure reaches CloseStreamOnce() a second time.
  stream.Cancel();
  EXPECT_EQ(1, server.closes.load()) << "Close() must not run twice";
  if (stream.read_pending()) stream.CloseRead();
  stream.DrainWrites();
  if (!stream.done()) stream.Done();
}

}  // namespace arrow::flight::transport::grpc
