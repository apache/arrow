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
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  // Connect the existing arrow::flight::FlightClient implementation.
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

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
  server->Shutdown();
  server->Wait();

  ASSERT_EQ(flight_server.ticket(), "basic-do-get-ticket");
}

TEST(AsyncGrpcTest, SchemaOnlyDoGet) {
  TestFlightServer flight_server;
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

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
  server->Shutdown();
  server->Wait();

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

  std::shared_ptr<Schema> schema() override { return resolved_->schema(); }

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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  HangingDoGetServer flight_server(HangingDoGetServer::HangAt::kStream);
  FlightServerOptions options(location);
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
  ASSERT_OK(client->Close());

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The producer was told to stop, exactly once.
  EXPECT_EQ(1, flight_server.stream_closes.load());
  // And the reactor is gone even though its payload future never completed.
  EXPECT_TRUE(WaitFor([&] {
    return detail::AsyncReactorBase::live_reactors() == before;
  })) << "a reactor leaked: still alive after the server stopped (before="
      << before << ", now=" << detail::AsyncReactorBase::live_reactors() << ")";
  // And the call reported completion exactly once (the CAS in FinishOnce).
  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->call_completed);
}

TEST(AsyncGrpcTest, DoGetClientDeadlineWhilePreparingFinishes) {
  // DoGetAsync() never resolves and the client's deadline expires: the RPC
  // must end anyway: the Shutdown()/Wait() below would otherwise block. No
  // stream exists here, so Close() has nothing to deliver and the producer
  // notices through ServerCallContext::is_cancelled().
  auto trace = std::make_shared<MiddlewareTrace>();
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  HangingDoGetServer flight_server(HangingDoGetServer::HangAt::kPrepare);
  FlightServerOptions options(location);
  options.middleware = {
      {"recording", std::make_shared<RecordingServerMiddlewareFactory>(trace)}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(500);
  auto reader = client->DoGet(impatient, Ticket{"hang"});
  EXPECT_FALSE(reader.ok()) << "the deadline must expire while the server prepares";

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  EXPECT_EQ(0, flight_server.stream_closes.load())
      << "no stream was ever prepared, so there is nothing to Close";
  std::lock_guard<std::mutex> guard(trace->mutex);
  EXPECT_EQ(1, trace->call_completed);
}

/// A DoGet stream whose first payload is an error: the transport must fail the
/// RPC and still tell the producer the RPC is over through Close().
class ErrorPayloadDoGetStream final : public AsyncFlightDataStream {
 public:
  explicit ErrorPayloadDoGetStream(std::atomic<int>* close_count)
      : close_count_(close_count) {}

  std::shared_ptr<Schema> schema() override {
    return arrow::schema({arrow::field("value", arrow::int64())});
  }

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    return resolved_->GetSchemaPayloadAsync();
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    return arrow::Future<std::optional<FlightPayload>>::MakeFinished(
        arrow::Status::IOError("the producer failed"));
  }

  Status Close() override {
    close_count_->fetch_add(1);
    return Status::OK();
  }

  std::shared_ptr<ResolvedStream> resolved_ = std::make_shared<ResolvedStream>(
      arrow::RecordBatchReader::Make(
          {}, arrow::schema({arrow::field("value", arrow::int64())}))
          .ValueOrDie());

 private:
  std::atomic<int>* close_count_;
};

/// Serves ErrorPayloadDoGetStream and counts the Close() it was handed.
class ErrorPayloadDoGetServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext&, const Ticket&) override {
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::make_shared<ErrorPayloadDoGetStream>(&stream_closes));
  }

  std::atomic<int> stream_closes{0};
};

TEST(AsyncGrpcTest, DoGetPumpErrorStillClosesTheStream) {
  // The producer's first NextAsync() fails: the RPC ends with that status, and
  // the producer must still be told the RPC is over through Close(), like the
  // write-failure and cancel paths already do.
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  ErrorPayloadDoGetServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // DoGet returns once the schema payload is written, the error payload follows.
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(Ticket{"error-payload"}));
  while (true) {
    auto chunk = reader->Next();
    if (!chunk.ok()) {
      EXPECT_EQ(chunk.status().code(), arrow::StatusCode::IOError) << chunk.status();
      break;
    }
    if (!chunk->data) {
      break;
    }
  }
  reader.reset();
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  EXPECT_EQ(1, flight_server.stream_closes.load())
      << "the producer was never told the RPC is over";
}

}  // namespace arrow::flight::transport::grpc
