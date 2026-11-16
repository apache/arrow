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

// DoExchange over the async generic service: the bidirectional RPC, whose handler
// awaits the reader's demand and each write in turn, so reads and writes are
// interleaved without blocking a callback thread. Covers the echo path, a
// metadata-only exchange, the client vanishing mid-exchange, and a handler whose
// future never resolves.

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

TEST(AsyncGrpcTest, DoExchangeEchoesBatches) {
  // Two batches up, two batches back, in order: this is the case that needs a
  // read and a write outstanding at the same time.
  FlightServerOptions options(LocalhostLocation());
  EchoExchangeTestServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("echo")));

  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  std::vector<std::shared_ptr<RecordBatch>> sent = {
      arrow::RecordBatch::Make(schema, 2,
                               {arrow::ArrayFromJSON(arrow::int64(), "[1, 2]")}),
      arrow::RecordBatch::Make(schema, 3,
                               {arrow::ArrayFromJSON(arrow::int64(), "[3, 4, 5]")})};
  ASSERT_OK(exchange.writer->Begin(schema));
  for (const auto& batch : sent) {
    ASSERT_OK(exchange.writer->WriteRecordBatch(*batch));
  }
  ASSERT_OK(exchange.writer->DoneWriting());

  ASSERT_OK_AND_ASSIGN(auto server_schema, exchange.reader->GetSchema());
  AssertSchemaEqual(*schema, *server_schema);
  for (const auto& batch : sent) {
    ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
    ASSERT_NE(nullptr, chunk.data);
    ASSERT_BATCHES_EQUAL(*batch, *chunk.data);
  }
  ASSERT_OK_AND_ASSIGN(auto end, exchange.reader->Next());
  ASSERT_EQ(nullptr, end.data);
  ASSERT_OK(exchange.writer->Close());

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ(1, flight_server.exchange_count());
  ASSERT_EQ(0, flight_server.exchanges_in_flight());
}

TEST(AsyncGrpcTest, DoExchangeMetadataOnlyDoesNotHang) {
  // A client that writes metadata and nothing else, then reads: the server's
  // handler must see the end of the exchange and reply, not wait for a schema
  // message that will never come (the case DataTest::TestDoExchangeNoData
  // covers). A hang here is the failure mode this test exists for.
  FlightServerOptions options(LocalhostLocation());
  EchoExchangeTestServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("echo")));

  ASSERT_OK(exchange.writer->WriteMetadata(arrow::Buffer::FromString("only-metadata")));
  ASSERT_OK(exchange.writer->DoneWriting());

  // The echo server reflects the metadata chunk back with no data.
  ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
  ASSERT_EQ(nullptr, chunk.data);
  ASSERT_NE(nullptr, chunk.app_metadata);
  ASSERT_EQ("only-metadata", chunk.app_metadata->ToString());
  ASSERT_OK_AND_ASSIGN(auto end, exchange.reader->Next());
  ASSERT_EQ(nullptr, end.data);
  ASSERT_OK(exchange.writer->Close());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ(1, flight_server.exchange_count());
  ASSERT_EQ(0, flight_server.exchanges_in_flight());
}

TEST(AsyncGrpcTest, DoExchangeClientDisconnectFinishes) {
  // A client that vanishes mid-exchange: the server's read fails, the reactor
  // must let the handler unwind and finish the RPC, and the exchange counter
  // added to the test server must come back to zero -- a hang or a leaked
  // handler leaves it above zero.
  FlightServerOptions options(LocalhostLocation());
  EchoExchangeTestServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("echo")));

  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  ASSERT_OK(exchange.writer->Begin(schema));
  ASSERT_OK(exchange.writer->WriteRecordBatch(*arrow::RecordBatch::Make(
      schema, 2, {arrow::ArrayFromJSON(arrow::int64(), "[1, 2]")})));
  // The server has entered its handler by now (it answered the schema), the
  // exchange is live.
  ASSERT_TRUE(WaitFor([&flight_server] { return flight_server.exchange_count() == 1; }));

  // Walk away without finishing: no DoneWriting, no Close, no reader drain.
  // The client's destructor closes the channel, which cancels the RPC.
  exchange.reader->Cancel();
  exchange.writer.reset();
  exchange.reader.reset();
  ASSERT_OK(client->Close());

  // The handler must leave, whichever status path it takes.
  const bool drained =
      WaitFor([&flight_server] { return flight_server.exchanges_in_flight() == 0; });
  ASSERT_TRUE(drained) << "the exchange handler never left (in flight: "
                       << flight_server.exchanges_in_flight() << ")";

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

// A DoExchange handler whose future never resolves.
class HangingExchangeServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(const ServerCallContext&,
                                  std::shared_ptr<AsyncFlightMessageReader>,
                                  std::shared_ptr<AsyncFlightMessageWriter>) override {
    handler_started.fetch_add(1);
    return arrow::Future<>::Make();  // never resolved
  }

  std::atomic<int> handler_started{0};
};

TEST(AsyncGrpcTest, DoExchangeClientCancelWithPendingHandlerFinishes) {
  // The client walks away with the handler still parked: the transport must
  // finish the RPC (and unwind reader/writer), not wait for the handler.
  const int64_t before = detail::AsyncReactorBase::live_reactors();

  HangingExchangeServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("hang")));
  // Wait until the handler is in (the descriptor reached it), so the cancel
  // lands on a live handler rather than a race with dispatch.
  ASSERT_TRUE(WaitFor([&flight_server] {
    return flight_server.handler_started.load() >= 1;
  })) << "the exchange handler never started";

  exchange.reader->Cancel();
  exchange.writer.reset();
  exchange.reader.reset();
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ExpectNoReactorLeak(before);
}

// An exchange whose handler reads until the end and records the status it was
// told about. A cancel must surface as an error, not as a clean end of input:
// a handler that commits at the end of its input would otherwise commit a
// truncated upload.
class DrainingExchangeServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(
      const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    auto state = std::make_shared<State>();
    state->reader = std::move(reader);
    state->writer = std::move(writer);
    state->owner = this;
    return Drain(state);
  }

  /// The status the handler saw when its read ended, and how many chunks it got.
  Status end_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return end_status_;
  }
  int chunks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return chunks_;
  }
  bool ended() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return ended_;
  }

 private:
  struct State {
    std::shared_ptr<AsyncFlightMessageReader> reader;
    std::shared_ptr<AsyncFlightMessageWriter> writer;
    DrainingExchangeServer* owner = nullptr;
  };

  arrow::Future<> Drain(const std::shared_ptr<State>& state) {
    return state->reader->NextAsync().Then(
        [state](const FlightStreamChunk& chunk) -> arrow::Future<> {
          if (chunk.data == nullptr && chunk.app_metadata == nullptr) {
            state->owner->Record(Status::OK());
            return arrow::Future<>::MakeFinished();
          }
          state->owner->RecordChunk();
          return state->owner->Drain(state);
        },
        [state](const Status& status) -> arrow::Future<> {
          // The read failed: a cancellation looks like any other read error
          // here, and must not be reported as a clean end.
          state->owner->Record(Status(status));
          return arrow::Future<>::MakeFinished();
        });
  }

  void Record(Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    ended_ = true;
    end_status_ = std::move(status);
  }
  void RecordChunk() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++chunks_;
  }

  mutable std::mutex mutex_;
  bool ended_ = false;
  Status end_status_;
  int chunks_ = 0;
};

TEST(AsyncGrpcTest, DoExchangeCancelIsReportedToTheHandlerNotAsEndOfInput) {
  FlightServerOptions options(LocalhostLocation());
  DrainingExchangeServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("drain")));
  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  ASSERT_OK(exchange.writer->Begin(schema));
  ASSERT_OK(exchange.writer->WriteRecordBatch(*arrow::RecordBatch::Make(
      schema, 2, {arrow::ArrayFromJSON(arrow::int64(), "[1, 2]")})));

  // Walk away with the reader still waiting on the server's read: the client's
  // close cancels the RPC.
  exchange.reader->Cancel();
  exchange.writer.reset();
  exchange.reader.reset();
  ASSERT_OK(client->Close());

  ASSERT_TRUE(WaitFor([&flight_server] { return flight_server.ended(); }))
      << "the handler never saw its read end";
  EXPECT_FALSE(flight_server.end_status().ok())
      << "a cancelled exchange was reported to the handler as a clean end of input";
  EXPECT_TRUE(flight_server.end_status().IsCancelled())
      << "expected a cancellation, got: " << flight_server.end_status().ToString();

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

// A handler that writes a batch, waits for it, and writes again: the contract
// says to await each returned future before writing again.
class TwoWriteExchangeServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(
      const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    auto schema = arrow::schema({arrow::field("a", arrow::int64())});
    auto batch = arrow::RecordBatch::Make(schema, 1,
                                          {arrow::ArrayFromJSON(arrow::int64(), "[1]")});
    return writer->BeginAsync(schema)
        .Then([writer, batch]() { return writer->WriteRecordBatchAsync(*batch); })
        .Then([writer, batch]() { return writer->WriteRecordBatchAsync(*batch); })
        .Then([this]() {
          std::lock_guard<std::mutex> lock(mutex_);
          done_ = true;
          return Status::OK();
        });
  }

  bool done() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return done_;
  }

 private:
  mutable std::mutex mutex_;
  bool done_ = false;
};

TEST(AsyncGrpcTest, DoExchangeSequentialWritesBothLand) {
  // Writing again after awaiting the previous future is the supported pattern:
  // the second write must reach the client, not be rejected.
  FlightServerOptions options(LocalhostLocation());
  TwoWriteExchangeServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("two-writes")));
  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  ASSERT_OK(exchange.writer->Begin(schema));

  int batches = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
    if (!chunk.data) break;
    ++batches;
  }
  EXPECT_EQ(2, batches) << "one of the two sequential writes was rejected";
  EXPECT_TRUE(WaitFor([&flight_server] { return flight_server.done(); }));

  ASSERT_OK(exchange.writer->DoneWriting());
  ASSERT_OK(exchange.writer->Close());
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

}  // namespace arrow::flight::transport::grpc
