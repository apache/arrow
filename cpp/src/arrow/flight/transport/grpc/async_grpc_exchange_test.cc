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

#include "arrow/flight/transport/grpc/async_grpc_exchange_internal.h"
#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

#include "arrow/array/util.h"
#include "arrow/scalar.h"

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
    // Distinct batches, in order: the echo handler awaits each write before
    // asking for the next message, so this also covers two sequential writes
    // whose flush completes between them (not a reentrant one).
    EXPECT_EQ(nullptr, chunk.app_metadata)
        << "a batch chunk carried metadata the client never sent";
  }
  // Exactly two messages, each with both members null: nothing was lost,
  // duplicated or invented at the end.
  ASSERT_OK_AND_ASSIGN(auto end, exchange.reader->Next());
  EXPECT_EQ(nullptr, end.data);
  EXPECT_EQ(nullptr, end.app_metadata);
  ASSERT_OK(exchange.writer->Close());

  // Cleanup before looking at the recording: it was written on a gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ(1, flight_server.exchange_count());
  ASSERT_EQ(0, flight_server.exchanges_in_flight());
}

TEST(AsyncGrpcTest, DoExchangeEchoesZeroRowBatchAndMetadata) {
  // The payloads the echo handler must not confuse with the end of the exchange: a
  // zero-row batch (real data) and a metadata-only chunk (a value). Both come back
  // in order, with the final chunk having both members null.
  EchoExchangeTestServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("echo")));

  auto schema = TwoColumnSchema();
  auto zero_rows = RecordBatch::Make(
      schema, 0,
      {ArrayFromJSON(arrow::int64(), "[]"), ArrayFromJSON(arrow::int64(), "[]")});
  auto batch = TwoColumnBatch(schema);

  ASSERT_OK(exchange.writer->Begin(schema));
  ASSERT_OK(exchange.writer->WriteRecordBatch(*zero_rows));
  ASSERT_OK(exchange.writer->WriteMetadata(Buffer::FromString("between")));
  ASSERT_OK(exchange.writer->WriteRecordBatch(*batch));
  ASSERT_OK(exchange.writer->DoneWriting());

  ASSERT_OK_AND_ASSIGN(auto server_schema, exchange.reader->GetSchema());
  AssertSchemaEqual(*schema, *server_schema);

  ASSERT_OK_AND_ASSIGN(auto first, exchange.reader->Next());
  ASSERT_NE(nullptr, first.data) << "a zero-row batch is data, not the end";
  EXPECT_EQ(0, first.data->num_rows());
  EXPECT_EQ(nullptr, first.app_metadata);

  ASSERT_OK_AND_ASSIGN(auto middle, exchange.reader->Next());
  EXPECT_EQ(nullptr, middle.data);
  ASSERT_NE(nullptr, middle.app_metadata);
  EXPECT_EQ("between", middle.app_metadata->ToString());

  ASSERT_OK_AND_ASSIGN(auto last, exchange.reader->Next());
  ASSERT_NE(nullptr, last.data);
  ASSERT_BATCHES_EQUAL(*batch, *last.data);
  EXPECT_EQ(nullptr, last.app_metadata)
      << "the metadata belongs to its own message, not the next batch";

  ASSERT_OK_AND_ASSIGN(auto end, exchange.reader->Next());
  EXPECT_EQ(nullptr, end.data);
  EXPECT_EQ(nullptr, end.app_metadata);
  ASSERT_OK(exchange.writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

TEST(AsyncGrpcTest, DoExchangeEchoesDictionaries) {
  // A dictionary-encoded exchange: the writer emits the dictionary messages itself
  // and the reader must resolve them, so the echoed batch equals the one sent and no
  // dictionary surfaces as a chunk. Two independent dictionary fields, because the
  // initial-dictionary handling is what broke on the upload side.
  EchoExchangeTestServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("echo")));

  const auto t0 = dictionary(arrow::int32(), utf8());
  const auto t1 = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d0", t0), arrow::field("d1", t1)});
  auto batch = RecordBatch::Make(
      schema, 3,
      {ArrayFromJSON(t0, R"(["a", "b", "a"])"), ArrayFromJSON(t1, R"(["x", "y", "x"])")});

  ASSERT_OK(exchange.writer->Begin(schema));
  ASSERT_OK(exchange.writer->WriteRecordBatch(*batch));
  ASSERT_OK(exchange.writer->DoneWriting());

  ASSERT_OK_AND_ASSIGN(auto server_schema, exchange.reader->GetSchema());
  EXPECT_EQ(2, server_schema->num_fields());

  ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
  ASSERT_NE(nullptr, chunk.data) << "a dictionary must not surface as a chunk";
  ASSERT_BATCHES_EQUAL(*batch, *chunk.data);
  ASSERT_OK_AND_ASSIGN(auto end, exchange.reader->Next());
  EXPECT_EQ(nullptr, end.data);
  ASSERT_OK(exchange.writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, flight_server.exchange_count());
  EXPECT_EQ(0, flight_server.exchanges_in_flight());
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
    {
      std::lock_guard<std::mutex> lock(mutex_);
      reader_ = state->reader;
    }
    return Drain(state);
  }

  /// The reader the handler was handed, for a post-handler defensive demand.
  std::shared_ptr<AsyncFlightMessageReader> reader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return reader_;
  }

  /// The status the handler saw when its read ended.
  Status end_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return end_status_;
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

  mutable std::mutex mutex_;
  std::shared_ptr<AsyncFlightMessageReader> reader_;
  bool ended_ = false;
  Status end_status_;
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

  // The handler's second demand sees the same error, never an empty success.
  {
    auto reader = flight_server.reader();
    ASSERT_NE(nullptr, reader);
    auto again = reader->NextAsync();
    ASSERT_TRUE(again.is_finished());
    EXPECT_TRUE(again.status().IsCancelled())
        << "a second demand after a cancel must not look like a clean end: "
        << again.status();
  }

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

// A handler that breaks the contract: it writes again while the previous
// write is still in flight (a batch large enough to stay on the wire until the
// client reads), and records what the overlapping write returned.
class OverlappingWriteExchangeServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(
      const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader>,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    auto schema = arrow::schema({arrow::field("a", arrow::int64())});
    return writer->BeginAsync(schema).Then([this, writer, schema]() -> arrow::Future<> {
      ARROW_ASSIGN_OR_RAISE(auto column,
                            arrow::MakeArrayFromScalar(arrow::Int64Scalar(7), 2 << 20));
      const auto batch = arrow::RecordBatch::Make(schema, column->length(), {column});
      auto first = writer->WriteRecordBatchAsync(*batch);
      first_was_pending = !first.is_finished();
      auto overlapping = writer->WriteMetadataAsync(Buffer::FromString("overlapping"));
      overlapping_status = overlapping.is_finished()
                               ? overlapping.status()
                               : Status::UnknownError("the overlapping write is pending");
      overlapping_write_made.MarkFinished();
      return first;
    });
  }

  // Set on the gRPC thread before overlapping_write_made is marked finished.
  bool first_was_pending = false;
  Status overlapping_status;
  arrow::Future<> overlapping_write_made = arrow::Future<>::Make();
};

TEST(AsyncGrpcTest, DoExchangeOverlappingWriteIsRejectedAndNotSent) {
  // A write made before the previous write's future completed is rejected with
  // Invalid: the rejected message must not reach the client anyway.
  FlightServerOptions options(LocalhostLocation());
  OverlappingWriteExchangeServer flight_server;
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  ASSERT_OK_AND_ASSIGN(auto exchange,
                       client->DoExchange(FlightDescriptor::Command("overlap")));
  ASSERT_OK(exchange.writer->Begin(arrow::schema({arrow::field("a", arrow::int64())})));
  // Nothing is read until the overlapping write was made, so the large batch
  // is still in flight when it is.
  ASSERT_TRUE(flight_server.overlapping_write_made.Wait(10));
  ASSERT_TRUE(flight_server.first_was_pending) << "the first write landed too early";
  EXPECT_TRUE(flight_server.overlapping_status.IsInvalid())
      << flight_server.overlapping_status;

  int batches = 0;
  int metadata_messages = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, exchange.reader->Next());
    if (chunk.data) {
      ++batches;
    } else if (chunk.app_metadata) {
      ++metadata_messages;
    } else {
      break;
    }
  }
  EXPECT_EQ(1, batches);
  EXPECT_EQ(0, metadata_messages) << "the rejected write was sent anyway";

  ASSERT_OK(exchange.writer->DoneWriting());
  ASSERT_OK(exchange.writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

namespace {

/// One IPC message at a time from a RecordBatchStream, as a FlightPayload.
class DecoderStreamForExchange {
 public:
  explicit DecoderStreamForExchange(std::shared_ptr<Schema> schema,
                                    std::vector<std::shared_ptr<RecordBatch>> batches)
      : stream_(arrow::RecordBatchReader::Make(std::move(batches), std::move(schema))
                    .ValueOrDie()) {}

  FlightPayload SchemaMessage() { return Get(stream_.GetSchemaPayload()); }
  FlightPayload Next() { return Get(stream_.Next()); }

 private:
  static FlightPayload Get(arrow::Result<FlightPayload> payload) {
    ARROW_EXPECT_OK(payload);
    return std::move(payload).ValueOrDie();
  }
  RecordBatchStream stream_;
};

/// A server that captures the reader/writer of the exchange and parks its
/// completion, so a test can drive the application's real handles against a
/// ManualServerStream.
class CapturingExchangeServer final : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(
      const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    this->reader = std::move(reader);
    this->writer = std::move(writer);
    descriptor = this->reader->descriptor();
    entered.MarkFinished();
    return completion;
  }

  std::shared_ptr<AsyncFlightMessageReader> reader;
  std::shared_ptr<AsyncFlightMessageWriter> writer;
  FlightDescriptor descriptor;
  Future<> entered = Future<>::Make();
  Future<> completion = Future<>::Make();
};

/// One DoExchange RPC driven by hand: a real ExchangeReactor bound to a manual
/// stream, with the call context (middleware, auth) the service would build.
class ManualExchange {
 public:
  ManualExchange() {
    helper_ =
        std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
            /*auth_handler=*/nullptr, MiddlewareFactoryList{});
    auto* context = &stream_.context();
    helper_->MakeCallContext(FlightMethod::DoExchange, context, &flight_context_);
    reactor_ = detail::MakeExchangeReactor(std::move(flight_context_), &server_);
    stream_.Bind(reactor_);
  }

  ~ManualExchange() {
    // A failed test must not leave the reactor holding a callback this fixture
    // will never deliver, nor a live reactor behind for the next test. gRPC's
    // order is Finish, then the outstanding callbacks, then OnDone.
    stream_.DrainWrites();
    if (stream_.read_pending()) {
      stream_.CloseRead();
    }
    stream_.DrainWrites();
    if (stream_.finish_count() == 0 || stream_.done()) {
      return;
    }
    stream_.Done();
  }

  /// Deliver the descriptor-only first message, as FlightClient does, and wait
  /// for the handler to be entered.
  void Start(const FlightDescriptor& descriptor) {
    FlightPayload payload;
    EXPECT_TRUE(internal::ToPayload(descriptor, &payload.descriptor).ok());
    stream_.DeliverRead(Serialize(payload));
    EXPECT_TRUE(server_.entered.Wait(10)) << "the exchange handler never started";
    ASSERT_EQ(descriptor, server_.descriptor);
  }

  /// Deliver one message of the exchange.
  void Deliver(const FlightPayload& payload) { stream_.DeliverRead(Serialize(payload)); }
  /// Deliver one already-serialized message.
  void DeliverRaw(const ::grpc::ByteBuffer& message) { stream_.DeliverRead(message); }
  /// Close the read side: the client half-closed.
  void HalfClose() { stream_.CloseRead(); }

  static ::grpc::ByteBuffer Serialize(const FlightPayload& payload) {
    ::grpc::ByteBuffer out;
    bool own_buffer = false;
    const auto status = FlightDataSerialize(payload, &out, &own_buffer);
    EXPECT_TRUE(status.ok()) << status.error_message();
    return out;
  }

  CapturingExchangeServer server_;
  ManualServerStream stream_;

 private:
  std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper_;
  detail::AsyncCallContext flight_context_{&stream_.context()};
  ::grpc::ServerGenericBidiReactor* reactor_ = nullptr;
};

}  // namespace

TEST(AsyncGrpcTest, ExchangeReaderContract) {
  // The reader's contract on a live call: one demand at a time, the
  // descriptor-only first message is not a chunk, metadata is a value, the schema
  // is read through, and a clean end repeats as an empty chunk.
  ManualExchange exchange;
  auto schema = TwoColumnSchema();
  exchange.Start(FlightDescriptor::Command("reader-contract"));

  // The descriptor-only first message carries nothing to hand the application:
  // the demand stays pending and exactly one read is armed for the next message.
  auto demand = exchange.server_.reader->NextAsync();
  EXPECT_FALSE(demand.is_finished()) << "the descriptor-only message is not a chunk";
  ASSERT_TRUE(exchange.stream_.read_pending()) << "the reader must arm the next read";

  // A concurrent demand is refused at once, and the first one keeps going.
  auto second = exchange.server_.reader->NextAsync();
  ASSERT_FALSE(second.status().ok());
  EXPECT_THAT(second.status().ToString(),
              ::testing::HasSubstr("one NextAsync at a time"));

  // The metadata-only message completes the first demand as a value.
  FlightPayload metadata;
  metadata.app_metadata = Buffer::FromString("reader-metadata");
  exchange.Deliver(metadata);
  ASSERT_TRUE(demand.is_finished()) << "the metadata message must settle the demand";
  ASSERT_OK_AND_ASSIGN(auto chunk, demand.result());
  EXPECT_EQ(nullptr, chunk.data);
  ASSERT_NE(nullptr, chunk.app_metadata);
  EXPECT_EQ("reader-metadata", chunk.app_metadata->ToString());

  // A schema message is read through: the demand stays pending, one read armed.
  auto after_schema = exchange.server_.reader->NextAsync();
  ASSERT_FALSE(after_schema.is_finished());
  DecoderStreamForExchange stream(schema, {});
  exchange.Deliver(stream.SchemaMessage());
  EXPECT_FALSE(after_schema.is_finished()) << "a schema message is not a chunk";
  ASSERT_TRUE(exchange.stream_.read_pending()) << "the reader re-armed the next read";

  // The clean half-close completes the pending demand as the empty end chunk.
  exchange.HalfClose();
  ASSERT_TRUE(after_schema.is_finished()) << "the half-close must settle the demand";
  ASSERT_TRUE(after_schema.status().ok()) << after_schema.status();
  const FlightStreamChunk& end = *after_schema.result();
  EXPECT_EQ(nullptr, end.data) << "the end of the exchange is an empty chunk";
  EXPECT_EQ(nullptr, end.app_metadata);
  EXPECT_FALSE(exchange.stream_.read_pending())
      << "no read may be armed after the read side closed";

  // Repeated demand after a clean end is the same empty chunk, not an error.
  auto again = exchange.server_.reader->NextAsync();
  ASSERT_TRUE(again.is_finished()) << "a demand after the end must resolve at once";
  ASSERT_TRUE(again.status().ok()) << again.status();
  EXPECT_EQ(nullptr, again.result()->data);

  exchange.server_.completion.MarkFinished();
  ASSERT_TRUE(exchange.stream_.finish_observed().Wait(10));
  EXPECT_TRUE(exchange.stream_.finish_status()->ok())
      << exchange.stream_.finish_status()->error_message();
}

TEST(AsyncGrpcTest, ExchangeReaderReportsErrorsOnRepeatedDemand) {
  // Cancellation and a deserialization failure stay errors on repeated demand:
  // neither may become a successful end of input.
  {
    SCOPED_TRACE("cancelled");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("cancel"));
    auto demand = exchange.server_.reader->NextAsync();
    ASSERT_FALSE(demand.is_finished());
    exchange.stream_.Cancel();
    EXPECT_TRUE(demand.is_finished());
    EXPECT_TRUE(demand.status().IsCancelled()) << demand.status();
    EXPECT_TRUE(exchange.server_.reader->NextAsync().status().IsCancelled());
  }
  {
    SCOPED_TRACE("malformed message");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("malformed"));
    auto demand = exchange.server_.reader->NextAsync();
    ASSERT_FALSE(demand.is_finished());
    ::grpc::Slice garbage(std::string("\x0a\x7f\x01\x02", 4));
    exchange.DeliverRaw(::grpc::ByteBuffer(&garbage, 1));
    ASSERT_TRUE(demand.is_finished());
    EXPECT_FALSE(demand.status().ok());
    EXPECT_FALSE(exchange.server_.reader->NextAsync().status().ok())
        << "a read failure must replay, not turn into a clean end";
  }
}

TEST(AsyncGrpcTest, ExchangeReaderWithNoDemandBuffersOneMessage) {
  // The reader is also a defensive internal type: a message that arrives while no
  // demand exists is held, and the next demand returns that buffered value once,
  // then the empty chunk. Driven directly on a reader with no reactor.
  auto reader = std::make_shared<detail::ExchangeReader>(nullptr);
  internal::FlightData data;
  data.app_metadata = Buffer::FromString("buffered");
  reader->OnMessage(std::move(data));

  auto first = reader->NextAsync();
  ASSERT_TRUE(first.is_finished()) << "a buffered message needs no read";
  ASSERT_OK_AND_ASSIGN(auto chunk, first.result());
  ASSERT_NE(nullptr, chunk.app_metadata);
  EXPECT_EQ("buffered", chunk.app_metadata->ToString());

  // A message that arrives with no demand is held and handed over once: the next
  // demand then sees the end the fixture reported, not the buffered value again.
  reader->OnReadClosed();
  auto second = reader->NextAsync();
  ASSERT_TRUE(second.is_finished());
  ASSERT_OK_AND_ASSIGN(auto end, second.result());
  EXPECT_EQ(nullptr, end.data);
  EXPECT_EQ(nullptr, end.app_metadata);
}

TEST(AsyncGrpcTest, ExchangeWriterContract) {
  // The writer's ordered refusals: a batch before Begin, a mismatched schema, and
  // every call after Close. None of them may put anything on the wire. Every flush
  // future completes only when its write does, so each is drained before it is
  // examined.
  ManualExchange exchange;
  exchange.Start(FlightDescriptor::Command("writer-contract"));
  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);

  auto write_before_begin = exchange.server_.writer->WriteRecordBatchAsync(*batch);
  ASSERT_FALSE(write_before_begin.status().ok());
  EXPECT_THAT(write_before_begin.status().ToString(),
              ::testing::HasSubstr("not started"));
  EXPECT_TRUE(exchange.stream_.writes().empty());

  auto begun = exchange.server_.writer->BeginAsync(schema);
  ASSERT_TRUE(exchange.stream_.write_pending()) << "Begin must write the schema";
  exchange.stream_.DrainWrites();
  ASSERT_TRUE(begun.is_finished());
  ASSERT_TRUE(begun.status().ok()) << begun.status();
  ASSERT_EQ(1u, exchange.stream_.writes().size());

  // Begin twice, once the first has completed.
  auto again = exchange.server_.writer->BeginAsync(schema);
  ASSERT_FALSE(again.status().ok());
  EXPECT_THAT(again.status().ToString(), ::testing::HasSubstr("already been started"));
  EXPECT_EQ(1u, exchange.stream_.writes().size());

  // A batch whose schema does not match is refused before it is serialized.
  auto mismatched =
      exchange.server_.writer->WriteRecordBatchAsync(*OneColumnBatch(OneColumnSchema()));
  exchange.stream_.DrainWrites();
  EXPECT_FALSE(mismatched.status().ok()) << mismatched.status();
  EXPECT_EQ(1u, exchange.stream_.writes().size())
      << "the mismatched batch must not reach the wire";

  // A valid batch still works after the refusal.
  auto valid = exchange.server_.writer->WriteRecordBatchAsync(*batch);
  exchange.stream_.DrainWrites();
  ASSERT_TRUE(valid.is_finished());
  EXPECT_TRUE(valid.status().ok()) << valid.status();
  EXPECT_EQ(2u, exchange.stream_.writes().size());

  auto closed = exchange.server_.writer->CloseAsync();
  exchange.stream_.DrainWrites();
  ASSERT_TRUE(closed.is_finished());
  EXPECT_TRUE(closed.status().ok()) << closed.status();
  auto after_close = exchange.server_.writer->WriteRecordBatchAsync(*batch);
  ASSERT_FALSE(after_close.status().ok());
  EXPECT_THAT(after_close.status().ToString(), ::testing::HasSubstr("not started"));
  EXPECT_EQ(2u, exchange.stream_.writes().size())
      << "a write after Close must not reach the wire";

  exchange.server_.completion.MarkFinished();
  ASSERT_TRUE(exchange.stream_.finish_observed().Wait(10));
  EXPECT_TRUE(exchange.stream_.finish_status()->ok())
      << exchange.stream_.finish_status()->error_message();
}

TEST(AsyncGrpcTest, DoExchangeOverlappingWritesAreRejected) {
  // While one write is in flight every other write, of every kind, is refused with
  // Invalid and never reaches the wire. Once the first write completes the writer
  // accepts the next one. (A second Begin is refused by the begun-writer check,
  // covered by ExchangeWriterContract.)
  struct Case {
    std::string name;
    std::function<arrow::Future<>(AsyncFlightMessageWriter*, const RecordBatch&,
                                  std::shared_ptr<Schema>)>
        rejected;
  };
  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);
  const std::vector<Case> cases = {
      {"record batch",
       [&](AsyncFlightMessageWriter* writer, const RecordBatch& b,
           std::shared_ptr<Schema>) { return writer->WriteRecordBatchAsync(b); }},
      {"batch with metadata",
       [&](AsyncFlightMessageWriter* writer, const RecordBatch& b,
           std::shared_ptr<Schema>) {
         return writer->WriteWithMetadataAsync(b, Buffer::FromString("overlap"));
       }},
      {"metadata",
       [&](AsyncFlightMessageWriter* writer, const RecordBatch&,
           std::shared_ptr<Schema>) {
         return writer->WriteMetadataAsync(Buffer::FromString("overlap"));
       }},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("overlap"));
    auto begun = exchange.server_.writer->BeginAsync(schema);
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(begun.is_finished());
    ASSERT_TRUE(begun.status().ok()) << begun.status();
    const auto writes_before = exchange.stream_.writes().size();

    // Leave the first write's completion pending, then offer the overlapping one.
    auto first = exchange.server_.writer->WriteRecordBatchAsync(*batch);
    ASSERT_TRUE(exchange.stream_.write_pending()) << "the first write never started";
    ASSERT_FALSE(first.is_finished()) << "the first write must still be in flight";
    auto rejected = test.rejected(exchange.server_.writer.get(), *batch, schema);
    ASSERT_FALSE(rejected.status().ok())
        << "the overlapping write must be refused immediately";
    EXPECT_THAT(rejected.status().ToString(), ::testing::HasSubstr("already in flight"));
    EXPECT_EQ(writes_before + 1, exchange.stream_.writes().size())
        << "the rejected write must not reach the wire";

    // Complete the first write: the writer is usable again.
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(first.is_finished());
    EXPECT_TRUE(first.status().ok()) << first.status();
    auto next = exchange.server_.writer->WriteMetadataAsync(Buffer::FromString("next"));
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(next.is_finished());
    EXPECT_TRUE(next.status().ok()) << next.status();
    EXPECT_EQ(writes_before + 2, exchange.stream_.writes().size());

    auto closed = exchange.server_.writer->CloseAsync();
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(closed.is_finished());
    ASSERT_TRUE(closed.status().ok()) << closed.status();
    exchange.server_.completion.MarkFinished();
    ASSERT_TRUE(exchange.stream_.finish_observed().Wait(10));
  }
}

TEST(AsyncGrpcTest, ExchangeWriterWriteOutcomes) {
  // A failed flush fails its own future, cancellation while a flush is pending
  // fails it as cancelled, and a reentrant write from a continuation still works.
  {
    SCOPED_TRACE("write failed");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("write-failure"));
    auto schema = TwoColumnSchema();
    auto begun = exchange.server_.writer->BeginAsync(schema);
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(begun.is_finished());
    ASSERT_TRUE(begun.status().ok()) << begun.status();
    auto write = exchange.server_.writer->WriteMetadataAsync(Buffer::FromString("x"));
    ASSERT_TRUE(exchange.stream_.write_pending());
    ASSERT_FALSE(write.is_finished());
    exchange.stream_.CompleteWrite(false);
    ASSERT_TRUE(write.is_finished());
    EXPECT_FALSE(write.status().ok()) << write.status();
    EXPECT_THAT(write.status().ToString(),
                ::testing::HasSubstr("Failed to write response"));
  }
  {
    SCOPED_TRACE("cancelled while pending");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("cancel-flush"));
    auto schema = TwoColumnSchema();
    auto begun = exchange.server_.writer->BeginAsync(schema);
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(begun.is_finished());
    ASSERT_TRUE(begun.status().ok()) << begun.status();
    auto write = exchange.server_.writer->WriteMetadataAsync(Buffer::FromString("y"));
    ASSERT_FALSE(write.is_finished());
    const auto writes_before = exchange.stream_.writes().size();
    exchange.stream_.Cancel();
    ASSERT_TRUE(write.is_finished());
    EXPECT_TRUE(write.status().IsCancelled()) << write.status();
    EXPECT_EQ(writes_before, exchange.stream_.writes().size())
        << "nothing more may be pumped after the cancellation";
  }
  {
    SCOPED_TRACE("reentrant write after a successful flush");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("reentrant"));
    auto schema = TwoColumnSchema();
    auto begun = exchange.server_.writer->BeginAsync(schema);
    exchange.stream_.DrainWrites();
    ASSERT_TRUE(begun.is_finished());
    ASSERT_TRUE(begun.status().ok()) << begun.status();
    auto writer = exchange.server_.writer;
    auto first = writer->WriteMetadataAsync(Buffer::FromString("one"));
    arrow::Future<> second = arrow::Future<>::Make();
    first.AddCallback([writer, second](const Status& status) mutable {
      if (!status.ok()) {
        second.MarkFinished(status);
        return;
      }
      auto chained = writer->WriteMetadataAsync(Buffer::FromString("two"));
      chained.AddCallback([second](const Status& s) mutable { second.MarkFinished(s); });
    });
    // Both flushes complete in order: the first write's continuation starts the
    // second, which is then completed on this thread.
    ASSERT_TRUE(WaitFor([&] {
      exchange.stream_.DrainWrites();
      return second.is_finished();
    })) << "the reentrant write never completed";
    EXPECT_TRUE(second.status().ok()) << second.status();
    auto writes = exchange.stream_.writes();
    EXPECT_EQ(3u, writes.size()) << "both messages must be sent";
  }
}

TEST(AsyncGrpcTest, ExchangeHandlesRefuseAfterTheCallEnds) {
  // The public handles are documented as unusable once the handler future
  // completes. The refusals are ordered, not uniform: a write is refused as
  // Cancelled because the dead exchange is checked first, while Close reports
  // Invalid "not started" without a begun writer.
  ManualExchange exchange;
  exchange.Start(FlightDescriptor::Command("detached"));
  auto reader = exchange.server_.reader;
  auto writer = exchange.server_.writer;
  auto schema = TwoColumnSchema();

  exchange.server_.completion.MarkFinished();
  ASSERT_TRUE(exchange.stream_.finish_observed().Wait(10));
  exchange.stream_.DrainWrites();
  exchange.stream_.Done();

  // The reader's demand on a finished call reports the cancellation, not a clean
  // end of input.
  auto demand = reader->NextAsync();
  ASSERT_TRUE(demand.is_finished());
  // A detached reader asks the dead exchange for nothing and reports its end.
  ASSERT_TRUE(demand.status().ok()) << demand.status();
  EXPECT_EQ(nullptr, demand.result()->data);
  EXPECT_EQ(nullptr, demand.result()->app_metadata);

  // Without a begun writer the "not started" validation runs first; the dead
  // exchange is only visible once the writer has been started.
  auto unbegun = writer->WriteRecordBatchAsync(*OneColumnBatch(OneColumnSchema()));
  ASSERT_FALSE(unbegun.status().ok());
  EXPECT_THAT(unbegun.status().ToString(), ::testing::HasSubstr("not started"));
  auto begun = writer->BeginAsync(schema);
  exchange.stream_.DrainWrites();  // the dead exchange refuses the write it queues
  ASSERT_FALSE(begun.status().ok());
  EXPECT_TRUE(begun.status().IsCancelled()) << begun.status();
}

TEST(AsyncGrpcTest, ExchangeHandlerCompletionStatus) {
  // The handler's future status is the RPC's: OK reaches the client as a clean end,
  // an IOError as that error.
  for (const bool fails : {false, true}) {
    SCOPED_TRACE(fails ? "handler fails" : "handler succeeds");
    ManualExchange exchange;
    exchange.Start(FlightDescriptor::Command("completion"));
    if (fails) {
      exchange.server_.completion.MarkFinished(Status::IOError("handler failed"));
    } else {
      exchange.server_.completion.MarkFinished();
    }
    ASSERT_TRUE(exchange.stream_.finish_observed().Wait(10));
    const auto status = *exchange.stream_.finish_status();
    if (fails) {
      EXPECT_FALSE(status.ok());
      EXPECT_THAT(status.error_message(), ::testing::HasSubstr("handler failed"));
    } else {
      EXPECT_TRUE(status.ok()) << status.error_message();
    }
  }
}

}  // namespace arrow::flight::transport::grpc
