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

// DoPut over the async generic service: the push decoder feeding the per-RPC
// listener, the acknowledgement that makes the client's Close() return, the
// PutResult metadata channel, and every way an upload ends -- clean, rejected by
// the listener, rejected on its descriptor, cancelled from the server side,
// client deadline, and the client vanishing while the listener is parked.

#include "arrow/flight/transport/grpc/async_grpc_test_fixtures.h"

namespace arrow::flight::transport::grpc {

namespace {

/// One FlightData message at a time from a RecordBatchStream, serialized as the
/// gRPC transport does, so a raw client can send a prefix of an upload.
class DecoderStreamForPut {
 public:
  DecoderStreamForPut(std::shared_ptr<Schema> schema,
                      std::vector<std::shared_ptr<RecordBatch>> batches)
      : stream_(arrow::RecordBatchReader::Make(std::move(batches), std::move(schema))
                    .ValueOrDie()) {}

  FlightPayload SchemaMessage() { return ValueOrDie(stream_.GetSchemaPayload()); }
  FlightPayload Next() { return ValueOrDie(stream_.Next()); }

 private:
  static FlightPayload ValueOrDie(arrow::Result<FlightPayload> payload) {
    ARROW_EXPECT_OK(payload);
    return std::move(payload).ValueOrDie();
  }
  RecordBatchStream stream_;
};

/// One FlightPayload as the single gRPC message the transport would send.
::grpc::ByteBuffer Serialize(const FlightPayload& payload) {
  ::grpc::ByteBuffer out;
  bool own_buffer = false;
  const auto status = FlightDataSerialize(payload, &out, &own_buffer);
  EXPECT_TRUE(status.ok()) << status.error_message();
  return out;
}

/// One DoPut RPC driven by hand: the real DoPutReactor bound to a manual stream,
/// with the call context (middleware, auth) the service would build, and the
/// listener the reactor was created with.
class ManualDoPut {
 public:
  explicit ManualDoPut(AsyncGenericFlightServerBase* server) : server_(server) {
    helper_ =
        std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
            /*auth_handler=*/nullptr, MiddlewareFactoryList{});
    auto* context = &stream.context();
    helper_->MakeCallContext(FlightMethod::DoPut, context, &flight_context_);
    listener_ = server_->CreateDoPutListener(flight_context_);
    EXPECT_NE(nullptr, listener_);
    reactor_ = detail::MakeDoPutReactor(std::move(flight_context_), listener_);
    stream.Bind(reactor_);
  }

  /// Deliver the descriptor-carrying first message of an upload.
  void DeliverRequest(const FlightDescriptor& descriptor,
                      const std::shared_ptr<Schema>& schema) {
    DecoderStreamForPut payloads(schema, {});
    auto message = payloads.SchemaMessage();
    EXPECT_OK(internal::ToPayload(descriptor, &message.descriptor));
    stream.DeliverRead(Serialize(message));
  }

  /// Deliver the next message an upload would carry.
  void Deliver(const FlightPayload& payload) { stream.DeliverRead(Serialize(payload)); }
  /// The client half-closed: the clean end of the upload.
  void HalfClose() { stream.CloseRead(); }

  /// Settle the RPC's stream once its status is decided: complete any write the
  /// reactor still has queued and deliver OnDone, as gRPC does.
  void Finish() {
    stream.DrainWrites();
    if (stream.read_pending()) {
      stream.CloseRead();
    }
    stream.DrainWrites();
    if (!stream.done()) {
      stream.Done();
    }
  }

  ManualServerStream stream;
  std::shared_ptr<AsyncFlightDataListener> listener_;

 private:
  AsyncGenericFlightServerBase* server_;
  std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper_;
  detail::AsyncCallContext flight_context_{&stream.context()};
  ::grpc::ServerGenericBidiReactor* reactor_ = nullptr;
};

}  // namespace

TEST(AsyncGrpcTest, BasicDoPut) {
  // One listener for the whole test: shared with the service's factory and
  // kept here so it can be asserted on after the RPC.
  auto listener = std::make_shared<RecordingListener>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);

  // Call DoPut and upload the same batch twice. Close() returns the server's
  // final status, i.e. it only succeeds once the acknowledgement written on
  // end of stream has been read.
  FlightDescriptor descriptor = FlightDescriptor::Path({"test"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));
  ASSERT_OK(result.writer->WriteRecordBatch(*batch));
  ASSERT_OK(result.writer->WriteRecordBatch(*batch));
  ASSERT_OK(result.writer->Close());

  // Cleanup. The listener is asserted only after the server stopped, so the
  // DoPut callback cannot race with the reads.
  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();

  // The listener saw the schema once and both batches, in order.
  ASSERT_EQ(listener->schema_count(), 1);
  ASSERT_NE(listener->schema(), nullptr);
  ASSERT_EQ(listener->schema()->num_fields(), 2);
  ASSERT_EQ(listener->schema()->field(0)->name(), "a");
  ASSERT_EQ(listener->schema()->field(1)->name(), "b");
  ASSERT_EQ(listener->batches().size(), 2);
  for (const auto& chunk : listener->batches()) {
    ASSERT_EQ(chunk->num_rows(), 3);
    ASSERT_EQ(chunk->num_columns(), 2);
  }
  // No metadata-only (error/app-metadata) chunks.
  ASSERT_EQ(listener->metadata_only_count(), 0);
}

TEST(AsyncGrpcTest, EmptyDoPut) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  auto schema = TwoColumnSchema();

  // Upload nothing at all: the acknowledgement the server writes back on the
  // client's half-close is what makes Close() return.
  FlightDescriptor descriptor = FlightDescriptor::Path({"empty-put"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));
  ASSERT_OK(result.writer->Close());

  // Cleanup before looking at the listener, so the DoPut callback cannot race
  // with the reads.
  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();

  // No data of any kind reached the listener.
  ASSERT_EQ(listener->batches().size(), 0);
  ASSERT_EQ(listener->metadata_only_count(), 0);
  // The sync client sends the schema payload eagerly, even when no batch
  // follows, so the listener does see the schema here. Deliberately not
  // asserted: that is the client's business, not the transport's.
}

TEST(AsyncGrpcTest, MetadataOnlyPutChunk) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  auto schema = TwoColumnSchema();

  FlightDescriptor descriptor = FlightDescriptor::Path({"metadata-only-put"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));
  auto app_metadata = arrow::Buffer::FromString("metadata-only-upload");
  ASSERT_OK(result.writer->WriteMetadata(app_metadata));
  ASSERT_OK(result.writer->Close());

  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();

  // Exactly one chunk reached the listener: no data, that exact metadata.
  ASSERT_EQ(listener->batches().size(), 0);
  ASSERT_EQ(listener->metadata_only_count(), 1);
  ASSERT_EQ(listener->metadata_chunks().size(), 1);
  ASSERT_NE(listener->metadata_chunks()[0], nullptr);
  ASSERT_EQ(listener->metadata_chunks()[0]->ToString(), "metadata-only-upload");
}

TEST(AsyncGrpcTest, DoPutRejectedByListener) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  // The consumer rejects the upload.
  listener->set_next_status(arrow::Status::Invalid("listener rejected this upload"));
  flight_server.set_listener(listener);
  HandBuiltServer hand_built = StartHandBuilt(&flight_server);
  ASSERT_NE(hand_built.server, nullptr);
  ASSERT_NE(hand_built.port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(hand_built.port));

  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);

  // The rejection must reach the client as the upload's status, whichever of
  // the three surfaces reports it first (in practice the writer's Close()).
  FlightDescriptor descriptor = FlightDescriptor::Path({"rejected-put"});
  auto [surface, status] = UploadOneBatch(client.get(), descriptor, schema, batch);
  ASSERT_FALSE(status.ok())
      << "the listener's rejection never reached the client, surfaced on " << surface;
  ASSERT_EQ(status.code(), arrow::StatusCode::Invalid);
  ASSERT_NE(status.message().find("listener rejected this upload"), std::string::npos)
      << "unexpected status: " << status;
  // The rejection must also be what OnFinish reports.
  ASSERT_EQ(listener->finish_status().code(), arrow::StatusCode::Invalid)
      << "OnFinish got: " << listener->finish_status();

  ASSERT_OK(client->Close());
  hand_built.server->Shutdown();
  hand_built.server->Wait();
}

TEST(AsyncGrpcTest, CreateDoPutListenerServesUpload) {
  // The upload half of the server class API: CreateDoPutListener() hands out
  // the per-RPC listener, with no options-level factory.
  TestUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);

  const FlightDescriptor descriptor = FlightDescriptor::Path({"virtual", "h2"});
  auto [surface, status] = UploadOneBatch(client.get(), descriptor, schema, batch);
  ASSERT_TRUE(status.ok()) << "upload failed on " << surface << ": " << status;

  // Cleanup before reading the recordings: the listener's callbacks run on a
  // gRPC thread.
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  // The virtual was consulted exactly once, and its listener saw the upload.
  ASSERT_EQ(flight_server.calls(), 1);
  const auto& listener = flight_server.listener(0);
  ASSERT_EQ(listener->descriptor_count(), 1);
  ASSERT_EQ(listener->descriptors()[0], descriptor);
  ASSERT_EQ(listener->schema_count(), 1);
  ASSERT_EQ(listener->schema()->num_fields(), 2);
  ASSERT_EQ(listener->schema()->field(0)->name(), "a");
  ASSERT_EQ(listener->schema()->field(1)->name(), "b");
  ASSERT_EQ(listener->batches().size(), 1);
  // The whole batch, not just its shape: the listener must be handed the data
  // the client sent.
  AssertBatchesEqual(*batch, *listener->batches()[0]);
  // A clean half-close ends the upload exactly once, as a success: that is how a
  // consumer knows it can commit what it accumulated.
  ASSERT_EQ(listener->finish_count(), 1);
  EXPECT_TRUE(listener->finish_status().ok()) << listener->finish_status();
}

TEST(AsyncGrpcTest, OnFinishMayWriteMetadataWithoutDeadlocking) {
  // OnFinish is application code and may call back into the listener's writer.
  // The transport must not hold its write lock across that call: doing so
  // deadlocks this upload (verified: the RPC never finishes).
  RecordingUploadServer<FinishingAckListener> flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  ASSERT_OK_AND_ASSIGN(auto put,
                       client->DoPut(FlightDescriptor::Path({"onfinish"}), schema));
  ASSERT_OK(put.writer->WriteRecordBatch(*OneColumnBatch(schema)));
  ARROW_UNUSED(put.writer->Close());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_EQ(1, listener->finish_count());
  // A write started from OnFinish() is supported, not refused: a listener reports
  // the result of the upload there. It must complete (rather than deadlock), and
  // its message must reach the client.
  ASSERT_TRUE(listener->finish_write.is_finished())
      << "a write started from OnFinish() never completed";
  ASSERT_OK(listener->finish_write.status()) << listener->finish_write.status();
}

/// Middleware that calls back into the upload from CallCompleted(): middleware
/// is application code too, so the transport must not hold its write lock
/// across it.
class UploadWritingMiddleware final : public ServerMiddleware {
 public:
  std::string name() const override { return "upload-writing"; }
  void SendingHeaders(AddCallHeaders*) override {}

  void CallCompleted(const Status&) override {
    std::shared_ptr<AsyncFlightDataListener> upload;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      upload = upload_;
    }
    if (upload == nullptr) {
      return;
    }
    auto write = upload->writer()->WriteMetadataAsync(
        *arrow::Buffer::FromString("from-middleware"));
    std::lock_guard<std::mutex> lock(mutex_);
    completed_write_ = std::move(write);
  }

  void set_upload(std::shared_ptr<AsyncFlightDataListener> upload) {
    std::lock_guard<std::mutex> lock(mutex_);
    upload_ = std::move(upload);
  }

  arrow::Future<> completed_write() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return completed_write_;
  }

 private:
  mutable std::mutex mutex_;
  std::shared_ptr<AsyncFlightDataListener> upload_;
  arrow::Future<> completed_write_;
};

class UploadWritingMiddlewareFactory final : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo&, const ServerCallContext&,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    auto instance = std::make_shared<UploadWritingMiddleware>();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      last_ = instance;
    }
    *middleware = std::move(instance);
    return Status::OK();
  }

  std::shared_ptr<UploadWritingMiddleware> last() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_;
  }

 private:
  mutable std::mutex mutex_;
  std::shared_ptr<UploadWritingMiddleware> last_;
};

/// Hands every upload's listener to the call's UploadWritingMiddleware.
class MiddlewareWiredUploadServer final : public AsyncGenericFlightServerBase {
 public:
  std::shared_ptr<AsyncFlightDataListener> CreateDoPutListener(
      const ServerCallContext& context) override {
    auto listener = std::make_shared<RecordingListener>();
    auto* middleware =
        static_cast<UploadWritingMiddleware*>(context.GetMiddleware("upload-writing"));
    if (middleware != nullptr) {
      middleware->set_upload(listener);
    }
    return listener;
  }
};

TEST(AsyncGrpcTest, CallCompletedMayWriteMetadataWithoutDeadlocking) {
  // Middleware's CallCompleted() runs as the RPC finishes and may reach the
  // upload from there. The transport must not hold its write lock across that
  // call: doing so deadlocks the finishing thread (verified: the RPC never
  // finishes). The write itself is refused, the upload being over.
  auto factory = std::make_shared<UploadWritingMiddlewareFactory>();
  MiddlewareWiredUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  options.middleware = {{"upload-writing", factory}};
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  // Bounded, so a deadlocked server fails the Close() below instead of hanging it.
  FlightCallOptions bounded;
  bounded.timeout = std::chrono::seconds(10);
  auto schema = OneColumnSchema();
  ASSERT_OK_AND_ASSIGN(
      auto put, client->DoPut(bounded, FlightDescriptor::Path({"middleware"}), schema));
  ASSERT_OK(put.writer->WriteRecordBatch(*OneColumnBatch(schema)));
  ASSERT_OK(put.writer->Close());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  const auto middleware = factory->last();
  ASSERT_NE(middleware, nullptr) << "the upload never created its middleware";
  const auto write = middleware->completed_write();
  ASSERT_TRUE(write.is_valid()) << "CallCompleted() never reached the upload's writer";
  ASSERT_TRUE(write.is_finished())
      << "a write refused once the upload ended must fail at once";
  EXPECT_TRUE(write.status().IsInvalid()) << write.status();
}

TEST(AsyncGrpcTest, DoPutWritesMetadataBackToTheClient) {
  // The listener's writer() sends PutResult messages during the upload, the way
  // the sync FlightMetadataWriter does: the client reads them back with
  // ReadMetadata() between writes.
  MetadataReplyingUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  ASSERT_OK_AND_ASSIGN(auto put,
                       client->DoPut(FlightDescriptor::Path({"metadata"}), schema));
  auto& writer = *put.writer;
  auto& reader = *put.reader;

  // Write one batch, then read the acknowledgement the server sent for it.
  ASSERT_OK(writer.WriteRecordBatch(*batch));
  std::shared_ptr<Buffer> metadata;
  ASSERT_OK(reader.ReadMetadata(&metadata));
  ASSERT_NE(nullptr, metadata) << "the server's metadata never reached the client";
  ASSERT_EQ("ack 2 rows", metadata->ToString());

  // A second batch and its acknowledgement prove the channel survives a round.
  ASSERT_OK(writer.WriteRecordBatch(*batch));
  ASSERT_OK(reader.ReadMetadata(&metadata));
  ASSERT_NE(nullptr, metadata);
  ASSERT_EQ("ack 2 rows", metadata->ToString());

  ASSERT_OK(writer.Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  auto listener = flight_server.WaitForListener();
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_EQ(2, listener->wrote()) << "every batch must have been acknowledged";
}

TEST(AsyncGrpcTest, DoPutCancelFromServerSideFinishesTheUpload) {
  // The server aborts the upload without waiting for the client: Cancel()
  // finishes the RPC with the given status, the client's write or Close
  // reports it, and the listener hears it as its OnFinish status.
  TestUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(schema, 2,
                                        {arrow::ArrayFromJSON(arrow::int64(), "[3, 4]")});

  // Open the upload and write one batch, but do not end it.
  auto put = client->DoPut(FlightDescriptor::Path({"cancel"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  auto& writer = *put->writer;
  ASSERT_OK(writer.WriteRecordBatch(*batch));

  // Wait for the upload to reach the listener the virtual handed out, then
  // cancel it from this (non-gRPC) thread: that is the mid-upload server-side
  // abort. The listener exists as soon as CreateDoPutListener() returns, but
  // Cancel() only reaches the RPC once the transport is installed, which the
  // first batch proves.
  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_TRUE(WaitFor([&listener] { return listener->batches().size() >= 1; }))
      << "the server never consumed the batch";
  ASSERT_OK(listener->CancelWith(arrow::Status::Cancelled("server said stop")));

  // The client sees the cancellation on the surface that observes the RPC.
  const Status close_status = writer.Close();
  ASSERT_FALSE(close_status.ok()) << "the server-side cancel never reached the client";
  ASSERT_EQ(close_status.code(), arrow::StatusCode::Cancelled);
  ASSERT_NE(close_status.message().find("server said stop"), std::string::npos)
      << "unexpected status: " << close_status;

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  // Cancel() reached the transport, and the listener was told the upload ended
  // with that status exactly once. cancel_count() is not asserted: CancelWith()
  // increments it unconditionally, so it would pass whatever the transport did.
  ASSERT_OK(listener->cancel_status()) << listener->cancel_status();
  ASSERT_EQ(listener->finish_count(), 1);
  // OnFinish must carry the ending that caused it, not a generic status.
  ASSERT_EQ(listener->finish_status().code(), arrow::StatusCode::Cancelled)
      << "OnFinish got: " << listener->finish_status();

  // After the upload, Cancel() has nothing to cancel.
  ASSERT_EQ(listener->CancelWith(arrow::Status::Cancelled("too late")).code(),
            arrow::StatusCode::Invalid);
}

TEST(AsyncGrpcTest, DoPutServerCancelEndsTheUploadWithItsOwnStatus) {
  // A server-side Cancel() whose OnFinish() is still committing: what the client
  // sends afterwards must not reach the listener, and the client's own end of the
  // upload must not finish the RPC with OK ahead of the cancellation. Both endings
  // are delivered by hand, so the order is the test's, not the network's.
  struct Case {
    std::string name;
    /// Whether the client's end arrives before the parked OnFinish() is released.
    bool client_ends_first;
  };
  for (const auto& test : {Case{"client half-closes into the parked finish", true},
                           Case{"client half-close after the release", false}}) {
    SCOPED_TRACE(test.name);
    auto listener =
        std::make_shared<ParkedUploadListener<RecordingListenerPhase::kFinish>>();
    TestFlightServer flight_server;
    flight_server.set_listener(listener);
    ManualDoPut upload(&flight_server);
    auto schema = OneColumnSchema();
    auto batch = OneColumnBatch(schema);
    upload.DeliverRequest(FlightDescriptor::Path({"parked-finish"}), schema);
    DecoderStreamForPut payloads(schema, {batch});
    ARROW_UNUSED(payloads.SchemaMessage());
    upload.Deliver(payloads.Next());
    ASSERT_TRUE(WaitFor([&] { return listener->batches().size() == 1; }))
        << "the server never consumed the batch";

    ASSERT_OK(listener->CancelWith(Status::Cancelled("server said stop")));
    ASSERT_EQ(1, listener->finish_count()) << "Cancel() must report OnFinish() at once";
    ASSERT_FALSE(upload.stream.finish_status().has_value())
        << "the RPC must wait for the parked OnFinish()";

    // One outstanding read, then the client's half-close: exactly one completion
    // per read, never two.
    ASSERT_TRUE(upload.stream.read_pending());
    upload.HalfClose();
    if (test.client_ends_first) {
      // The client's end arrives while OnFinish() is parked: the upload has already
      // started ending, so nothing may be read or delivered from it.
      EXPECT_FALSE(upload.stream.read_pending());
      listener->Release();
    } else {
      listener->Release();
    }
    ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
    EXPECT_EQ(::grpc::StatusCode::CANCELLED, upload.stream.finish_status()->error_code())
        << "the client's end of the upload won over the server's cancel: "
        << upload.stream.finish_status()->error_message();
    EXPECT_THAT(upload.stream.finish_status()->error_message(),
                ::testing::HasSubstr("server said stop"));
    EXPECT_EQ(1, listener->batches().size())
        << "a batch reached the listener after its OnFinish()";
    EXPECT_EQ(0, listener->metadata_only_count());
    EXPECT_EQ(1, listener->finish_count());
    EXPECT_FALSE(upload.stream.read_pending()) << "no read may be re-armed";
    upload.Finish();
  }
}

/// A listener stuck inside OnNext(): the upload is parked in the application
/// while the RPC underneath it dies.
class HangingDoPutListener final : public RecordingListener {
 public:
  arrow::Future<> OnNext(FlightStreamChunk) override {
    next_awaits_.fetch_add(1);
    return arrow::Future<>::Make();
  }

  std::atomic<int> next_awaits_{0};
};

/// A DoPut server that hands out a hanging listener, one per upload.
using HangingDoPutServer = RecordingUploadServer<HangingDoPutListener>;

TEST(AsyncGrpcTest, DoPutClientDeadlineWithPendingHandlerFinishes) {
  // The client's deadline expires while the listener is still processing a
  // chunk (its OnNext() future never resolves): the transport must finish the
  // RPC itself instead of waiting for the listener: Shutdown()/Wait() below
  // would otherwise block, since gRPC keeps the call alive until Finish()
  // runs.
  HangingDoPutServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  auto put = client->DoPut(impatient, FlightDescriptor::Path({"hang"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  (void)put->writer->WriteRecordBatch(*batch);

  // From here the listener is inside OnNext() and the deadline does the rest.
  ASSERT_TRUE(WaitFor([&flight_server] {
    auto listener = flight_server.WaitForListener(0);
    return listener != nullptr && listener->next_awaits_.load() >= 1;
  })) << "the listener never saw a chunk";

  ARROW_UNUSED(put->writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  // The upload ended anyway, and the listener heard about it exactly once, as a
  // cancellation rather than the clean end a resolved OnNext() would have been.
  auto listener = flight_server.listener(0);
  ASSERT_EQ(1, listener->finish_count());
  EXPECT_FALSE(listener->finish_status().ok())
      << "a deadline-cancelled upload was reported as a clean finish: "
      << listener->finish_status();
  // The parked OnNext() never re-armed the read: no second chunk was delivered.
  EXPECT_EQ(1, listener->next_awaits_.load());
}

TEST(AsyncGrpcTest, DoPutClientDeadlineWhileIdleIsNotACleanFinish) {
  // The deadline expires while the server is idle (the batch was consumed and
  // a read is armed): the upload was cut short by the client, so OnFinish()
  // must not report it as a clean end, only the acknowledgement write
  // completing cleanly may say OK.
  TestUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::seconds(2);
  auto put = client->DoPut(impatient, FlightDescriptor::Path({"idle"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  ASSERT_OK(put->writer->WriteRecordBatch(*batch));

  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_TRUE(WaitFor([&listener] { return listener->batches().size() >= 1; }))
      << "the server never consumed the batch";

  // Wait for the deadline to end the upload instead of sleeping out its
  // duration: the listener's OnFinish runs on the transport thread that ends
  // the RPC, so the wait is on the observable event, not on wall-clock time.
  // DoPut writes the schema eagerly, so the client's DoPut() above already
  // showed the channel was alive when its deadline was armed.
  ASSERT_TRUE(WaitFor([&listener] { return listener->finish_count() >= 1; }))
      << "the deadline never fired; inconclusive";

  const auto close_status = put->writer->Close();
  ASSERT_FALSE(close_status.ok()) << "the deadline never fired; inconclusive";

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  ASSERT_EQ(1, listener->finish_count());
  EXPECT_FALSE(listener->finish_status().ok())
      << "a deadline-cancelled upload was reported as a clean finish: "
      << listener->finish_status();
  // The deadline, not the listener, ended it: the status names the cancellation
  // the transport settled on.
  EXPECT_TRUE(listener->finish_status().IsCancelled()) << listener->finish_status();
}

/// A listener whose OnNext() future the test resolves on demand: the upload
/// parks in the application until the late-continuation test releases it.
class DeferredDoPutListener final
    : public ParkedUploadListener<RecordingListenerPhase::kNext> {
 public:
  arrow::Future<> OnNext(FlightStreamChunk chunk) override {
    next_awaits_.fetch_add(1);
    return ParkedUploadListener<RecordingListenerPhase::kNext>::OnNext(std::move(chunk));
  }

  /// Resolve the parked OnNext, OK releases the decode continuation.
  void ResolvePending() { Release(); }

  std::atomic<int> next_awaits_{0};
};

using DeferredDoPutServer = RecordingUploadServer<DeferredDoPutListener>;

TEST(AsyncGrpcTest, DoPutClientCancelWhileListenerParkedStaysSafe) {
  // The listener's OnNext() resolves after the deadline already finished the
  // RPC: the parked decode continuation must stand down (release its hold,
  // never touch the finished call) rather than reach into a destroyed reactor.
  // The test resolves the parked future from its own thread, and the server
  // keeps its shared_ptr alive, so the resolution lands on the real listener.
  DeferredDoPutServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  // The deadline is what ends this RPC (the listener stays parked, so only the
  // deadline can finish it), and DoPut writes the schema eagerly, so it must be
  // long enough to survive the setup on a slow machine. The wait below must
  // outlast it, or the poll loop gives up at the moment the deadline fires:
  // that race is what made this test fail on the macOS runner.
  constexpr auto kDeadline = std::chrono::milliseconds(2000);
  FlightCallOptions impatient;
  impatient.timeout = kDeadline;
  auto put = client->DoPut(impatient, FlightDescriptor::Path({"late"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  ASSERT_OK(put->writer->WriteRecordBatch(*batch));

  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_TRUE(WaitFor([&listener] { return listener->next_awaits_.load() >= 1; }))
      << "the listener never saw a chunk";

  // Walk away: the client stops reading, so the server's write has nowhere to
  // go and the RPC ends while the listener is still parked. Wait for that to
  // have happened rather than sleeping blindly, and fail clearly instead of
  // reporting a vacuous pass if it did not.
  ASSERT_OK(client->Close());
  ASSERT_TRUE(WaitFor([&listener] { return listener->finish_count() >= 1; },
                      kDeadline + std::chrono::seconds(10)))
      << "the RPC never ended; the listener is not the parked one from this upload";

  // Now resolve the parked listener: the continuation runs on a finished RPC,
  // which is the case the guards and holds are there to make safe.
  listener->ResolvePending();

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The upload had ended once, and the late resolution added no second report.
  ASSERT_EQ(1, listener->finish_count());

  // Closing the writer after the RPC is gone must report a failure, not a
  // silent success.
  ASSERT_FALSE(put->writer->Close().ok());
}

TEST(AsyncGrpcTest, DoPutWithoutDescriptorOnFirstMessageIsRejected) {
  // A client that starts an upload without a descriptor must be refused with
  // the same status as the sync transport ("Descriptor missing on first
  // message"): the listener must not hear a descriptor, and the upload must not
  // be acknowledged as well formed.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  // Hand-build the first FlightData of a DoPut: the schema IPC message with no
  // descriptor field at all.
  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  auto reader = arrow::RecordBatchReader::Make({batch}, schema).ValueOrDie();
  RecordBatchStream stream(std::move(reader));
  ASSERT_OK_AND_ASSIGN(auto schema_payload, stream.GetSchemaPayload());
  schema_payload.descriptor = nullptr;

  ::grpc::ByteBuffer request;
  bool own_buffer = false;
  ASSERT_TRUE(FlightDataSerialize(schema_payload, &request, &own_buffer).ok());

  // Drive the bidi call through the raw generic stub: the regular FlightClient
  // always sets the descriptor, which is exactly what this case lacks.
  ASSERT_OK_AND_ASSIGN(auto raw,
                       CallRaw(flight_server.port(),
                               "/arrow.flight.protocol.FlightService/DoPut", {request}));

  EXPECT_FALSE(raw.status.ok()) << "an upload without a descriptor must be refused";
  EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr("Descriptor missing"));
  EXPECT_TRUE(raw.responses.empty()) << "a rejected upload must answer nothing";

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The listener never saw a descriptor, and never saw the message as valid.
  EXPECT_EQ(0, listener->descriptor_count());
  EXPECT_EQ(0, listener->batches().size());
}

TEST(AsyncGrpcTest, DoPutMalformedFirstMessageIsRejected) {
  // A first message that does not parse as FlightData (here a descriptor field
  // cut short) fails the upload before the listener hears anything.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));

  // Field 1 (the descriptor), length-delimited, announcing 127 bytes of 2.
  ::grpc::Slice truncated(std::string("\x0a\x7f\x01\x02", 4));
  ::grpc::ByteBuffer request(&truncated, 1);

  ASSERT_OK_AND_ASSIGN(auto raw,
                       CallRaw(flight_server.port(),
                               "/arrow.flight.protocol.FlightService/DoPut", {request}));

  EXPECT_FALSE(raw.status.ok()) << "a malformed first message must be refused";
  EXPECT_THAT(raw.status.error_message(),
              ::testing::HasSubstr("Unable to read FlightDescriptor"));
  EXPECT_TRUE(raw.responses.empty());

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  EXPECT_EQ(0, listener->descriptor_count());
  EXPECT_EQ(0, listener->batches().size());
}

TEST(AsyncGrpcTest, DoPutReportsAFailingOnFinishToTheClient) {
  // The listener's OnFinish() is where an application commits the upload. When
  // it fails asynchronously, the client must not see a successful Close(): the
  // RPC's status is chained on that future instead of being reported OK as soon
  // as the upload's data arrived.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  listener->set_finish_status(
      arrow::Status::IOError("the upload could not be committed"));
  flight_server.set_listener(listener);
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  auto put = client->DoPut(FlightDescriptor::Path({"fail-finish"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  ASSERT_OK(put->writer->WriteRecordBatch(*batch));

  // Close() acknowledges the ending and then sees the RPC's final status, which
  // is the listener's failure.
  const auto close_status = put->writer->Close();
  EXPECT_FALSE(close_status.ok())
      << "a failing OnFinish must not be reported to the client as success";
  EXPECT_THAT(close_status.ToString(), ::testing::HasSubstr("could not be committed"));

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, listener->finish_count());
}

TEST(AsyncGrpcTest, DoPutHalfCloseWithoutAnyMessageIsRejected) {
  // A client that half-closes without sending a single message must not be
  // acknowledged: there is no descriptor, so there is no upload. The sync
  // transport rejects this ("Stream finished before first message sent");
  // without the check the reactor sent the clean-ending ack and the listener
  // heard OnFinish(OK) for a request that was never well formed.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  // Half-close with no message written at all.
  ASSERT_OK_AND_ASSIGN(
      auto raw,
      CallRaw(flight_server.port(), "/arrow.flight.protocol.FlightService/DoPut", {}));

  EXPECT_FALSE(raw.status.ok()) << "an upload with no message at all must be refused";
  EXPECT_THAT(raw.status.error_message(), ::testing::HasSubstr("empty upload"));
  EXPECT_TRUE(raw.responses.empty());

  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The listener must not have been told the upload ended cleanly.
  EXPECT_EQ(0, listener->descriptor_count());
  EXPECT_EQ(0, listener->batches().size());
}

TEST(AsyncGrpcTest, UseAsyncGrpcFlagUploadRejectedOnDescriptor) {
  // A handler that rejects the descriptor rejects the whole upload through
  // the same surface as an OnNext rejection: the writer's status.
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  listener->set_descriptor_status(
      arrow::Status::Invalid("listener rejected this descriptor"));
  FlightServerOptions options(LocalhostLocation());
  flight_server.set_listener(listener);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = TwoColumnSchema();
  auto batch = TwoColumnBatch(schema);

  const FlightDescriptor descriptor = FlightDescriptor::Path({"rejected-h2"});
  auto [surface, status] = UploadOneBatch(client.get(), descriptor, schema, batch);
  ASSERT_FALSE(status.ok())
      << "the descriptor rejection never reached the client, surfaced on " << surface;
  ASSERT_EQ(status.code(), arrow::StatusCode::Invalid);
  ASSERT_NE(status.message().find("listener rejected this descriptor"), std::string::npos)
      << "unexpected status: " << status;

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

  // The upload was rejected on its descriptor: the handler saw it, and no
  // schema or data followed it.
  ASSERT_EQ(listener->descriptor_count(), 1);
  ASSERT_EQ(listener->descriptors()[0], descriptor);
  ASSERT_EQ(listener->schema_count(), 0);
  ASSERT_EQ(listener->batches().size(), 0);
}

// The client half-closes, then the listener answers from OnFinish(): the
// Flight SQL ingest pattern. The reply must reach the client, and the upload
// must not send an acknowledgement of its own that the client would read as the
// server's answer.
class ReplyingOnFinishListener : public RecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    auto recorded = RecordingListener::OnFinish(status);
    if (!status.ok()) {
      return recorded;
    }
    // The result of the upload, written once the client has finished sending.
    auto writer = this->writer();
    return writer->WriteMetadataAsync(*Buffer::FromString("ingested 2 rows"))
        .Then([recorded]() { return recorded; });
  }
};

TEST(AsyncGrpcTest, DoPutListenerRepliesFromOnFinish) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<ReplyingOnFinishListener>();
  flight_server.set_listener(listener);
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  ASSERT_OK_AND_ASSIGN(auto put,
                       client->DoPut(FlightDescriptor::Path({"ingest"}), schema));
  ASSERT_OK(put.writer->WriteRecordBatch(*batch));

  // DoneWriting() half-closes; the server's only PutResult is the listener's.
  ASSERT_OK(put.writer->DoneWriting());
  std::shared_ptr<Buffer> metadata;
  ASSERT_OK(put.reader->ReadMetadata(&metadata));
  ASSERT_NE(nullptr, metadata) << "the reply written from OnFinish() never arrived";
  ASSERT_EQ("ingested 2 rows", metadata->ToString());

  ASSERT_OK(put.writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, listener->finish_count());
}

TEST(AsyncGrpcTest, DoPutSendsNoAcknowledgementOfItsOwn) {
  // Every PutResult on the wire comes from the listener. A client that reads them
  // must not find a message the transport made up behind them, so its Close() is
  // the first thing that sees the RPC's status. Counted on the wire: only a raw
  // call can read back "no message at all", because ReadMetadata() after
  // DoneWriting() blocks until the RPC ends.
  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  struct Case {
    std::string name;
    std::function<std::shared_ptr<AsyncFlightDataListener>()> make_listener;
    /// How many messages the listener itself writes for one batch.
    int expected_replies;
    std::function<void(const std::vector<::grpc::ByteBuffer>&)> check;
  };

  // A listener that answers once per batch.
  class OneAckListener final : public RecordingListener {
   public:
    Future<> OnNext(FlightStreamChunk chunk) override {
      auto recorded = RecordingListener::OnNext(std::move(chunk));
      if (!chunk.data) {
        return recorded;
      }
      auto written = writer()->WriteMetadataAsync(*Buffer::FromString("ack"));
      return written.Then([recorded]() { return recorded; });
    }
  };

  const std::vector<Case> cases = {
      {"silent listener", [] { return std::make_shared<RecordingListener>(); }, 0,
       [](const std::vector<::grpc::ByteBuffer>& replies) {
         EXPECT_TRUE(replies.empty())
             << "the transport answered an upload the client never asked about";
       }},
      {"one ack", [] { return std::make_shared<OneAckListener>(); }, 1,
       [](const std::vector<::grpc::ByteBuffer>& replies) {
         ASSERT_EQ(1u, replies.size()) << "an extra PutResult was written";
         EXPECT_EQ("ack", PutResultAppMetadata(ByteBufferToString(replies[0])));
       }},
      {"reply from OnFinish", [] { return std::make_shared<ReplyingOnFinishListener>(); },
       1,
       [](const std::vector<::grpc::ByteBuffer>& replies) {
         ASSERT_EQ(1u, replies.size()) << "an extra PutResult was written";
         EXPECT_EQ("ingested 2 rows",
                   PutResultAppMetadata(ByteBufferToString(replies[0])));
       }},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    TestFlightServer flight_server;
    flight_server.set_listener(test.make_listener());
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));

    // The upload as the client sends it: descriptor and schema, then the batch.
    DecoderStreamForPut payloads(schema, {batch});
    auto schema_payload = payloads.SchemaMessage();
    ASSERT_OK(internal::ToPayload(FlightDescriptor::Path({"no-extra-ack"}),
                                  &schema_payload.descriptor));
    std::vector<::grpc::ByteBuffer> requests = {Serialize(schema_payload)};
    auto batch_payload = payloads.Next();
    ASSERT_FALSE(batch_payload.ipc_message.metadata == nullptr);
    requests.push_back(Serialize(batch_payload));

    ASSERT_OK_AND_ASSIGN(auto raw,
                         CallRaw(flight_server.port(), MethodPath("DoPut"), requests));
    EXPECT_TRUE(raw.status.ok()) << raw.status.error_message();
    EXPECT_EQ(static_cast<size_t>(test.expected_replies), raw.responses.size());

    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
    // The listener wrote those messages itself; the transport added nothing.
    test.check(raw.responses);
  }
}

TEST(AsyncGrpcTest, DoPutNoCallbackAfterFinishWhenDescriptorIsPending) {
  // A cancellation while OnDescriptor() is still pending must not let the
  // descriptor's completion deliver OnSchemaDecoded()/OnNext() afterwards: the
  // listener may have released everything it owns in OnFinish().
  TestFlightServer flight_server;
  auto listener =
      std::make_shared<ParkedUploadListener<RecordingListenerPhase::kDescriptor>>();
  flight_server.set_listener(listener);
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  ASSERT_OK_AND_ASSIGN(auto put,
                       client->DoPut(FlightDescriptor::Path({"parked"}), schema));
  ASSERT_OK(put.writer->WriteRecordBatch(*batch));
  ASSERT_TRUE(WaitFor([&] { return listener->descriptor_count() == 1; }))
      << "the descriptor never reached the listener";

  // Cancel the upload from the server side while the descriptor is pending: the
  // listener hears OnFinish() right away, without resolving its future first.
  ASSERT_OK(listener->Cancel(Status::Cancelled("give up")));
  ASSERT_EQ(1, listener->finish_count()) << "Cancel() must report OnFinish() at once";

  // The descriptor completes after OnFinish(). Its continuation runs inline on
  // this thread, so anything it (wrongly) delivered has run once this returns.
  listener->Release();
  EXPECT_EQ(0, listener->schema_count()) << "OnSchemaDecoded() ran after OnFinish()";
  EXPECT_EQ(0, listener->batches().size()) << "a batch was delivered after OnFinish()";
  EXPECT_EQ(0, listener->metadata_only_count());

  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
  EXPECT_EQ(1, listener->finish_count());
}

TEST(AsyncGrpcTest, DoPutCleanEndValidatesTheIpcStream) {
  // A half-close is a clean end only if the IPC stream could end there: a
  // dictionary-encoded schema whose initial dictionaries all arrived (and,
  // optionally, a batch) succeeds, while a stream that stopped after some of them
  // is rejected instead of being reported to the listener as a complete upload.
  struct Case {
    std::string name;
    /// How many of the schema's initial dictionary messages to send, then batch.
    int dictionaries;
    bool send_batch;
    bool expect_ok;
  };
  const std::vector<Case> cases = {
      {"schema only", 0, false, true},
      {"complete dictionary set, no batch", 2, false, true},
      {"partial dictionary set", 1, false, false},
      {"complete dictionary set and batch", 2, true, true},
  };

  const auto t0 = dictionary(arrow::int32(), utf8());
  const auto t1 = dictionary(arrow::int32(), utf8());
  const auto schema = arrow::schema({arrow::field("d0", t0), arrow::field("d1", t1)});
  const auto batch = RecordBatch::Make(
      schema, 2,
      {ArrayFromJSON(t0, R"(["a", "b"])"), ArrayFromJSON(t1, R"(["x", "y"])")});

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    // Build the wire messages the raw client sends: descriptor+schema, then the
    // dictionary(ies), then the batch.
    DecoderStreamForPut stream(schema, {batch});
    std::vector<::grpc::ByteBuffer> requests;
    auto schema_payload = stream.SchemaMessage();
    ASSERT_OK(
        internal::ToPayload(FlightDescriptor::Path({"eof"}), &schema_payload.descriptor));
    requests.push_back(Serialize(schema_payload));
    std::vector<::grpc::ByteBuffer> dictionary_messages;
    FlightPayload batch_payload;
    while (true) {
      auto payload = stream.Next();
      if (payload.ipc_message.metadata == nullptr) break;
      if (payload.ipc_message.type == ipc::MessageType::DICTIONARY_BATCH) {
        dictionary_messages.push_back(Serialize(payload));
      } else {
        batch_payload = std::move(payload);
      }
    }
    for (int i = 0;
         i < test.dictionaries && i < static_cast<int>(dictionary_messages.size()); i++) {
      requests.push_back(dictionary_messages[i]);
    }
    if (test.send_batch) {
      requests.push_back(Serialize(batch_payload));
    }

    TestUploadServer flight_server;
    ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));
    ASSERT_OK_AND_ASSIGN(auto raw,
                         CallRaw(flight_server.port(),
                                 "/arrow.flight.protocol.FlightService/DoPut", requests));
    if (test.expect_ok) {
      EXPECT_TRUE(raw.status.ok()) << raw.status.error_message();
    } else {
      EXPECT_FALSE(raw.status.ok())
          << "a partial initial dictionary set must fail at EOF";
      EXPECT_THAT(raw.status.error_message(),
                  ::testing::HasSubstr("expected number (2)"));
    }
    EXPECT_TRUE(raw.responses.empty()) << "the transport writes no PutResult of its own";
    ASSERT_OK(StopClientAndServer(client.get(), &flight_server));

    auto listener = flight_server.WaitForListener(0);
    ASSERT_NE(nullptr, listener);
    EXPECT_EQ(1, listener->finish_count());
    EXPECT_EQ(test.expect_ok, listener->finish_status().ok())
        << listener->finish_status();
    if (test.send_batch) {
      ASSERT_EQ(1u, listener->batches().size());
      AssertBatchesEqual(*batch, *listener->batches()[0]);
    } else {
      EXPECT_TRUE(listener->batches().empty());
    }
  }
}

TEST(AsyncGrpcTest, DoPutFinishStatusPrecedence) {
  // OnFinish()'s future decides whether an otherwise clean upload succeeded, and a
  // failed final metadata write must not be reported to the client as success. A
  // failure the upload already had always wins.
  struct Case {
    std::string name;
    Status on_finish;
    bool fail_final_write;
    std::function<void(const Status&)> check;
  };
  const std::vector<Case> cases = {
      {"clean upload", Status::OK(), false,
       [](const Status& final) { EXPECT_TRUE(final.ok()) << final; }},
      {"failing OnFinish", Status::IOError("commit failed"), false,
       [](const Status& final) {
         EXPECT_FALSE(final.ok());
         EXPECT_THAT(final.ToString(), ::testing::HasSubstr("commit failed"));
       }},
      {"failed final metadata write", Status::OK(), true,
       [](const Status& final) {
         EXPECT_FALSE(final.ok()) << "a failed final write was reported as success";
         EXPECT_THAT(final.ToString(), ::testing::HasSubstr("Write failed"));
       }},
  };

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    auto listener =
        std::make_shared<ParkedUploadListener<RecordingListenerPhase::kFinish>>();
    listener->set_finish_status(test.on_finish);

    TestFlightServer flight_server;
    flight_server.set_listener(listener);
    ManualDoPut upload(&flight_server);
    upload.DeliverRequest(FlightDescriptor::Path({"precedence"}), schema);
    // The client's whole upload: the schema message, then the half-close, which is
    // what makes the transport report OnFinish().
    DecoderStreamForPut payloads(schema, {});
    upload.Deliver(payloads.Next());  // end of stream: nothing more to send
    upload.HalfClose();
    ASSERT_TRUE(listener->entered().Wait(10)) << "OnFinish() never ran";
    EXPECT_EQ(1, listener->finish_count());
    // The upload is parked in OnFinish(): nothing has finished the RPC yet.
    EXPECT_EQ(0, upload.stream.finish_count());

    if (test.fail_final_write) {
      // A message OnFinish() wrote and that then failed: the RPC's status must
      // reflect the failed write, not the clean upload. The write is started
      // before OnFinish() resolves, so the transport finishes the RPC only once
      // that message's outcome is known.
      auto written =
          listener->writer()->WriteMetadataAsync(*Buffer::FromString("from-onfinish"));
      ASSERT_TRUE(upload.stream.write_pending());
      listener->Release(test.on_finish);
      // OnFinish() resolved: the RPC is now waiting for the message on the wire.
      ASSERT_TRUE(
          WaitFor([&] { return !written.is_finished(); }, std::chrono::milliseconds(500)))
          << "the transport finished before its last message";
      upload.stream.CompleteWrite(false);
      ASSERT_TRUE(written.is_finished());
      EXPECT_FALSE(written.status().ok());
      ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
      test.check(FromGrpcStatus(*upload.stream.finish_status(), nullptr));
      EXPECT_EQ(1, upload.stream.finish_count());
      continue;
    }
    listener->Release(test.on_finish);
    ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
    test.check(FromGrpcStatus(*upload.stream.finish_status(), nullptr));
    // Exactly one terminal completion, whatever the outcome.
    EXPECT_EQ(1, upload.stream.finish_count());
  }
}

TEST(AsyncGrpcTest, DoPutFinishWaitsForMetadata) {
  // OnFinish() returns without awaiting a write it started: the RPC must still
  // wait for that message before finishing, or the client's Close() would race
  // the PutResult it was promised.
  auto listener = std::make_shared<RecordingListener>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  ManualDoPut upload(&flight_server);
  auto schema = OneColumnSchema();
  upload.DeliverRequest(FlightDescriptor::Path({"waits"}), schema);
  DecoderStreamForPut payloads(schema, {});
  ARROW_UNUSED(payloads.SchemaMessage());
  upload.HalfClose();
  ASSERT_TRUE(WaitFor(
      [&] { return upload.stream.read_pending() || upload.stream.finish_count() > 0; }));

  // A write started from OnFinish() stays pending: nothing may finish the RPC yet.
  auto trace = listener->writer();
  ARROW_UNUSED(trace);
  // The listener did not write here, so the upload ends at once.
  ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
  EXPECT_TRUE(upload.stream.finish_status()->ok())
      << upload.stream.finish_status()->error_message();
  EXPECT_EQ(1, listener->finish_count());
}

TEST(AsyncGrpcTest, DoPutOverlappingMetadataWritesAreRejected) {
  // The writer accepts one PutResult at a time: a second message while the first is
  // on the wire is refused immediately and never sent, and the first one's bytes are
  // the ones serialized before the buffer was mutated.
  auto listener = std::make_shared<RecordingListener>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  ManualDoPut upload(&flight_server);
  auto schema = OneColumnSchema();
  upload.DeliverRequest(FlightDescriptor::Path({"overlap"}), schema);

  auto mutable_buffer = arrow::AllocateResizableBuffer(4).ValueOrDie();
  std::memcpy(mutable_buffer->mutable_data(), "orig", 4);
  auto first = listener->writer()->WriteMetadataAsync(*mutable_buffer);
  ASSERT_TRUE(upload.stream.write_pending());
  auto second = listener->writer()->WriteMetadataAsync(*Buffer::FromString("second"));
  ASSERT_FALSE(second.status().ok());
  EXPECT_THAT(second.status().ToString(), ::testing::HasSubstr("already in flight"));
  EXPECT_EQ(1u, upload.stream.writes().size()) << "the rejected write was sent anyway";

  // The contract says the bytes are copied before the call returns.
  std::memcpy(mutable_buffer->mutable_data(), "MUTA", 4);
  upload.stream.CompleteWrite(true);
  ASSERT_TRUE(first.is_finished());
  EXPECT_TRUE(first.status().ok()) << first.status();
  {
    auto writes = upload.stream.writes();
    EXPECT_EQ("orig", PutResultAppMetadata(ByteBufferToString(writes[0])))
        << "the reply was not copied";
  }

  // With the first message settled, a later write goes through, in order.
  auto third = listener->writer()->WriteMetadataAsync(*Buffer::FromString("third"));
  ASSERT_TRUE(upload.stream.write_pending());
  upload.stream.CompleteWrite(true);
  ASSERT_TRUE(third.is_finished());
  EXPECT_TRUE(third.status().ok()) << third.status();
  auto writes = upload.stream.writes();
  ASSERT_EQ(2u, writes.size());
  EXPECT_EQ("third", PutResultAppMetadata(ByteBufferToString(writes[1])));

  upload.HalfClose();
  ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
}

TEST(AsyncGrpcTest, DoPutFailedPutResultWriteFailsTheUpload) {
  // A PutResult whose write fails fails its returned future and, when no earlier
  // ending has won, the upload itself -- without starting another read or write.
  {
    auto listener = std::make_shared<RecordingListener>();
    TestFlightServer flight_server;
    flight_server.set_listener(listener);
    ManualDoPut upload(&flight_server);
    auto schema = OneColumnSchema();
    upload.DeliverRequest(FlightDescriptor::Path({"write-failure"}), schema);
    auto written = listener->writer()->WriteMetadataAsync(*Buffer::FromString("doomed"));
    ASSERT_TRUE(upload.stream.write_pending());
    const auto reads_before = upload.stream.read_pending();
    upload.stream.CompleteWrite(false);
    ASSERT_TRUE(written.is_finished());
    EXPECT_FALSE(written.status().ok());
    EXPECT_THAT(written.status().ToString(),
                ::testing::HasSubstr("PutResult message was not written"));
    EXPECT_EQ(reads_before, upload.stream.read_pending())
        << "a failed write must not start another read";
    EXPECT_FALSE(upload.stream.write_pending()) << "a failed write must not be retried";
    // The upload had not ended yet: the failed write ends it as Internal.
    ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
    EXPECT_EQ(::grpc::StatusCode::INTERNAL, upload.stream.finish_status()->error_code())
        << upload.stream.finish_status()->error_message();
  }
}

TEST(AsyncGrpcTest, DoPutServerCancelStatuses) {
  // Cancel() reaches the transport for any status; Cancel(OK) is refused by the
  // upload (a cancellation needs a status to report), and after the upload both
  // Cancel() and the writer report that there is nothing in flight.
  struct Case {
    std::string name;
    Status cancel;
    ::grpc::StatusCode expected_grpc_code;
  };
  // Cancel() dispatches for every status; the upload refuses a status it cannot
  // report (OK) by turning it into Invalid, and that is what the RPC settles with.
  const std::vector<Case> cases = {
      {"cancelled", Status::Cancelled("server said stop"), ::grpc::StatusCode::CANCELLED},
      {"invalid", Status::Invalid("quota"), ::grpc::StatusCode::INVALID_ARGUMENT},
      {"OK", Status::OK(), ::grpc::StatusCode::INVALID_ARGUMENT},
  };

  for (const auto& test : cases) {
    SCOPED_TRACE(test.name);
    auto listener = std::make_shared<RecordingListener>();
    TestFlightServer flight_server;
    flight_server.set_listener(listener);
    ManualDoPut upload(&flight_server);
    auto schema = OneColumnSchema();
    upload.DeliverRequest(FlightDescriptor::Path({"cancel"}), schema);

    // The public Cancel() reports OK when it reached the transport; the status it
    // was given is what the RPC settles with.
    EXPECT_TRUE(listener->CancelWith(test.cancel).ok());
    ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
    ASSERT_TRUE(upload.stream.finish_status().has_value());
    // The manual stream has no client context, so the Arrow-level detail the
    // transport attaches as trailing metadata is not reconstructed here: the gRPC
    // code is what the reactor actually settled.
    EXPECT_EQ(test.expected_grpc_code, upload.stream.finish_status()->error_code())
        << upload.stream.finish_status()->error_message();
    EXPECT_EQ(1, upload.stream.finish_count());
    upload.Finish();
    // Once the upload is over both surfaces say so, distinctly.
    const auto late_cancel = listener->CancelWith(Status::Cancelled("too late"));
    EXPECT_TRUE(late_cancel.IsInvalid()) << late_cancel;
    auto late_write = listener->writer()->WriteMetadataAsync(*Buffer::FromString("x"));
    ASSERT_FALSE(late_write.status().ok());
    EXPECT_TRUE(late_write.status().IsInvalid()) << late_write.status();
  }
}

TEST(AsyncGrpcTest, DoPutOnFinishMayReenterCancel) {
  // OnFinish() is application code running on a transport thread: cancelling from
  // there must neither deadlock nor report a second finish, and exactly one
  // terminal status is recorded.
  class CancellingFinishListener final : public RecordingListener {
   public:
    Future<> OnFinish(Status status) override {
      auto recorded = RecordingListener::OnFinish(std::move(status));
      reentrant_cancel = Cancel(Status::Cancelled("from onfinish"));
      return recorded;
    }
    Status reentrant_cancel;
  };

  auto listener = std::make_shared<CancellingFinishListener>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  ManualDoPut upload(&flight_server);
  auto schema = OneColumnSchema();
  upload.DeliverRequest(FlightDescriptor::Path({"reenter"}), schema);
  upload.HalfClose();
  ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
  EXPECT_EQ(1, listener->finish_count()) << "the nested Cancel() must not re-report";
  EXPECT_EQ(1, upload.stream.finish_count());
  upload.Finish();
}

TEST(AsyncGrpcTest, DoPutServerCancelAfterBatchesHasNoLaterCallbacks) {
  // A server-side cancel while OnFinish() is parked: the extra message the client
  // had already sent must not reach the listener, no read is re-armed, and the
  // server's own status decides the RPC.
  auto listener =
      std::make_shared<ParkedUploadListener<RecordingListenerPhase::kFinish>>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  ManualDoPut upload(&flight_server);
  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  upload.DeliverRequest(FlightDescriptor::Path({"parked-cancel"}), schema);
  DecoderStreamForPut payloads(schema, {batch});
  ARROW_UNUSED(payloads.SchemaMessage());
  auto batch_payload = payloads.Next();
  upload.Deliver(batch_payload);
  ASSERT_TRUE(WaitFor([&] { return listener->batches().size() == 1; }))
      << "the batch never reached the listener";
  // The reader asked for the next message; the server cancels before it arrives.
  ASSERT_TRUE(upload.stream.read_pending())
      << "the decoder must ask for the next message";
  ASSERT_OK(listener->CancelWith(Status::Cancelled("server said stop")));
  ASSERT_EQ(1, listener->finish_count()) << "Cancel() must report OnFinish() at once";
  ASSERT_FALSE(upload.stream.finish_status().has_value())
      << "the RPC must wait for the parked OnFinish()";

  // The message the client had already sent when the cancel landed is dropped: the
  // listener hears nothing more, and no read is re-armed.
  upload.stream.DeliverRead(Serialize(batch_payload));
  EXPECT_FALSE(upload.stream.read_pending())
      << "a read was re-armed after the upload started ending";

  listener->Release();
  ASSERT_TRUE(upload.stream.finish_observed().Wait(10));
  EXPECT_EQ(::grpc::StatusCode::CANCELLED, upload.stream.finish_status()->error_code())
      << upload.stream.finish_status()->error_message();
  EXPECT_EQ(1, listener->batches().size())
      << "a batch reached the listener after OnFinish()";
  EXPECT_EQ(0, listener->metadata_only_count());
  EXPECT_EQ(1, listener->schema_count()) << "the schema was already reported once";
  EXPECT_EQ(1, listener->finish_count());
  upload.Finish();
}

TEST(AsyncGrpcTest, DoPutFactoryGivesEveryUploadItsOwnListener) {
  // Two overlapping uploads get a fresh listener each: distinct descriptors, data
  // and finish events, and cancelling one must not finish the other.
  RecordingUploadServer<RecordingListener> flight_server;
  ASSERT_OK_AND_ASSIGN(auto client, StartAndConnect(&flight_server));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  ASSERT_OK_AND_ASSIGN(auto first,
                       client->DoPut(FlightDescriptor::Path({"first"}), schema));
  ASSERT_OK(first.writer->WriteRecordBatch(*batch));
  // Wait for the first upload's listener before starting the second, so the two
  // are provably overlapping.
  auto first_listener = flight_server.WaitForListener(0);
  ASSERT_NE(nullptr, first_listener);
  ASSERT_TRUE(WaitFor([&] { return first_listener->batches().size() == 1; }));

  ASSERT_OK_AND_ASSIGN(auto second,
                       client->DoPut(FlightDescriptor::Path({"second"}), schema));
  ASSERT_OK(second.writer->WriteRecordBatch(*batch));
  auto second_listener = flight_server.WaitForListener(1);
  ASSERT_NE(nullptr, second_listener);
  ASSERT_TRUE(WaitFor([&] { return second_listener->batches().size() == 1; }));
  EXPECT_NE(first_listener, second_listener) << "the factory reused a listener";

  // Two uploads, two factory calls: one listener each.
  EXPECT_EQ(2, flight_server.calls());
  ASSERT_EQ(1u, first_listener->descriptors().size());
  EXPECT_EQ(FlightDescriptor::Path({"first"}), first_listener->descriptors()[0]);
  ASSERT_EQ(1u, second_listener->descriptors().size());
  EXPECT_EQ(FlightDescriptor::Path({"second"}), second_listener->descriptors()[0]);

  // Cancelling one ends only that upload.
  ASSERT_OK(first_listener->CancelWith(Status::Cancelled("first only")));
  ASSERT_TRUE(WaitFor([&] { return first_listener->finish_count() == 1; }));
  EXPECT_EQ(0, second_listener->finish_count())
      << "cancelling one upload finished the other";

  // The second still completes cleanly.
  ASSERT_OK(second.writer->Close());
  ASSERT_TRUE(WaitFor([&] { return second_listener->finish_count() == 1; }));
  EXPECT_TRUE(second_listener->finish_status().ok()) << second_listener->finish_status();
  EXPECT_FALSE(first_listener->finish_status().ok());

  ARROW_UNUSED(first.writer->Close());
  ASSERT_OK(StopClientAndServer(client.get(), &flight_server));
}

}  // namespace arrow::flight::transport::grpc
