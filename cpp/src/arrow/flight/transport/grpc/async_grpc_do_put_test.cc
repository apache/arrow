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

TEST(AsyncGrpcTest, BasicDoPut) {
  // One listener for the whole test: shared with the service's factory and
  // kept here so it can be asserted on after the RPC.
  auto listener = std::make_shared<RecordingListener>();
  TestFlightServer flight_server;
  flight_server.set_listener(listener);
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

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
  server->Shutdown();
  server->Wait();

  // The listener saw the schema once and both batches, in order.
  ASSERT_EQ(listener->schema_count(), 1);
  AssertTwoColumnSchema(listener->schema());
  ASSERT_EQ(listener->batches().size(), 2);
  for (const auto& chunk : listener->batches()) {
    ASSERT_EQ(chunk->num_rows(), 3);
    ASSERT_EQ(chunk->num_columns(), 2);
  }
  // No metadata-only (error/app-metadata) chunks, and nothing the consumer
  // rejected: a non-OK OnNext status would have failed DoPut with no ack.
  ASSERT_EQ(listener->metadata_only_count(), 0);
  ASSERT_OK(listener->last_status());
}

TEST(AsyncGrpcTest, EmptyDoPut) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});

  // Upload nothing at all: the acknowledgement the server writes back on the
  // client's half-close is what makes Close() return.
  FlightDescriptor descriptor = FlightDescriptor::Path({"empty-put"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));
  ASSERT_OK(result.writer->Close());

  // Cleanup before looking at the listener, so the DoPut callback cannot race
  // with the reads.
  ASSERT_OK(client->Close());
  server->Shutdown();
  server->Wait();

  // No data of any kind reached the listener.
  ASSERT_EQ(listener->batches().size(), 0);
  ASSERT_EQ(listener->metadata_only_count(), 0);
  ASSERT_OK(listener->last_status());
  // The sync client sends the schema payload eagerly, even when no batch
  // follows, so the listener does see the schema here. Deliberately not
  // asserted: that is the client's business, not the transport's.
}

TEST(AsyncGrpcTest, MetadataOnlyPutChunk) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<RecordingListener>();
  flight_server.set_listener(listener);
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});

  FlightDescriptor descriptor = FlightDescriptor::Path({"metadata-only-put"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));
  auto app_metadata = arrow::Buffer::FromString("metadata-only-upload");
  ASSERT_OK(result.writer->WriteMetadata(app_metadata));
  ASSERT_OK(result.writer->Close());

  ASSERT_OK(client->Close());
  server->Shutdown();
  server->Wait();

  // Exactly one chunk reached the listener: no data, that exact metadata.
  ASSERT_EQ(listener->batches().size(), 0);
  ASSERT_EQ(listener->metadata_only_count(), 1);
  ASSERT_EQ(listener->metadata_chunks().size(), 1);
  ASSERT_NE(listener->metadata_chunks()[0], nullptr);
  ASSERT_EQ(listener->metadata_chunks()[0]->ToString(), "metadata-only-upload");
}

TEST(AsyncGrpcTest, DoPutRejectedByListener) {
  TestFlightServer flight_server;
  auto listener = std::make_shared<FinishRecordingListener>();
  // The consumer rejects the upload.
  listener->set_next_status(arrow::Status::Invalid("listener rejected this upload"));
  flight_server.set_listener(listener);
  AsyncGenericFlightService service(&flight_server, MakeAsyncHelper());

  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);
  ASSERT_NE(port, 0);

  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(port));

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
  server->Shutdown();
  server->Wait();
}

TEST(AsyncGrpcTest, CreateDoPutListenerServesUpload) {
  // The upload half of the server class API: CreateDoPutListener() hands out
  // the per-RPC listener, with no options-level factory.
  TestUploadServer flight_server;
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
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
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The virtual was consulted exactly once, and its listener saw the upload.
  ASSERT_EQ(flight_server.calls(), 1);
  const auto& listener = flight_server.listener(0);
  ASSERT_EQ(listener->descriptor_count(), 1);
  ASSERT_EQ(listener->descriptors()[0], descriptor);
  ASSERT_EQ(listener->schema_count(), 1);
  ASSERT_EQ(listener->batches().size(), 1);
  ASSERT_EQ(listener->batches()[0]->num_rows(), 3);
}

TEST(AsyncGrpcTest, DoPutWithoutListenerIsUnimplemented) {
  // A server class that does not hand out a listener (CreateDoPutListener()
  // returns nullptr, the base default) does not accept uploads: the RPC is
  // answered UNIMPLEMENTED. There is no options-level fallback.
  TestFlightServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = arrow::schema({arrow::field("a", arrow::int64())});
  auto batch = arrow::RecordBatch::Make(schema, 2,
                                        {arrow::ArrayFromJSON(arrow::int64(), "[7, 8]")});
  auto [surface, status] = UploadOneBatch(
      client.get(), FlightDescriptor::Path({"no-listener"}), schema, batch);
  ASSERT_FALSE(status.ok()) << "the upload succeeded on " << surface
                            << " with no listener to serve it";
  ASSERT_EQ(status.code(), arrow::StatusCode::NotImplemented) << status;

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

TEST(AsyncGrpcTest, DoPutListenerSeesCleanFinish) {
  // A client that ends the upload normally (DoneWriting + Close) makes the
  // listener's OnFinish fire once, with OK: that is how a consumer knows the
  // upload ended and it can commit what it accumulated.
  TestUploadServer flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  auto [surface, status] =
      UploadOneBatch(client.get(), FlightDescriptor::Path({"finish"}), schema, batch);
  ASSERT_TRUE(status.ok()) << "upload failed on " << surface << ": " << status;

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // OnFinish ran exactly once, and said the upload ended cleanly.
  ASSERT_EQ(flight_server.listener(0)->finish_count(), 1);
  ASSERT_TRUE(flight_server.listener(0)->finish_status().ok())
      << "unexpected finish status: " << flight_server.listener(0)->finish_status();
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

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_EQ(1, listener->finish_count());
  // The write is refused (the upload is ending) rather than deadlocking.
  ASSERT_TRUE(listener->finish_write.is_finished())
      << "a write refused while the upload ends must fail at once";
  EXPECT_TRUE(listener->finish_write.status().IsInvalid())
      << listener->finish_write.status();
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
    auto listener = std::make_shared<FinishRecordingListener>();
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

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

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
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

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
  listener->CancelWith(arrow::Status::Cancelled("server said stop"));

  // The client sees the cancellation on the surface that observes the RPC.
  const Status close_status = writer.Close();
  ASSERT_FALSE(close_status.ok()) << "the server-side cancel never reached the client";
  ASSERT_EQ(close_status.code(), arrow::StatusCode::Cancelled);
  ASSERT_NE(close_status.message().find("server said stop"), std::string::npos)
      << "unexpected status: " << close_status;

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // Cancel() reached the transport, and the listener was told the upload ended
  // with that status exactly once.
  ASSERT_EQ(listener->cancel_count(), 1);
  ASSERT_OK(listener->cancel_status()) << listener->cancel_status();
  ASSERT_EQ(listener->finish_count(), 1);
  // OnFinish must carry the ending that caused it, not a generic status.
  ASSERT_EQ(listener->finish_status().code(), arrow::StatusCode::Cancelled)
      << "OnFinish got: " << listener->finish_status();

  // After the upload, Cancel() has nothing to cancel.
  listener->CancelWith(arrow::Status::Cancelled("too late"));
  ASSERT_EQ(listener->cancel_status().code(), arrow::StatusCode::Invalid);
}

/// A listener whose OnFinish() stays pending until the test releases it, the
/// way an application committing the upload asynchronously does: the window in
/// which an ending is reported but the RPC is not finished yet.
class ParkedFinishListener final : public FinishRecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    static_cast<void>(FinishRecordingListener::OnFinish(std::move(status)));
    std::lock_guard<std::mutex> lock(mutex_);
    parked_ = arrow::Future<>::Make();
    return parked_;
  }

  /// Complete the parked OnFinish() future, on the calling thread.
  void ReleaseFinish() {
    arrow::Future<> parked;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      parked = parked_;
    }
    if (parked.is_valid()) {
      parked.MarkFinished();
    }
  }

 private:
  arrow::Future<> parked_;
};

TEST(AsyncGrpcTest, DoPutServerCancelEndsTheUploadWithItsOwnStatus) {
  // A server-side Cancel() whose OnFinish() is still committing: what the
  // client sends afterwards must not reach the listener, and the client ending
  // the upload normally must not finish the RPC with OK ahead of the
  // cancellation: the first ending decides the RPC's status.
  RecordingUploadServer<ParkedFinishListener> flight_server;
  FlightServerOptions options(LocalhostLocation());
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);
  ASSERT_OK_AND_ASSIGN(auto put,
                       client->DoPut(FlightDescriptor::Path({"parked-finish"}), schema));
  ASSERT_OK(put.writer->WriteRecordBatch(*batch));
  auto listener = flight_server.WaitForListener(0);
  ASSERT_NE(listener, nullptr) << "the server never handed out a DoPut listener";
  ASSERT_TRUE(WaitFor([&listener] { return listener->batches().size() >= 1; }))
      << "the server never consumed the first batch";

  listener->CancelWith(arrow::Status::Cancelled("server said stop"));
  ASSERT_OK(listener->cancel_status());
  ASSERT_EQ(1, listener->finish_count()) << "Cancel() must report OnFinish() at once";

  // Sent after OnFinish() was reported: must never reach the listener.
  ARROW_UNUSED(put.writer->WriteRecordBatch(*batch));
  // Keep OnFinish() pending long enough for the client's normal end of the
  // upload to reach the server first.
  std::thread release([&listener] {
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    listener->ReleaseFinish();
  });
  const Status close_status = put.writer->Close();
  release.join();

  EXPECT_EQ(close_status.code(), arrow::StatusCode::Cancelled)
      << "the client's end of the upload won over the server's cancel: " << close_status;
  EXPECT_NE(close_status.message().find("server said stop"), std::string::npos)
      << close_status;
  EXPECT_EQ(1, listener->batches().size())
      << "a batch reached the listener after its OnFinish()";
  EXPECT_EQ(1, listener->finish_count());

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
}

/// A listener stuck inside OnNext(): the upload is parked in the application
/// while the RPC underneath it dies.
class HangingDoPutListener final : public FinishRecordingListener {
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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  HangingDoPutServer flight_server;
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_OK_AND_ASSIGN(auto client, ConnectTo(flight_server.port()));

  auto schema = OneColumnSchema();
  auto batch = OneColumnBatch(schema);

  FlightCallOptions impatient;
  impatient.timeout = std::chrono::milliseconds(1000);
  auto put = client->DoPut(impatient, FlightDescriptor::Path({"hang"}), schema);
  ASSERT_TRUE(put.ok()) << put.status();
  (void)put->writer->WriteRecordBatch(*batch);

  // From here the listener is inside OnNext() and the deadline does the rest.
  ASSERT_TRUE(WaitFor([&flight_server] {
    auto listener = flight_server.WaitForListener(0);
    return listener != nullptr && listener->next_awaits_.load() >= 1;
  })) << "the listener never saw a chunk";

  ARROW_UNUSED(put->writer->Close());
  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The upload ended anyway, and the listener heard about it exactly once.
  ASSERT_EQ(1, flight_server.listener(0)->finish_count());
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
  impatient.timeout = std::chrono::milliseconds(600);
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

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  ASSERT_EQ(1, listener->finish_count());
  EXPECT_FALSE(listener->finish_status().ok())
      << "a deadline-cancelled upload was reported as a clean finish: "
      << listener->finish_status();
}

/// A listener whose OnNext() future the test resolves on demand: the upload
/// parks in the application until the late-continuation test releases it.
class DeferredDoPutListener final : public FinishRecordingListener {
 public:
  arrow::Future<> OnNext(FlightStreamChunk) override {
    next_awaits_.fetch_add(1);
    std::lock_guard<std::mutex> lock(mutex_);
    pending_ = arrow::Future<>::Make();
    return pending_;
  }

  /// Resolve the parked OnNext, OK releases the decode continuation.
  void ResolvePending() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_.is_valid() && !pending_.is_finished()) {
      pending_.MarkFinished(arrow::Status::OK());
    }
  }

  std::atomic<int> next_awaits_{0};

 private:
  std::mutex mutex_;
  arrow::Future<> pending_;
};

using DeferredDoPutServer = RecordingUploadServer<DeferredDoPutListener>;

TEST(AsyncGrpcTest, DoPutClientCancelWhileListenerParkedStaysSafe) {
  // The listener's OnNext() resolves after the deadline already finished the
  // RPC: the parked decode continuation must stand down (release its hold,
  // never touch the finished call) rather than reach into a destroyed reactor.
  // The test resolves the parked future from its own thread, and the server
  // keeps its shared_ptr alive, so the resolution lands on the real listener.
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  DeferredDoPutServer flight_server;
  FlightServerOptions options(location);
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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
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
  std::shared_ptr<::grpc::Channel> channel = ::grpc::CreateChannel(
      std::string(kLoopbackHost) + ":" + std::to_string(flight_server.port()),
      ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  // Bound the call: were the descriptor ever accepted, the client would wait
  // forever for an acknowledgement, turning a logic error into a hung suite.
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  ::grpc::CompletionQueue cq;
  void* tag = nullptr;
  bool ok = false;

  auto call = stub.Call(&context, "/arrow.flight.protocol.FlightService/DoPut", &cq,
                        reinterpret_cast<void*>(1));
  ASSERT_NE(call, nullptr);
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never started";

  call->Write(request, reinterpret_cast<void*>(2));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the message was never written";

  call->WritesDone(reinterpret_cast<void*>(3));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "WritesDone never completed";

  ::grpc::Status status;
  call->Finish(&status, reinterpret_cast<void*>(4));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never finished";

  EXPECT_FALSE(status.ok()) << "an upload without a descriptor must be refused";
  EXPECT_THAT(status.error_message(), ::testing::HasSubstr("Descriptor missing"));

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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));

  // Field 1 (the descriptor), length-delimited, announcing 127 bytes of 2.
  ::grpc::Slice truncated(std::string("\x0a\x7f\x01\x02", 4));
  ::grpc::ByteBuffer request(&truncated, 1);

  std::shared_ptr<::grpc::Channel> channel = ::grpc::CreateChannel(
      std::string(kLoopbackHost) + ":" + std::to_string(flight_server.port()),
      ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  ::grpc::CompletionQueue cq;
  void* tag = nullptr;
  bool ok = false;

  auto call = stub.Call(&context, "/arrow.flight.protocol.FlightService/DoPut", &cq,
                        reinterpret_cast<void*>(1));
  ASSERT_NE(call, nullptr);
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never started";
  call->Write(request, reinterpret_cast<void*>(2));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the message was never written";
  call->WritesDone(reinterpret_cast<void*>(3));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "WritesDone never completed";
  ::grpc::Status status;
  call->Finish(&status, reinterpret_cast<void*>(4));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never finished";

  EXPECT_FALSE(status.ok()) << "a malformed first message must be refused";
  EXPECT_THAT(status.error_message(),
              ::testing::HasSubstr("Unable to read FlightDescriptor"));

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
  auto listener = std::make_shared<FailingFinishListener>();
  flight_server.set_listener(listener);
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
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

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());
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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
  ASSERT_OK(flight_server.Init(options));
  ASSERT_GT(flight_server.port(), 0);

  std::shared_ptr<::grpc::Channel> channel = ::grpc::CreateChannel(
      std::string(kLoopbackHost) + ":" + std::to_string(flight_server.port()),
      ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  ::grpc::CompletionQueue cq;
  void* tag = nullptr;
  bool ok = false;

  auto call = stub.Call(&context, "/arrow.flight.protocol.FlightService/DoPut", &cq,
                        reinterpret_cast<void*>(1));
  ASSERT_NE(call, nullptr);
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never started";

  // Half-close with no message written at all.
  call->WritesDone(reinterpret_cast<void*>(2));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "WritesDone never completed";

  ::grpc::Status status;
  call->Finish(&status, reinterpret_cast<void*>(3));
  ASSERT_TRUE(cq.Next(&tag, &ok)) << "the call never finished";

  EXPECT_FALSE(status.ok()) << "an upload with no message at all must be refused";
  EXPECT_THAT(status.error_message(), ::testing::HasSubstr("empty upload"));

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
  ASSERT_OK_AND_ASSIGN(auto location, Location::Parse(kServerUri));
  FlightServerOptions options(location);
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

  ASSERT_OK(client->Close());
  ASSERT_OK(flight_server.Shutdown());
  ASSERT_OK(flight_server.Wait());

  // The upload was rejected on its descriptor: the handler saw it, and no
  // schema or data followed it.
  ASSERT_EQ(listener->descriptor_count(), 1);
  ASSERT_EQ(listener->descriptors()[0], descriptor);
  ASSERT_EQ(listener->schema_count(), 0);
  ASSERT_EQ(listener->batches().size(), 0);
}

}  // namespace arrow::flight::transport::grpc
