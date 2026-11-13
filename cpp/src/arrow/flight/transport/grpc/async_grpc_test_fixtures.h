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

// Shared fixtures for the async generic gRPC Flight transport tests.
//
// One translation unit per part of the transport would otherwise duplicate the
// adapters, the canned test servers and the gRPC call helpers, so they live
// here. This header is internal (the `_fixtures` suffix keeps it out of the
// installed headers) and is only included by the async_grpc_*_test.cc files.

#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_async.h"
#include "arrow/flight/server_middleware.h"
#include "arrow/flight/test_auth_handlers.h"
#include "arrow/flight/transport/grpc/async_grpc_service.h"
#include "arrow/flight/transport/grpc/async_grpc_service_internal.h"
#include "arrow/ipc/writer.h"
#include "arrow/scalar.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::flight::transport::grpc {

// The ticket the test server answers with a stream that has a schema and no
// batches at all, every other ticket gets the 3-batch stream.
constexpr const char* kSchemaOnlyTicket = "schema-only-do-get-ticket";

// One canned FlightInfo, `descriptor` names it for the assertions.
inline std::unique_ptr<FlightInfo> MakeTestFlightInfo(FlightDescriptor descriptor) {
  return std::make_unique<FlightInfo>(
      FlightInfo::Make(*arrow::schema({arrow::field("value", arrow::int64())}),
                       std::move(descriptor), std::vector<FlightEndpoint>{},
                       /*total_records=*/-1, /*total_bytes=*/-1)
          .ValueOrDie());
}

// An AsyncFlightDataStream over an in-memory RecordBatchReader: every payload is
// already available, so each future is finished when it is created.
class ResolvedStream : public AsyncFlightDataStream {
 public:
  explicit ResolvedStream(std::shared_ptr<RecordBatchReader> reader)
      : reader_(std::move(reader)), mapper_(*reader_->schema()) {}

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    FlightPayload payload;
    auto status = ipc::GetSchemaPayload(*reader_->schema(), ipc_options_, mapper_,
                                        &payload.ipc_message);
    if (!status.ok()) {
      return arrow::Future<FlightPayload>::MakeFinished(status);
    }
    return arrow::Future<FlightPayload>::MakeFinished(std::move(payload));
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    auto batch = reader_->Next();
    if (!batch.ok()) {
      return arrow::Future<std::optional<FlightPayload>>::MakeFinished(batch.status());
    }
    if (*batch == nullptr) {
      // End of stream: the async interface reports it as a nullopt.
      return arrow::Future<std::optional<FlightPayload>>::MakeFinished(std::nullopt);
    }
    FlightPayload payload;
    auto status = ipc::GetRecordBatchPayload(**batch, ipc_options_, &payload.ipc_message);
    if (!status.ok()) {
      return arrow::Future<std::optional<FlightPayload>>::MakeFinished(status);
    }
    return arrow::Future<std::optional<FlightPayload>>::MakeFinished(std::move(payload));
  }

  Status Close() override { return reader_->Close(); }

 private:
  std::shared_ptr<RecordBatchReader> reader_;
  ipc::DictionaryFieldMapper mapper_;
  ipc::IpcWriteOptions ipc_options_ = ipc::IpcWriteOptions::Defaults();
};

// A vector-backed async iterator: every NextAsync() resolves immediately, and
// exhaustion is reported as a null item.  Instantiated below for both iterator
// interfaces the streaming RPCs use.
template <typename T, typename Base>
class VectorAsyncIterator final : public Base {
 public:
  explicit VectorAsyncIterator(std::vector<T> items) : items_(std::move(items)) {}

  arrow::Future<std::shared_ptr<T>> NextAsync() override {
    if (next_ >= items_.size()) {
      return arrow::Future<std::shared_ptr<T>>::MakeFinished(std::shared_ptr<T>{});
    }
    return arrow::Future<std::shared_ptr<T>>::MakeFinished(
        std::make_shared<T>(std::move(items_[next_++])));
  }

 private:
  std::vector<T> items_;
  size_t next_ = 0;
};

using SimpleListingAdapter = VectorAsyncIterator<FlightInfo, AsyncFlightListing>;
using VectorAsyncResultStream = VectorAsyncIterator<Result, AsyncResultStream>;

// An async iterator that yields one item and then fails: the error must become
// the RPC status, not truncate the stream into a success.  `make_item` produces
// that first item, or the status that fails the RPC before it.
template <typename T, typename Base>
class FailAfterFirstAsyncIterator final : public Base {
 public:
  FailAfterFirstAsyncIterator(std::function<arrow::Result<T>()> make_item,
                              std::string error_message)
      : make_item_(std::move(make_item)), error_message_(std::move(error_message)) {}

  arrow::Future<std::shared_ptr<T>> NextAsync() override {
    if (served_) {
      return arrow::Future<std::shared_ptr<T>>::MakeFinished(
          arrow::Status::IOError(error_message_));
    }
    served_ = true;
    auto item = make_item_();
    if (!item.ok()) {
      return arrow::Future<std::shared_ptr<T>>::MakeFinished(item.status());
    }
    return arrow::Future<std::shared_ptr<T>>::MakeFinished(
        std::make_shared<T>(std::move(*item)));
  }

 private:
  std::function<arrow::Result<T>()> make_item_;
  std::string error_message_;
  bool served_ = false;
};

using MidStreamErrorAsyncResultStream =
    FailAfterFirstAsyncIterator<Result, AsyncResultStream>;
using MidStreamErrorAsyncListing =
    FailAfterFirstAsyncIterator<FlightInfo, AsyncFlightListing>;

// Serves the canned answers the async-transport tests drive: 3 batches of 5
// rows for any ticket except kSchemaOnlyTicket, uploads through the listener the
// test installs, and one canned answer per unary/streaming RPC. The handlers
// run on gRPC threads, the tests read the recordings only after the server has
// shut down.
class TestFlightServer final : public AsyncGenericFlightServerBase {
 public:
  // The listener the next DoPut RPC is handed, nullptr (the default) refuses
  // uploads, which is the base class behavior.
  std::shared_ptr<AsyncFlightDataListener> CreateDoPutListener(
      const ServerCallContext&) override {
    return listener_;
  }

  void set_listener(std::shared_ptr<AsyncFlightDataListener> listener) {
    listener_ = std::move(listener);
  }

  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext&, const Ticket& request) override {
    ticket_ = request.ticket;
    auto schema = arrow::schema(
        {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
    if (request.ticket == kSchemaOnlyTicket) {
      // An empty reader still carries the schema, NextAsync() reports the end
      // of stream right away.
      return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
          std::make_shared<ResolvedStream>(
              arrow::RecordBatchReader::Make({}, schema).ValueOrDie()));
    }
    auto batch = arrow::RecordBatch::Make(
        schema, 5,
        {arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3, 4, 5]"),
         arrow::ArrayFromJSON(arrow::int64(), "[10, 20, 30, 40, 50]")});
    return arrow::Future<std::shared_ptr<AsyncFlightDataStream>>::MakeFinished(
        std::make_shared<ResolvedStream>(
            arrow::RecordBatchReader::Make({batch, batch, batch}).ValueOrDie()));
  }

  // The unary trio: canned responses. `last_descriptor_` records the request
  // of the last one served (it is written on a gRPC thread, so tests read it
  // only after the server has shut down), Command("fail") is the sentinel that
  // drives the error-path test.
  arrow::Future<std::shared_ptr<FlightInfo>> GetFlightInfoAsync(
      const ServerCallContext&, const FlightDescriptor& request) override {
    last_descriptor_ = request;
    if (request == FlightDescriptor::Command("fail")) {
      return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
          arrow::Status::KeyError("no such flight"));
    }
    if (request == FlightDescriptor::Command("null-info")) {
      // Answered with a null FlightInfo: the transport must answer what the
      // sync transport does for that case (grpc_server.cc: "Flight not found").
      return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
          std::shared_ptr<FlightInfo>{});
    }
    return arrow::Future<std::shared_ptr<FlightInfo>>::MakeFinished(
        std::make_shared<FlightInfo>(
            FlightInfo::Make(*arrow::schema({arrow::field("value", arrow::int64())}),
                             FlightDescriptor{}, std::vector<FlightEndpoint>{}, -1, -1)
                .ValueOrDie()));
  }

  arrow::Future<std::shared_ptr<SchemaResult>> GetSchemaAsync(
      const ServerCallContext&, const FlightDescriptor& request) override {
    last_descriptor_ = request;
    return arrow::Future<std::shared_ptr<SchemaResult>>::MakeFinished(
        std::shared_ptr<SchemaResult>(
            SchemaResult::Make(*arrow::schema({arrow::field("value", arrow::int64())}))
                .ValueOrDie()));
  }

  arrow::Future<std::shared_ptr<PollInfo>> PollFlightInfoAsync(
      const ServerCallContext&, const FlightDescriptor& request) override {
    last_descriptor_ = request;
    auto poll_info = std::make_shared<PollInfo>();
    poll_info->descriptor = FlightDescriptor::Command("poll-next");
    poll_info->progress = 0.5;
    return arrow::Future<std::shared_ptr<PollInfo>>::MakeFinished(std::move(poll_info));
  }

  // The streaming trio: ListActions (a vector), DoAction (an AsyncResultStream)
  // and ListFlights (an AsyncFlightListing). `last_criteria_` records the
  // filter the client sent (read only after the server stopped), the
  // "fail-mid-stream" sentinels drive the error-path tests.
  arrow::Future<std::vector<ActionType>> ListActionsAsync(
      const ServerCallContext&) override {
    std::vector<ActionType> actions = {ActionType("echo", "echo back"),
                                       ActionType("fail", "always fails")};
    if (empty_actions_) {
      actions.clear();
    }
    return arrow::Future<std::vector<ActionType>>::MakeFinished(std::move(actions));
  }

  arrow::Future<std::shared_ptr<AsyncResultStream>> DoActionAsync(
      const ServerCallContext&, const Action& action) override {
    last_action_type_ = action.type;
    if (action.type == "fail-mid-stream") {
      return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
          std::make_shared<MidStreamErrorAsyncResultStream>(
              [] { return Result{arrow::Buffer::FromString("first")}; },
              "mid-stream action failure"));
    }
    if (action.type == "null-result-stream") {
      // Answered with a null stream: the transport answers CANCELLED for this
      // case (grpc_server.cc:404-406), before writing anything.
      return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
          std::shared_ptr<AsyncResultStream>{});
    }
    std::vector<Result> made;
    made.emplace_back(arrow::Buffer::FromString("one"));
    if (action.type == "multi") {
      made.emplace_back(arrow::Buffer::FromString("two"));
      made.emplace_back(arrow::Buffer::FromString("three"));
    }
    return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
        std::make_shared<VectorAsyncResultStream>(std::move(made)));
  }

  arrow::Future<std::shared_ptr<AsyncFlightListing>> ListFlightsAsync(
      const ServerCallContext&, const Criteria* criteria) override {
    if (criteria != nullptr) {
      last_criteria_ = *criteria;
    }
    const std::string expression = criteria == nullptr ? "" : criteria->expression;
    if (expression == "fail-mid-stream") {
      return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
          std::make_shared<MidStreamErrorAsyncListing>(
              [] {
                return std::move(
                    *MakeTestFlightInfo(FlightDescriptor::Path({"first-listing"})));
              },
              "mid-stream listing failure"));
    }
    if (expression.empty()) {
      // No criteria: the sync handler passes an empty (never null) Criteria
      // to the handler and the async reactor must do the same, so an empty
      // expression is reachable here.
      return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
          std::make_shared<SimpleListingAdapter>(std::vector<FlightInfo>{}));
    }
    std::vector<FlightInfo> flights;
    for (const auto& name : {"first-listing", "second-listing"}) {
      flights.push_back(std::move(*MakeTestFlightInfo(FlightDescriptor::Path({name}))));
    }
    return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
        std::make_shared<SimpleListingAdapter>(std::move(flights)));
  }

  void set_empty_actions() { empty_actions_ = true; }

  const std::string& ticket() const { return ticket_; }
  const FlightDescriptor& last_descriptor() const { return last_descriptor_; }
  const std::string& last_action_type() const { return last_action_type_; }
  const Criteria& last_criteria() const { return last_criteria_; }

 private:
  std::shared_ptr<AsyncFlightDataListener> listener_;
  std::string ticket_;
  FlightDescriptor last_descriptor_;
  std::string last_action_type_;
  Criteria last_criteria_;
  bool empty_actions_ = false;
};

// Echoes every chunk of a DoExchange back, in order, on the async API: the
// reader's demand drives the loop, each write is awaited, and the all-null
// chunk ends the exchange. `exchanges_in_flight` proves an abandoned
// exchange still leaves the handler: the state object is freed when the last
// continuation dies and decrements the counter on destruction.
class EchoExchangeTestServer : public AsyncGenericFlightServerBase {
 public:
  arrow::Future<> DoExchangeAsync(
      const ServerCallContext&, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    exchange_count_.fetch_add(1);
    exchanges_in_flight_.fetch_add(1);
    auto state = std::make_shared<EchoState>();
    state->reader = std::move(reader);
    state->writer = std::move(writer);
    state->in_flight = &exchanges_in_flight_;
    return EchoNext(state);
  }

  int exchanges_in_flight() const { return exchanges_in_flight_.load(); }
  int exchange_count() const { return exchange_count_.load(); }

 private:
  struct EchoState {
    std::shared_ptr<AsyncFlightMessageReader> reader;
    std::shared_ptr<AsyncFlightMessageWriter> writer;
    std::atomic<int>* in_flight = nullptr;
    bool begun = false;
    ~EchoState() { in_flight->fetch_sub(1); }
  };

  arrow::Future<> EchoNext(const std::shared_ptr<EchoState>& state) {
    // The recursion depth follows the batch count of one exchange.  Flatten
    // into a loop if a single exchange ever carries thousands.
    return state->reader->NextAsync().Then(
        [this, state](const FlightStreamChunk& chunk) -> arrow::Future<> {
          if (chunk.data == nullptr && chunk.app_metadata == nullptr) {
            // End of the exchange.
            return arrow::Future<>::MakeFinished();
          }
          arrow::Future<> written = arrow::Future<>::MakeFinished();
          if (chunk.data != nullptr && !state->begun) {
            state->begun = true;
            written = state->writer->BeginAsync(chunk.data->schema());
          }
          if (chunk.data != nullptr && chunk.app_metadata != nullptr) {
            written = written.Then([state, chunk]() {
              return state->writer->WriteWithMetadataAsync(*chunk.data,
                                                           chunk.app_metadata);
            });
          } else if (chunk.data != nullptr) {
            written = written.Then([state, chunk]() {
              return state->writer->WriteRecordBatchAsync(*chunk.data);
            });
          } else {
            written = written.Then([state, chunk]() {
              return state->writer->WriteMetadataAsync(chunk.app_metadata);
            });
          }
          return written.Then([this, state]() { return EchoNext(state); });
        });
  }

  std::atomic<int> exchanges_in_flight_{0};
  std::atomic<int> exchange_count_{0};
};

// Records what the decoder hands to the application: the schema, every chunk,
// and whether the consumer accepted it. OnNext returns whatever status the
// test configured, so the upload's error path is reachable from here.
class RecordingListener : public AsyncFlightDataListener {
 public:
  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++schema_count_;
    schema_ = std::move(schema);
    return arrow::Status::OK();
  }

  arrow::Future<> OnNext(FlightStreamChunk chunk) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (chunk.data) {
      batches_.push_back(std::move(chunk.data));
    } else {
      ++metadata_only_count_;
      metadata_chunks_.push_back(std::move(chunk.app_metadata));
    }
    last_status_ = next_status_;
    return arrow::Future<>::MakeFinished(next_status_);
  }

  // Records the upload's descriptor and reports whatever status the test
  // configured, so the descriptor rejection path is reachable from here too.
  arrow::Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
    std::lock_guard<std::mutex> lock(mutex_);
    descriptors_.push_back(descriptor);
    return arrow::Future<>::MakeFinished(descriptor_status_);
  }

  // Consumer-side rejection of the upload, as the decoder tests do.
  void set_next_status(arrow::Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    next_status_ = std::move(status);
  }
  // Rejection before any schema or data arrives.
  void set_descriptor_status(arrow::Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    descriptor_status_ = std::move(status);
  }

  int schema_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return schema_count_;
  }
  int descriptor_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(descriptors_.size());
  }
  int metadata_only_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_only_count_;
  }
  std::shared_ptr<arrow::Schema> schema() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return schema_;
  }
  std::vector<FlightDescriptor> descriptors() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return descriptors_;
  }
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return batches_;
  }
  std::vector<std::shared_ptr<arrow::Buffer>> metadata_chunks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return metadata_chunks_;
  }
  arrow::Status last_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_status_;
  }

 protected:
  /// The callbacks run on gRPC threads while the test thread reads the
  /// accessors, so everything they share goes through this.
  mutable std::mutex mutex_;
  int schema_count_ = 0;
  int metadata_only_count_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<FlightDescriptor> descriptors_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::vector<std::shared_ptr<arrow::Buffer>> metadata_chunks_;
  arrow::Status descriptor_status_;
  arrow::Status next_status_;
  arrow::Status last_status_;

 protected:
  int finish_count_ = 0;
  int cancel_count_ = 0;
  Status finish_status_;
  Status cancel_status_;
};

// A listener that records the terminal events on top of RecordingListener:
// OnFinish (the upload ended, whichever way) and what Cancel() reported, so
// the tests can assert on an upload cancelled from the server side.
class FinishRecordingListener : public RecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    std::lock_guard<std::mutex> lock(mutex_);
    finish_count_++;
    finish_status_ = std::move(status);
    return arrow::Future<>::MakeFinished(arrow::Status::OK());
  }

  /// Cancel from whatever thread the test drives, records what Cancel() said.
  void CancelWith(Status status) {
    const arrow::Status cancel_status = Cancel(std::move(status)).status();
    std::lock_guard<std::mutex> lock(mutex_);
    cancel_status_ = cancel_status;
    ++cancel_count_;
  }

  int finish_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_count_;
  }
  int cancel_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancel_count_;
  }
  Status finish_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_status_;
  }
  Status cancel_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancel_status_;
  }
};

// A server that accepts uploads: the transport asks the server for the per-RPC
// listener through CreateDoPutListener().  One fresh listener per RPC, as
// overlapping uploads require.  The test either reads them after the server has
// stopped, or waits for them with WaitForListener when it must act mid-upload.
//
// The listener type is the fixture's parameter, so a test can plug in one that
// parks, defers or cancels an upload without re-implementing the seam.
template <typename Listener>
class RecordingUploadServer : public AsyncGenericFlightServerBase {
 public:
  std::shared_ptr<AsyncFlightDataListener> CreateDoPutListener(
      const ServerCallContext&) override {
    auto listener = std::make_shared<Listener>();
    std::lock_guard<std::mutex> lock(mutex_);
    listeners_.push_back(listener);
    return listener;
  }

  int calls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(listeners_.size());
  }

  std::shared_ptr<Listener> listener(size_t i) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return listeners_[i];
  }

  /// \brief Wait (bounded) for the listener of the i-th upload to exist.
  /// The factory runs on a gRPC thread, so a test that must act mid-upload
  /// waits for it here instead of racing the read.
  std::shared_ptr<Listener> WaitForListener(size_t i = 0) const {
    for (int attempt = 0; attempt < 500; attempt++) {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        if (listeners_.size() > i) return listeners_[i];
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return nullptr;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::shared_ptr<Listener>> listeners_;
};

using TestUploadServer = RecordingUploadServer<FinishRecordingListener>;

/// A listener that acknowledges every batch it is handed with one application
/// metadata message, the way an application reports per-chunk progress, and
/// takes the next batch only once that ack is written: chaining OnNext()'s
/// future on WriteMetadataAsync()'s is the upload's backpressure. The batch
/// recording itself is RecordingListener's.
class MetadataReplyingListener final : public RecordingListener {
 public:
  arrow::Future<> OnNext(FlightStreamChunk chunk) override {
    if (!chunk.data) {
      return RecordingListener::OnNext(std::move(chunk));
    }
    const auto ack = arrow::Buffer::FromString(
        "ack " + std::to_string(chunk.data->num_rows()) + " rows");
    return writer()->WriteMetadataAsync(*ack).Then([this, chunk]() {
      {
        std::lock_guard<std::mutex> lock(mutex_);
        ++wrote_;
      }
      return RecordingListener::OnNext(chunk);
    });
  }

  int wrote() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return wrote_;
  }

 private:
  int wrote_ = 0;
};

/// A listener whose OnFinish fails asynchronously: the commit it performs is
/// what decides whether the upload succeeded, so the client must not be told
/// the upload is OK.
class FailingFinishListener final : public FinishRecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    static_cast<void>(FinishRecordingListener::OnFinish(std::move(status)));
    return arrow::Future<>::MakeFinished(
        arrow::Status::IOError("the upload could not be committed"));
  }
};

/// A listener that calls back into the writer from OnFinish: OnFinish is
/// application code running on a transport thread, so the transport must not
/// hold its own write lock across it.
class FinishingAckListener final : public FinishRecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    finish_write =
        writer()->WriteMetadataAsync(*arrow::Buffer::FromString("from-onfinish"));
    return FinishRecordingListener::OnFinish(std::move(status));
  }

  arrow::Future<> finish_write;
};

/// Hands out a MetadataReplyingListener for every upload.
using MetadataReplyingUploadServer = RecordingUploadServer<MetadataReplyingListener>;

// Middleware execution counts, shared between the factory, the middleware and
// the test thread (all three touch it).
struct MiddlewareTrace {
  std::mutex mutex;
  int start_call = 0;
  int sending_headers = 0;
  int call_completed = 0;
};

// Records middleware execution so a test can assert StartCall/CallCompleted
// ran on the async path (the sync path is covered by TestMiddleware).
class RecordingServerMiddleware : public ServerMiddleware {
 public:
  explicit RecordingServerMiddleware(std::shared_ptr<MiddlewareTrace> trace)
      : trace_(std::move(trace)) {}

  std::string name() const override { return "recording"; }
  void SendingHeaders(AddCallHeaders* /*outgoing_headers*/) override {
    std::lock_guard<std::mutex> guard(trace_->mutex);
    trace_->sending_headers++;
  }
  void CallCompleted(const Status& /*status*/) override {
    std::lock_guard<std::mutex> guard(trace_->mutex);
    trace_->call_completed++;
  }

 private:
  std::shared_ptr<MiddlewareTrace> trace_;
};

class RecordingServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  explicit RecordingServerMiddlewareFactory(std::shared_ptr<MiddlewareTrace> trace)
      : trace_(std::move(trace)) {}

  Status StartCall(const CallInfo& /*info*/, const ServerCallContext& /*context*/,
                   std::shared_ptr<ServerMiddleware>* middleware) override {
    {
      std::lock_guard<std::mutex> guard(trace_->mutex);
      trace_->start_call++;
    }
    *middleware = std::make_shared<RecordingServerMiddleware>(trace_);
    return Status::OK();
  }

 private:
  std::shared_ptr<MiddlewareTrace> trace_;
};

class RejectingServerMiddlewareFactory : public ServerMiddlewareFactory {
 public:
  Status StartCall(const CallInfo&, const ServerCallContext&,
                   std::shared_ptr<ServerMiddleware>*) override {
    return Status::Invalid("rejected by middleware");
  }
};

// The gRPC transport builds the generic service with the shared context
// helper (which runs middleware and auth). Tests that register the service
// on a hand-built grpc::ServerBuilder must pass it explicitly.
inline std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>
MakeAsyncHelper() {
  return std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
      /*auth_handler=*/nullptr, MiddlewareFactoryList{});
}

// The host every test server listens on and every client connects to:
// 127.0.0.1, not localhost. localhost also resolves to [::1], gRPC then binds
// the ephemeral port on both, and when another process already holds it on
// 127.0.0.1 the server keeps only [::1] while the client reaches that process.
static const char kLoopbackHost[] = "127.0.0.1";
// kLoopbackHost with an ephemeral port, for a hand-built grpc::ServerBuilder.
static const char kListenAddress[] = "127.0.0.1:0";
// kLoopbackHost with an ephemeral port, for a FlightServerBase's options.
static const char kServerUri[] = "grpc://127.0.0.1:0";

// The method name no FlightService stub knows: the transport must answer it
// with the UNIMPLEMENTED fallback reactor rather than any typed path. It is
// called over a raw generic stub, because only a generic client can name a
// method the service does not have.
constexpr const char* kUnknownMethod =
    "/arrow.flight.protocol.FlightService/NoSuchMethod";

// Call `kUnknownMethod` on the server listening on `port` with the sync-style
// generic stub API, and return the server's status.
inline ::grpc::Status CallUnknownMethod(int port) {
  std::shared_ptr<::grpc::Channel> channel =
      ::grpc::CreateChannel(std::string(kLoopbackHost) + ":" + std::to_string(port),
                            ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  ::grpc::ByteBuffer request;
  ::grpc::ByteBuffer response;
  ::grpc::CompletionQueue cq;
  std::unique_ptr<::grpc::ClientAsyncResponseReader<::grpc::ByteBuffer>> call =
      stub.PrepareUnaryCall(&context, kUnknownMethod, request, &cq);
  call->StartCall();
  ::grpc::Status status;
  call->Finish(&response, &status, reinterpret_cast<void*>(1));
  void* tag = nullptr;
  bool ok = false;
  cq.Next(&tag, &ok);
  return status;
}

// Upload `messages` over a raw generic DoPut call to the server listening on
// `port`, half-close, and return the server's status. Only a raw call can send
// FlightData the FlightClient would refuse to build. The 10 s deadline bounds
// the call: were a bad upload ever accepted, the client would wait forever for
// an acknowledgement, turning a logic error into a hung suite.
inline ::grpc::Status CallDoPutRaw(int port,
                                   const std::vector<::grpc::ByteBuffer>& messages) {
  std::shared_ptr<::grpc::Channel> channel =
      ::grpc::CreateChannel(std::string(kLoopbackHost) + ":" + std::to_string(port),
                            ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  ::grpc::CompletionQueue cq;
  std::unique_ptr<::grpc::GenericClientAsyncReaderWriter> call =
      stub.PrepareCall(&context, "/arrow.flight.protocol.FlightService/DoPut", &cq);
  void* tag = nullptr;
  bool ok = false;
  call->StartCall(reinterpret_cast<void*>(1));
  EXPECT_TRUE(cq.Next(&tag, &ok)) << "the call never started";
  for (const auto& message : messages) {
    call->Write(message, reinterpret_cast<void*>(2));
    EXPECT_TRUE(cq.Next(&tag, &ok)) << "the message was never written";
  }
  call->WritesDone(reinterpret_cast<void*>(3));
  EXPECT_TRUE(cq.Next(&tag, &ok)) << "WritesDone never completed";
  ::grpc::Status status;
  call->Finish(&status, reinterpret_cast<void*>(4));
  EXPECT_TRUE(cq.Next(&tag, &ok)) << "the call never finished";
  return status;
}

// The generic service and the server a test registered it on by hand, with the
// port the server bound. `service` is declared first so it outlives `server`.
struct HandBuiltServer {
  std::unique_ptr<AsyncGenericFlightService> service;
  std::unique_ptr<::grpc::Server> server;
  int port = 0;
};

// Serve `base` on kListenAddress from a hand-built grpc::ServerBuilder. The
// caller checks `server` is non-null and `port` non-zero.
inline HandBuiltServer StartHandBuilt(
    AsyncGenericFlightServerBase* base,
    std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper =
        MakeAsyncHelper()) {
  HandBuiltServer out;
  out.service = std::make_unique<AsyncGenericFlightService>(base, std::move(helper));
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(kListenAddress, ::grpc::InsecureServerCredentials(),
                           &out.port);
  builder.RegisterCallbackGenericService(out.service.get());
  out.server = builder.BuildAndStart();
  return out;
}

// The single int64 column ("a") the upload tests use.
inline std::shared_ptr<Schema> OneColumnSchema() {
  return arrow::schema({arrow::field("a", arrow::int64())});
}

// One 2-row batch of OneColumnSchema(), rows [1, 2].
inline std::shared_ptr<RecordBatch> OneColumnBatch(
    const std::shared_ptr<Schema>& schema = OneColumnSchema()) {
  return arrow::RecordBatch::Make(schema, 2,
                                  {arrow::ArrayFromJSON(arrow::int64(), "[1, 2]")});
}

// The location the tests start their servers on, see kServerUri.
inline const Location& LocalhostLocation() {
  static const Location location = *Location::Parse(kServerUri);
  return location;
}

// Connect a FlightClient to the server listening on `port`.
inline arrow::Result<std::unique_ptr<FlightClient>> ConnectTo(int port) {
  ARROW_ASSIGN_OR_RAISE(auto location, Location::ForScheme("grpc", kLoopbackHost, port));
  return FlightClient::Connect(location);
}

// The "a", "b" int64 schema the canned streams serve.
inline std::shared_ptr<Schema> TwoColumnSchema() {
  return arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
}

// One 3-row batch of TwoColumnSchema(), the payload the DoPut tests upload.
inline std::shared_ptr<RecordBatch> TwoColumnBatch(
    const std::shared_ptr<Schema>& schema = TwoColumnSchema()) {
  return arrow::RecordBatch::Make(schema, 3,
                                  {arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3]"),
                                   arrow::ArrayFromJSON(arrow::int64(), "[10, 20, 30]")});
}

// Drain a DoAction result stream, projecting each Result to a string.
inline arrow::Result<std::vector<std::string>> DrainResultBodies(ResultStream* results) {
  std::vector<std::string> out;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto result, results->Next());
    if (result == nullptr) {
      return out;
    }
    out.push_back(result->body->ToString());
  }
}

// Drain a ListFlights listing, projecting each FlightInfo to its path.
inline arrow::Result<std::vector<std::vector<std::string>>> DrainListingPaths(
    FlightListing* listing) {
  std::vector<std::vector<std::string>> out;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto info, listing->Next());
    if (info == nullptr) {
      return out;
    }
    out.push_back(info->descriptor().path);
  }
}

// Poll `predicate` until it holds or the timeout expires, the DoExchange tests
// use it to bound server-side cleanup instead of sleeping a fixed time.
template <typename Predicate>
inline bool WaitFor(Predicate predicate,
                    std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}

// Expect every reactor alive after `before` was sampled to be gone, within
// WaitFor's bound: a leaked reactor would hold its call's state forever.
inline void ExpectNoReactorLeak(int64_t before) {
  EXPECT_TRUE(WaitFor([&] {
    return detail::AsyncReactorBase::live_reactors() == before;
  })) << "a reactor leaked: still alive after the server stopped (before="
      << before << ", now=" << detail::AsyncReactorBase::live_reactors() << ")";
}

// Close the client and stop the server, all three unconditionally, and return
// the first error.
inline Status StopClientAndServer(FlightClient* client,
                                  AsyncGenericFlightServerBase* server) {
  Status status = client->Close();
  Status shutdown = server->Shutdown();
  Status wait = server->Wait();
  if (status.ok()) status = shutdown;
  if (status.ok()) status = wait;
  return status;
}

// Upload one batch and report whichever surface first reports an error: the
// DoPut call, the write, or Close.
inline std::pair<std::string, arrow::Status> UploadOneBatch(
    FlightClient* client, const FlightDescriptor& descriptor,
    const std::shared_ptr<arrow::Schema>& schema,
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto result = client->DoPut(descriptor, schema);
  if (!result.ok()) {
    return {"DoPut", result.status()};
  }
  auto write_status = result->writer->WriteRecordBatch(*batch);
  if (!write_status.ok()) {
    return {"WriteRecordBatch", write_status};
  }
  return {"Close", result->writer->Close()};
}

}  // namespace arrow::flight::transport::grpc
