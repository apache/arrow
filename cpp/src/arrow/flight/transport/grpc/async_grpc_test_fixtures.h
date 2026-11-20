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
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
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
inline FlightInfo MakeTestFlightInfo(FlightDescriptor descriptor) {
  return FlightInfo::Make(*arrow::schema({arrow::field("value", arrow::int64())}),
                          std::move(descriptor), std::vector<FlightEndpoint>{},
                          /*total_records=*/-1, /*total_bytes=*/-1)
      .ValueOrDie();
}

// An AsyncFlightDataStream over an in-memory RecordBatchReader: every payload is
// already available, so each future is finished when it is created.
class ResolvedStream : public AsyncFlightDataStream {
 public:
  explicit ResolvedStream(std::shared_ptr<RecordBatchReader> reader)
      : stream_(std::move(reader)) {}

  arrow::Future<FlightPayload> GetSchemaPayloadAsync() override {
    return stream_.GetSchemaPayload();
  }

  arrow::Future<std::optional<FlightPayload>> NextAsync() override {
    ARROW_ASSIGN_OR_RAISE(auto payload, stream_.Next());
    if (payload.ipc_message.metadata == nullptr) {
      return std::optional<FlightPayload>();  // end of stream
    }
    return std::optional<FlightPayload>(std::move(payload));
  }

  Status Close() override { return stream_.Close(); }

 private:
  RecordBatchStream stream_;
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

/// An iterator whose very first NextAsync() fails: the RPC must end with that error
/// before anything is written.
template <typename T, typename Base>
class FailFirstAsyncIterator final : public Base {
 public:
  explicit FailFirstAsyncIterator(std::string error_message)
      : error_message_(std::move(error_message)) {}

  arrow::Future<std::shared_ptr<T>> NextAsync() override {
    return arrow::Future<std::shared_ptr<T>>::MakeFinished(
        arrow::Status::IOError(error_message_));
  }

 private:
  std::string error_message_;
};

using FailFirstAsyncResultStream = FailFirstAsyncIterator<Result, AsyncResultStream>;
using FailFirstAsyncListing = FailFirstAsyncIterator<FlightInfo, AsyncFlightListing>;

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
    handler_calls_.fetch_add(1);
    return listener_;
  }

  void set_listener(std::shared_ptr<AsyncFlightDataListener> listener) {
    listener_ = std::move(listener);
  }

  arrow::Future<std::shared_ptr<AsyncFlightDataStream>> DoGetAsync(
      const ServerCallContext&, const Ticket& request) override {
    handler_calls_.fetch_add(1);
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
    handler_calls_.fetch_add(1);
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
    handler_calls_.fetch_add(1);
    last_descriptor_ = request;
    if (request == FlightDescriptor::Command("fail")) {
      return arrow::Future<std::shared_ptr<SchemaResult>>::MakeFinished(
          arrow::Status::KeyError("no such schema"));
    }
    if (request == FlightDescriptor::Command("null-schema")) {
      return arrow::Future<std::shared_ptr<SchemaResult>>::MakeFinished(
          std::shared_ptr<SchemaResult>{});
    }
    return arrow::Future<std::shared_ptr<SchemaResult>>::MakeFinished(
        std::shared_ptr<SchemaResult>(
            SchemaResult::Make(*arrow::schema({arrow::field("value", arrow::int64())}))
                .ValueOrDie()));
  }

  arrow::Future<std::shared_ptr<PollInfo>> PollFlightInfoAsync(
      const ServerCallContext&, const FlightDescriptor& request) override {
    handler_calls_.fetch_add(1);
    last_descriptor_ = request;
    if (request == FlightDescriptor::Command("fail")) {
      return arrow::Future<std::shared_ptr<PollInfo>>::MakeFinished(
          arrow::Status::KeyError("no such poll"));
    }
    if (request == FlightDescriptor::Command("null-poll")) {
      return arrow::Future<std::shared_ptr<PollInfo>>::MakeFinished(
          std::shared_ptr<PollInfo>{});
    }
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
    handler_calls_.fetch_add(1);
    std::vector<ActionType> actions = {ActionType("echo", "echo back"),
                                       ActionType("fail", "always fails")};
    if (fail_actions_) {
      return arrow::Future<std::vector<ActionType>>::MakeFinished(
          arrow::Status::KeyError("no such actions"));
    }
    if (empty_actions_) {
      actions.clear();
    }
    return arrow::Future<std::vector<ActionType>>::MakeFinished(std::move(actions));
  }

  arrow::Future<std::shared_ptr<AsyncResultStream>> DoActionAsync(
      const ServerCallContext&, const Action& action) override {
    handler_calls_.fetch_add(1);
    last_action_type_ = action.type;
    if (action.type == "fail-mid-stream") {
      return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
          std::make_shared<MidStreamErrorAsyncResultStream>(
              [] { return Result{arrow::Buffer::FromString("first")}; },
              "mid-stream action failure"));
    }
    if (action.type == "fail-first-next") {
      return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
          std::make_shared<FailFirstAsyncResultStream>("first demand failed"));
    }
    if (action.type == "empty-result-stream") {
      return arrow::Future<std::shared_ptr<AsyncResultStream>>::MakeFinished(
          std::make_shared<VectorAsyncResultStream>(std::vector<Result>{}));
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
    handler_calls_.fetch_add(1);
    criteria_was_null_ = criteria == nullptr;
    if (criteria != nullptr) {
      last_criteria_ = *criteria;
    }
    const std::string expression = criteria == nullptr ? "" : criteria->expression;
    if (expression == "fail-mid-stream") {
      return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
          std::make_shared<MidStreamErrorAsyncListing>(
              [] {
                return MakeTestFlightInfo(FlightDescriptor::Path({"first-listing"}));
              },
              "mid-stream listing failure"));
    }
    if (expression == "fail-first-next") {
      return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
          std::make_shared<FailFirstAsyncListing>("first demand failed"));
    }
    if (expression == "null-listing") {
      return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
          std::shared_ptr<AsyncFlightListing>{});
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
      flights.push_back(MakeTestFlightInfo(FlightDescriptor::Path({name})));
    }
    return arrow::Future<std::shared_ptr<AsyncFlightListing>>::MakeFinished(
        std::make_shared<SimpleListingAdapter>(std::move(flights)));
  }

  /// \brief Handshake and DoExchange delegate to the base class.
  ///
  /// Declared so the call is counted like every other handler: a malformed
  /// request must never reach one, whichever RPC it names.
  Status Handshake(const ServerCallContext& context, const std::string& password,
                   std::string* response) override {
    handler_calls_.fetch_add(1);
    return AsyncGenericFlightServerBase::Handshake(context, password, response);
  }

  arrow::Future<> DoExchangeAsync(
      const ServerCallContext& context, std::shared_ptr<AsyncFlightMessageReader> reader,
      std::shared_ptr<AsyncFlightMessageWriter> writer) override {
    handler_calls_.fetch_add(1);
    return AsyncGenericFlightServerBase::DoExchangeAsync(context, std::move(reader),
                                                         std::move(writer));
  }

  void set_empty_actions() { empty_actions_ = true; }
  void set_failing_actions() { fail_actions_ = true; }

  /// How many handler overrides have run: zero proves a request was rejected
  /// before dispatch.
  int handler_calls() const { return handler_calls_.load(); }

  const std::string& ticket() const { return ticket_; }
  const FlightDescriptor& last_descriptor() const { return last_descriptor_; }
  const std::string& last_action_type() const { return last_action_type_; }
  const Criteria& last_criteria() const { return last_criteria_; }
  bool criteria_was_null() const { return criteria_was_null_; }

 private:
  std::atomic<int> handler_calls_{0};
  std::shared_ptr<AsyncFlightDataListener> listener_;
  std::string ticket_;
  FlightDescriptor last_descriptor_;
  std::string last_action_type_;
  Criteria last_criteria_;
  bool criteria_was_null_ = false;
  bool empty_actions_ = false;
  bool fail_actions_ = false;
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
// the descriptor, which status the test configured for each callback, and the
// terminal events (OnFinish and what Cancel() reported), so the upload's error
// path and its ending are both reachable from here. The callbacks run on gRPC
// threads while the test thread reads the accessors, so everything they share
// goes through mutex_.
class RecordingListener : public AsyncFlightDataListener {
 public:
  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++schema_count_;
    schema_ = std::move(schema);
    return schema_status_;
  }

  arrow::Future<> OnNext(FlightStreamChunk chunk) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (chunk.data) {
      batches_.push_back(std::move(chunk.data));
    } else {
      ++metadata_only_count_;
      metadata_chunks_.push_back(std::move(chunk.app_metadata));
    }
    return next_status_;
  }

  // Records the upload's descriptor and reports whatever status the test
  // configured, so the descriptor rejection path is reachable from here too.
  arrow::Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
    std::lock_guard<std::mutex> lock(mutex_);
    descriptors_.push_back(descriptor);
    return descriptor_status_;
  }

  arrow::Future<> OnFinish(Status status) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++finish_count_;
    finish_status_ = std::move(status);
    return finish_result_;
  }

  /// \brief Cancel from whatever thread the test drives, recording what Cancel() said.
  Status CancelWith(Status status) {
    const Status cancel_status = Cancel(std::move(status));
    std::lock_guard<std::mutex> lock(mutex_);
    cancel_status_ = cancel_status;
    return cancel_status_;
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
  // Rejection of the schema.
  void set_schema_status(arrow::Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    schema_status_ = std::move(status);
  }
  // The result OnFinish() returns: a failing one overrides a successful upload.
  void set_finish_status(arrow::Status status) {
    std::lock_guard<std::mutex> lock(mutex_);
    finish_result_ = std::move(status);
  }
  /// The status OnFinish() returns, for a test that completes it late.
  void set_finish_result(Future<> result) {
    std::lock_guard<std::mutex> lock(mutex_);
    finish_result_ = std::move(result);
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
  int finish_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_count_;
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
  Status finish_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_status_;
  }
  Status cancel_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancel_status_;
  }

 protected:
  /// The callbacks run on gRPC threads while the test thread reads the
  /// accessors, so everything they share goes through this.
  mutable std::mutex mutex_;
  int schema_count_ = 0;
  int metadata_only_count_ = 0;
  int finish_count_ = 0;
  std::shared_ptr<arrow::Schema> schema_;
  std::vector<FlightDescriptor> descriptors_;
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::vector<std::shared_ptr<arrow::Buffer>> metadata_chunks_;
  arrow::Status schema_status_;
  arrow::Status descriptor_status_;
  arrow::Status next_status_;
  arrow::Status finish_status_;
  arrow::Status cancel_status_;
  Future<> finish_result_ = Future<>::MakeFinished();
};

/// Which RecordingListener callback ParkedUploadListener() parks.
enum class RecordingListenerPhase { kDescriptor, kNext, kFinish };

/// \brief A RecordingListener whose selected callback parks until the test
/// releases it, the way an application that must do asynchronous work before
/// accepting a message does. The base records the callback first; subsequent
/// callbacks of the same kind use the base result.
template <RecordingListenerPhase kPhase>
class ParkedUploadListener : public RecordingListener {
 public:
  Future<> OnDescriptor(const FlightDescriptor& descriptor) override {
    auto recorded = RecordingListener::OnDescriptor(descriptor);
    if constexpr (kPhase == RecordingListenerPhase::kDescriptor) {
      return Park(recorded);
    }
    return recorded;
  }

  Future<> OnNext(FlightStreamChunk chunk) override {
    auto recorded = RecordingListener::OnNext(std::move(chunk));
    if constexpr (kPhase == RecordingListenerPhase::kNext) {
      return Park(recorded);
    }
    return recorded;
  }

  Future<> OnFinish(Status status) override {
    auto recorded = RecordingListener::OnFinish(std::move(status));
    if constexpr (kPhase == RecordingListenerPhase::kFinish) {
      return Park(recorded);
    }
    return recorded;
  }

  /// A future completed once the parked callback has been entered.
  Future<> entered() const { return entered_; }

  /// Release the parked callback, once, with `status`; later calls are no-ops.
  void Release(Status status = Status::OK()) {
    Future<> pending;
    {
      std::lock_guard<std::mutex> lock(park_mutex_);
      if (!pending_.is_valid() || pending_.is_finished()) {
        return;
      }
      pending = std::move(pending_);
    }
    pending.MarkFinished(std::move(status));
  }

 private:
  Future<> Park(Future<> recorded) {
    {
      std::lock_guard<std::mutex> lock(park_mutex_);
      if (parked_) {
        return recorded;  // only the first callback of this kind parks
      }
      parked_ = true;
      pending_ = Future<>::Make();
    }
    entered_.MarkFinished();
    return pending_;
  }

  mutable std::mutex park_mutex_;
  bool parked_ = false;
  Future<> pending_;
  Future<> entered_ = Future<>::Make();
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

using TestUploadServer = RecordingUploadServer<RecordingListener>;

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

/// A listener that calls back into the writer from OnFinish: OnFinish is
/// application code running on a transport thread, so the transport must not
/// hold its own write lock across it.
class FinishingAckListener final : public RecordingListener {
 public:
  arrow::Future<> OnFinish(Status status) override {
    finish_write =
        writer()->WriteMetadataAsync(*arrow::Buffer::FromString("from-onfinish"));
    return RecordingListener::OnFinish(std::move(status));
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
  /// The status of every CallCompleted(): the Arrow-level oracle for a call
  /// that never reached a handler, or reached one that rejected it.
  std::vector<Status> completed_statuses;
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
  void CallCompleted(const Status& status) override {
    std::lock_guard<std::mutex> guard(trace_->mutex);
    trace_->completed_statuses.push_back(status);
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

/// The bytes of a gRPC message, for parsing a reply back from the wire.
inline std::string ByteBufferToString(::grpc::ByteBuffer message) {
  std::vector<::grpc::Slice> slices;
  std::string out;
  if (!message.Dump(&slices).ok()) {
    return out;
  }
  for (const auto& slice : slices) {
    out.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
  }
  return out;
}

/// The full method path of one FlightService RPC, for a raw generic call.
inline std::string MethodPath(std::string_view method) {
  return std::string("/arrow.flight.protocol.FlightService/") + std::string(method);
}

/// The wire bytes of a Flight request message.
///
/// The Flight types serialize exactly like their generated protobuf
/// counterparts, and unlike those protobuf types they are exported from the
/// DLL: a Windows test cannot name `arrow::flight::protocol` types at all.
template <typename T>
std::string WireBytes(const T& value) {
  std::string out;
  const auto status = value.SerializeToString(&out);
  if (!status.ok()) {
    ADD_FAILURE() << "the request did not serialize: " << status;
    return std::string();
  }
  return out;
}

// What one raw generic call to the service produced.
struct RawCallResult {
  ::grpc::Status status;
  /// The status the transport settled, with the Arrow details a FlightClient
  /// would reconstruct (category, flight code). Computed before the context
  /// dies, because the details live in its trailing metadata.
  arrow::Status arrow_status;
  /// Every response message, in the order the server wrote them.
  std::vector<::grpc::ByteBuffer> responses;
};

/// Call one method of the async generic service over a raw bidi generic stub:
/// the only way to send a request the FlightClient would refuse to build, or to
/// name a method the service does not have. The 10 s deadline bounds the call:
/// a server that accepted a bad request instead of rejecting it would otherwise
/// hang the suite.
///
/// An empty `requests` sends no message at all (the client writes and
/// half-closes at once); a ByteBuffer built from an empty Slice sends one valid
/// zero-byte protobuf message. The two are different requests.
inline arrow::Result<RawCallResult> CallRaw(
    int port, std::string_view method_path,
    const std::vector<::grpc::ByteBuffer>& requests) {
  std::shared_ptr<::grpc::Channel> channel =
      ::grpc::CreateChannel(std::string(kLoopbackHost) + ":" + std::to_string(port),
                            ::grpc::InsecureChannelCredentials());
  ::grpc::GenericStub stub(channel);
  ::grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  ::grpc::CompletionQueue cq;
  std::unique_ptr<::grpc::GenericClientAsyncReaderWriter> call =
      stub.PrepareCall(&context, std::string(method_path), &cq);
  RawCallResult out;
  auto tag = [](int id) { return reinterpret_cast<void*>(static_cast<intptr_t>(id)); };
  auto await = [&cq, &tag](int id) -> bool {
    void* done = nullptr;
    bool ok = false;
    if (!cq.Next(&done, &ok)) {
      return false;
    }
    return done == tag(id);
  };

  call->StartCall(tag(1));
  if (!await(1)) {
    return arrow::Status::IOError("raw call ", method_path, " never started");
  }
  // A write completing with ok=false means the server already refused the
  // request; stop sending and collect the RPC's final status.
  for (size_t i = 0; i < requests.size(); i++) {
    call->Write(requests[i], tag(2));
    if (!await(2)) {
      break;
    }
  }
  call->WritesDone(tag(3));
  static_cast<void>(await(3));
  // Read to EOF before finishing: the server may write several replies, and the
  // final status is only delivered once the response stream is drained.
  while (true) {
    ::grpc::ByteBuffer response;
    call->Read(&response, tag(4));
    void* done = nullptr;
    bool ok = false;
    if (!cq.Next(&done, &ok) || done != tag(4) || !ok) {
      break;
    }
    out.responses.push_back(std::move(response));
  }
  call->Finish(&out.status, tag(5));
  static_cast<void>(await(5));
  out.arrow_status = FromGrpcStatus(out.status, &context);
  cq.Shutdown();
  while (true) {
    void* done = nullptr;
    bool ok = false;
    if (!cq.Next(&done, &ok)) {
      break;
    }
  }
  return out;
}

/// \brief A gRPC server stream a test drives by hand, with a real reactor bound.
///
/// gRPC's ServerCallbackReaderWriter is the "API only to support mocking"
/// interface (see server_callback.h): binding a real reactor to an instance of
/// this class exercises that reactor's callback state machine deterministically,
/// with no network, no deadline and no race: the test says when a read or a write
/// completes and with what. The reactor's algorithm is never copied here.
///
/// Rules that keep a test honest: no callback runs under the fixture's lock, a
/// second outstanding operation in the same direction is a test failure (the
/// reactors never do that), reads and writes may coexist, and Finish() only
/// records the status: Done() delivers OnDone once the outstanding callbacks
/// have drained, the way gRPC orders them.
class ManualServerStream final
    : public ::grpc::ServerCallbackReaderWriter<::grpc::ByteBuffer, ::grpc::ByteBuffer> {
 public:
  ManualServerStream() = default;
  ~ManualServerStream() override {
    // A test that fails before draining its call must not leave the reactor
    // holding a callback this stream will never deliver.
    if (reactor_ == nullptr) {
      return;
    }
    if (finish_count_ == 0) {
      // The RPC was never settled (a test that cancelled or failed mid-call):
      // cancel it, so the reactor's reference is dropped instead of leaving a
      // live reactor behind for the next test.
      reactor_->OnCancel();
    }
    if (!done_) {
      reactor_->OnDone();
    }
  }

  // --- gRPC's ServerCallbackReaderWriter ------------------------------------

  void Finish(::grpc::Status status) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (finish_count_ != 0) {
        ADD_FAILURE() << "Finish() was called twice";
      }
      finish_status_ = std::move(status);
      ++finish_count_;
    }
    // Unlocked: a continuation on this gate may inspect the fixture.
    finish_observed_.MarkFinished();
  }

  void SendInitialMetadata() override { initial_metadata_ = true; }

  void Read(::grpc::ByteBuffer* message) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (read_buffer_ != nullptr) {
      ADD_FAILURE() << "a second read was armed while one was outstanding";
      return;
    }
    if (finish_count_ != 0) {
      ADD_FAILURE() << "a read was armed after Finish()";
      return;
    }
    read_buffer_ = message;
  }

  void Write(const ::grpc::ByteBuffer* message, ::grpc::WriteOptions) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (write_pending_) {
      ADD_FAILURE() << "a second write was armed while one was outstanding";
      return;
    }
    if (finish_count_ != 0) {
      ADD_FAILURE() << "a write was armed after Finish()";
      return;
    }
    writes_.push_back(*message);
    write_pending_ = true;
  }

  void WriteAndFinish(const ::grpc::ByteBuffer*, ::grpc::WriteOptions,
                      ::grpc::Status) override {
    ADD_FAILURE() << "unexpected WriteAndFinish";
  }

  // --- what the test drives -------------------------------------------------

  /// Bind the reactor under test. Any operation the reactor requested before
  /// the bind is replayed by gRPC's own bind path.
  void Bind(::grpc::ServerGenericBidiReactor* reactor) {
    reactor_ = reactor;
    BindReactor(reactor);
  }

  /// Complete the outstanding read with `request`, then run the reactor's
  /// callback, unlocked.
  void DeliverRead(::grpc::ByteBuffer request) {
    ::grpc::ByteBuffer* buffer = nullptr;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      buffer = read_buffer_;
      read_buffer_ = nullptr;
    }
    if (buffer == nullptr) {
      ADD_FAILURE() << "no read was outstanding";
      return;
    }
    // Into an owned copy: a gRPC ByteBuffer references slices by pointer, so the
    // caller's message must not be able to go out of scope under the reactor.
    *buffer = OwnedCopy(request);
    reactor_->OnReadDone(true);
  }

  /// A deep copy of `message` whose slices this fixture owns.
  ::grpc::ByteBuffer OwnedCopy(::grpc::ByteBuffer& message) {
    std::vector<::grpc::Slice> slices;
    const auto status = message.Dump(&slices);
    EXPECT_TRUE(status.ok());
    ::grpc::Slice owned;
    if (slices.size() == 1) {
      owned = ::grpc::Slice(slices[0]);
    } else {
      std::string bytes;
      for (const auto& slice : slices) {
        bytes.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
      }
      owned = ::grpc::Slice(bytes);
    }
    owned_slices_.push_back(std::move(owned));
    return ::grpc::ByteBuffer(&owned_slices_.back(), 1);
  }

  /// Complete the outstanding read with ok=false: the client half-closed.
  void CloseRead() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (read_buffer_ == nullptr) {
        ADD_FAILURE() << "no read was outstanding";
        return;
      }
      read_buffer_ = nullptr;
    }
    reactor_->OnReadDone(false);
  }

  /// Complete the outstanding write with `ok`.
  void CompleteWrite(bool ok) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!write_pending_) {
        ADD_FAILURE() << "no write was outstanding";
        return;
      }
      write_pending_ = false;
    }
    reactor_->OnWriteDone(ok);
  }

  /// Complete every outstanding write successfully.
  void DrainWrites() {
    while (WritePending()) {
      CompleteWrite(true);
    }
  }

  /// Cancel the RPC, as gRPC does when the client goes away.
  void Cancel() { reactor_->OnCancel(); }

  /// Deliver OnDone, which gRPC does once the call is over and drained.
  void Done() {
    if (DoneDelivered()) {
      ADD_FAILURE() << "OnDone was delivered twice";
      return;
    }
    if (FinishCount() == 0) {
      ADD_FAILURE() << "OnDone was delivered before Finish()";
    }
    if (ReadPending() || WritePending()) {
      ADD_FAILURE() << "OnDone was delivered with a callback still outstanding";
    }
    done_ = true;
    reactor_->OnDone();
  }

  ::grpc::CallbackServerContext& context() { return context_; }
  std::vector<::grpc::ByteBuffer> writes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return writes_;
  }
  bool read_pending() const { return ReadPending(); }
  bool write_pending() const { return WritePending(); }
  int finish_count() const { return FinishCount(); }
  bool done() const { return DoneDelivered(); }
  std::optional<::grpc::Status> finish_status() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_status_;
  }
  /// A future completed when the reactor finishes the call: a wait-only gate.
  Future<> finish_observed() const { return finish_observed_; }

 private:
  // gRPC's ServerCallbackReaderWriter internals. This fixture is the mock, so
  // gRPC's internal MaybeDone/refcount scheduler is deliberately not used: the
  // test drives Finish/OnDone itself.
  ::grpc::internal::ServerReactor* reactor() override { return reactor_; }
  ::grpc_call* call() override { return nullptr; }
  void CallOnDone() override {}

  bool ReadPending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return read_buffer_ != nullptr;
  }
  bool WritePending() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return write_pending_;
  }
  int FinishCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return finish_count_;
  }
  bool DoneDelivered() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return done_;
  }

  mutable std::mutex mutex_;
  ::grpc::CallbackServerContext context_;
  ::grpc::ServerGenericBidiReactor* reactor_ = nullptr;
  ::grpc::ByteBuffer* read_buffer_ = nullptr;
  bool write_pending_ = false;
  bool initial_metadata_ = false;
  bool done_ = false;
  std::vector<::grpc::ByteBuffer> writes_;
  /// Backing storage for the deep copies DeliverRead() hands the reactor. Bounded
  /// by the number of messages one test delivers; a deque never invalidates them.
  std::deque<::grpc::Slice> owned_slices_;
  int finish_count_ = 0;
  std::optional<::grpc::Status> finish_status_;
  Future<> finish_observed_ = Future<>::Make();
};

// The generic service and the server a test registered it on by hand, with the
// port the server bound. `service` is declared first so it outlives `server`.
struct HandBuiltServer {
  std::unique_ptr<AsyncGenericFlightService> service;
  std::unique_ptr<::grpc::Server> server;
  int port = 0;
};

// Serve `base` on kListenAddress from a hand-built grpc::ServerBuilder, with the
// context helper (middleware and auth) the gRPC transport would otherwise
// build. The caller checks `server` is non-null and `port` non-zero.
inline HandBuiltServer StartHandBuilt(
    AsyncGenericFlightServerBase* base,
    std::shared_ptr<GrpcServerCallContextHelper<::grpc::CallbackServerContext>> helper =
        std::make_shared<GrpcServerCallContextHelper<::grpc::CallbackServerContext>>(
            /*auth_handler=*/nullptr, MiddlewareFactoryList{})) {
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
    const std::shared_ptr<Schema>& schema) {
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

// Start `server` on `options`' location and connect a client to it: the setup
// every test that drives a real client repeats. Startup failures are tested
// with a bare Init(), not through here.
inline arrow::Result<std::unique_ptr<FlightClient>> StartAndConnect(
    AsyncGenericFlightServerBase* server,
    const FlightServerOptions& options = FlightServerOptions(LocalhostLocation())) {
  ARROW_RETURN_NOT_OK(server->Init(options));
  return ConnectTo(server->port());
}

inline arrow::Result<std::unique_ptr<FlightClient>> ConnectToWarmed(int port) {
  ARROW_ASSIGN_OR_RAISE(auto client, ConnectTo(port));
  // The status is not checked: an unimplemented method is enough to connect.
  static_cast<void>(client->ListActions({}));
  return client;
}

// The "a", "b" int64 schema the canned streams serve.
inline std::shared_ptr<Schema> TwoColumnSchema() {
  return arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
}

// One 3-row batch of TwoColumnSchema(), the payload the DoPut tests upload.
inline std::shared_ptr<RecordBatch> TwoColumnBatch(
    const std::shared_ptr<Schema>& schema) {
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
