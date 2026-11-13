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

#ifdef _WIN32
#  include <windows.h>
#else
#  include <unistd.h>
#endif
#include <csignal>
#if defined(__linux__)
#  include <sys/prctl.h>
#endif

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <gflags/gflags.h>

// Serve DoGet over the async generic Flight transport.
//
// Deriving from AsyncGenericFlightServerBase (arrow/flight/server_async.h)
// selects the async generic gRPC transport.  DoGet is answered by DoGetAsync:
// it returns a future for a stream of payloads (AsyncFlightDataStream) instead
// of the payloads themselves, so the gRPC callback thread serving the RPC
// never waits for data.
//
// DelayedAsyncStream shows what that is for: each batch is produced on the
// stream's own worker thread, --batch_delay_ms after the transport asks for
// it.  The transport adds a callback to the future and returns, so the delay
// never blocks a gRPC callback thread (which would stall every RPC multiplexed
// onto it).  With --batch_delay_ms=0 the delay is gone and this is a plain
// fast DoGet.
//
// Any ticket streams the same batches.  Every other RPC - GetFlightInfo
// included - answers UNIMPLEMENTED, so call DoGet directly instead of
// resolving the flight first.
//
// Usage:
//   flight-async-generic-doget-example --port=31337 --batch_delay_ms=200
//   flight-async-generic-doget-example --port=0 --demo --batches=5
//
// Run with no arguments to print this message and exit.

DEFINE_int32(port, 0, "Port to listen on (0 picks a free port)");
DEFINE_int32(batches, 5, "Number of batches in the DoGet stream");
DEFINE_int32(batch_delay_ms, 50,
             "Delay before each batch, spent on the stream's own worker thread");
DEFINE_bool(demo, false, "Run the DoGet self-check against the server, then exit");

namespace flight = arrow::flight;

/// \brief The current process' id, portably.
int64_t CurrentPid() {
#ifdef _WIN32
  return static_cast<int64_t>(::GetCurrentProcessId());
#else
  return static_cast<int64_t>(::getpid());
#endif
}

/// \brief A reader with `num_batches` batches of three int64 values.
class BatchReader : public arrow::RecordBatchReader {
 public:
  BatchReader(std::shared_ptr<arrow::Schema> schema, int32_t num_batches)
      : schema_(std::move(schema)), num_batches_(num_batches) {}

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    if (next_batch_ >= num_batches_) {
      *batch = nullptr;  // end of stream
      return arrow::Status::OK();
    }
    arrow::Int64Builder builder;
    for (int64_t i = 0; i < 3; i++) {
      ARROW_RETURN_NOT_OK(builder.Append(3 * next_batch_ + i));
    }
    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    *batch = arrow::RecordBatch::Make(schema_, array->length(), {array});
    ++next_batch_;
    return arrow::Status::OK();
  }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  int32_t num_batches_;
  int32_t next_batch_ = 0;
};

/// \brief The stream's worker state.  Heap-allocated and shared with the worker
/// thread: the transport deletes the stream as soon as a payload completes
/// (the end-of-stream payload is completed *on the worker*), so the worker must
/// not touch the stream object after that.  Everything it needs lives here.
struct WorkerState {
  std::unique_ptr<flight::FlightDataStream> stream;
  int32_t delay_ms = 0;
  std::mutex mutex;
  std::condition_variable ready;
  bool closed = false;
  /// The payload the transport is waiting for, if one was requested.
  arrow::Future<std::optional<flight::FlightPayload>> pending;
  bool has_pending = false;
  /// Whether the wrapped stream has been closed, so Close() closes it once.
  bool stream_closed = false;
  std::thread thread;
};

/// \brief The worker: wait for a payload request, sleep delay_ms, produce it,
/// complete the future the transport holds.
void RunWorker(std::shared_ptr<WorkerState> state) {
#if defined(__linux__)
  // Name the thread, so /proc/<pid>/task/*/comm distinguishes this server's own
  // per-stream workers (doget_worker) from gRPC's pool threads (event_engine /
  // lifeguard / grpc_global_timer).  Thread names are inherited, and gRPC names
  // its threads, so an anonymous thread created from a gRPC callback would
  // otherwise show up as another "event_engine".  comm is capped at 15
  // characters.
  prctl(PR_SET_NAME, "doget_worker", 0, 0, 0);
#endif
  std::unique_lock<std::mutex> lock(state->mutex);
  while (!state->closed) {
    state->ready.wait(lock, [&] { return state->has_pending || state->closed; });
    if (state->closed) {
      break;
    }
    // Take the request: after this, only this thread resolves the future.
    state->has_pending = false;
    arrow::Future<std::optional<flight::FlightPayload>> future = state->pending;
    lock.unlock();
    std::this_thread::sleep_for(std::chrono::milliseconds(state->delay_ms));
    lock.lock();
    const bool cancelled = state->closed;
    lock.unlock();
    // Runs the transport's callback inline, on this thread: it serializes the
    // payload, and on end of stream it calls Close() (hence the lock dance).
    if (cancelled) {
      future.MarkFinished(arrow::Status::Cancelled());
    } else {
      auto payload = state->stream->Next();
      if (!payload.ok()) {
        future.MarkFinished(payload.status());
      } else if (payload->ipc_message.metadata == nullptr) {
        // End of stream: the async interface reports it as a nullopt.
        future.MarkFinished(std::nullopt);
      } else {
        future.MarkFinished(std::move(*payload));
      }
    }
    lock.lock();
  }
  // The worker is the only thread that touched the wrapped stream, so it is the
  // one that closes it (see Close(), which only asks it to stop).
  lock.unlock();
  ARROW_WARN_NOT_OK(state->stream->Close(), "closing the DoGet stream failed");
  lock.lock();
  state->ready.notify_all();
}

/// \brief The stream the async transport pulls from, with a sleep per batch.
///
/// NextAsync() hands the transport a future and returns at once, the delay and
/// the serialization of the wrapped synchronous stream happen on this stream's
/// own worker thread.  The transport keeps one payload in flight at a time --
/// it asks for the next only once the write completed -- so a single worker is
/// enough.
///
/// One thread per stream.  A server with many slow streams may prefer a shared
/// timer or thread pool, swapping WorkerState's thread for a task on that pool.
class DelayedAsyncStream : public flight::AsyncFlightDataStream {
 public:
  DelayedAsyncStream(std::unique_ptr<flight::FlightDataStream> stream, int32_t delay_ms)
      : state_(std::make_shared<WorkerState>()) {
    state_->stream = std::move(stream);
    state_->delay_ms = delay_ms;
    std::shared_ptr<WorkerState> state = state_;
    state_->thread = std::thread([state] { RunWorker(state); });
  }

  ~DelayedAsyncStream() override {
    Stop();
    if (state_->thread.joinable()) {
      if (state_->thread.get_id() == std::this_thread::get_id()) {
        // Deleted from inside its own worker (the transport releases the
        // stream as a payload completes): joining would deadlock.
        state_->thread.detach();
      } else {
        state_->thread.join();
      }
    }
  }

  arrow::Future<flight::FlightPayload> GetSchemaPayloadAsync() override {
    // The schema is ready now: no delay, no worker.  This runs before the first
    // NextAsync(), so it cannot overlap the worker's use of the stream.
    return arrow::Future<flight::FlightPayload>::MakeFinished(
        state_->stream->GetSchemaPayload());
  }

  arrow::Future<std::optional<flight::FlightPayload>> NextAsync() override {
    arrow::Future<std::optional<flight::FlightPayload>> future =
        arrow::Future<std::optional<flight::FlightPayload>>::Make();
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->closed) {
        future.MarkFinished(arrow::Status::Cancelled());
        return future;
      }
      state_->pending = future;
      state_->has_pending = true;
    }
    state_->ready.notify_one();
    return future;
  }

  arrow::Status Close() override {
    // Only ask the worker to stop: the wrapped stream belongs to the worker
    // thread, which may be inside stream->Next() right now, and Next()/Close()
    // both mutate the IPC reader. Closing from this (gRPC) thread would race
    // with that call and can corrupt the stream. Stop() is also what resolves
    // a payload the worker has not taken yet, so this stays non-blocking; the
    // worker closes the stream after it stops producing.
    Stop();
    return arrow::Status::OK();
  }

 private:
  /// \brief Stop the worker, resolve a payload it has not taken yet.  Idempotent.
  /// Does not join: called by the transport, whose threads must not block on
  /// the worker's sleep.
  void Stop() {
    arrow::Future<std::optional<flight::FlightPayload>> pending;
    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      state_->closed = true;
      if (state_->has_pending) {
        state_->has_pending = false;
        pending = state_->pending;
      }
    }
    state_->ready.notify_all();
    if (pending.is_valid()) {
      // Runs the transport's callback inline, it reacts by calling Close().
      pending.MarkFinished(arrow::Status::Cancelled());
    }
  }

  std::shared_ptr<WorkerState> state_;
};

/// \brief The server: DoGetAsync is the only handler overridden, so every other
/// RPC answers UNIMPLEMENTED.
class ExampleServer : public flight::AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<flight::AsyncFlightDataStream>> DoGetAsync(
      const flight::ServerCallContext& context, const flight::Ticket& request) override {
    std::cout << "DoGet: ticket=" << request.ticket << std::endl;
    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    auto reader = std::make_shared<BatchReader>(schema, FLAGS_batches);
    std::shared_ptr<flight::AsyncFlightDataStream> stream =
        std::make_shared<DelayedAsyncStream>(
            std::make_unique<flight::RecordBatchStream>(reader), FLAGS_batch_delay_ms);
    // The stream is ready here, but its payloads are not: the transport will
    // wait on the futures NextAsync() returns.
    return arrow::Future<std::shared_ptr<flight::AsyncFlightDataStream>>::MakeFinished(
        std::move(stream));
  }
};

/// \brief The --demo self-check: drain one DoGet stream and count its batches.
bool RunDemo(int port) {
  auto location = flight::Location::ForGrpcTcp("127.0.0.1", port);
  if (!location.ok()) {
    std::cerr << "Failed to build client location: " << location.status() << std::endl;
    return false;
  }
  auto client = flight::FlightClient::Connect(*location);
  if (!client.ok()) {
    std::cerr << "Failed to connect: " << client.status() << std::endl;
    return false;
  }
  auto reader = (*client)->DoGet(flight::Ticket("demo"));
  if (!reader.ok()) {
    std::cerr << "DoGet failed: " << reader.status() << std::endl;
    return false;
  }
  int64_t num_batches = 0;
  while (true) {
    auto chunk = (*reader)->Next();
    if (!chunk.ok()) {
      std::cerr << "DoGet drain failed: " << chunk.status() << std::endl;
      return false;
    }
    if (chunk->data == nullptr) break;  // end of stream
    ++num_batches;
  }
  if (num_batches == FLAGS_batches) {
    std::cout << "PASS: DoGet streamed " << num_batches << " batches" << std::endl;
    return true;
  }
  std::cout << "FAIL: DoGet streamed " << num_batches << " batches, expected "
            << FLAGS_batches << std::endl;
  return false;
}

int main(int argc, char** argv) {
  if (argc == 1) {
    // As in the other examples: a bare run (e.g. from ctest) does not start a
    // server, it just prints the usage.
    std::cout << "Usage: " << argv[0]
              << " [--port=PORT] [--batches=N] [--batch_delay_ms=MS] [--demo]"
              << std::endl;
    return EXIT_SUCCESS;
  }
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  ExampleServer server;
  auto location = flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port);
  if (!location.ok()) {
    std::cerr << location.status() << std::endl;
    return EXIT_FAILURE;
  }
  auto status = server.Init(flight::FlightServerOptions(*location));
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  std::cout << "Serving Flight on " << server.location().ToString() << std::endl;
  std::cout << "Server pid " << CurrentPid() << ", port " << server.port() << std::endl;

  if (FLAGS_demo) {
    bool ok = RunDemo(server.port());
    ARROW_WARN_NOT_OK(server.Shutdown(), "Error shutting down server");
    ARROW_WARN_NOT_OK(server.Wait(), "Error waiting for server shutdown");
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
  }

  status = server.SetShutdownOnSignals({SIGINT, SIGTERM});
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  status = server.Serve();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
