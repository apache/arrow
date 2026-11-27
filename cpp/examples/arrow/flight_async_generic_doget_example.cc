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

#include <csignal>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/thread_pool.h>
#include <gflags/gflags.h>

// Serve DoGet over the async generic Flight transport.
//
// Deriving from AsyncGenericFlightServerBase (arrow/flight/server_async.h)
// selects the async generic gRPC transport.  DoGet is answered by DoGetAsync:
// it returns a future for a stream of payloads (AsyncFlightDataStream) instead
// of the payloads themselves, so the gRPC callback thread serving the RPC
// never waits for data.
//
// DelayedAsyncStream shows what that is for: each batch is produced on Arrow's
// IO thread pool, --batch_delay_ms after the transport asks for it.  The
// transport adds a callback to the future and returns, so the delay never
// blocks a gRPC callback thread (which would stall every RPC multiplexed onto
// it).  The transport continues on the thread that completes the future, so
// each batch's write is issued from that IO thread too.  With
// --batch_delay_ms=0 the delay is gone and this is a plain fast DoGet.
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
             "Delay before each batch, spent on Arrow's IO thread pool");
DEFINE_bool(demo, false, "Run the DoGet self-check against the server, then exit");

namespace flight = arrow::flight;

/// \brief The stream the async transport pulls from, with a sleep per batch.
///
/// NextAsync() hands the transport a future and returns at once: the delay and
/// the serialization of the wrapped synchronous stream run as a task on Arrow's
/// IO thread pool, which is meant for blocking work, so the sleep parks an IO
/// thread instead of tying up a gRPC callback thread or a thread that other RPCs
/// share.  The transport keeps one payload in flight at a time -- it asks for
/// the next only once the write completed -- so at most one task reads the
/// stream at a time.
class DelayedAsyncStream : public flight::AsyncFlightDataStream {
 public:
  DelayedAsyncStream(std::shared_ptr<arrow::RecordBatchReader> reader, int32_t delay_ms)
      : state_(std::make_shared<State>(std::move(reader), delay_ms)) {}

  arrow::Future<flight::FlightPayload> GetSchemaPayloadAsync() override {
    // The schema is ready now: no delay, no task.
    return state_->stream.GetSchemaPayload();
  }

  arrow::Future<std::optional<flight::FlightPayload>> NextAsync() override {
    // The task owns a reference to the state: the transport may delete this
    // stream as soon as the payload completes.
    return arrow::DeferNotOk(arrow::io::default_io_context().executor()->Submit(
        [state = state_]() -> arrow::Result<std::optional<flight::FlightPayload>> {
          std::this_thread::sleep_for(std::chrono::milliseconds(state->delay_ms));
          std::lock_guard<std::mutex> lock(state->mutex);
          if (state->closed) {
            return arrow::Status::Cancelled("the DoGet stream was closed");
          }
          ARROW_ASSIGN_OR_RAISE(auto payload, state->stream.Next());
          if (payload.ipc_message.metadata == nullptr) {
            return std::nullopt;  // end of stream
          }
          return payload;
        }));
  }

  arrow::Status Close() override {
    // Next() and Close() both touch the IPC reader, so Close() takes the lock a
    // running task holds while it reads (never while it sleeps).  A task still
    // sleeping completes its future as Cancelled.
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->closed = true;
    return state_->stream.Close();
  }

 private:
  struct State {
    State(std::shared_ptr<arrow::RecordBatchReader> reader, int32_t delay_ms)
        : stream(std::move(reader)), delay_ms(delay_ms) {}
    flight::RecordBatchStream stream;
    int32_t delay_ms;
    std::mutex mutex;
    bool closed = false;
  };
  std::shared_ptr<State> state_;
};

/// \brief `num_batches` batches of three int64 values each.
arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> MakeReader(
    const std::shared_ptr<arrow::Schema>& schema, int32_t num_batches) {
  arrow::RecordBatchVector batches;
  for (int64_t b = 0; b < num_batches; b++) {
    arrow::Int64Builder builder;
    ARROW_RETURN_NOT_OK(
        builder.AppendValues(std::vector<int64_t>{3 * b, 3 * b + 1, 3 * b + 2}));
    ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
    batches.push_back(arrow::RecordBatch::Make(schema, array->length(), {array}));
  }
  return arrow::RecordBatchReader::Make(std::move(batches), schema);
}

/// \brief The server: DoGetAsync is the only handler overridden, so every other
/// RPC answers UNIMPLEMENTED.
class ExampleServer : public flight::AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<flight::AsyncFlightDataStream>> DoGetAsync(
      const flight::ServerCallContext& context, const flight::Ticket& request) override {
    std::cout << "DoGet: ticket=" << request.ticket << std::endl;
    auto schema = arrow::schema({arrow::field("value", arrow::int64())});
    ARROW_ASSIGN_OR_RAISE(auto reader, MakeReader(schema, FLAGS_batches));
    // The stream is ready here, but its payloads are not: the transport will
    // wait on the futures NextAsync() returns.
    return std::shared_ptr<flight::AsyncFlightDataStream>(
        std::make_shared<DelayedAsyncStream>(std::move(reader), FLAGS_batch_delay_ms));
  }
};

/// \brief The --demo self-check: drain one DoGet stream and count its batches.
arrow::Status RunDemo(int port) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location));
  ARROW_ASSIGN_OR_RAISE(auto reader, client->DoGet(flight::Ticket("demo")));
  int64_t num_batches = 0;
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto chunk, reader->Next());
    if (chunk.data == nullptr) break;  // end of stream
    ++num_batches;
  }
  if (num_batches != FLAGS_batches) {
    return arrow::Status::Invalid("DoGet streamed ", num_batches, " batches, expected ",
                                  FLAGS_batches);
  }
  std::cout << "PASS: DoGet streamed " << num_batches << " batches" << std::endl;
  return arrow::Status::OK();
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

  if (FLAGS_demo) {
    status = RunDemo(server.port());
    ARROW_WARN_NOT_OK(server.Shutdown(), "Error shutting down server");
    ARROW_WARN_NOT_OK(server.Wait(), "Error waiting for server shutdown");
    if (!status.ok()) {
      std::cout << "FAIL: " << status.ToString() << std::endl;
      return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
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
