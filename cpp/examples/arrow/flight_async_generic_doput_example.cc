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

#include <signal.h>
#ifdef _WIN32
#  include <windows.h>
#else
#  include <unistd.h>
#endif

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <gflags/gflags.h>

// Serve DoPut over the async generic Flight transport.
//
// Deriving from AsyncGenericFlightServerBase (arrow/flight/server_async.h)
// selects the async generic gRPC transport.  On this path DoPut is not answered
// by a handler method: the transport hands every decoded upload to the
// AsyncFlightDataListener the server class hands out (CreateDoPutListener), one
// listener per RPC.  The callbacks are:
//
// * OnDescriptor(const FlightDescriptor&) - the descriptor of the upload;
//   returning non-OK rejects the upload (the client sees that status when it
//   closes the writer).
// * OnSchemaDecoded(std::shared_ptr<Schema>) - from ipc::Listener, the decoded
//   schema.
// * OnNext(FlightStreamChunk) - once per decoded message, chunk.data is the
//   RecordBatch, or nullptr for metadata-only messages.
//
// All of them run on gRPC callback threads, so they must return promptly: a
// slow callback stalls the RPC and every other call that thread would serve.
// This listener only counts and prints, a server with real work would hand the
// chunk to its own queue or thread pool and return at once.
//
// Usage:
//   flight-async-generic-doput-example --port=31337
//   flight-async-generic-doput-example --port=0 --demo
//
// Run with no arguments to print this message and exit.

DEFINE_int32(port, 0, "Port to listen on (0 picks a free port)");
DEFINE_bool(demo, false, "Run the DoPut self-check against the server, then exit");

namespace flight = arrow::flight;

/// \brief The current process' id, portably.
int64_t CurrentPid() {
#ifdef _WIN32
  return static_cast<int64_t>(::GetCurrentProcessId());
#else
  return static_cast<int64_t>(::getpid());
#endif
}

constexpr int kNumBatches = 3;
constexpr int64_t kRowsPerBatch = 4;

/// \brief Make a one-column batch of `num_rows` int64 values starting at `value`.
arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeInt64Batch(
    const std::shared_ptr<arrow::Schema>& schema, int64_t value, int64_t num_rows) {
  arrow::Int64Builder builder;
  for (int64_t i = 0; i < num_rows; i++) {
    ARROW_RETURN_NOT_OK(builder.Append(value + i));
  }
  ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
  return arrow::RecordBatch::Make(schema, array->length(), {array});
}

/// \brief The AsyncFlightDataListener serving one DoPut RPC: it records what it is
/// handed as the upload streams in.  The counters are atomics because the
/// callbacks run on gRPC threads.
class CountingUploadListener : public flight::AsyncFlightDataListener {
 public:
  arrow::Future<> OnDescriptor(const flight::FlightDescriptor& descriptor) override {
    std::cout << "DoPut descriptor: " << descriptor.ToString() << std::endl;
    // Returning non-OK here would reject the upload, the client sees that
    // status when it closes the writer.
    return arrow::Future<>::MakeFinished(arrow::Status::OK());
  }

  arrow::Status OnSchemaDecoded(std::shared_ptr<arrow::Schema> schema) override {
    std::cout << "DoPut schema:";
    for (const auto& field : schema->fields()) {
      std::cout << " " << field->name() << ":" << field->type()->ToString();
    }
    std::cout << std::endl;
    return arrow::Status::OK();
  }

  arrow::Future<> OnNext(flight::FlightStreamChunk chunk) override {
    if (chunk.data != nullptr) {  // null for metadata-only messages
      num_batches_.fetch_add(1);
      num_rows_.fetch_add(chunk.data->num_rows());
    }
    return arrow::Future<>::MakeFinished(arrow::Status::OK());
  }

  /// \brief The upload ended, whichever way: here is where a consumer commits.
  /// status == OK means the client ended it normally, anything else is why it
  /// did not complete.
  arrow::Future<> OnFinish(arrow::Status status) override {
    finished_ = true;
    if (status.ok()) {
      std::cout << "DoPut finished: the client ended the upload" << std::endl;
    } else {
      std::cout << "DoPut finished: " << status.ToString() << std::endl;
    }
    return arrow::Future<>::MakeFinished(arrow::Status::OK());
  }

  int64_t num_batches() const { return num_batches_.load(); }
  int64_t num_rows() const { return num_rows_.load(); }
  bool finished() const { return finished_; }

 private:
  std::atomic<int64_t> num_batches_{0};
  std::atomic<int64_t> num_rows_{0};
  std::atomic<bool> finished_{false};
};

/// \brief The server.  Deriving from AsyncGenericFlightServerBase selects the
/// async generic transport.  Uploads are served by the listener the server
/// class itself hands out: the transport calls CreateDoPutListener() once per
/// DoPut RPC, the server-class counterpart of DoGetAsync.  Returning nullptr
/// refuses uploads (they answer UNIMPLEMENTED).
///
/// One listener per RPC, as overlapping uploads require.  This demo has a
/// single --demo client, so it keeps the listener it handed out in order to
/// check it in main(), once the server has stopped.
class UploadServer : public flight::AsyncGenericFlightServerBase {
 public:
  std::shared_ptr<flight::AsyncFlightDataListener> CreateDoPutListener(
      const flight::ServerCallContext& context) override {
    auto listener = std::make_shared<CountingUploadListener>();
    // Called concurrently, once per DoPut RPC, from gRPC callback threads: an
    // unsynchronised shared_ptr assignment here is a data race that corrupts
    // the heap. The lock only serializes the bookkeeping for the demo's
    // assertion; a real server keeps no such member.
    std::lock_guard<std::mutex> lock(listener_mutex_);
    last_listener_ = listener;
    return listener;
  }

  /// The listener the last DoPut RPC got, for the demo's assertions.
  std::shared_ptr<CountingUploadListener> last_listener() const {
    std::lock_guard<std::mutex> lock(listener_mutex_);
    return last_listener_;
  }

 private:
  mutable std::mutex listener_mutex_;
  std::shared_ptr<CountingUploadListener> last_listener_;
};

/// \brief The --demo client half: upload kNumBatches batches and close the
/// writer.  What the listener saw is read back in main(), once the server has
/// stopped.
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

  auto schema = arrow::schema({arrow::field("value", arrow::int64())});
  auto put = (*client)->DoPut(flight::FlightDescriptor::Command("upload"), schema);
  if (!put.ok()) {
    std::cerr << "DoPut failed: " << put.status() << std::endl;
    return false;
  }
  arrow::Status status;
  for (int i = 0; i < kNumBatches && status.ok(); i++) {
    auto batch = MakeInt64Batch(schema, i * kRowsPerBatch, kRowsPerBatch);
    if (!batch.ok()) {
      std::cerr << "Failed to make a batch: " << batch.status() << std::endl;
      return false;
    }
    status = put->writer->WriteRecordBatch(**batch);
  }
  if (status.ok()) {
    status = put->writer->DoneWriting();
  }
  if (status.ok()) {
    status = put->writer->Close();
  }
  if (!status.ok()) {
    std::cerr << "DoPut upload failed: " << status << std::endl;
    return false;
  }
  return true;
}

int main(int argc, char** argv) {
  if (argc == 1) {
    // As in the other examples: a bare run (e.g. from ctest) does not start a
    // server, it just prints the usage.
    std::cout << "Usage: " << argv[0] << " [--port=PORT] [--demo]" << std::endl;
    return EXIT_SUCCESS;
  }
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  UploadServer server;

  auto location = flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port);
  if (!location.ok()) {
    std::cerr << location.status() << std::endl;
    return EXIT_FAILURE;
  }
  flight::FlightServerOptions options(*location);
  // UploadServer derives from AsyncGenericFlightServerBase: the server class
  // alone selects the async generic transport, so there is no option to set.
  // Uploads come from the server's CreateDoPutListener() override.
  auto status = server.Init(options);
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  int port = server.port();
  std::cout << "Serving Flight on " << server.location().ToString() << std::endl;
  std::cout << "Server pid " << CurrentPid() << ", port " << port << std::endl;

  if (FLAGS_demo) {
    bool ok = RunDemo(port);
    ARROW_WARN_NOT_OK(server.Shutdown(), "Error shutting down server");
    ARROW_WARN_NOT_OK(server.Wait(), "Error waiting for server shutdown");
    // The listener callbacks run on gRPC threads: read its counters only once
    // the server has stopped (the same pattern the flight tests use).
    const auto listener = server.last_listener();
    if (listener == nullptr) {
      std::cout << "FAIL: the server handed out no DoPut listener" << std::endl;
      ok = false;
    } else if (listener->num_batches() == kNumBatches &&
               listener->num_rows() == kNumBatches * kRowsPerBatch &&
               listener->finished()) {
      std::cout << "PASS: listener saw " << listener->num_batches() << " batches, "
                << listener->num_rows() << " rows, and its OnFinish said the client "
                << "ended the upload" << std::endl;
    } else {
      std::cout << "FAIL: listener saw " << listener->num_batches() << " batches, "
                << listener->num_rows() << " rows, finished=" << listener->finished()
                << ", expected " << kNumBatches << " batches, "
                << kNumBatches * kRowsPerBatch << " rows, finished" << std::endl;
      ok = false;
    }
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
