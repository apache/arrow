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

#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/io/interfaces.h>
#include <arrow/util/thread_pool.h>
#include <gflags/gflags.h>

// A catalog server on the async generic Flight transport.
//
// Deriving from AsyncGenericFlightServerBase (arrow/flight/server_async.h)
// selects the async generic gRPC transport. Every handler returns a future:
// here each answer is computed on Arrow's IO thread pool, so no gRPC callback
// thread ever waits for the catalog. The transport continues the RPC on the
// thread that completes the future.
//
// The catalog holds named schemas. It serves:
//   GetFlightInfo, GetSchema, PollFlightInfo  look up one flight by path
// RPCs it does not override answer UNIMPLEMENTED.
//
// Usage:
//   flight-async-server-example --port=31337
//   flight-async-server-example --port=0 --demo

DEFINE_int32(port, 0, "Port to listen on (0 picks a free port)");
DEFINE_bool(demo, false, "Run the client self-check against the server, then exit");

namespace flight = arrow::flight;

namespace {

/// \brief Run `fn` on Arrow's IO thread pool and return its future.
template <typename Fn>
auto OnIoPool(Fn&& fn) {
  return arrow::DeferNotOk(
      arrow::io::default_io_context().executor()->Submit(std::forward<Fn>(fn)));
}

/// \brief The flights the server knows, shared by its handlers.
class Catalog {
 public:
  Catalog() {
    Add("prices", arrow::schema({arrow::field("ticker", arrow::utf8()),
                                 arrow::field("price", arrow::float64())}));
    Add("trades", arrow::schema({arrow::field("ticker", arrow::utf8()),
                                 arrow::field("quantity", arrow::int64())}));
  }

  arrow::Result<std::shared_ptr<flight::FlightInfo>> Find(
      const flight::FlightDescriptor& descriptor) const {
    if (descriptor.type != flight::FlightDescriptor::PATH ||
        descriptor.path.size() != 1) {
      return arrow::Status::Invalid("Expected a one-element path, got ",
                                    descriptor.ToString());
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = schemas_.find(descriptor.path[0]);
    if (it == schemas_.end()) {
      return arrow::Status::KeyError("No flight named ", descriptor.path[0]);
    }
    return MakeInfo(it->first, it->second);
  }

 private:
  void Add(std::string name, std::shared_ptr<arrow::Schema> schema) {
    schemas_.emplace(std::move(name), std::move(schema));
  }

  static arrow::Result<std::shared_ptr<flight::FlightInfo>> MakeInfo(
      const std::string& name, const std::shared_ptr<arrow::Schema>& schema) {
    ARROW_ASSIGN_OR_RAISE(auto info, flight::FlightInfo::Make(
                                         *schema, flight::FlightDescriptor::Path({name}),
                                         /*endpoints=*/{}, /*total_records=*/-1,
                                         /*total_bytes=*/-1));
    return std::make_shared<flight::FlightInfo>(std::move(info));
  }

  mutable std::mutex mutex_;
  std::map<std::string, std::shared_ptr<arrow::Schema>> schemas_;
};

class ExampleServer : public flight::AsyncGenericFlightServerBase {
 public:
  arrow::Future<std::shared_ptr<flight::FlightInfo>> GetFlightInfoAsync(
      const flight::ServerCallContext&,
      const flight::FlightDescriptor& request) override {
    // `request` is only valid during this call: the lambda captures a copy.
    return OnIoPool([this, request] { return catalog_.Find(request); });
  }

  arrow::Future<std::shared_ptr<flight::SchemaResult>> GetSchemaAsync(
      const flight::ServerCallContext&,
      const flight::FlightDescriptor& request) override {
    return OnIoPool(
        [this, request]() -> arrow::Result<std::shared_ptr<flight::SchemaResult>> {
          ARROW_ASSIGN_OR_RAISE(auto info, catalog_.Find(request));
          arrow::ipc::DictionaryMemo memo;
          ARROW_ASSIGN_OR_RAISE(auto schema, info->GetSchema(&memo));
          return flight::SchemaResult::Make(*schema);
        });
  }

  arrow::Future<std::shared_ptr<flight::PollInfo>> PollFlightInfoAsync(
      const flight::ServerCallContext&,
      const flight::FlightDescriptor& request) override {
    // The catalog answers at once, so the query is always complete.
    return OnIoPool(
        [this, request]() -> arrow::Result<std::shared_ptr<flight::PollInfo>> {
          ARROW_ASSIGN_OR_RAISE(auto info, catalog_.Find(request));
          auto poll = std::make_shared<flight::PollInfo>();
          poll->info = std::make_unique<flight::FlightInfo>(std::move(*info));
          poll->progress = 1.0;
          return poll;
        });
  }

 private:
  Catalog catalog_;
};

/// \brief The --demo self-check: call every served RPC once.
arrow::Status RunDemo(int port) {
  ARROW_ASSIGN_OR_RAISE(auto location, flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, flight::FlightClient::Connect(location));
  const auto prices = flight::FlightDescriptor::Path({"prices"});

  ARROW_ASSIGN_OR_RAISE(auto info, client->GetFlightInfo(prices));
  std::cout << "GetFlightInfo: " << info->descriptor().ToString() << std::endl;

  ARROW_ASSIGN_OR_RAISE(auto schema_result, client->GetSchema(prices));
  arrow::ipc::DictionaryMemo memo;
  ARROW_ASSIGN_OR_RAISE(auto schema, schema_result->GetSchema(&memo));
  std::cout << "GetSchema: " << schema->ToString() << std::endl;

  ARROW_ASSIGN_OR_RAISE(auto poll, client->PollFlightInfo(prices));
  std::cout << "PollFlightInfo: progress=" << poll->progress.value_or(0) << std::endl;

  // An unknown flight fails with the handler's own status.
  if (client->GetFlightInfo(flight::FlightDescriptor::Path({"missing"})).ok()) {
    return arrow::Status::Invalid("An unknown flight was served");
  }
  std::cout << "PASS" << std::endl;
  return client->Close();
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 1) {
    // As in the other examples: a bare run (e.g. from ctest) does not start a
    // server, it just prints the usage.
    std::cout << "Usage: " << argv[0] << " [--port=PORT] [--demo]" << std::endl;
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
  if (status.ok()) status = server.Serve();
  if (!status.ok()) {
    std::cerr << status.ToString() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}
