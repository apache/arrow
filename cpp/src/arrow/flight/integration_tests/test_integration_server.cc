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

// Server for integration testing.

// Integration testing covers files and scenarios. The former
// validates that Arrow data survives a round-trip through a Flight
// service. The latter tests specific features of Arrow Flight.

#include <signal.h>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/test_common.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/json_integration.h"
#include "arrow/util/logging.h"

#include "arrow/flight/integration_tests/test_integration.h"
#include "arrow/flight/serialization_internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/server_auth.h"
#include "arrow/flight/test_util.h"

DEFINE_int32(port, 31337, "Server port to listen on");
DEFINE_string(scenario, "", "Integration test senario to run");

namespace arrow {
namespace flight {
namespace integration_tests {

struct IntegrationDataset {
  std::shared_ptr<Schema> schema;
  std::vector<std::shared_ptr<RecordBatch>> chunks;
};

class RecordBatchListReader : public RecordBatchReader {
 public:
  explicit RecordBatchListReader(IntegrationDataset dataset)
      : dataset_(dataset), current_(0) {}

  std::shared_ptr<Schema> schema() const override { return dataset_.schema; }

  Status ReadNext(std::shared_ptr<RecordBatch>* batch) override {
    if (current_ >= dataset_.chunks.size()) {
      *batch = nullptr;
      return Status::OK();
    }
    *batch = dataset_.chunks[current_];
    current_++;
    return Status::OK();
  }

 private:
  IntegrationDataset dataset_;
  uint64_t current_;
};

class FlightIntegrationTestServer : public FlightServerBase {
  Status GetFlightInfo(const ServerCallContext& context, const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (request.type == FlightDescriptor::PATH) {
      if (request.path.size() == 0) {
        return Status::Invalid("Invalid path");
      }

      auto data = uploaded_chunks.find(request.path[0]);
      if (data == uploaded_chunks.end()) {
        return Status::KeyError("Could not find flight.", request.path[0]);
      }
      auto flight = data->second;

      ARROW_ASSIGN_OR_RAISE(auto server_location,
                            Location::ForGrpcTcp("127.0.0.1", port()));
      FlightEndpoint endpoint1({{request.path[0]}, {server_location}});

      FlightInfo::Data flight_data;
      RETURN_NOT_OK(internal::SchemaToString(*flight.schema, &flight_data.schema));
      flight_data.descriptor = request;
      flight_data.endpoints = {endpoint1};
      flight_data.total_records = 0;
      for (const auto& chunk : flight.chunks) {
        flight_data.total_records += chunk->num_rows();
      }
      flight_data.total_bytes = -1;
      FlightInfo value(flight_data);

      *info = std::unique_ptr<FlightInfo>(new FlightInfo(value));
      return Status::OK();
    } else {
      return Status::NotImplemented(request.type);
    }
  }

  Status DoGet(const ServerCallContext& context, const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    auto data = uploaded_chunks.find(request.ticket);
    if (data == uploaded_chunks.end()) {
      return Status::KeyError("Could not find flight.", request.ticket);
    }
    auto flight = data->second;

    *data_stream = std::unique_ptr<FlightDataStream>(
        new NumberingStream(std::unique_ptr<FlightDataStream>(new RecordBatchStream(
            std::shared_ptr<RecordBatchReader>(new RecordBatchListReader(flight))))));

    return Status::OK();
  }

  Status DoPut(const ServerCallContext& context,
               std::unique_ptr<FlightMessageReader> reader,
               std::unique_ptr<FlightMetadataWriter> writer) override {
    const FlightDescriptor& descriptor = reader->descriptor();

    if (descriptor.type != FlightDescriptor::DescriptorType::PATH) {
      return Status::Invalid("Must specify a path");
    } else if (descriptor.path.size() < 1) {
      return Status::Invalid("Must specify a path");
    }

    std::string key = descriptor.path[0];

    IntegrationDataset dataset;
    ARROW_ASSIGN_OR_RAISE(dataset.schema, reader->GetSchema());
    arrow::flight::FlightStreamChunk chunk;
    while (true) {
      ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());
      if (chunk.data == nullptr) break;
      RETURN_NOT_OK(chunk.data->ValidateFull());
      dataset.chunks.push_back(chunk.data);
      if (chunk.app_metadata) {
        RETURN_NOT_OK(writer->WriteMetadata(*chunk.app_metadata));
      }
    }
    uploaded_chunks[key] = dataset;
    return Status::OK();
  }

  std::unordered_map<std::string, IntegrationDataset> uploaded_chunks;
};

class IntegrationTestScenario : public Scenario {
 public:
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    server->reset(new FlightIntegrationTestServer());
    return Status::OK();
  }

  Status MakeClient(FlightClientOptions* options) override {
    ARROW_UNUSED(options);
    return Status::NotImplemented("Not implemented, see test_integration_client.cc");
  }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    ARROW_UNUSED(client);
    return Status::NotImplemented("Not implemented, see test_integration_client.cc");
  }
};

}  // namespace integration_tests
}  // namespace flight
}  // namespace arrow

std::unique_ptr<arrow::flight::FlightServerBase> g_server;

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing server for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::shared_ptr<arrow::flight::integration_tests::Scenario> scenario;

  if (!FLAGS_scenario.empty()) {
    ARROW_CHECK_OK(
        arrow::flight::integration_tests::GetScenario(FLAGS_scenario, &scenario));
  } else {
    scenario =
        std::make_shared<arrow::flight::integration_tests::IntegrationTestScenario>();
  }
  arrow::flight::Location location;
  ARROW_CHECK_OK(
      arrow::flight::Location::ForGrpcTcp("0.0.0.0", FLAGS_port).Value(&location));
  arrow::flight::FlightServerOptions options(location);

  ARROW_CHECK_OK(scenario->MakeServer(&g_server, &options));

  ARROW_CHECK_OK(g_server->Init(options));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(g_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Server listening on localhost:" << g_server->port() << std::endl;
  ARROW_CHECK_OK(g_server->Serve());
  return 0;
}
