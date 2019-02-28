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

// Example server implementation for integration testing purposes

#include <signal.h>
#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/test-common.h"
#include "arrow/ipc/json-integration.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/util/logging.h"

#include "arrow/flight/internal.h"
#include "arrow/flight/server.h"
#include "arrow/flight/test-util.h"

DEFINE_int32(port, 31337, "Server port to listen on");

namespace arrow {
namespace flight {

class FlightIntegrationTestServer : public FlightServerBase {
  Status GetFlightInfo(const FlightDescriptor& request,
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

      FlightEndpoint endpoint1({{request.path[0]}, {}});

      FlightInfo::Data flight_data;
      RETURN_NOT_OK(internal::SchemaToString(*flight->schema(), &flight_data.schema));
      flight_data.descriptor = request;
      flight_data.endpoints = {endpoint1};
      flight_data.total_records = flight->num_rows();
      flight_data.total_bytes = -1;
      FlightInfo value(flight_data);

      *info = std::unique_ptr<FlightInfo>(new FlightInfo(value));
      return Status::OK();
    } else {
      return Status::NotImplemented(request.type);
    }
  }

  Status DoGet(const Ticket& request,
               std::unique_ptr<FlightDataStream>* data_stream) override {
    auto data = uploaded_chunks.find(request.ticket);
    if (data == uploaded_chunks.end()) {
      return Status::KeyError("Could not find flight.", request.ticket);
    }
    auto flight = data->second;

    *data_stream = std::unique_ptr<FlightDataStream>(new RecordBatchStream(
        std::shared_ptr<RecordBatchReader>(new TableBatchReader(*flight))));

    return Status::OK();
  }

  Status DoPut(std::unique_ptr<FlightMessageReader> reader) override {
    const FlightDescriptor& descriptor = reader->descriptor();

    if (descriptor.type != FlightDescriptor::DescriptorType::PATH) {
      return Status::Invalid("Must specify a path");
    } else if (descriptor.path.size() < 1) {
      return Status::Invalid("Must specify a path");
    }

    std::string key = descriptor.path[0];

    std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
    std::shared_ptr<arrow::RecordBatch> chunk;
    while (true) {
      RETURN_NOT_OK(reader->ReadNext(&chunk));
      if (chunk == nullptr) break;
      retrieved_chunks.push_back(chunk);
    }
    std::shared_ptr<arrow::Table> retrieved_data;
    RETURN_NOT_OK(arrow::Table::FromRecordBatches(reader->schema(), retrieved_chunks,
                                                  &retrieved_data));
    uploaded_chunks[key] = retrieved_data;
    return Status::OK();
  }

  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> uploaded_chunks;
};

}  // namespace flight
}  // namespace arrow

std::unique_ptr<arrow::flight::FlightIntegrationTestServer> g_server;

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing server for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  g_server.reset(new arrow::flight::FlightIntegrationTestServer);
  ARROW_CHECK_OK(g_server->Init(FLAGS_port));
  // Exit with a clean error code (0) on SIGTERM
  ARROW_CHECK_OK(g_server->SetShutdownOnSignals({SIGTERM}));

  std::cout << "Server listening on localhost:" << FLAGS_port << std::endl;
  ARROW_CHECK_OK(g_server->Serve());
  return 0;
}
