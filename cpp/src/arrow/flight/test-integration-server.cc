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
#include "arrow/ipc/json.h"
#include "arrow/record_batch.h"

#include "arrow/flight/server.h"
#include "arrow/flight/test-util.h"

DEFINE_int32(port, 31337, "Server port to listen on");

namespace arrow {
namespace flight {

class JsonReaderRecordBatchStream : public FlightDataStream {
 public:
  explicit JsonReaderRecordBatchStream(
      std::unique_ptr<ipc::internal::json::JsonReader>&& reader)
      : index_(0), pool_(default_memory_pool()), reader_(std::move(reader)) {}

  std::shared_ptr<Schema> schema() override { return reader_->schema(); }

  Status Next(ipc::internal::IpcPayload* payload) override {
    if (index_ >= reader_->num_record_batches()) {
      // Signal that iteration is over
      payload->metadata = nullptr;
      return Status::OK();
    }

    std::shared_ptr<RecordBatch> batch;
    RETURN_NOT_OK(reader_->ReadRecordBatch(index_, &batch));
    index_++;

    if (!batch) {
      // Signal that iteration is over
      payload->metadata = nullptr;
      return Status::OK();
    } else {
      return ipc::internal::GetRecordBatchPayload(*batch, pool_, payload);
    }
  }

 private:
  int index_;
  MemoryPool* pool_;
  std::unique_ptr<ipc::internal::json::JsonReader> reader_;
};

class FlightIntegrationTestServer : public FlightServerBase {
  Status ReadJson(const std::string& json_path,
                  std::unique_ptr<ipc::internal::json::JsonReader>* out) {
    std::shared_ptr<io::ReadableFile> in_file;
    std::cout << "Opening JSON file '" << json_path << "'" << std::endl;
    RETURN_NOT_OK(io::ReadableFile::Open(json_path, &in_file));

    int64_t file_size = 0;
    RETURN_NOT_OK(in_file->GetSize(&file_size));

    std::shared_ptr<Buffer> json_buffer;
    RETURN_NOT_OK(in_file->Read(file_size, &json_buffer));

    RETURN_NOT_OK(arrow::ipc::internal::json::JsonReader::Open(json_buffer, out));
    return Status::OK();
  }

  Status GetFlightInfo(const FlightDescriptor& request,
                       std::unique_ptr<FlightInfo>* info) override {
    if (request.type == FlightDescriptor::PATH) {
      if (request.path.size() == 0) {
        return Status::Invalid("Invalid path");
      }

      std::unique_ptr<arrow::ipc::internal::json::JsonReader> reader;
      RETURN_NOT_OK(ReadJson(request.path.back(), &reader));

      FlightEndpoint endpoint1({{request.path.back()}, {}});

      FlightInfo::Data flight_data;
      RETURN_NOT_OK(internal::SchemaToString(*reader->schema(), &flight_data.schema));
      flight_data.descriptor = request;
      flight_data.endpoints = {endpoint1};
      flight_data.total_records = reader->num_record_batches();
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
    std::unique_ptr<arrow::ipc::internal::json::JsonReader> reader;
    RETURN_NOT_OK(ReadJson(request.ticket, &reader));

    *data_stream = std::unique_ptr<FlightDataStream>(
        new JsonReaderRecordBatchStream(std::move(reader)));

    return Status::OK();
  }
};

}  // namespace flight
}  // namespace arrow

std::unique_ptr<arrow::flight::FlightIntegrationTestServer> g_server;

void Shutdown(int signal) {
  if (g_server != nullptr) {
    g_server->Shutdown();
  }
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing server for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // SIGTERM shuts down the server
  signal(SIGTERM, Shutdown);

  g_server.reset(new arrow::flight::FlightIntegrationTestServer);
  g_server->Run(FLAGS_port);
  return 0;
}
