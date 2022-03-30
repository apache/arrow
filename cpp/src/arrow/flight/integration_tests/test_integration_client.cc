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

// Client implementation for Flight integration testing. Loads
// RecordBatches from the given JSON file and uploads them to the
// Flight server, which stores the data and schema in memory. The
// client then requests the data from the server and compares it to
// the data originally uploaded.

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <gflags/gflags.h>

#include "arrow/io/file.h"
#include "arrow/io/test_common.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/extension_type.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/json_integration.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/integration_tests/test_integration.h"
#include "arrow/flight/test_util.h"

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(path, "", "Resource path to request");
DEFINE_string(scenario, "", "Integration test scenario to run");

namespace arrow {
namespace flight {
namespace integration_tests {

/// \brief Helper to read all batches from a JsonReader
Status ReadBatches(std::unique_ptr<testing::IntegrationJsonReader>& reader,
                   std::vector<std::shared_ptr<RecordBatch>>* chunks) {
  std::shared_ptr<RecordBatch> chunk;
  for (int i = 0; i < reader->num_record_batches(); i++) {
    RETURN_NOT_OK(reader->ReadRecordBatch(i, &chunk));
    RETURN_NOT_OK(chunk->ValidateFull());
    chunks->push_back(chunk);
  }
  return Status::OK();
}

/// \brief Upload the a list of batches to a Flight server, validating
/// the application metadata on the side.
Status UploadBatchesToFlight(const std::vector<std::shared_ptr<RecordBatch>>& chunks,
                             FlightStreamWriter& writer,
                             FlightMetadataReader& metadata_reader) {
  int counter = 0;
  for (const auto& chunk : chunks) {
    std::shared_ptr<Buffer> metadata = Buffer::FromString(std::to_string(counter));
    RETURN_NOT_OK(writer.WriteWithMetadata(*chunk, metadata));
    // Wait for the server to ack the result
    std::shared_ptr<Buffer> ack_metadata;
    RETURN_NOT_OK(metadata_reader.ReadMetadata(&ack_metadata));
    if (!ack_metadata) {
      return Status::Invalid("Expected metadata value: ", metadata->ToString(),
                             " but got nothing.");
    } else if (!ack_metadata->Equals(*metadata)) {
      return Status::Invalid("Expected metadata value: ", metadata->ToString(),
                             " but got: ", ack_metadata->ToString());
    }
    counter++;
  }
  return writer.Close();
}

/// \brief Retrieve the given Flight and compare to the original expected batches.
Status ConsumeFlightLocation(
    FlightClient* read_client, const Ticket& ticket,
    const std::vector<std::shared_ptr<RecordBatch>>& retrieved_data) {
  std::unique_ptr<FlightStreamReader> stream;
  RETURN_NOT_OK(read_client->DoGet(ticket, &stream));

  int counter = 0;
  const int expected = static_cast<int>(retrieved_data.size());
  for (const auto& original_batch : retrieved_data) {
    ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, stream->Next());
    if (chunk.data == nullptr) {
      return Status::Invalid("Got fewer batches than expected, received so far: ",
                             counter, " expected ", expected);
    }

    if (!original_batch->Equals(*chunk.data)) {
      return Status::Invalid("Batch ", counter, " does not match");
    }

    const auto st = chunk.data->ValidateFull();
    if (!st.ok()) {
      return Status::Invalid("Batch ", counter, " is not valid: ", st.ToString());
    }

    if (std::to_string(counter) != chunk.app_metadata->ToString()) {
      return Status::Invalid("Expected metadata value: " + std::to_string(counter) +
                             " but got: " + chunk.app_metadata->ToString());
    }
    counter++;
  }

  ARROW_ASSIGN_OR_RAISE(FlightStreamChunk chunk, stream->Next());
  if (chunk.data != nullptr) {
    return Status::Invalid("Got more batches than the expected ", expected);
  }

  return Status::OK();
}

class IntegrationTestScenario : public Scenario {
 public:
  Status MakeServer(std::unique_ptr<FlightServerBase>* server,
                    FlightServerOptions* options) override {
    ARROW_UNUSED(server);
    ARROW_UNUSED(options);
    return Status::NotImplemented("Not implemented, see test_integration_server.cc");
  }

  Status MakeClient(FlightClientOptions* options) override {
    ARROW_UNUSED(options);
    return Status::OK();
  }

  Status RunClient(std::unique_ptr<FlightClient> client) override {
    // Make sure the required extension types are registered.
    ExtensionTypeGuard uuid_ext_guard(uuid());
    ExtensionTypeGuard dict_ext_guard(dict_extension_type());

    FlightDescriptor descr{FlightDescriptor::PATH, "", {FLAGS_path}};

    // 1. Put the data to the server.
    std::unique_ptr<testing::IntegrationJsonReader> reader;
    std::cout << "Opening JSON file '" << FLAGS_path << "'" << std::endl;
    auto in_file = *io::ReadableFile::Open(FLAGS_path);
    ABORT_NOT_OK(
        testing::IntegrationJsonReader::Open(default_memory_pool(), in_file, &reader));

    std::shared_ptr<Schema> original_schema = reader->schema();
    std::vector<std::shared_ptr<RecordBatch>> original_data;
    ABORT_NOT_OK(ReadBatches(reader, &original_data));

    std::unique_ptr<FlightStreamWriter> write_stream;
    std::unique_ptr<FlightMetadataReader> metadata_reader;
    ABORT_NOT_OK(client->DoPut(descr, original_schema, &write_stream, &metadata_reader));
    ABORT_NOT_OK(UploadBatchesToFlight(original_data, *write_stream, *metadata_reader));

    // 2. Get the ticket for the data.
    std::unique_ptr<FlightInfo> info;
    ABORT_NOT_OK(client->GetFlightInfo(descr, &info));

    std::shared_ptr<Schema> schema;
    ipc::DictionaryMemo dict_memo;
    ABORT_NOT_OK(info->GetSchema(&dict_memo).Value(&schema));

    if (info->endpoints().size() == 0) {
      std::cerr << "No endpoints returned from Flight server." << std::endl;
      return Status::IOError("No endpoints returned from Flight server.");
    }

    for (const FlightEndpoint& endpoint : info->endpoints()) {
      const auto& ticket = endpoint.ticket;

      // 3. Stream data from the server, comparing individual batches.
      if (endpoint.locations.size() == 0) {
        RETURN_NOT_OK(ConsumeFlightLocation(client.get(), ticket, original_data));
      } else {
        for (const auto& location : endpoint.locations) {
          std::cout << "Verifying location " << location.ToString() << std::endl;
          std::unique_ptr<FlightClient> read_client;
          RETURN_NOT_OK(FlightClient::Connect(location, &read_client));
          RETURN_NOT_OK(ConsumeFlightLocation(read_client.get(), ticket, original_data));
          RETURN_NOT_OK(read_client->Close());
        }
      }
    }
    return Status::OK();
  }
};

}  // namespace integration_tests
}  // namespace flight
}  // namespace arrow

constexpr int kRetries = 3;

arrow::Status RunScenario(arrow::flight::integration_tests::Scenario* scenario) {
  auto options = arrow::flight::FlightClientOptions::Defaults();
  std::unique_ptr<arrow::flight::FlightClient> client;

  RETURN_NOT_OK(scenario->MakeClient(&options));
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp(FLAGS_host, FLAGS_port));
  RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, options, &client));
  return scenario->RunClient(std::move(client));
}

int main(int argc, char** argv) {
  arrow::util::ArrowLog::InstallFailureSignalHandler();

  gflags::SetUsageMessage("Integration testing client for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::shared_ptr<arrow::flight::integration_tests::Scenario> scenario;
  if (!FLAGS_scenario.empty()) {
    ARROW_CHECK_OK(
        arrow::flight::integration_tests::GetScenario(FLAGS_scenario, &scenario));
  } else {
    scenario =
        std::make_shared<arrow::flight::integration_tests::IntegrationTestScenario>();
  }

  // ARROW-11908: retry a few times in case a client is slow to bring up the server
  auto status = arrow::Status::OK();
  for (int i = 0; i < kRetries; i++) {
    status = RunScenario(scenario.get());
    if (status.ok()) break;
    // Failed, wait a bit and try again
    std::this_thread::sleep_for(std::chrono::milliseconds((i + 1) * 500));
  }
  ABORT_NOT_OK(status);

  arrow::util::ArrowLog::UninstallSignalAction();
  return 0;
}
