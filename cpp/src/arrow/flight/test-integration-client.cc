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

#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/file.h"
#include "arrow/io/test-common.h"
#include "arrow/ipc/dictionary.h"
#include "arrow/ipc/json-integration.h"
#include "arrow/ipc/writer.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"

#include "arrow/flight/api.h"
#include "arrow/flight/test-util.h"

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(path, "", "Resource path to request");

/// \brief Helper to read a MetadataRecordBatchReader into a Table.
arrow::Status ReadToTable(arrow::flight::MetadataRecordBatchReader& reader,
                          std::shared_ptr<arrow::Table>* retrieved_data) {
  // For integration testing, we expect the server numbers the
  // batches, to test the application metadata part of the spec.
  std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
  arrow::flight::FlightStreamChunk chunk;
  int counter = 0;
  while (true) {
    RETURN_NOT_OK(reader.Next(&chunk));
    if (!chunk.data) break;
    retrieved_chunks.push_back(chunk.data);
    if (std::to_string(counter) != chunk.app_metadata->ToString()) {
      return arrow::Status::Invalid(
          "Expected metadata value: " + std::to_string(counter) +
          " but got: " + chunk.app_metadata->ToString());
    }
    counter++;
  }
  return arrow::Table::FromRecordBatches(reader.schema(), retrieved_chunks,
                                         retrieved_data);
}

/// \brief Helper to read a JsonReader into a Table.
arrow::Status ReadToTable(std::unique_ptr<arrow::ipc::internal::json::JsonReader>& reader,
                          std::shared_ptr<arrow::Table>* retrieved_data) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
  std::shared_ptr<arrow::RecordBatch> chunk;
  for (int i = 0; i < reader->num_record_batches(); i++) {
    RETURN_NOT_OK(reader->ReadRecordBatch(i, &chunk));
    retrieved_chunks.push_back(chunk);
  }
  return arrow::Table::FromRecordBatches(reader->schema(), retrieved_chunks,
                                         retrieved_data);
}

/// \brief Upload the contents of a RecordBatchReader to a Flight
/// server, validating the application metadata on the side.
arrow::Status UploadReaderToFlight(arrow::RecordBatchReader* reader,
                                   arrow::flight::FlightStreamWriter& writer,
                                   arrow::flight::FlightMetadataReader& metadata_reader) {
  int counter = 0;
  while (true) {
    std::shared_ptr<arrow::RecordBatch> chunk;
    RETURN_NOT_OK(reader->ReadNext(&chunk));
    if (chunk == nullptr) break;
    std::shared_ptr<arrow::Buffer> metadata =
        arrow::Buffer::FromString(std::to_string(counter));
    RETURN_NOT_OK(writer.WriteWithMetadata(*chunk, metadata));
    // Wait for the server to ack the result
    std::shared_ptr<arrow::Buffer> ack_metadata;
    RETURN_NOT_OK(metadata_reader.ReadMetadata(&ack_metadata));
    if (!ack_metadata->Equals(*metadata)) {
      return arrow::Status::Invalid("Expected metadata value: " + metadata->ToString() +
                                    " but got: " + ack_metadata->ToString());
    }
    counter++;
  }
  return writer.Close();
}

/// \brief Helper to read a flight into a Table.
arrow::Status ConsumeFlightLocation(const arrow::flight::Location& location,
                                    const arrow::flight::Ticket& ticket,
                                    const std::shared_ptr<arrow::Schema>& schema,
                                    std::shared_ptr<arrow::Table>* retrieved_data) {
  std::unique_ptr<arrow::flight::FlightClient> read_client;
  RETURN_NOT_OK(arrow::flight::FlightClient::Connect(location, &read_client));

  std::unique_ptr<arrow::flight::FlightStreamReader> stream;
  RETURN_NOT_OK(read_client->DoGet(ticket, &stream));

  return ReadToTable(*stream, retrieved_data);
}

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing client for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<arrow::flight::FlightClient> client;
  arrow::flight::Location location;
  ABORT_NOT_OK(arrow::flight::Location::ForGrpcTcp(FLAGS_host, FLAGS_port, &location));
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(location, &client));

  arrow::flight::FlightDescriptor descr{
      arrow::flight::FlightDescriptor::PATH, "", {FLAGS_path}};

  // 1. Put the data to the server.
  std::unique_ptr<arrow::ipc::internal::json::JsonReader> reader;
  std::cout << "Opening JSON file '" << FLAGS_path << "'" << std::endl;
  std::shared_ptr<arrow::io::ReadableFile> in_file;
  ABORT_NOT_OK(arrow::io::ReadableFile::Open(FLAGS_path, &in_file));
  ABORT_NOT_OK(arrow::ipc::internal::json::JsonReader::Open(arrow::default_memory_pool(),
                                                            in_file, &reader));

  std::shared_ptr<arrow::Table> original_data;
  ABORT_NOT_OK(ReadToTable(reader, &original_data));

  std::unique_ptr<arrow::flight::FlightStreamWriter> write_stream;
  std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
  ABORT_NOT_OK(client->DoPut(descr, reader->schema(), &write_stream, &metadata_reader));
  std::unique_ptr<arrow::RecordBatchReader> table_reader(
      new arrow::TableBatchReader(*original_data));
  ABORT_NOT_OK(UploadReaderToFlight(table_reader.get(), *write_stream, *metadata_reader));

  // 2. Get the ticket for the data.
  std::unique_ptr<arrow::flight::FlightInfo> info;
  ABORT_NOT_OK(client->GetFlightInfo(descr, &info));

  std::shared_ptr<arrow::Schema> schema;
  arrow::ipc::DictionaryMemo dict_memo;
  ABORT_NOT_OK(info->GetSchema(&dict_memo, &schema));

  if (info->endpoints().size() == 0) {
    std::cerr << "No endpoints returned from Flight server." << std::endl;
    return -1;
  }

  for (const arrow::flight::FlightEndpoint& endpoint : info->endpoints()) {
    const auto& ticket = endpoint.ticket;

    auto locations = endpoint.locations;
    if (locations.size() == 0) {
      locations = {location};
    }

    for (const auto location : locations) {
      std::cout << "Verifying location " << location.ToString() << std::endl;
      // 3. Download the data from the server.
      std::shared_ptr<arrow::Table> retrieved_data;
      ABORT_NOT_OK(ConsumeFlightLocation(location, ticket, schema, &retrieved_data));

      // 4. Validate that the data is equal.
      if (!original_data->Equals(*retrieved_data)) {
        std::cerr << "Data does not match!" << std::endl;
        return 1;
      }
    }
  }
  return 0;
}
