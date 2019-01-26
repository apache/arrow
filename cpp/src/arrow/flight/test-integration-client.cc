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

// Client implementation for Flight integration testing. Requests the given
// path from the Flight server, which reads that file and sends it as a stream
// to the client. The client writes the server stream to the IPC file format at
// the given output file path. The integration test script then uses the
// existing integration test tools to compare the output binary with the
// original JSON

#include <iostream>
#include <memory>
#include <string>

#include <gflags/gflags.h>

#include "arrow/io/test-common.h"
#include "arrow/ipc/json.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"

#include "arrow/flight/server.h"
#include "arrow/flight/test-util.h"

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(path, "", "Resource path to request");

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing client for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<arrow::flight::FlightClient> client;
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(FLAGS_host, FLAGS_port, &client));

  arrow::flight::FlightDescriptor descr{
      arrow::flight::FlightDescriptor::PATH, "", {FLAGS_path}};

  // 1. Put the data to the server.
  std::unique_ptr<arrow::ipc::internal::json::JsonReader> reader;
  std::shared_ptr<arrow::io::ReadableFile> in_file;
  std::cout << "Opening JSON file '" << FLAGS_path << "'" << std::endl;
  ABORT_NOT_OK(arrow::io::ReadableFile::Open(FLAGS_path, &in_file));

  int64_t file_size = 0;
  ABORT_NOT_OK(in_file->GetSize(&file_size));

  std::shared_ptr<arrow::Buffer> json_buffer;
  ABORT_NOT_OK(in_file->Read(file_size, &json_buffer));

  ABORT_NOT_OK(arrow::ipc::internal::json::JsonReader::Open(json_buffer, &reader));

  std::unique_ptr<arrow::ipc::RecordBatchWriter> write_stream;
  ABORT_NOT_OK(client->DoPut(descr, reader->schema(), &write_stream));

  std::vector<std::shared_ptr<arrow::RecordBatch>> original_chunks;
  for (int i = 0; i < reader->num_record_batches(); i++) {
    std::shared_ptr<arrow::RecordBatch> batch;
    ABORT_NOT_OK(reader->ReadRecordBatch(i, &batch));
    original_chunks.push_back(batch);
    ABORT_NOT_OK(write_stream->WriteRecordBatch(*batch));
  }
  ABORT_NOT_OK(write_stream->Close());

  // 2. Get the ticket for the data.
  std::unique_ptr<arrow::flight::FlightInfo> info;
  ABORT_NOT_OK(client->GetFlightInfo(descr, &info));

  // 3. Download the data from the server.
  std::shared_ptr<arrow::Schema> schema;
  ABORT_NOT_OK(info->GetSchema(&schema));

  if (info->endpoints().size() == 0) {
    std::cerr << "No endpoints returned from Flight server." << std::endl;
    return -1;
  }

  arrow::flight::Ticket ticket = info->endpoints()[0].ticket;
  std::unique_ptr<arrow::RecordBatchReader> stream;
  ABORT_NOT_OK(client->DoGet(ticket, schema, &stream));

  std::vector<std::shared_ptr<arrow::RecordBatch>> retrieved_chunks;
  std::shared_ptr<arrow::RecordBatch> chunk;
  while (true) {
    ABORT_NOT_OK(stream->ReadNext(&chunk));
    if (chunk == nullptr) break;
    retrieved_chunks.push_back(chunk);
  }

  // 4. Validate that the data is equal.

  std::shared_ptr<arrow::Table> original_data;
  std::shared_ptr<arrow::Table> retrieved_data;

  ABORT_NOT_OK(
      arrow::Table::FromRecordBatches(reader->schema(), original_chunks, &original_data));
  ABORT_NOT_OK(
      arrow::Table::FromRecordBatches(schema, retrieved_chunks, &retrieved_data));

  if (!original_data->Equals(*retrieved_data)) {
    std::cerr << "Data does not match!" << std::endl;
    return 1;
  }

  return 0;
}
