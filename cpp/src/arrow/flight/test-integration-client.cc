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

#include "arrow/flight/server.h"
#include "arrow/flight/test-util.h"

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(path, "", "Resource path to request");
DEFINE_string(output, "", "Where to write requested resource");

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing client for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<arrow::flight::FlightClient> client;
  ABORT_NOT_OK(arrow::flight::FlightClient::Connect(FLAGS_host, FLAGS_port, &client));

  arrow::flight::FlightDescriptor descr{
      arrow::flight::FlightDescriptor::PATH, "", {FLAGS_path}};
  std::unique_ptr<arrow::flight::FlightInfo> info;
  ABORT_NOT_OK(client->GetFlightInfo(descr, &info));

  std::shared_ptr<arrow::Schema> schema;
  ABORT_NOT_OK(info->GetSchema(&schema));

  if (info->endpoints().size() == 0) {
    std::cerr << "No endpoints returned from Flight server." << std::endl;
    return -1;
  }

  arrow::flight::Ticket ticket = info->endpoints()[0].ticket;
  std::unique_ptr<arrow::RecordBatchReader> stream;
  ABORT_NOT_OK(client->DoGet(ticket, schema, &stream));

  std::shared_ptr<arrow::io::FileOutputStream> out_file;
  ABORT_NOT_OK(arrow::io::FileOutputStream::Open(FLAGS_output, &out_file));
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  ABORT_NOT_OK(arrow::ipc::RecordBatchFileWriter::Open(out_file.get(), schema, &writer));

  std::shared_ptr<arrow::RecordBatch> chunk;
  while (true) {
    ABORT_NOT_OK(stream->ReadNext(&chunk));
    if (chunk == nullptr) break;
    ABORT_NOT_OK(writer->WriteRecordBatch(*chunk));
  }

  ABORT_NOT_OK(writer->Close());

  return 0;
}
