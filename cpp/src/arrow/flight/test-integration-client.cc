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

// Client implementation for Flight integration testing. Requests the
// given path from the Flight server, then serializes the result back
// to JSON and writes it to standard out.

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

using namespace arrow;

DEFINE_string(host, "localhost", "Server port to connect to");
DEFINE_int32(port, 31337, "Server port to connect to");
DEFINE_string(path, "", "Resource path to request");
DEFINE_string(output, "", "Where to write requested resource");

int main(int argc, char** argv) {
  gflags::SetUsageMessage("Integration testing client for Flight.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::unique_ptr<flight::FlightClient> client;
  ABORT_NOT_OK(flight::FlightClient::Connect(FLAGS_host, FLAGS_port, &client));

  flight::FlightDescriptor descr{flight::FlightDescriptor::PATH, "", {FLAGS_path}};
  std::unique_ptr<flight::FlightInfo> info;
  ABORT_NOT_OK(client->GetFlightInfo(descr, &info));

  std::shared_ptr<Schema> schema;
  ABORT_NOT_OK(info->GetSchema(&schema));
  flight::Ticket ticket = info->endpoints()[0].ticket;
  std::unique_ptr<RecordBatchReader> stream;
  ABORT_NOT_OK(client->DoGet(ticket, schema, &stream));

  std::shared_ptr<io::FileOutputStream> out_file;
  ABORT_NOT_OK(io::FileOutputStream::Open(FLAGS_output, &out_file));
  std::shared_ptr<ipc::RecordBatchWriter> writer;
  ABORT_NOT_OK(ipc::RecordBatchFileWriter::Open(out_file.get(), schema, &writer));

  std::shared_ptr<RecordBatch> chunk;
  for (;;) {
    ABORT_NOT_OK(stream->ReadNext(&chunk));
    if (chunk == nullptr) break;
    ABORT_NOT_OK(writer->WriteRecordBatch(*chunk));
  }

  ABORT_NOT_OK(writer->Close());

  return 0;
}
