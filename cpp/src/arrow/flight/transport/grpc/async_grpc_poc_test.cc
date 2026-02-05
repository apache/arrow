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

#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include "arrow/flight/client.h"
#include "arrow/flight/transport/grpc/async_grpc_server.h"
#include "arrow/testing/gtest_util.h"

namespace arrow::flight::transport::grpc {

// Simple DoGet test. This creates a FlightServer and calls DoGet
// from a FlightClient. The server uses the async callback API and
// serializes FlightData directly from the ByteBuffer using FlightDataSerialize.
TEST(AsyncGrpcTest, BasicDoGet) {
  FlightCallbackService service;
  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:0", ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  // Connect existing arrow::flight::FlightClient implementation
  std::string uri = "grpc://localhost:" + std::to_string(port);
  ASSERT_OK_AND_ASSIGN(auto location, arrow::flight::Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));

  // Call DoGet
  arrow::flight::Ticket ticket{"test"};
  ASSERT_OK_AND_ASSIGN(auto reader, client->DoGet(ticket));

  // Read schema
  ASSERT_OK_AND_ASSIGN(auto schema, reader->GetSchema());
  ASSERT_EQ(schema->num_fields(), 2);
  ASSERT_EQ(schema->field(0)->name(), "a");
  ASSERT_EQ(schema->field(1)->name(), "b");

  // Read batches
  int batch_count = 0;
  while (true) {
    ASSERT_OK_AND_ASSIGN(auto chunk, reader->Next());
    if (!chunk.data) break;
    ASSERT_EQ(chunk.data->num_rows(), 5);
    batch_count++;
  }
  ASSERT_EQ(batch_count, 5);

  // Cleanup
  ASSERT_OK(client->Close());
  server->Shutdown();
  server->Wait();
}

// Simple DoPut test. This creates a FlightServer and calls DoPut
// from a FlightClient. The server uses the async callback API and
// deserializes FlightData directly from the ByteBuffer using FlightDataDeserialize.
TEST(AsyncGrpcTest, BasicDoPut) {
  FlightCallbackService service;
  int port = 0;
  ::grpc::ServerBuilder builder;
  builder.AddListeningPort("localhost:0", ::grpc::InsecureServerCredentials(), &port);
  builder.RegisterCallbackGenericService(&service);
  auto server = builder.BuildAndStart();
  ASSERT_NE(server, nullptr);

  // Connect existing arrow::flight::FlightClient implementation
  std::string uri = "grpc://localhost:" + std::to_string(port);
  ASSERT_OK_AND_ASSIGN(auto location, arrow::flight::Location::Parse(uri));
  ASSERT_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));

  // Create test data
  auto schema = arrow::schema(
      {arrow::field("a", arrow::int64()), arrow::field("b", arrow::int64())});
  auto batch =
      arrow::RecordBatch::Make(schema, 3,
                               {arrow::ArrayFromJSON(arrow::int64(), "[1, 2, 3]"),
                                arrow::ArrayFromJSON(arrow::int64(), "[10, 20, 30]")});

  // Call DoPut
  arrow::flight::FlightDescriptor descriptor =
      arrow::flight::FlightDescriptor::Path({"test"});
  ASSERT_OK_AND_ASSIGN(auto result, client->DoPut(descriptor, schema));

  // Send batches
  ASSERT_OK(result.writer->WriteRecordBatch(*batch));
  ASSERT_OK(result.writer->WriteRecordBatch(*batch));
  ASSERT_OK(result.writer->Close());

  // Cleanup
  ASSERT_OK(client->Close());
  server->Shutdown();
  server->Wait();
}
}  // namespace arrow::flight::transport::grpc
