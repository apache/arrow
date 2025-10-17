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

#include "arrow/array.h"

#include "arrow/testing/gtest_util.h"

#include "arrow/flight/sql/odbc/odbc_impl/flight_sql_stream_chunk_buffer.h"
#include "arrow/flight/sql/odbc/odbc_impl/json_converter.h"
#include "arrow/flight/test_flight_server.h"
#include "arrow/flight/test_util.h"

#include <gtest/gtest.h>

namespace arrow::flight::sql::odbc {

using arrow::Array;
using arrow::flight::FlightCallOptions;
using arrow::flight::FlightClientOptions;
using arrow::flight::FlightDescriptor;
using arrow::flight::FlightEndpoint;
using arrow::flight::Location;
using arrow::flight::Ticket;
using arrow::flight::sql::FlightSqlClient;

class FlightStreamChunkBufferTest : public ::testing::Test {
  // Sets up two mock servers for each test case.
  // This is for testing endpoint iteration only.

 protected:
  void SetUp() override {
    // Set up server 1
    server1 = std::make_shared<arrow::flight::TestFlightServer>();
    ASSERT_OK_AND_ASSIGN(auto location1, Location::ForGrpcTcp("0.0.0.0", 0));
    arrow::flight::FlightServerOptions options1(location1);
    ASSERT_OK(server1->Init(options1));
    ASSERT_OK_AND_ASSIGN(server_location1,
                         Location::ForGrpcTcp("localhost", server1->port()));

    // Set up server 2
    server2 = std::make_shared<arrow::flight::TestFlightServer>();
    ASSERT_OK_AND_ASSIGN(auto location2, Location::ForGrpcTcp("0.0.0.0", 0));
    arrow::flight::FlightServerOptions options2(location2);
    ASSERT_OK(server2->Init(options2));
    ASSERT_OK_AND_ASSIGN(server_location2,
                         Location::ForGrpcTcp("localhost", server2->port()));

    // Make SQL Client that is connected to server 1
    ASSERT_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location1));
    sql_client.reset(new FlightSqlClient(std::move(client)));
  }

  void TearDown() override {
    ASSERT_OK(server1->Shutdown());
    ASSERT_OK(server2->Shutdown());
  }

 public:
  arrow::flight::Location server_location1;
  std::shared_ptr<arrow::flight::TestFlightServer> server1;
  arrow::flight::Location server_location2;
  std::shared_ptr<arrow::flight::TestFlightServer> server2;
  std::shared_ptr<FlightSqlClient> sql_client;
};

FlightInfo MultipleEndpointsFlightInfo(Location location1, Location location2) {
  // Sever will generate random data for `ticket-ints-1`
  FlightEndpoint endpoint1({Ticket{"ticket-ints-1"}, {location1}, std::nullopt, {}});
  FlightEndpoint endpoint2({Ticket{"ticket-ints-1"}, {location2}, std::nullopt, {}});

  FlightDescriptor descr1{FlightDescriptor::PATH, "", {"examples", "ints"}};

  auto schema1 = arrow::flight::ExampleIntSchema();

  return arrow::flight::MakeFlightInfo(*schema1, descr1, {endpoint1, endpoint2}, 1000,
                                       100000, false, "");
}

void VerifyArraysContainIntsOnly(std::shared_ptr<Array> intArray) {
  for (int64_t i = 0; i < intArray->length(); ++i) {
    // null values are accepted
    if (!intArray->IsNull(i)) {
      auto scalar_data = intArray->GetScalar(i).ValueOrDie();
      std::string scalar_str = ConvertToJson(*scalar_data);
      ASSERT_TRUE(std::all_of(scalar_str.begin(), scalar_str.end(), ::isdigit));
    }
  }
}

TEST_F(FlightStreamChunkBufferTest, TestMultipleEndpointsInt) {
  FlightClientOptions client_options = FlightClientOptions::Defaults();
  FlightCallOptions options;
  FlightInfo info = MultipleEndpointsFlightInfo(server_location1, server_location2);
  std::shared_ptr<FlightInfo> info_ptr = std::make_shared<FlightInfo>(info);

  FlightStreamChunkBuffer chunk_buffer(*sql_client, client_options, options, info_ptr);

  FlightStreamChunk current_chunk;

  // Server returns 5 batch of results from each endpoints.
  // Each batch contains 8 columns
  int num_chunks = 0;
  while (chunk_buffer.GetNext(&current_chunk)) {
    num_chunks++;

    int num_cols = current_chunk.data->num_columns();
    ASSERT_EQ(8, num_cols);

    for (int i = 0; i < num_cols; i++) {
      auto array = current_chunk.data->column(i);
      // Each array has random length
      ASSERT_GT(array->length(), 0);

      VerifyArraysContainIntsOnly(array);
    }
  }

  // Verify 5 batches of data is returned by each of the two endpoints.
  // In total 10 batches should be returned.
  ASSERT_EQ(10, num_chunks);
}
}  // namespace arrow::flight::sql::odbc
